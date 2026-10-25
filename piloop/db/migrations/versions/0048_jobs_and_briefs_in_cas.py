"""``leases.job_cid`` and ``runs.brief_cid``: a lease's job and a run's brief are blobs in the home's CAS.

A lease kept its job's JSON, about 90 KB for an agent's brief, also after it ended, and the run kept the same brief. The
upgrade puts each brief in the CAS (``migrate`` passes it): an agent's job names its brief by ``brief_cid``, so a run's
brief and its job's are one blob. Then it puts each job in the CAS, and replaces the columns. The downgrade reads them
back from the CAS.

``leases`` is rebuilt to make ``job_cid`` NOT NULL. The rebuild keeps AUTOINCREMENT and the table's sequence: a fencing
token must never be used again, also the token of a lease that a cascade deleted. The move of a brief is not a change
of its run: ``runs_updated_at`` is off while it runs, so ``updated_at`` stays.

Revision ID: 0048
Revises: 0047
"""

import json
from collections.abc import Callable, Sequence

import sqlalchemy as sa
from alembic import op
from alembic.operations import BatchOperations
from covalence import Cid

from piloop.db.session import migration_cas
from piloop.db.triggers import recreate

revision: str = '0048'
down_revision: str | None = '0047'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column('leases', sa.Column('job_cid', sa.String(), nullable=True))
    _put('leases', 'job', 'job_cid', _brief_by_address)
    _rebuild(lambda batch: (batch.alter_column('job_cid', nullable=False), batch.drop_column('job')))
    op.add_column('runs', sa.Column('brief_cid', sa.String(), nullable=True))
    op.execute('DROP TRIGGER runs_updated_at')
    _put('runs', 'brief', 'brief_cid')
    recreate(op, 'runs')
    op.execute('ALTER TABLE runs DROP COLUMN brief')


def downgrade() -> None:
    op.add_column('runs', sa.Column('brief', sa.String(), nullable=True))
    op.execute('DROP TRIGGER runs_updated_at')
    _get('runs', 'brief_cid', 'brief')
    recreate(op, 'runs')
    op.execute('ALTER TABLE runs DROP COLUMN brief_cid')
    op.execute("ALTER TABLE leases ADD COLUMN job JSON DEFAULT '{}' NOT NULL")
    _get('leases', 'job_cid', 'job', _brief_inline)
    op.execute('ALTER TABLE leases DROP COLUMN job_cid')


def _same(value: str) -> str:
    return value


def _brief_by_address(job: str) -> str:
    """An agent's job with ``brief_cid``, its brief's address, in place of the text."""
    found = json.loads(job)
    if found.get('kind') != 'agent':
        return job
    brief = found.pop('brief', None)
    found['brief_cid'] = None if brief is None else str(migration_cas().put(brief.encode()))
    return json.dumps(found)


def _brief_inline(job: str) -> str:
    """An agent's job with its ``brief`` text, read from the CAS, in place of ``brief_cid``."""
    found = json.loads(job)
    if found.get('kind') != 'agent':
        return job
    address = found.pop('brief_cid', None)
    found['brief'] = None if address is None else _read(Cid(address), 'the brief of a job').decode()
    return json.dumps(found)


def _read(address: Cid, what: str) -> bytes:
    """The blob at ``address``. Raises when the CAS lacks it: the downgrade cannot go on."""
    if (value := migration_cas().get(address)) is None:
        raise RuntimeError(
            f'{what} is {address}, which the CAS lacks: restore the database backup that the upgrade wrote'
            ' (piloop.db.bak-…) instead of downgrading'
        )
    return value


def _put(table: str, text: str, cid: str, change: Callable[[str], str] = _same) -> None:
    """Put each ``text`` of ``table`` in the CAS, as ``change`` makes it, and set ``cid`` to its address.

    One row at a time: they are large.
    """
    cas, con = migration_cas(), op.get_bind()
    for (row,) in con.execute(sa.text(f'SELECT id FROM {table} WHERE {text} IS NOT NULL ORDER BY id')).all():
        value = con.execute(sa.text(f'SELECT {text} FROM {table} WHERE id = :id'), {'id': row}).scalar_one()
        address = str(cas.put(change(value).encode()))
        con.execute(sa.text(f'UPDATE {table} SET {cid} = :cid WHERE id = :id'), {'cid': address, 'id': row})


def _get(table: str, cid: str, text: str, change: Callable[[str], str] = _same) -> None:
    """Set each ``text`` of ``table`` to the blob at its ``cid``, as ``change`` makes it. Raises when the CAS lacks one."""
    con = op.get_bind()
    for (row,) in con.execute(sa.text(f'SELECT id FROM {table} WHERE {cid} IS NOT NULL ORDER BY id')).all():
        address = con.execute(sa.text(f'SELECT {cid} FROM {table} WHERE id = :id'), {'id': row}).scalar_one()
        value = change(_read(Cid(address), f'the {text} of {table} row {row}').decode())
        con.execute(sa.text(f'UPDATE {table} SET {text} = :value WHERE id = :id'), {'value': value, 'id': row})


def _rebuild(change: Callable[[BatchOperations], object]) -> None:
    """Rebuild ``leases`` with ``change``, with AUTOINCREMENT, and with the sequence that it had."""
    con = op.get_bind()
    seq = con.execute(sa.text("SELECT seq FROM sqlite_sequence WHERE name = 'leases'")).scalar()
    with op.batch_alter_table('leases', recreate='always', table_kwargs={'sqlite_autoincrement': True}) as batch:
        change(batch)
    con.execute(sa.text("DELETE FROM sqlite_sequence WHERE name IN ('leases', '_alembic_tmp_leases')"))
    if seq is not None:
        con.execute(sa.text("INSERT INTO sqlite_sequence (name, seq) VALUES ('leases', :seq)"), {'seq': seq})
