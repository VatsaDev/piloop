"""Workers that a provisioner starts: ``worker_instances``, a drained worker, and events on an instance.

- ``worker_instances``: each worker that a pool's provisioner starts, from before its start to its end.
- ``lease_workers.drained_at``, and the ``draining`` reason of ``lease_workers.no_lease``: a drained worker takes no
  more leases.
- An event or a comment can be on an ``instance``. SQLite cannot change a CHECK in place, so ``events``, ``comments``
  and ``lease_workers`` are rebuilt with their rows, ids and indexes. ``comments_escalation_event`` writes into
  ``events``, so it goes while ``events`` is swapped: the swap's rename checks every trigger of the schema.

The downgrade drops the table and the column, and the events and comments on an instance, which no older piloop reads.

Revision ID: 0044
Revises: 0043
"""

import re
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

import piloop.db.models
from piloop.db.triggers import recreate

revision: str = '0044'
down_revision: str | None = '0043'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
TARGET_KINDS = ('campaign', 'idea', 'run', 'candidate', 'measurement', 'agent_session', 'comment')
NO_LEASE = ('memory', 'load', 'slots', 'nothing', 'unreachable')


def upgrade() -> None:
    op.create_table(
        'worker_instances',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('pool', sa.String(), nullable=False),
        sa.Column(
            'state',
            sa.Enum(
                'starting',
                'running',
                'draining',
                'ended',
                name='workerinstancestate',
                native_enum=False,
                length=16,
                create_constraint=True,
            ),
            server_default='starting',
            nullable=False,
        ),
        sa.Column('handle', sa.JSON(), server_default=sa.text("'{}'"), nullable=False),
        sa.Column(
            'requested_at',
            piloop.db.models.UTCDateTime(),
            server_default=sa.text('(CURRENT_TIMESTAMP)'),
            nullable=False,
        ),
        sa.Column('registered_at', piloop.db.models.UTCDateTime(), nullable=True),
        sa.Column('ended_at', piloop.db.models.UTCDateTime(), nullable=True),
        sa.Column(
            'end',
            sa.Enum(
                'drained',
                'register_timeout',
                'start_refused',
                'lost',
                'gone',
                name='instanceend',
                native_enum=False,
                length=16,
                create_constraint=True,
            ),
            nullable=True,
        ),
        sa.CheckConstraint('(state = \'ended\') = ("end" IS NOT NULL)', name='worker_instances.an ended one says why'),
        sa.CheckConstraint("typeof(name) IN ('text')", name='worker_instances.name must be a text value'),
        sa.CheckConstraint("typeof(pool) IN ('text')", name='worker_instances.pool must be a text value'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name'),
        sqlite_autoincrement=True,
    )
    op.create_index('ix_worker_instances_state', 'worker_instances', ['state'], unique=False)
    op.add_column('lease_workers', sa.Column('drained_at', piloop.db.models.UTCDateTime(), nullable=True))
    _check(('lease_workers',), 'no_lease', ('draining', *NO_LEASE))
    _check(('events', 'comments'), 'target_kind', (*TARGET_KINDS, 'instance'))


def downgrade() -> None:
    on_instance = "SELECT id FROM comments WHERE target_kind = 'instance'"
    op.execute(
        f"DELETE FROM events WHERE target_kind = 'instance' OR target_kind = 'comment' AND target_id IN ({on_instance})"
    )
    op.execute("DELETE FROM comments WHERE target_kind = 'instance'")
    _check(('events', 'comments'), 'target_kind', TARGET_KINDS)
    op.execute("UPDATE lease_workers SET no_lease = NULL WHERE no_lease = 'draining'")
    _check(('lease_workers',), 'no_lease', NO_LEASE)
    op.execute('ALTER TABLE lease_workers DROP COLUMN drained_at')
    op.drop_index('ix_worker_instances_state', table_name='worker_instances')
    op.drop_table('worker_instances')


def _check(tables: Sequence[str], column: str, values: Sequence[str]) -> None:
    """Rebuild each of ``tables`` with its CHECK of ``column`` taking ``values``, and give it back its triggers."""
    if 'events' in tables:
        op.execute('DROP TRIGGER comments_escalation_event')  # ``comments``' own, which its recreate installs
    for table in tables:
        _rebuild(table, column, values)
    for table in tables:
        recreate(op, table)


def _rebuild(table: str, column: str, values: Sequence[str]) -> None:
    """Copy ``table`` into a new one whose CHECK of ``column`` takes ``values``, with its ids, then swap them."""
    bind = op.get_bind()
    sql = bind.execute(
        text("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = :t"), {'t': table}
    ).scalar_one()
    indexes = (
        bind.execute(
            text("SELECT sql FROM sqlite_master WHERE type = 'index' AND tbl_name = :t AND sql IS NOT NULL"),
            {'t': table},
        )
        .scalars()
        .all()
    )
    stored = bind.execute(text(f"SELECT name FROM pragma_table_xinfo('{table}') WHERE hidden = 0")).scalars().all()
    columns = ', '.join(f'"{c}"' for c in stored)
    check = f'CHECK ({column} IN ({", ".join(repr(v) for v in values)}))'
    new, found = re.subn(rf'CHECK \({column} IN \([^)]*\)\)', check, sql)
    assert found == 1, f'{table} has no single CHECK of {column}: {sql}'
    op.execute(re.sub(rf'^CREATE TABLE "?{table}"?', f'CREATE TABLE {table}_new', new))
    op.execute(f'INSERT INTO {table}_new ({columns}) SELECT {columns} FROM {table}')
    op.execute(f'DROP TABLE {table}')
    op.execute(f'ALTER TABLE {table}_new RENAME TO {table}')
    for index in indexes:
        op.execute(index)
