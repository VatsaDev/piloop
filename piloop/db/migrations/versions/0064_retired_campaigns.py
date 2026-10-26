"""A campaign can be ``retired``, and only the campaigns that are not retired need unique names.

The ``status`` CHECK of ``campaigns`` admits ``retired``. The table's ``UNIQUE (name)`` becomes the partial unique
index ``ix_campaigns_name``, on the campaigns that are not retired. Batch mode rebuilds the table, keeps its row ids,
and drops its triggers, so ``recreate`` puts them back.

The downgrade makes each retired campaign ``stopped``. One whose name another campaign has too gets ``<name>.<id>``.

Revision ID: 0064
Revises: 0063
"""

from collections.abc import Iterator, Sequence
from contextlib import contextmanager

import sqlalchemy as sa
from alembic import op

from piloop.db.triggers import recreate

revision: str = '0064'
down_revision: str | None = '0063'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
BEFORE = ('active', 'stopped')
AFTER = ('active', 'stopped', 'retired')
UNNAMED = {'uq': 'uq_%(table_name)s_%(column_0_name)s'}
"""A name for the table's unnamed ``UNIQUE (name)``, so that batch mode can drop it."""


def upgrade() -> None:
    with _rebuilding(), op.batch_alter_table('campaigns', recreate='always', naming_convention=UNNAMED) as batch_op:
        batch_op.drop_constraint('uq_campaigns_name', type_='unique')
        batch_op.drop_constraint('campaignstatus', type_='check')
        batch_op.create_check_constraint('campaignstatus', f'status IN ({_listed(AFTER)})')
    op.create_index(
        'ix_campaigns_name', 'campaigns', ['name'], unique=True, sqlite_where=sa.text("status != 'retired'")
    )
    recreate(op, 'campaigns')


def downgrade() -> None:
    op.execute(
        "UPDATE campaigns SET name = name || '.' || id WHERE status = 'retired'"
        ' AND name IN (SELECT name FROM campaigns GROUP BY name HAVING count(*) > 1)'
    )
    op.execute("UPDATE campaigns SET status = 'stopped' WHERE status = 'retired'")
    op.drop_index('ix_campaigns_name', 'campaigns')
    with _rebuilding(), op.batch_alter_table('campaigns', recreate='always', naming_convention=UNNAMED) as batch_op:
        batch_op.drop_constraint('campaignstatus', type_='check')
        batch_op.create_check_constraint('campaignstatus', f'status IN ({_listed(BEFORE)})')
        batch_op.create_unique_constraint('uq_campaigns_name', ['name'])
    recreate(op, 'campaigns')


@contextmanager
def _rebuilding() -> Iterator[None]:
    """Let batch mode rename its copy to ``campaigns``. The triggers of other tables read ``campaigns``, and SQLite
    refuses the rename while they name a table that is gone, unless the rename is the legacy one."""
    op.execute('PRAGMA legacy_alter_table = ON')
    try:
        yield
    finally:
        op.execute('PRAGMA legacy_alter_table = OFF')


def _listed(values: tuple[str, ...]) -> str:
    return ', '.join(f'{v!r}' for v in values)
