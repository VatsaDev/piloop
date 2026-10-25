"""A token's ``scope`` is its ``access``: ``read``, ``act``, ``admin`` or ``worker``, as the API takes tokens of people too.

No release made a ``lease`` token, which only a build of main could mint for a lease that is long gone: the upgrade
deletes them. The downgrade deletes every token that is not a worker's, which the older code cannot read.

Revision ID: 0051
Revises: 0050
"""

from collections.abc import Sequence

from alembic import op

revision: str = '0051'
down_revision: str | None = '0050'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
BEFORE = ('worker', 'lease')
AFTER = ('read', 'act', 'admin', 'worker')


def upgrade() -> None:
    op.execute(f'DELETE FROM tokens WHERE scope NOT IN ({_listed(AFTER)})')
    with op.batch_alter_table('tokens', recreate='always') as batch:
        batch.drop_constraint('tokenscope', type_='check')
        batch.alter_column('scope', new_column_name='access')
        batch.create_check_constraint('access', f'access IN ({_listed(AFTER)})')


def downgrade() -> None:
    op.execute(f'DELETE FROM tokens WHERE access NOT IN ({_listed(BEFORE)})')
    with op.batch_alter_table('tokens', recreate='always') as batch:
        batch.drop_constraint('access', type_='check')
        batch.alter_column('access', new_column_name='scope')
        batch.create_check_constraint('tokenscope', f'scope IN ({_listed(BEFORE)})')


def _listed(values: tuple[str, ...]) -> str:
    return ', '.join(f'{v!r}' for v in values)
