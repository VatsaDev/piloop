"""``runs.scoring``: the daemon scores the commits of the run now.

It replaces a guess: a finished run whose idea is still ``scheduled`` and has no later run. A human who parks or rejects
the idea while its commits are scored broke that guess, and the run lost its place under ``[stop] max_runs``.

The upgrade sets ``scoring`` on the runs that the guess selects, so a daemon that stopped while it scored them resumes
them. Batch mode rebuilds the table to add the CHECK, which drops its triggers, so ``recreate`` puts them back.

Revision ID: 0010
Revises: 0009
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from piloop.db.triggers import recreate

revision: str = '0010'
down_revision: str | None = '0009'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
FINISHED = "('succeeded', 'failed', 'timed_out')"
CHECK = 'runs.scoring needs a finished status'


def upgrade() -> None:
    with op.batch_alter_table('runs', schema=None, recreate='always') as batch_op:
        batch_op.add_column(sa.Column('scoring', sa.Boolean(), server_default=sa.text('0'), nullable=False))
        batch_op.create_check_constraint(CHECK, f'scoring = 0 OR status IN {FINISHED}')
    recreate(op, 'runs')
    op.execute(
        f"""UPDATE runs SET scoring = 1
        WHERE status IN {FINISHED}
          AND idea_id IN (SELECT id FROM ideas WHERE status = 'scheduled')
          AND NOT EXISTS (SELECT 1 FROM runs AS later WHERE later.idea_id = runs.idea_id AND later.id > runs.id)"""
    )


def downgrade() -> None:
    with op.batch_alter_table('runs', schema=None, recreate='always') as batch_op:
        batch_op.drop_constraint(CHECK, type_='check')
        batch_op.drop_column('scoring')
    recreate(op, 'runs')
