"""``measurements.rung`` and ``stopped``: the rung of the evaluation ladder that a measurement measured, and why its
climb stopped there. Every measurement before them measured the full panel: NULL.

Batch mode rebuilds the table for the CHECKs. It keeps the row ids and drops the table's triggers, so ``recreate`` puts
them back.

Revision ID: 0002
Revises: 0001
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from piloop.db.triggers import recreate

revision: str = '0002'
down_revision: str | None = '0001'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

STOP = 'measurements.a stop is at a rung, with no error'
TEXT = {column: f'measurements.{column} must be a text value' for column in ('rung', 'stopped')}


def upgrade() -> None:
    with op.batch_alter_table('measurements', recreate='always') as batch:
        batch.add_column(sa.Column('rung', sa.String(), nullable=True))
        batch.add_column(sa.Column('stopped', sa.String(), nullable=True))
        batch.create_check_constraint(STOP, 'stopped IS NULL OR (rung IS NOT NULL AND error IS NULL)')
        for column, name in TEXT.items():
            batch.create_check_constraint(name, f"typeof({column}) IN ('text', 'null')")
    recreate(op, 'measurements')


def downgrade() -> None:
    with op.batch_alter_table('measurements', recreate='always') as batch:
        for name in (STOP, *TEXT.values()):
            batch.drop_constraint(name, type_='check')
        batch.drop_column('stopped')
        batch.drop_column('rung')
    recreate(op, 'measurements')
