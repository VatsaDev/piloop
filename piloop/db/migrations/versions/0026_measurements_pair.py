"""``measurements.pair_id``: the bar's measurement that ran next to a candidate's in a paired evaluation.

A batch rebuild adds it with its foreign key, which drops the table's triggers, so ``recreate`` puts them back.

Revision ID: 0026
Revises: 0025
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from piloop.db.triggers import recreate

revision: str = '0026'
down_revision: str | None = '0025'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table('measurements', schema=None, recreate='always') as batch_op:
        pair = sa.ForeignKey('measurements.id', ondelete='SET NULL', name='measurements_pair')
        batch_op.add_column(sa.Column('pair_id', sa.Integer(), pair, nullable=True))
    recreate(op, 'measurements')


def downgrade() -> None:
    with op.batch_alter_table('measurements', schema=None, recreate='always') as batch_op:
        batch_op.drop_column('pair_id')
    recreate(op, 'measurements')
