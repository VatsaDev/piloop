"""``workers.kind`` holds any role name: the CHECK of the closed enum becomes a text CHECK and the ``workers_kind`` trigger.

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

BUILT_IN = sa.Enum(
    'worker', 'watchdog', 'research', name='workerkind', native_enum=False, create_constraint=True, length=16
)
TEXT_CHECK = 'workers.kind must be a text value'


def upgrade() -> None:
    with op.batch_alter_table('workers', schema=None) as batch_op:
        batch_op.alter_column('kind', existing_type=BUILT_IN, type_=sa.String(), existing_nullable=False)
        batch_op.create_check_constraint(TEXT_CHECK, "typeof(kind) IN ('text')")
    recreate(op, 'workers')


def downgrade() -> None:
    op.execute("DELETE FROM workers WHERE kind NOT IN ('worker', 'watchdog', 'research')")  # the old enum has no others
    with op.batch_alter_table('workers', schema=None) as batch_op:
        batch_op.drop_constraint(TEXT_CHECK, type_='check')
        batch_op.alter_column('kind', existing_type=sa.String(), type_=BUILT_IN, existing_nullable=False)
    recreate(op, 'workers')
