"""``lease_workers.cursor``: the event after which a worker reads its messages, which are events now.

A worker that exists already starts at the newest event: no message was an event before.

Revision ID: 0029
Revises: 0028
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0029'
down_revision: str | None = '0028'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column('lease_workers', sa.Column('cursor', sa.Integer(), server_default=sa.text('0'), nullable=False))
    op.execute('UPDATE lease_workers SET cursor = (SELECT coalesce(max(id), 0) FROM events)')


def downgrade() -> None:
    with op.batch_alter_table('lease_workers', schema=None) as batch_op:
        batch_op.drop_column('cursor')
