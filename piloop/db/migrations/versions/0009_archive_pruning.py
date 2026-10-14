"""``archives.pruned_at``: the workspace copies of an archiving stay until the daemon prunes them.

An archiving used to remove its local copies at once, so every row that exists already was pruned when it was made:
``pruned_at`` is its ``created_at``. From now on the daemon prunes by age or when the disk is short, and sets it then.

Revision ID: 0009
Revises: 0008
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

import piloop.db.models

revision: str = '0009'
down_revision: str | None = '0008'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column('archives', sa.Column('pruned_at', piloop.db.models.UTCDateTime(), nullable=True))
    op.execute('UPDATE archives SET pruned_at = created_at')


def downgrade() -> None:
    with op.batch_alter_table('archives', schema=None) as batch_op:
        batch_op.drop_column('pruned_at')
