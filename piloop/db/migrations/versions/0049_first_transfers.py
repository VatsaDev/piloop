"""``first_transfers``: what a worker's empty clone of each repo of record starts from, built once, by its address.

The downgrade drops the table: an older server builds each worker's first transfer again, in its request.

Revision ID: 0049
Revises: 0048
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0049'
down_revision: str | None = '0048'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        'first_transfers',
        sa.Column('campaign_id', sa.Integer(), nullable=False),
        sa.Column('revision', sa.String(), nullable=False),
        sa.Column('blob', sa.String(), nullable=False),
        sa.CheckConstraint("typeof(revision) IN ('text')", name='first_transfers.revision must be a text value'),
        sa.CheckConstraint("typeof(blob) IN ('text')", name='first_transfers.blob must be a text value'),
        sa.ForeignKeyConstraint(['campaign_id'], ['campaigns.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('campaign_id'),
    )


def downgrade() -> None:
    op.drop_table('first_transfers')
