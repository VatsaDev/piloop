"""The ``secrets`` table of the ``local`` secrets provider: one value for each name of the home and of each campaign.

Revision ID: 0050
Revises: 0049
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0050'
down_revision: str | None = '0049'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        'secrets',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('campaign_id', sa.Integer(), nullable=True),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('value', sa.String(), nullable=False),
        sa.ForeignKeyConstraint(['campaign_id'], ['campaigns.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('campaign_id', 'name'),
    )
    op.create_index(
        'ux_secrets_home_name', 'secrets', ['name'], unique=True, sqlite_where=sa.text('campaign_id IS NULL')
    )


def downgrade() -> None:
    op.drop_index('ux_secrets_home_name', table_name='secrets')
    op.drop_table('secrets')
