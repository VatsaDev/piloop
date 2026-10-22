"""``limits``: live values for the ``Live`` keys of a provider's parameters, which the daemon applies on its next tick.

Revision ID: 0025
Revises: 0023
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

import piloop.db.models

revision: str = '0025'
down_revision: str | None = '0023'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        'limits',
        sa.Column('key', sa.String(), nullable=False),
        sa.Column('value', sa.JSON(), nullable=False),
        sa.Column('reason', sa.String(), nullable=False),
        sa.Column('actor', sa.String(), nullable=False),
        sa.Column('at', piloop.db.models.UTCDateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False),
        sa.CheckConstraint("typeof(key) IN ('text')", name='limits.key must be a text value'),
        sa.CheckConstraint("typeof(reason) IN ('text')", name='limits.reason must be a text value'),
        sa.CheckConstraint("typeof(actor) IN ('text')", name='limits.actor must be a text value'),
        sa.PrimaryKeyConstraint('key'),
    )


def downgrade() -> None:
    op.drop_table('limits')
