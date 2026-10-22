"""``tokens``: the bearer tokens of the server that ``piloop serve --listen`` runs, each kept as its SHA-256 only.

Revision ID: 0022
Revises: 0021
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

import piloop.db.models

revision: str = '0022'
down_revision: str | None = '0021'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        'tokens',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column(
            'scope',
            sa.Enum('worker', 'lease', name='tokenscope', native_enum=False, length=16, create_constraint=True),
            nullable=False,
        ),
        sa.Column('sha256', sa.String(), nullable=False),
        sa.Column(
            'created_at', piloop.db.models.UTCDateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name'),
        sa.UniqueConstraint('sha256'),
    )


def downgrade() -> None:
    op.drop_table('tokens')
