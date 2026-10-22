"""``lease_workers``: each worker's pool and last host sample. Being unreachable is a worker's state, not a lease's.

Revision ID: 0023
Revises: 0022
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

import piloop.db.models

revision: str = '0023'
down_revision: str | None = '0022'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        'lease_workers',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('pool', sa.String(), nullable=False),
        sa.Column('version', sa.String(), nullable=False),
        sa.Column('runners', sa.JSON(), server_default=sa.text("'[]'"), nullable=False),
        sa.Column('sample', sa.JSON(), server_default=sa.text("'{}'"), nullable=False),
        sa.Column('seen_at', piloop.db.models.UTCDateTime(), nullable=False),
        sa.Column(
            'no_lease',
            sa.Enum(
                'memory',
                'load',
                'slots',
                'nothing',
                'unreachable',
                name='nolease',
                native_enum=False,
                length=16,
                create_constraint=True,
            ),
            nullable=True,
        ),
        sa.CheckConstraint("typeof(id) IN ('text')", name='lease_workers.id must be a text value'),
        sa.CheckConstraint("typeof(pool) IN ('text')", name='lease_workers.pool must be a text value'),
        sa.CheckConstraint("typeof(version) IN ('text')", name='lease_workers.version must be a text value'),
        sa.PrimaryKeyConstraint('id'),
    )
    # native, not a batch rebuild: the copy would lose AUTOINCREMENT, and a fencing token must never be used again
    op.execute('ALTER TABLE leases DROP COLUMN unreachable_at')


def downgrade() -> None:
    op.add_column('leases', sa.Column('unreachable_at', piloop.db.models.UTCDateTime(), nullable=True))
    op.drop_table('lease_workers')
