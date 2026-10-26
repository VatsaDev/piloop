"""The ``migrations`` table: one row for core and each plugin that migrated the database, with its package and its
fingerprint.

From here, core and each plugin (``piloop.db.schema``) migrate their own chains in their own version tables, and
record their fingerprints here. ``PRAGMA user_version`` held core's before.

Revision ID: 0060
Revises: 0059
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0060'
down_revision: str | None = '0059'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        'migrations',
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('package', sa.String(), nullable=False),
        sa.Column('fingerprint', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('name'),
    )


def downgrade() -> None:
    op.drop_table('migrations')
