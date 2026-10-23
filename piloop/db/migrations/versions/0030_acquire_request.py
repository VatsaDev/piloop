"""``leases.acquire_request`` and ``leases.report_request``: the request ids of the acquire and of the report.

An acquire sent again with its request id gets the lease that it took. ``report_request`` is ``request_id`` renamed,
now that a lease has two request ids.

Revision ID: 0030
Revises: 0029
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0030'
down_revision: str | None = '0029'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # native, not a batch rebuild: the copy would lose AUTOINCREMENT, and a fencing token must never be used again.
    # The rename keeps the CHECK's name, "leases.request_id must be a text value"; only a rebuild could change it.
    op.add_column('leases', sa.Column('acquire_request', sa.String(), nullable=True))
    op.create_index('ix_leases_acquire_request', 'leases', ['acquire_request'], unique=True)
    op.execute('ALTER TABLE leases RENAME COLUMN request_id TO report_request')


def downgrade() -> None:
    op.execute('ALTER TABLE leases RENAME COLUMN report_request TO request_id')
    op.drop_index('ix_leases_acquire_request', table_name='leases')
    op.execute('ALTER TABLE leases DROP COLUMN acquire_request')
