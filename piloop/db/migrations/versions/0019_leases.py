"""``leases``: every agent and every evaluation runs on a lease, a time-limited claim of one worker on one job.

Revision ID: 0019
Revises: 0018
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

import piloop.db.models

revision: str = '0019'
down_revision: str | None = '0018'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        'leases',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column(
            'kind',
            sa.Enum('agent', 'evaluation', name='leasekind', native_enum=False, length=16, create_constraint=True),
            nullable=False,
        ),
        sa.Column('agent_session_id', sa.Integer(), nullable=True),
        sa.Column('evaluation_id', sa.Integer(), nullable=True),
        sa.Column(
            'state',
            sa.Enum(
                'queued',
                'acquired',
                'running',
                'reported',
                'expired',
                'cancelled',
                name='leasestate',
                native_enum=False,
                length=16,
                create_constraint=True,
            ),
            server_default='queued',
            nullable=False,
        ),
        sa.Column('worker', sa.String(), nullable=True),
        sa.Column('job', sa.JSON(), server_default=sa.text("'{}'"), nullable=False),
        sa.Column('handle', sa.JSON(), server_default=sa.text("'{}'"), nullable=False),
        sa.Column(
            'created_at', piloop.db.models.UTCDateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False
        ),
        sa.Column('acquired_at', piloop.db.models.UTCDateTime(), nullable=True),
        sa.Column('started_at', piloop.db.models.UTCDateTime(), nullable=True),
        sa.Column('renewed_at', piloop.db.models.UTCDateTime(), nullable=True),
        sa.Column('expires_at', piloop.db.models.UTCDateTime(), nullable=True),
        sa.Column('unreachable_at', piloop.db.models.UTCDateTime(), nullable=True),
        sa.Column('revoked_at', piloop.db.models.UTCDateTime(), nullable=True),
        sa.Column('ended_at', piloop.db.models.UTCDateTime(), nullable=True),
        sa.Column('request_id', sa.String(), nullable=True),
        sa.Column('receipt', sa.JSON(), server_default=sa.text("'{}'"), nullable=False),
        sa.CheckConstraint(
            "(kind = 'agent') = (agent_session_id IS NOT NULL) AND (kind = 'evaluation') = (evaluation_id IS NOT NULL)",
            name='leases.a lease has the job of its kind',
        ),
        sa.CheckConstraint("typeof(worker) IN ('text', 'null')", name='leases.worker must be a text value'),
        sa.CheckConstraint("typeof(request_id) IN ('text', 'null')", name='leases.request_id must be a text value'),
        sa.ForeignKeyConstraint(['agent_session_id'], ['agent_sessions.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sqlite_autoincrement=True,
    )
    op.create_index('ix_leases_agent_session', 'leases', ['agent_session_id'])
    op.create_index('ix_leases_evaluation', 'leases', ['evaluation_id'])
    op.create_index('ix_leases_state', 'leases', ['state'])


def downgrade() -> None:
    op.drop_table('leases')
