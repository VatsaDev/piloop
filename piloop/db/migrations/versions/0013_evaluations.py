"""``evaluations``: the evaluation queue, what the daemon evaluates now or next.

Revision ID: 0013
Revises: 0012
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

import piloop.db.models

revision: str = '0013'
down_revision: str | None = '0012'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        'evaluations',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('candidate_id', sa.Integer(), nullable=False),
        sa.Column('evaluator_id', sa.Integer(), nullable=False),
        sa.Column(
            'created_at', piloop.db.models.UTCDateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False
        ),
        sa.Column('started_at', piloop.db.models.UTCDateTime(), nullable=True),
        sa.Column('cancelled_by', sa.String(), nullable=True),
        sa.CheckConstraint(
            "typeof(cancelled_by) IN ('text', 'null')", name='evaluations.cancelled_by must be a text value'
        ),
        sa.ForeignKeyConstraint(['candidate_id'], ['candidates.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['evaluator_id'], ['evaluators.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('candidate_id', 'evaluator_id'),
        sqlite_autoincrement=True,
    )


def downgrade() -> None:
    op.drop_table('evaluations')
