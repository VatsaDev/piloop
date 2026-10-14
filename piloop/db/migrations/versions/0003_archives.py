"""``archives``: what the daemon moved from a project's workspace into the artifact store.

Revision ID: 0003
Revises: 0002
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0003'
down_revision: str | None = '0002'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        'archives',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('project_id', sa.Integer(), nullable=False),
        sa.Column('idea_id', sa.Integer(), nullable=True),
        sa.Column('manifest', sa.String(), nullable=True),
        sa.Column('files', sa.Integer(), server_default=sa.text('0'), nullable=False),
        sa.Column('bytes', sa.Integer(), server_default=sa.text('0'), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False),
        sa.ForeignKeyConstraint(['project_id'], ['projects.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_archives_project_idea', 'archives', ['project_id', 'idea_id'])


def downgrade() -> None:
    op.drop_index('ix_archives_project_idea', table_name='archives')
    op.drop_table('archives')
