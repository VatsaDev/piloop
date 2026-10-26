"""``journal.campaign_id``: each line of the orchestrator's journal belongs to a campaign.

The journal was home-wide. A line gets the only campaign of the home, else the campaign of the agent that wrote it
(``<role>#<agent_session_id>``). Any other line keeps NULL rather than a guess, which would show it in another
campaign's wake. ``piloop sql`` still reads it: no line is deleted. Batch mode rebuilds the table for the foreign key,
keeps the row ids, and drops the table's triggers, so ``recreate`` puts them back. The downgrade drops the column.

Revision ID: 0002
Revises: 0001
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

from piloop.db.triggers import recreate

revision: str = '0002'
down_revision: str | None = '0001'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table('journal', recreate='always') as batch:
        batch.add_column(
            sa.Column(
                'campaign_id', sa.Integer(), sa.ForeignKey('campaigns.id', name='journal_campaign'), nullable=True
            )
        )
    op.create_index('ix_journal_campaign', 'journal', ['campaign_id'])
    recreate(op, 'journal')
    bind = op.get_bind()
    match bind.execute(text('SELECT id FROM campaigns')).scalars().all():
        case [only]:
            bind.execute(text('UPDATE journal SET campaign_id = :c'), {'c': only})
        case _:
            bind.execute(
                text(
                    'UPDATE journal SET campaign_id = ('
                    "SELECT a.campaign_id FROM agent_sessions a WHERE journal.author = a.kind || '#' || a.id)"
                )
            )


def downgrade() -> None:
    op.drop_index('ix_journal_campaign', table_name='journal')
    with op.batch_alter_table('journal', recreate='always') as batch:
        batch.drop_column('campaign_id')
    recreate(op, 'journal')
