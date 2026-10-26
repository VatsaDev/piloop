"""``tokens.agent_session_id``: the agent session that a token was minted for when its agent launched.

The tokens before it are people's and workers' and keep NULL. The downgrade deletes the agents' tokens, which the older
code would take for people's. No foreign key (``Token.agent_session_id``).

Revision ID: 0062
Revises: 0061
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0062'
down_revision: str | None = '0061'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table('tokens', recreate='always') as batch:
        batch.add_column(sa.Column('agent_session_id', sa.Integer(), nullable=True))
        batch.create_unique_constraint('tokens_agent_session_unique', ['agent_session_id'])


def downgrade() -> None:
    op.execute('DELETE FROM tokens WHERE agent_session_id IS NOT NULL')
    with op.batch_alter_table('tokens', recreate='always') as batch:
        batch.drop_constraint('tokens_agent_session_unique', type_='unique')
        batch.drop_column('agent_session_id')
