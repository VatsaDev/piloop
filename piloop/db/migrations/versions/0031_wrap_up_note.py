"""``agent_session.wrap_up`` names the note that carries the prompt (``$.comment``), and has no ``$.error``.

The daemon leaves a wrap-up as a note on the agent, which its executor delivers: a prompt that does not reach the agent
is that note's ``comment.undelivered``. The upgrade drops the ``$.error`` of the older rows. The downgrade drops
``$.comment``, which the code before does not read.

Revision ID: 0031
Revises: 0030
"""

from collections.abc import Sequence

from alembic import op

revision: str = '0031'
down_revision: str | None = '0030'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("UPDATE events SET payload = json_remove(payload, '$.error') WHERE kind = 'agent_session.wrap_up'")


def downgrade() -> None:
    op.execute("UPDATE events SET payload = json_remove(payload, '$.comment') WHERE kind = 'agent_session.wrap_up'")
