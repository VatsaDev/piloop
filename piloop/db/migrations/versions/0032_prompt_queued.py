"""``comment.delivered`` means that the agent read the prompt. What the older rows recorded is ``comment.queued``.

Before, the daemon recorded ``comment.delivered`` once the agent's runtime held the prompt, whether or not the agent
read it. The upgrade renames those rows. The downgrade renames ``comment.queued`` back; a ``comment.delivered`` of an
agent that read the prompt stays, which the code before reads as the same outcome.

Revision ID: 0032
Revises: 0031
"""

from collections.abc import Sequence

from alembic import op

revision: str = '0032'
down_revision: str | None = '0031'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "UPDATE events SET payload = json_set(payload, '$.kind', 'comment.queued') WHERE kind = 'comment.delivered'"
    )


def downgrade() -> None:
    op.execute(
        "UPDATE events SET payload = json_set(payload, '$.kind', 'comment.delivered') WHERE kind = 'comment.queued'"
    )
