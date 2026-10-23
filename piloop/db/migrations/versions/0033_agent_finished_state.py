"""``agent_session.finished`` says the state of the closed row (``$.state``), not how the process ended (``$.status``).

A cancelled agent's process exits once the daemon killed it, so its event said ``exited`` and its row ``killed``. The
upgrade reads the state from the row when the process exited, and else writes ``killed``: a timeout, or no process.
The downgrade writes ``$.status`` back from the state, ``null`` for ``killed``.

Revision ID: 0033
Revises: 0032
"""

from collections.abc import Sequence

from alembic import op

revision: str = '0033'
down_revision: str | None = '0032'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE events SET payload = json_remove(json_set(payload, '$.state', CASE json_extract(payload, '$.status')
            WHEN 'exited' THEN coalesce(
                (SELECT state FROM agent_sessions WHERE id = events.target_id AND state IN ('exited', 'killed')),
                'exited'
            )
            ELSE 'killed' END), '$.status')
        WHERE kind = 'agent_session.finished'
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE events SET payload = json_remove(json_set(payload, '$.status', CASE json_extract(payload, '$.state')
            WHEN 'exited' THEN 'exited' ELSE NULL END), '$.state')
        WHERE kind = 'agent_session.finished'
        """
    )
