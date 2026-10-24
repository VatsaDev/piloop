"""An event that a trigger writes has milliseconds in its time, as ``updated_at`` has.

The persistent trigger of ``comment.escalation`` is created again with the time. The TEMP triggers of the change log
come from ``actor_triggers.sql``, which every connection runs. The downgrade creates the trigger as it was.

Revision ID: 0042
Revises: 0041
"""

from collections.abc import Sequence

from alembic import op

from piloop.db.triggers import install

revision: str = '0042'
down_revision: str | None = '0041'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

ESCALATION = 'comments_escalation_event'
# Frozen here, as of revision 0041.
OLD_ESCALATION = (
    "CREATE TRIGGER comments_escalation_event AFTER INSERT ON comments\nWHEN NEW.kind = 'escalation'\nBEGIN\n"
    '  INSERT INTO events (actor, target_kind, target_id, payload)\n'
    "  VALUES (NEW.author, 'comment', NEW.id, json_object('kind', 'comment.' || NEW.kind, 'target_kind',"
    " NEW.target_kind, 'target_id', NEW.target_id));\nEND;"
)


def upgrade() -> None:
    op.execute(f'DROP TRIGGER IF EXISTS {ESCALATION}')
    install(op, [ESCALATION])


def downgrade() -> None:
    op.execute(f'DROP TRIGGER IF EXISTS {ESCALATION}')
    op.execute(OLD_ESCALATION)
