"""Every role is a provider, so every role is in the stored config: the role trigger lists no built-in role.

``agent_sessions_kind`` refused a row whose kind was neither a built-in role nor a ``roles`` key of the stored config.
Each stored config already has the built-in roles as keys, so the trigger now reads the config alone. A row without a
campaign has no roles to check.

Revision ID: 0040
Revises: 0039
"""

from collections.abc import Sequence

from alembic import op

revision: str = '0040'
down_revision: str | None = '0039'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
NEW = """CREATE TRIGGER agent_sessions_kind BEFORE INSERT ON agent_sessions
WHEN NEW.campaign_id IS NOT NULL AND (SELECT json_type(config, '$.roles.' || json_quote(NEW.kind)) FROM campaigns WHERE id = NEW.campaign_id) IS NULL
BEGIN
  SELECT RAISE(ABORT, 'agent_sessions.kind must be a role of the campaign: a key of roles in its config');
END;"""
OLD = """CREATE TRIGGER agent_sessions_kind BEFORE INSERT ON agent_sessions
WHEN NEW.kind NOT IN ('implementer', 'watchdog', 'research', 'scout') AND (SELECT json_type(config, '$.roles.' || json_quote(NEW.kind)) FROM campaigns WHERE id = NEW.campaign_id) IS NULL
BEGIN
  SELECT RAISE(ABORT, 'agent_sessions.kind must be a role of the campaign: implementer, watchdog, research, scout or a [roles.<name>] table of its piloop.toml');
END;"""


def upgrade() -> None:
    op.execute('DROP TRIGGER agent_sessions_kind')
    op.execute(NEW)


def downgrade() -> None:
    op.execute('DROP TRIGGER agent_sessions_kind')
    op.execute(OLD)
