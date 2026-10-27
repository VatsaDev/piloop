"""The harnesses and the roles are instances of their capabilities: ``plugins.harness.<name>`` and ``plugins.role.<name>``.

The stored copy of each ``piloop.toml`` (``campaigns.config``) moves its keys as ``piloop.deprecated`` moves the
file's: ``harness`` to ``plugins.harness``, each harness's entry of ``model`` and ``max_concurrent`` into its table, and
``roles`` to ``plugins.role``, and ``harnesses`` and ``runtime`` to the ``use`` of ``plugins.harness`` and
``plugins.runtime``. ``agent_sessions_kind`` reads the roles at their new key. The downgrade moves them back.

Revision ID: 0065
Revises: 0064
"""

import json
from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

from piloop.deprecated import Move

revision: str = '0065'
down_revision: str | None = '0064'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
MOVES = (
    Move('harness.*', 'plugins.harness.*'),
    Move('model.*', 'plugins.harness.*.model'),
    Move('max_concurrent.*', 'plugins.harness.*.max_concurrent'),
    Move('roles.*', 'plugins.role.*'),
    Move('harnesses', 'plugins.harness.use'),
    Move('runtime', 'plugins.runtime.use'),
)
NEW = """CREATE TRIGGER agent_sessions_kind BEFORE INSERT ON agent_sessions
WHEN NEW.campaign_id IS NOT NULL AND (SELECT json_type(config, '$.plugins.role.' || json_quote(NEW.kind)) FROM campaigns WHERE id = NEW.campaign_id) IS NULL
BEGIN
  SELECT RAISE(ABORT, 'agent_sessions.kind must be a role of the campaign: a key of plugins.role in its config');
END;"""
OLD = """CREATE TRIGGER agent_sessions_kind BEFORE INSERT ON agent_sessions
WHEN NEW.campaign_id IS NOT NULL AND (SELECT json_type(config, '$.roles.' || json_quote(NEW.kind)) FROM campaigns WHERE id = NEW.campaign_id) IS NULL
BEGIN
  SELECT RAISE(ABORT, 'agent_sessions.kind must be a role of the campaign: a key of roles in its config');
END;"""


def upgrade() -> None:
    _rewrite(MOVES)
    op.execute('DROP TRIGGER agent_sessions_kind')
    op.execute(NEW)


def downgrade() -> None:
    _rewrite(tuple(m.inverse() for m in reversed(MOVES)))
    op.execute('DROP TRIGGER agent_sessions_kind')
    op.execute(OLD)


def _rewrite(moves: Sequence[Move]) -> None:
    bind = op.get_bind()
    for campaign_id, config in bind.execute(text('SELECT id, config FROM campaigns')).all():
        bind.execute(
            text('UPDATE campaigns SET config = :c WHERE id = :id'),
            {'c': json.dumps(moved(json.loads(config), moves)), 'id': campaign_id},
        )


def moved(config: dict[str, object], moves: Sequence[Move] = MOVES) -> dict[str, object]:
    """Return a stored config with ``moves`` applied: by default, in the shape of this revision."""
    for move in moves:
        move.apply(config)
    return config
