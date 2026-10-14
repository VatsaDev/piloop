"""The scout is a role of its own, and the stored configs take the keys of this release.

A scout sweep was a research row with a ``scout.requested`` event. It is now a ``workers`` row of kind ``scout``, which
the ``workers_kind`` trigger admits as a built-in role. The stored copy of each ``piloop.toml`` (``projects.config``)
is rewritten as ``piloop.deprecated`` rewrites the file, so the daemon reads it before the next ``piloop init``:

- ``roles.research.scout`` becomes the ``roles.scout`` table, with the keys of research that the scout used, and the
  ``scout`` trigger leaves research's triggers.
- ``roles.research.buffer`` becomes the ``buffer`` of research's ``low_water`` triggers.
- ``roles.research.context`` becomes the top-level ``context``.
- ``{claude_config_dir}`` and ``{codex_home}`` in a usage glob become ``{env:NAME:default}`` placeholders.
- A watchdog's stored triggers get ``stopped``: the closing review that every campaign had before it was a trigger.

The downgrade turns the scout rows back into research rows with their event, and restores the trigger. It leaves the
stored configs: an older piloop reads them after its own ``piloop init``, as after any other downgrade.

Revision ID: 0011
Revises: 0010
"""

import json
from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

from piloop.db.triggers import recreate

revision: str = '0011'
down_revision: str | None = '0010'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
SCOUT_ROWS = "SELECT target_id FROM events WHERE kind = 'scout.requested' AND target_kind = 'worker'"
SCOUT_SHARED = ('runtime', 'harnesses', 'model', 'timeout', 'mcp', 'addendum', 'workdir')
RESEARCH_TRIGGERS = ({'kind': 'low_water'}, {'kind': 'seed'})
PLACEHOLDERS = {'{claude_config_dir}': '{env:CLAUDE_CONFIG_DIR:~/.claude}', '{codex_home}': '{env:CODEX_HOME:~/.codex}'}
DEFAULT_BUFFER = 2
OLD_KIND_TRIGGER = """CREATE TRIGGER workers_kind BEFORE INSERT ON workers
WHEN NEW.kind NOT IN ('worker', 'watchdog', 'research') AND (SELECT json_type(config, '$.roles.' || json_quote(NEW.kind)) FROM projects WHERE id = NEW.project_id) IS NULL
BEGIN
  SELECT RAISE(ABORT, 'workers.kind must be a role of the project: worker, watchdog, research or a [roles.<name>] table of its piloop.toml');
END;"""


def upgrade() -> None:
    bind = op.get_bind()
    for project_id, config in bind.execute(text('SELECT id, config FROM projects')).all():
        stored = upgraded(json.loads(config))
        bind.execute(
            text('UPDATE projects SET config = :c WHERE id = :id'), {'c': json.dumps(stored), 'id': project_id}
        )
    op.execute(f"UPDATE workers SET kind = 'scout' WHERE kind = 'research' AND id IN ({SCOUT_ROWS})")
    op.execute('DROP TRIGGER workers_kind')
    op.execute('DROP TRIGGER workers_updated_at')
    recreate(op, 'workers')


def downgrade() -> None:
    op.execute(
        "INSERT INTO events (kind, actor, target_kind, target_id, payload) SELECT 'scout.requested', 'daemon', 'worker',"
        f" id, '{{}}' FROM workers WHERE kind = 'scout' AND id NOT IN ({SCOUT_ROWS})"
    )
    op.execute("UPDATE workers SET kind = 'research' WHERE kind = 'scout'")
    op.execute('DROP TRIGGER workers_kind')
    op.execute(OLD_KIND_TRIGGER)


def upgraded(config: dict[str, object]) -> dict[str, object]:
    """Return a stored config in the shape of this revision. A config that has no such key comes back unchanged."""
    roles = config.get('roles')
    research = roles.get('research') if isinstance(roles, dict) else None
    if isinstance(roles, dict) and isinstance(research, dict):
        scout = research.pop('scout', None)
        if isinstance(scout, dict):
            roles['scout'] = {k: research[k] for k in SCOUT_SHARED if k in research} | scout
        if 'context' in research:
            config['context'] = research.pop('context')
        buffer = research.pop('buffer', DEFAULT_BUFFER)
        triggers = research.get('triggers')
        if isinstance(triggers, list) or buffer != DEFAULT_BUFFER:
            kept = [t for t in (triggers if isinstance(triggers, list) else RESEARCH_TRIGGERS) if not _kind(t, 'scout')]
            research['triggers'] = [t | {'buffer': buffer} if _kind(t, 'low_water') else t for t in kept]
    watchdog = roles.get('watchdog') if isinstance(roles, dict) else None
    triggers = watchdog.get('triggers') if isinstance(watchdog, dict) else None
    if isinstance(watchdog, dict) and isinstance(triggers, list) and not any(_kind(t, 'stopped') for t in triggers):
        watchdog['triggers'] = [*triggers, {'kind': 'stopped'}]
    harnesses = config.get('harness')
    for harness in harnesses.values() if isinstance(harnesses, dict) else ():
        usage = harness.get('usage') if isinstance(harness, dict) else None
        if isinstance(usage, dict) and isinstance(glob := usage.get('file'), str):
            for old, new in PLACEHOLDERS.items():
                glob = glob.replace(old, new)
            usage['file'] = glob
    return config


def _kind(trigger: object, kind: str) -> bool:
    return isinstance(trigger, dict) and trigger.get('kind') == kind
