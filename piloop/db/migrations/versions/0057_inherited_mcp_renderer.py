"""A built-in harness's table that sets ``mcp`` to null, and inherits an argv with ``{mcp}``, inherits the renderer too.

piloop 0.2 stored a ``[harness.<built-in>]`` table with every key, ``mcp = null`` among them. A table now keeps the
built-in's keys that it does not set, so one without its own ``resume`` gets the built-in's, whose ``{mcp}`` needs a
renderer, and the stored config no longer validates. The stored copy of each ``piloop.toml`` (``campaigns.config``)
loses such an ``mcp``: the table inherits the built-in's renderer with its argv.

The downgrade leaves the stored configs: the older piloop reads them as before.

Revision ID: 0057
Revises: 0056
"""

import json
from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

revision: str = '0057'
down_revision: str | None = '0056'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
WITH_MCP = {name: ('headless', 'resume') for name in ('claude', 'codex', 'pi')}
"""The argv keys of each built-in harness that hold ``{mcp}``."""


def upgrade() -> None:
    bind = op.get_bind()
    for campaign_id, config in bind.execute(text('SELECT id, config FROM campaigns')).all():
        stored = upgraded(json.loads(config))
        bind.execute(
            text('UPDATE campaigns SET config = :c WHERE id = :id'), {'c': json.dumps(stored), 'id': campaign_id}
        )


def downgrade() -> None:
    pass


def upgraded(config: object) -> object:
    """Return a stored config in the shape of this revision."""
    tables = config.get('harness') if isinstance(config, dict) else None
    for name, table in tables.items() if isinstance(tables, dict) else ():
        inherits = isinstance(table, dict) and any(key not in table for key in WITH_MCP.get(name, ()))
        if inherits and 'mcp' in table and table['mcp'] is None:
            del table['mcp']
    return config
