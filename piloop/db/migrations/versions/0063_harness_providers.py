"""A ``[harness.<name>]`` table names no MCP renderer and no usage spec: its provider renders and reads.

The stored copy of each ``piloop.toml`` (``campaigns.config``) loses ``mcp`` and ``usage`` from every harness table. A
table with a new name gets ``provider``: its usage's ``tally``, else its ``mcp``, which named one of the built-in
harnesses. The downgrade turns ``provider`` back into ``mcp``: the older piloop then reads no usage for that harness.

Revision ID: 0063
Revises: 0062
"""

import json
from collections.abc import Callable, Sequence

from alembic import op
from sqlalchemy import text

revision: str = '0063'
down_revision: str | None = '0062'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
BUILT_IN = ('pi', 'claude', 'codex')


def upgrade() -> None:
    _rewrite(upgraded)


def downgrade() -> None:
    _rewrite(downgraded)


def _rewrite(change: Callable[[dict[str, object]], dict[str, object]]) -> None:
    bind = op.get_bind()
    for campaign_id, config in bind.execute(text('SELECT id, config FROM campaigns')).all():
        bind.execute(
            text('UPDATE campaigns SET config = :c WHERE id = :id'),
            {'c': json.dumps(change(json.loads(config))), 'id': campaign_id},
        )


def _tables(config: dict[str, object]) -> list[tuple[str, dict[str, object]]]:
    tables = config.get('harness')
    return [(n, t) for n, t in tables.items() if isinstance(t, dict)] if isinstance(tables, dict) else []


def upgraded(config: dict[str, object]) -> dict[str, object]:
    """Return a stored config in the shape of this revision."""
    for name, table in _tables(config):
        mcp, usage = table.pop('mcp', None), table.pop('usage', None)
        tally = usage.get('tally') if isinstance(usage, dict) else None
        if name not in BUILT_IN and (named := tally or mcp) is not None:
            table['provider'] = named
    return config


def downgraded(config: dict[str, object]) -> dict[str, object]:
    """Return a stored config in the shape of the revision before."""
    for _, table in _tables(config):
        if (named := table.pop('provider', None)) is not None:
            table['mcp'] = named
    return config
