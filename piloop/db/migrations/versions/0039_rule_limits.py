"""``[stop]``, ``[spend] limit`` and ``confidence`` are parameters of the stop rule and of the verdict rule.

The stored copy of each ``piloop.toml`` (``campaigns.config``) is rewritten as ``piloop.deprecated`` rewrites the file,
so the daemon reads it before the next ``piloop init``:

- each key of ``stop`` that is set becomes a key of ``plugins.stop_rule``;
- ``spend.limit`` becomes ``plugins.stop_rule.max_spend``;
- a ``confidence`` other than the default becomes ``plugins.verdict_rule.confidence``.

A key that the rule's table sets keeps its value there. The downgrade moves the keys back.

Revision ID: 0039
Revises: 0038
"""

import json
from collections.abc import Callable, Sequence

from alembic import op
from sqlalchemy import text

revision: str = '0039'
down_revision: str | None = '0038'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
STOP_KEYS = ('target', 'max_runs', 'max_hours', 'max_tokens', 'plateau')
CONFIDENCE = 0.95


def upgrade() -> None:
    _rewrite(upgraded)


def downgrade() -> None:
    _rewrite(downgraded)


def _rewrite(change: Callable[[dict[str, object]], dict[str, object]]) -> None:
    bind = op.get_bind()
    for campaign_id, config in bind.execute(text('SELECT id, config FROM campaigns')).all():
        stored = change(json.loads(config))
        bind.execute(
            text('UPDATE campaigns SET config = :c WHERE id = :id'), {'c': json.dumps(stored), 'id': campaign_id}
        )


def upgraded(config: dict[str, object]) -> dict[str, object]:
    """Return a stored config in the shape of this revision."""
    stop = config.pop('stop', None)
    limits = {k: v for k, v in stop.items() if v is not None} if isinstance(stop, dict) else {}
    spend = config.get('spend')
    if isinstance(spend, dict) and 'limit' in spend:
        limits['max_spend'] = spend.pop('limit')
    confidence = config.pop('confidence', CONFIDENCE)
    _into(config, 'stop_rule', limits)
    _into(config, 'verdict_rule', {'confidence': confidence} if confidence != CONFIDENCE else {})
    return config


def downgraded(config: dict[str, object]) -> dict[str, object]:
    """Return a stored config in the shape of the revision before."""
    plugins = config.get('plugins')
    rule = plugins.get('stop_rule') if isinstance(plugins, dict) else None
    verdict = plugins.get('verdict_rule') if isinstance(plugins, dict) else None
    stop = {k: rule.pop(k) for k in STOP_KEYS if k in rule} if isinstance(rule, dict) else {}
    config['stop'] = stop
    spend = config.get('spend')
    if isinstance(rule, dict) and 'max_spend' in rule:
        limit = rule.pop('max_spend')
        if isinstance(spend, dict):
            spend['limit'] = limit
    if isinstance(verdict, dict) and 'confidence' in verdict:
        config['confidence'] = verdict.pop('confidence')
    return config


def _into(config: dict[str, object], rule: str, keys: dict[str, object]) -> None:
    if not keys:
        return
    plugins = config.setdefault('plugins', {})
    table = plugins.setdefault(rule, {}) if isinstance(plugins, dict) else None
    if isinstance(table, dict):
        for key, value in keys.items():
            table.setdefault(key, value)
