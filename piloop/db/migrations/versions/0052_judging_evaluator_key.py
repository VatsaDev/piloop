"""An evaluator's key covers its judging metrics only: the current evaluator of each campaign takes its new key.

A watch metric is no longer part of an evaluator (``CampaignConfig.evaluator_definition``). Each campaign's current
evaluator gets the definition and key that its stored config has under that rule, so the next ``piloop init`` of an
unchanged ``piloop.toml`` finds it, and nothing is measured again. An older evaluator keeps its key: its roles are not
stored, so its judging metrics are unknown. The new definition names them ``judging``, not ``metrics``, so no old key
can equal a new one. The downgrade gives each current evaluator its key under the old rule again, unless another row
of its campaign has that key.

Revision ID: 0052
Revises: 0051
"""

import hashlib
import json
from collections.abc import Callable, Sequence

from alembic import op
from sqlalchemy import text

revision: str = '0052'
down_revision: str | None = '0051'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
WATCH = 'watch'


def upgrade() -> None:
    _rekey(judging)


def downgrade() -> None:
    _rekey(every_metric)


def judging(config: dict[str, object]) -> dict[str, object]:
    """The definition of this revision: the evaluate command, the links, and the primaries and guardrails by name."""
    metrics = [m for m in _metrics(config) if m.get('role', WATCH) != WATCH]
    return {
        'evaluate': config['evaluate'],
        'link': config.get('link', []),
        'judging': [{'name': m['name'], 'direction': m['direction']} for m in sorted(metrics, key=_name)],
    }


def every_metric(config: dict[str, object]) -> dict[str, object]:
    """The definition of the revision before: the evaluate command, the links, and every metric in config order."""
    return {
        'evaluate': config['evaluate'],
        'link': config.get('link', []),
        'metrics': [{'name': m['name'], 'direction': m['direction']} for m in _metrics(config)],
    }


def _metrics(config: dict[str, object]) -> list[dict[str, object]]:
    metrics = config.get('metrics', [])
    return [m for m in metrics if isinstance(m, dict)] if isinstance(metrics, list) else []


def _name(metric: dict[str, object]) -> str:
    return str(metric['name'])


def _rekey(define: Callable[[dict[str, object]], dict[str, object]]) -> None:
    bind = op.get_bind()
    current = text('SELECT e.id, e.campaign_id, c.config FROM evaluators e JOIN campaigns c ON c.evaluator_id = e.id')
    for evaluator_id, campaign_id, stored in bind.execute(current).all():
        config = json.loads(stored)
        if 'evaluate' not in config:  # no config that piloop loads lacks it: such a campaign never measures again
            continue
        definition = define(config)
        key = hashlib.sha256(json.dumps(definition, sort_keys=True).encode()).hexdigest()[:16]
        bind.execute(
            text(
                'UPDATE evaluators SET key = :key, definition = :definition WHERE id = :id AND NOT EXISTS'
                ' (SELECT 1 FROM evaluators o WHERE o.campaign_id = :campaign AND o.key = :key AND o.id != :id)'
            ),
            {'key': key, 'definition': json.dumps(definition), 'id': evaluator_id, 'campaign': campaign_id},
        )
