"""The spend is a fact that a source reads: ``[spend]`` is a ``command`` source of ``[[facts]]``.

The stored copy of each ``piloop.toml`` (``campaigns.config``) is rewritten as ``piloop.deprecated`` rewrites the file:

- ``spend`` becomes a ``command`` source of ``facts`` that reads ``spend``, after ``campaign`` and ``optimization``, its command wrapped so
  that it prints ``METRIC spend=<number>``;
- its ``closing_review`` becomes ``plugins.stop_rule.while_held``;
- ``plugins.stop_rule.max_spend`` becomes ``hold_at = {"spend": <value>}``.

The latest reading of each campaign (``campaigns.spend_usd``, ``spend_error``, ``spend_at``) becomes ``fact.read`` events
at its time: the spend that it read, then its error if it failed since. The columns go. The downgrade adds them back
from the latest events, and moves the keys back.

Revision ID: 0056
Revises: 0055
"""

import json
from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

revision: str = '0056'
down_revision: str | None = '0055'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
READ = 'fact.read'
SPEND = 'spend'
WRAPPER = ['sh', '-c', 'out=$("$@") && printf "METRIC spend=%s\\n" "$out"', 'sh']


def upgrade() -> None:
    bind = op.get_bind()
    for campaign_id, config in bind.execute(text('SELECT id, config FROM campaigns')).all():
        bind.execute(
            text('UPDATE campaigns SET config = :c WHERE id = :id'),
            {'c': json.dumps(upgraded(json.loads(config))), 'id': campaign_id},
        )
    readings = 'SELECT id, spend_usd, spend_error, spend_at FROM campaigns WHERE spend_at IS NOT NULL'
    for campaign_id, usd, error, at in bind.execute(text(readings)).all():
        payloads = [{'kind': READ, 'values': {SPEND: usd}, 'error': None}] if usd is not None else []
        if error is not None:
            payloads.append({'kind': READ, 'values': {SPEND: None}, 'error': error})
        for payload in payloads:
            bind.execute(
                text(
                    'INSERT INTO events (at, actor, target_kind, target_id, payload)'
                    " VALUES (:at, 'daemon', 'campaign', :id, :payload)"
                ),
                {'at': at, 'id': campaign_id, 'payload': json.dumps(payload)},
            )
    for column in ('spend_at', 'spend_error', 'spend_usd'):
        op.execute(f'ALTER TABLE campaigns DROP COLUMN {column}')


def downgrade() -> None:
    op.execute('ALTER TABLE campaigns ADD COLUMN spend_usd FLOAT')
    op.execute('ALTER TABLE campaigns ADD COLUMN spend_error VARCHAR')
    op.execute('ALTER TABLE campaigns ADD COLUMN spend_at DATETIME')
    bind = op.get_bind()
    reads = (
        f"SELECT target_id, at, json_extract(payload, '$.values.{SPEND}'), json_extract(payload, '$.error')"
        f" FROM events WHERE kind = '{READ}' AND target_kind = 'campaign'"
        f" AND json_type(payload, '$.values.{SPEND}') IS NOT NULL ORDER BY id"
    )
    latest: dict[int, tuple[str, float | None, str | None]] = {}
    for campaign_id, at, usd, error in bind.execute(text(reads)).all():
        before = latest.get(campaign_id)
        latest[campaign_id] = (at, usd if usd is not None else (before[1] if before is not None else None), error)
    for campaign_id, (at, usd, error) in latest.items():
        bind.execute(
            text('UPDATE campaigns SET spend_usd = :usd, spend_error = :error, spend_at = :at WHERE id = :id'),
            {'usd': usd, 'error': error, 'at': at, 'id': campaign_id},
        )
    bind.execute(text(f"DELETE FROM events WHERE kind = '{READ}'"))
    for campaign_id, config in bind.execute(text('SELECT id, config FROM campaigns')).all():
        bind.execute(
            text('UPDATE campaigns SET config = :c WHERE id = :id'),
            {'c': json.dumps(downgraded(json.loads(config))), 'id': campaign_id},
        )


def upgraded(config: dict[str, object]) -> dict[str, object]:
    """Return a stored config in the shape of this revision."""
    spend = config.pop('spend', None)
    plugins = config.get('plugins')
    rule = plugins.get('stop_rule') if isinstance(plugins, dict) else None
    if isinstance(rule, dict) and 'max_spend' in rule:
        rule.setdefault('hold_at', {SPEND: rule.pop('max_spend')})
    if not isinstance(spend, dict):
        return config
    source: dict[str, object] = {'provider': 'command', 'command': [*WRAPPER, *spend.get('command', [])]}
    source['metrics'] = [SPEND]
    if 'interval' in spend:
        source['interval'] = spend['interval']
    if spend.get('notices'):
        source['notices'] = {SPEND: spend['notices']}
    listed = config.setdefault('facts', [{'provider': 'campaign'}, {'provider': 'optimization'}])
    if isinstance(listed, list):
        listed.append(source)
    if (review := spend.get('closing_review')) is not None and isinstance(
        plugins := config.setdefault('plugins', {}), dict
    ):
        stop = plugins.setdefault('stop_rule', {})
        if isinstance(stop, dict):
            stop.setdefault('while_held', review)
    return config


def downgraded(config: dict[str, object]) -> dict[str, object]:
    """Return a stored config in the shape of the revision before."""
    listed = config.pop('facts', None)
    sources = listed if isinstance(listed, list) else []
    source = next((s for s in sources if isinstance(s, dict) and s.get('metrics') == [SPEND]), None)
    plugins = config.get('plugins')
    plugins = plugins if isinstance(plugins, dict) else {}
    rule = plugins.get('stop_rule')
    review = 'run'
    if isinstance(rule, dict):
        review = rule.pop('while_held', review)
        held = rule.pop('hold_at', {})
        if isinstance(held, dict) and SPEND in held:
            rule['max_spend'] = held[SPEND]
        if rule == {}:
            del plugins['stop_rule']
    config['spend'] = None
    if source is not None:
        command = source.get('command', [])
        notices = source.get('notices', {})
        config['spend'] = {
            'command': command[len(WRAPPER) :] if isinstance(command, list) else command,
            'notices': notices.get(SPEND, []) if isinstance(notices, dict) else [],
            'interval': source.get('interval', 300),
            'closing_review': review,
        }
    return config
