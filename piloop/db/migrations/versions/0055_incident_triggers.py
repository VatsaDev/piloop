"""The ``anomaly`` trigger is ``run_failed`` and ``dead_at_startup``, and ``anomaly.reported`` names its trigger.

The stored copy of each ``piloop.toml`` (``campaigns.config``) is rewritten as ``piloop.deprecated`` rewrites the file:
an ``anomaly`` entry of a role's ``triggers`` becomes the two triggers, in its place. A list of sections that names the
``anomalies`` section names ``idle_queue``, ``same_as_parent`` and ``reported`` there instead. Each
``anomaly.reported`` event's ``anomaly`` (its kind) becomes ``trigger``, the name of the trigger that finds that kind.

The downgrade puts back one ``anomaly`` trigger where the first of the two was, the ``anomalies`` section where the
first of the three was, and the kinds; an incident of another trigger reads as a failed run.

Revision ID: 0055
Revises: 0054
"""

import json
from collections.abc import Callable, Sequence
from functools import partial

from alembic import op
from sqlalchemy import text

revision: str = '0055'
down_revision: str | None = '0054'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
TRIGGERS = ('run_failed', 'dead_at_startup')
SECTIONS = ('idle_queue', 'same_as_parent', 'reported')
KINDS = {'failed': 'run_failed', 'dead at startup': 'dead_at_startup'}
"""Each kind that ``anomaly.reported`` held, by the trigger that finds it now. Older kinds keep their value."""


RENAME = """UPDATE events SET payload = json_remove(json_set(payload, '$.{new}', {value}), '$.{old}')
WHERE kind = 'anomaly.reported' AND json_extract(payload, '$.{old}') {match}"""
"""Rename the key ``old`` of an ``anomaly.reported`` payload to ``new``, with ``value``, where its value ``match``es."""


def upgrade() -> None:
    _rewrite(upgraded)
    bind = op.get_bind()
    for kind, trigger in KINDS.items():
        bind.execute(
            text(RENAME.format(old='anomaly', new='trigger', value=':to', match='= :was')), {'to': trigger, 'was': kind}
        )
    bind.execute(
        text(
            RENAME.format(old='anomaly', new='trigger', value="json_extract(payload, '$.anomaly')", match='IS NOT NULL')
        )
    )


def downgrade() -> None:
    _rewrite(downgraded)
    bind = op.get_bind()
    for kind, trigger in KINDS.items():
        bind.execute(
            text(RENAME.format(old='trigger', new='anomaly', value=':to', match='= :was')), {'to': kind, 'was': trigger}
        )
    bind.execute(text(RENAME.format(old='trigger', new='anomaly', value="'failed'", match='IS NOT NULL')))


def _rewrite(change: Callable[[dict[str, object]], dict[str, object]]) -> None:
    bind = op.get_bind()
    for campaign_id, config in bind.execute(text('SELECT id, config FROM campaigns')).all():
        stored = change(json.loads(config))
        bind.execute(
            text('UPDATE campaigns SET config = :c WHERE id = :id'), {'c': json.dumps(stored), 'id': campaign_id}
        )


def upgraded(config: dict[str, object]) -> dict[str, object]:
    """Return a stored config in the shape of this revision."""
    return _lists(config, partial(_split, old='anomaly', new=TRIGGERS), partial(_split, old='anomalies', new=SECTIONS))


def downgraded(config: dict[str, object]) -> dict[str, object]:
    """Return a stored config in the shape of the revision before."""
    return _lists(config, partial(_join, old=TRIGGERS, new='anomaly'), partial(_join, old=SECTIONS, new='anomalies'))


type Change = Callable[[list[object]], list[object]]


def _lists(config: dict[str, object], triggers: Change, sections: Change) -> dict[str, object]:
    """``config`` with each role's ``triggers`` and every list of sections changed."""
    roles = config.get('roles')
    for table in roles.values() if isinstance(roles, dict) else ():
        if isinstance(table, dict) and isinstance(table.get('triggers'), list):
            table['triggers'] = triggers(table['triggers'])
        if isinstance(table, dict) and isinstance(table.get('sections'), list):
            table['sections'] = sections(table['sections'])
    status = config.get('status')
    if isinstance(status, dict) and isinstance(status.get('sections'), list):
        status['sections'] = sections(status['sections'])
    return config


def _named(entry: object, names: Sequence[str]) -> bool:
    return isinstance(entry, dict) and entry.get('provider') in names


def _split(entries: list[object], old: str, new: Sequence[str]) -> list[object]:
    """``entries`` with each entry of provider ``old`` replaced by one entry for each of ``new``."""
    return [e for entry in entries for e in ([{'provider': n} for n in new] if _named(entry, [old]) else [entry])]


def _join(entries: list[object], old: Sequence[str], new: str) -> list[object]:
    """``entries`` with the entries of ``old`` replaced by one ``new`` entry where the first of them was."""
    first = next((i for i, entry in enumerate(entries) if _named(entry, old)), None)
    if first is None:
        return entries
    kept = [entry for entry in entries[:first] if not _named(entry, old)]
    return [*kept, {'provider': new}, *(entry for entry in entries[first:] if not _named(entry, old))]
