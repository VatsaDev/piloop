"""``wake_budget`` is the ``budget`` of a role's ``lessons`` section, and ``[status]`` lists the overview's sections.

The stored copy of each ``piloop.toml`` (``campaigns.config``) is rewritten as ``piloop.deprecated`` rewrites the file,
so the daemon reads it before the next ``piloop init``. ``memory.wake_budget`` and each role's ``wake_budget`` go. A
role whose budget differs from its old default gets ``sections``: its default list, with the lessons at that budget
first, or without them at 0. The watchdog's old default, 0, changes nothing: it now reads the lessons. The downgrade puts the budgets back and drops ``status`` and every role's ``sections``.

Revision ID: 0053
Revises: 0052
"""

import json
from collections.abc import Callable, Sequence

from alembic import op
from sqlalchemy import text

revision: str = '0053'
down_revision: str | None = '0052'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
LESSONS = 'lessons'
BUDGET = 400
OVERVIEW = (
    'headline',
    'goal',
    'tracks',
    'trajectory',
    'runs',
    'queue',
    'agents',
    'evaluations',
    'candidates',
    'anomalies',
    'inbox',
    'events',
)
SWEEP = ('incidents', *OVERVIEW, 'tools', 'pack')
"""A sweep's default sections after the lessons."""
WATCHDOG = 'watchdog'
"""The one role whose default had no lessons (its ``wake_budget`` was 0)."""


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
    memory = config.get('memory')
    shared = memory.pop('wake_budget', BUDGET) if isinstance(memory, dict) else BUDGET
    roles = config.get('roles')
    for name, table in roles.items() if isinstance(roles, dict) else ():
        if not isinstance(table, dict):
            continue
        own = table.pop('wake_budget', None)
        default = 0 if name == WATCHDOG else BUDGET
        budget = own if own is not None else default if name == WATCHDOG else shared
        if budget == default or 'sections' in table:
            continue
        rest = ('history',) if table.get('runs') or name == 'implementer' else SWEEP
        lessons = [{'provider': LESSONS, 'budget': budget}] if budget else []
        table['sections'] = [*lessons, *({'provider': n} for n in rest)]
    return config


def downgraded(config: dict[str, object]) -> dict[str, object]:
    """Return a stored config in the shape of the revision before."""
    config.pop('status', None)
    memory = config.get('memory')
    if isinstance(memory, dict):
        memory['wake_budget'] = BUDGET
    roles = config.get('roles')
    for table in roles.values() if isinstance(roles, dict) else ():
        sections = table.pop('sections', None) if isinstance(table, dict) else None
        if isinstance(sections, list):
            lessons = next((e for e in sections if isinstance(e, dict) and e.get('provider') == LESSONS), None)
            table['wake_budget'] = 0 if lessons is None else lessons.get('budget', BUDGET)
    return config
