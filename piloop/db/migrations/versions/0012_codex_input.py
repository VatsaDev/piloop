"""The ``input`` of a Codex agent's usage is its uncached input, as for the other harnesses.

Codex counts the cached input in ``input_tokens``, and piloop stored that number. The codex tally now subtracts
``cache_read``, and this revision does the same to the rows that it read before. A row is a Codex row when the tally of
its harness is ``codex``: the tally of the harness in the project's stored config, else the tally of the built-in
harness of that name. A row whose harness is in neither (renamed or removed since) is a Codex row when its ``source``
is a Codex rollout file. ``total`` stays the harness's own count. The downgrade adds ``cache_read`` back.

Revision ID: 0012
Revises: 0011
"""

import json
import re
from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

revision: str = '0012'
down_revision: str | None = '0011'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
BUILT_IN_TALLIES = {'pi': 'pi', 'claude': 'claude', 'codex': 'codex'}
ROLLOUT = re.compile(r'/rollout-[^/]*\.jsonl$')
ROWS = (
    'SELECT workers.id, workers.harness, workers.usage, projects.config FROM workers'
    ' LEFT JOIN projects ON projects.id = workers.project_id WHERE workers.usage IS NOT NULL'
)


def upgrade() -> None:
    _shift(-1)


def downgrade() -> None:
    _shift(+1)


def _shift(sign: int) -> None:
    """Add ``sign`` times ``cache_read`` to the ``input`` of each Codex row. Upgrade never takes it below zero."""
    bind = op.get_bind()
    for worker_id, harness, stored, config in bind.execute(text(ROWS)).all():
        usage = json.loads(stored)
        if not _codex(harness, usage, json.loads(config) if config else {}):
            continue
        shifted = usage['input'] + sign * usage['cache_read']
        if shifted < 0:
            continue
        usage['input'] = shifted
        bind.execute(text('UPDATE workers SET usage = :u WHERE id = :id'), {'u': json.dumps(usage), 'id': worker_id})


def _codex(harness: str | None, usage: dict[str, object], config: dict[str, object]) -> bool:
    harnesses = config.get('harness')
    definition = harnesses.get(harness) if isinstance(harnesses, dict) else None
    if isinstance(definition, dict):
        spec = definition.get('usage')
        tally = spec.get('tally') if isinstance(spec, dict) else None
    else:
        tally = BUILT_IN_TALLIES.get(str(harness))
    if tally is not None:
        return tally == 'codex'
    return bool(ROLLOUT.search(str(usage.get('source', ''))))
