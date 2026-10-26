"""``[memory] documents`` is ``documents`` of the ``local`` memory provider, in ``[plugins.memory]``.

The stored copy of each ``piloop.toml`` (``campaigns.config``) loses ``memory``. Its documents, when it names any, go to
``plugins.memory``, which selects ``local`` unless it selects a provider already. The downgrade moves them back.

Revision ID: 0061
Revises: 0060
"""

import json
from collections.abc import Callable, Sequence

from alembic import op
from sqlalchemy import text

revision: str = '0061'
down_revision: str | None = '0060'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
LOCAL = 'local'


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


def upgraded(config: dict[str, object]) -> dict[str, object]:
    """Return a stored config in the shape of this revision."""
    memory = config.pop('memory', None)
    documents = memory.get('documents') if isinstance(memory, dict) else None
    plugins = config.setdefault('plugins', {}) if documents else None
    table = plugins.setdefault('memory', {'provider': LOCAL}) if isinstance(plugins, dict) else None
    if isinstance(table, dict):
        table['documents'] = documents
    return config


def downgraded(config: dict[str, object]) -> dict[str, object]:
    """Return a stored config in the shape of the revision before."""
    plugins = config.get('plugins')
    table = plugins.get('memory') if isinstance(plugins, dict) else None
    if isinstance(table, dict) and 'documents' in table:
        config['memory'] = {'documents': table.pop('documents')}
    return config
