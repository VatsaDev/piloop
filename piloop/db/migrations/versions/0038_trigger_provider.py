"""A trigger entry names its trigger with ``provider``, not ``kind``.

The stored copy of each ``piloop.toml`` (``campaigns.config``) is rewritten as ``piloop.deprecated`` rewrites the file,
so the daemon reads it before the next ``piloop init``: ``kind`` becomes ``provider`` in each entry of each role's
``triggers``. The downgrade renames it back.

Revision ID: 0038
Revises: 0037
"""

import json
from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

revision: str = '0038'
down_revision: str | None = '0037'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    _rename('kind', 'provider')


def downgrade() -> None:
    _rename('provider', 'kind')


def _rename(old: str, new: str) -> None:
    bind = op.get_bind()
    for campaign_id, config in bind.execute(text('SELECT id, config FROM campaigns')).all():
        stored = renamed(json.loads(config), old, new)
        bind.execute(
            text('UPDATE campaigns SET config = :c WHERE id = :id'), {'c': json.dumps(stored), 'id': campaign_id}
        )


def renamed(config: dict[str, object], old: str, new: str) -> dict[str, object]:
    """Return a stored config with ``old`` renamed ``new`` in every trigger entry."""
    roles = config.get('roles')
    for role in roles.values() if isinstance(roles, dict) else ():
        triggers = role.get('triggers') if isinstance(role, dict) else None
        if isinstance(triggers, list):
            role['triggers'] = [
                {(new if k == old else k): v for k, v in t.items()} if isinstance(t, dict) else t for t in triggers
            ]
    return config
