"""The herdr runtime is removed, and the stored configs lose its keys.

The stored copy of each ``piloop.toml`` (``campaigns.config``) is rewritten as ``piloop init`` would store it now:

- A harness table loses the keys that only the herdr runtime read: ``interactive``, ``startup_dialogs``,
  ``busy_screens``, ``failed_turn_screens``, ``queue_key`` and ``composer``.
- A stopped campaign that ran on herdr runs on ``subprocess``, and a role that chose herdr takes the campaign's runtime.
  ``piloop.db.session.on_herdr`` refuses the migration while an active campaign or a live agent is on herdr, so no
  other campaign names it. When one does anyway, its ``runtime`` stays, and its config fails with the reason.

The downgrade leaves the stored configs: an older piloop reads them after its own ``piloop init``, as after any other
downgrade.

Revision ID: 0034
Revises: 0033
"""

import json
from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

revision: str = '0034'
down_revision: str | None = '0033'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
HERDR_KEYS = ('interactive', 'startup_dialogs', 'busy_screens', 'failed_turn_screens', 'queue_key', 'composer')


def upgrade() -> None:
    bind = op.get_bind()
    for campaign_id, status, config in bind.execute(text('SELECT id, status, config FROM campaigns')).all():
        stored = upgraded(json.loads(config), stopped=status == 'stopped')
        bind.execute(
            text('UPDATE campaigns SET config = :c WHERE id = :id'), {'c': json.dumps(stored), 'id': campaign_id}
        )


def downgrade() -> None:
    pass


def upgraded(config: dict[str, object], *, stopped: bool) -> dict[str, object]:
    """Return a stored config in the shape of this revision."""
    harnesses = config.get('harness')
    for harness in harnesses.values() if isinstance(harnesses, dict) else ():
        if isinstance(harness, dict):
            for key in HERDR_KEYS:
                harness.pop(key, None)
    if not stopped:
        return config
    if config.get('runtime') == 'herdr':
        config['runtime'] = 'subprocess'
    roles = config.get('roles')
    for role in roles.values() if isinstance(roles, dict) else ():
        if isinstance(role, dict) and role.get('runtime') == 'herdr':
            role['runtime'] = None
    return config
