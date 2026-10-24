"""Entire is removed: ``agent_sessions.checkpoint_id`` goes, and the stored configs lose ``entire_agent``.

The stored copy of each ``piloop.toml`` (``campaigns.config``) is rewritten as ``piloop init`` would store it now: a
harness table loses ``entire_agent``. The checkpoint refs (``refs/entire/``) in the campaign repos stay.

The downgrade adds the column back, empty, and leaves the stored configs: an older piloop reads them after its own
``piloop init``, as after any other downgrade.

Revision ID: 0036
Revises: 0035
"""

import json
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

revision: str = '0036'
down_revision: str | None = '0035'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute('ALTER TABLE agent_sessions DROP COLUMN checkpoint_id')
    bind = op.get_bind()
    for campaign_id, config in bind.execute(text('SELECT id, config FROM campaigns')).all():
        stored = upgraded(json.loads(config))
        bind.execute(
            text('UPDATE campaigns SET config = :c WHERE id = :id'), {'c': json.dumps(stored), 'id': campaign_id}
        )


def downgrade() -> None:
    op.add_column('agent_sessions', sa.Column('checkpoint_id', sa.String(), nullable=True))


def upgraded(config: object) -> object:
    """Return a stored config in the shape of this revision."""
    harnesses = config.get('harness') if isinstance(config, dict) else None
    for harness in harnesses.values() if isinstance(harnesses, dict) else ():
        if isinstance(harness, dict):
            harness.pop('entire_agent', None)
    return config
