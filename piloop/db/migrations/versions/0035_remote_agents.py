"""Agents run on workers elsewhere, every agent in a worktree of its own.

- ``agent_sessions.machine``: the worker that runs an agent, when it is not the daemon's own.
- A job's ``workspace`` is now the snapshot of the workspace in the CAS, not its place: the jobs that name a place lose
  it, and an agent that a worker re-attaches to keeps writing its workspace through the link that its worktree has.
- ``[roles.<name>] workdir`` is removed: the stored configs lose it, and the open leases of agents that worked in the
  campaign's checkout end, so the next daemon fails their rows as it does any agent that it cannot adopt.

The downgrade drops the column and leaves the configs and the leases: an older piloop reads them after its own
``piloop init``.

Revision ID: 0035
Revises: 0034
"""

import json
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

revision: str = '0035'
down_revision: str | None = '0034'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

CUT = '{"cancelled": "workdir is removed in piloop 0.3.0: the agent worked in the campaign checkout"}'


def upgrade() -> None:
    # native, not a batch rebuild, which drops the table's triggers
    op.add_column('agent_sessions', sa.Column('machine', sa.String(), nullable=True))
    for path in ('$.artifact.workspace', '$.bar.artifact.workspace'):
        op.execute(f"UPDATE leases SET job = json_remove(job, '{path}') WHERE json_type(job, '{path}') = 'text'")
    op.execute(
        f"UPDATE leases SET state = 'cancelled', ended_at = CURRENT_TIMESTAMP, receipt = '{CUT}'"
        " WHERE state IN ('queued', 'acquired', 'running') AND json_extract(job, '$.artifact.kind') = 'checkout'"
    )
    bind = op.get_bind()
    for campaign_id, config in bind.execute(text('SELECT id, config FROM campaigns')).all():
        bind.execute(
            text('UPDATE campaigns SET config = :c WHERE id = :id'),
            {'c': json.dumps(upgraded(json.loads(config))), 'id': campaign_id},
        )


def downgrade() -> None:
    op.execute('ALTER TABLE agent_sessions DROP COLUMN machine')


def upgraded(config: object) -> object:
    """``config`` without the ``workdir`` of its roles."""
    roles = config.get('roles') if isinstance(config, dict) else None
    for role in roles.values() if isinstance(roles, dict) else ():
        if isinstance(role, dict):
            role.pop('workdir', None)
    return config
