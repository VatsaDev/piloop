"""The ``ending`` state of an agent session: the daemon ended the agent, and its process is not gone yet.

The ``state`` CHECK of ``agent_sessions`` admits ``ending``. A ``starting`` or ``running`` row that the daemon ended
already (it has an ``agent_session.stop_ended`` event) becomes ``ending``. Batch mode rebuilds the table and drops its
triggers, so ``recreate`` puts them back.

The downgrade turns every ``ending`` back into ``running``. Its ``agent_session.stop_ended`` event stays.

Revision ID: 0020
Revises: 0019
"""

from collections.abc import Sequence

from alembic import op

from piloop.db.triggers import recreate

revision: str = '0020'
down_revision: str | None = '0019'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
BEFORE = ('requested', 'starting', 'running', 'exited', 'killed')
AFTER = ('requested', 'starting', 'running', 'ending', 'exited', 'killed')
ENDED = "SELECT target_id FROM events WHERE target_kind = 'agent_session' AND kind = 'agent_session.stop_ended'"
"""The agent sessions that the daemon ended."""


def upgrade() -> None:
    _states(AFTER)
    op.execute(f"UPDATE agent_sessions SET state = 'ending' WHERE state IN ('starting', 'running') AND id IN ({ENDED})")
    recreate(op, 'agent_sessions')


def downgrade() -> None:
    op.execute("UPDATE agent_sessions SET state = 'running' WHERE state = 'ending'")
    _states(BEFORE)
    recreate(op, 'agent_sessions')


def _states(states: tuple[str, ...]) -> None:
    with op.batch_alter_table('agent_sessions', schema=None, recreate='always') as batch_op:
        batch_op.drop_constraint('agentsessionstate', type_='check')
        batch_op.create_check_constraint('agentsessionstate', f'state IN ({", ".join(f"{s!r}" for s in states)})')
