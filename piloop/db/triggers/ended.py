"""An agent whose session has ended leaves no finding: a TEMP trigger on ``comments``."""

from piloop.db.models import ENDED, AgentSessionState, CommentKind
from piloop.db.triggers.ddl import Trigger

NO_FINDING = ', '.join(f"'{state}'" for state in (AgentSessionState.ENDING, *ENDED))
"""The states of an agent session whose findings are refused, as a SQL list."""


def no_late_finding() -> Trigger:
    """TEMP: refuse a ``finding`` from a connection whose agent session is ``ending``, ``exited`` or ``killed``.

    A finding settles a run as finished, so one that lands after the daemon ended the agent would undo a stop or a
    cancel. The daemon sets the row ``ending`` before it kills the agent, and SQLite has one writer at a time: a
    finding commits before the end, or this trigger refuses it. A connection of another actor has no agent session.
    """
    return Trigger(
        'comments_no_late_finding',
        'comments',
        'BEFORE INSERT',
        when=(
            f"NEW.kind = '{CommentKind.FINDING}' AND EXISTS (SELECT 1 FROM agent_sessions"
            f' WHERE id = (SELECT agent_session FROM temp.session_actor) AND state IN ({NO_FINDING}))'
        ),
        body="SELECT RAISE(ABORT, 'the agent session of this connection has ended: its finding is refused')",
        temp=True,
    )


ENDED_AGENTS = (no_late_finding(),)
"""What an agent whose session has ended can no longer write."""
