"""The event that gives an incident to an agent, and the incidents that an agent got."""

from typing import Literal

from sqlalchemy.orm import Session

from piloop.db.events import in_order
from piloop.db.models import Event, EventModel
from piloop.ids import AgentSessionId, Ref, TargetKind, TargetRef


class AnomalyReported(EventModel):
    """An incident that an agent was requested for, on the agent's row: one event for each incident."""

    kind: Literal['anomaly.reported'] = 'anomaly.reported'
    subject: Ref
    """The row that the incident is about."""
    trigger: str
    """The trigger that found it, by name: ``run_failed``."""
    line: str
    """The incident as the agent's brief shows it."""


def assigned(s: Session, agent_session_id: AgentSessionId) -> list[str]:
    """Return the incident lines that an agent got, in order. A routine sweep got none."""
    rows = in_order(s, AnomalyReported, Event.target == TargetRef(TargetKind.AGENT_SESSION, agent_session_id))
    return [a.line for _, a in rows]
