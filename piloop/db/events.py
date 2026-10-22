"""Add to the events feed. Every state change that the daemon makes goes through here."""

from sqlalchemy.orm import Session

from piloop.actor import Actor, System
from piloop.db.models import Event
from piloop.ids import TargetKind, TargetRef

ANOMALY_EVENT = 'anomaly.reported'
"""An event on an agent's row, one for each anomaly it was requested for, with ``{key, anomaly}`` as payload."""


def emit(
    s: Session,
    kind: str,
    target_kind: TargetKind,
    target_id: int,
    *,
    actor: Actor = System.DAEMON,
    payload: dict[str, object] | None = None,
) -> Event:
    """Add an event to the session. The caller commits."""
    event = Event(kind=kind, actor=actor.signature, target=TargetRef(target_kind, target_id), payload=payload or {})
    s.add(event)
    return event
