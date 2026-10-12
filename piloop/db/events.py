"""Add to the events feed. Every state change that the daemon makes goes through here."""

from sqlalchemy.orm import Session

from piloop.db.models import Event, TargetKind

ANOMALY_EVENT = 'anomaly.reported'
"""An event on a watchdog's row, one for each anomaly it was requested for, with ``{key, anomaly}`` as payload."""


def emit(
    s: Session,
    kind: str,
    target_kind: TargetKind,
    target_id: int,
    *,
    actor: str = 'daemon',
    payload: dict[str, object] | None = None,
) -> Event:
    """Add an event to the session. The caller commits."""
    event = Event(kind=kind, actor=actor, target_kind=target_kind, target_id=target_id, payload=payload or {})
    s.add(event)
    return event
