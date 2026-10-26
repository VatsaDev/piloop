"""Ask the daemon for something, and answer it, in the events feed.

An ask is an event whose payload says what is asked: ``StopRequested``, ``CancelRequested``, ``RemeasureRequested`` or
``RejudgeRequested``, each declared by the layer that acts on it. The daemon answers an ask with ``RequestAnswered``,
in the transaction of the act that answers it. An ask is **pending** while it is the latest ask of its kind on its
target and no ``RequestAnswered`` names it: a newer ask replaces an older one. ``pending`` is the one place that says
so.
"""

from typing import ClassVar, Literal

from sqlalchemy import ColumnElement, and_, exists, func, select
from sqlalchemy.orm import Session, aliased

from piloop.actor import System
from piloop.db.events import emit, has, payload_of
from piloop.db.models import Event, EventModel, ReadBy


class RequestAnswered(EventModel):
    """The daemon answered an ask: it did what the ask asked, or found that there is nothing to do. On the ask's target."""

    READ_BY: ClassVar[ReadBy] = ReadBy.READERS
    kind: Literal['request.answered'] = 'request.answered'
    ask: int
    """The id of the event of the ask."""


def pending[M: EventModel](s: Session, model: type[M], *where: ColumnElement[bool]) -> list[tuple[Event, M]]:
    """The pending asks of ``model`` that match ``where``, oldest first, with their payloads."""
    newer = aliased(Event)
    answer = aliased(Event)
    latest = ~exists().where(
        newer.kind == Event.kind,
        newer.target_kind == Event.target_kind,
        newer.target_id == Event.target_id,
        newer.id > Event.id,
    )
    answered = exists().where(
        answer.target_kind == Event.target_kind,
        answer.target_id == Event.target_id,
        answer.kind.in_(RequestAnswered.kinds()),
        func.json_extract(answer.payload, '$.ask') == Event.id,
    )
    query = select(Event).where(and_(has(model), latest, ~answered, *where)).order_by(Event.id)
    return [(e, payload_of(e, model)) for e in s.scalars(query)]


def answer(s: Session, ask: Event) -> None:
    """Answer ``ask`` as the daemon. The caller commits, with the act that answers it."""
    emit(s, RequestAnswered(ask=ask.id), ask.target, System.DAEMON)
