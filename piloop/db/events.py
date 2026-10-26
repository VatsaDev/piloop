"""Add to the events feed, and find events in it. Every state change that piloop makes goes through ``emit``."""

from typing import Literal

from pydantic_core import to_jsonable_python
from sqlalchemy import ColumnElement, and_, func, select
from sqlalchemy.orm import Session

from piloop.actor import Actor
from piloop.db.models import Event, EventModel
from piloop.ids import CampaignId, TargetRef


class SubjectSettled(EventModel):
    """On a subject (``idea#12``): its work is over.

    So the top-level workspace entries named after it (``idea12``, ``idea12-…``) are final.
    """

    kind: Literal['subject.settled'] = 'subject.settled'
    campaign: CampaignId


def emit(s: Session, event: EventModel, target: TargetRef, actor: Actor) -> Event:
    """Add ``event`` on ``target``, signed by ``actor``, to the session. The caller commits."""
    row = Event(actor=actor.signature, target=target, payload=event)
    s.add(row)
    return row


def has(model: type[EventModel], **fields: object) -> ColumnElement[bool]:
    """The events of ``model``'s kinds whose payload has each value of ``fields``, or one of the values of a tuple.

    Raises:
        TypeError: ``model`` has no such field.
    """
    clauses = [Event.kind.in_(model.kinds())]
    for name, wanted in fields.items():
        field = model.model_fields.get(name)
        if field is None:
            raise TypeError(f'{model.__qualname__} has no field {name!r}')
        values = wanted if isinstance(wanted, tuple) else (wanted,)
        path = func.json_extract(Event.payload, f'$.{field.alias or name}')
        clauses.append(path.in_([to_jsonable_python(v) for v in values]))
    return and_(*clauses)


def latest[M: EventModel](s: Session, model: type[M], *where: ColumnElement[bool]) -> tuple[Event, M] | None:
    """The newest event of ``model`` that matches ``where``, with its payload, or None."""
    found = s.scalars(select(Event).where(has(model), *where).order_by(Event.id.desc()).limit(1)).first()
    return None if found is None else (found, payload_of(found, model))


def in_order[M: EventModel](s: Session, model: type[M], *where: ColumnElement[bool]) -> list[tuple[Event, M]]:
    """The events of ``model`` that match ``where``, oldest first, with their payloads."""
    found = s.scalars(select(Event).where(has(model), *where).order_by(Event.id))
    return [(e, payload_of(e, model)) for e in found]


def payload_of[M: EventModel](event: Event, model: type[M]) -> M:
    """The payload of ``event`` as ``model``.

    Raises:
        TypeError: it is of another model: the module that registers its kind is not loaded, or another one is.
    """
    if not isinstance(event.payload, model):
        raise TypeError(f'event#{event.id} is {type(event.payload).__qualname__}, not {model.__qualname__}')
    return event.payload
