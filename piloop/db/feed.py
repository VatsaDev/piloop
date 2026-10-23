"""The events feed after a cursor: the events of some topics, oldest first, and the cursor to read after next.

The event id is the cursor. SQLite has one writer at a time and allocates an id inside the writing transaction, so
the events at or below the newest id that a reader sees are all committed: a later commit never takes a smaller id.
Every piloop connection, ``piloop sql`` included, keeps it so: events are append-only, and a new event takes the next
id (``piloop.db.triggers.immutable``). Otherwise a reader whose cursor had passed a deleted, moved or explicit id would
miss the event that takes it.
"""

from collections.abc import Generator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass

from sqlalchemy import ColumnElement, func, or_, select
from sqlalchemy.orm import Session

from piloop.db.events import has
from piloop.db.models import Event, EventModel
from piloop.db.session import read_transaction


@dataclass(frozen=True)
class Topic:
    """The events of ``model``. With ``to``, only those addressed to that worker: the model's ``worker``."""

    model: type[EventModel]
    to: str | None = None

    def matches(self) -> ColumnElement[bool]:
        return has(self.model) if self.to is None else has(self.model, worker=self.to)


@dataclass(frozen=True)
class Page:
    """The events of a read, by id, oldest first."""

    ids: list[int]
    next: int
    """The cursor for the next read. It passes every event that the read examined, those that it left out too, so a
    read that returns nothing still moves on."""
    more: bool
    """Events newer than ``next`` exist already: read again without waiting. They may not match."""


class CursorAhead(Exception):
    """The cursor is past the newest event: it is not a cursor of this database. Read a new snapshot."""

    code = 'cursor_ahead'

    def __init__(self, after: int, newest: int) -> None:
        super().__init__(
            f'cursor {after} is ahead of the feed, which ends at {newest}: read a new snapshot and use its cursor'
        )


def newest(s: Session) -> int:
    """The id of the newest event, 0 when there is none."""
    return s.scalar(select(func.max(Event.id))) or 0


@contextmanager
def snapshot(s: Session) -> Generator[int]:
    """Hold one read transaction on ``s`` and yield its cursor: the id of the newest event that it sees."""
    with read_transaction(s):
        yield newest(s)


def after(
    s: Session, cursor: int, topics: Sequence[Topic] = (), *, limit: int, where: Sequence[ColumnElement[bool]] = ()
) -> Page:
    """The ids of at most ``limit`` events after ``cursor`` that match ``where`` and a topic, oldest first.

    No topics is every event. Events never change, so a caller reads their rows by id afterwards.

    Raises:
        ValueError: ``cursor`` is negative.
        CursorAhead: ``cursor`` is past the newest event.
    """
    if cursor < 0:
        raise ValueError(f'cursor must be 0 or more, not {cursor}')
    query = select(Event.id).where(Event.id > cursor, *where).order_by(Event.id).limit(limit)
    if topics:
        query = query.where(or_(*(topic.matches() for topic in topics)))
    with snapshot(s) as end:
        if cursor > end:
            raise CursorAhead(cursor, end)
        ids = list(s.scalars(query.where(Event.id <= end)))
    last = ids[-1] if len(ids) == limit else end
    return Page(ids=ids, next=last, more=last < end)
