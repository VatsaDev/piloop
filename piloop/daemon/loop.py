"""The daemon's loop: it sleeps until the feed moves or a deadline comes, then runs the consumers that it wakes.

A **consumer** is one job of the daemon, such as pushing comments or checking the triggers. It names the event models
that wake it (``wakes_on``). Its ``run`` reads the tables, not the events, so an event says only "look again": running
it twice costs one more read (level-triggered, as Kubernetes controllers are). It returns when it must run again for
time alone, a **due** that it computes from the rows each time, so a restart loses none. Nothing else is periodic.

Each **turn** reads the events after the loop's cursor, runs each consumer that one of them routes to or whose due came,
in the order of the consumers, then moves the cursor. Then the loop waits on the database (``Database.wait_until``):
for a commit of this process, an event of another (the doorbell), a ``wake``, or the earliest due. The cursor lives in
memory: a daemon that starts runs every consumer once, which covers what it missed.
"""

import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import partial

from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.db.feed import newest
from piloop.db.models import Event, EventModel, UnknownEvent, utcnow
from piloop.db.session import Database

type Kinds = frozenset[type[EventModel]]
"""Event models. A consumer that names one wakes on its kinds and on those of its subclasses, so ``EventModel`` is all."""
PAGE = 1000
"""How many events one turn reads at most. A turn with more reads again at once."""


@dataclass(frozen=True)
class Turn:
    """When a turn runs: its time, and how long the loop was away past the due that it planned (a suspend)."""

    now: datetime
    absent: timedelta = timedelta(0)


@dataclass(frozen=True)
class Consumer:
    """One job of the daemon: the event models that wake it, and the job."""

    name: str
    """Its name, unique in the daemon. ``Loop.wake`` runs it by this name."""
    wakes_on: Kinds
    """The event models whose new events run it in the next turn."""
    run: Callable[[Turn], datetime | None]
    """Do the job from the tables, and return when it must run again for time alone, or None."""


class Soonest:
    """The earliest of the dues that a consumer finds as it runs: what its ``run`` returns."""

    def __init__(self) -> None:
        self.at: datetime | None = None

    def add(self, *dues: datetime | None) -> None:
        """Keep each due that comes before the earliest so far. None is no due."""
        for due in dues:
            if due is not None and (self.at is None or due < self.at):
                self.at = due


class Loop:
    """The consumers, the cursor and the dues of one daemon process.

    Args:
        db: The daemon's database.
        consumers: In the order that a turn runs them.
        now: The clock. A test passes its own.
    """

    def __init__(self, db: Database, consumers: Sequence[Consumer], now: Callable[[], datetime] = utcnow) -> None:
        self._db, self._consumers, self._now = db, tuple(consumers), now
        self._routes: dict[str, tuple[Consumer, ...]] = {}
        """The consumers of each event kind, found on its first event."""
        self._due: dict[str, datetime | None] = {}
        self._woken: set[str] = set()
        """The consumers that ``wake`` asked for, for the next turn."""
        self._lock = threading.Lock()
        self._cursor = 0

    def start(self) -> None:
        """Read the cursor, then run every consumer once: what happened while no daemon ran."""
        with self._db.session() as s:
            self._cursor = newest(s)
        self.run_all(Turn(self._now()))

    def run_all(self, turn: Turn) -> None:
        """Run every consumer once, whatever woke it."""
        self._run(self._consumers, turn)

    def wake(self, name: str) -> None:
        """Run the consumer ``name`` in the next turn, for a change that no event says: a thread of this process ended.

        Safe from any thread.
        """
        with self._lock:
            self._woken.add(name)
        self._db.notify()

    def turn(self, absent: timedelta = timedelta(0)) -> bool:
        """Run the consumers that the events after the cursor, ``wake`` and the dues ask for. Return True if events wait.

        Each consumer runs at most once in a turn, whatever woke it.
        """
        now = self._now()
        with self._db.session() as s:
            page = s.execute(select(Event.id, Event.kind).where(Event.id > self._cursor).order_by(Event.id).limit(PAGE))
            rows = page.all()
        with self._lock:
            woken, self._woken = self._woken, set()
        due = {name for name, at in self._due.items() if at is not None and at <= now}
        routed = {c.name for _, kind in rows for c in self._route(kind)}
        if rows:
            self._cursor = rows[-1][0]
        ran = [c for c in self._consumers if c.name in routed | woken | due]
        if ran:
            self._run(ran, Turn(now, absent))
        return len(rows) == PAGE

    def next_due(self) -> datetime | None:
        """The earliest due of the consumers, or None when only events wake them."""
        return min((at for at in self._due.values() if at is not None), default=None)

    def serve(self, stopped: Callable[[], bool]) -> None:
        """Turn until ``stopped`` holds. Whoever makes it hold calls ``Database.notify``, so that the wait sees it."""
        if stopped():  # a signal during the daemon's start
            return
        self.start()
        absent = timedelta(0)
        while not stopped():
            if self.turn(absent):
                continue
            planned = self.next_due()
            left = (planned - self._now()).total_seconds() if planned is not None else None
            # it returns at once when an event, a wake or the due is there already
            self._db.wait_until(
                partial(_moved, self._cursor), give_up=lambda: stopped() or self._has_woken(), timeout=_timeout(left)
            )
            late = self._now() - planned if planned is not None else timedelta(0)
            absent = max(late, timedelta(0))

    def _has_woken(self) -> bool:
        with self._lock:
            return bool(self._woken)

    def _route(self, kind: str) -> tuple[Consumer, ...]:
        if (found := self._routes.get(kind)) is None:
            model = EventModel.registered().get(kind, UnknownEvent)
            found = self._routes[kind] = tuple(
                c for c in self._consumers if any(issubclass(model, w) for w in c.wakes_on)
            )
        return found

    def _run(self, consumers: Sequence[Consumer], turn: Turn) -> None:
        for consumer in consumers:
            self._due[consumer.name] = consumer.run(turn)


def routes(consumers: Sequence[Consumer], models: Mapping[str, type[EventModel]]) -> dict[str, list[str]]:
    """The consumers that each kind of ``models`` wakes, by name: what ``Loop`` routes, for a test to check."""
    return {
        kind: [c.name for c in consumers if any(issubclass(model, w) for w in c.wakes_on)]
        for kind, model in models.items()
    }


def _moved(cursor: int, s: Session) -> bool:
    """Whether the feed has an event after ``cursor``."""
    return newest(s) > cursor


def _timeout(left: float | None) -> float:
    return float('inf') if left is None else left
