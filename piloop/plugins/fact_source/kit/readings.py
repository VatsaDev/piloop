"""``Readings``: a fact source of named values that the daemon reads every ``interval`` seconds.

Each reading is a ``fact.read`` event on the campaign, the one record of its values. The source supplies each name
that it reads (``reading``), and how much its value grew since the latest keep (``since_keep``), from those events. It
posts a note at each of its ``notices``, and an escalation when its readings start to fail.
"""

import math
from abc import abstractmethod
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Literal, Self

from pydantic import Field, model_validator
from sqlalchemy import ColumnElement, func, select
from sqlalchemy.orm import Session

from piloop.db.events import has, latest, payload_of
from piloop.db.models import Campaign, Comment, Event, EventModel
from piloop.db.triggers import DAEMON_ACTOR
from piloop.ids import CommentKind, TargetKind, TargetRef
from piloop.plugins.fact_source.contract import FACT_SOURCES, FactReadError, FactSource
from piloop.plugins.fact_source.facts import Fact, FactValue, reading, since_keep
from piloop.scoring.ledger import last_keep_at

NOTICE_PRIORITY = 1
NEVER_READ = datetime.min.replace(tzinfo=UTC)
"""The due of a source that never read: now."""


class FactRead(EventModel):
    """One reading of a ``Readings`` source: each value by its name, or None for each when the reading failed."""

    kind: Literal['fact.read'] = 'fact.read'
    values: dict[str, float | None]
    error: str | None = None


class Readings(FactSource):
    """A source that the daemon reads every ``interval`` seconds.

    It supplies each name that it reads (``reading``), and how much its value grew since the latest keep
    (``since_keep``), from its ``fact.read`` events.
    """

    interval: float = Field(default=300, gt=0)
    """The number of seconds between two readings."""
    notices: dict[str, list[Annotated[float, Field(gt=0)]]] = {}
    """Values by the name that it reads. When a value passes one, the daemon posts one note to the inbox, at priority
    1."""

    @model_validator(mode='after')
    def _notices_name_its_values(self) -> Self:
        if unknown := sorted(set(self.notices) - set(self.names())):
            raise ValueError(f'notices for {", ".join(unknown)}, which the source does not read')
        return self

    @abstractmethod
    def names(self) -> tuple[str, ...]:
        """The names of the values that it reads."""

    @abstractmethod
    def read(self, repo: Path) -> dict[str, float]:
        """Read every value of ``names``. The daemon calls it outside any transaction, at the campaign's repo root.

        Raises:
            FactReadError: No values were read.
        """

    def supplies(self) -> tuple[Fact[FactValue], ...]:
        """``reading(name)`` and ``since_keep(name)`` for each name of ``names``."""
        return tuple(fact for name in self.names() for fact in (reading(name), since_keep(name)))

    def gather(
        self, s: Session, campaign: Campaign, wanted: frozenset[Fact[FactValue]], now: datetime
    ) -> dict[Fact[FactValue], FactValue]:
        """Each value of the latest reading that has it, None after a failed one, and its growth since the latest keep."""
        found: dict[Fact[FactValue], FactValue] = {}
        for name in self.names():
            value = _latest(s, campaign, name)
            if reading(name) in wanted:
                found[reading(name)] = value
            if since_keep(name) in wanted:
                found[since_keep(name)] = None if value is None else value - _at_keep(s, campaign, name, value)
        return found

    def due(self, s: Session, campaign: Campaign) -> datetime | None:
        """When the source reads next: ``interval`` after its latest reading, or now before the first."""
        last = latest(s, FactRead, _on(campaign), _of(self.names()[0]))
        return NEVER_READ if last is None else last[0].at + timedelta(seconds=self.interval)

    def record(self, s: Session, campaign: Campaign, values: dict[str, float] | FactReadError) -> None:
        """Record a reading as a ``fact.read`` event, with a note for each notice that it passed. The caller commits.

        The first reading passes every notice below it. The first failed reading of a series posts an escalation at
        priority 0: a stop rule that reads the values may hold on them.
        """
        on = TargetRef(TargetKind.CAMPAIGN, campaign.id)
        if isinstance(values, FactReadError):
            failing = self.names()[0]
            last = latest(s, FactRead, _on(campaign), _of(failing))
            s.add(
                Event(
                    target=on,
                    actor=DAEMON_ACTOR,
                    payload=FactRead(values=dict.fromkeys(self.names()), error=str(values)),
                )
            )
            if last is None or last[1].values.get(failing) is not None:
                body = f'Fact source {FACT_SOURCES.name_of(self)} ({", ".join(self.names())}): the reading failed ({values}).'
                s.add(Comment(target=on, author=DAEMON_ACTOR, kind=CommentKind.ESCALATION, priority=0, body=body))
            return
        before = {name: _last_known(s, campaign, name) for name in self.notices}
        s.add(Event(target=on, actor=DAEMON_ACTOR, payload=FactRead(values=dict(values))))
        for name, notices in self.notices.items():
            was = -math.inf if (known := before[name]) is None else known
            for notice in notices:
                if was < notice <= values[name]:
                    body = f'Notice: {name} is {values[name]:g}, past the {notice:g} notice.'
                    s.add(
                        Comment(
                            target=on, author=DAEMON_ACTOR, kind=CommentKind.NOTE, priority=NOTICE_PRIORITY, body=body
                        )
                    )


def _on(campaign: Campaign) -> ColumnElement[bool]:
    return Event.target == TargetRef(TargetKind.CAMPAIGN, campaign.id)


def _of(name: str) -> ColumnElement[bool]:
    """Select the readings that have a value, or None, for ``name``."""
    return func.json_type(Event.payload, f'$.values."{name}"').is_not(None)


def _known(name: str) -> ColumnElement[bool]:
    """Select the readings that read a value for ``name``: not the failed ones."""
    return func.json_type(Event.payload, f'$.values."{name}"').in_(('real', 'integer'))


def _latest(s: Session, campaign: Campaign, name: str) -> float | None:
    """The value of ``name`` of the latest reading that has it: None before the first, and after a failed reading."""
    last = latest(s, FactRead, _on(campaign), _of(name))
    return None if last is None else last[1].values.get(name)


def _last_known(s: Session, campaign: Campaign, name: str) -> float | None:
    """The value of ``name`` of the latest reading that read one, failed readings since or not."""
    last = latest(s, FactRead, _on(campaign), _known(name))
    return None if last is None else last[1].values.get(name)


def _at_keep(s: Session, campaign: Campaign, name: str, now: float) -> float:
    """``name`` at the end of the run of the campaign's latest keep: the latest reading by then, else the first after.

    Before the first keep it is 0, so all of the value counts.
    """
    at = last_keep_at(s, campaign)
    if at is None:
        return 0.0
    readings = select(Event).where(has(FactRead), _on(campaign), _known(name))
    found = (
        s.scalars(readings.where(Event.at <= at).order_by(Event.id.desc()).limit(1)).first()
        or s.scalars(readings.where(Event.at > at).order_by(Event.id).limit(1)).first()
    )
    then = payload_of(found, FactRead).values.get(name) if found is not None else None
    return now if then is None else then
