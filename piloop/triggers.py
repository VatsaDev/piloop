"""The triggers of the roles: the conditions that make the daemon request an agent (https://piloop.autoresearch.inc/concepts/roles#triggers).

A trigger is a pydantic model. Its fields are its parameters, from its entry in ``[roles.<name>] triggers``. On each
tick, for each project and each sweeping role without a pending request, the scheduler calls ``Trigger.fire`` of each
trigger of the role, in the order of the list. A package adds a trigger with an entry point in ``TRIGGER_GROUP``.
"""

import sqlite3
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import cache
from importlib.metadata import entry_points
from typing import Annotated, ClassVar, Literal, NamedTuple, Protocol

from pydantic import BaseModel, BeforeValidator, ConfigDict, SerializeAsAny, field_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.db.events import ANOMALY_EVENT, SCOUT_EVENT
from piloop.db.models import (
    OPEN_RUNS,
    Idea,
    IdeaStatus,
    Project,
    Run,
    Worker,
    WorkerKind,
    WorkerState,
    count,
    project_runs,
    utcnow,
)
from piloop.db.sql import schema_ddl

TRIGGER_GROUP = 'piloop.triggers'
"""The entry point group of the triggers that packages add. Each entry point names a ``Trigger`` subclass."""


class Incident(NamedTuple):
    """An incident-class anomaly that no agent has received yet."""

    key: str
    line: str
    """The anomaly in one line, as the brief of the agent that gets it shows it."""


@dataclass(frozen=True)
class TrackBacklog:
    """One track's part of the backlog: its runnable and waiting ideas, and its share of the target."""

    name: str
    runnable: int
    waiting: int
    """Ideas that only prerequisites in flight block (``piloop.campaign.ideas.waiting``)."""
    target: int
    """The track's share of ``Backlog.target``, rounded up. It is 0 for a track with share 0."""

    @property
    def gap(self) -> int:
        """The ideas that the track needs to reach its share. Negative if it has more."""
        return self.target - self.runnable - self.waiting


@dataclass(frozen=True)
class Backlog:
    """The input to ``low_water``: the runnable ideas, the target, and what the sweeps in flight will add."""

    tracks: tuple[TrackBacklog, ...]
    """One entry per track, in config order. A track below its share is short, whatever the other tracks have."""
    target: int
    """One idea per worker slot plus ``[roles.research] buffer``, but never more than ``[stop] max_runs`` allows."""
    free: int
    """The runs that the project can start now."""
    sweeps: int
    """The research sweeps in flight."""
    per_sweep: int
    """The ideas that one sweep may insert: ``[roles.research] max_ideas``."""

    @property
    def pending(self) -> int:
        """The ideas that the sweeps in flight can still insert."""
        return self.sweeps * self.per_sweep

    @property
    def runnable(self) -> int:
        """The ideas that the queue can take now: ``ready``, not blocked, and on a track with a share."""
        return sum(t.runnable for t in self.tracks)

    @property
    def waiting(self) -> int:
        """The ideas that become runnable when the runs in flight that they wait for finish."""
        return sum(t.waiting for t in self.tracks)

    @property
    def short(self) -> int:
        """The ideas missing from the target after the sweeps in flight insert theirs.

        This is the sum of the tracks' shortfalls. A surplus on one track does not cover a shortfall on another. If no
        track is short, it is the negative surplus. An idea that waits for a run in flight is not missing.
        """
        gaps = [t.gap for t in self.tracks]
        wanted = sum(g for g in gaps if g > 0) if any(g > 0 for g in gaps) else sum(gaps)
        return wanted - self.pending

    @property
    def idle(self) -> int:
        """The free worker slots that no runnable idea and no sweep in flight will fill."""
        return self.free - self.runnable - self.pending

    def held(self) -> str:
        """Describe the backlog without the sweeps.

        For example: ``1 of 3 runnable ideas, 2 more waiting on runs in flight``.
        """
        waits = f', {self.waiting} more waiting on runs in flight' if self.waiting else ''
        return f'{self.runnable} of {self.target} runnable ideas{waits}'

    def __str__(self) -> str:
        coming = f', {self.pending} more from {self.sweeps} sweeps in flight' if self.sweeps else ''
        per_track = ''
        if len(self.tracks) > 1:
            per_track = ' (' + ', '.join(f'{t.name} {t.runnable} of {t.target}' for t in self.tracks) + ')'
        backlog = f'{self.held()}{per_track}{coming}'
        return f'{self.idle} free worker slots without a runnable idea; {backlog}' if self.idle > 0 else backlog


class Tick(Protocol):
    """What a trigger reads to decide. The scheduler makes one for each sweeping role of each project on each tick.

    A trigger reads with ``s`` and writes nothing. "The role's rows" are the ``workers`` rows of the role in the
    project. A scout sweep is a research row in scout mode (``piloop.agents.scout``).
    """

    @property
    def s(self) -> Session:
        """The tick's session."""
        ...

    @property
    def project(self) -> Project:
        """The project of the tick."""
        ...

    @property
    def role(self) -> str:
        """The name of the role: its ``[roles.<name>]`` table and ``workers.kind``."""
        ...

    @property
    def last(self) -> Worker | None:
        """The role's latest row that is not a scout sweep. None before the first."""
        ...

    @property
    def max_concurrent(self) -> int:
        """The role's ``max_concurrent``."""
        ...

    @property
    def interval(self) -> float:
        """The role's ``interval``: seconds between two routine sweeps of the ``cron`` trigger. 0 turns it off."""
        ...

    @property
    def scout_interval(self) -> float | None:
        """``[roles.research.scout] interval`` for the research role. None for other roles, or when scout mode is off."""
        ...

    def backlog(self) -> Backlog:
        """The project's backlog of ideas."""
        ...

    def incidents(self) -> list[Incident]:
        """The project's incidents that no agent has received yet, oldest first."""
        ...

    def live(self) -> int:
        """The number of the role's rows that are starting or running, scout sweeps left out."""
        ...

    def killed_streak(self) -> int:
        """The number of the role's latest rows, newest first and scout sweeps left out, that were killed in a row."""
        ...

    def empty_sweeps(self, states: tuple[WorkerState, ...]) -> list[Worker]:
        """The role's latest rows in ``states``, newest first and scout sweeps left out, until one that defined an idea.

        A row defined an idea if it inserted one or developed a seed into one (``piloop.campaign.ideas.defined_by``).
        """
        ...

    def latest_scout(self) -> Worker | None:
        """The role's latest scout sweep."""
        ...

    def routine(self) -> Worker | None:
        """The role's latest row that got no anomaly."""
        ...


def since(tick: Tick) -> datetime:
    """Return when the role's latest row started, or when the project was registered if the role has no row yet."""
    return tick.last.started_at if tick.last is not None else tick.project.created_at


@dataclass(frozen=True)
class Fire:
    """A trigger that fired: the reason for the request, and what the requested row gets."""

    reason: str
    """The ``workers.reason`` of the request. The agent's brief shows it."""
    count: int = 1
    """The number of rows to request. When several triggers fire together, the largest count wins."""
    alone: bool = False
    """The row is for this trigger alone, because it changes what the agent does: an incident sweep, a scout sweep.

    Such a fire is requested alone when it is the first fire of the tick. After another fire, it waits for a later
    tick.
    """
    events: tuple[tuple[str, dict[str, object]], ...] = ()
    """``(kind, payload)`` of the events to emit on the first requested row."""


class Trigger(BaseModel, ABC):
    """One entry of ``[roles.<name>] triggers``: ``{ kind = "<trigger>", <parameter> = <value> }``.

    The daemon checks the entries in the order of the list.
    """

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    ROLE: ClassVar[str | None] = None
    """The only role that may use the trigger, or None for any sweeping role."""

    kind: str
    """The name of the trigger: ``anomaly``, ``cron``, ``seed``, ``low_water``, ``scout``, ``sql``, or one that a
    package adds."""

    @abstractmethod
    def fire(self, tick: Tick) -> Fire | None:
        """Return the request that the trigger makes now, or None."""


class AnomalyTrigger(Trigger):
    """``anomaly``: an incident-class anomaly (``piloop.reports.overview``) that no agent of the project got yet.

    The row gets the new anomalies, one ``anomaly.reported`` event each. For the watchdog, that makes it an incident
    sweep.
    """

    kind: Literal['anomaly'] = 'anomaly'

    def fire(self, tick: Tick) -> Fire | None:
        incidents = tick.incidents()
        if not incidents:
            return None
        payloads: list[dict[str, object]] = [{'key': i.key, 'anomaly': i.line} for i in incidents]
        events = tuple((ANOMALY_EVENT, payload) for payload in payloads)
        return Fire('anomaly: ' + ', '.join(i.key for i in incidents), alone=True, events=events)


class CronTrigger(Trigger):
    """``cron``: the role's ``interval`` seconds since the start of its last routine row, one that got no anomaly.

    The first one counts from the registration of the project. An ``interval`` of 0 turns it off.
    """

    kind: Literal['cron'] = 'cron'

    def fire(self, tick: Tick) -> Fire | None:
        if tick.interval <= 0:
            return None
        routine = tick.routine() if tick.last is not None else None
        since = routine.started_at if routine is not None else tick.project.created_at
        if utcnow() - since < timedelta(seconds=tick.interval):
            return None
        return Fire(f'timer: {tick.interval:g}s')


class SeedTrigger(Trigger):
    """``seed``: a seed filed after the role's latest row started, while a slot of the role is free.

    It fires however full the backlog is. If the latest row got the seed but was killed, the seed is retried, once per
    batch, as ``low_water`` retries. A seed that a sweep read and did not develop is in the next sweep's brief,
    whatever trigger fires it. Gotcha: a sweep's brief is read a moment before its row's ``started_at`` is set, so a
    seed filed in that moment waits for the next sweep.
    """

    kind: Literal['seed'] = 'seed'

    def fire(self, tick: Tick) -> Fire | None:
        if tick.live() >= tick.max_concurrent:
            return None
        seeds = select(Idea).where(Idea.project_id == tick.project.id, Idea.status == IdeaStatus.SEED)
        waiting = count(tick.s, seeds)
        if not waiting:
            return None
        last = tick.last
        fresh = waiting if last is None else count(tick.s, seeds.where(Idea.created_at > last.started_at))
        if fresh:
            return Fire(f'{fresh} new seeds to develop')
        assert last is not None  # With no row yet, every waiting seed is fresh.
        if last.state is WorkerState.KILLED and tick.killed_streak() <= tick.max_concurrent:
            return Fire(f'{waiting} seeds to develop; retrying after worker#{last.id} was killed: {last.error}')
        return None


class LowWaterTrigger(Trigger):
    """``low_water``: the backlog has fewer ideas than its target, and there is a reason to sweep now.

    Only the research role uses it. The target is one idea per worker slot plus ``[roles.research] buffer``. So ideas
    exist before slots free up, and a free slot does not wait a whole sweep for an idea. The ideas that sweeps in
    flight can insert already count as backlog. Nothing is requested while every research slot is busy. Runnable
    ideas count. Ideas blocked only by prerequisites in flight also count, because they run when those finish. An idea
    blocked by a prerequisite that is not running does not count. Below the target, a sweep fires for one of these
    reasons:

    - The project has no sweep yet, so there is nothing to wait for.
    - The last batch was killed. Lost prompts, timeouts or a daemon stop say nothing about the backlog, so the batch
      is retried once. This applies to a streak of killed rows no longer than one batch (``max_concurrent``). A second
      killed batch is a systemic fault and counts as a normal last sweep.
    - New results: runs finished since the role's latest row started, which that sweep could not read. A sweep with
      nothing new to read reads the same history and proposes nothing. So each consecutive sweep that defined no idea
      doubles the number of finished runs the next sweep waits for. A sweep that defines an idea resets this. The
      wait is never longer than what can still come: with no run in flight, the finished runs are all there is.
    - The backlog is low. Research keeps sweeping while the backlog is short, not only when results come in or a slot
      is idle. A sweep that defined an idea is followed at once. After empty sweeps, research rests 2**n − 1 times as
      long as its latest ended sweep took. n is the number of consecutive ended sweeps, killed ones included, that
      defined no idea. An empty queue never deadlocks, and research that keeps finding nothing uses at most 1/2**n of
      the wall-clock time.

    It requests enough sweeps to fill the shortfall at ``max_ideas`` each, within the role's free slots.
    """

    ROLE: ClassVar[str | None] = WorkerKind.RESEARCH.value
    kind: Literal['low_water'] = 'low_water'

    def fire(self, tick: Tick) -> Fire | None:
        backlog = tick.backlog()
        if backlog.per_sweep == 0 or backlog.short <= 0 or backlog.sweeps >= tick.max_concurrent:
            return None
        batch = min(-(-backlog.short // backlog.per_sweep), tick.max_concurrent - backlog.sweeps)
        reason = self._reason(tick, backlog)
        return None if reason is None else Fire(reason, count=batch)

    def _reason(self, tick: Tick, backlog: Backlog) -> str | None:
        last = tick.last
        if last is None:
            return str(backlog)
        if last.state is WorkerState.KILLED and tick.killed_streak() <= tick.max_concurrent:
            return f'{backlog}; retrying after worker#{last.id} was killed: {last.error}'
        mine = project_runs(tick.project.id)
        finished = count(tick.s, mine.where(Run.finished_at > last.started_at))
        in_flight = count(tick.s, mine.where(Run.status.in_(OPEN_RUNS)))
        barren = len(tick.empty_sweeps((WorkerState.EXITED,)))
        if finished >= max(min(2**barren, finished + in_flight), 1):
            backoff = f' (after {barren} sweeps that defined no idea)' if barren else ''
            return f'{backlog}; {finished} runs finished since worker#{last.id} started{backoff}'
        empty = tick.empty_sweeps((WorkerState.EXITED, WorkerState.KILLED))
        if not empty:
            return str(backlog)
        latest = empty[0]
        assert latest.ended_at is not None  # An exited or killed row has ended.
        rest = (2 ** len(empty) - 1) * (latest.ended_at - latest.started_at)
        if utcnow() - latest.ended_at < rest:
            return None
        return f'{backlog}; rested {rest.total_seconds():.0f}s after {len(empty)} sweeps that defined no idea'


class ScoutTrigger(Trigger):
    """``scout``: ``[roles.research.scout] interval`` seconds since the last scout sweep started. Research only.

    It requests a scout sweep, a research row in scout mode (``piloop.agents.scout``). It does nothing while the
    ``[roles.research.scout]`` table is not there.
    """

    ROLE: ClassVar[str | None] = WorkerKind.RESEARCH.value
    kind: Literal['scout'] = 'scout'

    def fire(self, tick: Tick) -> Fire | None:
        if tick.scout_interval is None:
            return None
        last = tick.latest_scout()
        start = last.started_at if last is not None else tick.project.created_at
        if utcnow() - start < timedelta(seconds=tick.scout_interval):
            return None
        return Fire(f'scout: every {tick.scout_interval:g}s', alone=True, events=((SCOUT_EVENT, {}),))


SQL_PARAMETERS = ('project', 'since')


class SqlTrigger(Trigger):
    """``sql``: a query on the piloop database that returns at least one row.

    The query may use two parameters: ``:project``, the project's id, and ``:since``, when the role's latest row
    started (or when the project was registered), as UTC text (``2026-01-31 12:00:00.000000``). Compare ``:since`` with
    a ``created_at`` column to fire only on what is new. The query is checked when ``piloop.toml`` loads: it must be
    one statement that only reads.
    """

    kind: Literal['sql'] = 'sql'
    when: str
    """The query, for example ``SELECT 1 FROM candidates WHERE project_id = :project AND verdict = 'keep' AND
    created_at > :since``."""

    @field_validator('when')
    @classmethod
    def _reads_only(cls, when: str) -> str:
        if (error := sql_error(when, SQL_PARAMETERS)) is not None:
            raise ValueError(f'the query does not run on the piloop database: {error}')
        return when

    def fire(self, tick: Tick) -> Fire | None:
        params: dict[str, object] = {'project': tick.project.id, 'since': since(tick).strftime('%Y-%m-%d %H:%M:%S.%f')}
        row = tick.s.connection().exec_driver_sql(self.when, params).first()
        return None if row is None else Fire(f'sql: {" ".join(self.when.split())}')


READS = (sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION, sqlite3.SQLITE_RECURSIVE)


@cache
def sql_error(statement: str, parameters: tuple[str, ...] = ()) -> str | None:
    """Return why ``statement`` is not one query that reads the piloop schema, or None when it is.

    The statement is prepared on an empty copy of the schema, under an authorizer that denies every write. It may use
    the named ``parameters`` and no others.
    """

    def authorize(action: int, *_: str | None) -> int:
        return sqlite3.SQLITE_OK if action in READS else sqlite3.SQLITE_DENY

    con = sqlite3.connect(':memory:')
    try:
        con.executescript(schema_ddl())
        con.set_authorizer(authorize)
        con.execute(f'EXPLAIN {statement}', dict.fromkeys(parameters, 0))
    except sqlite3.Error as e:
        return str(e)
    finally:
        con.close()
    return None


BUILTIN_TRIGGERS: dict[str, type[Trigger]] = {
    'anomaly': AnomalyTrigger,
    'cron': CronTrigger,
    'seed': SeedTrigger,
    'low_water': LowWaterTrigger,
    'scout': ScoutTrigger,
    'sql': SqlTrigger,
}


@cache
def triggers() -> dict[str, type[Trigger]]:
    """Return every trigger by name: the built-in ones, then the ones that packages add (``TRIGGER_GROUP``).

    Raises:
        TypeError: An entry point does not name a ``Trigger`` subclass.
        ValueError: An entry point reuses the name of a built-in trigger.
    """
    found = dict(BUILTIN_TRIGGERS)
    for ep in entry_points(group=TRIGGER_GROUP):
        loaded = ep.load()
        if not (isinstance(loaded, type) and issubclass(loaded, Trigger)):
            raise TypeError(f'entry point {ep.name} = {ep.value} in {TRIGGER_GROUP} is not a piloop Trigger class')
        if ep.name in found:
            raise ValueError(f'entry point {ep.name} = {ep.value} in {TRIGGER_GROUP}: {ep.name!r} is a trigger already')
        found[ep.name] = loaded
    return found


def _parse(entry: object) -> object:
    """Validate a ``triggers`` entry as the trigger that its ``kind`` names."""
    if not isinstance(entry, dict):
        return entry
    kind = entry.get('kind')
    known = triggers()
    if not isinstance(kind, str) or kind not in known:
        raise ValueError(f'unknown trigger kind {kind!r}; known: {", ".join(known)}')
    return known[kind].model_validate(entry)


TriggerEntry = Annotated[SerializeAsAny[Trigger], BeforeValidator(_parse)]
"""The type of one ``triggers`` entry: the model of the trigger that its ``kind`` names, dumped with its parameters."""
