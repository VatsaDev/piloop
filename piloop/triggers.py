"""The triggers of the roles: the conditions that make the daemon request an agent (https://piloop.autoresearch.inc/concepts/roles#triggers).

A trigger is a pydantic model. Its fields are its parameters, from its entry in ``[roles.<name>] triggers``. On each
tick, for each campaign and each sweeping role without a pending request, the scheduler calls ``Trigger.fire`` of each
trigger of the role, in the order of the list. On the tick that stops the campaign, it calls only the triggers with
``ON_STOP``, and while the campaign runs, only the others. A package adds a trigger with an entry point in the group of ``TRIGGERS``.
"""

import sqlite3
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import cache
from typing import Annotated, ClassVar, Literal, NamedTuple, Protocol

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, SerializeAsAny, field_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.db.events import ANOMALY_EVENT
from piloop.db.models import (
    OPEN_RUNS,
    AgentSession,
    AgentSessionState,
    Campaign,
    Idea,
    IdeaStatus,
    Run,
    campaign_runs,
    count,
    utcnow,
)
from piloop.db.sql import schema_ddl, with_renamed
from piloop.plugins.host import Capability


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
    """One idea per run slot plus the ``buffer`` of ``low_water``, but never more than ``[stop] max_runs`` allows."""
    free: int
    """The runs that the campaign can start now."""
    sweeps: int
    """The role's sweeps in flight."""
    per_sweep: int
    """The ideas that one sweep may insert: the role's ``max_ideas``."""
    on_hold: bool = False
    """The queue holds the unpinned ideas until the head has a score from the current evaluator
    (``ledger.head_unmeasured``)."""

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
        """The free implementer slots that no runnable idea and no sweep in flight will fill."""
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
        return f'{self.idle} free implementer slots without a runnable idea; {backlog}' if self.idle > 0 else backlog


class Tick(Protocol):
    """What a trigger reads to decide. The scheduler makes one for each sweeping role of each campaign on each tick.

    A trigger reads with ``s`` and writes nothing. "The role's rows" are the ``agent_sessions`` rows of the role in the
    campaign.
    """

    @property
    def s(self) -> Session:
        """The tick's session."""
        ...

    @property
    def campaign(self) -> Campaign:
        """The campaign of the tick."""
        ...

    @property
    def role(self) -> str:
        """The name of the role: its ``[roles.<name>]`` table and ``agent_sessions.kind``."""
        ...

    @property
    def last(self) -> AgentSession | None:
        """The role's latest row. None before the first."""
        ...

    @property
    def max_concurrent(self) -> int:
        """The role's ``max_concurrent``."""
        ...

    @property
    def interval(self) -> float:
        """The role's ``interval``: seconds between two routine sweeps of the ``cron`` trigger. 0 turns it off."""
        ...

    def backlog(self, buffer: int, /) -> Backlog:
        """The campaign's backlog of ideas, with ``buffer`` ideas beyond one per run slot, as the role's sweeps fill it."""
        ...

    def incidents(self) -> list[Incident]:
        """The campaign's incidents that no agent has received yet, oldest first."""
        ...

    def live(self) -> int:
        """The number of the role's rows that are starting or running."""
        ...

    def killed_streak(self) -> int:
        """The number of the role's latest rows, newest first, that were killed in a row."""
        ...

    def empty_sweeps(self, states: tuple[AgentSessionState, ...]) -> list[AgentSession]:
        """The role's latest rows in ``states``, newest first, until one that defined an idea.

        A row defined an idea if it inserted one or developed a seed into one (``piloop.campaign.ideas.defined_by``).
        """
        ...

    def routine(self) -> AgentSession | None:
        """The role's latest row that got no anomaly."""
        ...


def since(tick: Tick) -> datetime:
    """Return when the role's latest row started, or when the campaign was registered if the role has no row yet."""
    return tick.last.started_at if tick.last is not None else tick.campaign.created_at


@dataclass(frozen=True)
class Fire:
    """A trigger that fired: the reason for the request, and what the requested row gets."""

    reason: str
    """The ``agent_sessions.reason`` of the request. The agent's brief shows it."""
    count: int = 1
    """The number of rows to request. When several triggers fire together, the largest count wins."""
    alone: bool = False
    """The row is for this trigger alone, because it changes what the agent does: an incident sweep.

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

    ON_STOP: ClassVar[bool] = False
    """The daemon checks the trigger once, on the tick that stops the campaign, instead of on every tick while it runs."""
    INVESTIGATES: ClassVar[bool] = False
    """The agent that the trigger requests gets incidents to investigate. An agent of a role with such a trigger that
    dies at startup is then no incident: the agent that it would request could die the same way, and so on."""

    kind: str
    """The name of the trigger. A package can add triggers."""

    @abstractmethod
    def fire(self, tick: Tick) -> Fire | None:
        """Return the request that the trigger makes now, or None."""


class AnomalyTrigger(Trigger):
    """The daemon detected an incident that no agent of the campaign got yet. The agent gets it to investigate.

    Incidents are the incident-class anomalies of ``piloop.reports.overview``. The row gets the new anomalies, one ``anomaly.reported`` event each. For the watchdog, that makes it an incident
    sweep.
    """

    INVESTIGATES: ClassVar[bool] = True
    kind: Literal['anomaly'] = 'anomaly'

    def fire(self, tick: Tick) -> Fire | None:
        incidents = tick.incidents()
        if not incidents:
            return None
        payloads: list[dict[str, object]] = [{'key': i.key, 'anomaly': i.line} for i in incidents]
        events = tuple((ANOMALY_EVENT, payload) for payload in payloads)
        return Fire('anomaly: ' + ', '.join(i.key for i in incidents), alone=True, events=events)


class CronTrigger(Trigger):
    """The role's ``interval`` passed since the start of its last routine agent, one that got no anomaly.

    The first one counts from the registration of the campaign. An ``interval`` of 0 turns it off.
    """

    kind: Literal['cron'] = 'cron'

    def fire(self, tick: Tick) -> Fire | None:
        if tick.interval <= 0:
            return None
        routine = tick.routine() if tick.last is not None else None
        since = routine.started_at if routine is not None else tick.campaign.created_at
        if utcnow() - since < timedelta(seconds=tick.interval):
            return None
        return Fire(f'timer: {tick.interval:g}s')


class SeedTrigger(Trigger):
    """A seed was filed after the role's latest agent started, and a slot of the role is free.

    It fires however full the backlog is. If the latest row got the seed but was killed, the seed is retried, once per
    batch, as ``low_water`` retries. A seed that a sweep read and did not develop is in the next sweep's brief,
    whatever trigger fires it. Gotcha: a sweep's brief is read a moment before its row's ``started_at`` is set, so a
    seed filed in that moment waits for the next sweep.
    """

    kind: Literal['seed'] = 'seed'

    def fire(self, tick: Tick) -> Fire | None:
        if tick.live() >= tick.max_concurrent:
            return None
        seeds = select(Idea).where(Idea.campaign_id == tick.campaign.id, Idea.status == IdeaStatus.SEED)
        waiting = count(tick.s, seeds)
        if not waiting:
            return None
        last = tick.last
        fresh = waiting if last is None else count(tick.s, seeds.where(Idea.created_at > last.started_at))
        if fresh:
            return Fire(f'{fresh} new seeds to develop')
        assert last is not None  # With no row yet, every waiting seed is fresh.
        if last.state is AgentSessionState.KILLED and tick.killed_streak() <= tick.max_concurrent:
            return Fire(f'{waiting} seeds to develop; retrying after agent_session#{last.id} was killed: {last.error}')
        return None


class LowWaterTrigger(Trigger):
    """The backlog of runnable ideas is shorter than its target, and there is a reason to sweep now.

    The target is one idea per run slot plus ``buffer``. So ideas exist before slots free up, and a free slot does not
    wait a whole sweep for an idea. The ideas that the role's sweeps in flight can insert already count as backlog.
    Nothing is requested while every slot of the role is busy. Runnable ideas count. Ideas blocked only by
    prerequisites in flight also count, because they run when those finish. An idea blocked by a prerequisite that is
    not running does not count. Below the target, a sweep fires for one of these reasons:

    - The campaign has no sweep yet, so there is nothing to wait for.
    - The last batch was killed. Lost prompts, timeouts or a daemon stop say nothing about the backlog, so the batch
      is retried once. This applies to a streak of killed rows no longer than one batch (``max_concurrent``). A second
      killed batch is a systemic fault and counts as a normal last sweep.
    - New results: runs finished since the role's latest row started, which that sweep could not read. A sweep with
      nothing new to read reads the same history and proposes nothing. So each consecutive sweep that defined no idea
      doubles the number of finished runs the next sweep waits for. A sweep that defines an idea resets this. The
      wait is never longer than what can still come: with no run in flight, the finished runs are all there is.
    - The backlog is low. The role keeps sweeping while the backlog is short, not only when results come in or a slot
      is idle. A sweep that defined an idea is followed at once. After empty sweeps, the role rests 2**n − 1 times as
      long as its latest ended sweep took. n is the number of consecutive ended sweeps, killed ones included, that
      defined no idea. An empty queue never deadlocks, and a role that keeps finding nothing uses at most 1/2**n of
      the wall-clock time.

    It requests enough sweeps to fill the shortfall at the role's ``max_ideas`` each, within the role's free slots.
    Nothing is requested while the queue holds runnable ideas for the head to be measured again (``Backlog.on_hold``):
    no idea that a sweep adds could run sooner.
    """

    kind: Literal['low_water'] = 'low_water'
    buffer: int = Field(default=2, ge=0)
    """The number of ready ideas to keep in addition to one for each run slot (the ``max_concurrent`` of every role
    that does runs). With this backlog, a free slot finds an idea at once and does not wait for a sweep. A larger value
    makes implementers wait less for the role, but the ideas they run are older."""

    def fire(self, tick: Tick) -> Fire | None:
        backlog = tick.backlog(self.buffer)
        if backlog.per_sweep == 0 or backlog.short <= 0 or backlog.sweeps >= tick.max_concurrent:
            return None
        if backlog.on_hold and backlog.runnable:
            return None
        batch = min(-(-backlog.short // backlog.per_sweep), tick.max_concurrent - backlog.sweeps)
        reason = self._reason(tick, backlog)
        return None if reason is None else Fire(reason, count=batch)

    def _reason(self, tick: Tick, backlog: Backlog) -> str | None:
        last = tick.last
        if last is None:
            return str(backlog)
        if last.state is AgentSessionState.KILLED and tick.killed_streak() <= tick.max_concurrent:
            return f'{backlog}; retrying after agent_session#{last.id} was killed: {last.error}'
        mine = campaign_runs(tick.campaign.id)
        finished = count(tick.s, mine.where(Run.finished_at > last.started_at))
        in_flight = count(tick.s, mine.where(Run.status.in_(OPEN_RUNS)))
        barren = len(tick.empty_sweeps((AgentSessionState.EXITED,)))
        if finished >= max(min(2**barren, finished + in_flight), 1):
            backoff = f' (after {barren} sweeps that defined no idea)' if barren else ''
            return f'{backlog}; {finished} runs finished since agent_session#{last.id} started{backoff}'
        empty = tick.empty_sweeps((AgentSessionState.EXITED, AgentSessionState.KILLED))
        if not empty:
            return str(backlog)
        latest = empty[0]
        assert latest.ended_at is not None  # An exited or killed row has ended.
        rest = (2 ** len(empty) - 1) * (latest.ended_at - latest.started_at)
        if utcnow() - latest.ended_at < rest:
            return None
        return f'{backlog}; rested {rest.total_seconds():.0f}s after {len(empty)} sweeps that defined no idea'


class StoppedTrigger(Trigger):
    """The campaign stopped (``[stop]``). It fires once, on the tick that stops the campaign.

    The daemon checks it after it dropped what was pending, so its row starts. The watchdog's closing review is this
    trigger. The reason is ``campaign stopped: <the condition>``.
    """

    ON_STOP: ClassVar[bool] = True
    kind: Literal['stopped'] = 'stopped'

    def fire(self, tick: Tick) -> Fire | None:
        return Fire(f'campaign stopped: {tick.campaign.stopped_reason}')


SQL_PARAMETERS = ('campaign', 'since')


class SqlTrigger(Trigger):
    """A query on the piloop database returns at least one row.

    The query may use two parameters: ``:campaign``, the campaign's id, and ``:since``, when the role's latest row
    started (or when the campaign was registered), as UTC text (``2026-01-31 12:00:00.000000``). Compare ``:since`` with
    a ``created_at`` column to fire only on what is new. The query is checked when ``piloop.toml`` loads: it must be
    one statement that only reads.
    """

    kind: Literal['sql'] = 'sql'
    when: str
    """The query, for example ``SELECT 1 FROM candidates WHERE campaign_id = :campaign AND verdict = 'keep' AND
    created_at > :since``."""

    @field_validator('when')
    @classmethod
    def _reads_only(cls, when: str) -> str:
        if (error := sql_error(when, SQL_PARAMETERS)) is not None:
            raise ValueError(f'the query does not run on the piloop database: {error}')
        return when

    def fire(self, tick: Tick) -> Fire | None:
        params: dict[str, object] = {
            'campaign': tick.campaign.id,
            'since': since(tick).strftime('%Y-%m-%d %H:%M:%S.%f'),
        }
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
        return with_renamed(str(e))
    finally:
        con.close()
    return None


def _check(name: str, loaded: object) -> type[Trigger]:
    if isinstance(loaded, type) and issubclass(loaded, Trigger):
        return loaded
    raise TypeError('is not a piloop Trigger class')


TRIGGERS = Capability('trigger', group='piloop.triggers', check=_check)
"""Every trigger by kind. Each entry point of the group names a ``Trigger`` subclass, and its name is the ``kind``.
The roles name triggers by kind, so no single key selects one."""


def _parse(entry: object) -> object:
    """Validate a ``triggers`` entry as the trigger that its ``kind`` names."""
    if not isinstance(entry, dict):
        return entry
    return TRIGGERS.get(str(entry.get('kind'))).model_validate(entry)


TriggerEntry = Annotated[SerializeAsAny[Trigger], BeforeValidator(_parse)]
"""The type of one ``triggers`` entry: the model of the trigger that its ``kind`` names, dumped with its parameters."""
