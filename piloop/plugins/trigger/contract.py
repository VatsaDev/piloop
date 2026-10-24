"""The triggers of the roles: the conditions that make the daemon request an agent (https://piloop.autoresearch.inc/concepts/roles#triggers).

A trigger is a provider of ``TRIGGERS``: its fields are its parameters, from its entry in ``[roles.<name>] triggers``. On each
tick, for each campaign and each sweeping role without a pending request, the scheduler calls ``Trigger.fire`` of each
trigger of the role, in the order of the list. On the tick that stops the campaign, it calls only the triggers with
``ON_STOP``, and while the campaign runs, only the others. A package adds a trigger with an entry point in the group of ``TRIGGERS``.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, ClassVar, NamedTuple, Protocol

from sqlalchemy.orm import Session

from piloop.db.models import AgentSession, AgentSessionState, Campaign, EventModel
from piloop.plugins.host import Capability, Parameters


class Incident(NamedTuple):
    """An incident-class anomaly that no agent has received yet."""

    name: str
    """The anomaly in a few words: ``run#36 failed``."""
    event: EventModel
    """The event that gives it to the agent that the trigger requests."""


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

    def stall_step(self, before: datetime | None, /) -> int:
        """The runs without a keep over the stall mark (``piloop.campaign.stop.stall``), rounded down.

        It is 0 until the campaign stalls, 1 from the mark, 2 from twice the mark, and so on. A keep resets it. With
        ``before``, only the runs that finished by then count.
        """
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

    def cancel(self, agent_session: AgentSession, /) -> EventModel | None:
        """The latest request to cancel the row (``piloop agent cancel``), or None. A cancelled row ends killed."""
        ...

    def empty_sweeps(self, states: tuple[AgentSessionState, ...]) -> list[AgentSession]:
        """The role's latest rows in ``states``, newest first, until one that defined an idea.

        A row defined an idea if it inserted one or developed a seed into one (``piloop.campaign.ideas.defined_by``).
        """
        ...

    def routine(self) -> AgentSession | None:
        """The role's latest row that got no anomaly."""
        ...


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
    events: tuple[EventModel, ...] = ()
    """The events to emit on the first requested row."""


class Trigger(Parameters, ABC):
    """One entry of ``[roles.<name>] triggers``: ``{ provider = "<trigger>", <parameter> = <value> }``.

    The daemon checks the entries in the order of the list.
    """

    ON_STOP: ClassVar[bool] = False
    """The daemon checks the trigger once, on the tick that stops the campaign, instead of on every tick while it runs."""
    INVESTIGATES: ClassVar[bool] = False
    """The agent that the trigger requests gets incidents to investigate. An agent of a role with such a trigger that
    dies at startup is then no incident: the agent that it would request could die the same way, and so on."""

    @abstractmethod
    def fire(self, tick: Tick) -> Fire | None:
        """Return the request that the trigger makes now, or None."""


TRIGGERS = Capability('trigger', Trigger)
"""Every trigger by name. The roles name their triggers in their entries, so no single key selects one."""

TriggerEntry = Annotated[Trigger, TRIGGERS.entry]
"""The type of one ``triggers`` entry: the trigger that its ``provider`` names, with its parameters."""
