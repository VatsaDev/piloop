"""The campaign type capability: what one kind of campaign stores, how it runs and what it shows.

``campaigns.type`` names the type of a campaign. Every installed type's schema is migrated. ``optimization``, built in,
is the research model: ideas, runs, candidates and their evaluations. In the daemon, a type plans its campaigns through
the hooks of its ``Planner``, which the campaigns' pass calls in one transaction.
"""

from abc import ABC, abstractmethod
from collections.abc import Callable, Collection
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Final, Protocol

from piloop.plugins.host import Capability, Parameters

if TYPE_CHECKING:  # piloop.config imports the capabilities, and the database imports piloop.config
    from sqlalchemy.orm import Session

    from piloop.daemon.run_controller import RunController
    from piloop.db.models import AgentSession, Campaign, RunEnd
    from piloop.ids import AgentSessionId, RoleName, TargetRef
    from piloop.otel import Tracer
    from piloop.plugins.fact_source.facts import Facts
    from piloop.plugins.trigger.contract import Backlog


CAMPAIGNS: Final = 'campaigns'
"""The name of the campaigns' pass in the daemon's loop, which a type's thread that ends wakes."""


class Take(StrEnum):
    """What becomes of a requested agent of a type's own when the pass has a slot for it."""

    LAUNCH = 'launch'
    WAIT = 'wait'
    """It stays requested: for now it cannot launch."""


@dataclass(frozen=True)
class CannotLaunch:
    """It never can launch: the pass closes it with ``reason``."""

    reason: str


@dataclass(frozen=True)
class TypeHost:
    """What the daemon gives a campaign type's ``Planner``."""

    tracer: Tracer
    home: Path
    controller: RunController
    submit: Callable[[Callable[[], None]], Future[None]]
    """Runs a body in a thread of the daemon. Its end wakes the campaigns' pass."""


@dataclass(frozen=True)
class Steering:
    """What else an agent of a type's own work is steered by, beside the comments on itself."""

    targets: tuple[TargetRef, ...]
    """The rows whose comments it gets too, such as its run and its idea."""
    since: datetime
    """It gets their comments written since then: its brief has the older ones."""
    signatures: tuple[str, ...]
    """Its own other names: their comments are its own, and never go back to it."""


class Planner(Protocol):
    """A campaign type at work in one daemon process: what the campaigns' pass asks of it."""

    def tidy(self, s: Session, campaign: Campaign) -> None:
        """Settle what changed before the stop rule reads the campaign."""
        ...

    def remeasure(self, s: Session, campaign: Campaign) -> None:
        """Measure what must be measured again, unless the stop rule holds the campaign or it is paused."""
        ...

    def queue(self, s: Session, campaign: Campaign, left: Facts) -> None:
        """Request the agents of its own work, within what the stop rule ``left``, before the triggers fire."""
        ...

    def drain(self, s: Session, campaign: Campaign) -> None:
        """End the queued work of a campaign that stops."""
        ...

    def ended(self, s: Session, agent_session: AgentSession, end: RunEnd) -> None:
        """An agent that never started closed, for ``end``: end what it was requested for."""
        ...

    def take(self, s: Session, agent_session: AgentSession, rotation: list[str], full: set[str]) -> Take | CannotLaunch:
        """Whether a requested agent of a role that does its work launches now."""
        ...

    def fail_open(self, s: Session, end: RunEnd, *, reaped: bool, sparing: Collection[AgentSessionId]) -> None:
        """End its work in flight as ``end``.

        The daemon stops, or a previous one left the work (``reaped``). The agents of ``sparing`` go on.
        """
        ...

    def backlog(self, s: Session, campaign: Campaign, name: RoleName, left: Facts, buffer: int) -> Backlog:
        """The campaign's backlog as the sweeping role ``name`` fills it (``Pass.backlog``)."""
        ...

    def steered_by(self, s: Session, agent_session: AgentSession) -> Steering | None:
        """What else the agent is steered by, or None for an agent of no work of the type's."""
        ...

    def found_at(self, s: Session, agent_session: AgentSession) -> datetime | None:
        """When the agent left its finding, if its work takes one: the daemon ends it a grace later."""
        ...

    def resume_unscored(self) -> None:
        """Take up what a previous daemon process left half done. The daemon calls it when it starts."""
        ...

    def in_flight(self) -> dict[str, Future[None]]:
        """Its threads in flight, by what they work on: a stop waits for them."""
        ...


class CampaignType(Parameters, ABC):
    """A kind of campaign. ``campaigns.type`` names it."""

    @abstractmethod
    def planner(self, host: TypeHost) -> Planner:
        """The type at work in the daemon process that ``host`` serves. The daemon asks once."""


CAMPAIGN_TYPES = Capability('campaign_type', CampaignType, noun='campaign type')
"""Every campaign type by name. No key selects one: each campaign names its own."""
