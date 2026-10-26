"""What a provider or a capability adds to core while the daemon runs: its ``Contribution``.

The daemon asks each provider that it opened once, in the order that it opened them, then each capability that runs
machinery of its own (``CONTRIBUTORS``), then each installed provider of a capability that a campaign chooses, built
with its defaults (``installed``). It hands the result to its loop and its server (``collect``). What a package adds
before anything opens, it declares in a ``Slot`` (``piloop.plugins.host``).

Inspiration: Backstage's backend plugins, which register their routers, scheduled tasks and startup and shutdown hooks
with the core services.
"""

from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from fastapi import APIRouter
from pydantic import TypeAdapter, ValidationError

from piloop.daemon.loop import Consumer
from piloop.plugins.host import PluginError, Scope, Slot, declared
from piloop.settings import AgentSettings, DaemonSettings

if TYPE_CHECKING:  # the run controller and the database import the contracts that import this module
    from piloop.daemon.run_controller import RunController
    from piloop.db.session import Database
    from piloop.otel import Tracer
    from piloop.plugins.campaign_type.contract import Planner
    from piloop.runtime.factory import RuntimeFactory


class Leaving(StrEnum):
    """How the daemon stops."""

    STOP = 'stop'
    """For good: what is still open fails."""
    HAND_OVER = 'hand_over'
    """For an in-place restart: nothing is written, and the next daemon process adopts what runs."""


class Lifecycle(Protocol):
    """What a contribution does when the daemon starts and stops, beside its consumers."""

    def start(self) -> None:
        """Take over what a previous daemon process left. The daemon calls it before its loop's first turn."""
        ...

    def in_flight(self) -> Mapping[str, Future[None]]:
        """Its threads in flight, by what they work on: a stop waits for them."""
        ...

    def stop(self, leaving: Leaving, timeout: float) -> None:
        """Start nothing more, and wait up to ``timeout`` seconds for what is in flight. The executor stopped first."""
        ...


@dataclass(frozen=True)
class DaemonHost:
    """What the daemon gives the machinery of a capability (``CONTRIBUTORS``)."""

    db: Database
    tracer: Tracer
    home: Path
    controller: RunController
    """The daemon's executor, launcher, sources and CAS, and the runs that it executes."""
    runtime_factory: RuntimeFactory
    now: Callable[[], datetime]
    """The clock."""
    wake: Callable[[str], None]
    """Runs the consumer of this name in the next turn of the loop: a thread of the daemon ended (``Loop.wake``)."""
    daemon: DaemonSettings
    agents: AgentSettings
    planners: Mapping[str, Planner]
    """Each installed campaign type at work in this daemon, by its name (``campaigns.type``)."""
    thread: Callable[[Callable[[], None]], Future[None]]
    """Runs a body in a thread of the daemon."""


@dataclass(frozen=True)
class Contribution:
    """What one provider adds while the daemon runs."""

    consumers: tuple[Consumer, ...] = ()
    """Jobs of the daemon's loop. Each names its ``Phase``: within one, a turn runs them in the order of their
    contributions."""
    router: APIRouter | None = None
    """Routes of the daemon's server, beside the API's. A route of a method and path that is taken is refused. Only a
    provider adds routes: the API's document lists them without a daemon."""
    lifecycle: Lifecycle | None = None
    """What it does when the daemon starts and stops."""


@runtime_checkable
class Contributes(Protocol):
    """A provider that adds to core."""

    def contribution(self) -> Contribution:
        """What it adds to core. The daemon asks once, after it opened the provider."""
        ...


@dataclass(frozen=True)
class Contributions:
    """Every contribution of a daemon process, in the order that it opened their providers."""

    consumers: tuple[Consumer, ...] = ()
    routers: tuple[APIRouter, ...] = ()
    lifecycles: tuple[Lifecycle, ...] = ()


type Machinery = Callable[[DaemonHost], Contribution]
"""What a capability runs itself, beside its providers: the reader of its providers, say. It adds no routes."""


CONTRIBUTORS: Slot[Machinery] = Slot('contributors', TypeAdapter(Machinery), 'contributor')
"""What runs in the daemon for a capability, by the capability's name, or for a provider that needs the daemon's host,
by the provider's name: entry points ``piloop.contributors``."""


def installed() -> tuple[Contributes, ...]:
    """Each installed provider that ``Contributes`` of a capability that a campaign chooses, or that no table chooses.

    Each is built with its defaults, in registry order: by capability, then by provider.

    Raises:
        PluginError: A provider that contributes cannot be built with its defaults. The message names it.
    """
    found: list[Contributes] = []
    for capability in declared():
        if capability.choice is not None and capability.choice.scope is not Scope.CAMPAIGN:
            continue
        for name, provider in capability.providers().items():
            if not issubclass(provider.model, Contributes):
                continue
            try:
                built = provider.model()
            except ValidationError as e:
                errors = '; '.join(f'{".".join(map(str, err["loc"]))}: {err["msg"]}' for err in e.errors())
                raise PluginError(
                    f'the {capability.noun} {name!r} contributes, and cannot be built with its defaults: {errors}'
                ) from e
            found.append(built)
    return tuple(found)


def contributions_of(providers: Sequence[Contributes]) -> list[tuple[str, Contribution]]:
    """Ask each of ``providers`` for its contribution, by the name of its class."""
    return [(type(p).__qualname__, p.contribution()) for p in providers]


def collect(contributions: Sequence[tuple[str, Contribution]], taken: Sequence[str]) -> Contributions:
    """Join ``contributions``, by their owners' names, and refuse a consumer name that ``taken`` or another has.

    Raises:
        PluginError: Two consumers have one name, or a capability's machinery adds routes. The message names both sides.
    """
    owners = dict.fromkeys(taken, 'the daemon')
    consumers: list[Consumer] = []
    routers: list[APIRouter] = []
    lifecycles: list[Lifecycle] = []
    for owner, contribution in contributions:
        routers += [contribution.router] if contribution.router is not None else []
        lifecycles += [contribution.lifecycle] if contribution.lifecycle is not None else []
        for consumer in contribution.consumers:
            if (other := owners.get(consumer.name)) is not None:
                raise PluginError(f'{owner} contributes the consumer {consumer.name!r}, which {other} has already')
            owners[consumer.name] = owner
            consumers.append(consumer)
    return Contributions(tuple(consumers), tuple(routers), tuple(lifecycles))


def machinery(host: DaemonHost) -> list[tuple[str, Contribution]]:
    """Ask the machinery of each capability (``CONTRIBUTORS``) for its contribution, by the capability's name.

    Raises:
        PluginError: One adds routes.
    """
    found = [(name, make(host)) for name, make in CONTRIBUTORS.loaded().items()]
    if routed := [name for name, contribution in found if contribution.router is not None]:
        raise PluginError(f'the machinery of {", ".join(routed)} adds routes: only a provider adds routes')
    return found
