"""What a provider that the daemon opened adds to core beside its capability's contract: its ``Contribution``.

The daemon asks each provider that it opened once (``collect``), in the order that it opened them, and hands the result
to its loop and its server. The daemon opens no provider that a campaign chooses: each installed one adds its
contribution once, built with its defaults (``installed``). What a package adds before anything opens, it declares in a
``Slot`` (``piloop.plugins.host``).

Inspiration: Backstage's backend plugins, which register their routers and scheduled tasks with the core services.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from fastapi import APIRouter
from pydantic import ValidationError

from piloop.daemon.loop import Consumer
from piloop.plugins.host import PluginError, Scope, declared


@dataclass(frozen=True)
class Contribution:
    """What one provider adds while the daemon runs."""

    consumers: tuple[Consumer, ...] = ()
    """Jobs of the daemon's loop. A turn runs them after ``limits`` and before the scheduler's own."""
    router: APIRouter | None = None
    """Routes of the daemon's server, beside the API's. A route of a method and path that is taken is refused."""


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


def collect(contributors: Sequence[Contributes], taken: Sequence[str]) -> Contributions:
    """Ask each of ``contributors`` for its contribution, and refuse a consumer name that ``taken`` or another has.

    Raises:
        PluginError: Two consumers have one name. The message names both sides.
    """
    owners = dict.fromkeys(taken, 'the daemon')
    consumers: list[Consumer] = []
    routers: list[APIRouter] = []
    for contributor in contributors:
        owner = type(contributor).__qualname__
        contribution = contributor.contribution()
        routers += [contribution.router] if contribution.router is not None else []
        for consumer in contribution.consumers:
            if (other := owners.get(consumer.name)) is not None:
                raise PluginError(f'{owner} contributes the consumer {consumer.name!r}, which {other} has already')
            owners[consumer.name] = owner
            consumers.append(consumer)
    return Contributions(tuple(consumers), tuple(routers))
