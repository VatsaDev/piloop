"""What a provider that the daemon opened adds to the kernel beside its capability's contract: its ``Contribution``.

The daemon asks each provider that it opened once (``collect``), in the order that it opened them, and hands the result
to its loop and its server. What a package adds before anything opens, it declares in a ``Slot``
(``piloop.plugins.host``).

Inspiration: Backstage's backend plugins, which register their routers and scheduled tasks with the core services.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from fastapi import APIRouter

from piloop.daemon.loop import Consumer
from piloop.plugins.host import PluginError


@dataclass(frozen=True)
class Contribution:
    """What one provider adds while the daemon runs."""

    consumers: tuple[Consumer, ...] = ()
    """Jobs of the daemon's loop. A turn runs them after ``limits`` and before the scheduler's own."""
    router: APIRouter | None = None
    """Routes of the daemon's server, beside the API's. A route of a method and path that is taken is refused."""


class Contributes(Protocol):
    """A provider that adds to the kernel: the executor."""

    def contribution(self) -> Contribution:
        """What it adds to the kernel. The daemon asks once, after it opened the provider."""
        ...


@dataclass(frozen=True)
class Contributions:
    """Every contribution of a daemon process, in the order that it opened their providers."""

    consumers: tuple[Consumer, ...] = ()
    routers: tuple[APIRouter, ...] = ()


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
