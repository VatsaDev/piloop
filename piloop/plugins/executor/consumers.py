"""The executor capability's machinery: it hands the daemon's executor the live limits (``piloop limits``)."""

from datetime import datetime

from piloop.daemon.loop import Consumer, Kinds, Phase, Turn
from piloop.db.triggers.changelog import LimitSet, LimitUnset
from piloop.plugins.contribution import Contribution, DaemonHost

LIMITS: Kinds = frozenset({LimitSet, LimitUnset})
"""A live limit that changes: it can give the executor more slots. Its own events say so too (``capacity_events``)."""


def contribution(host: DaemonHost) -> Contribution:
    """The consumer ``limits``, first in a turn: every other one sees the executor with the live limits."""

    def limits(_turn: Turn) -> datetime | None:
        host.controller.configure()
        return None

    return Contribution(consumers=(Consumer('limits', LIMITS, limits, Phase.CONFIGURE),))
