"""What the ``optimization`` campaign type runs in the daemon: its evaluation queue."""

from datetime import datetime

from piloop.daemon.loop import Consumer, Kinds, Phase, Turn
from piloop.db.triggers.changelog import EvaluationCancelled
from piloop.plugins.contribution import Contribution, DaemonHost
from piloop.plugins.executor.consumers import LIMITS
from piloop.scoring.ledger import MeasurementRecorded

EVALUATIONS: Kinds = frozenset({EvaluationCancelled, MeasurementRecorded, *LIMITS})
"""What wakes the evaluation queue: a cancel to stop, a slot that an evaluation freed, or more slots. The executor's
``capacity_events`` say that it has more slots too."""


def contribution(host: DaemonHost) -> Contribution:
    """The consumer ``evaluations``.

    It stops the evaluations that someone cancelled, and submits those that a slot freed for.
    """

    def evaluations(_turn: Turn) -> datetime | None:
        host.controller.stop_cancelled()
        host.controller.dispatch()
        return None

    wakes = EVALUATIONS | host.controller.executor.capacity_events()
    return Contribution(consumers=(Consumer('evaluations', wakes, evaluations, Phase.UPKEEP),))
