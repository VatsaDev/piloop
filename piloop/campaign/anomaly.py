"""The kinds of anomaly that the daemon detects (``piloop.reports.overview``), and the event that gives one to an agent."""

from enum import StrEnum
from typing import Literal

from piloop.db.models import EventModel
from piloop.ids import Ref


class AnomalyKind(StrEnum):
    """A kind of anomaly that the daemon detects. The value follows the row reference: ``run#36 failed``."""

    RUN_FAILED = 'failed'
    DEAD_AT_STARTUP = 'dead at startup'
    SAME_AS_PARENT = 'scored exactly its parent'
    IDLE_QUEUE = 'idle'
    SILENT = 'silent while its jobs run'


class AnomalyReported(EventModel):
    """An anomaly that an agent was requested for, on the agent's row: one event for each anomaly."""

    kind: Literal['anomaly.reported'] = 'anomaly.reported'
    subject: Ref
    """The row that the anomaly is about."""
    anomaly: AnomalyKind
    line: str
    """The anomaly as the agent's brief shows it."""
