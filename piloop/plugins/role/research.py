from typing import ClassVar

from pydantic import Field

from piloop.plugins.role.contract import Files, SweepRole
from piloop.plugins.trigger.contract import TriggerEntry
from piloop.plugins.trigger.low_water import LowWaterTrigger
from piloop.plugins.trigger.seed import SeedTrigger
from piloop.plugins.trigger.stalled import StalledTrigger


class ResearchRole(SweepRole):
    """``[roles.research]``: research develops seeds and keeps ideas ready for the implementers."""

    PROMPT: ClassVar[str] = 'research.md'

    triggers: list[TriggerEntry] = [LowWaterTrigger(), SeedTrigger(), StalledTrigger()]
    """A short backlog, a new seed, and a stall."""
    writes: list[str] = [
        'seeds via idea develop, or their status to rejected with a note',
        'ideas via idea add',
        'comments(finding, escalation) on the campaign',
        'comments(note) on an idea',
    ]
    """What a research sweep may write, as its brief states it."""
    model: dict[str, str] | None = {'claude': 'claude-opus-5-5', 'codex': 'gpt-6.1-sol'}
    """The model for each harness in this role, merged over the campaign's ``model``. A table that you set replaces
    this one."""
    files: Files = Files.IDEAS
    """Research develops seeds into ideas and adds ideas of its own."""
    timeout: float = 900
    """The number of seconds one sweep may run before it is stopped."""
    max_concurrent: int = Field(default=1, ge=1)
    """The maximum number of sweeps that run at the same time, each on the next harness of the rotation. The default
    is 1: sweeps that run at the same time read the same evidence and often duplicate each other, and two of them can
    race to develop the same seed."""


RESEARCH = ResearchRole()
