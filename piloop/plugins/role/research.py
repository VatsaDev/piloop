from importlib.resources import files

from pydantic import Field
from sqlalchemy import select

from piloop.db.models import Idea, count
from piloop.ids import IdeaStatus
from piloop.plugins.role.contract import Files, Sweep, SweepRole
from piloop.plugins.trigger.contract import TriggerEntry
from piloop.plugins.trigger.low_water import LowWaterTrigger
from piloop.plugins.trigger.stalled import StalledTrigger


class ResearchRole(SweepRole):
    """``[roles.research]``: research develops seeds and keeps ideas ready for the implementers."""

    PROMPT = files(__package__) / 'prompts' / 'research.md'

    triggers: list[TriggerEntry] = Field(
        default=[{'provider': 'low_water'}, {'provider': 'seed'}, {'provider': 'stalled'}], validate_default=True
    )
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

    def context(self, sweep: Sweep) -> list[str]:
        """The seeds to develop, this sweep's share of the backlog, the tracks short of their share, and the stall.

        The backlog's target has the ``buffer`` of the role's first ``low_water`` trigger, or none without one. The
        stall mark is that of its first ``stalled`` trigger: without one, the role has no stall.
        """
        buffer = next((t.buffer for t in self.triggers if isinstance(t, LowWaterTrigger)), 0)
        backlog = sweep.backlog(buffer)
        most = self.max_ideas
        mark = next((t.mark(sweep.campaign.config) for t in self.triggers if isinstance(t, StalledTrigger)), None)
        runs = sweep.without_keep(None)
        stalled = mark is not None and runs >= mark.every
        pivot = min(most, 1) if stalled else 0  # A stall gets its pivot whatever the backlog holds.
        own = max(min(backlog.short + most, most), pivot)  # This sweep's share of the shortfall.
        campaign = sweep.campaign
        seeds = count(sweep.s, select(Idea).where(Idea.campaign_id == campaign.id, Idea.status == IdeaStatus.SEED))
        pivots = ': stalled, so one of your own ideas pivots (section 3)' if pivot else ''
        short = ', '.join(
            f'{t.name} {t.gap} ({t.runnable} of {t.target} runnable)' for t in backlog.tracks if t.gap > 0
        )
        tracks = [
            f'- Tracks short of their share of the backlog: {short or "none"}. Give every idea and every developed seed'
            ' the track it serves (`--track`); fill the short ones first.'
        ]
        return [
            f'- Seeds to develop first: {seeds}, listed in the research pack oldest first.',
            f'- Ideas of your own after the seeds: propose {own} (the backlog holds {backlog.held()};'
            ' developed seeds do not count against it). Fewer only when what is left duplicates queued work;'
            ' name that work in your finding.',
            *(tracks if campaign.config.multitrack else []),
            *(
                [
                    f'- Measured runs since the last keep: {runs}, of the {mark.every} that mark a stall ({mark.why}){pivots}.'
                ]
                if mark is not None
                else []
            ),
        ]
