from datetime import timedelta
from typing import TYPE_CHECKING, NamedTuple

from pydantic import Field

from piloop.plugins.trigger.contract import Fire, Pass, Trigger
from piloop.plugins.trigger.kit.reasons import futile, news

if TYPE_CHECKING:  # piloop.config types the roles with their triggers: only the annotation names it
    from piloop.config import CampaignConfig


class Mark(NamedTuple):
    """The runs without a keep that mark a stall, and how the number was found, for a brief."""

    every: int
    why: str


class StalledTrigger(Trigger):
    """The campaign reached a new stall step, and no sweep of the role started in it, however full the backlog is.

    The campaign has stalled when the runs since the last keep reach the stall mark, ``every``. Each further mark of
    runs without a keep is a new step, and a keep ends the stall. The backlog may hold no idea that pivots, and the
    brief of a sweep on a stall allows one idea of the role's own. So a stall of N runs gets about N / mark sweeps, not
    one per result. The back-off of ``low_water`` for new results still applies, and a killed sweep is retried at the
    next step. As with ``low_water``, nothing is requested while every slot of the role is busy, while its
    ``max_ideas`` is 0, or while the queue holds its ideas for the head (``Backlog.on_hold``).
    """

    every: int | None = Field(default=None, ge=1)
    """The runs without a keep that mark a stall. When unset, two rounds of the run slots: twice the sum of the
    ``max_concurrent`` of the roles that do runs."""

    def mark(self, cfg: CampaignConfig) -> Mark:
        """The stall mark of a campaign with the configuration ``cfg``."""
        if self.every is not None:
            return Mark(self.every, f'every = {self.every} of the stalled trigger')
        slots = ' + '.join(f'[plugins.role.{name}] max_concurrent' for name in cfg.roles.doing_runs())
        return Mark(2 * cfg.run_slots, f'two rounds of {slots} = {cfg.run_slots}')

    def fire(self, p: Pass) -> Fire | None:
        every = self.mark(p.campaign.config).every
        step = p.without_keep(None) // every
        if step == 0 or futile(p, p.backlog(0)):
            return None
        if p.last is not None and p.without_keep(p.last.started_at) // every >= step:
            return None
        reason = news(p, f'stalled, step {step}')
        return None if reason is None else Fire(reason)

    def due_in(self, p: Pass) -> timedelta | None:
        return None
