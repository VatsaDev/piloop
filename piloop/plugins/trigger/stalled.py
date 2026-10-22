from typing import Literal

from piloop.plugins.trigger.contract import Fire, Tick, Trigger
from piloop.plugins.trigger.kit.reasons import futile, news


class StalledTrigger(Trigger):
    """The campaign reached a new stall step, and no sweep of the role started in it, however full the backlog is.

    The campaign has stalled when the runs since the last keep reach the stall mark (``piloop.campaign.stop.stall``).
    Each further mark of runs without a keep is a new step, and a keep ends the stall. The backlog may hold no idea
    that pivots, and the brief of a sweep on a stall allows one idea of the role's own. So a stall of N runs gets
    about N / mark sweeps, not one per result. The back-off of ``low_water`` for new results still applies, and a
    killed sweep is retried at the next step. As with ``low_water``, nothing is requested while every slot of the role
    is busy, while its ``max_ideas`` is 0, or while the queue holds its ideas for the head (``Backlog.on_hold``).
    """

    kind: Literal['stalled'] = 'stalled'

    def fire(self, tick: Tick) -> Fire | None:
        step = tick.stall_step(None)
        if step == 0 or futile(tick, tick.backlog(0)):
            return None
        if tick.last is not None and tick.stall_step(tick.last.started_at) >= step:
            return None
        reason = news(tick, f'stalled, step {step}')
        return None if reason is None else Fire(reason)
