from datetime import timedelta

from piloop.plugins.trigger.contract import Fire, Pass, Trigger
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

    def fire(self, p: Pass) -> Fire | None:
        step = p.stall_step(None)
        if step == 0 or futile(p, p.backlog(0)):
            return None
        if p.last is not None and p.stall_step(p.last.started_at) >= step:
            return None
        reason = news(p, f'stalled, step {step}')
        return None if reason is None else Fire(reason)

    def due_in(self, p: Pass) -> timedelta | None:
        return None
