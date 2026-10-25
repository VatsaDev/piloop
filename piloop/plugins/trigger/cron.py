from datetime import datetime, timedelta

from piloop.plugins.trigger.contract import Fire, Pass, Trigger


class CronTrigger(Trigger):
    """The role's ``interval`` passed since the start of its last routine agent, one that got no anomaly.

    The first one counts from the registration of the campaign. An ``interval`` of 0 turns it off.
    """

    def fire(self, p: Pass) -> Fire | None:
        if p.interval <= 0 or p.now < _next(p):
            return None
        return Fire(f'timer: {p.interval:g}s')

    def due_in(self, p: Pass) -> timedelta | None:
        return None if p.interval <= 0 else max(_next(p) - p.now, timedelta(0))


def _next(p: Pass) -> datetime:
    """When the role's next routine sweep is due."""
    routine = p.routine() if p.last is not None else None
    since = routine.started_at if routine is not None else p.campaign.created_at
    return since + timedelta(seconds=p.interval)
