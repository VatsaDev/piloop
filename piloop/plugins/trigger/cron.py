from datetime import timedelta
from typing import Literal

from piloop.db.models import utcnow
from piloop.plugins.trigger.contract import Fire, Tick, Trigger


class CronTrigger(Trigger):
    """The role's ``interval`` passed since the start of its last routine agent, one that got no anomaly.

    The first one counts from the registration of the campaign. An ``interval`` of 0 turns it off.
    """

    kind: Literal['cron'] = 'cron'

    def fire(self, tick: Tick) -> Fire | None:
        if tick.interval <= 0:
            return None
        routine = tick.routine() if tick.last is not None else None
        since = routine.started_at if routine is not None else tick.campaign.created_at
        if utcnow() - since < timedelta(seconds=tick.interval):
            return None
        return Fire(f'timer: {tick.interval:g}s')
