from datetime import timedelta
from typing import ClassVar

from piloop.plugins.trigger.contract import Fire, Pass, Trigger


class StoppedTrigger(Trigger):
    """The campaign stopped. It fires once, when the runs that the stop wound down are judged.

    The stop came from its stop rule or a request. So the closing review sees the final head. The watchdog's closing review is this trigger. The reason is ``campaign
    stopped: <the condition>``.
    """

    ON_STOP: ClassVar[bool] = True

    def fire(self, p: Pass) -> Fire | None:
        return Fire(f'campaign stopped: {p.campaign.stopped_reason}')

    def due_in(self, p: Pass) -> timedelta | None:
        return None
