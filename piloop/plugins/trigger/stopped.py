from typing import ClassVar, Literal

from piloop.plugins.trigger.contract import Fire, Tick, Trigger


class StoppedTrigger(Trigger):
    """The campaign stopped (``[stop]``). It fires once, on the tick that stops the campaign.

    The daemon checks it after it dropped what was pending, so its row starts. The watchdog's closing review is this
    trigger. The reason is ``campaign stopped: <the condition>``.
    """

    ON_STOP: ClassVar[bool] = True
    kind: Literal['stopped'] = 'stopped'

    def fire(self, tick: Tick) -> Fire | None:
        return Fire(f'campaign stopped: {tick.campaign.stopped_reason}')
