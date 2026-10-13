"""What a package can add to piloop through entry points: a trigger, a role, and objects of the wrong type."""

from pathlib import Path
from typing import Literal

from piloop.config import SweepRole
from piloop.triggers import Fire, Tick, Trigger


class EveryTick(Trigger):
    """``every_tick``: fires on every tick, with ``label`` as the reason."""

    kind: Literal['every_tick'] = 'every_tick'
    label: str = 'every tick'

    def fire(self, tick: Tick) -> Fire | None:
        return Fire(f'{self.label} for {tick.role}')


class Broken(Trigger):
    """``broken``: a trigger whose check always fails."""

    kind: Literal['broken'] = 'broken'

    def fire(self, tick: Tick) -> Fire | None:
        raise RuntimeError('the probe is down')


AUDITOR = SweepRole(prompt=str(Path(__file__).parent / 'auditor.md'), triggers=[EveryTick(label='audit')], timeout=5)
NOT_A_TRIGGER = 'every_tick'
NOT_A_ROLE = 'auditor'
