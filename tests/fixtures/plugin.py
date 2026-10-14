"""What a package can add to piloop through entry points: triggers, roles, a runtime, and objects of the wrong type."""

from pathlib import Path
from typing import ClassVar, Literal

from piloop.config import SweepRole, WorkerRole
from piloop.harnesses import Invocation
from piloop.runtime import Check, Handle
from piloop.runtime.subprocess import SubprocessRuntime
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


class Typed(SubprocessRuntime):
    """``typed``: a background process, like ``subprocess``, that starts the interactive argument list of a harness."""

    kind: ClassVar[str] = 'typed'
    invocation: ClassVar[Invocation] = Invocation.INTERACTIVE


class Shown(SubprocessRuntime):
    """``shown``: a background process, like ``subprocess``, with a pane of its own to focus, attach to and check."""

    kind: ClassVar[str] = 'shown'
    invocation: ClassVar[Invocation] = Invocation.HEADLESS

    def focus(self, handle: Handle) -> str:
        return f'focused pane {handle.data["pid"]}'

    def attach(self, handle: Handle) -> str:
        return f'attached to pane {handle.data["pid"]}'

    @classmethod
    def view(cls, handle: Handle) -> str | None:
        return f'pane {handle.data["pid"]}'

    @classmethod
    def checks(cls, executables: set[str]) -> list[Check]:
        return [Check('shown', f'ready for {", ".join(sorted(executables))}')]


class Unsure:
    """A class with a runtime's name and without an invocation."""

    kind: ClassVar[str] = 'unsure'


AUDITOR = SweepRole(prompt=str(Path(__file__).parent / 'auditor.md'), triggers=[EveryTick(label='audit')], timeout=5)
QUICK = WorkerRole(takes='priority >= 3', timeout=60, max_concurrent=1)
NOT_A_TRIGGER = 'every_tick'
NOT_A_ROLE = 'auditor'
NOT_A_RUNTIME = 'typed'
