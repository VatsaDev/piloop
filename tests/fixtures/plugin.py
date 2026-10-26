"""What a package can add to piloop through entry points: triggers, roles, a runtime, a stop rule, a capability of
its own with a provider and a verdict rule that calls it, and objects of the wrong type."""

from abc import ABC, abstractmethod
from collections.abc import Sequence
from datetime import timedelta
from importlib.resources import files
from typing import Annotated

from piloop.ids import CandidateId
from piloop.plugins.fact_source.facts import ATTEMPTS, EVALUATIONS, FUNNEL, Fact, FactValue, since_keep
from piloop.plugins.host import Capability, Parameters, Tier
from piloop.plugins.role.contract import ImplementerRole, SweepRole
from piloop.plugins.runtime.contract import RUNTIMES, Handle, RuntimeName
from piloop.plugins.runtime.subprocess import Subprocess, SubprocessRuntime
from piloop.plugins.stop_rule.contract import GO, Escalate, Go, Hold, Stop, StopCase, StopRule
from piloop.plugins.trigger.contract import Fire, Pass, Quiet, Trigger, TriggerEntry
from piloop.plugins.verdict_rule.bootstrap import Bootstrap
from piloop.plugins.verdict_rule.contract import (
    Case,
    Judgement,
    Leave,
    Lineage,
    Moved,
    Node,
    Outcome,
    Rebase,
    VerdictRule,
)


class EveryPass(Trigger):
    """``every_pass``: fires on every pass, with ``label`` as the reason."""

    label: str = 'every pass'

    def fire(self, p: Pass) -> Fire | None:
        return Fire(f'{self.label} for {p.role}')

    def due_in(self, p: Pass) -> timedelta | None:
        return None


class Awake(EveryPass):
    """``awake``: fires on every pass, also while its campaign is paused, held or stopped."""

    WHILE = frozenset({Quiet.PAUSED, Quiet.HELD, Quiet.STOPPED})

    def fire(self, p: Pass) -> Fire | None:
        return Fire(f'{self.label} for {p.role} ({p.quiet or "running"})')


class Broken(Trigger):
    """``broken``: a trigger whose check always fails."""

    def fire(self, p: Pass) -> Fire | None:
        raise RuntimeError('the probe is down')

    def due_in(self, p: Pass) -> timedelta | None:
        return None


class Typed(Subprocess):
    """``typed``: a background process, like ``subprocess``, under a name of its own."""


class Shown(Subprocess):
    """``shown``: a background process, like ``subprocess``, whose output is one line that names its pid."""

    def open(self) -> ShownRuntime:
        return ShownRuntime(RUNTIMES.name_of(self))


class ShownRuntime(SubprocessRuntime):
    def read(self, handle: Handle, lines: int = 80) -> str:
        return f'pane {handle.data["pid"]}'


class GoesOn(StopRule):
    """``goes_on``: a stop rule without limits, so it reads no fact and never holds or stops a campaign."""

    def reads(self) -> list[Fact[FactValue]]:
        return []

    def check(self, case: StopCase) -> Go | Hold | Stop:
        return GO


WATCHED: dict[Fact[FactValue], float] = {EVALUATIONS: 100, ATTEMPTS: 50, since_keep('spend'): 300, FUNNEL: 1}


class Watches(StopRule):
    """``watches``: limits on the facts that ``limits`` does not limit, which it never acts on."""

    def reads(self) -> list[Fact[FactValue]]:
        return list(WATCHED)

    def check(self, case: StopCase) -> Go:
        return GO

    def describe(self, case: StopCase) -> list[str]:
        shown = {fact: case.facts.get(fact) for fact in WATCHED}
        return [
            f'{fact.name} {value if isinstance(value, int | float) else "—"}/{WATCHED[fact]:g}'
            for fact, value in shown.items()
        ]


class Escalates(StopRule):
    """``escalates``: a stop rule that always tells a human ``reason``, and lets the campaign go on."""

    reason: str

    def reads(self) -> list[Fact[FactValue]]:
        return []

    def check(self, case: StopCase) -> Escalate:
        return Escalate(self.reason)


class Auditor(SweepRole):
    """``auditor``: a sweep on every pass."""

    PROMPT = files(__package__) / 'auditor.md'
    triggers: list[TriggerEntry] = [EveryPass(label='audit')]
    timeout: float = 5


class Quick(ImplementerRole):
    """``quick``: an implementer of the ideas of priority 3 and more."""

    takes: str | None = 'priority >= 3'
    timeout: float = 60
    max_concurrent: int = 1


class Fast(ImplementerRole):
    """A role whose default runtime is ``nice``, a runtime only once a test installs it: defaults are not validated."""

    takes: str | None = 'priority >= 9'
    runtime: RuntimeName | None = 'nice'


class VerdictCheck(Parameters, ABC):
    """A capability that a plugin declares: a check that can refuse a candidate that ``bootstrap`` keeps."""

    @abstractmethod
    def check(self, case: Case) -> str | None:
        """Return why the candidate must not be kept, or None."""


VERDICT_CHECKS = Capability('verdict_check', VerdictCheck, tier=Tier.PURE)


class NoUnitRegression(VerdictCheck):
    """``no_unit_regression``: refuses a keep when a unit of the primary metric got worse."""

    def check(self, case: Case) -> str | None:
        name, direction = case.primary.name, case.primary.direction
        mine, theirs = case.candidate.units.get(name, {}), case.bar.units.get(name, {})
        worse = sorted(u for u in mine.keys() & theirs.keys() if direction.gain(mine[u], theirs[u]) < 0)
        return f'{len(worse)} units of {name} got worse: {", ".join(worse[:3])}' if worse else None


class Checked(Bootstrap):
    """``checked``: what ``bootstrap`` decides, but a keep that one of ``checks`` refuses is ``interesting``."""

    checks: list[Annotated[VerdictCheck, VERDICT_CHECKS.entry]]

    def decide(self, case: Case) -> Judgement:
        judged = super().decide(case)
        held = [f'{VERDICT_CHECKS.name_of(c)}: {why}' for c in self.checks if (why := c.check(case)) is not None]
        if judged.verdict is not Outcome.KEEP or not held:
            return judged
        return Judgement(Outcome.INTERESTING, f'{judged.reason}; held by {"; ".join(held)}')


class Undecided(VerdictRule):
    """A verdict rule without ``decide``."""


NOT_A_TRIGGER = 'every_pass'
NOT_A_ROLE = 'auditor'
NOT_A_RUNTIME = 'typed'


class BaseBar(Bootstrap):
    """``base_bar``: the built-in rule, but a run's bar is its base, a keep or not."""

    def bar(self, ancestry: Sequence[Node]) -> CandidateId:
        return ancestry[0].id


class EveryKeep(Bootstrap):
    """``every_keep``: the built-in rule, but every keep raises the bar, a branch run's on the trunk too."""

    def raises_bar(self, node: Node, lineage: Lineage) -> bool:
        return node.verdict is Outcome.KEEP


class FirstHead(Bootstrap):
    """``first_head``: the built-in rule, but the head is the first trunk keep."""

    def head(self, trunk: Sequence[Node]) -> CandidateId | None:
        return trunk[0].id if trunk else None


class Leaves(Bootstrap):
    """``leaves``: the built-in rule, but a keep that lacks the head is not merged onto it."""

    def on_moved(self, moved: Moved) -> Rebase | Leave:
        return Leave()


class NoCompose(Bootstrap):
    """``no_compose``: the built-in rule, but a merge onto the head that conflicts files no seed."""

    def on_moved(self, moved: Moved) -> Rebase | Leave:
        return Rebase(conflicts=Leave())


class Astray(Bootstrap):
    """``astray``: the built-in rule, but its lineage methods choose a candidate that they were not given."""

    def bar(self, ancestry: Sequence[Node]) -> CandidateId:
        return CandidateId(10**6)

    def head(self, trunk: Sequence[Node]) -> CandidateId | None:
        return CandidateId(10**6)
