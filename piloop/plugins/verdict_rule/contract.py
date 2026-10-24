"""The contract of a verdict rule: the types that piloop gives a rule, and the type that the rule returns.

After piloop measures a candidate, it asks the verdict rule whether to keep the candidate. It calls the rule's
``decide`` with a ``Case``: the measurements of the candidate, the measurements of its bar (the candidate that it must
beat), and the metrics that decide. The rule returns a ``Judgement``: ``keep``, ``interesting``, ``discard`` or
``crash``, and a reason. piloop stores both on the candidate.

The rule reads nothing else: it is a pure provider. ``[plugins.verdict_rule] provider`` in ``piloop.toml`` selects the
rule, and the other keys of that table are its fields. Without the table, the rule is ``bootstrap``.

This module imports only the Python standard library, the plugin host and the metric types. A rule imports this module, and nothing
else of piloop.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import StrEnum

from piloop.plugins.host import Capability, Choice, Parameters, Scope, Tier
from piloop.plugins.metric import Direction


class Outcome(StrEnum):
    """A verdict: what a rule gives, and what ``candidates.verdict`` holds. The column is NULL until a candidate is judged."""

    KEEP = 'keep'
    INTERESTING = 'interesting'
    DISCARD = 'discard'
    CRASH = 'crash'


@dataclass(frozen=True)
class Criterion:
    """A metric that decides the verdict: the primary of the candidate's track, or one of its guardrails."""

    name: str
    direction: Direction
    margin: float | None = None
    """The largest regression from the bar that a guardrail may show, in the metric's units. None on a primary."""
    floor: float | None = None
    """The worst value that a candidate may have, whatever the bar, or None."""


@dataclass(frozen=True)
class Side:
    """One side of the case: the candidate, or the bar that it must beat."""

    name: str
    """How a reason names it: ``candidate#73``."""
    means: dict[str, float]
    """The mean of each metric over its successful measurements. Empty when none succeeded."""
    units: dict[str, dict[str, float]]
    """The per-unit results of each metric that has them: unit id → the unit's mean contribution."""
    samples: dict[str, list[float]] = field(default_factory=dict)
    """The samples of each metric that ran in pairs with the other side's: the i-th of the candidate and the i-th of
    the bar ran one after the other, on one worker. Empty without pairs."""


@dataclass(frozen=True)
class Case:
    """All that a rule gets to judge one candidate."""

    primary: Criterion
    guardrails: tuple[Criterion, ...]
    """The campaign's guardrails, then the primary of every other track."""
    candidate: Side
    bar: Side


@dataclass(frozen=True)
class Judgement:
    """What a rule returns: the verdict, and the reason that piloop stores and shows with it."""

    verdict: Outcome
    reason: str


class VerdictRule(Parameters, ABC):
    """A verdict rule: its fields are its parameters. A rule has no error case: it returns a verdict for every case."""

    @abstractmethod
    def decide(self, case: Case) -> Judgement:
        """Return the verdict on ``case.candidate`` against ``case.bar``, and the reason."""


VERDICT_RULES = Capability(
    'verdict_rule', VerdictRule, tier=Tier.PURE, choice=Choice(Scope.CAMPAIGN, default='bootstrap')
)
"""The verdict rule capability: every verdict rule by name. The built-in rule is ``bootstrap``
(``piloop.plugins.verdict_rule.bootstrap``)."""
