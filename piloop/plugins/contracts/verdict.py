"""The contract of a verdict rule: the types that piloop gives a rule, and the type that the rule returns.

After piloop measures a candidate, it asks the verdict rule whether to keep the candidate. It calls the rule's
``decide`` with a ``Case``: the measurements of the candidate, the measurements of its bar (the candidate that it must
beat), and the metrics that decide. The rule returns a ``Judgement``: ``keep``, ``interesting``, ``discard`` or
``crash``, and a reason. piloop stores both on the candidate.

The rule reads nothing else: it is a pure provider. ``[plugins.verdict_rule] provider`` in ``piloop.toml`` selects the
rule. The other keys of that table are the rule's parameters. Without the table, the rule is ``bootstrap``.

This module imports only the Python standard library and the plugin host. A rule imports this module, and nothing
else of piloop.
"""

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from piloop.plugins.host import PureCapability, Selection


class Direction(StrEnum):
    """Whether a larger or a smaller value of a metric is better."""

    HIGHER = 'higher'
    LOWER = 'lower'

    def better(self, a: float, b: float) -> bool:
        """Whether ``a`` is better than ``b``."""
        return a > b if self is Direction.HIGHER else a < b

    def gain(self, a: float, b: float) -> float:
        """How much better ``a`` is than ``b``, in the metric's units. Positive when ``a`` is better."""
        return a - b if self is Direction.HIGHER else b - a


class Outcome(StrEnum):
    """The verdicts that a rule gives. Each is the ``Verdict`` of the same value.

    The other verdicts are not judgements, and only piloop sets them: ``pending``, ``reference``, ``unmeasured`` and
    ``recovered``.
    """

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


@dataclass(frozen=True)
class Case:
    """All that a rule gets to judge one candidate."""

    primary: Criterion
    guardrails: tuple[Criterion, ...]
    """The project's guardrails, then the primary of every other track."""
    confidence: float
    """``confidence`` in ``piloop.toml``: the confidence level that a gain must reach, between 0.5 and 1."""
    candidate: Side
    bar: Side
    parameters: str = '{}'
    """The rule's parameters, the other keys of ``[plugins.verdict_rule]``, as a JSON object. piloop checked them
    against the rule's ``Parameters`` when it loaded the configuration."""


@dataclass(frozen=True)
class Judgement:
    """What a rule returns: the verdict, and the reason that piloop stores and shows with it."""

    verdict: Outcome
    reason: str


class VerdictRule(Protocol):
    """The method that a verdict rule has. A rule has no error case: it returns a verdict for every case."""

    def decide(self, case: Case) -> Judgement:
        """Return the verdict on ``case.candidate`` against ``case.bar``, and the reason."""
        ...


def number(value: float, *, signed: bool = False) -> str:
    """Format a value in a reason: whole numbers in full (``3688770``), others to 4 significant digits (``0.8312``).

    The format of ``piloop.text.metric``, which a rule cannot import. A reason is stored, so its format does not
    follow a change of how the renderers show a metric. ``signed`` adds the sign of a delta (``+12``).
    """
    sign = ('+' if value >= 0 else '-') if signed else ('-' if value < 0 else '')
    size = abs(value)
    if size.is_integer() or size >= 1e4:
        return f'{sign}{size:.0f}'
    return f'{sign}{size:.4g}'


VERDICT_RULES = PureCapability(
    'verdict rule',
    group='piloop.verdict_rules',
    contract=VerdictRule,
    selection=Selection('project', 'plugins.verdict_rule.provider'),
)
"""The verdict rule capability: every verdict rule by name. The built-in rule is ``bootstrap``
(``piloop.plugins.builtin.verdict``)."""
DEFAULT = 'bootstrap'
"""The rule of a project without ``[plugins.verdict_rule]``."""
