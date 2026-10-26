"""The contract of a verdict rule: the types that piloop gives a rule, and the type that the rule returns.

After piloop measures a candidate, it asks the verdict rule whether to keep the candidate. It calls the rule's
``decide`` with a ``Case``: the measurements of the candidate, the measurements of its bar (the candidate that it must
beat), and the metrics that decide. The rule returns a ``Judgement``: ``keep``, ``interesting``, ``discard`` or
``crash``, and a reason. piloop stores both on the candidate.

The rule also decides the lineage: the bar of a run (``bar``), which keeps raise it (``raises_bar``), the head
(``head``), and what follows a keep that lacks the head (``on_moved``). Each method but ``decide`` has the built-in behaviour as
its default.

The rule reads nothing else: it is a pure provider. ``[plugins.verdict_rule] provider`` in ``piloop.toml`` selects the
rule, and the other keys of that table are its fields. Without the table, the rule is ``bootstrap``.

This module imports only the Python standard library, the plugin host, the ids and the metric types. A rule imports this
module, and nothing else of piloop.
"""

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import assert_never

from piloop.ids import CandidateId
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
    """The metric's name, as its ``METRIC`` line prints it."""
    direction: Direction
    """Whether a larger or a smaller value is better."""
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
    """The primary metric of the candidate's track."""
    guardrails: tuple[Criterion, ...]
    """The campaign's guardrails, then the primary of every other track."""
    candidate: Side
    """The candidate that the rule judges."""
    bar: Side
    """What the candidate must beat: its run's bar (https://piloop.autoresearch.inc/concepts/verdict#the-words)."""
    parent: Side | None = None
    """The candidate's parent, which the candidate changed. None for a candidate without one."""


@dataclass(frozen=True)
class Judgement:
    """What a rule returns: the verdict, and the reason that piloop stores and shows with it."""

    verdict: Outcome
    """``keep``, ``interesting``, ``discard`` or ``crash``."""
    reason: str
    """Why, as piloop stores and shows it with the verdict."""


@dataclass(frozen=True)
class Trunk:
    """The lineage of the head: the keeps that no branch run made, from the baseline."""


@dataclass(frozen=True)
class Branch:
    """A lineage that a branch run (``runs.branch``) started from ``root``."""

    root: CandidateId
    """The candidate that the branch run started from."""


type Lineage = Trunk | Branch
"""Where a candidate grew, or where a run keeps."""


@dataclass(frozen=True)
class Imported:
    """A reference: a version that someone imported (``piloop candidate import``). It grew on no lineage."""


@dataclass(frozen=True)
class Node:
    """One candidate, as the lineage methods of a rule read it."""

    id: CandidateId
    """The candidate's id."""
    verdict: Outcome | None
    """None while it is pending, and for a commit that is never judged."""
    made: Lineage | Imported
    """Where it was made: on the side of the run that made it, or imported. The baseline is on the trunk."""
    means: dict[str, float]
    """The mean of each metric over its successful measurements under the current evaluator."""


@dataclass(frozen=True)
class Moved:
    """A keep of a trunk run that lacks the head: its bar is no longer the head, or someone kept it by hand.

    piloop makes it ``interesting`` (``moved``): as a keep it would drop what the head kept since.
    """

    candidate: Node
    """The keep that lacks the head."""
    head: Node
    """The head that it lacks."""


@dataclass(frozen=True)
class Seed:
    """File a seed idea on the candidate's track that composes it onto the head by hand. A stopped campaign files none."""


@dataclass(frozen=True)
class Leave:
    """Do nothing more: the candidate stays ``interesting``, and its change does not reach the trunk."""


@dataclass(frozen=True)
class Rebase:
    """Merge the candidate onto the head. piloop measures the merge and judges it against the head."""

    conflicts: Seed | Leave = Seed()
    """What follows when the merge conflicts."""


class VerdictRule(Parameters, ABC):
    """A verdict rule: its fields are its parameters. A rule has no error case: it returns a verdict for every case."""

    @abstractmethod
    def decide(self, case: Case) -> Judgement:
        """Return the verdict on ``case.candidate`` against ``case.bar``, and the reason."""

    def bar(self, ancestry: Sequence[Node]) -> CandidateId:
        """Return the bar of a run that starts from ``ancestry[0]``. The ancestry goes from it, parent by parent, to its root.

        By default the bar is the nearest keep or reference, else the root. A non-keep is never a bar, pinned or not.
        Otherwise a run pinned to an ``interesting`` candidate would be held to that candidate's numbers, which a fix to
        it could never beat. A run pinned to a non-keep starts from its tree and must beat the latest keep of its
        lineage.
        """
        found = (n.id for n in ancestry if n.verdict is Outcome.KEEP or isinstance(n.made, Imported))
        return next(found, ancestry[-1].id)

    def raises_bar(self, node: Node, lineage: Lineage) -> bool:
        """Return whether ``node``, a commit of a run that keeps on ``lineage``, becomes the bar of the run's next commits.

        The trunk's keeps are those that raise its bar. By default a keep does, and a known keep that the run reached
        again too. On the trunk, a keep that a branch run made does not: a trunk run that merges a branch keep onto the
        head must beat the head, so a branch's numbers are never a trunk run's bar.
        """
        match lineage:
            case Branch():
                return node.verdict is Outcome.KEEP
            case Trunk():
                return node.verdict is Outcome.KEEP and not isinstance(node.made, Branch)
            case _:
                assert_never(lineage)

    def head(self, trunk: Sequence[Node]) -> CandidateId | None:
        """Return the head among the trunk's keeps, oldest first, or None when there is none.

        Every run that pins no base starts from the head. By default it is the latest trunk keep. A trunk keep lands
        only while its bar is the head, and a candidate is always newer than its bar, so each trunk keep is newer than
        the one before. Scores do not matter: after an evaluator change, the head stays where it was.
        """
        return trunk[-1].id if trunk else None

    def on_moved(self, moved: Moved) -> Rebase | Leave:
        """Return what follows a keep that lacks the head. By default its merge onto the head, and a seed on a conflict."""
        return Rebase()


VERDICT_RULES = Capability(
    'verdict_rule', VerdictRule, tier=Tier.PURE, choice=Choice(Scope.CAMPAIGN, default='bootstrap')
)
"""The verdict rule capability: every verdict rule by name. The built-in rule is ``bootstrap``
(``piloop.plugins.verdict_rule.bootstrap``)."""
