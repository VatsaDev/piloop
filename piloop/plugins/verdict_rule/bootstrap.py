"""``bootstrap``: the built-in verdict rule. It keeps a candidate when the primary metric beats the bar with confidence.

Each guardrail metric can get worse by at most its margin, and never past its floor. The rule imports only the Python
standard library, pydantic and its contract, as a rule from a plugin does. It is an entry point of piloop's own
``pyproject.toml``, in ``piloop.verdict_rules``. https://piloop.autoresearch.inc/concepts/verdict describes the rule.
"""

import random
from dataclasses import dataclass
from enum import StrEnum
from statistics import fmean

from pydantic import Field

from piloop.plugins.metric import Direction, number
from piloop.plugins.verdict_rule.contract import Case, Criterion, Judgement, Outcome, Side, VerdictRule

RESAMPLES = 4000
"""Bootstrap resamples per comparison. With 4000, the 5% quantile changes by much less than one unit between seeds."""
SEED = 0
"""Each comparison uses a new generator with this seed, so the same numbers always give the same verdict."""


class Over(StrEnum):
    """What the bootstrap resamples."""

    UNITS = 'units'
    """The per-unit results: the worst case is a bound of the sum of the unit differences."""
    PAIRS = 'pairs'
    """The pairs of a paired evaluation: the worst case is a bound of the mean of the pair differences."""


@dataclass(frozen=True)
class Comparison:
    """One metric of a candidate compared with the same metric of the bar."""

    criterion: Criterion
    mine: float
    theirs: float
    """The means of the pairs when ``over`` is ``PAIRS``, else the means of each side."""
    worst: float
    """The worst-case gain at the configured confidence: the bootstrap's lower bound, else the gain."""
    count: int
    """How many units or pairs the bootstrap resampled. 0 when the comparison uses aggregates (``why`` says why)."""
    over: Over = Over.UNITS
    why: str = ''

    @property
    def gain(self) -> float:
        """How much the candidate beats the bar. Positive when the candidate is better."""
        return self.criterion.direction.gain(self.mine, self.theirs)

    def text(self, confidence: float) -> str:
        """Format the comparison in the metric's direction.

        For example: ``score 104 vs 80 (+24; 95% worst case -1.25 over 69 units)``.
        """
        sign = 1 if self.criterion.direction is Direction.HIGHER else -1
        # + 0.0 turns -0 into +0 for a lower-is-better metric that did not change
        change = (
            f'{self.criterion.name} {number(self.mine)} vs {number(self.theirs)}'
            f' ({number(sign * self.gain + 0.0, signed=True)}'
        )
        if self.count:
            worst = f'{number(sign * self.worst + 0.0, signed=True)} over {self.count} {self.over}'
            return f'{change}; {confidence:.0%} worst case {worst})'
        return f'{change}; aggregate: {self.why})'


def lower_bound(diffs: list[float], confidence: float) -> float:
    """Return the lower ``confidence`` bound of ``sum(diffs)`` from a bootstrap over the units.

    The units are resampled with replacement. A unit is a cluster: one record, one file, whatever the evaluate command
    reports as additive. Resampling whole units keeps their correlation, so a gain from three units shows the spread of
    three units, not of all of them.
    """
    rng = random.Random(SEED)
    n = len(diffs)
    sums = sorted(sum(rng.choices(diffs, k=n)) for _ in range(RESAMPLES))
    return sums[int((1 - confidence) * RESAMPLES)]


def compare(
    criterion: Criterion,
    mine: float,
    theirs: float,
    mine_units: dict[str, float] | None,
    theirs_units: dict[str, float] | None,
    confidence: float,
    *,
    mine_samples: list[float] | None = None,
    theirs_samples: list[float] | None = None,
) -> Comparison:
    """Compare with a paired bootstrap over the units that both sides report, else over the pairs of samples.

    Units come first: the pairs of a deterministic metric all differ by the same amount, so they hide the spread of
    the units. Compare the aggregates when neither can be paired.
    """
    gain = criterion.direction.gain
    mine_samples, theirs_samples = mine_samples or [], theirs_samples or []
    if (mine_units is None or theirs_units is None) and mine_samples and len(mine_samples) == len(theirs_samples):
        diffs = [gain(a, b) for a, b in zip(mine_samples, theirs_samples, strict=True)]
        worst = lower_bound(diffs, confidence) / len(diffs)
        return Comparison(criterion, fmean(mine_samples), fmean(theirs_samples), worst, len(diffs), Over.PAIRS)
    if mine_units is None or theirs_units is None:
        missing = {(True, True): '', (True, False): ' for the candidate', (False, True): ' for the bar'}
        why = 'no per-unit results' + missing[mine_units is None, theirs_units is None]
        return Comparison(criterion, mine, theirs, gain(mine, theirs), 0, why=why)
    if mine_units.keys() != theirs_units.keys():
        return Comparison(criterion, mine, theirs, gain(mine, theirs), 0, why='the unit sets differ')
    diffs = [gain(mine_units[u], theirs_units[u]) for u in sorted(mine_units)]
    return Comparison(criterion, mine, theirs, lower_bound(diffs, confidence), len(diffs))


class Bootstrap(VerdictRule):
    """``bootstrap``: the primary must beat the bar with confidence, and the guardrails may regress a little."""

    confidence: float = Field(default=0.95, gt=0.5, lt=1)
    """The confidence level of the paired bootstrap behind the verdict. For a ``keep``, the lower bound of the
    primary's gain must be above zero at this level. A guardrail's margin is tested on the bound at this level too."""

    def decide(self, case: Case) -> Judgement:
        """Return the verdict on the candidate (``_verdict``), and the reason.

        When the deciding metrics equal the parent's (``same``), the reason says so first: the change did not affect
        what the evaluation measures, whatever the verdict against the bar.
        """
        judged = self._verdict(case)
        parent = case.parent
        if parent is None or not same(case, parent):
            return judged
        which = 'primary and guardrails' if case.guardrails else case.primary.name
        return Judgement(judged.verdict, f'no-op: {which} identical to parent {parent.name}; {judged.reason}')

    def _verdict(self, case: Case) -> Judgement:
        """Return the verdict on the candidate against the bar, and the reason.

        The rules apply in this order:

        1. No primary value is ``crash``.
        2. A guardrail past its floor is ``discard``.
        3. A bar with no score is ``keep``.
        4. A primary that did not go up is ``discard``.
        5. A primary whose worst case at ``confidence`` is not above zero is ``interesting``.
        6. A guardrail whose worst case is beyond its margin is ``interesting``.
        7. Anything else is ``keep``.

        Whatever the rule that decides, the reason names every guardrail past its floor or beyond its margin. A
        keep's reason names every guardrail.
        """
        primary, mine, theirs = case.primary, case.candidate.means, case.bar.means
        if primary.name not in mine:
            return Judgement(Outcome.CRASH, 'no successful measurement')
        floors = {
            m.name: f'{m.name} {number(mine[m.name])} past its floor {number(m.floor)}'
            for m in case.guardrails
            if m.floor is not None and m.name in mine and m.direction.better(m.floor, mine[m.name])
        }
        floored = 'floor: ' + '; '.join(floors.values())
        if primary.name not in theirs:
            if floors:
                return Judgement(Outcome.DISCARD, floored)
            return Judgement(Outcome.KEEP, f'{case.bar.name} has no score under the current evaluator')
        guards = _guardrails(case, self.confidence)
        beyond = [t for m, t, held in guards if held and m.name in mine and m.name in theirs and m.name not in floors]
        past = f'; guardrails past their margin: {"; ".join(beyond)}' if beyond else ''
        if floors:
            return Judgement(Outcome.DISCARD, floored + past)
        p = _compare(case, primary, self.confidence)
        reason = f'against {case.bar.name}: {p.text(self.confidence)}'
        if p.gain <= 0:
            return Judgement(Outcome.DISCARD, reason + past)
        if p.worst <= 0:
            return Judgement(Outcome.INTERESTING, f'{reason}: the gain is not significant{past}')
        if guards:
            reason += '; guardrails: ' + '; '.join(text for _, text, _ in guards)
        return Judgement(Outcome.INTERESTING if any(held for _, _, held in guards) else Outcome.KEEP, reason)


def same(case: Case, parent: Side) -> bool:
    """Return whether the candidate has a primary value and equals ``parent`` on the primary and every guardrail.

    Watch metrics are left out. A timing metric (``eval_seconds``) differs on every evaluation, and a metric that does
    not decide a verdict cannot make a change matter.
    """
    mine, theirs = case.candidate.means, parent.means
    deciding = (case.primary, *case.guardrails)
    return case.primary.name in mine and all(mine.get(m.name) == theirs.get(m.name) for m in deciding)


def _compare(case: Case, criterion: Criterion, confidence: float) -> Comparison:
    """Compare the candidate with the bar on ``criterion``, which both sides measured."""
    name = criterion.name
    mine, theirs = case.candidate, case.bar
    return compare(
        criterion,
        mine.means[name],
        theirs.means[name],
        mine.units.get(name),
        theirs.units.get(name),
        confidence,
        mine_samples=mine.samples.get(name),
        theirs_samples=theirs.samples.get(name),
    )


def _guardrails(case: Case, confidence: float) -> list[tuple[Criterion, str, bool]]:
    """Compare each guardrail with the bar. Return it, the text of the comparison, and whether it holds the keep back.

    A guardrail holds the keep back when its worst case is beyond its margin, or when one side did not measure it.
    """
    found: list[tuple[Criterion, str, bool]] = []
    for m in case.guardrails:
        if m.name not in case.candidate.means or m.name not in case.bar.means:
            found.append((m, f'{m.name} unmeasured', True))
            continue
        g = _compare(case, m, confidence)
        margin = m.margin or 0
        breached = g.worst < -margin
        text = f'{g.text(confidence)} {"beyond" if breached else "within"} its margin {number(margin)}'
        found.append((m, text, breached))
    return found
