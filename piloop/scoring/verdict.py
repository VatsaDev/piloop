"""The daemon's verdict rule: the primary must beat the bar with confidence, and the guardrails may regress a little.

The functions here are pure: numbers in, ``(verdict, reason)`` out. ``ledger.judge`` reads the numbers from the
database and the units files. https://piloop.autoresearch.inc/concepts/verdict states the rule.
"""

import random
from dataclasses import dataclass

from piloop.config import Direction, Metric, Track
from piloop.db.models import Verdict
from piloop.scoring.evaluate import Units
from piloop.text import metric

RESAMPLES = 4000
"""Bootstrap resamples per comparison. With 4000, the 5% quantile changes by much less than one unit between seeds."""
SEED = 0
"""Each comparison uses a new generator with this seed, so the same numbers always give the same verdict."""


@dataclass(frozen=True)
class Comparison:
    """One metric of a candidate compared with the same metric of the bar."""

    metric: Metric
    mine: float
    theirs: float
    worst: float
    """The worst-case gain at the configured confidence: the bootstrap's lower bound with units, else the gain."""
    units: int
    """The number of units the bootstrap paired. 0 when the comparison uses aggregates (``why`` says why)."""
    why: str = ''

    @property
    def gain(self) -> float:
        """How much the candidate beats the bar. Positive when the candidate is better."""
        return self.metric.gain(self.mine, self.theirs)

    def text(self, confidence: float) -> str:
        """Format the comparison in the metric's direction.

        For example: ``score 104 vs 80 (+24; 95% worst case -1.25 over 69 units)``.
        """
        sign = 1 if self.metric.direction is Direction.HIGHER else -1
        # + 0.0 turns -0 into +0 for a lower-is-better metric that did not change
        change = f'{self.metric.name} {metric(self.mine)} vs {metric(self.theirs)} ({metric(sign * self.gain + 0.0, signed=True)}'
        if self.units:
            return f'{change}; {confidence:.0%} worst case {sign * self.worst + 0.0:+.4g} over {self.units} units)'
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
    metric: Metric,
    mine: float,
    theirs: float,
    mine_units: dict[str, float] | None,
    theirs_units: dict[str, float] | None,
    confidence: float,
) -> Comparison:
    """Compare with a paired bootstrap over the units that both sides report.

    Compare the aggregates when the units cannot be paired.
    """
    if mine_units is None or theirs_units is None:
        missing = {(True, True): '', (True, False): ' for the candidate', (False, True): ' for the bar'}
        why = 'no per-unit results' + missing[mine_units is None, theirs_units is None]
        return Comparison(metric, mine, theirs, metric.gain(mine, theirs), 0, why)
    if mine_units.keys() != theirs_units.keys():
        return Comparison(metric, mine, theirs, metric.gain(mine, theirs), 0, 'the unit sets differ')
    diffs = [metric.gain(mine_units[u], theirs_units[u]) for u in sorted(mine_units)]
    return Comparison(metric, mine, theirs, lower_bound(diffs, confidence), len(diffs))


def decide(
    track: Track,
    confidence: float,
    mine: dict[str, float],
    theirs: dict[str, float],
    mine_units: Units,
    theirs_units: Units,
    *,
    bar: str,
) -> tuple[Verdict, str]:
    """Return the verdict on a candidate of ``track``, and the reason.

    ``mine`` holds the candidate's mean metrics and ``theirs`` the bar's. The rules apply in this order:

    1. No primary value is ``crash``.
    2. A guardrail past its floor is ``discard``.
    3. A bar with no score is ``keep``.
    4. A primary that did not go up is ``discard``.
    5. A primary whose worst case at ``confidence`` is not above zero is ``interesting``.
    6. A guardrail whose worst case is beyond its margin is ``interesting``.
    7. Anything else is ``keep``.

    The track's guardrails are the project's guardrails, then the primary of every other track. Whatever the rule
    that decides, the reason names every guardrail past its floor or beyond its margin. A keep's reason names every
    guardrail.
    """
    primary = track.primary
    if primary.name not in mine:
        return Verdict.CRASH, 'no successful measurement'
    floors = {
        m.name: f'{m.name} {metric(mine[m.name])} past its floor {metric(m.floor)}'
        for m in track.guardrails
        if m.floor is not None and m.name in mine and m.better(m.floor, mine[m.name])
    }
    floored = 'floor: ' + '; '.join(floors.values())
    if primary.name not in theirs:
        if floors:
            return Verdict.DISCARD, floored
        return Verdict.KEEP, f'{bar} has no score under the current evaluator'
    guards = _guardrails(track, confidence, mine, theirs, mine_units, theirs_units)
    beyond = [text for m, text, held in guards if held and m.name in mine and m.name in theirs and m.name not in floors]
    past = f'; guardrails past their margin: {"; ".join(beyond)}' if beyond else ''
    if floors:
        return Verdict.DISCARD, floored + past
    p = compare(
        primary,
        mine[primary.name],
        theirs[primary.name],
        mine_units.get(primary.name),
        theirs_units.get(primary.name),
        confidence,
    )
    reason = f'against {bar}: {p.text(confidence)}'
    if p.gain <= 0:
        return Verdict.DISCARD, reason + past
    if p.worst <= 0:
        return Verdict.INTERESTING, f'{reason}: the gain is not significant{past}'
    if guards:
        reason += '; guardrails: ' + '; '.join(text for _, text, _ in guards)
    return (Verdict.INTERESTING if any(breached for _, _, breached in guards) else Verdict.KEEP), reason


def _guardrails(
    track: Track,
    confidence: float,
    mine: dict[str, float],
    theirs: dict[str, float],
    mine_units: Units,
    theirs_units: Units,
) -> list[tuple[Metric, str, bool]]:
    """Compare each guardrail with the bar. Return it, the text of the comparison, and whether it holds the keep back.

    A guardrail holds the keep back when its worst case is beyond its margin, or when one side did not measure it.
    """
    found: list[tuple[Metric, str, bool]] = []
    for m in track.guardrails:
        if m.name not in mine or m.name not in theirs:
            found.append((m, f'{m.name} unmeasured', True))
            continue
        g = compare(m, mine[m.name], theirs[m.name], mine_units.get(m.name), theirs_units.get(m.name), confidence)
        margin = m.margin or 0
        breached = g.worst < -margin
        found.append(
            (m, f'{g.text(confidence)} {"beyond" if breached else "within"} its margin {metric(margin)}', breached)
        )
    return found


def same(track: Track, mine: dict[str, float], theirs: dict[str, float]) -> bool:
    """Return whether ``mine`` has a primary value and equals ``theirs`` on the primary and every guardrail.

    Watch metrics are left out. A timing metric (``eval_seconds``) differs on every evaluation, and a metric that does
    not decide a verdict cannot make a change matter.
    """
    deciding = [track.primary, *track.guardrails]
    return track.primary.name in mine and all(mine.get(m.name) == theirs.get(m.name) for m in deciding)
