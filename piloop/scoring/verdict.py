"""The daemon's verdict rule: the primary must beat the bar with confidence, and the guardrails may regress a little.

The functions here are pure: numbers in, ``(verdict, reason)`` out. ``ledger.judge`` reads the numbers from the
database and the units files. https://piloop.autoresearch.inc/concepts/verdict states the rule.
"""

import random
from dataclasses import dataclass

from piloop.config import Direction, Metric, Track
from piloop.db.models import Verdict
from piloop.scoring.evaluate import Units

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
        change = f'{self.metric.name} {self.mine:g} vs {self.theirs:g} ({sign * self.gain + 0.0:+g}'
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

    The track's guardrails are the project's guardrails, then the primary of every other track.
    """
    primary = track.primary
    if primary.name not in mine:
        return Verdict.CRASH, 'no successful measurement'
    floors = [
        f'{m.name} {mine[m.name]:g} past its floor {m.floor:g}'
        for m in track.guardrails
        if m.floor is not None and m.name in mine and m.better(m.floor, mine[m.name])
    ]
    if floors:
        return Verdict.DISCARD, 'floor: ' + '; '.join(floors)
    if primary.name not in theirs:
        return Verdict.KEEP, f'{bar} has no score under the current evaluator'
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
        return Verdict.DISCARD, reason
    if p.worst <= 0:
        return Verdict.INTERESTING, f'{reason}: the gain is not significant'
    guards: list[str] = []
    breached = False
    for m in track.guardrails:
        if m.name not in mine or m.name not in theirs:
            guards.append(f'{m.name} unmeasured')
            breached = True
            continue
        g = compare(m, mine[m.name], theirs[m.name], mine_units.get(m.name), theirs_units.get(m.name), confidence)
        beyond = g.worst < -(m.margin or 0)
        breached |= beyond
        guards.append(f'{g.text(confidence)} {"beyond" if beyond else "within"} its margin {m.margin:g}')
    if guards:
        reason += '; guardrails: ' + '; '.join(guards)
    return (Verdict.INTERESTING if breached else Verdict.KEEP), reason


def same(track: Track, mine: dict[str, float], theirs: dict[str, float]) -> bool:
    """Return whether ``mine`` has a primary value and equals ``theirs`` on the primary and every guardrail.

    Watch metrics are left out. A timing metric (``eval_seconds``) differs on every evaluation, and a metric that does
    not decide a verdict cannot make a change matter.
    """
    deciding = [track.primary, *track.guardrails]
    return track.primary.name in mine and all(mine.get(m.name) == theirs.get(m.name) for m in deciding)
