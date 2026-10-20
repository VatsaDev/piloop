"""``min_gain``: keep a candidate only when it beats its bar by at least ``min_gain``. No state, no I/O.

Use it when a tiny gain is not worth a keep. Each keep moves the head, and every later run must beat the head. The
floors of the guardrail metrics still apply. Their margins do not.
"""

import json
from dataclasses import dataclass

from piloop.plugins.contracts.verdict import Case, Judgement, Outcome, number


@dataclass(frozen=True)
class Parameters:
    """``[plugins.verdict_rule]``: ``min_gain``, the smallest gain that keeps, in the primary metric's units."""

    min_gain: float


class MinGain:
    """A gain of at least ``min_gain`` keeps, a smaller one is interesting, and no gain discards."""

    Parameters = Parameters

    def decide(self, case: Case) -> Judgement:
        least = Parameters(**json.loads(case.parameters)).min_gain
        primary, mine, theirs = case.primary, case.candidate.means, case.bar.means
        if primary.name not in mine:
            return Judgement(Outcome.CRASH, 'no successful measurement')
        floors = [m for m in case.guardrails if m.floor is not None and m.name in mine]
        floored = [m.name for m in floors if m.floor is not None and m.direction.better(m.floor, mine[m.name])]
        if floored:
            return Judgement(Outcome.DISCARD, f'past its floor: {", ".join(floored)}')
        if primary.name not in theirs:
            return Judgement(Outcome.KEEP, f'{case.bar.name} has no score')
        gain = primary.direction.gain(mine[primary.name], theirs[primary.name])
        why = f'against {case.bar.name}: {primary.name} gains {number(gain, signed=True)}, keeps at {number(least)}'
        if gain >= least:
            return Judgement(Outcome.KEEP, why)
        return Judgement(Outcome.INTERESTING if gain > 0 else Outcome.DISCARD, why)


MIN_GAIN = MinGain()
"""The provider: an instance, as the entry point of a pure provider names."""
