"""``min_gain``: the built-in verdict, but a keep needs a gain of at least ``min_gain``. No state, no I/O.

Use it when a tiny gain is not worth a keep. Each keep moves the head, and every later run must beat the head. The
rule calls the built-in rule ``bootstrap`` and changes one thing. The confidence, the guardrails and the reasons stay
those of ``bootstrap``.
"""

import json
from dataclasses import dataclass

from piloop.capabilities import BOOTSTRAP, Case, Judgement, Outcome, number


@dataclass(frozen=True)
class Parameters:
    """``[plugins.verdict_rule]``: ``min_gain``, the smallest gain that keeps, in the primary metric's units."""

    min_gain: float


class MinGain:
    """What ``bootstrap`` decides, but a keep with a gain below ``min_gain`` is ``interesting``."""

    Parameters = Parameters

    def decide(self, case: Case) -> Judgement:
        judged = BOOTSTRAP.decide(case)
        name = case.primary.name
        if judged.verdict is not Outcome.KEEP or name not in case.bar.means:
            return judged
        least = Parameters(**json.loads(case.parameters)).min_gain
        gain = case.primary.direction.gain(case.candidate.means[name], case.bar.means[name])
        if gain >= least:
            return judged
        return Judgement(Outcome.INTERESTING, f'{judged.reason}; the gain is below min_gain {number(least)}')


MIN_GAIN = MinGain()
"""The provider: an instance, as the entry point of a pure provider names."""
