"""``min_gain``: the built-in verdict, but a keep needs a gain of at least ``min_gain``. No state, no I/O.

Use it when a tiny gain is not worth a keep. Each keep moves the head, and every later run must beat the head. The
rule is the built-in rule ``bootstrap`` and changes one thing. The confidence, the guardrails and the reasons stay
those of ``bootstrap``.
"""

from piloop.capabilities import Bootstrap, Case, Judgement, Outcome, number


class MinGain(Bootstrap):
    """What ``bootstrap`` decides, but a keep with a gain below ``min_gain`` is ``interesting``."""

    min_gain: float
    """The smallest gain that keeps, in the primary metric's units."""

    def decide(self, case: Case) -> Judgement:
        judged = super().decide(case)
        name = case.primary.name
        if judged.verdict is not Outcome.KEEP or name not in case.bar.means:
            return judged
        gain = case.primary.direction.gain(case.candidate.means[name], case.bar.means[name])
        if gain >= self.min_gain:
            return judged
        return Judgement(Outcome.INTERESTING, f'{judged.reason}; the gain is below min_gain {number(self.min_gain)}')
