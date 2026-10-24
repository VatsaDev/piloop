"""``after_keeps``: stop the campaign after ``keeps`` keeps, or at a limit of the built-in rule. No state, no I/O.

Use it for a campaign that wants a few good changes and no more, for example "make the build pass on the new
compiler". It stops when enough candidates are kept. The rule is the built-in rule ``limits`` with one more limit, so
``max_hours``, ``max_tokens``, ``max_spend`` and the others still apply.
"""

from piloop.capabilities import Fact, Go, Hold, Limits, Stop, StopCase


class AfterKeeps(Limits):
    """The campaign ends after ``keeps`` keeps. ``limits`` holds it and stops it at its own limits."""

    keeps: int
    """The number of keeps that ends the campaign."""

    def limits(self) -> dict[Fact, float]:
        return super().limits() | {Fact.KEEPS: self.keeps}

    def check(self, case: StopCase) -> Go | Hold | Stop:
        answer = super().check(case)
        keeps = case.progress.keeps
        if not isinstance(answer, Go) or keeps is None or keeps < self.keeps:
            return answer
        return Stop(f'after_keeps: {keeps} keeps')
