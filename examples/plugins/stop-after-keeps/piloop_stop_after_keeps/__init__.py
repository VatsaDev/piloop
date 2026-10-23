"""``after_keeps``: stop the campaign after ``keeps`` keeps. The built-in rule keeps the cost limits. No state, no I/O.

Use it for a campaign that wants a few good changes and no more, for example "make the build pass on the new
compiler". It stops when enough candidates are kept. The rule is the built-in rule ``limits`` for ``max_hours``,
``max_tokens`` and ``[spend]``, so they still limit the cost.
"""

from dataclasses import replace

from piloop.capabilities import Fact, FirstLimit, Go, Hold, Limits, Stop, StopCase


class AfterKeeps(FirstLimit):
    """The campaign ends after ``keeps`` keeps. ``limits`` holds it and stops it at its cost limits."""

    keeps: int
    """The number of keeps that ends the campaign."""

    def reads(self, limits: Limits) -> list[Fact]:
        return [Fact.KEEPS, *super().reads(costs(limits))]

    def check(self, case: StopCase) -> Go | Hold | Stop:
        answer = super().check(replace(case, limits=costs(case.limits)))
        keeps = case.progress.keeps
        if isinstance(answer, Hold) or keeps is None or keeps < self.keeps:
            return answer
        return Stop(f'after_keeps: {keeps} keeps')


def costs(limits: Limits) -> Limits:
    """Only the limits on what the campaign costs: ``max_hours``, ``max_tokens`` and ``[spend] limit``."""
    return Limits(max_hours=limits.max_hours, max_tokens=limits.max_tokens, max_spend=limits.max_spend)
