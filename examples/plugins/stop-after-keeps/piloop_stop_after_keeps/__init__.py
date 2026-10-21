"""``after_keeps``: stop the campaign after ``keeps`` keeps. The built-in rule keeps the cost limits. No state, no I/O.

Use it for a campaign that wants a few good changes and no more, for example "make the build pass on the new
compiler". It stops when enough candidates are kept. The rule calls the built-in rule ``limits`` for ``max_hours``,
``max_tokens`` and ``[spend]``, so they still limit the cost.
"""

import json
from dataclasses import dataclass, replace

from piloop.plugins.builtin.stop import LIMITS
from piloop.plugins.contracts.stop import Fact, Go, Hold, Limits, Stop, StopCase


@dataclass(frozen=True)
class Parameters:
    """``[plugins.stop_rule]``: ``keeps``, the number of keeps that ends the campaign."""

    keeps: int


class AfterKeeps:
    """The campaign ends after ``keeps`` keeps. ``limits`` holds it and stops it at its cost limits."""

    Parameters = Parameters

    def reads(self, limits: Limits, parameters: str) -> list[Fact]:
        return [Fact.KEEPS, *LIMITS.reads(costs(limits), '{}')]

    def check(self, case: StopCase) -> Go | Hold | Stop:
        answer = LIMITS.check(replace(case, limits=costs(case.limits), parameters='{}'))
        keeps = case.progress.keeps
        if isinstance(answer, Hold) or keeps is None or keeps < Parameters(**json.loads(case.parameters)).keeps:
            return answer
        return Stop(f'after_keeps: {keeps} keeps')


def costs(limits: Limits) -> Limits:
    """Only the limits on what the campaign costs: ``max_hours``, ``max_tokens`` and ``[spend] limit``."""
    return Limits(max_hours=limits.max_hours, max_tokens=limits.max_tokens, max_spend=limits.max_spend)


AFTER_KEEPS = AfterKeeps()
"""The provider: an instance, as the entry point of a pure provider names."""
