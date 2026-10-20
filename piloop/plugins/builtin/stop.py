"""``limits``: the built-in stop rule. It stops the campaign at the first limit of ``[stop]`` that the campaign reaches.

The rule imports only the Python standard library and its contract, as a rule from a plugin does. It is an entry
point of piloop's own ``pyproject.toml``, in ``piloop.stop_rules``.
"""

from piloop.plugins.contracts.stop import Fact, Limits, Progress, StopCase
from piloop.plugins.contracts.verdict import Direction, number


class FirstLimit:
    """``limits``: the campaign ends at the first limit of ``[stop]`` that it reached.

    In this order: ``target``, ``max_runs``, ``max_hours``, ``max_tokens``, ``plateau``. ``max_runs`` also waits until
    no attempt is unsettled, because one that ends without a scored candidate does not count.
    """

    def reads(self, limits: Limits, parameters: str) -> list[Fact]:
        """The fact of each limit that is set: an open-ended campaign costs no query."""
        facts = {
            Fact.HEAD: limits.target,
            Fact.RUNS: limits.max_runs,
            Fact.HOURS: limits.max_hours,
            Fact.TOKENS: limits.max_tokens,
            Fact.WITHOUT_KEEP: limits.plateau,
        }
        return [fact for fact, limit in facts.items() if limit is not None]

    def check(self, case: StopCase) -> str | None:
        limits, progress = case.limits, case.progress
        checks = (
            _target(case, limits.target) if limits.target is not None else None,
            _max_runs(progress, limits.max_runs) if limits.max_runs is not None else None,
            _max_hours(progress, limits.max_hours) if limits.max_hours is not None else None,
            _max_tokens(progress, limits.max_tokens) if limits.max_tokens is not None else None,
            _plateau(progress, limits.plateau) if limits.plateau is not None else None,
        )
        return next((reason for reason in checks if reason is not None), None)


LIMITS = FirstLimit()
"""The provider, and the default stop rule: the entry point names this instance."""


def _target(case: StopCase, target: float) -> str | None:
    value = case.progress.head
    if value is None or (value != target and not case.direction.better(value, target)):
        return None
    sign = '≥' if case.direction is Direction.HIGHER else '≤'
    return f'target: {case.primary} {number(value)} {sign} {number(target)}'


def _max_runs(progress: Progress, limit: int) -> str | None:
    runs = progress.runs
    return f'max_runs: {runs} runs' if runs is not None and runs >= limit and not progress.unsettled else None


def _max_hours(progress: Progress, limit: float) -> str | None:
    if progress.hours is None or progress.hours < limit:
        return None
    return f'max_hours: {progress.hours:.1f}h since the first run'


def _max_tokens(progress: Progress, limit: int) -> str | None:
    tokens = progress.tokens
    return f'max_tokens: {tokens} tokens spent' if tokens is not None and tokens >= limit else None


def _plateau(progress: Progress, limit: int) -> str | None:
    count = progress.without_keep
    return f'plateau: {count} runs without a keep' if count is not None and count >= limit else None
