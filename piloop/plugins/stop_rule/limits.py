"""``limits``: the built-in stop rule. It holds at ``[spend] limit``, and stops at the first limit of ``[stop]``.

The rule imports only the Python standard library and its contract, as a rule from a plugin does. It is an entry
point of piloop's own ``pyproject.toml``, in ``piloop.stop_rules``.
"""

from collections.abc import Callable

from piloop.plugins.metric import Direction, number
from piloop.plugins.stop_rule.contract import GO, Fact, Go, Hold, Limits, Progress, Stop, StopCase


class FirstLimit:
    """``limits``: hold while the spend is unknown or at ``[spend] limit``, else stop at the first limit reached.

    The hold comes first: a ``[stop]`` limit does not stop a held campaign. A stop that someone requests still stops
    it (``piloop.campaign.stop.requested``). The stops come in the order of ``STOPS``. ``max_runs`` also waits until
    no attempt is unsettled, because one that ends without a scored candidate does not count.
    """

    def reads(self, limits: Limits, parameters: str) -> list[Fact]:
        """The fact of each limit that is set: an open-ended campaign costs no query."""
        stops = [fact for fact, limit, _ in STOPS if limit(limits) is not None]
        return stops + ([Fact.SPEND] if limits.max_spend is not None else [])

    def check(self, case: StopCase) -> Go | Hold | Stop:
        limits = case.limits
        if limits.max_spend is not None and (held := _spend(case.progress, limits.max_spend)) is not None:
            return Hold(held)
        reasons = (reached(case, value) for _, limit, reached in STOPS if (value := limit(limits)) is not None)
        return next((Stop(reason) for reason in reasons if reason is not None), GO)


LIMITS = FirstLimit()
"""The provider, and the default stop rule: the entry point names this instance."""


def _spend(progress: Progress, limit: float) -> str | None:
    if progress.spend_error is not None:
        return f'spend unknown: {progress.spend_error}'
    if progress.spend is None:
        return 'spend not read yet'
    return 'spend limit reached' if progress.spend >= limit else None


def _target(case: StopCase, target: float) -> str | None:
    value = case.progress.head
    if value is None or (value != target and not case.direction.better(value, target)):
        return None
    sign = '≥' if case.direction is Direction.HIGHER else '≤'
    return f'target: {case.primary} {number(value)} {sign} {number(target)}'


def _max_runs(case: StopCase, limit: float) -> str | None:
    runs = case.progress.runs
    return f'max_runs: {runs} runs' if runs is not None and runs >= limit and not case.progress.unsettled else None


def _max_hours(case: StopCase, limit: float) -> str | None:
    hours = case.progress.hours
    return f'max_hours: {hours:.1f}h since the first run' if hours is not None and hours >= limit else None


def _max_tokens(case: StopCase, limit: float) -> str | None:
    tokens = case.progress.tokens
    return f'max_tokens: {tokens} tokens spent' if tokens is not None and tokens >= limit else None


def _plateau(case: StopCase, limit: float) -> str | None:
    count = case.progress.without_keep
    return f'plateau: {count} runs without a keep' if count is not None and count >= limit else None


STOPS: tuple[tuple[Fact, Callable[[Limits], float | None], Callable[[StopCase, float], str | None]], ...] = (
    (Fact.HEAD, lambda limits: limits.target, _target),
    (Fact.RUNS, lambda limits: limits.max_runs, _max_runs),
    (Fact.HOURS, lambda limits: limits.max_hours, _max_hours),
    (Fact.TOKENS, lambda limits: limits.max_tokens, _max_tokens),
    (Fact.WITHOUT_KEEP, lambda limits: limits.plateau, _plateau),
)
"""The stops of ``[stop]``, in the order that ``FirstLimit`` tries them: the fact that each reads, its limit, and the
reason when the limit is reached."""
