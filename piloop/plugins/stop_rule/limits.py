"""``limits``: the built-in stop rule. It holds at ``max_spend``, and stops at the first other limit that is reached.

The rule imports only the Python standard library, pydantic, and its contract, as a rule from a plugin does. It is an
entry point of piloop's own ``pyproject.toml``, in ``piloop.stop_rules``.
"""

from collections.abc import Callable

from pydantic import Field

from piloop.plugins.metric import Direction, number
from piloop.plugins.stop_rule.contract import GO, Fact, Go, Hold, Progress, Stop, StopCase, StopRule


class Limits(StopRule):
    """``limits``: hold while the spend is unknown or at ``max_spend``, else stop at the first limit reached.

    ``[plugins.stop_rule]`` sets the limits. One is enough. Without one, the campaign has no end.

    The daemon checks the limits on every tick, before it requests anything. When one is reached, the campaign is
    ``stopped`` (``campaigns.status``, ``stopped_reason``). Running implementers are told to commit and stop, and are
    ended 10 minutes later. What they committed is scored. Most harnesses read the message only when the turn ends: an
    implementer that is ended before it left its finding ends its run ``campaign_stopped``, which does not count toward
    ``max_runs`` or ``plateau``. Pending requests and queued runs are dropped (``campaign_stopped``). Once the last
    commits are scored, the ``stopped`` triggers fire once, which by default requests the watchdog's closing review,
    and the triggers request nothing more. The daemon files no seeds for the campaign either. An agent that you request with ``piloop launch`` after
    the stop still starts.

    ``piloop campaign stop`` stops a campaign the same way on the next tick, whatever the limits are. With ``--kill``,
    the implementers are ended at once instead of told. ``piloop init`` resumes a stopped campaign when its limits
    changed.

    ``max_runs`` and ``plateau`` count the same runs: those that reached the evaluator, with a candidate that is
    measured or waits for its measurement. A run that ended at a cheap check with no commit, with only
    ``Piloop-Eval: skip`` commits, or before it committed because of a harness or host fault, says nothing about the
    metric, so it counts toward neither. A run that a daemon stop, a daemon crash or a campaign stop ended does not
    count either. ``max_spend``, ``max_tokens`` and ``max_hours`` stay hard limits on what all runs cost.

    The hold comes first: a limit does not stop a held campaign. A stop that someone requests still stops it
    (``piloop.campaign.stop.requested``). The stops come in the order of ``STOPS``. ``max_runs`` also waits until no
    attempt is unsettled, because one that ends without a scored candidate does not count.
    """

    target: float | None = None
    """Stop when the primary metric at the head (the latest keep on the trunk) reaches this value, in its direction.
    Only for a campaign with one track."""
    max_runs: int | None = Field(default=None, ge=1)
    """Stop after this many runs that count, once none is queued, running or being scored. A run counts when it reached
    the evaluator, and also while it is queued, running or being scored, so piloop never starts more runs than this. A
    finished run that does not count frees its place, and piloop starts another run in it."""
    max_hours: float | None = Field(default=None, gt=0)
    """Stop after this many hours of wall-clock time since the first run of the campaign started. The time that the
    campaign was paused (``piloop pause``) does not count. An agent's timeout ends at the stop too."""
    max_tokens: int | None = Field(default=None, ge=1)
    """Stop when all the agents of the campaign together spent this many tokens (the ``total`` of
    ``agent_sessions.usage``), summed over the agents that ended."""
    plateau: int | None = Field(default=None, ge=1)
    """Stop after this many finished runs that reached the evaluator without a new ``keep`` candidate. The count starts
    again at each keep, and when the campaign is set ``active`` after a stop (``piloop init``, or an UPDATE of
    ``campaigns.status``), so a reactivated campaign gets this many runs again. A new evaluator alone does not reset
    it."""
    max_spend: float | None = Field(default=None, gt=0)
    """Hold the campaign when the spend that ``[spend] command`` prints reaches this many USD: the daemon launches no
    agent and starts no evaluation. Work that already runs finishes and is scored. Needs ``[spend]``. To resume, raise
    it and run ``piloop init``."""

    def limits(self) -> dict[Fact, float]:
        found = {
            Fact.RUNS: self.max_runs,
            Fact.HOURS: self.max_hours,
            Fact.TOKENS: self.max_tokens,
            Fact.WITHOUT_KEEP: self.plateau,
            Fact.HEAD: self.target,
            Fact.SPEND: self.max_spend,
        }
        return {fact: limit for fact, limit in found.items() if limit is not None}

    def check(self, case: StopCase) -> Go | Hold | Stop:
        limits = self.limits()
        if (spend := limits.get(Fact.SPEND)) is not None and (held := _spend(case.progress, spend)) is not None:
            return Hold(held)
        reasons = (reached(case, limits[fact]) for fact, reached in STOPS if fact in limits)
        return next((Stop(reason) for reason in reasons if reason is not None), GO)


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


STOPS: tuple[tuple[Fact, Callable[[StopCase, float], str | None]], ...] = (
    (Fact.HEAD, _target),
    (Fact.RUNS, _max_runs),
    (Fact.HOURS, _max_hours),
    (Fact.TOKENS, _max_tokens),
    (Fact.WITHOUT_KEEP, _plateau),
)
"""The stops, in the order that ``Limits`` tries them: the fact that each limits, and the reason when its limit is
reached."""
