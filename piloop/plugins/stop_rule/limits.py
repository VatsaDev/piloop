"""``limits``: the built-in stop rule. It holds at ``hold_at``, and stops at the first other limit that is reached.

The rule imports only the Python standard library, pydantic, its contract and the facts it reads, as a rule from a
plugin does. It is an entry point of piloop's own ``pyproject.toml``, in ``piloop.stop_rules``.
"""

from collections.abc import Callable
from typing import Annotated

from pydantic import Field

from piloop.plugins.fact_source.facts import (
    HEAD,
    HOURS,
    KEEPS,
    RUNS,
    TOKENS,
    UNSETTLED,
    WITHOUT_KEEP,
    Fact,
    Facts,
    FactValue,
    reading,
)
from piloop.plugins.metric import Direction, number
from piloop.plugins.stop_rule.contract import GO, Escalate, Go, Hold, Stop, StopCase, StopRule

NONE = '—'


class Limits(StopRule):
    """``limits``: hold while a fact of ``hold_at`` is unknown or at its limit, else stop at the first limit reached.

    ``[plugins.stop_rule]`` sets the limits. One is enough. Without one, the campaign has no end.

    The daemon checks the limits on every pass, before it requests anything. When one is reached, the campaign is
    ``stopped`` (``campaigns.status``, ``stopped_reason``). Running implementers are told to save their work and stop, and
    are ended 10 minutes later. What they saved is scored. Most harnesses read the message only when the turn ends: an
    implementer that is ended before it left its finding ends its run ``campaign_stopped``, which does not count toward
    ``max_runs`` or ``plateau``. Pending requests and queued runs are dropped (``campaign_stopped``). Once the last
    commits are scored, the ``stopped`` triggers fire once, which by default requests the watchdog's closing review,
    and the triggers request nothing more. The daemon files no seeds for the campaign either. An agent that you request with ``piloop launch`` after
    the stop still starts.

    ``piloop campaign stop`` stops a campaign the same way at once, whatever the limits are. With ``--kill``,
    the implementers are ended at once instead of told. ``piloop init`` resumes a stopped campaign when its limits
    changed.

    ``max_runs`` and ``plateau`` count the same runs: those that reached the evaluator, with a candidate that is
    measured or waits for its measurement. A run that ended at a cheap check with no version, with only versions that
    asked to be skipped, or before it saved one because of a harness or host fault, says nothing about the metric, so
    it counts toward neither. A run that a daemon stop, a daemon crash or a campaign stop ended does not
    count either. ``hold_at``, ``max_tokens`` and ``max_hours`` stay hard limits on what all runs cost.

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
    hold_at: dict[str, Annotated[float, Field(gt=0)]] = {}
    """Hold the campaign while a fact that a source reads, by its name, is at this value or above, and while it is
    unknown: the daemon launches no agent and starts no evaluation. Work that already runs finishes and is scored.
    Example: ``{ spend = 1000 }``, with a source in ``[[facts]]`` that reads ``spend``. To resume, raise it and run
    ``piloop init``."""

    def limits(self) -> dict[Fact[FactValue], float]:
        found = {
            RUNS: self.max_runs,
            HOURS: self.max_hours,
            TOKENS: self.max_tokens,
            WITHOUT_KEEP: self.plateau,
            HEAD: self.target,
        }
        stops = {fact: limit for fact, limit in found.items() if limit is not None}
        return stops | {reading(name): limit for name, limit in self.hold_at.items()}

    def reads(self) -> list[Fact[FactValue]]:
        """The runs and the hours, which ``describe`` always shows, and the facts that it limits.

        With a limit on runs, the unsettled attempts too.
        """
        limits = self.limits()
        return list(dict.fromkeys([RUNS, HOURS, *limits, *([UNSETTLED] if RUNS in limits else [])]))

    def check(self, case: StopCase) -> Go | Hold | Stop | Escalate:
        limits = self.limits()
        holds = (_hold(case.facts, name, limit) for name, limit in self.hold_at.items())
        if (held := next((h for h in holds if h is not None), None)) is not None:
            return held
        reasons = (reached(case, limits[fact]) for fact, reached in STOPS if fact in limits)
        return next((Stop(reason) for reason in reasons if reason is not None), GO)

    def describe(self, case: StopCase) -> list[str]:
        """``run 7/200 · 2.1h/12h``: the runs and the hours, then each other limit against its fact.

        Then each value that it holds at, against its limit. Without a limit that stops the campaign, it says so: a
        ``hold_at`` only holds it.
        """
        limits = self.limits()
        parts = [show(case, limits.get(fact)) for fact, show in SHOWN if fact in (RUNS, HOURS) or fact in limits]
        held = [_show_held(case.facts, name, limit) for name, limit in self.hold_at.items()]
        stops = limits.keys() - {reading(name) for name in self.hold_at}
        return [*parts, *held] if stops else [*parts, *held, 'no limit stops it: open-ended']


def _hold(facts: Facts, name: str, limit: float) -> Hold | Escalate | None:
    """Hold while ``name`` is unknown or at its limit. At the limit, tell a human once for each limit."""
    if (value := facts.get(reading(name))) is None:
        return Hold(f'{name} not known')
    if value < limit:
        return None
    return Escalate(
        f'{name} reached its hold_at of {number(limit)}: nothing launches until you raise it and run piloop init',
        then=Hold(f'{name} at its limit'),
    )


def _show_held(facts: Facts, name: str, limit: float) -> str:
    value = facts.get(reading(name))
    return f'{name} {NONE if value is None else number(value)}/{number(limit)}'


def _target(case: StopCase, target: float) -> str | None:
    value = case.facts.get(HEAD)
    if value is None or (value != target and not case.direction.better(value, target)):
        return None
    sign = '≥' if case.direction is Direction.HIGHER else '≤'
    return f'target: {case.primary} {number(value)} {sign} {number(target)}'


def _max_runs(case: StopCase, limit: float) -> str | None:
    runs = case.facts.get(RUNS)
    return f'max_runs: {runs} runs' if runs is not None and runs >= limit and not case.facts.get(UNSETTLED) else None


def _max_hours(case: StopCase, limit: float) -> str | None:
    hours = case.facts.get(HOURS)
    return f'max_hours: {hours:.1f}h since the first run' if hours is not None and hours >= limit else None


def _max_tokens(case: StopCase, limit: float) -> str | None:
    tokens = case.facts.get(TOKENS)
    return f'max_tokens: {tokens} tokens spent' if tokens is not None and tokens >= limit else None


def _plateau(case: StopCase, limit: float) -> str | None:
    count = case.facts.get(WITHOUT_KEEP)
    return f'plateau: {count} runs without a keep' if count is not None and count >= limit else None


STOPS: tuple[tuple[Fact[FactValue], Callable[[StopCase, float], str | None]], ...] = (
    (HEAD, _target),
    (RUNS, _max_runs),
    (HOURS, _max_hours),
    (TOKENS, _max_tokens),
    (WITHOUT_KEEP, _plateau),
)
"""The stops, in the order that ``Limits`` tries them: the fact that each limits, and the reason when its limit is
reached."""


def _of(limit: float | None, unit: str = '') -> str:
    return '' if limit is None else f'/{number(limit)}{unit}'


def _tokens(tokens: float) -> str:
    """A short form of a token count, ``1.2M``: the format of ``piloop.usage.human``, which a rule cannot import."""
    if tokens >= 1_000_000:
        return f'{tokens / 1_000_000:.1f}M'
    return f'{tokens / 1_000:.0f}k' if tokens >= 1_000 else f'{tokens:.0f}'


def _show_runs(case: StopCase, limit: float | None) -> str:
    return f'run {case.facts.get(RUNS) or 0}{_of(limit)}'


def _show_hours(case: StopCase, limit: float | None) -> str:
    return f'{case.facts.get(HOURS) or 0:.1f}h{_of(limit, "h")}'


def _show_tokens(case: StopCase, limit: float | None) -> str:
    return f'{_tokens(case.facts.get(TOKENS) or 0)}{"" if limit is None else f"/{_tokens(limit)}"} tok'


def _show_without_keep(case: StopCase, limit: float | None) -> str:
    return f'{case.facts.get(WITHOUT_KEEP) or 0}{_of(limit)} runs without a keep'


def _show_keeps(case: StopCase, limit: float | None) -> str:
    return f'{case.facts.get(KEEPS) or 0}{_of(limit)} keeps'


def _show_head(case: StopCase, limit: float | None) -> str:
    value = case.facts.get(HEAD)
    head = NONE if value is None else number(value)
    sign = '≥' if case.direction is Direction.HIGHER else '≤'
    return f'head {head}' if limit is None else f'target {sign} {number(limit)} (head {head})'


SHOWN: tuple[tuple[Fact[FactValue], Callable[[StopCase, float | None], str]], ...] = (
    (RUNS, _show_runs),
    (HOURS, _show_hours),
    (TOKENS, _show_tokens),
    (WITHOUT_KEEP, _show_without_keep),
    (KEEPS, _show_keeps),
    (HEAD, _show_head),
)
"""How ``describe`` shows each fact, against its limit when it has one, in this order."""
