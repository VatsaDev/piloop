"""The contract of a stop rule: the types that piloop gives a stop rule, and the types that the rule returns.

On each tick of the daemon, piloop asks the stop rule of each active campaign whether the campaign goes on, holds, or
stops. It asks in two steps. First it calls ``reads``: the rule names the facts that it needs. piloop gets only those
facts. Then it calls ``check`` with a ``StopCase``: those facts. The rule returns one answer:

- ``Go``: piloop starts what the campaign needs.
- ``Hold``: piloop starts no agent and no evaluation for the campaign. Work that runs finishes. The hold is not a
  pause: on each tick piloop asks again, and the hold ends when the rule answers ``Go``.
- ``Stop``: piloop stops the campaign.

The rule reads nothing else: it is a pure provider. ``[plugins.stop_rule] provider`` in ``piloop.toml`` selects the
rule, and the other keys of that table are its fields, its limits among them. Without the table, the rule is
``limits``. piloop reads the limits too, by fact (``limits``): it starts no more runs than a limit on ``runs``, research
pivots at half a limit on ``without_keep``, and a limit on ``spend`` escalates when the spend reaches it.

This module imports only the Python standard library, the plugin host, and the metric types (``piloop.plugins.metric``).
A rule imports this module, and nothing else of piloop.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum

from piloop.plugins.host import Capability, Choice, Parameters, Scope, Tier
from piloop.plugins.metric import Direction


class Fact(StrEnum):
    """A fact of the campaign. Most facts cost queries on every tick, so a rule names the ones it reads."""

    HEAD = 'head'
    RUNS = 'runs'
    HOURS = 'hours'
    TOKENS = 'tokens'
    WITHOUT_KEEP = 'without_keep'
    KEEPS = 'keeps'
    SPEND = 'spend'


@dataclass(frozen=True)
class Progress:
    """Where the campaign is, counted on this tick. A fact that the rule does not read is None."""

    head: float | None = None
    """``head``: the primary metric at the head under the current evaluator. Also None without a head, or when the
    head has no score."""
    runs: int | None = None
    """``runs``: the runs that hold a place under ``max_runs``: those that count, and the attempts that may still
    count."""
    unsettled: int | None = None
    """``runs`` too: the attempts whose outcome can still change: queued, running, or being scored."""
    hours: float | None = None
    """``hours``: the hours since the first run started, without the time that the campaign was paused. Also None
    before the first run."""
    tokens: int | None = None
    """``tokens``: the tokens that the agents of the campaign spent, summed over the agents that ended."""
    without_keep: int | None = None
    """``without_keep``: the finished runs that count since the latest keep, or since the campaign was last set
    active again."""
    keeps: int | None = None
    """``keeps``: the candidates of runs that are kept, on the trunk or on a branch. The baseline is not one."""
    spend: float | None = None
    """``spend``: the spend in USD that ``[spend] command`` printed last. Also None before its first reading, and
    while its latest run failed."""
    spend_error: str | None = None
    """``spend`` too: why the latest run of ``[spend] command`` failed, or None after a run that printed a spend."""


@dataclass(frozen=True)
class StopCase:
    """Everything that a stop rule reads."""

    primary: str
    """The name of the primary metric of the campaign's first track."""
    direction: Direction
    progress: Progress


@dataclass(frozen=True)
class Go:
    """The campaign goes on: piloop starts what it needs."""


GO = Go()


@dataclass(frozen=True)
class Hold:
    """piloop starts no agent and no evaluation for the campaign until a check answers ``Go`` again."""

    reason: str
    """Why, for ``piloop status`` (``launches held: <reason>``)."""


@dataclass(frozen=True)
class Stop:
    """piloop stops the campaign."""

    reason: str
    """Why, for ``campaigns.stopped_reason`` and the closing review."""


class StopRule(Parameters, ABC):
    """A stop rule: its fields are its parameters. A rule has no error: it answers every case."""

    @abstractmethod
    def limits(self) -> dict[Fact, float]:
        """Return the limit that the rule sets on each fact that it limits."""

    def reads(self) -> list[Fact]:
        """Return the facts that ``check`` reads. piloop gets only those. By default, the facts that it limits."""
        return list(self.limits())

    @abstractmethod
    def check(self, case: StopCase) -> Go | Hold | Stop:
        """Return whether the campaign goes on, holds, or stops now."""


STOP_RULES = Capability('stop_rule', StopRule, tier=Tier.PURE, choice=Choice(Scope.CAMPAIGN, default='limits'))
"""The stop rule capability: every stop rule by name. The built-in rule is ``limits``
(``piloop.plugins.stop_rule.limits``)."""
