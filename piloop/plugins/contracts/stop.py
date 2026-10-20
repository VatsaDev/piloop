"""The contract of a stop rule: the types that piloop gives a stop rule, and the types that the rule returns.

On each tick of the daemon, piloop asks the stop rule whether the campaign ends. First it calls ``reads``: the rule
names the facts of the progress that it needs. piloop counts only those facts. Then it calls ``check`` with a
``StopCase``: the limits of ``[stop]``, those facts, and the rule's parameters. The rule returns a reason to stop, or
None. piloop stops the campaign when it gets a reason.

The rule reads nothing else: it is a pure provider. ``[plugins.stop_rule] provider`` in ``piloop.toml`` selects the
rule. The other keys of that table are the rule's parameters. Without the table, the rule is ``limits``.

This module imports only the Python standard library, the plugin host, and the metric types of the verdict contract.
A rule imports this module, and nothing else of piloop.
"""

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from piloop.plugins.contracts.verdict import Direction
from piloop.plugins.host import PureCapability, Selection


@dataclass(frozen=True)
class Limits:
    """``[stop]``: the limits that end the campaign. None is no limit."""

    target: float | None = None
    max_runs: int | None = None
    max_hours: float | None = None
    max_tokens: int | None = None
    plateau: int | None = None


class Fact(StrEnum):
    """A fact of the campaign's progress. Each fact costs queries on every tick, so a rule names the ones it reads."""

    HEAD = 'head'
    RUNS = 'runs'
    HOURS = 'hours'
    TOKENS = 'tokens'
    WITHOUT_KEEP = 'without_keep'
    KEEPS = 'keeps'


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
    """``hours``: the hours since the first run started, without the time that the project was paused. Also None
    before the first run."""
    tokens: int | None = None
    """``tokens``: the tokens that the agents of the project spent, summed over the agents that ended."""
    without_keep: int | None = None
    """``without_keep``: the finished runs that count since the latest keep, or since the project was last set
    active again."""
    keeps: int | None = None
    """``keeps``: the candidates of runs that are kept, on the trunk or on a branch. The baseline is not one."""


@dataclass(frozen=True)
class StopCase:
    """Everything that a stop rule reads."""

    primary: str
    """The name of the primary metric of the project's first track."""
    direction: Direction
    limits: Limits
    progress: Progress
    parameters: str = '{}'
    """The rule's parameters, the other keys of ``[plugins.stop_rule]``, as a JSON object. piloop checked them
    against the rule's ``Parameters`` when it loaded the configuration."""


class StopRule(Protocol):
    """The contract of a stop rule. A rule has no error: every case either ends the campaign or does not."""

    def reads(self, limits: Limits, parameters: str) -> list[Fact]:
        """Return the facts that ``check`` reads with these limits and parameters. The daemon counts only those."""
        ...

    def check(self, case: StopCase) -> str | None:
        """Return why the campaign ends now, or None to go on."""
        ...


STOP_RULES = PureCapability(
    'stop rule',
    group='piloop.stop_rules',
    contract=StopRule,
    selection=Selection('project', 'plugins.stop_rule.provider'),
)
"""The stop rule capability: every stop rule by name. The built-in rule is ``limits``
(``piloop.plugins.builtin.stop``)."""
DEFAULT = 'limits'
"""The rule of a project without ``[plugins.stop_rule]``."""
