"""The contract of a stop rule: the types that piloop gives a stop rule, and the types that the rule returns.

On each pass of the daemon, piloop asks the stop rule of each active campaign whether the campaign goes on, holds, or
stops. It asks in two steps. First it calls ``reads``: the rule names the facts that it needs. piloop gets only those
facts. Then it calls ``check`` with a ``StopCase``: those facts. The rule returns one answer:

- ``Go``: piloop starts what the campaign needs, within what ``left`` says is left of each fact.
- ``Hold``: piloop starts no agent and no evaluation for the campaign. Work that runs finishes. The hold is not a
  pause: on each pass piloop asks again, and the hold ends when the rule answers ``Go``.
- ``Stop``: piloop stops the campaign.
- ``Escalate``: piloop tells a human why, one priority-0 escalation for each distinct reason, and then acts on its
  ``then``: by default, the campaign goes on. A rule escalates itself: piloop posts no escalation of its own for a
  hold.

``describe`` returns the rule's own view of the campaign against its limits, for ``piloop status`` and the briefs:
piloop formats none of the facts itself. ``while_held`` says what the closing review of a stop does while the rule
holds.

The rule reads nothing else: it is a pure provider. ``[plugins.stop_rule] provider`` in ``piloop.toml`` selects the
rule, and the other keys of that table are its fields, its limits among them. Without the table, the rule is
``limits``. ``check_config`` refuses a campaign that the rule cannot serve, when the file loads.

A fact is a typed key (``piloop.plugins.fact_source.facts``) that a fact source of the campaign supplies, by its name.
Every fact that a rule reads needs a source in the campaign.

This module imports only the Python standard library, the plugin host, the facts and the metric types. A rule imports
this module and the facts it reads, and nothing else of piloop.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import StrEnum

from piloop.plugins.fact_source.facts import Fact, Facts, FactValue
from piloop.plugins.host import Capability, One, Parameters, Scope, Tier
from piloop.plugins.metric import Direction


@dataclass(frozen=True)
class StopCase:
    """Everything that a stop rule reads."""

    primary: str
    """The name of the primary metric of the campaign's first track."""
    direction: Direction
    """Whether a larger or a smaller value of the primary metric is better."""
    facts: Facts
    """The facts that the rule reads (``reads``), from the campaign's fact sources."""


@dataclass(frozen=True)
class Shape:
    """What a stop rule may refuse of a campaign when its file loads (``StopRule.check_config``)."""

    tracks: int
    """The number of tracks: one per primary metric."""


@dataclass(frozen=True)
class Go:
    """The campaign goes on: piloop starts what it needs, within ``left``."""

    left: Facts = field(default_factory=Facts)
    """How much of each fact is left before the rule stops the campaign, by fact. piloop starts no more runs than
    ``RUNS`` leaves, and cuts each agent's timeout where ``HOURS`` runs out. A fact that is not in it does not bound
    what piloop starts."""


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


@dataclass(frozen=True)
class Escalate:
    """piloop posts one escalation at priority 0 for each distinct ``reason``, and then acts on ``then``."""

    reason: str
    """What a human must decide, for the escalation: ``Stop rule <name>: <reason>``."""
    then: Go | Hold | Stop = GO
    """What piloop does with the campaign: by default it goes on."""


class WhileHeld(StrEnum):
    """What the closing review does when the campaign stopped while its stop rule held launches."""

    RUN = 'run'
    """The review runs, also while the hold lasts."""
    SKIP = 'skip'
    """No review: the stop ends at once, and the daemon posts an escalation that says so."""
    WAIT = 'wait'
    """The review starts when the hold ends. The daemon goes on asking the stop rule until it does."""


class StopRule(Parameters, ABC):
    """A stop rule: its fields are its parameters. A rule has no error: it answers every case."""

    while_held: WhileHeld = WhileHeld.RUN
    """What the closing review does when the campaign stops (``piloop campaign stop``) while the rule holds its
    launches: ``run`` it anyway, ``skip`` it, or ``wait`` until the hold ends."""

    @abstractmethod
    def reads(self) -> list[Fact[FactValue]]:
        """Return the facts that ``check`` and ``describe`` read: piloop gets only those."""

    def describe(self, case: StopCase) -> list[str]:
        """Return where the campaign stands against the rule, in short parts: ``run 7/200``, ``2.1h/12h``.

        ``piloop status`` and the briefs join them into the campaign's progress line. By default, none.
        """
        return []

    @abstractmethod
    def check(self, case: StopCase) -> Go | Hold | Stop | Escalate:
        """Return whether the campaign goes on, holds or stops, or tell a human why and then do one of those."""

    def check_config(self, shape: Shape) -> str | None:
        """Return why the rule cannot serve a campaign of ``shape``, or None. By default, it serves every campaign."""
        return None


STOP_RULES = Capability('stop_rule', StopRule, tier=Tier.PURE, table=One(Scope.CAMPAIGN, default='limits'))
"""The stop rule capability: every stop rule by name. The built-in rule is ``limits``
(``piloop.plugins.stop_rule.limits``)."""
