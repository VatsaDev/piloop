"""Campaign configuration: ``piloop.toml`` at the repo root.

``piloop init`` loads the file and stores it in ``campaigns.config``, and the daemon schedules from that copy.
``PILOOP_CAMPAIGN_*`` environment variables override the file when it is loaded, so they apply to ``piloop init`` and
``piloop doctor``, not to the daemon.

``docs/reference/configuration.mdx`` is generated from these models. Run ``make docs`` after you change them.
"""

import hashlib
import json
import math
import os
import re
import tomllib
from dataclasses import dataclass
from enum import StrEnum
from operator import attrgetter
from pathlib import Path
from typing import Annotated, Any, Self, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    ValidationError,
    model_validator,
)
from pydantic_settings import (
    BaseSettings,
    InitSettingsSource,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

from piloop import deprecated
from piloop.ids import IMPLEMENTER, ROLE_NAME, RoleName, RungName, TrackName
from piloop.mcp import EnvName, McpServer, ServerName, env_names
from piloop.plugins.fact_source.contract import FACT_SOURCES, FactSource, FactSourceEntry, sources
from piloop.plugins.harness.contract import Harness, Harnesses, defaults
from piloop.plugins.host import Plugins, PluginTables, Scope
from piloop.plugins.metric import Direction
from piloop.plugins.role.contract import ROLES, ImplementerRole, RoleConfig, SweepRole, new_role
from piloop.plugins.runtime.contract import RuntimeName
from piloop.plugins.section.contract import Section, SectionEntry, Surface, chosen
from piloop.plugins.stop_rule.contract import STOP_RULES, Shape

CONFIG_FILE = 'piloop.toml'
BRIEF_FILE = 'brief.md'
TRANSCRIPT = 'transcript.txt'
"""The file in an agent's log directory with the end of its screen or output, written when it ends."""


class MetricRole(StrEnum):
    """How a metric affects the verdict. https://piloop.autoresearch.inc/concepts/verdict explains the rule."""

    PRIMARY = 'primary'
    GUARDRAIL = 'guardrail'
    WATCH = 'watch'


class Tolerance(BaseModel):
    """How far a metric may go: never past ``floor``, nor worse than the bar by more than ``margin`` or ``fraction``.

    A guardrail has one, and so has each stop of a rung of the ladder. A guardrail tests its regression on the confidence bound of the bootstrap when the evaluate command reports
    per-unit results or pairs, else on the aggregate. A stop of the ladder tests the rung's values.
    """

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    floor: float | None = None
    """Optional. The worst value allowed, whatever the bar: at least this for ``higher``, at most this for ``lower``."""
    margin: float | None = Field(default=None, ge=0)
    """The largest regression from the bar, in the metric's own units: ``20`` for 20 ms. Not with ``fraction``."""
    fraction: float | None = Field(default=None, gt=0)
    """The largest regression from the bar, as a fraction of the bar's value: ``0.1`` for 10%. Not with ``margin``."""

    @model_validator(mode='after')
    def _one_regression(self) -> Self:
        if self.margin is not None and self.fraction is not None:
            raise ValueError('set margin or fraction, not both')
        return self

    @property
    def regression(self) -> bool:
        """Whether it limits the regression from the bar: it has a ``margin`` or a ``fraction``."""
        return self.margin is not None or self.fraction is not None

    @property
    def empty(self) -> bool:
        """Whether it limits nothing."""
        return not self.regression and self.floor is None


class Ratio(BaseModel):
    """A metric that piloop computes from two that the evaluate command reports: ``numerator / denominator``.

    Each side's value is the ratio of its means. With per-unit results for both, the bootstrap takes the ratio of the
    unit sums in each resample, so the verdict tests the ratio with confidence; with pairs, the ratio of each pair.
    """

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    numerator: str
    """The metric above the line, which the evaluate command reports. Example: ``cost_usd``."""
    denominator: str
    """The metric below the line, which the evaluate command reports. Example: ``solved``."""


class Metric(Tolerance):
    """One metric that the evaluate command reports, by its name in ``metrics`` of its result.

    Its ``floor``, ``margin`` and ``fraction`` (``Tolerance``) apply to a guardrail, which needs a ``margin`` or a
    ``fraction``, and to each primary of a campaign with several tracks: when a candidate of another track is judged,
    that primary is a guardrail.
    """

    name: str
    """The name of the metric, as the evaluate command reports it."""
    direction: Direction
    """``higher`` when a larger value is better, ``lower`` when a smaller value is better."""
    role: MetricRole = MetricRole.WATCH
    """How the metric affects the verdict. A ``primary`` metric must beat the bar with confidence for a ``keep``, and
    each track has exactly one. A ``guardrail`` may get worse by at most its ``margin`` or ``fraction``, or the candidate is at most
    ``interesting``. It must never pass its ``floor``, or the candidate is a ``discard``. A ``watch`` metric is
    recorded and never affects the verdict."""
    track: TrackName | None = None
    """Primary metrics only. The name of the track (objective) that this metric is the primary of. When unset, the
    track has the metric's name, so a campaign with one primary needs nothing here. With several primaries, each one
    names its own track, and each track needs a ``[tracks.<name>]`` table."""
    ratio: Ratio | None = None
    """Optional. Primaries and guardrails only. piloop computes this metric from two others of ``[[metrics]]``, and the
    evaluate command does not report it. Example: ``{ numerator = "cost_usd", denominator = "solved" }``."""
    repeats: int | None = Field(default=None, ge=1)
    """Optional. Marks a noisy metric, such as a time: each evaluation of a run's candidate that this metric judges runs
    the evaluate command ``repeats`` times on the candidate and as many times on its bar, one after the other, and the
    verdict compares these pairs. Load then slows both sides alike. With several, the largest number counts. Allowed on
    a guardrail, and on a primary."""

    @model_validator(mode='after')
    def _check(self) -> Self:
        if self.role is MetricRole.GUARDRAIL and not self.regression:
            raise ValueError(f'guardrail {self.name!r} needs a margin or a fraction')
        if self.role is MetricRole.WATCH and self.repeats is not None:
            raise ValueError(f'{self.name!r}: repeats applies to primaries and guardrails only')
        if self.role is MetricRole.WATCH and not self.empty:
            raise ValueError(f'{self.name!r}: floor, margin and fraction apply to guardrails and primaries only')
        if self.role is MetricRole.WATCH and self.ratio is not None:
            raise ValueError(f'{self.name!r}: ratio applies to primaries and guardrails only')
        if self.role is not MetricRole.PRIMARY and self.track is not None:
            raise ValueError(f'{self.name!r}: track applies to primaries only')
        return self

    @property
    def track_name(self) -> TrackName:
        """The track that a primary decides: its ``track``, or else its own name."""
        return self.track or TrackName(self.name)

    def better(self, a: float, b: float) -> bool:
        """Whether ``a`` is better than ``b`` for this metric."""
        return self.direction.better(a, b)

    def gain(self, a: float, b: float) -> float:
        """How much better ``a`` is than ``b``, in the metric's units. Positive when ``a`` is better."""
        return self.direction.gain(a, b)


class Rung(BaseModel):
    """``[[ladder]]``: one rung of the evaluation ladder, which a run's evaluation climbs before the full panel.

    A rung without ``evaluate`` is reported by the evaluate command itself, as a line with ``"rung": "<name>"`` before
    its full result. A rung with ``evaluate`` runs that command first, which reports one line. After each rung, the
    climb of a run's own version stops when ``stop`` says so, and piloop ends the evaluate command.
    """

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    name: RungName = Field(pattern=r'^[a-z][a-z0-9_-]{0,31}$')
    """The rung's name, which its line names and which the measurements of it record."""
    stop: dict[str, Tolerance] = Field(min_length=1)
    """When the climb stops at this rung, by metric of ``[[metrics]]``. Example:
    ``{ accuracy = { margin = 0.03 }, latency_ms = { fraction = 0.1 } }``."""
    evaluate: list[str] | None = Field(default=None, min_length=1)
    """Optional. A command (argv) of its own, which runs before the evaluate command and reports this rung alone. The
    rungs that have one come first in ``[[ladder]]``."""

    @model_validator(mode='after')
    def _each_stop_limits(self) -> Self:
        if empty := sorted(name for name, test in self.stop.items() if test.empty):
            raise ValueError(f'stop of {empty}: set floor, margin or fraction')
        return self


class TrackConfig(BaseModel):
    """``[tracks.<name>]``: one track of the campaign. A track is an objective with its own primary metric and brief.

    A track has the name of its primary's ``track``, or else the name of the primary. All tracks build on one trunk. A
    run starts from the head, which is the latest keep on the trunk. A keep on any track must also hold the primary of
    every other track within that primary's ``margin`` and ``floor``. A campaign with one primary has one track and
    needs no table. A campaign with several primaries needs one table for each track, and the shares must sum to 1.

    Experimental: campaigns with more than one track.
    """

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    share: float = Field(default=1, ge=0, le=1)
    """The fraction of the ``max_concurrent`` of each role that does runs that the ideas of this track get. A free slot
    of a role goes to the track that is furthest below its share and has an idea that can run, and then to the most
    urgent idea of that track. A track with no idea that can run gives its slot to another track. A share of 0 stops the track: its ideas
    wait."""
    addendum: str | None = None
    """A repo-relative file that is appended to the brief of this track's runs, after ``[roles.implementer] addendum``. Use
    it to say how work on this objective is done, for example "profile first" or "keep the output identical"."""


@dataclass(frozen=True)
class Track:
    """One track, as the daemon judges and schedules it."""

    name: TrackName
    primary: Metric
    share: float
    addendum: str | None
    guardrails: tuple[Metric, ...]
    """What a candidate of this track must hold: the campaign's guardrails, then the primary of every other track, each
    with its margin and floor."""

    @property
    def repeats(self) -> int | None:
        """The pairs of each evaluation of this track's candidates: the largest ``repeats`` of the metrics that judge.

        None: one sample, unpaired.
        """
        return max((m.repeats for m in (self.primary, *self.guardrails) if m.repeats is not None), default=None)


"""The form of a role name: the agents' signature and ``agent_sessions.kind``."""
RESERVED_NAMES = frozenset({'run', 'orchestrator', 'human', 'daemon'})
"""Names that already sign writes: a run, and the actors that are not roles."""


class StatusConfig(BaseModel):
    """``[status]``: what ``piloop status``, the API and a UI show of the campaign."""

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    sections: list[SectionEntry] | None = None
    """The sections, in order: a list of ``{ provider = "<section>", ... }`` entries. When unset, every installed
    section that status shows by default, in their order (``Section.SHOWN_IN``, ``Section.ORDER``): the campaign
    overview."""

    def shown(self) -> list[Section]:
        """The sections that status shows: ``sections``, else the defaults of status."""
        return chosen(self.sections, Surface.STATUS)


class Roles(RootModel[dict[str, RoleConfig]]):
    """``[roles]``: one table for each role, by its name, over the role's defaults.

    Every role is a provider (``ROLES``): piloop's (implementer, watchdog, research, scout) and those that packages add,
    each with its table over its defaults, then the roles that the file defines. A table with a new name defines a new
    role: a role that does runs (``ImplementerRole``) with ``runs = true``, else a sweeping role (``SweepRole``).
    """

    model_config = ConfigDict(frozen=True)
    root: Annotated[dict[str, RoleConfig], ROLES.tables(new=new_role)] = Field(default={}, validate_default=True)

    @model_validator(mode='before')
    @classmethod
    def _no_worker(cls, data: object) -> object:
        if isinstance(data, dict) and 'worker' in data:
            raise ValueError(deprecated.removed_worker())
        return data

    @model_validator(mode='after')
    def _check(self) -> Self:
        for name in self.root.keys() - ROLES.providers().keys():
            if not re.fullmatch(ROLE_NAME, name) or name in RESERVED_NAMES:
                raise ValueError(
                    f'{name!r} is not a role name: use lowercase letters, digits, _ and - ({ROLE_NAME}), and none of'
                    f' {sorted(RESERVED_NAMES)}'
                )
        return self

    def all(self) -> dict[RoleName, RoleConfig]:
        """Every role by name: the providers, then the roles that the file defines, in its order.

        ``_check`` makes each key a ``RoleName``.
        """
        return {RoleName(name): role for name, role in self.root.items()}

    def sweeping(self) -> dict[RoleName, SweepRole]:
        """The sweeping roles by name, in the order of ``all``."""
        return {name: role for name, role in self.all().items() if isinstance(role, SweepRole)}

    def doing_runs(self) -> dict[RoleName, ImplementerRole]:
        """The roles that do runs by name, in the order in which they fill their slots: the implementer last."""
        mine = {name: role for name, role in self.all().items() if isinstance(role, ImplementerRole)}
        implementer = mine.pop(IMPLEMENTER)
        return {**mine, IMPLEMENTER: implementer}


class CampaignConfig(BaseModel):
    """Everything piloop needs to know about a campaign repo."""

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    name: str
    """A short name for the campaign: letters, digits, ``.``, ``_`` and ``-``, not all digits. A campaign gets it when
    it is registered, unless the registration names another, and piloop names its git refs after it."""
    metrics: list[Metric] = Field(min_length=1)
    """Every metric that the evaluate command reports, each with its ``role``. Each track has one ``primary``, so a campaign
    with one track has exactly one."""
    tracks: dict[TrackName, TrackConfig] = {}
    """``[tracks.<name>]``: the share and addendum of each track. Needed only with several primaries."""
    evaluate: list[str] = Field(min_length=1)
    """The command (argv) that scores a commit. It runs in a clean worktree of the commit and writes one JSON line,
    ``{"metrics": {"<name>": <value>}}``, to ``$PILOOP_RESULT_FILE``."""
    runtime: RuntimeName = 'subprocess'
    """Where the agents run: the name of a runtime, which starts, watches, steers and stops them. A package can
    add a runtime with a ``piloop.runtimes`` entry point. A role can override it (``[roles.<name>] runtime``)."""
    harnesses: list[str] = Field(default_factory=defaults, min_length=1)
    """The enabled harnesses. Runs use them in turn, unless an idea pins one. When unset, the harnesses that are on by
    default: ``pi``."""
    model: dict[str, str]
    """The model id for each harness, for example
    ``{pi = "anthropic/claude-opus-5-5", claude = "claude-opus-5-5", codex = "gpt-6.1-sol"}``."""
    harness: Harnesses = Field(default={}, validate_default=True)
    """``[harness.<name>]``: the table of each harness over its defaults: the built-in harnesses (``pi``, ``claude``,
    ``codex``), those that packages add, and new ones. A table with a new name and ``provider = "<harness>"`` is that
    harness over its defaults, with another name; without ``provider``, it runs its argv and does nothing else."""
    max_concurrent: dict[str, Annotated[int, Field(ge=1)]] = {}
    """The maximum number of agents of one harness, across all roles of the campaign, that are started or queued to
    start. Example: ``{claude = 2}`` when the provider of its model limits the request rate. When it is the turn of a
    harness at its limit, the slot goes to the next harness of the rotation. An idea or request that pins that harness
    waits. When unset, only the limits of the roles apply."""
    evaluate_timeout: float = 600
    """The number of seconds before the evaluate command is stopped. The evaluation is then a crash."""
    ladder: list[Rung] = []
    """``[[ladder]]``: the rungs that a run's evaluation of its own version climbs before the full panel, cheapest
    first. The climb stops at the first rung whose ``stop`` holds. A baseline, a head, a bar, a reference and a
    remeasure climb every rung and never stop."""
    roles: Roles = Field(default={}, validate_default=True)
    """``[roles.<name>]``: the table of each role over its defaults: the built-in roles (implementer, watchdog,
    research, scout), the roles that packages add, and the roles that you define."""
    link: list[str] = []
    """Repo-relative paths, ignored by git, that piloop puts into every worktree read-only, for example data or virtual
    environments. Each job gets them as they are when it is made."""
    context: list[str] = []
    """Repo-relative files or globs (for example ``docs/error-analysis-*.md``) that hold the campaign's analysis:
    error analyses, comparisons with a reference, evaluation reports. The research pack lists each one with its
    outline, newest first, so that every sweep that reads the pack reads them before it proposes work."""
    mcp: dict[ServerName, McpServer] = {}
    """``[mcp.<name>]``: the MCP servers that a role can list in ``[roles.<name>] mcp``."""
    status: StatusConfig = StatusConfig()
    """``[status]``: the sections that ``piloop status``, the API and a UI show."""
    facts: list[FactSourceEntry] | None = None
    """``[[facts]]``: where the campaign's facts come from, which its stop rule and its sections read: a list of
    ``{ provider = "<source>", ... }`` entries. When unset, every installed source that is on by default: ``campaign``,
    the hours and tokens of any campaign, and ``optimization``, its research. A list that adds a source lists those
    too, to keep their facts."""
    plugins: Annotated[Plugins, PluginTables(Scope.CAMPAIGN)] = {}
    """``[plugins.<capability>]``: the provider of each capability of the campaign, by its name (``provider``), and its
    parameters, the other keys of the table. A capability without a table, or a table without ``provider``,
    has its default provider."""

    @model_validator(mode='after')
    def _check(self) -> Self:
        self._check_tracks()
        self._check_ratios()
        self._check_ladder()
        self._check_facts()
        self._check_stop_rule()
        self._check_harnesses(self.harnesses, self.model, 'campaign')
        if unknown := [h for h in self.max_concurrent if h not in self.harness]:
            raise ValueError(f'max_concurrent: unknown harnesses {unknown}; known: {sorted(self.harness)}')
        roles = self.roles.all()
        for name in roles:
            self._check_harnesses(self.role_harnesses(name), self.role_models(name), f'roles.{name}')
        for name in roles:
            self._check_mcp(name)
        addenda = [a for role in roles.values() if (a := role.addendum) is not None]
        addenda += [t.addendum for t in self.tracks.values() if t.addendum is not None]
        for rel in [*self.link, *addenda, *self.context]:
            if Path(rel).is_absolute() or '..' in Path(rel).parts:
                raise ValueError(f'{rel!r} must be a relative path inside the repo')
        return self

    def _check_ratios(self) -> None:
        """Each ratio divides two other metrics that the evaluate command reports, and no stop of the ladder reads one."""
        reported = set(self.reported)
        for m in self.metrics:
            if m.ratio is None:
                continue
            parts = (m.ratio.numerator, m.ratio.denominator)
            if missing := [p for p in parts if p not in reported]:
                raise ValueError(f'{m.name!r}: ratio of {missing}, which are not metrics that evaluate reports')
            if len(set(parts)) == 1:
                raise ValueError(f'{m.name!r}: ratio of {parts[0]!r} by itself')
        ratios = {m.name for m in self.metrics if m.ratio is not None}
        for rung in self.ladder:
            if named := sorted(rung.stop.keys() & ratios):
                raise ValueError(f'ladder.{rung.name}.stop: {named} are ratios; stop on their parts')

    @property
    def reported(self) -> list[str]:
        """The metrics that the evaluate command may report: every one but the ratios, which piloop computes."""
        return [m.name for m in self.metrics if m.ratio is None]

    @property
    def required(self) -> list[str]:
        """The metrics that the full panel must report: the primaries and guardrails, and the parts of their ratios."""
        judging = [m for m in self.metrics if m.role is not MetricRole.WATCH]
        names = [p for m in judging for p in ((m.ratio.numerator, m.ratio.denominator) if m.ratio else (m.name,))]
        return list(dict.fromkeys(names))

    def _check_ladder(self) -> None:
        """Each rung has its own name, stops on metrics of ``[[metrics]]``, and the rungs with a command come first."""
        names = [rung.name for rung in self.ladder]
        if repeated := sorted({n for n in names if names.count(n) > 1}):
            raise ValueError(f'ladder: rungs {repeated} appear more than once')
        known = {m.name for m in self.metrics}
        for rung in self.ladder:
            if unknown := sorted(rung.stop.keys() - known):
                raise ValueError(f'ladder.{rung.name}.stop: {unknown} are not in [[metrics]]')
        owned = [rung.evaluate is not None for rung in self.ladder]
        if owned != sorted(owned, reverse=True):
            raise ValueError('ladder: the rungs with their own evaluate come first')

    def _check_stop_rule(self) -> None:
        """Check that the stop rule serves this campaign (``StopRule.check_config``), and has a source for each fact."""
        rule = STOP_RULES.selected(self.plugins)
        name = STOP_RULES.name_of(rule)
        if (why := rule.check_config(Shape(tracks=len(self.all_tracks)))) is not None:
            raise ValueError(f'the stop rule {name!r} {why}')
        supplied = {fact for source in self.fact_sources() for fact in source.supplies()}
        if missing := [fact.name for fact in rule.reads() if fact not in supplied]:
            raise ValueError(
                f'the stop rule {name!r} reads {", ".join(missing)}, which no fact source of this campaign supplies: add'
                ' one to [[facts]] that does'
            )

    def _check_facts(self) -> None:
        """Check that no two fact sources supply the same fact."""
        seen: dict[str, FactSource] = {}
        for source in self.fact_sources():
            for fact in source.supplies():
                if (other := seen.setdefault(fact.name, source)) is not source:
                    names = f'{FACT_SOURCES.name_of(other)} and {FACT_SOURCES.name_of(source)}'
                    raise ValueError(f'the fact {fact.name!r} comes from two sources of [[facts]]: {names}')

    def fact_sources(self) -> list[FactSource]:
        """The campaign's fact sources: ``facts``, else every installed source that is on by default."""
        return sources(self.facts)

    def _check_tracks(self) -> None:
        """Check that each track has one primary.

        With several tracks, each primary also needs a margin, each track needs a table, and the shares must sum to 1.
        """
        primaries = [m for m in self.metrics if m.role is MetricRole.PRIMARY]
        if not primaries:
            raise ValueError('at least one metric must be primary')
        names = [m.track_name for m in primaries]
        if len(primaries) == 1 and not primaries[0].empty:
            raise ValueError(
                f'{primaries[0].name!r}: floor, margin and fraction on a primary apply only with several tracks'
            )
        if len(primaries) > 1:
            if dupes := sorted({n for n in names if names.count(n) > 1}):
                raise ValueError(f'tracks {dupes} have more than one primary: give each primary its own track')
            if unguarded := [m.name for m in primaries if not m.regression]:
                raise ValueError(
                    f'primaries {unguarded} need a margin or a fraction: with several tracks each guards the others'
                )
            if untabled := [n for n in names if n not in self.tracks]:
                raise ValueError(f'tracks {untabled} need a [tracks.<name>] table with their share')
        if unknown := sorted(set(self.tracks) - set(names)):
            raise ValueError(f'[tracks] {unknown} name no track a primary declares; tracks are {names}')
        total = sum(self.tracks[n].share if n in self.tracks else 1 for n in names)
        if not math.isclose(total, 1):
            raise ValueError(f'track shares sum to {total:g}, not 1')

    def _check_harnesses(self, harnesses: list[str], models: dict[str, str], where: str) -> None:
        unknown = [h for h in harnesses if h not in self.harness]
        if unknown:
            raise ValueError(f'{where}: unknown harnesses {unknown}; known: {sorted(self.harness)}')
        missing = [h for h in harnesses if h not in models]
        if missing:
            raise ValueError(f'{where}: no model configured for harnesses {missing}')

    def _check_mcp(self, name: RoleName) -> None:
        """Every server the role lists exists, and every harness the role may run can take them in its argv."""
        unknown = [n for n in self.role(name).mcp if n not in self.mcp]
        if unknown:
            raise ValueError(f'roles.{name}.mcp: no [mcp.<name>] table for {unknown}')
        if not self.role(name).mcp:
            return
        for harness in self.role_harnesses(name):
            if why := self.harness_for(harness).refuses_servers():
                raise ValueError(f'roles.{name}.mcp: harness {harness!r}: {why}; or take it out of the rotation')

    def role_servers(self, name: RoleName) -> dict[str, McpServer]:
        """The MCP servers that the agents of a role get, by name."""
        return {server: self.mcp[server] for server in self.role(name).mcp}

    def role_secrets(self, name: RoleName, harness: str) -> dict[EnvName, EnvName]:
        """The variables of an agent of a role on ``harness`` that take a secret, with the secret's name.

        Each variable of the role's MCP servers is the secret of its own name, unless the harness's ``env`` gives it a
        value or another secret.
        """
        definition = self.harness_for(harness)
        servers = env_names(self.role_servers(name).values())
        return {n: n for n in servers if n not in definition.env} | definition.secrets

    def role(self, name: RoleName) -> RoleConfig:
        """The ``[roles.<name>]`` table. The keys that only one role has need ``self.roles.<name>``.

        Raises:
            KeyError: ``piloop.toml`` has no such role.
        """
        return self.roles.all()[name]

    def role_harnesses(self, name: RoleName) -> list[str]:
        """The rotation a role uses: its own list, or else the campaign's."""
        return self.role(name).harnesses or self.harnesses

    def role_models(self, name: RoleName) -> dict[str, str]:
        """The model for each harness of a role: the campaign's map, with the role's overrides on top."""
        return {**self.model, **(self.role(name).model or {})}

    def role_runtime(self, name: RoleName) -> str:
        """The name of the runtime a role uses: its own, or else the campaign's."""
        return self.role(name).runtime or self.runtime

    @property
    def run_slots(self) -> int:
        """The number of runs that can be open at the same time: the ``max_concurrent`` of every role that does runs."""
        return sum(role.max_concurrent for role in self.roles.doing_runs().values())

    @property
    def guardrails(self) -> list[Metric]:
        """The campaign's guardrails. A candidate of any track must stay within their margins and floors."""
        return [m for m in self.metrics if m.role is MetricRole.GUARDRAIL]

    @property
    def all_tracks(self) -> list[Track]:
        """Every track, in the order of its primary in ``metrics``. The first one is the default."""
        primaries = [m for m in self.metrics if m.role is MetricRole.PRIMARY]
        return [
            Track(
                name=m.track_name,
                primary=m,
                share=(table := self.tracks.get(m.track_name, TrackConfig())).share,
                addendum=table.addendum,
                guardrails=(*self.guardrails, *(p for p in primaries if p is not m)),
            )
            for m in primaries
        ]

    @property
    def multitrack(self) -> bool:
        """Whether the campaign has more than one track. Only then do the renderers show track names."""
        return len(self.all_tracks) > 1

    def track(self, name: TrackName) -> Track:
        """The track called ``name``.

        Raises:
            KeyError: ``piloop.toml`` has no such track. The message lists the tracks it has.
        """
        found = next((t for t in self.all_tracks if t.name == name), None)
        if found is None:
            raise KeyError(f'no track {name!r} in piloop.toml; tracks: {", ".join(t.name for t in self.all_tracks)}')
        return found

    @property
    def evaluator_definition(self) -> dict[str, object]:
        """What decides a verdict's numbers: the evaluate command, the linked inputs and the judging metrics.

        ``judging`` has the name and direction of each primary and guardrail, by name, and the parts of a ratio. ``ladder``, only when there is
        one, has each rung's name and command: a rung's values compare only under the same rungs. Its ``stop`` is the
        policy of the climb, as a margin is of the verdict. A watch metric never decides a verdict, so adding, removing or redirecting one measures nothing again: the
        older measurements just lack it. Promoting a watch metric to a guardrail, or demoting a guardrail, does. Roles
        among the judging metrics, margins, floors and the verdict rule are the policy of the verdict, not part of the
        score.
        """
        judging = sorted((m for m in self.metrics if m.role is not MetricRole.WATCH), key=attrgetter('name'))
        rungs = [{'name': rung.name, 'evaluate': rung.evaluate} for rung in self.ladder]
        return {
            'evaluate': list(self.evaluate),
            'link': list(self.link),
            'judging': [
                {'name': m.name, 'direction': m.direction.value}
                | ({'ratio': m.ratio.model_dump()} if m.ratio is not None else {})
                for m in judging
            ],
        } | ({'ladder': rungs} if rungs else {})

    @property
    def evaluator_key(self) -> str:
        """A short hash of ``evaluator_definition``. It identifies an ``evaluators`` row."""
        return hashlib.sha256(json.dumps(self.evaluator_definition, sort_keys=True).encode()).hexdigest()[:16]

    def harness_for(self, name: str) -> Harness:
        """The harness called ``name``.

        Raises:
            KeyError: The campaign has no such harness.
        """
        return self.harness[name]


class ConfigError(ValueError):
    """A ``piloop.toml`` that does not load. The message names the file, each bad key and value, and the fix."""


DOCS = 'https://piloop.autoresearch.inc/reference/configuration'
FIXES = {
    'extra_forbidden': f'delete the key or correct its spelling; {DOCS} lists every key',
    'missing': f'add the key; {DOCS} says what it takes',
}


def explain(where: object, error: ValidationError) -> str:
    """Pydantic's errors as one line per bad key: the key and value (``metrics[0].role = 'boss'``), problem and fix."""
    lines = [f'{where} is not a valid piloop config ({error.error_count()} problem(s)):']
    for e in error.errors():
        key = ''.join(f'[{part}]' if isinstance(part, int) else f'.{part}' for part in e['loc']).lstrip('.')
        value = '' if e['type'] == 'missing' or isinstance(e['input'], dict | list) else f' = {e["input"]!r}'
        message = e['msg'].removeprefix('Value error, ')
        fix = FIXES.get(e['type'], f'{DOCS} describes the key')
        lines.append(f'  {key or "(the file)"}{value}: {message}. Fix: {fix}')
    return '\n'.join(lines)


def prompt_files(role: RoleConfig) -> dict[str, str]:
    """Return the prompt files that a role's table sets, by key: ``prompt``, and a sweep's two prompts."""
    keys = {'prompt': role.prompt}
    if isinstance(role, SweepRole):
        keys |= {'incident_prompt': role.incident_prompt, 'routine_prompt': role.routine_prompt}
    return {key: rel for key, rel in keys.items() if rel is not None}


def load_campaign_config(campaign: Path, **overrides: object) -> CampaignConfig:
    """Load ``<campaign>/piloop.toml``.

    ``overrides`` take precedence over ``PILOOP_CAMPAIGN_*`` environment variables, which take precedence over the
    file. Deprecated keys still load through ``piloop.deprecated``, with a ``PiloopDeprecationWarning``.

    Raises:
        FileNotFoundError: The campaign has no ``piloop.toml``.
        ConfigError: The file is not valid TOML, not a valid config, or names a prompt file that does not exist.
    """
    toml = campaign / CONFIG_FILE
    if not toml.is_file():
        raise FileNotFoundError(toml)
    try:
        raw = tomllib.loads(toml.read_text())
    except tomllib.TOMLDecodeError as e:
        raise ConfigError(f'{toml} is not valid TOML: {e}') from e
    if (removed := deprecated.removed_env(os.environ)) is not None:
        raise ConfigError(f'{toml} is not loaded: {removed}')
    deprecated.apply(toml, raw)

    class _Loader(BaseSettings, CampaignConfig):
        model_config = SettingsConfigDict(env_prefix='PILOOP_CAMPAIGN_', env_nested_delimiter='__', extra='forbid')

        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return init_settings, env_settings, InitSettingsSource(settings_cls, raw)

    # pydantic validates the values; the cast only tells the type checker to let them through
    kwargs = cast(dict[str, Any], {k: v for k, v in overrides.items() if v is not None})
    try:
        cfg = CampaignConfig.model_validate(_Loader(**kwargs).model_dump())
    except ValidationError as e:
        raise ConfigError(explain(toml, e)) from e
    missing = [
        f'  roles.{name}.{key} = {rel!r}: no such file. Fix: create it, or correct the path'
        for name, role in cfg.roles.all().items()
        for key, rel in prompt_files(role).items()
        if not (campaign / rel).is_file()
    ]
    if missing:
        raise ConfigError('\n'.join([f'{toml} is not a valid piloop config ({len(missing)} problem(s)):', *missing]))
    return cfg
