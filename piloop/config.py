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
from pathlib import Path, PurePosixPath
from typing import Annotated, Any, Self, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic_settings import (
    BaseSettings,
    EnvSettingsSource,
    InitSettingsSource,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)
from sqlalchemy.orm import Session

from piloop import deprecated
from piloop.db.models import AgentSession, Campaign
from piloop.harnesses import DEFAULT_HARNESSES, Harness
from piloop.ids import IMPLEMENTER, RESEARCH, ROLE_NAME, SCOUT, WATCHDOG, RoleName, TrackName
from piloop.mcp import McpServer, ServerName
from piloop.plugins.host import Plugins, PluginTables, Scope
from piloop.plugins.metric import Direction
from piloop.plugins.role.contract import ROLES, ImplementerRole, RoleConfig, RoleEntry, SweepRole
from piloop.plugins.role.research import ResearchRole
from piloop.plugins.role.scout import ScoutRole
from piloop.plugins.role.watchdog import WatchdogRole
from piloop.plugins.runtime.contract import RuntimeName
from piloop.plugins.stop_rule.contract import STOP_RULES, Fact

CONFIG_FILE = 'piloop.toml'
BRIEF_FILE = 'brief.md'
TRANSCRIPT = 'transcript.txt'
"""The file in an agent's log directory with the end of its screen or output, written when it ends."""


class MetricRole(StrEnum):
    """How a metric affects the verdict. https://piloop.autoresearch.inc/concepts/verdict explains the rule."""

    PRIMARY = 'primary'
    GUARDRAIL = 'guardrail'
    WATCH = 'watch'


class Metric(BaseModel):
    """One metric. The evaluate command prints it as a ``METRIC name=value`` line."""

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    name: str
    """The name of the metric, as the evaluate command prints it."""
    direction: Direction
    """``higher`` when a larger value is better, ``lower`` when a smaller value is better."""
    role: MetricRole = MetricRole.WATCH
    """How the metric affects the verdict. A ``primary`` metric must beat the bar with confidence for a ``keep``, and
    each track has exactly one. A ``guardrail`` may get worse by at most its ``margin``, or the candidate is at most
    ``interesting``. It must never pass its ``floor``, or the candidate is a ``discard``. A ``watch`` metric is
    recorded and never affects the verdict."""
    track: TrackName | None = None
    """Primary metrics only. The name of the track (objective) that this metric is the primary of. When unset, the
    track has the metric's name, so a campaign with one primary needs nothing here. With several primaries, each one
    names its own track, and each track needs a ``[tracks.<name>]`` table."""
    margin: float | None = Field(default=None, ge=0)
    """The largest regression from the bar that a candidate may show, in the metric's own units (an absolute value,
    not a fraction). Required on a guardrail. Also required on every primary of a campaign with several tracks: when a
    candidate of another track is judged, that primary is a guardrail. When the evaluate command writes per-unit
    results, the margin is tested on the confidence bound of the bootstrap. Otherwise it is tested on the aggregate."""
    floor: float | None = None
    """Optional. The worst value a candidate may have: at least this value for ``higher``, at most this value for
    ``lower``. It is a hard limit on the aggregate, independent of the bar, and only an edit of ``piloop.toml`` changes
    it. Allowed on a guardrail, and on a primary of a campaign with several tracks, where it applies to the candidates
    of the other tracks."""

    repeats: int | None = Field(default=None, ge=1)
    """Optional. Marks a noisy metric, such as a time: each evaluation of a run's candidate that this metric judges runs
    the evaluate command ``repeats`` times on the candidate and as many times on its bar, one after the other, and the
    verdict compares these pairs. Load then slows both sides alike. With several, the largest number counts. Allowed on
    a guardrail, and on a primary."""

    @model_validator(mode='after')
    def _check(self) -> Self:
        if self.role is MetricRole.GUARDRAIL and self.margin is None:
            raise ValueError(f'guardrail {self.name!r} needs a margin')
        if self.role is MetricRole.WATCH and self.repeats is not None:
            raise ValueError(f'{self.name!r}: repeats applies to primaries and guardrails only')
        if self.role is MetricRole.WATCH and (self.margin is not None or self.floor is not None):
            raise ValueError(f'{self.name!r}: margin and floor apply to guardrails and primaries only')
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


class ClosingReview(StrEnum):
    """What happens to the closing review of a campaign that stops while ``[spend]`` holds its launches."""

    RUN = 'run'
    """The review runs, also past the limit or with an unknown spend."""
    SKIP = 'skip'
    """No review: the stop ends at once, and the daemon posts an escalation that says so."""
    WAIT = 'wait'
    """The review starts when the hold ends. The daemon goes on reading the spend until it starts."""


class Spend(BaseModel):
    """``[spend]``: what the campaign costs outside piloop, for example GPUs or APIs, in USD.

    Every ``interval`` seconds, the daemon runs ``command`` to read the spend from the campaign (``campaigns.spend_usd``,
    ``spend_error``, ``spend_at``). When the spend passes a ``notices`` threshold, the daemon posts one note to the
    inbox at priority 1. ``[plugins.stop_rule] max_spend`` is the limit: there, the daemon posts one escalation at
    priority 0, and the stop rule holds the campaign. A command that fails holds the campaign in the same way, with one
    escalation for each series of failures: an unknown spend is never read as $0.
    """

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    command: list[str] = Field(min_length=1)
    """The command (argv) that prints the campaign's spend so far in USD, as one number. It runs at the repo root with
    the daemon's environment, which is where its secrets come from. It must exit with code 0 within one minute.
    Example: ``["uv", "run", "scripts/spend.py"]``."""
    notices: list[Annotated[float, Field(gt=0)]] = []
    """Amounts in USD below the spend limit. When the spend passes one, the daemon posts one note to the inbox."""
    interval: float = Field(default=300, gt=0)
    """The number of seconds between two runs of ``command``."""
    closing_review: ClosingReview = ClosingReview.RUN
    """What happens to the closing review when the campaign stops while launches are held (``piloop campaign
    stop``): ``run`` it anyway, ``skip`` it, or ``wait`` until the hold ends."""


class Memory(BaseModel):
    """``[memory]``: what past campaigns leave for the next ones. See the research memory page."""

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    documents: list[str] = []
    """Repo-relative globs of text documents, for example ``["docs/evidence/**/*.md"]``: campaign reports, orchestrator
    logs, design notes. ``piloop init`` and the stop of the campaign copy them into the ``documents`` table, where
    every agent of every campaign can search them. Never name a document that is derived from validation or test
    data."""
    wake_budget: int = Field(default=400, ge=0)
    """The size in tokens (about 4 characters each) of the overview that opens the briefs of implementers and research
    sweeps: the current lesson of each mechanism family on the same repo, from the newest campaign that tried it, one
    line each. Lessons in the campaign's own metrics come first (``lessons.metric``), then
    lessons in related metrics, then the rest. Within those, dead or promising come before untested, then the larger
    effects of each metric, then newer campaigns: effects in different metrics are never compared by size. The brief
    then says how to read more. A role can change it with ``[roles.<kind>] wake_budget``. 0
    turns it off."""

    @field_validator('documents')
    @classmethod
    def _inside_the_repo(cls, globs: list[str]) -> list[str]:
        for pattern in globs:
            path = PurePosixPath(pattern)
            if not path.parts or path.is_absolute() or '..' in path.parts:
                raise ValueError(f'{pattern!r} is not a glob inside the repo: name files relative to its root, no ..')
        return globs


class Roles(BaseModel):
    """One table for each role. A built-in role without a table uses its defaults.

    A table with a new name defines a new role: a role that does runs (``ImplementerRole``) with ``runs = true``, else a
    sweeping role (``SweepRole``). A role that a package adds with a ``piloop.roles`` entry point is in the config that
    ``piloop init`` stores, with its table over its defaults.
    """

    model_config = ConfigDict(extra='allow', frozen=True, use_attribute_docstrings=True)
    __pydantic_extra__: dict[str, RoleEntry] = Field(init=False)

    implementer: ImplementerRole = ImplementerRole()
    """``[roles.implementer]``."""
    watchdog: WatchdogRole = WatchdogRole()
    """``[roles.watchdog]``."""
    research: ResearchRole = ResearchRole()
    """``[roles.research]``."""
    scout: ScoutRole = ScoutRole()
    """``[roles.scout]``."""

    @model_validator(mode='after')
    def _check(self) -> Self:
        for name in self.__pydantic_extra__:
            if not re.fullmatch(ROLE_NAME, name) or name in RESERVED_NAMES:
                raise ValueError(
                    f'{name!r} is not a role name: use lowercase letters, digits, _ and - ({ROLE_NAME}), and none of'
                    f' {sorted(RESERVED_NAMES)}'
                )
        return self

    @property
    def _defined(self) -> dict[RoleName, RoleEntry]:
        """The roles that the file defines, in its order. ``_check`` makes each key a ``RoleName``."""
        return {RoleName(name): role for name, role in self.__pydantic_extra__.items()}

    def sweeping(self) -> dict[RoleName, SweepRole]:
        """The sweeping roles by name: the built-in ones, then the sweeping roles of the file in its order."""
        mine = {name: role for name, role in self._defined.items() if isinstance(role, SweepRole)}
        return {
            WATCHDOG: self.watchdog,
            RESEARCH: self.research,
            SCOUT: self.scout,
            **mine,
        }

    def doing_runs(self) -> dict[RoleName, ImplementerRole]:
        """The roles that do runs by name, in the order in which they fill their slots.

        The roles that you define come first, in the order of the file, and the implementer comes last.
        """
        mine = {name: role for name, role in self._defined.items() if isinstance(role, ImplementerRole)}
        return {**mine, IMPLEMENTER: self.implementer}

    def all(self) -> dict[RoleName, RoleConfig]:
        """Every role by name: the built-in roles, then the roles that you define in the order of the file."""
        return {
            IMPLEMENTER: self.implementer,
            WATCHDOG: self.watchdog,
            RESEARCH: self.research,
            SCOUT: self.scout,
            **self._defined,
        }


class CampaignConfig(BaseModel):
    """Everything piloop needs to know about a campaign repo."""

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    name: str
    """A short name for the campaign. piloop names its git refs after it."""
    metrics: list[Metric] = Field(min_length=1)
    """Every metric the evaluate command prints, each with its ``role``. Each track has one ``primary``, so a campaign
    with one track has exactly one."""
    tracks: dict[TrackName, TrackConfig] = {}
    """``[tracks.<name>]``: the share and addendum of each track. Needed only with several primaries."""
    evaluate: list[str] = Field(min_length=1)
    """The command (argv) that scores a commit. It runs in a clean worktree of the commit and must print
    ``METRIC <name>=<value>`` lines."""
    runtime: RuntimeName = 'subprocess'
    """Where the agents run: the name of a runtime, which starts, watches, steers and stops them. A package can
    add a runtime with a ``piloop.runtimes`` entry point. A role can override it (``[roles.<name>] runtime``)."""
    harnesses: list[str] = Field(default=['pi'], min_length=1)
    """The enabled harnesses. Runs use them in turn, unless an idea pins one."""
    model: dict[str, str]
    """The model id for each harness, for example
    ``{pi = "anthropic/claude-opus-5-5", claude = "claude-opus-5-5", codex = "gpt-6.1-sol"}``."""
    harness: dict[str, Harness] = {}
    """Harness definitions by name: overrides of the built-in harnesses, or new ones."""
    max_concurrent: dict[str, Annotated[int, Field(ge=1)]] = {}
    """The maximum number of agents of one harness, across all roles of the campaign, that are started or queued to
    start. Example: ``{claude = 2}`` when the provider of its model limits the request rate. When it is the turn of a
    harness at its limit, the slot goes to the next harness of the rotation. An idea or request that pins that harness
    waits. When unset, only the limits of the roles apply."""
    evaluate_timeout: float = 600
    """The number of seconds before the evaluate command is stopped. The evaluation is then a crash."""
    roles: Roles = Roles()
    """The settings of each role (``[roles.implementer]``, ``[roles.watchdog]``, ``[roles.research]``, ``[roles.scout]``),
    and the roles that you define (``[roles.<name>]``)."""
    link: list[str] = []
    """Repo-relative paths, ignored by git, that piloop puts into every worktree read-only, for example data or virtual
    environments. Each job gets them as they are when it is made."""
    context: list[str] = []
    """Repo-relative files or globs (for example ``docs/error-analysis-*.md``) that hold the campaign's analysis:
    error analyses, comparisons with a reference, evaluation reports. The research pack lists each one with its
    outline, newest first, so that every sweep that reads the pack reads them before it proposes work."""
    mcp: dict[ServerName, McpServer] = {}
    """``[mcp.<name>]``: the MCP servers that a role can list in ``[roles.<name>] mcp``."""
    spend: Spend | None = None
    """``[spend]``: how the daemon reads what the campaign spends outside piloop. None by default."""
    memory: Memory = Memory()
    """``[memory]``: documents to search and the lessons overview in briefs."""
    plugins: Annotated[Plugins, PluginTables(Scope.CAMPAIGN)] = {}
    """``[plugins.<capability>]``: the provider of each capability of the campaign, by its name (``provider``), and its
    parameters, the other keys of the table. A capability without a table, or a table without ``provider``,
    has its default provider."""

    @model_validator(mode='after')
    def _check(self) -> Self:
        self._check_tracks()
        self._check_limits()
        self._check_harnesses(self.harnesses, self.model, 'campaign')
        if unknown := [h for h in self.max_concurrent if h not in DEFAULT_HARNESSES and h not in self.harness]:
            raise ValueError(f'max_concurrent: unknown harnesses {unknown}; built-ins are {sorted(DEFAULT_HARNESSES)}')
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

    def _check_limits(self) -> None:
        """Check the stop rule's limits against the rest of the file, by fact."""
        rule = STOP_RULES.selected(self.plugins)
        limits, name = rule.limits(), STOP_RULES.name_of(rule)
        if Fact.HEAD in limits and self.multitrack:
            raise ValueError(f'the stop rule {name!r} limits the head, which needs a campaign of one track')
        if Fact.SPEND in rule.reads() and self.spend is None:
            raise ValueError(f'the stop rule {name!r} reads the spend: add [spend] with the command that prints it')
        spend = limits.get(Fact.SPEND)
        if self.spend is not None and spend is not None and (above := [n for n in self.spend.notices if n >= spend]):
            raise ValueError(f'[spend] notices {above} are not below the spend limit {spend:g}')

    def _check_tracks(self) -> None:
        """Check that each track has one primary.

        With several tracks, each primary also needs a margin, each track needs a table, and the shares must sum to 1.
        """
        primaries = [m for m in self.metrics if m.role is MetricRole.PRIMARY]
        if not primaries:
            raise ValueError('at least one metric must be primary')
        names = [m.track_name for m in primaries]
        if len(primaries) == 1 and (primaries[0].margin is not None or primaries[0].floor is not None):
            raise ValueError(f'{primaries[0].name!r}: margin and floor on a primary apply only with several tracks')
        if len(primaries) > 1:
            if dupes := sorted({n for n in names if names.count(n) > 1}):
                raise ValueError(f'tracks {dupes} have more than one primary: give each primary its own track')
            if unguarded := [m.name for m in primaries if m.margin is None]:
                raise ValueError(f'primaries {unguarded} need a margin: with several tracks each guards the others')
            if untabled := [n for n in names if n not in self.tracks]:
                raise ValueError(f'tracks {untabled} need a [tracks.<name>] table with their share')
        if unknown := sorted(set(self.tracks) - set(names)):
            raise ValueError(f'[tracks] {unknown} name no track a primary declares; tracks are {names}')
        total = sum(self.tracks[n].share if n in self.tracks else 1 for n in names)
        if not math.isclose(total, 1):
            raise ValueError(f'track shares sum to {total:g}, not 1')

    def _check_harnesses(self, harnesses: list[str], models: dict[str, str], where: str) -> None:
        unknown = [h for h in harnesses if h not in DEFAULT_HARNESSES and h not in self.harness]
        if unknown:
            raise ValueError(f'{where}: unknown harnesses {unknown}; built-ins are {sorted(DEFAULT_HARNESSES)}')
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

    def role(self, name: RoleName) -> RoleConfig:
        """The ``[roles.<name>]`` table. The keys that only one role has need ``self.roles.<name>``.

        Raises:
            KeyError: ``piloop.toml`` has no such role.
        """
        return self.roles.all()[name]

    def wake_budget(self, name: RoleName) -> int:
        """The tokens of the lessons overview in a brief of this role: its own ``wake_budget``, or else ``[memory]``'s."""
        own = self.role(name).wake_budget
        return self.memory.wake_budget if own is None else own

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
        """What decides a score: the evaluate command, the names and directions of the metrics, and the linked inputs.

        Roles, margins, floors and the verdict rule are the policy of the verdict, not part of the score. A change to
        them does not change a verdict already given and needs no new measurements.
        """
        include = {'evaluate': True, 'link': True, 'metrics': {'__all__': {'name', 'direction'}}}
        return self.model_dump(mode='json', include=include)

    @property
    def evaluator_key(self) -> str:
        """A short hash of ``evaluator_definition``. It identifies an ``evaluators`` row."""
        return hashlib.sha256(json.dumps(self.evaluator_definition, sort_keys=True).encode()).hexdigest()[:16]

    def harness_for(self, name: str) -> Harness:
        """The harness definition in use: the campaign's override, or else the built-in one."""
        return self.harness.get(name) or DEFAULT_HARNESSES[name]


def config_of(campaign: Campaign) -> CampaignConfig:
    """The config the daemon schedules from: the copy of ``piloop.toml`` that the last ``piloop init`` stored."""
    return CampaignConfig.model_validate(campaign.config)


def harness_of(s: Session, agent_session: AgentSession) -> Harness | None:
    """The definition of the harness that an agent session row runs, from its campaign's config.

    None before the launcher picks a harness, and when ``piloop.toml`` no longer names it (a row keeps only the name).
    """
    if agent_session.harness is None or agent_session.campaign_id is None:
        return None
    try:
        return config_of(s.get_one(Campaign, agent_session.campaign_id)).harness_for(agent_session.harness)
    except KeyError:
        return None


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


def with_registered_roles(raw: dict[str, object]) -> None:
    """Add each role that a package adds (``ROLES``) to a parsed ``piloop.toml``, its table over its defaults.

    A value that is not a table is left for the validation to refuse.
    """
    tables = raw.get('roles', {})
    added = {name: p.model() for name, p in ROLES.providers().items() if not p.source.built_in}
    if not added or not isinstance(tables, dict):
        return
    merged = dict(tables)
    for name, role in added.items():
        table = merged.get(name, {})
        merged[name] = role.model_dump(mode='json') | table if isinstance(table, dict) else table
    raw['roles'] = merged


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
    deprecated.apply(toml, raw)
    deprecated.old_env(toml, os.environ)
    with_registered_roles(raw)

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
            deprecated.until(
                '0.3.0', 'PILOOP_PROJECT_<KEY> still applies after PILOOP_CAMPAIGN_<KEY>: delete this source'
            )
            old = EnvSettingsSource(
                settings_cls, env_prefix=deprecated.OLD_ENV_PREFIX, env_nested_delimiter='__', case_sensitive=False
            )
            return init_settings, env_settings, old, InitSettingsSource(settings_cls, raw)

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
