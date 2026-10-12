"""Project configuration: ``piloop.toml`` at the repo root. ``PILOOP_PROJECT_*`` environment variables override it.

``docs/reference/configuration.mdx`` is generated from these models. Run ``make docs`` after you change them.
"""

import hashlib
import json
import math
import tomllib
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any, Self, cast

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator
from pydantic_settings import BaseSettings, InitSettingsSource, PydanticBaseSettingsSource, SettingsConfigDict
from sqlalchemy.orm import Session

from piloop import deprecated
from piloop.db.models import Project, Worker, WorkerKind
from piloop.harnesses import DEFAULT_HARNESSES, Harness
from piloop.mcp import MCP, RENDERERS, McpServer, ServerName

CONFIG_FILE = 'piloop.toml'
BRIEF_FILE = 'brief.md'


class Direction(StrEnum):
    """Whether a larger or a smaller value of a metric is better."""

    HIGHER = 'higher'
    LOWER = 'lower'


class RuntimeKind(StrEnum):
    """Where worker processes run."""

    HERDR = 'herdr'
    SUBPROCESS = 'subprocess'


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
    track: str | None = None
    """Primary metrics only. The name of the track (objective) that this metric is the primary of. When unset, the
    track has the metric's name, so a project with one primary needs nothing here. With several primaries, each one
    names its own track, and each track needs a ``[tracks.<name>]`` table."""
    margin: float | None = Field(default=None, ge=0)
    """The largest regression from the bar that a candidate may show, in the metric's own units (an absolute value,
    not a fraction). Required on a guardrail. Also required on every primary of a project with several tracks: when a
    candidate of another track is judged, that primary is a guardrail. When the evaluate command writes per-unit
    results, the margin is tested on the confidence bound of the bootstrap. Otherwise it is tested on the aggregate."""
    floor: float | None = None
    """Optional. The worst value a candidate may have: at least this value for ``higher``, at most this value for
    ``lower``. It is a hard limit on the aggregate, independent of the bar, and only an edit of ``piloop.toml`` changes
    it. Allowed on a guardrail, and on a primary of a project with several tracks, where it applies to the candidates
    of the other tracks."""

    @model_validator(mode='after')
    def _check(self) -> Self:
        if self.role is MetricRole.GUARDRAIL and self.margin is None:
            raise ValueError(f'guardrail {self.name!r} needs a margin')
        if self.role is MetricRole.WATCH and (self.margin is not None or self.floor is not None):
            raise ValueError(f'{self.name!r}: margin and floor apply to guardrails and primaries only')
        if self.role is not MetricRole.PRIMARY and self.track is not None:
            raise ValueError(f'{self.name!r}: track applies to primaries only')
        return self

    @property
    def track_name(self) -> str:
        """The track that a primary decides: its ``track``, or else its own name."""
        return self.track or self.name

    def better(self, a: float, b: float) -> bool:
        """Whether ``a`` is better than ``b`` for this metric."""
        return a > b if self.direction is Direction.HIGHER else a < b

    def gain(self, a: float, b: float) -> float:
        """How much better ``a`` is than ``b``, in the metric's units. Positive when ``a`` is better."""
        return a - b if self.direction is Direction.HIGHER else b - a


class TrackConfig(BaseModel):
    """``[tracks.<name>]``: one track of the project. A track is an objective with its own primary metric and brief.

    A track has the name of its primary's ``track``, or else the name of the primary. All tracks build on one trunk. A
    run starts from the head, which is the latest keep on the trunk. A keep on any track must also hold the primary of
    every other track within that primary's ``margin`` and ``floor``. A project with one primary has one track and
    needs no table. A project with several primaries needs one table for each track, and the shares must sum to 1.

    Experimental: projects with more than one track.
    """

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    share: float = Field(default=1, ge=0, le=1)
    """The fraction of ``[roles.worker] max_concurrent`` that the ideas of this track get. A free worker slot goes to
    the track that is furthest below its share and has an idea that can run, and then to the most urgent idea of that
    track. A track with no idea that can run gives its slot to another track. A share of 0 stops the track: its ideas
    wait."""
    addendum: str | None = None
    """A repo-relative file that is appended to the brief of this track's runs, after ``[roles.worker] addendum``. Use
    it to say how work on this objective is done, for example "profile first" or "keep the output identical"."""


@dataclass(frozen=True)
class Track:
    """One track, as the daemon judges and schedules it."""

    name: str
    primary: Metric
    share: float
    addendum: str | None
    guardrails: tuple[Metric, ...]
    """What a candidate of this track must hold: the project's guardrails, then the primary of every other track, each
    with its margin and floor."""


class RoleConfig(BaseModel):
    """The keys that every ``[roles.<kind>]`` table accepts. Each role's table below adds keys and sets defaults."""

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    harnesses: list[str] | None = Field(default=None, min_length=1)
    """The harness rotation of this role. When unset, the role uses the project's ``harnesses``."""
    model: dict[str, str] | None = None
    """The model for each harness in this role. It is merged over the project's ``model``."""
    timeout: float = 600
    """The number of seconds one agent may run before it is stopped."""
    max_concurrent: int = Field(default=1, ge=1)
    """The maximum number of agents of this role that run at the same time. More requests stay ``requested`` until a
    slot is free."""
    addendum: str | None = None
    """A repo-relative file that is appended to ``brief.md`` in the briefs of this role only. Use it for instructions
    that only this role needs, for example how the research agent searches."""
    mcp: list[str] = []
    """The MCP servers that the agents of this role get, by the name of their ``[mcp.<name>]`` table. Every harness in
    the role's rotation must be able to take them (``[harness.<name>] mcp``). The built-in ``claude``, ``codex`` and
    ``pi`` harnesses can."""


class WorkerRole(RoleConfig):
    """``[roles.worker]``: a worker does one idea in a worktree. The ``queue`` trigger starts it."""

    timeout: float = 1800
    """The number of seconds one worker may run before it is stopped."""
    max_concurrent: int = Field(default=4, ge=1)
    """The maximum number of workers that run at the same time. Each worker is one harness process on this machine. A
    run that is being scored after its worker ended does not count."""
    explore_every: int = Field(default=4, ge=0)
    """At least one run in this many starts from an alternative starting point. When none of the last
    ``explore_every - 1`` runs did, the next slot goes to the most urgent ready idea that is pinned to a base other
    than the head (a reference, an older keep or a note), whatever its priority. Nothing changes while no such idea is
    ready. 0 turns this off."""


class WatchdogRole(RoleConfig):
    """``[roles.watchdog]``: the watchdog investigates the anomalies that the daemon detects and reviews progress."""

    interval: float = 1800
    """The number of seconds between two progress reviews, counted from the last review. A sweep that investigates an
    anomaly is not a review. 0 turns the reviews off."""


SCOUT_SEEDS = 3
"""The number of seeds a scout sweep may file when ``[roles.research.scout] seeds`` is not set."""


class Scout(BaseModel):
    """``[roles.research.scout]``: the scout mode of the research role. It is off when the table is not there.

    Every ``interval`` seconds, a research sweep runs in scout mode (``prompts/research_scout.md``). It states the open
    problems of the campaign from the evidence, and searches X, GitHub and arXiv for outside techniques that address
    them. It files at most ``seeds`` of them as seeds, signed ``scout#<worker id>``, and puts the rest in the technique
    library (the ``techniques`` table). It never files a defined idea. The search tool limits what one sweep spends on
    xAI (``scripts/xai_search_mcp.py --max-usd``). Scout sweeps stop with all other launches under ``[spend]``.

    Experimental: scout mode.
    """

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    interval: float = Field(gt=0)
    """The number of seconds between two scout sweeps, counted from the start of the last one. The first sweep counts
    from the registration of the project. Required: there is no default."""
    seeds: int = Field(default=SCOUT_SEEDS, ge=1)
    """The maximum number of seeds one scout sweep may file. The other techniques it finds go to the library."""


class ResearchRole(RoleConfig):
    """``[roles.research]``: research develops seeds, keeps ideas ready for the workers and scouts for techniques."""

    timeout: float = 900
    """The number of seconds one sweep may run before it is stopped."""
    max_concurrent: int = Field(default=1, ge=1)
    """The maximum number of sweeps that run at the same time, each on the next harness of the rotation. The default
    is 1: sweeps that run at the same time read the same evidence and often duplicate each other, and two of them can
    race to develop the same seed."""
    max_ideas: int = Field(default=1, ge=0)
    """The maximum number of new ideas one sweep may insert. The seeds it develops do not count. ``low_water``
    requests enough sweeps to fill the backlog at this many ideas each. 0 turns ``low_water`` off: research then only
    develops seeds."""
    context: list[str] = []
    """Repo-relative files or globs (for example ``docs/error-analysis-*.md``) that hold the project's analysis:
    error analyses, comparisons with a reference, evaluation reports. The research pack lists each one with its
    outline, newest first, so that research reads them before it proposes ideas."""
    buffer: int = Field(default=2, ge=0)
    """The number of ready ideas that ``low_water`` keeps in addition to one for each worker slot
    (``[roles.worker] max_concurrent``). With this backlog, a free slot finds an idea at once and does not wait for a
    sweep. A larger value makes workers wait less for research, but the ideas they run are older."""
    scout: Scout | None = None
    """``[roles.research.scout]``: sweeps that search for outside techniques on a schedule. Off by default."""


class Stop(BaseModel):
    """``[stop]``: conditions that end the campaign. One is enough. Without the table, the campaign has no end.

    The daemon checks the conditions on every tick, before it requests anything. When one holds, the project is
    ``stopped`` (``projects.status``, ``stopped_reason``). Running workers are told to commit and stop, and are ended
    ten minutes later. What they committed is scored. Pending requests and queued runs are dropped
    (``campaign stopped``). One last watchdog sweep is requested, and nothing new starts.

    ``piloop init`` resumes a stopped project when this table changed.
    """

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    target: float | None = None
    """Stop when the primary metric at the head (the latest keep on the trunk) reaches this value, in its direction.
    Only for a project with one track."""
    max_runs: int | None = Field(default=None, ge=1)
    """Stop after this many worker runs. A run counts while it runs, and when it ends with a candidate that is
    measured or waits for its measurement. A run that stopped at its own first check, with no commit or only
    ``Piloop-Eval: skip`` commits, does not count. A run that a daemon stop, a daemon crash or a campaign stop ended
    does not count either. ``[spend]`` and ``max_tokens`` limit what those runs cost."""
    max_hours: float | None = Field(default=None, gt=0)
    """Stop after this many hours of wall-clock time since the first run of the project started."""
    max_tokens: int | None = Field(default=None, ge=1)
    """Stop when all the agents of the project together spent this many tokens (the ``total`` of ``workers.usage``),
    summed over the agents that ended."""
    plateau: int | None = Field(default=None, ge=1)
    """Stop after this many finished runs in a row without a new ``keep`` candidate. Only the runs that ``max_runs``
    counts are counted."""


class Spend(BaseModel):
    """``[spend]``: a hard budget in USD on what the campaign costs outside piloop, for example GPUs or APIs.

    Every ``interval`` seconds, the daemon runs ``command`` to read the spend from the project (``projects.spend_usd``,
    ``spend_error``, ``spend_at``). When the spend passes a ``notices`` threshold, the daemon posts one note to the
    inbox at priority 1. At ``limit``, the daemon launches no agent and starts no evaluation (no request, no
    re-measure), and posts one escalation at priority 0. Work that already runs finishes and is scored. A command that
    fails stops launches in the same way, with one escalation for each series of failures: an unknown spend is never
    read as $0. To resume, raise ``limit`` and run ``piloop init``.
    """

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    command: list[str] = Field(min_length=1)
    """The command (argv) that prints the project's spend so far in USD, as one number. It runs at the repo root with
    the daemon's environment, which is where its secrets come from. It must exit with code 0 within one minute.
    Example: ``["uv", "run", "scripts/spend.py"]``."""
    limit: float = Field(gt=0)
    """The spend in USD at which launches and evaluations stop."""
    notices: list[Annotated[float, Field(gt=0)]] = []
    """Amounts in USD below ``limit``. When the spend passes one, the daemon posts one note to the inbox."""
    interval: float = Field(default=300, gt=0)
    """The number of seconds between two runs of ``command``."""

    @model_validator(mode='after')
    def _check(self) -> Self:
        if above := [n for n in self.notices if n >= self.limit]:
            raise ValueError(f'notices {above} are not below the limit {self.limit:g}')
        return self


class Roles(BaseModel):
    """One table for each role. A role without a table uses its defaults."""

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    worker: WorkerRole = WorkerRole()
    """``[roles.worker]``."""
    watchdog: WatchdogRole = WatchdogRole()
    """``[roles.watchdog]``."""
    research: ResearchRole = ResearchRole()
    """``[roles.research]``."""


class ProjectConfig(BaseModel):
    """Everything piloop needs to know about a project repo."""

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    name: str
    """A short name for the project. piloop names its git refs and Herdr workspaces after it."""
    metrics: list[Metric] = Field(min_length=1)
    """Every metric the evaluate command prints, each with its ``role``. Each track has one ``primary``, so a project
    with one track has exactly one."""
    tracks: dict[str, TrackConfig] = {}
    """``[tracks.<name>]``: the share and addendum of each track. Needed only with several primaries."""
    confidence: float = Field(default=0.95, gt=0.5, lt=1)
    """The confidence level of the paired bootstrap behind the verdict. For a ``keep``, the lower bound of the
    primary's gain must be above zero at this level. A guardrail's margin is tested on the bound at this level too."""
    evaluate: list[str] = Field(min_length=1)
    """The command (argv) that scores a commit. It runs in a clean worktree of the commit and must print
    ``METRIC <name>=<value>`` lines."""
    runtime: RuntimeKind = RuntimeKind.HERDR
    """Where workers run. With ``herdr``, every worker gets a terminal pane that you and the orchestrator can read and
    type into. With ``subprocess``, every worker is a background process."""
    harnesses: list[str] = Field(default=['pi'], min_length=1)
    """The enabled harnesses. Runs use them in turn, unless an idea pins one."""
    model: dict[str, str]
    """The model id for each harness, for example
    ``{pi = "anthropic/claude-opus-5-5", claude = "claude-opus-5-5", codex = "gpt-6.1-sol"}``."""
    harness: dict[str, Harness] = {}
    """Harness definitions by name: overrides of the built-in harnesses, or new ones."""
    max_concurrent: dict[str, Annotated[int, Field(ge=1)]] = {}
    """The maximum number of agents of one harness, across all roles of the project, that are started or queued to
    start. Example: ``{claude = 2}`` when the provider of its model limits the request rate. When it is the turn of a
    harness at its limit, the slot goes to the next harness of the rotation. An idea or request that pins that harness
    waits. When unset, only the limits of the roles apply."""
    evaluate_timeout: float = 600
    """The number of seconds before the evaluate command is stopped. The evaluation is then a crash."""
    roles: Roles = Roles()
    """The settings of each role (``[roles.worker]``, ``[roles.watchdog]``, ``[roles.research]``). See
    https://piloop.autoresearch.inc/concepts/roles."""
    link: list[str] = []
    """Repo-relative paths, ignored by git, that piloop symlinks into every worktree, for example data or virtual
    environments."""
    mcp: dict[ServerName, McpServer] = {}
    """``[mcp.<name>]``: the MCP servers that a role can list in ``[roles.<kind>] mcp``. See ``McpServer``."""
    stop: Stop = Stop()
    """``[stop]``: conditions that end the campaign. None by default."""
    spend: Spend | None = None
    """``[spend]``: a hard budget on what the campaign spends outside piloop. None by default."""

    @model_validator(mode='after')
    def _check(self) -> Self:
        self._check_tracks()
        self._check_harnesses(self.harnesses, self.model, 'project')
        if unknown := [h for h in self.max_concurrent if h not in DEFAULT_HARNESSES and h not in self.harness]:
            raise ValueError(f'max_concurrent: unknown harnesses {unknown}; built-ins are {sorted(DEFAULT_HARNESSES)}')
        for kind in WorkerKind:
            self._check_harnesses(self.role_harnesses(kind), self.role_models(kind), f'roles.{kind}')
        for kind in WorkerKind:
            self._check_mcp(kind)
        addenda = [a for kind in WorkerKind if (a := self.role(kind).addendum) is not None]
        addenda += [t.addendum for t in self.tracks.values() if t.addendum is not None]
        for rel in [*self.link, *addenda, *self.roles.research.context]:
            if Path(rel).is_absolute() or '..' in Path(rel).parts:
                raise ValueError(f'{rel!r} must be a relative path inside the repo')
        return self

    def _check_tracks(self) -> None:
        """Check that each track has one primary.

        With several tracks, each primary also needs a margin, each track needs a table, the shares must sum to 1,
        and ``[stop] target`` is not allowed.
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
            if self.stop.target is not None:
                raise ValueError('[stop] target needs a project of one track: per-track targets are not supported')
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

    def _check_mcp(self, kind: WorkerKind) -> None:
        """Every server the role lists exists, and every harness the role may run can take them in its argv."""
        unknown = [n for n in self.role(kind).mcp if n not in self.mcp]
        if unknown:
            raise ValueError(f'roles.{kind}.mcp: no [mcp.<name>] table for {unknown}')
        if not self.role(kind).mcp:
            return
        for name in self.role_harnesses(kind):
            harness = self.harness_for(name)
            argv = harness.interactive if self.runtime is RuntimeKind.HERDR else harness.headless
            if harness.mcp is None or MCP not in argv:
                raise ValueError(
                    f'roles.{kind}.mcp: harness {name!r} takes no MCP servers in its {self.runtime} argv; give it'
                    f' mcp = "<renderer>" and a {MCP} element ({sorted(RENDERERS)}), or take it out of the rotation'
                )
            if why := RENDERERS[harness.mcp].refuses(argv):
                raise ValueError(f'roles.{kind}.mcp: harness {name!r} under the {self.runtime} runtime: {why}')

    def role_servers(self, kind: WorkerKind) -> dict[str, McpServer]:
        """The MCP servers that the agents of a role get, by name."""
        return {name: self.mcp[name] for name in self.role(kind).mcp}

    def role(self, kind: WorkerKind) -> RoleConfig:
        """The ``[roles.<kind>]`` table. The keys that only one role has need ``self.roles.<kind>``."""
        return {
            WorkerKind.WORKER: self.roles.worker,
            WorkerKind.WATCHDOG: self.roles.watchdog,
            WorkerKind.RESEARCH: self.roles.research,
        }[kind]

    def role_harnesses(self, kind: WorkerKind) -> list[str]:
        """The rotation a role uses: its own list, or else the project's."""
        return self.role(kind).harnesses or self.harnesses

    def role_models(self, kind: WorkerKind) -> dict[str, str]:
        """The model for each harness of a role: the project's map, with the role's overrides on top."""
        return {**self.model, **(self.role(kind).model or {})}

    @property
    def guardrails(self) -> list[Metric]:
        """The project's guardrails. A candidate of any track must stay within their margins and floors."""
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
        """Whether the project has more than one track. Only then do the renderers show track names."""
        return len(self.all_tracks) > 1

    def track(self, name: str) -> Track:
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

        Roles, margins, floors and ``confidence`` are the policy of the verdict, not part of the score. A change to
        them does not change a verdict already given and needs no new measurements.
        """
        include = {'evaluate': True, 'link': True, 'metrics': {'__all__': {'name', 'direction'}}}
        return self.model_dump(mode='json', include=include)

    @property
    def evaluator_key(self) -> str:
        """A short hash of ``evaluator_definition``. It identifies an ``evaluators`` row."""
        return hashlib.sha256(json.dumps(self.evaluator_definition, sort_keys=True).encode()).hexdigest()[:16]

    def harness_for(self, name: str) -> Harness:
        """The harness definition in use: the project's override, or else the built-in one."""
        return self.harness.get(name) or DEFAULT_HARNESSES[name]


def config_of(project: Project) -> ProjectConfig:
    """The config the daemon schedules from: the copy of ``piloop.toml`` that the last ``piloop init`` stored."""
    return ProjectConfig.model_validate(project.config)


def harness_of(s: Session, worker: Worker) -> Harness | None:
    """The definition of the harness that a worker row runs, from its project's config.

    None before the launcher picks a harness, and when ``piloop.toml`` no longer names it (a row keeps only the name).
    """
    if worker.harness is None or worker.project_id is None:
        return None
    try:
        return config_of(s.get_one(Project, worker.project_id)).harness_for(worker.harness)
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


def load_project_config(project: Path, **overrides: object) -> ProjectConfig:
    """Load ``<project>/piloop.toml``.

    ``overrides`` take precedence over ``PILOOP_PROJECT_*`` environment variables, which take precedence over the
    file. Deprecated keys still load through ``piloop.deprecated``, with a ``PiloopDeprecationWarning``.

    Raises:
        FileNotFoundError: The project has no ``piloop.toml``.
        ConfigError: The file is not valid TOML or not a valid config.
    """
    toml = project / CONFIG_FILE
    if not toml.is_file():
        raise FileNotFoundError(toml)
    try:
        raw = tomllib.loads(toml.read_text())
    except tomllib.TOMLDecodeError as e:
        raise ConfigError(f'{toml} is not valid TOML: {e}') from e
    deprecated.bridge(toml, raw)

    class _Loader(BaseSettings, ProjectConfig):
        model_config = SettingsConfigDict(env_prefix='PILOOP_PROJECT_', env_nested_delimiter='__', extra='forbid')

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
        return ProjectConfig.model_validate(_Loader(**kwargs).model_dump())
    except ValidationError as e:
        raise ConfigError(explain(toml, e)) from e
