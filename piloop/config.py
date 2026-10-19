"""Project configuration: ``piloop.toml`` at the repo root.

``piloop init`` loads the file and stores it in ``projects.config``, and the daemon schedules from that copy.
``PILOOP_PROJECT_*`` environment variables override the file when it is loaded, so they apply to ``piloop init`` and
``piloop doctor``, not to the daemon.

``docs/reference/configuration.mdx`` is generated from these models. Run ``make docs`` after you change them.
"""

import hashlib
import json
import math
import re
import tomllib
from dataclasses import dataclass
from enum import StrEnum
from functools import cache
from importlib.metadata import entry_points
from pathlib import Path, PurePosixPath
from typing import Annotated, Any, ClassVar, Literal, Self, cast

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, InitSettingsSource, PydanticBaseSettingsSource, SettingsConfigDict
from sqlalchemy.orm import Session

from piloop import deprecated
from piloop.db.models import Project, Worker, WorkerKind
from piloop.harnesses import DEFAULT_HARNESSES, Harness
from piloop.mcp import McpServer, ServerName
from piloop.runtime.factory import runtimes
from piloop.triggers import (
    AnomalyTrigger,
    CronTrigger,
    LowWaterTrigger,
    SeedTrigger,
    StoppedTrigger,
    TriggerEntry,
    sql_error,
)

CONFIG_FILE = 'piloop.toml'
BRIEF_FILE = 'brief.md'


class Direction(StrEnum):
    """Whether a larger or a smaller value of a metric is better."""

    HIGHER = 'higher'
    LOWER = 'lower'


def _registered_runtime(name: str) -> str:
    if name not in runtimes():
        raise ValueError(f'unknown runtime {name!r}; registered runtimes: {", ".join(runtimes())}')
    return name


RuntimeName = Annotated[str, AfterValidator(_registered_runtime)]
"""The name of a registered runtime (``piloop.runtime.factory.runtimes``): ``subprocess``, ``herdr``, or one that a
package adds."""


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
    """The fraction of the ``max_concurrent`` of each role that does runs that the ideas of this track get. A free slot
    of a role goes to the track that is furthest below its share and has an idea that can run, and then to the most
    urgent idea of that track. A track with no idea that can run gives its slot to another track. A share of 0 stops the track: its ideas
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


ROLE_NAME = r'[a-z][a-z0-9_-]{0,19}'
"""The form of a role name: the agents' signature, ``workers.kind``, and the start of their Herdr agent name."""
RESERVED_NAMES = frozenset({'run', 'orchestrator', 'human', 'daemon'})
"""Names that already sign writes: a run, and the actors that are not roles."""
ROLE_GROUP = 'piloop.roles'
"""The entry point group of the roles that packages add. Each entry point names a ``SweepRole`` or a ``WorkerRole``
with its defaults."""


class Workdir(StrEnum):
    """Where the agent of a sweeping role works. An agent that does a run works in a new worktree at its run's base."""

    HEAD = 'head'
    """A detached worktree at the head. It is removed when the agent ends, and nothing committed in it is scored."""
    PROJECT = 'project'
    """The project checkout itself, shared with you. piloop does not reset or remove it."""


class Files(StrEnum):
    """What the agents of a role may file with ``piloop idea``. The CLI refuses the rest."""

    NOTHING = 'nothing'
    """No ideas and no seeds. A hunch goes in a finding."""
    SEEDS = 'seeds'
    """Seeds (``piloop idea seed``), for research to develop."""
    IDEAS = 'ideas'
    """Defined ideas (``piloop idea add``, ``piloop idea develop``) and seeds."""


class RoleConfig(BaseModel):
    """The keys that every ``[roles.<name>]`` table accepts. Each role's table below adds keys and sets defaults."""

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    PROMPT: ClassVar[str] = ''
    """The built-in prompt of a built-in role, a file under ``piloop/agents/prompts/``. Empty for other roles."""
    IDEAS: ClassVar[str] = ''
    """The ideas line of a built-in role's brief when the line for its ``files`` does not fit it. ``{piloop}`` is the CLI
    and ``{project}`` is the project's name. Empty: the line for ``files`` (``piloop.agents.roles.IDEAS``)."""

    prompt: str | None = None
    """The file that starts the brief of this role: a path relative to the repo root, or an absolute path. When unset,
    a built-in role uses its built-in prompt. A new role needs it."""
    runtime: RuntimeName | None = None
    """Where the agents of this role run. When unset, the role uses the project's ``runtime``."""
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
    the role's rotation must be able to take them in the invocation of the role's runtime (``[harness.<name>] mcp``)."""
    writes: list[str] = []
    """What the agents of this role may write, as their brief states it. Only ``files`` is enforced."""
    files: Files = Files.SEEDS
    """What the agents of this role may file with ``piloop idea``: ``nothing``, ``seeds`` or ``ideas``. The CLI
    refuses the rest."""
    wake_budget: int | None = Field(default=None, ge=0)
    """The size in tokens of the lessons overview that opens this role's briefs. When unset, ``[memory]
    wake_budget``. 0 turns it off for this role."""


class WorkerRole(RoleConfig):
    """``[roles.worker]``, and every ``[roles.<name>]`` table with ``runs = true``: a role that does runs.

    The run queue starts its agents. Each agent does one idea in a new worktree at its run's base, and its commits are
    scored and judged when it ends. A new role of this kind takes the worker's defaults, its prompt included.
    """

    PROMPT: ClassVar[str] = 'worker.md'

    runs: Literal[True] = True
    """The agents of this role do runs. A table without ``runs = true`` is a sweeping role."""
    takes: str | None = None
    """The ideas that this role takes: an SQL condition on the ``ideas`` table, without parameters, for example
    ``priority >= 3`` or ``track = 'speed'``. When unset, the role takes every runnable idea. A free slot of a role
    that you define is filled before a slot of the worker, so the worker takes what the other roles leave."""

    timeout: float = 1800
    """The number of seconds one worker may run before it is stopped."""
    max_concurrent: int = Field(default=4, ge=1)
    """The maximum number of workers that run at the same time. Each worker is one harness process on this machine. A
    run that is being scored after its worker ended does not count."""
    writes: list[str] = ['commits', 'comments(finding) on your idea', 'comments(note) on your run']
    """What a worker may write, as its brief states it."""
    files: Files = Files.NOTHING
    """A worker files no ideas and no seeds. Its hunches go in its finding, which research reads."""
    recovery_requeues: int = Field(default=2, ge=0)
    """How many times one idea goes back to the queue from the work its worker left uncommitted, when the run did not
    succeed (``recovered``): a new worker then starts from that work to finish it. After that, the idea settles as
    after any failed run, so an idea whose workers keep dying is not run forever. 0 never requeues: the recovered
    candidate waits for you or the orchestrator."""
    explore_every: int = Field(default=4, ge=0)
    """At least one run of this role in this many starts from an alternative starting point. When none of the role's
    last ``explore_every - 1`` runs did, the next slot goes to the most urgent ready idea that is pinned to a base
    other than the head (a reference, an older keep or a note), whatever its priority. Nothing changes while no such
    idea is ready. 0 turns this off."""

    @field_validator('takes')
    @classmethod
    def _reads_ideas(cls, takes: str | None) -> str | None:
        if takes is not None and (error := sql_error(ideas_taken(takes))) is not None:
            raise ValueError(f'the condition does not run on the ideas table: {error}')
        return takes


def ideas_taken(takes: str) -> str:
    """Return the query that selects the ids of the ideas that ``[roles.<name>] takes`` selects."""
    return f'SELECT id FROM ideas WHERE {takes}'


class SweepRole(RoleConfig):
    """``[roles.<name>]`` of a sweeping role: the watchdog, research, the scout, or a new role that you define.

    A sweep is an agent without a run. Its triggers request it, and nothing is scored when it ends. A table with a new
    name defines a new role, and needs ``prompt`` and ``triggers``.
    """

    INCIDENT_PROMPT: ClassVar[str] = ''
    """The built-in ``incident_prompt`` of a built-in role, a file under ``piloop/agents/prompts/``. Empty: none."""
    ROUTINE_PROMPT: ClassVar[str] = ''
    """The built-in ``routine_prompt`` of a built-in role, a file under ``piloop/agents/prompts/``. Empty: none."""

    runs: Literal[False] = False
    """The agents of this role do no runs. Set ``runs = true`` in a new table to define a role that does runs."""
    triggers: list[TriggerEntry]
    """When the daemon requests an agent of this role: a list of ``{ kind = "<trigger>", ... }`` entries, checked in
    order on each tick. A package can add triggers. An empty list means that agents start only on request
    (``piloop launch``)."""
    workdir: Workdir = Workdir.HEAD
    """Where the agent works: ``head``, a detached worktree at the head, or ``project``, the project checkout."""
    interval: float = Field(default=0, ge=0)
    """The number of seconds between two routine sweeps of the ``cron`` trigger, counted from the start of the last
    one. A sweep that investigates an anomaly is not a routine sweep. 0 turns the timer off."""
    max_ideas: int = Field(default=1, ge=0)
    """The maximum number of new ideas one sweep may insert, as its brief states it. The seeds it develops do not
    count. ``low_water`` requests enough sweeps to fill the backlog at this many ideas each. 0 pauses the role's new
    ideas: ``low_water`` is off, and an idea that a sweep of the role adds (``piloop idea add``) is ``proposed``, so it
    waits for a review, also from a sweep that started before the change."""
    incident_prompt: str | None = None
    """A file that follows ``prompt`` in the brief of an incident sweep, a sweep that got anomalies (the ``anomaly``
    trigger): a path relative to the repo root, or an absolute path. When unset, a built-in role uses its built-in
    file (the watchdog's ``watchdog_incident.md``), and another role adds nothing."""
    routine_prompt: str | None = None
    """A file that follows ``prompt`` in the brief of every other sweep. When unset, a built-in role uses its built-in
    file (the watchdog's ``watchdog_progress.md``), and another role adds nothing."""

    @model_validator(mode='after')
    def _check(self) -> Self:
        if self.prompt is None and not self.PROMPT:
            raise ValueError('a new role needs a prompt: the file that starts its brief')
        return self


class WatchdogRole(SweepRole):
    """``[roles.watchdog]``: the watchdog investigates the anomalies that the daemon detects and reviews progress."""

    PROMPT: ClassVar[str] = 'watchdog.md'
    INCIDENT_PROMPT: ClassVar[str] = 'watchdog_incident.md'
    ROUTINE_PROMPT: ClassVar[str] = 'watchdog_progress.md'

    triggers: list[TriggerEntry] = [AnomalyTrigger(), CronTrigger(), StoppedTrigger()]
    """An anomaly requests an incident sweep, the timer requests a progress review, and the campaign's stop requests
    the closing review."""
    interval: float = Field(default=1800, ge=0)
    """The number of seconds between two progress reviews, counted from the last review. A sweep that investigates an
    anomaly is not a review. 0 turns the reviews off."""
    writes: list[str] = [
        'comments(escalation, finding, note)',
        'lessons on the project once it stopped',
        'seeds via idea seed',
    ]
    """What a watchdog may write, as its brief states it."""
    wake_budget: int | None = Field(default=0, ge=0)
    """The size in tokens of the lessons overview that opens a watchdog's brief. Off by default: the closing review
    reads the lessons itself before it writes its own."""


class ResearchRole(SweepRole):
    """``[roles.research]``: research develops seeds and keeps ideas ready for the workers."""

    PROMPT: ClassVar[str] = 'research.md'

    triggers: list[TriggerEntry] = [LowWaterTrigger(), SeedTrigger()]
    """A short backlog, and a new seed."""
    writes: list[str] = [
        'seeds via idea develop, or their status to rejected with a note',
        'ideas via idea add',
        'comments(finding, escalation) on the project',
        'comments(note) on an idea',
    ]
    """What a research sweep may write, as its brief states it."""
    files: Files = Files.IDEAS
    """Research develops seeds into ideas and adds ideas of its own."""
    timeout: float = 900
    """The number of seconds one sweep may run before it is stopped."""
    max_concurrent: int = Field(default=1, ge=1)
    """The maximum number of sweeps that run at the same time, each on the next harness of the rotation. The default
    is 1: sweeps that run at the same time read the same evidence and often duplicate each other, and two of them can
    race to develop the same seed."""


class ScoutRole(SweepRole):
    """``[roles.scout]``: the scout files outside techniques as seeds for the measured losses. It is off by default.

    Every ``interval`` seconds, a scout sweep states the open problems of the campaign from the evidence, and searches
    X, GitHub and arXiv for outside techniques that address them. It files at most ``seeds`` of them as seeds, signed
    ``scout#<worker id>``, and puts the rest in the technique library (the ``techniques`` table). It never files a
    defined idea. The search tool limits what one sweep spends on xAI (``scripts/xai_search_mcp.py --max-usd``).

    Experimental: the scout.
    """

    PROMPT: ClassVar[str] = 'scout.md'
    IDEAS: ClassVar[str] = (
        '- File a seed: `{piloop} idea seed {project} "<technique: its mechanism, one line>" --from "<link>; targets'
        ' <the measured loss it might reduce, with its size>; <its substance: a number, code, an ablation>"`: research'
        ' develops it into an idea or rejects it with the reason.'
        '\n- File a technique into the library: `{piloop} sql "INSERT INTO techniques (project_id, title, mechanism,'
        " url, created_by) VALUES (<project id>, '<technique>', '<mechanism and its substance, one line>', '<link>',"
        " '<your signature>')\"`."
    )

    triggers: list[TriggerEntry] = [CronTrigger()]
    """The timer: a scout sweep every ``interval`` seconds."""
    interval: float = Field(default=0, ge=0)
    """The number of seconds between two scout sweeps, counted from the start of the last one. The first sweep counts
    from the registration of the project. 0 (the default) turns the scout off."""
    seeds: int = Field(default=3, ge=1)
    """The maximum number of seeds one scout sweep may file. The other techniques it finds go to the library."""
    writes: list[str] = [
        'seeds via idea seed (at most the count under Context)',
        'techniques (the library), by INSERT',
        'one comments(finding) on the project',
    ]
    """What a scout sweep may write, as its brief states it."""
    timeout: float = 900
    """The number of seconds one sweep may run before it is stopped."""


class Stop(BaseModel):
    """``[stop]``: conditions that end the campaign. One is enough. Without the table, the campaign has no end.

    The daemon checks the conditions on every tick, before it requests anything. When one holds, the project is
    ``stopped`` (``projects.status``, ``stopped_reason``). Running workers are told to commit and stop, and are ended
    10 minutes later. What they committed is scored. A worker in the middle of a turn reads the message only when the
    turn ends: one that is ended before it left its finding closes its run as ``campaign stopped``, which does not
    count toward ``max_runs`` or ``plateau``. Pending requests and queued runs are dropped
    (``campaign stopped``). The ``stopped`` triggers fire once, which by default requests the watchdog's closing
    review, and the triggers request nothing more. An agent that you request with ``piloop launch`` after the stop
    still starts.

    ``piloop init`` resumes a stopped project when this table changed.

    ``max_runs`` and ``plateau`` count the same runs: those that reached the evaluator, with a candidate that is
    measured or waits for its measurement. A run that ended at a cheap check with no commit, with only
    ``Piloop-Eval: skip`` commits, or before it committed because of a harness or host fault, says nothing about the
    metric, so it counts toward neither. A run that a daemon stop, a daemon crash or a campaign stop ended does not
    count either. ``[spend]``, ``max_tokens`` and ``max_hours`` stay hard limits on what all runs cost.
    """

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    target: float | None = None
    """Stop when the primary metric at the head (the latest keep on the trunk) reaches this value, in its direction.
    Only for a project with one track."""
    max_runs: int | None = Field(default=None, ge=1)
    """Stop after this many runs that count (see above), once none is queued, running or being scored. A run also
    counts while it is queued, running or being scored, so piloop never starts more runs than this. A finished run that
    does not count frees its place, and piloop starts another run in it."""
    max_hours: float | None = Field(default=None, gt=0)
    """Stop after this many hours of wall-clock time since the first run of the project started."""
    max_tokens: int | None = Field(default=None, ge=1)
    """Stop when all the agents of the project together spent this many tokens (the ``total`` of ``workers.usage``),
    summed over the agents that ended."""
    plateau: int | None = Field(default=None, ge=1)
    """Stop after this many finished runs that count (see above) without a new ``keep`` candidate. The count starts
    again at each keep, and when the project is set ``active`` after a stop (``piloop init``, or an UPDATE of
    ``projects.status``), so a reactivated campaign gets this many runs again. A new evaluator alone does not reset
    it."""


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


class Memory(BaseModel):
    """``[memory]``: what past campaigns leave for the next ones. See the research memory page."""

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    documents: list[str] = []
    """Repo-relative globs of text documents, for example ``["docs/evidence/**/*.md"]``: campaign reports, orchestrator
    logs, design notes. ``piloop init`` and the stop of the campaign copy them into the ``documents`` table, where
    every agent of every project can search them. Never name a document that is derived from validation or test
    data."""
    wake_budget: int = Field(default=400, ge=0)
    """The size in tokens (about 4 characters each) of the overview that opens the briefs of workers and research
    sweeps: the current lesson of each mechanism family on the same repo, from the newest campaign that tried it, one
    line each. Lessons in the project's own metrics come first (``lessons.metric``), then
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


def _role(table: object) -> object:
    """Validate a ``[roles.<name>]`` table with a new name: a role that does runs with ``runs = true``, else a sweep."""
    does_runs = isinstance(table, WorkerRole) or (isinstance(table, dict) and bool(table.get('runs')))
    return (WorkerRole if does_runs else SweepRole).model_validate(table)


RoleEntry = Annotated[WorkerRole | SweepRole, BeforeValidator(_role)]
"""The type of a ``[roles.<name>]`` table with a new name."""


class Roles(BaseModel):
    """One table for each role. A built-in role without a table uses its defaults.

    A table with a new name defines a new role: a role that does runs (``WorkerRole``) with ``runs = true``, else a
    sweeping role (``SweepRole``). A role that a package adds with a ``piloop.roles`` entry point is in the config that
    ``piloop init`` stores, with its table over its defaults.
    """

    model_config = ConfigDict(extra='allow', frozen=True, use_attribute_docstrings=True)
    __pydantic_extra__: dict[str, RoleEntry] = Field(init=False)

    worker: WorkerRole = WorkerRole()
    """``[roles.worker]``."""
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

    def sweeping(self) -> dict[str, SweepRole]:
        """The sweeping roles by name: the built-in ones, then the sweeping roles of the file in its order."""
        mine = {name: role for name, role in self.__pydantic_extra__.items() if isinstance(role, SweepRole)}
        return {
            WorkerKind.WATCHDOG: self.watchdog,
            WorkerKind.RESEARCH: self.research,
            WorkerKind.SCOUT: self.scout,
            **mine,
        }

    def doing_runs(self) -> dict[str, WorkerRole]:
        """The roles that do runs by name, in the order in which they fill their slots.

        The roles that you define come first, in the order of the file, and the worker comes last.
        """
        mine = {name: role for name, role in self.__pydantic_extra__.items() if isinstance(role, WorkerRole)}
        return {**mine, WorkerKind.WORKER: self.worker}

    def all(self) -> dict[str, RoleConfig]:
        """Every role by name: the built-in roles, then the roles that you define in the order of the file."""
        return {
            WorkerKind.WORKER: self.worker,
            WorkerKind.WATCHDOG: self.watchdog,
            WorkerKind.RESEARCH: self.research,
            WorkerKind.SCOUT: self.scout,
            **self.__pydantic_extra__,
        }


@cache
def registered_roles() -> dict[str, SweepRole | WorkerRole]:
    """Return the roles that packages add (``ROLE_GROUP``), by the name of their entry point.

    Raises:
        TypeError: An entry point does not name a ``SweepRole`` or a ``WorkerRole``.
        ValueError: An entry point reuses the name of a built-in role.
    """
    found: dict[str, SweepRole | WorkerRole] = {}
    for ep in entry_points(group=ROLE_GROUP):
        loaded = ep.load()
        if not isinstance(loaded, SweepRole | WorkerRole):
            raise TypeError(
                f'entry point {ep.name} = {ep.value} in {ROLE_GROUP} is not a piloop SweepRole or WorkerRole'
            )
        if ep.name in Roles.model_fields:
            raise ValueError(f'entry point {ep.name} = {ep.value} in {ROLE_GROUP}: {ep.name!r} is a built-in role')
        found[ep.name] = loaded
    return found


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
    runtime: RuntimeName = 'subprocess'
    """Where the agents run: the name of a runtime, which starts, watches, steers, shows and stops them. A package can
    add a runtime with a ``piloop.runtimes`` entry point. A role can override it (``[roles.<name>] runtime``)."""
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
    """The settings of each role (``[roles.worker]``, ``[roles.watchdog]``, ``[roles.research]``, ``[roles.scout]``),
    and the roles that you define (``[roles.<name>]``)."""
    link: list[str] = []
    """Repo-relative paths, ignored by git, that piloop symlinks into every worktree, for example data or virtual
    environments."""
    context: list[str] = []
    """Repo-relative files or globs (for example ``docs/error-analysis-*.md``) that hold the project's analysis:
    error analyses, comparisons with a reference, evaluation reports. The research pack lists each one with its
    outline, newest first, so that every sweep that reads the pack reads them before it proposes work."""
    mcp: dict[ServerName, McpServer] = {}
    """``[mcp.<name>]``: the MCP servers that a role can list in ``[roles.<name>] mcp``."""
    stop: Stop = Stop()
    """``[stop]``: conditions that end the campaign. None by default."""
    spend: Spend | None = None
    """``[spend]``: a hard budget on what the campaign spends outside piloop. None by default."""
    memory: Memory = Memory()
    """``[memory]``: documents to search and the lessons overview in briefs."""

    @model_validator(mode='after')
    def _check(self) -> Self:
        self._check_tracks()
        self._check_harnesses(self.harnesses, self.model, 'project')
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

    def _check_mcp(self, kind: str) -> None:
        """Every server the role lists exists, and every harness the role may run can take them in its argv."""
        unknown = [n for n in self.role(kind).mcp if n not in self.mcp]
        if unknown:
            raise ValueError(f'roles.{kind}.mcp: no [mcp.<name>] table for {unknown}')
        if not self.role(kind).mcp:
            return
        runtime = self.role_runtime(kind)
        invocation = runtimes()[runtime].invocation
        for name in self.role_harnesses(kind):
            if why := self.harness_for(name).refuses_servers(invocation):
                raise ValueError(
                    f'roles.{kind}.mcp: harness {name!r} under the {runtime} runtime ({invocation}): {why}; or take it'
                    ' out of the rotation'
                )

    def role_servers(self, kind: str) -> dict[str, McpServer]:
        """The MCP servers that the agents of a role get, by name."""
        return {name: self.mcp[name] for name in self.role(kind).mcp}

    def role(self, kind: str) -> RoleConfig:
        """The ``[roles.<kind>]`` table. The keys that only one role has need ``self.roles.<kind>``.

        Raises:
            KeyError: ``piloop.toml`` has no such role.
        """
        return self.roles.all()[kind]

    def wake_budget(self, kind: str) -> int:
        """The tokens of the lessons overview in a brief of this role: its own ``wake_budget``, or else ``[memory]``'s."""
        own = self.role(kind).wake_budget
        return self.memory.wake_budget if own is None else own

    def role_harnesses(self, kind: str) -> list[str]:
        """The rotation a role uses: its own list, or else the project's."""
        return self.role(kind).harnesses or self.harnesses

    def role_models(self, kind: str) -> dict[str, str]:
        """The model for each harness of a role: the project's map, with the role's overrides on top."""
        return {**self.model, **(self.role(kind).model or {})}

    def role_runtime(self, kind: str) -> str:
        """The name of the runtime a role uses: its own, or else the project's."""
        return self.role(kind).runtime or self.runtime

    @property
    def runtimes(self) -> set[str]:
        """The names of the runtimes that the roles of the project use."""
        return {self.role_runtime(kind) for kind in self.roles.all()}

    @property
    def run_slots(self) -> int:
        """The number of runs that can be open at the same time: the ``max_concurrent`` of every role that does runs."""
        return sum(role.max_concurrent for role in self.roles.doing_runs().values())

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


def with_registered_roles(raw: dict[str, object]) -> None:
    """Add each role that a package adds (``registered_roles``) to a parsed ``piloop.toml``, its table over its defaults.

    A value that is not a table is left for the validation to refuse.
    """
    tables = raw.get('roles', {})
    if not (added := registered_roles()) or not isinstance(tables, dict):
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


def load_project_config(project: Path, **overrides: object) -> ProjectConfig:
    """Load ``<project>/piloop.toml``.

    ``overrides`` take precedence over ``PILOOP_PROJECT_*`` environment variables, which take precedence over the
    file. Deprecated keys still load through ``piloop.deprecated``, with a ``PiloopDeprecationWarning``.

    Raises:
        FileNotFoundError: The project has no ``piloop.toml``.
        ConfigError: The file is not valid TOML, not a valid config, or names a prompt file that does not exist.
    """
    toml = project / CONFIG_FILE
    if not toml.is_file():
        raise FileNotFoundError(toml)
    try:
        raw = tomllib.loads(toml.read_text())
    except tomllib.TOMLDecodeError as e:
        raise ConfigError(f'{toml} is not valid TOML: {e}') from e
    deprecated.bridge(toml, raw)
    with_registered_roles(raw)

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
        cfg = ProjectConfig.model_validate(_Loader(**kwargs).model_dump())
    except ValidationError as e:
        raise ConfigError(explain(toml, e)) from e
    missing = [
        f'  roles.{name}.{key} = {rel!r}: no such file. Fix: create it, or correct the path'
        for name, role in cfg.roles.all().items()
        for key, rel in prompt_files(role).items()
        if not (project / rel).is_file()
    ]
    if missing:
        raise ConfigError('\n'.join([f'{toml} is not a valid piloop config ({len(missing)} problem(s)):', *missing]))
    return cfg
