"""The executor capability: how jobs reach the machines that run them, and how their results come back.

A **job** is one agent session or one evaluation, described as data (``AgentJob``, ``EvaluationJob``): what to check
out, what to run, what to bring back. Core decides what runs (the planner, the roles, the evaluation queue); the
executor decides where and how, and keeps track of it. When a job starts, ends or is lost, the executor calls the core
operations (``Results``), which change the campaign's rows. Like Airflow's executors (``LocalExecutor``,
``CeleryExecutor``): core queues work and reads results, and the executor owns the workers.

Host tier: an executor runs processes. ``[plugins.executor] provider`` in ``config.toml`` selects one (``workers`` by
default), and the other keys of the table are its parameters (``Parameters``). The entry point names a class:
``Executor(host, parameters)``.

Core enforces the domain's invariants only: a job's row closes once, a measurement is recorded once. An executor that
can deliver a result twice, or late (a worker presumed dead), protects the work itself: ``workers`` does it with leases
and fencing tokens, checked inside the session in which it calls ``Results``. What counts as lost work, and how to
protect it, is the executor's choice.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Annotated, ClassVar, Literal, Protocol, TypeIs

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter
from sqlalchemy.orm import Session

from piloop.db.models import UsageJson
from piloop.db.session import Database
from piloop.ids import AgentSessionId, CandidateId, RunId
from piloop.mcp import McpServer
from piloop.otel import Tracer
from piloop.plugins.host import Capability, PluginTable, Selection
from piloop.plugins.runtime.contract import AgentExit, AgentSpec
from piloop.runtime.factory import RuntimeFactory
from piloop.usage import UsageSpec


class GitTree(BaseModel):
    """An artifact: the tree of ``commit`` in ``repo``, made into a worktree at the job's ``cwd`` with its ``links``."""

    model_config = ConfigDict(frozen=True)

    kind: Literal['git-tree'] = 'git-tree'
    repo: Path
    commit: str
    links: dict[str, Path] = Field(default_factory=dict)
    """Paths linked into the worktree (``git.add_worktree``): data and environments that are not committed."""


class Checkout(BaseModel):
    """An artifact that is already there: the campaign checkout, for a sweep whose ``workdir`` is the repo."""

    model_config = ConfigDict(frozen=True)

    kind: Literal['checkout'] = 'checkout'
    path: Path


type Artifact = Annotated[GitTree | Checkout, Field(discriminator='kind')]


class Commits(BaseModel):
    """How an agent's commits come back: what it left uncommitted is committed on its own, then the head is pinned."""

    model_config = ConfigDict(frozen=True)

    message: str
    """The message of the commit of what the agent left uncommitted."""
    ref: str
    """Where the head is pinned before the worktree goes. Core pins the run's commits from it, then deletes it."""


class AgentJob(BaseModel):
    """The description of an agent's job: everything a worker needs, so it reads no campaign table.

    Secrets go by name: the MCP ``servers`` name the variables, and the worker takes their values from its own
    environment. The worker renders the servers into the argv (``{mcp}``) in the materialized ``cwd``.
    """

    model_config = ConfigDict(frozen=True)

    kind: Literal['agent'] = 'agent'
    agent_session: AgentSessionId
    runtime: str
    """The runtime that starts the agent on the worker's host (``runtime`` in ``piloop.toml``)."""
    campaign: str
    """The campaign's name, which a runtime can name what it makes after (Herdr's workspace)."""
    campaign_path: Path
    artifact: Artifact
    cwd: Path
    log_dir: Path
    timeout: float
    """The seconds the agent may run, counted from its start, also across a restart of its worker."""
    brief: str | None
    """The agent's brief, which the worker writes to ``log_dir`` before the start: its first prompt names the file."""
    spec: AgentSpec | None
    """What the runtime starts. None for an agent that a previous daemon process left (``LeftJob``): it is
    re-attached, never started."""
    mcp: str | None = None
    """The renderer of the harness's MCP servers (``Harness.mcp``), or None for a harness that takes none."""
    servers: dict[str, McpServer] = Field(default_factory=dict)
    """The MCP servers of the agent's role, which name their variables, never their values."""
    usage: UsageSpec | None = None
    """How to read the session's tokens when it ends (``Harness.usage``)."""
    entire_agent: str | None = None
    """The Entire agent to attach the session to when it ends (``Harness.entire_agent``)."""
    captures_session: bool = False
    """Whether the session id is needed when the agent ends: the worker asks the runtime for it."""
    session_id: str | None = None
    """The session id the launcher made for the argv (``{session_id}``), or None when the runtime reports it."""
    commits: Commits | None = None
    """Bring the agent's commits back. None for a sweep: nothing that it commits comes back."""


class EvaluationJob(BaseModel):
    """The description of an evaluation's job: the evaluator's runner and its arguments, on the candidate's tree."""

    model_config = ConfigDict(frozen=True)

    kind: Literal['evaluation'] = 'evaluation'
    evaluation: int
    candidate: CandidateId
    run: RunId | None = None
    """The run whose commit it measures, named on the measurement's event. None for a measurement on request."""
    runner: str
    """The evaluator runner (``evaluators.runner``). A worker takes only jobs of the runners that it has."""
    artifact: GitTree
    cwd: Path
    log_dir: Path
    command: list[str]
    timeout: float
    required: list[str]
    """The metrics the evaluate command must print."""
    units_file: Path
    """Where the evaluate command writes its per-unit results (``UNITS_ENV``)."""


type Job = Annotated[AgentJob | EvaluationJob, Field(discriminator='kind')]
JOB: TypeAdapter[AgentJob | EvaluationJob] = TypeAdapter(Job)


class AgentFinished(BaseModel):
    """An agent's process ended, as its executor saw it. Core decides the rest (``Results.finished``)."""

    kind: Literal['agent_finished'] = 'agent_finished'
    process: AgentExit
    session_id: str | None = None
    usage: UsageJson | None = None
    checkpoint: str | None = None
    changed: bool = True
    """Whether the agent changed its checkout: a commit, or an uncommitted change outside the links."""
    head: str | None = None
    """The head of its commits, pinned at ``Commits.ref``, when the job asked for them back and there are some."""


class AgentNotStarted(BaseModel):
    """An agent's job did not start, so no process ran: its worktree, its secrets or its start failed, or a stop came."""

    kind: Literal['agent_not_started'] = 'agent_not_started'
    reason: str
    seconds: float = 0
    """How long it tried."""


type AgentReport = AgentFinished | AgentNotStarted
"""How an agent's job ended, as its executor saw it."""


class EvaluationReport(BaseModel):
    """What an evaluation measured: one run of the evaluator, which the control plane records as one measurement."""

    kind: Literal['evaluation'] = 'evaluation'
    metrics: dict[str, float]
    seconds: float
    error: str | None = None
    units: dict[str, dict[str, float]] = Field(default_factory=dict)


type Report = Annotated[AgentFinished | AgentNotStarted | EvaluationReport, Field(discriminator='kind')]
REPORT: TypeAdapter[AgentReport | EvaluationReport] = TypeAdapter(Report)


@dataclass(frozen=True)
class AgentKey:
    """The key of an agent's job: its ``agent_sessions`` row."""

    agent_session: AgentSessionId


@dataclass(frozen=True)
class EvaluationKey:
    """The key of an evaluation's job: its row of the evaluation queue (``evaluations``)."""

    evaluation: int


type JobKey = AgentKey | EvaluationKey


def key_of(job: AgentJob | EvaluationJob) -> JobKey:
    """Return the key of ``job``."""
    return AgentKey(job.agent_session) if isinstance(job, AgentJob) else EvaluationKey(job.evaluation)


@dataclass(frozen=True)
class LeftJob:
    """A job that a previous daemon process left in flight, for ``Executor.adopt``.

    An agent's comes with its description as its row has it (no spec: re-attach it, never start it) when its row is
    ``running``, and without one while it was still starting. An evaluation's is its key only.
    """

    key: JobKey
    job: AgentJob | None = None
    handle: dict[str, object] | None = None
    """The agent's handle in its runtime, when its start was recorded."""
    started_at: datetime | None = None
    """When the agent started, which its timeout counts from."""


class Settled(Exception):
    """A result for a job whose row is settled already: closed, measured, or gone. Core refused it; nothing changed."""


type Guard = Callable[[Session], None]
"""An executor's own check and write, which a ``Results`` operation runs in its transaction before its own writes.

The leases kit passes one: its fence, and the lease's change. It raises to refuse, and then nothing changes. Any other
executor passes none.
"""


class Results(Protocol):
    """The core operations that an executor calls on what its jobs do. Each commits in a write transaction of its own.

    Core checks only the domain: a job's row closes once (``Settled``).
    """

    def started(
        self, key: AgentKey, handle: dict[str, object], session_id: str | None, *, guard: Guard | None = None
    ) -> None:
        """An agent started: record where it runs (``handle``), which steers it. Raises ``Settled``."""
        ...

    def session(self, key: AgentKey, session_id: str, *, guard: Guard | None = None) -> None:
        """The runtime reported the agent's session id: the usage and the Entire attach need it. Raises ``Settled``."""
        ...

    def finished(
        self, job: AgentJob | EvaluationJob, report: AgentReport | EvaluationReport, *, guard: Guard | None = None
    ) -> None:
        """Apply the result of a job that ended: close the agent's row, or record the measurement. Raises ``Settled``.

        ``report`` is of the job's kind.
        """
        ...

    def lost(self, key: JobKey, reason: str, *, guard: Guard | None = None) -> None:
        """Nothing came back from a job. Raises ``Settled``.

        An agent's run fails as ``worker lost``, and an evaluation waits again for a slot of the queue.
        """
        ...


@dataclass(frozen=True)
class Host:
    """What piloop gives an executor."""

    db: Database
    results: Results
    tracer: Tracer
    home: Path
    runtimes: RuntimeFactory
    """Makes the runtime of a job (``AgentJob.runtime``) on this machine."""
    entire_version: str | None = None
    """What ``entire version`` printed on this machine when the daemon started, or None without ``entire``: the
    attach is still tried then, and the event says why it failed. Below ``MIN_ENTIRE``, no session is attached."""


class NoParameters(BaseModel):
    """The ``Parameters`` of an executor that takes no keys in ``[plugins.executor]`` besides ``provider``."""

    model_config = ConfigDict(extra='forbid', frozen=True)


class Executor(Protocol):
    """Runs the jobs that piloop submits, and reports their results through ``Host.results``.

    The daemon calls it from its threads; an executor makes its methods safe for that.
    """

    Parameters: ClassVar[type[BaseModel]]
    """The keys of ``[plugins.executor]`` besides ``provider``."""

    def __init__(self, host: Host, parameters: BaseModel) -> None:
        """Made once, when the daemon starts, with ``parameters`` checked against ``Parameters``."""
        ...

    @classmethod
    def capacity(cls, parameters: BaseModel) -> int | None:
        """How many evaluations it runs at the same time with ``parameters``, or None for no bound.

        Core submits an evaluation only when one of these slots is free, in the order of its queue: the order and the
        fairness are core's. ``piloop eval queue`` shows the number, without a daemon: it takes no instance.
        """
        ...

    def submit(self, job: AgentJob | EvaluationJob) -> None:
        """Run ``job``, now or when a slot is free. Every job ends with ``Results.finished`` or ``Results.lost``."""
        ...

    def cancel(self, key: JobKey) -> None:
        """Stop a job: a kill or a cancel. It still reports how it ended. Called again, it tries again."""
        ...

    def adopt(self, left: Sequence[LeftJob]) -> list[JobKey]:
        """Take over the jobs that a previous daemon process left, and return the keys of those it cannot.

        Core fails the rows of an agent that it cannot adopt, and asks again for an evaluation.
        """
        ...

    def tick(self, now: datetime) -> datetime | None:
        """The daemon's periodic call: do the executor's own housekeeping, and return when it next has some."""
        ...

    def stop(self, timeout: float) -> None:
        """Stop every job, and wait up to ``timeout`` for them to report. Submit no more."""
        ...

    def hand_over(self) -> None:
        """Leave the jobs running for the next daemon process (an in-place restart), which adopts them."""
        ...

    def close(self) -> None:
        """Release what the executor holds. The daemon calls it last."""
        ...


def _is_executor(loaded: object) -> TypeIs[type[Executor]]:
    methods = ('capacity', 'submit', 'cancel', 'adopt', 'tick', 'stop', 'hand_over', 'close')
    parameters = getattr(loaded, 'Parameters', None)
    return (
        isinstance(loaded, type)
        and isinstance(parameters, type)
        and issubclass(parameters, BaseModel)
        and all(callable(getattr(loaded, m, None)) for m in methods)
    )


def _check(name: str, loaded: object) -> type[Executor]:
    if _is_executor(loaded):
        return loaded
    raise TypeError('is not a piloop executor class: it needs Parameters and the methods of Executor')


EXECUTORS = Capability(
    'executor', group='piloop.executors', check=_check, selection=Selection('home', 'plugins.executor.provider')
)
"""Every executor by name. ``workers``, built in and the default, runs every job on a lease (``piloop.plugins.executor.kit.leases``)."""
DEFAULT_EXECUTOR = 'workers'
"""The executor of a home whose ``config.toml`` has no ``[plugins.executor]``."""


def selected(table: PluginTable | None) -> tuple[type[Executor], BaseModel]:
    """The executor that ``[plugins.executor]`` selects, ``DEFAULT_EXECUTOR`` without one, and its checked parameters.

    Raises:
        MissingProvider: No executor has that name.
        ValidationError: Its ``Parameters`` refuse the other keys of the table.
    """
    provider = EXECUTORS.get(table.provider if table is not None else DEFAULT_EXECUTOR)
    return provider, provider.Parameters.model_validate(table.keys if table is not None else {})
