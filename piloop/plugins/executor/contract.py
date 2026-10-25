"""The executor capability: how jobs reach the machines that run them, and how their results come back.

A **job** is one agent session or one evaluation, described as data (``AgentJob``, ``EvaluationJob``): what to check
out, what to run, what to bring back. Core decides what runs (the planner, the roles, the evaluation queue); the
executor decides where and how, and keeps track of it. When a job starts, ends or is lost, the executor calls the core
operations (``Results``), which change the campaign's rows. Like Airflow's executors (``LocalExecutor``,
``CeleryExecutor``): core queues work and reads results, and the executor owns the workers.

Host tier: an executor runs processes. ``[plugins.executor] provider`` in ``config.toml`` selects one (``workers`` by
default). The entry point names an ``ExecutorConfig`` class, whose fields are the other keys of the table, and the
daemon calls its ``open`` once to make the ``Executor``.

Core enforces the domain's invariants only: a job's row closes once, a measurement is recorded once. An executor that
can deliver a result twice, or late (a worker presumed dead), protects the work itself: ``workers`` does it with leases
and fencing tokens, checked inside the session in which it calls ``Results``. What counts as lost work, and how to
protect it, is the executor's choice.
"""

from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Annotated, Literal, NewType, Protocol, Self

from covalence import Cid
from pydantic import AfterValidator, BaseModel, ConfigDict, Field, TypeAdapter
from sqlalchemy.orm import Session

from piloop.db.session import Database
from piloop.ids import AgentSessionId, CampaignId, CandidateId, CommentId, Revision, RunId
from piloop.mcp import McpServer
from piloop.net import Route
from piloop.otel import Tracer
from piloop.plugins.cas.contract import BlobAddress, Cas
from piloop.plugins.host import Capability, Choice, Parameters, Scope
from piloop.plugins.network.contract import Network
from piloop.plugins.runtime.contract import AgentExit, AgentStart
from piloop.plugins.vcs.contract import VCS, Repository, Transfer, Vcs
from piloop.runtime.factory import RuntimeFactory
from piloop.usage import Usage, UsageSpec


def _relative(place: str) -> str:
    if not place or PurePosixPath(place).is_absolute() or '..' in PurePosixPath(place).parts:
        raise ValueError(f'{place!r} is not a relative path inside a home')
    return place


type Place = Annotated[str, AfterValidator(_relative)]
"""Where a job's files go, relative to a home (``runs/12``): the worker makes it under its own home, and core puts
what comes back under its own. A runner's ``_work/<name>``: the job names the directory, the machine roots it."""

RepoId = NewType('RepoId', CampaignId)
"""A repo of record: the campaign whose checkout it is. Two campaigns on clones of one project have two, and so do
two tenants' forks: a vcs's own identity (git's root commit) is the same for all of them."""


class Workspace(BaseModel):
    """A campaign's shared workspace, as a job gets it: read-only copies of its files when the job was made.

    What the job writes, changes or adds there comes back in its report (``AgentFinished.workspace``), and core merges
    it into the workspace at ``place`` under its own home, path by path (``piloop.store.tree.merge``).
    """

    model_config = ConfigDict(frozen=True)

    place: Place
    manifest: BlobAddress
    """Its tree in the CAS when the job was made."""


class Source(BaseModel):
    """An artifact: the files of ``revision`` in ``repo``, checked out at the job's place, with ``links`` in it."""

    model_config = ConfigDict(frozen=True)

    vcs: Annotated[Vcs, VCS.entry]
    """The campaign's vcs provider, with its parameters (``[plugins.vcs]``): the worker checks out with it."""
    repo: RepoId
    revision: Revision
    links: dict[str, BlobAddress] = Field(default_factory=dict)
    """Inputs that no version holds (data, environments), by name in the checkout: each the manifest of its tree in the
    CAS, put there read-only (``piloop.store.tree.Copies``)."""
    workspace: Workspace | None = None
    """The campaign's shared workspace, put into the checkout as ``workspace/``. None: an evaluation's, which has none."""
    history: list[Revision] = Field(default_factory=list)
    """Other versions of ``repo`` that the checkout's repository holds: the campaign's candidates, which an agent reads
    and composes."""


class MissingBlobs(Exception):
    """A report names blobs that the CAS does not hold: sending it again never applies it, so its worker loses the job.

    A worker puts every blob before it reports, so a blob missing then is in another CAS, or lost.
    """


DEADLINE_ENV = 'PILOOP_DEADLINE'
"""The variable in an agent's environment that holds the Unix time at which its ``AgentJob.timeout`` ends it. Every
executor sets it when it starts the agent."""


class AgentJob(BaseModel):
    """The description of an agent's job: everything a worker needs, so it reads no campaign table.

    It names no path of the machine that made it. The worker makes the job's ``place`` under its own home: the log
    directory, with the worktree in it. Secrets go by name: the MCP ``servers`` name the variables, and the worker takes
    their values from its own environment and renders the servers into the argv (``{mcp}``).
    """

    model_config = ConfigDict(frozen=True)

    kind: Literal['agent'] = 'agent'
    agent_session: AgentSessionId
    runtime: str
    """The runtime that starts the agent on the worker's host (``runtime`` in ``piloop.toml``)."""
    artifact: Source
    place: Place
    timeout: float
    """The seconds the agent may run, counted from its start, also across a restart of its worker."""
    brief: str | None
    """The agent's brief, which the worker writes to its log directory before the start: its first prompt names the
    file."""
    spec: AgentStart | None
    """What the runtime starts, once the worker fills in its places. None for an agent that a previous daemon process
    left (``LeftJob``): it is re-attached, never started."""
    mcp: str | None = None
    """The renderer of the harness's MCP servers (``Harness.mcp``), or None for a harness that takes none."""
    servers: dict[str, McpServer] = Field(default_factory=dict)
    """The MCP servers of the agent's role, which name their variables, never their values."""
    usage: UsageSpec | None = None
    """How to read the session's tokens when it ends (``Harness.usage``)."""
    session_id: str | None = None
    """The session id the launcher made for the argv (``{session_id}``), or None when the runtime reports it."""
    capture: RunId | None = None
    """Bring the agent's work back, with what it left unsaved as a version recovered for this run
    (``Repository.capture``). None for a sweep: nothing that it saves comes back."""

    @property
    def needs_session_id(self) -> bool:
        """Whether the session id is needed when the agent ends, for ``usage``: the worker asks the runtime for it."""
        return self.usage is not None


class Bar(BaseModel):
    """The bar of a paired evaluation: the candidate that the measured one must beat, and its tree."""

    model_config = ConfigDict(frozen=True)

    candidate: CandidateId
    artifact: Source


class EvaluationJob(BaseModel):
    """The description of an evaluation's job: the evaluator's runner and its arguments, on the candidate's tree.

    It runs the evaluator ``repeats`` times on the candidate. With a ``bar``, it is a **paired evaluation**: each run of
    the candidate has a run of the bar next to it, in the order candidate, bar, bar, candidate, candidate, bar. So a
    load that changes slowly hits both sides alike.
    """

    model_config = ConfigDict(frozen=True)

    kind: Literal['evaluation'] = 'evaluation'
    evaluation: int
    candidate: CandidateId
    run: RunId | None = None
    """The run whose version it measures, named on the measurement's event. None for a measurement on request."""
    runner: str
    """The evaluator runner (``evaluators.runner``). A worker takes only jobs of the runners that it has."""
    artifact: Source
    place: Place
    """Its log directory under the worker's home: each run of the evaluator has a directory of its own in it."""
    command: list[str]
    timeout: float
    required: list[str]
    """The metrics the evaluate command must print."""
    bar: Bar | None = None
    """The bar to run next to the candidate. None: the candidate alone."""
    repeats: int = Field(default=1, ge=1)
    """How many times the evaluator runs on the candidate, and on the bar."""


type Job = Annotated[AgentJob | EvaluationJob, Field(discriminator='kind')]
JOB: TypeAdapter[AgentJob | EvaluationJob] = TypeAdapter(Job)


class AgentFinished(BaseModel):
    """An agent's process ended, as its executor saw it. Core decides the rest (``Results.finished``)."""

    kind: Literal['agent_finished'] = 'agent_finished'
    process: AgentExit
    session_id: str | None = None
    usage: Usage | None = None
    changed: bool = True
    """Whether the agent changed its checkout: a saved version, or an unsaved change outside the links."""
    work: Transfer | None = None
    """Its work: its versions, when the job asked for them back (``AgentJob.capture``) and there are some."""
    files: dict[str, BlobAddress] = Field(default_factory=dict)
    """Its log directory's files by relative path: core puts them in the job's place under its own home."""
    read: list[CommentId] = Field(default_factory=list)
    """The prompts that the agent read: it took them from its runtime's queue."""
    unread: list[CommentId] = Field(default_factory=list)
    """The prompts that still waited in its runtime's queue when it ended."""
    workspace: dict[str, BlobAddress | None] = Field(default_factory=dict)
    """The files of its ``Workspace`` that it wrote, changed, added or removed (None), by relative path."""

    def blobs(self) -> list[Cid]:
        """Every blob that it names: what the CAS holds before core applies it."""
        work = [self.work.blob] if self.work is not None else []
        return [*self.files.values(), *(blob for blob in self.workspace.values() if blob is not None), *work]


class AgentNotStarted(BaseModel):
    """An agent's job did not start, so no process ran: its worktree, its secrets or its start failed, or a stop came."""

    kind: Literal['agent_not_started'] = 'agent_not_started'
    reason: str
    seconds: float = 0
    """How long it tried."""
    files: dict[str, BlobAddress] = Field(default_factory=dict)
    """Its log directory's files by relative path (``AgentFinished.files``): a failed start's screen, say."""

    def blobs(self) -> list[Cid]:
        """Every blob that it names: what the CAS holds before core applies it."""
        return list(self.files.values())


type AgentReport = AgentFinished | AgentNotStarted
"""How an agent's job ended, as its executor saw it."""


class Sample(BaseModel):
    """One run of the evaluator on one tree, which the control plane records as one measurement."""

    metrics: dict[str, float]
    seconds: float
    error: str | None = None
    units: dict[str, dict[str, float]] = Field(default_factory=dict)


class EvaluationReport(BaseModel):
    """What an evaluation measured, in the order of its runs. It stops at the first run that fails."""

    kind: Literal['evaluation'] = 'evaluation'
    samples: list[Sample] = Field(min_length=1)
    """The candidate's samples. The first one ran first."""
    bar_samples: list[Sample] = Field(default_factory=list)
    """The bar's samples of a paired evaluation: the i-th ran next to the i-th of ``samples``."""
    files: dict[str, BlobAddress] = Field(default_factory=dict)
    """Its log directory's files by relative path (``AgentFinished.files``)."""

    def blobs(self) -> list[Cid]:
        """Every blob that it names: what the CAS holds before core applies it."""
        return list(self.files.values())


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


@dataclass(frozen=True)
class Prompt:
    """A message for a running agent's next turn: a comment, by its id, and the text that the agent gets."""

    id: CommentId
    text: str


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
        self,
        key: AgentKey,
        handle: dict[str, object],
        session_id: str | None,
        *,
        machine: str | None = None,
        guard: Guard | None = None,
    ) -> None:
        """An agent started: record where it runs (``handle``), which steers it. Raises ``Settled``.

        ``machine`` names another machine that runs it, as the executor knows it (a worker); None: this daemon's.
        """
        ...

    def session(self, key: AgentKey, session_id: str, *, guard: Guard | None = None) -> None:
        """The runtime reported the agent's session id: the usage needs it. Raises ``Settled``."""
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

        An agent's run ends ``worker_lost``, and an evaluation waits again for a slot of the queue.
        """
        ...

    def queued(self, key: AgentKey, prompt: CommentId, error: str | None) -> None:
        """A prompt waits for its agent in the agent's runtime, or ``error`` says why it does not.

        Whether the agent read it comes in the agent's report (``AgentFinished.read``).
        """
        ...


@dataclass(frozen=True)
class Host:
    """What piloop gives an executor."""

    db: Database
    results: Results
    tracer: Tracer
    home: Path
    cas: Cas
    """The CAS that jobs' inputs and results go through: a job names blobs, never paths."""
    repos: Callable[[RepoId], Path]
    """The checkout of a repo of record (``Source.repo``). Raises ``KeyError`` for a campaign that is gone."""
    records: Callable[[RepoId], Repository]
    """A repo of record, opened with its campaign's vcs provider: what a worker that lacks versions is sent from. Raises
    ``KeyError`` for a campaign that is gone."""
    runtimes: RuntimeFactory
    """Makes the runtime of a job (``AgentJob.runtime``) on this machine."""
    reserve: int
    """Bytes kept free on the disk of ``home`` (``[storage] min_free_gb``): below it, a job's end evicts the linked
    inputs that no job uses (``Site.evict_inputs``)."""
    network: Network
    """The server's network: what gives a machine that the executor starts its credential (``Network.enroll``)."""


@dataclass(frozen=True)
class Capacity:
    """How many more jobs of each kind an executor can take now: its free slots. None: no bound."""

    agents: int | None
    evaluations: int | None


class Executor[C: ExecutorConfig](Protocol):
    """Runs the jobs that piloop submits, and reports their results through ``Host.results``.

    The daemon calls it from its threads; an executor makes its methods safe for that.
    """

    def capacity(self) -> Capacity:
        """How many more jobs of each kind it can take now: its free slots, less the jobs it has and has not started.

        Core submits no more than that, in its own order: it launches agents across campaigns, and evaluations in the
        order of the evaluation queue. The order and the fairness are core's.
        """
        ...

    def configure(self, config: C) -> None:
        """Take ``config``, the one that opened it with the live limits applied. The daemon calls it when one changes."""
        ...

    def submit(self, job: AgentJob | EvaluationJob) -> None:
        """Run ``job``, now or when a slot is free. Every job ends with ``Results.finished`` or ``Results.lost``."""
        ...

    def prompt(self, key: AgentKey, prompt: Prompt) -> None:
        """Give ``prompt`` to the running agent's runtime at least once, then call ``Results.queued``.

        Core calls it once for each prompt in each daemon process, and again after a restart when no outcome was
        recorded: an executor that may deliver it twice skips the ids that it has seen.
        """
        ...

    def cancel(self, key: JobKey) -> None:
        """Stop a job: a kill or a cancel. It still reports how it ended. Called again, it tries again."""
        ...

    def adopt(self, left: Sequence[LeftJob]) -> list[JobKey]:
        """Take over the jobs that a previous daemon process left, and return the keys of those it cannot.

        Core fails the rows of an agent that it cannot adopt, and asks again for an evaluation.
        """
        ...

    def routes(self) -> Sequence[Route]:
        """The routes that its workers on other machines call, under ``/v1/executor/`` (``piloop serve --listen``)."""
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


class ExecutorConfig(Parameters, ABC):
    """An executor's ``[plugins.executor]``: its fields are the keys of the table besides ``provider``."""

    @abstractmethod
    def open(self, host: Host) -> Executor[Self]:
        """Make the executor. The daemon calls it once, when it starts."""


EXECUTORS = Capability('executor', ExecutorConfig, choice=Choice(Scope.HOME, default='workers'))
"""Every executor by name. ``workers``, built in and the default, runs every job on a lease
(``piloop.plugins.executor.kit.leases``)."""
