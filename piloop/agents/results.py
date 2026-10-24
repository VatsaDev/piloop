"""The core operations that an executor calls on what its jobs do (``Results``): they change the campaign's rows.

Each checks the domain only: a job's row closes once (``Settled``). Each holds the write lock from its check to the
commit, in a write transaction of its own, so two calls for one job never both pass the check. Fencing a result that comes late, or twice, is
the executor's (``piloop.plugins.executor.contract``). An agent's end is a ``JobEnded`` event on its row: what waits for
the agent reads it there (``Launcher.launch``), whichever process applied it.
"""

import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Literal, assert_never

from covalence import Cid
from pydantic import Field, TypeAdapter, ValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.actor import Agent, SignedAgent, System
from piloop.agents.roles import harness_of
from piloop.db.events import emit, has, latest
from piloop.db.models import (
    LIVE,
    AgentSession,
    AgentSessionState,
    Campaign,
    Comment,
    CommentKind,
    Evaluation,
    Event,
    EventModel,
    Idea,
    Measurement,
    Run,
    RunEnd,
    utcnow,
)
from piloop.db.session import Database, write_transaction
from piloop.ids import (
    AgentSessionId,
    CandidateId,
    CommentId,
    EvaluatorId,
    IdeaId,
    MeasurementId,
    RoleName,
    RunId,
    TargetKind,
    TargetRef,
)
from piloop.otel import Tracer
from piloop.plugins.cas.contract import BlobAddress, Cas
from piloop.plugins.executor.contract import (
    AgentFinished,
    AgentJob,
    AgentKey,
    AgentNotStarted,
    AgentReport,
    EvaluationJob,
    EvaluationKey,
    EvaluationReport,
    Guard,
    JobKey,
    MissingBlobs,
    Sample,
    Settled,
    Workspace,
)
from piloop.plugins.runtime.contract import AgentExit, ExitStatus
from piloop.plugins.vcs.contract import Transfer
from piloop.scoring.ledger import MeasurementRecorded, save_units
from piloop.settings import TMP_DIR, UNITS_DIR
from piloop.store.tree import Manifest, Staged, merge, stage, write_files

STARTUP_SECONDS = 120
"""An agent that ends sooner than this and leaves nothing did not start to work: a crash, broken tools, no quota."""
NEVER_WORKED = f'it ended within {STARTUP_SECONDS}s and left nothing'
"""Why an agent that ended with no error did not start to work (``_never_worked``)."""


class AgentSessionStarted(EventModel):
    """The launcher started an agent: its row is ``starting``."""

    kind: Literal['agent_session.started'] = 'agent_session.started'
    role: RoleName
    harness: str
    model: str | None
    reason: str | None
    """The row's ``reason``: why it was requested."""


class AgentSessionFinished(EventModel):
    """An agent's row closed: its process ended, or its job never started or was lost."""

    kind: Literal['agent_session.finished'] = 'agent_session.finished'
    role: RoleName
    reason: str | None
    """The row's ``reason``: why it was requested."""
    state: AgentSessionState
    """The row's ``state``: ``exited``, or ``killed`` when the daemon or a timeout ended it, or no process ran. How the
    process ended is in ``agent_session.job_ended``."""
    error: str | None


class StartupAnomaly(EventModel):
    """An agent did not start to work (``NotStarted``): an environment fault, not bad work."""

    kind: Literal['anomaly.startup'] = 'anomaly.startup'
    role: RoleName
    harness: str | None
    run: RunId | None
    seconds: float
    error: str | None
    """Why it did not start. None only in an event of 0.1.0, which did not always know."""


class WrapUp(EventModel):
    """The daemon told an agent to commit and stop, with the ``comment``: the note that carries the prompt.

    An implementer of a stopped campaign, or an agent cancelled with its wrap-up, is ended ``STOP_GRACE_SECONDS`` later.
    An implementer whose role timeout is near is ended by its timeout. A note that does not reach the agent has its
    ``comment.undelivered``; the grace period starts anyway.
    """

    kind: Literal['agent_session.wrap_up'] = 'agent_session.wrap_up'
    reason: str
    comment: CommentId | None = None
    """None when the agent no longer ran, or for a wrap-up from before the note carried the prompt."""


class AgentEnded(EventModel):
    """The daemon ended an agent, with the ``reason`` and the run's ``end``: a stop, a cancel, or its finding.

    An implementer of a stopped campaign is ended when the grace period after its wrap-up prompt is over, or at once for
    a stop that ends it now. Most harnesses take a message only when the turn ends, so the implementer may never have
    read the prompt. The scheduler sets the row ``ending`` and commits it with the event before the kill, so
    a finding that the agent writes after it is refused. The run ends ``campaign_stopped`` or ``cancelled`` unless its
    implementer left its finding before.
    """

    kind: Literal['agent_session.stop_ended'] = 'agent_session.stop_ended'
    reason: str
    end: RunEnd


class WorkspaceConflict(EventModel):
    """What an agent wrote in its workspace that another agent wrote too, meanwhile: the first one's stays."""

    kind: Literal['agent_session.workspace_conflict'] = 'agent_session.workspace_conflict'
    beside: dict[str, str] = Field(default_factory=dict)
    """Workspace path → where this agent's file went instead: beside it, named after the agent."""
    blocked: dict[str, BlobAddress] = Field(default_factory=dict)
    """Workspace path → this agent's file in the CAS, where a directory or a symlink stands now: it is only there."""
    kept: list[str] = Field(default_factory=list)
    """The paths that this agent removed and another one changed meanwhile: they stay."""


class PromptOutcome(EventModel):
    """What came of a prompt to an agent, on the agent.

    ``comment.queued`` once the agent's runtime holds the prompt, then ``comment.delivered`` once the agent read it.
    ``comment.undelivered`` when it never did: the runtime refused it, or the agent ended before it read it.
    """

    kind: Literal['comment.queued', 'comment.delivered', 'comment.undelivered']
    comment: CommentId
    error: str | None = None
    """Why it did not reach the agent: only on ``comment.undelivered``."""


FINAL_OUTCOMES = ('comment.delivered', 'comment.undelivered')
"""The outcomes of a prompt after which nothing more comes of it."""
UNREAD = 'the agent ended before it read it'
"""The error of the ``comment.undelivered`` of a prompt that still waited in the agent's queue when it ended."""


def queue_prompt(s: Session, agent_session_id: AgentSessionId, text: str, author: str) -> CommentId | None:
    """Write ``text`` as a note on a running agent, which the daemon delivers (``Executor.prompt``). The caller commits.

    Return the note's id, or None when the agent is not running.
    """
    agent = s.get(AgentSession, agent_session_id)
    if agent is None or agent.state is not AgentSessionState.RUNNING:
        return None
    note = Comment(
        target_kind=TargetKind.AGENT_SESSION,
        target_id=agent_session_id,
        author=author,
        kind=CommentKind.NOTE,
        body=text,
    )
    s.add(note)
    s.flush()
    return note.id


@dataclass(frozen=True)
class Ran:
    """The agent started to work, and its process ended."""

    process: AgentExit
    commits: Transfer | None = None
    """Its versions in the CAS, when it was asked for them and made some."""
    kind: Literal['ran'] = 'ran'


@dataclass(frozen=True)
class NotStarted:
    """The agent did not start to work: a fault of the host or the harness, not an attempt.

    Its job did not start (``AgentNotStarted``), or it ended within ``STARTUP_SECONDS`` and left nothing
    (``_never_worked``).
    """

    reason: str
    kind: Literal['not_started'] = 'not_started'


@dataclass(frozen=True)
class Lost:
    """Nothing came back from the agent (``Results.lost``): its executor lost it."""

    reason: str
    kind: Literal['lost'] = 'lost'


@dataclass(frozen=True)
class DaemonStopped:
    """The daemon stopped before the agent started or while it ran. The stop writes the rows."""

    commits: Transfer | None = None
    """Its versions in the CAS, when it ran, was asked for them and made some."""
    kind: Literal['daemon_stopped'] = 'daemon_stopped'


@dataclass(frozen=True)
class HandedOver:
    """An in-place restart left the agent running for the next daemon process, which waits for it again."""

    kind: Literal['handed_over'] = 'handed_over'


type Ended = Annotated[Ran | NotStarted | Lost | DaemonStopped | HandedOver, Field(discriminator='kind')]
"""How an agent's job ended, for what waits for it. Only ``Ran`` has a process exit: only then did a process end."""


class JobEnded(EventModel):
    """How an agent's job ended, on its row: what waits for the agent reads it there (``Launcher.launch``)."""

    kind: Literal['agent_session.job_ended'] = 'agent_session.job_ended'
    ended: Ended


@dataclass(frozen=True)
class Proposer:
    """The agent that proposed an implementer's idea, and its harness session."""

    actor: SignedAgent
    harness: str | None
    """Its ``[harness.<name>]``. None when its row is gone."""
    executable: str | None
    """Its harness's ``executable``. None when its row is gone, or ``piloop.toml`` no longer names the harness."""
    session_id: str | None
    """Its harness's session id. None until piloop knows it, or when its row is gone."""


class Identified(EventModel):
    """An agent's harness session is new to its row (``learn``), with what the agent does in the campaign."""

    kind: Literal['agent_session.identified'] = 'agent_session.identified'
    session_id: str
    """The harness's own session id."""
    harness: str
    """The ``[harness.<name>]`` that runs the agent."""
    executable: str | None
    """The harness's ``executable``. None when ``piloop.toml`` no longer names the harness."""
    role: RoleName
    model: str | None
    campaign: str | None
    """The campaign's name."""
    run: RunId | None
    idea: IdeaId | None
    proposed_by: Proposer | None
    """The agent that proposed the run's idea (``ideas.created_by``). None when a person or the orchestrator did, or
    without a run."""


SIGNED: TypeAdapter[SignedAgent] = TypeAdapter(SignedAgent)
"""Reads an agent from its signature, such as an ``ideas.created_by``."""


class CampaignResults:
    """``Results``: what an agent's or an evaluation's end changes in the campaign's rows.

    A job's files come back in ``cas``, and go into its place under ``home``, where the readers of logs look.
    """

    def __init__(self, db: Database, tracer: Tracer, home: Path, cas: Cas, stopping: Callable[[], bool]) -> None:
        self._db = db
        self._tracer = tracer
        self._home = home
        self._units_dir = home / UNITS_DIR
        self._cas = cas
        self._stopping = stopping
        """Whether the daemon stops: an agent that ends then closes as killed by the stop, and a measurement is not
        recorded (the next daemon measures it again)."""

    def _apply(self, guard: Guard | None, change: Callable[[Session], None]) -> None:
        """Run ``guard``, then ``change``, in one write transaction, and commit. Nothing changes when one raises."""
        with self._db.session() as s:
            write_transaction(s)
            if guard is not None:
                guard(s)
            change(s)
            s.commit()

    def started(
        self,
        key: AgentKey,
        handle: dict[str, object],
        session_id: str | None,
        *,
        machine: str | None = None,
        guard: Guard | None = None,
    ) -> None:
        """The agent runs: its row is ``running``, with its handle in its runtime on ``machine``: ``piloop attach``."""

        def change(s: Session) -> None:
            w = _open(s, key)
            w.handle, w.machine = handle, machine
            if w.state is AgentSessionState.STARTING:  # a row that the daemon ended meanwhile stays ending
                w.state = AgentSessionState.RUNNING
            learn(s, w, session_id)

        self._apply(guard, change)

    def session(self, key: AgentKey, session_id: str, *, guard: Guard | None = None) -> None:
        def change(s: Session) -> None:
            learn(s, _open(s, key), session_id)

        self._apply(guard, change)

    def finished(
        self, job: AgentJob | EvaluationJob, report: AgentReport | EvaluationReport, *, guard: Guard | None = None
    ) -> None:
        """Apply the report once the CAS holds every blob that it names, else raise ``MissingBlobs``.

        Its files go into the job's place in the transaction that applies it: what reads the row closed finds them.
        """
        if missing := self._cas.missing(report.blobs()):
            raise MissingBlobs(f'its report names blobs that the CAS lacks: {", ".join(map(str, missing))}')
        workspace = job.artifact.workspace if isinstance(job, AgentJob) else None
        changed = report.workspace if isinstance(report, AgentFinished) and workspace is not None else {}
        # read from the CAS before the write lock: a workspace file can be large, and a remote CAS slow
        base = Manifest.get(self._cas, workspace.manifest) if workspace is not None and changed else None
        with tempfile.TemporaryDirectory(dir=self._scratch()) as scratch:
            staged = stage(self._cas, changed, Path(scratch))

            def change(s: Session) -> None:
                if isinstance(job, AgentJob):
                    assert isinstance(report, AgentFinished | AgentNotStarted)
                    self._agent(s, job, report)
                    if workspace is not None and base is not None:
                        self._workspace(s, job, workspace, base, staged, changed)
                else:
                    assert isinstance(report, EvaluationReport)
                    self._measured(s, job, report)
                write_files(self._cas, report.files, self._home / job.place)

            self._apply(guard, change)
        if isinstance(job, AgentJob) and isinstance(report, AgentFinished):
            for comment in report.read:
                self._tracer.event('comment.delivered', comment=comment, agent_session=job.agent_session)
            for comment in report.unread:
                self._tracer.event(
                    'comment.undelivered', comment=comment, agent_session=job.agent_session, error=UNREAD
                )

    def lost(self, key: JobKey, reason: str, *, guard: Guard | None = None) -> None:
        def change(s: Session) -> None:
            if isinstance(key, EvaluationKey):
                evaluation = s.get(Evaluation, key.evaluation)
                if evaluation is None:
                    raise Settled(f'evaluation#{key.evaluation} left the queue')
                evaluation.started_at = None
                return
            w = _open(s, key)
            w.state, w.ended_at, w.error = AgentSessionState.KILLED, utcnow(), reason
            session_finished(s, w)
            _ended(s, w.id, Lost(reason))

        self._apply(guard, change)

    def queued(self, key: AgentKey, prompt: CommentId, error: str | None) -> None:
        """Record ``comment.queued``, or ``comment.undelivered`` with ``error``, unless the comment has an outcome."""
        outcome = PromptOutcome(
            kind='comment.queued' if error is None else 'comment.undelivered', comment=prompt, error=error
        )
        self._apply(None, lambda s: _record(s, key.agent_session, outcome))
        self._tracer.event(outcome.kind, comment=prompt, agent_session=key.agent_session, error=error)

    def _agent(self, s: Session, job: AgentJob, report: AgentReport) -> None:
        """Close the row from the report, and emit ``AgentSessionFinished``, ``JobEnded`` and ``StartupAnomaly``.

        The anomaly only when the agent did not start to work (``NotStarted``): its job did not start, or it ended
        within ``STARTUP_SECONDS`` and left nothing (``_never_worked``), unless the daemon told it to stop or ended it.
        While the daemon stops, the row closes as killed by the stop (``RunEnd.DAEMON_STOPPED``), with the usage that was read.
        """
        w = _open(s, AgentKey(job.agent_session))
        w.ended_at = utcnow()
        match report:
            case AgentNotStarted(reason=reason, seconds=seconds):
                if self._stopping():
                    self._stopped(s, w, None)
                    return
                w.state, w.error = AgentSessionState.KILLED, reason
                ended: Ran | NotStarted = NotStarted(reason)
            case AgentFinished(process=process):
                learn(s, w, report.session_id)
                for comment in report.read:
                    _record(s, w.id, PromptOutcome(kind='comment.delivered', comment=comment))
                for comment in report.unread:
                    _record(s, w.id, PromptOutcome(kind='comment.undelivered', comment=comment, error=UNREAD))
                w.usage = report.usage
                if self._stopping():
                    self._stopped(s, w, report.commits)
                    return
                asked = _asked_to_end(s, w.id)
                ending = w.state is AgentSessionState.ENDING  # its error says why the daemon ended it
                w.state = (
                    AgentSessionState.KILLED
                    if ending or process.status is ExitStatus.TIMED_OUT
                    else AgentSessionState.EXITED
                )
                w.error = w.error if ending else process.error
                seconds = process.seconds
                worked = asked or not _never_worked(s, w, report)
                ended = Ran(process, report.commits) if worked else NotStarted(process.error or NEVER_WORKED)
            case _:
                assert_never(report)
        session_finished(s, w)
        if isinstance(ended, NotStarted):
            anomaly = StartupAnomaly(
                role=w.kind,
                harness=w.harness,
                run=w.run_id,
                seconds=round(seconds, 1),
                error=ended.reason,
            )
            emit(s, anomaly, TargetRef(TargetKind.AGENT_SESSION, w.id), System.DAEMON)
            self._tracer.event(anomaly.kind, agent_session=w.id, harness=w.harness, error=ended.reason)
        _ended(s, w.id, ended)

    def _workspace(
        self,
        s: Session,
        job: AgentJob,
        workspace: Workspace,
        base: Manifest,
        staged: dict[str, Staged | None],
        changed: dict[str, Cid | None],
    ) -> None:
        """Merge what the agent wrote or removed in its workspace into the campaign's, path by path (``merge``).

        A path that another agent wrote meanwhile keeps that agent's file: this one's goes beside it, named after the
        agent, and this one's removal is not done. ``WorkspaceConflict`` says which.
        """
        w = s.get_one(AgentSession, job.agent_session)
        merged = merge(self._home / workspace.place, base, staged, f'{w.kind}-{w.id}')
        if merged.beside or merged.blocked or merged.kept:
            blocked = {rel: blob for rel in merged.blocked if (blob := changed[rel]) is not None}
            conflict = WorkspaceConflict(beside=merged.beside, blocked=blocked, kept=merged.kept)
            emit(s, conflict, TargetRef(TargetKind.AGENT_SESSION, w.id), System.DAEMON)
            self._tracer.event(
                conflict.kind,
                agent_session=w.id,
                beside=len(merged.beside),
                blocked=len(merged.blocked),
                kept=len(merged.kept),
            )

    def _scratch(self) -> Path:
        """``<home>/tmp``, where the blobs of a merge wait: on the disk of the workspace, so that a move is a rename."""
        scratch = self._home / TMP_DIR
        scratch.mkdir(parents=True, exist_ok=True)
        return scratch

    @staticmethod
    def _stopped(s: Session, w: AgentSession, commits: Transfer | None) -> None:
        w.state, w.error = AgentSessionState.KILLED, RunEnd.DAEMON_STOPPED
        session_finished(s, w)
        _ended(s, w.id, DaemonStopped(commits))

    def _measured(self, s: Session, job: EvaluationJob, report: EvaluationReport) -> None:
        """Record each sample of an evaluation as one measurement, and take the evaluation out of the queue.

        A sample of the candidate names the bar's sample that ran next to it (``Measurement.pair``).

        A cancelled evaluation records nothing: ``queue.cancel`` recorded its crash. Nor does one that ends while the
        daemon stops: its evaluator was killed, and the next daemon measures it again.
        """
        evaluation = s.get(Evaluation, job.evaluation)
        if evaluation is None or evaluation.started_at is None:
            raise Settled(f'evaluation#{job.evaluation} is not running')
        s.delete(evaluation)
        if self._stopping() or evaluation.cancelled_by is not None:
            return
        evaluator_id = evaluation.evaluator_id
        bar = job.bar
        assert bar is not None or not report.bar_samples  # a bar's samples come only from a paired evaluation
        pairs = (
            []
            if bar is None
            else [record(s, self._units_dir, None, bar.candidate, evaluator_id, b) for b in report.bar_samples]
        )
        for i, sample in enumerate(report.samples):
            pair = pairs[i] if i < len(pairs) else None
            record(s, self._units_dir, job.run, job.candidate, evaluator_id, sample, pair=pair)


def _open(s: Session, key: AgentKey) -> AgentSession:
    """Return the agent's row while it is starting, running or ending. Raises ``Settled`` once it is closed."""
    w = s.get(AgentSession, key.agent_session)
    if w is None or w.state not in LIVE:
        raise Settled(f'agent_session#{key.agent_session} is closed')
    return w


def learn(s: Session, w: AgentSession, session_id: str | None) -> None:
    """Put ``session_id`` on the row. When the row did not have it, emit ``Identified``. The caller commits.

    Each write of an agent's session id goes through here: the launcher's minted id, and the ids that its executor
    reports at the start, in each heartbeat and in the final report.
    """
    if session_id is None or session_id == w.session_id:
        return
    w.session_id = session_id
    assert w.harness is not None  # the launcher picks the harness before it launches the agent
    run = None if w.run_id is None else s.get(Run, w.run_id)
    idea = None if run is None else s.get_one(Idea, run.idea_id)
    definition = harness_of(s, w)
    identified = Identified(
        session_id=session_id,
        harness=w.harness,
        executable=None if definition is None else definition.executable,
        role=w.kind,
        model=w.model,
        campaign=None if w.campaign_id is None else s.get_one(Campaign, w.campaign_id).name,
        run=w.run_id,
        idea=None if idea is None else idea.id,
        proposed_by=None if idea is None else _proposer(s, idea.created_by),
    )
    emit(s, identified, TargetRef(TargetKind.AGENT_SESSION, w.id), System.DAEMON)


def _proposer(s: Session, created_by: str) -> Proposer | None:
    """The agent whose signature ``created_by`` is, with its harness session. None for any other author."""
    try:
        agent = SIGNED.validate_python(created_by)
    except ValidationError:  # created_by is text: a person, the orchestrator, or a name that no agent signs
        return None
    row = s.get(AgentSession, agent.session)
    if row is None:
        return Proposer(agent, None, None, None)
    definition = harness_of(s, row)
    return Proposer(agent, row.harness, None if definition is None else definition.executable, row.session_id)


def session_finished(s: Session, w: AgentSession) -> None:
    """Emit ``AgentSessionFinished`` for the row that just closed. The caller commits."""
    event = AgentSessionFinished(role=w.kind, reason=w.reason, state=w.state, error=w.error)
    emit(s, event, TargetRef(TargetKind.AGENT_SESSION, w.id), System.DAEMON)


def _record(s: Session, agent_session_id: AgentSessionId, outcome: PromptOutcome) -> None:
    """Emit ``outcome`` on the agent, unless the comment has one: any before a queue, a ``FINAL_OUTCOMES`` one else."""
    on_agent = TargetRef(TargetKind.AGENT_SESSION, agent_session_id)
    before = PromptOutcome.kinds() if outcome.kind == 'comment.queued' else FINAL_OUTCOMES
    found = select(Event.id).where(Event.target == on_agent, has(PromptOutcome, comment=outcome.comment))
    if s.scalar(found.where(Event.kind.in_(before)).limit(1)) is None:
        emit(s, outcome, on_agent, System.DAEMON)


def _ended(s: Session, agent_session_id: AgentSessionId, ended: Ended) -> None:
    emit(
        s,
        JobEnded(ended=ended),
        TargetRef(TargetKind.AGENT_SESSION, agent_session_id),
        System.DAEMON,
    )


def ended_of(s: Session, agent_session_id: AgentSessionId) -> Ended | None:
    """Return how the agent's job ended (``JobEnded``), or None when no result closed its row: a stop did."""
    found = latest(s, JobEnded, Event.target == TargetRef(TargetKind.AGENT_SESSION, agent_session_id))
    return None if found is None else found[1].ended


def _asked_to_end(s: Session, agent_session_id: AgentSessionId) -> bool:
    """Whether the daemon told the agent to stop or ended it (``WrapUp``, ``AgentEnded``): no failed start then."""
    on_agent_session = Event.target == TargetRef(TargetKind.AGENT_SESSION, agent_session_id)
    asked = select(Event.id).where(on_agent_session, has(WrapUp) | has(AgentEnded))
    return s.scalar(asked.limit(1)) is not None


def _never_worked(s: Session, w: AgentSession, report: AgentFinished) -> bool:
    """Return whether a started agent ended within ``STARTUP_SECONDS`` and left nothing behind.

    Nothing behind means no change to its checkout (``AgentFinished.changed``: no commit, no uncommitted change outside
    the links) and no comment signed by it.

    A run's work is a change to its worktree or a comment, so that decides for an agent with a run, whatever its
    harness and its tokens: an agent whose tools fail at launch still reads its brief and says that it is blocked.
    A sweep's work can be rows that no comment signs, so a sweep that spent tokens worked, and only a sweep whose
    harness's usage is read is judged. The tokens can be unknown for a sweep that worked too, when its session id
    was not found.
    """
    if report.process.status is ExitStatus.TIMED_OUT or report.process.seconds >= STARTUP_SECONDS or report.changed:
        return False
    if w.run_id is None:
        definition = harness_of(s, w)
        if definition is None or definition.usage is None or (report.usage is not None and report.usage.total > 0):
            return False
    signed = select(Comment.id).where(Comment.author == Agent(role=w.kind, session=w.id).signature).limit(1)
    return s.scalar(signed) is None


def record(
    s: Session,
    units_dir: Path,
    run_id: RunId | None,
    candidate_id: CandidateId,
    evaluator_id: EvaluatorId,
    sample: Sample,
    *,
    pair: MeasurementId | None = None,
) -> MeasurementId:
    """Store one sample as a measurement, with its per-unit results, and return its id. The caller commits.

    ``pair`` is the bar's measurement that ran next to it in a paired evaluation.
    """
    measurement = Measurement(
        candidate_id=candidate_id,
        evaluator_id=evaluator_id,
        metrics=sample.metrics,
        error=sample.error,
        seconds=sample.seconds,
        pair_id=pair,
    )
    s.add(measurement)
    s.flush()
    save_units(units_dir, measurement.id, sample.units)
    recorded = MeasurementRecorded(
        candidate=candidate_id, metrics=sample.metrics, error=sample.error, run=run_id, pair=pair
    )
    emit(s, recorded, TargetRef(TargetKind.MEASUREMENT, measurement.id), System.DAEMON)
    return measurement.id
