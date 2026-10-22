"""The core operations that an executor calls on what its jobs do (``Results``): they change the campaign's rows.

Each checks the domain only: a job's row closes once (``Settled``). Each holds the write lock from its check to the
commit, in a write transaction of its own, so two calls for one job never both pass the check. Fencing a result that comes late, or twice, is
the executor's (``piloop.plugins.executor.contract``). An agent's end is an ``ENDED`` event on its row: what waits for
the agent reads it there (``Launcher.launch``), whichever process applied it.
"""

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from pydantic import TypeAdapter
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop import deprecated
from piloop.actor import Agent
from piloop.config import harness_of
from piloop.db.events import emit
from piloop.db.models import (
    LIVE,
    AgentSession,
    AgentSessionState,
    Comment,
    Evaluation,
    Event,
    Measurement,
    TargetKind,
    utcnow,
)
from piloop.db.session import Database, write_transaction
from piloop.otel import Tracer
from piloop.plugins.executor.contract import (
    AgentJob,
    AgentKey,
    AgentReport,
    EvaluationJob,
    EvaluationKey,
    EvaluationReport,
    Guard,
    JobKey,
    Settled,
)
from piloop.plugins.runtime.contract import AgentExit, AgentStatus
from piloop.scoring.evaluate import EvalResult
from piloop.scoring.ledger import save_units

STOPPED_ERROR = 'daemon stopped'
STARTUP_SECONDS = 120
"""An agent that ends sooner than this and leaves nothing did not start to work: a dialog, a crash, broken tools.

Longer than the default Herdr ``startup_grace_seconds``: a Herdr agent that never starts its first turn is watched for
the whole grace, and must still count as a failed start, not as a run that worked and left nothing."""
STARTUP_ANOMALY = 'anomaly.startup'
"""The event on an implementer that did not start to work (``End.NOT_STARTED``): an environment fault, not bad work."""
WRAP_UP = 'agent_session.wrap_up'
"""Event on an implementer of a stopped campaign, or of a run cancelled with its wrap-up: the implementer was told to commit and stop.

The daemon ends the implementer ``STOP_GRACE_SECONDS`` later."""
STOP_ENDED = 'agent_session.stop_ended'
"""Event on an agent that the daemon ended for a stop or a cancel, with the ``reason`` and the run's ``error``.

An implementer of a stopped campaign is ended when the grace period after its wrap-up prompt is over, or at once for a stop
that kills. An implementer in the middle of a turn takes a message only when the turn ends, so it may never have read the
prompt. The scheduler commits the event before the kill, and the run closes with the ``error``
(``CAMPAIGN_STOPPED_ERROR`` or ``CANCELLED_ERROR``) unless its implementer left its finding."""
ASKED_TO_END = (WRAP_UP, STOP_ENDED)
"""Events on an agent that the daemon told to stop or ended. Such an agent never counts as a failed start."""
ENDED_EVENT = 'agent_session.job_ended'
"""The event on an agent's row that says how its job ended (``Ended``), with the head of its commits."""


class End(StrEnum):
    """How an agent's job ended, for what waits for it (``Ended``)."""

    RAN = 'ran'
    """Its executor reported it, and it started to work. The process says how it ended."""
    NOT_STARTED = 'not_started'
    """It did not start to work: its start failed, or it ended within ``STARTUP_SECONDS`` and left nothing
    (``_never_worked``). A fault of the host or the harness, not an attempt."""
    LOST = 'lost'
    """Nothing came back from it (``Results.lost``): the executor lost it."""
    STOPPED = 'daemon_stopped'
    """The daemon stopped before it started or while it ran. The stop writes the rows."""
    HANDED_OVER = 'handed_over'
    """An in-place restart left it running for the next daemon process, which waits for it again."""


@dataclass(frozen=True)
class Ended:
    """How an agent's job ended (``end``), how its process ended, and the head of its commits."""

    end: End
    process: AgentExit
    head: str | None = None
    """The head of its commits, pinned at the job's ``Commits.ref``, when it was asked for them and made some."""


ENDED: TypeAdapter[Ended] = TypeAdapter(Ended)
"""How an ``ENDED_EVENT`` holds an ``Ended``."""


class CampaignResults:
    """``Results``: what an agent's or an evaluation's end changes in the campaign's rows."""

    def __init__(self, db: Database, tracer: Tracer, units_dir: Path, stopping: Callable[[], bool]) -> None:
        self._db = db
        self._tracer = tracer
        self._units_dir = units_dir
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
        self, key: AgentKey, handle: dict[str, object], session_id: str | None, *, guard: Guard | None = None
    ) -> None:
        """The agent runs: its row is ``running`` with its handle, which the daemon steers it by.

        Steering goes to the runtime by this handle, so it works for an agent on this machine only. It is replaced by
        an ``Executor.prompt`` when an executor that runs agents elsewhere lands.
        """

        def change(s: Session) -> None:
            w = _open(s, key)
            w.handle, w.state = handle, AgentSessionState.RUNNING
            if session_id is not None:
                w.session_id = session_id

        self._apply(guard, change)

    def session(self, key: AgentKey, session_id: str, *, guard: Guard | None = None) -> None:
        def change(s: Session) -> None:
            _open(s, key).session_id = session_id

        self._apply(guard, change)

    def finished(
        self, job: AgentJob | EvaluationJob, report: AgentReport | EvaluationReport, *, guard: Guard | None = None
    ) -> None:
        def change(s: Session) -> None:
            if isinstance(job, AgentJob):
                assert isinstance(report, AgentReport)
                self._agent(s, job, report)
            else:
                assert isinstance(report, EvaluationReport)
                self._measured(s, job, report)

        self._apply(guard, change)

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
            payload: dict[str, object] = {'reason': w.reason, 'status': AgentStatus.EXITED, 'error': reason}
            emit(s, f'{w.kind}.finished', TargetKind.AGENT_SESSION, w.id, payload=payload)
            _ended(s, w.id, Ended(End.LOST, AgentExit(AgentStatus.EXITED, 0, error=reason)))

        self._apply(guard, change)

    def _agent(self, s: Session, job: AgentJob, report: AgentReport) -> None:
        """Close the row from the report, and emit ``<kind>.finished``, ``ENDED_EVENT`` and ``STARTUP_ANOMALY``.

        The anomaly only when it applies. An agent that ended within ``STARTUP_SECONDS`` and left nothing did not start to work (``_never_worked``),
        unless the daemon told it to stop or ended it. While the daemon stops, the row closes as killed by the stop
        (``STOPPED_ERROR``), with the usage that was read.
        """
        w = _open(s, AgentKey(job.agent_session))
        if report.session_id is not None:
            w.session_id = report.session_id
        w.ended_at, w.usage = utcnow(), report.usage
        if self._stopping():
            w.state, w.error = AgentSessionState.KILLED, STOPPED_ERROR
            stopped = AgentExit(report.status, report.seconds, report.exit_code, STOPPED_ERROR)
            _ended(s, w.id, Ended(End.STOPPED, stopped, report.head))
            return
        asked = _asked_to_end(s, w.id)
        killed = report.status is AgentStatus.TIMED_OUT or report.startup_failed or STOP_ENDED in asked
        startup_failed = report.startup_failed or (not asked and _never_worked(s, w, report))
        w.state = AgentSessionState.KILLED if killed else AgentSessionState.EXITED
        w.error, w.checkpoint_id = report.error, report.checkpoint
        emit(
            s,
            f'{w.kind}.finished',
            TargetKind.AGENT_SESSION,
            w.id,
            payload={'reason': w.reason, 'status': report.status, 'error': report.error},
        )
        if startup_failed:
            anomaly: dict[str, object] = {
                'kind': w.kind,
                'harness': w.harness,
                'run': w.run_id,
                'seconds': round(report.seconds, 1),
                'error': report.error,
            }
            emit(s, STARTUP_ANOMALY, TargetKind.AGENT_SESSION, w.id, payload=anomaly)
            self._tracer.event(STARTUP_ANOMALY, agent_session=w.id, harness=w.harness, error=report.error)
        process = AgentExit(report.status, report.seconds, report.exit_code, report.error)
        _ended(s, w.id, Ended(End.NOT_STARTED if startup_failed else End.RAN, process, report.head))

    def _measured(self, s: Session, job: EvaluationJob, report: EvaluationReport) -> None:
        """Record what an evaluation measured as one measurement, and take it out of the queue.

        A cancelled evaluation records nothing: ``queue.cancel`` recorded its crash. Nor does one that ends while the
        daemon stops: its evaluator was killed, and the next daemon measures it again.
        """
        evaluation = s.get(Evaluation, job.evaluation)
        if evaluation is None or evaluation.started_at is None:
            raise Settled(f'evaluation#{job.evaluation} is not running')
        s.delete(evaluation)
        if self._stopping() or evaluation.cancelled_by is not None:
            return
        result = EvalResult(report.metrics, report.seconds, report.error, report.units)
        record(s, self._units_dir, job.run, job.candidate, evaluation.evaluator_id, result)


def _open(s: Session, key: AgentKey) -> AgentSession:
    """Return the agent's row while it is starting or running. Raises ``Settled`` once it is closed."""
    w = s.get(AgentSession, key.agent_session)
    if w is None or w.state not in LIVE:
        raise Settled(f'agent_session#{key.agent_session} is closed')
    return w


def _ended(s: Session, agent_session_id: int, ended: Ended) -> None:
    emit(s, ENDED_EVENT, TargetKind.AGENT_SESSION, agent_session_id, payload=ENDED.dump_python(ended, mode='json'))


def ended_of(s: Session, agent_session_id: int) -> Ended | None:
    """Return how the agent's job ended (``ENDED_EVENT``), or None when no result closed its row: a stop did."""
    payload = s.scalar(
        select(Event.payload)
        .where(
            Event.target_kind == TargetKind.AGENT_SESSION,
            Event.target_id == agent_session_id,
            Event.kind == ENDED_EVENT,
        )
        .order_by(Event.id.desc())
        .limit(1)
    )
    return None if payload is None else ENDED.validate_python(payload)


def _asked_to_end(s: Session, agent_session_id: int) -> set[str]:
    """Return the ``ASKED_TO_END`` events on the agent: a stop or a cancel ended it, not a failed start."""
    on_agent_session = (Event.target_kind == TargetKind.AGENT_SESSION) & (Event.target_id == agent_session_id)
    return set(s.scalars(select(Event.kind).where(on_agent_session, Event.kind.in_(ASKED_TO_END))))


def _never_worked(s: Session, w: AgentSession, report: AgentReport) -> bool:
    """Return whether a started agent ended within ``STARTUP_SECONDS`` and left nothing behind.

    Nothing behind means no change to its checkout (``AgentReport.changed``: no commit, no uncommitted change outside
    the links) and no comment signed by it. Herdr reports some first-launch dialogs as idle, so such an agent looks
    finished.

    A run's work is a change to its worktree or a comment, so that decides for an agent with a run, whatever its
    harness and its tokens: an agent whose tools fail at launch still reads its brief and says that it is blocked.
    A sweep's work can be rows that no comment signs, so a sweep that spent tokens worked, and only a sweep whose
    harness's usage is read is judged. The tokens can be unknown for a sweep that worked too: Herdr reports no
    session for a harness without Herdr hooks.
    """
    if report.status is AgentStatus.TIMED_OUT or report.seconds >= STARTUP_SECONDS or report.changed:
        return False
    if w.run_id is None:
        definition = harness_of(s, w)
        if definition is None or definition.usage is None or (report.usage is not None and report.usage['total'] > 0):
            return False
    signed = (
        select(Comment.id).where(Comment.author.in_(deprecated.signatures(Agent(role=w.kind, session=w.id)))).limit(1)
    )
    return s.scalar(signed) is None


def record(
    s: Session, units_dir: Path, run_id: int | None, candidate_id: int, evaluator_id: int, result: EvalResult
) -> int:
    """Store one evaluation as a measurement, with its per-unit results, and return its id. The caller commits."""
    measurement = Measurement(
        candidate_id=candidate_id,
        evaluator_id=evaluator_id,
        metrics=result.metrics,
        error=result.error,
        seconds=result.seconds,
    )
    s.add(measurement)
    s.flush()
    save_units(units_dir, measurement.id, result.units)
    payload: dict[str, object] = {
        'candidate': candidate_id,
        'metrics': result.metrics,
        'error': result.error,
        'run': run_id,
    }
    emit(s, 'measurement.recorded', TargetKind.MEASUREMENT, measurement.id, payload=payload)
    return measurement.id
