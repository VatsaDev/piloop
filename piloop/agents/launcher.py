"""One lifecycle for every agent role (https://piloop.autoresearch.inc/concepts/roles): brief, start, wait, collect, close the ``agent_sessions`` row.

Roles differ in data (``piloop.agents.roles``) and in the content of their brief. ``Launcher.launch`` is the only code
that starts, waits for and closes an agent process. It never raises: a failure goes on the agent session row and its span.
"""

import threading
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from covalence import Cid
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from piloop import deprecated
from piloop.actor import Agent, System
from piloop.agents.results import (
    AgentSessionStarted,
    DaemonStopped,
    Ended,
    HandedOver,
    NotStarted,
    PromptOutcome,
    SessionNotResumed,
    Unresumed,
    ended_of,
    learn,
    session_finished,
)
from piloop.agents.roles import harness_of
from piloop.config import BRIEF_FILE, CampaignConfig
from piloop.db.events import emit, latest
from piloop.db.models import LIVE, AgentSession, AgentSessionState, Campaign, Event, utcnow
from piloop.db.session import Database
from piloop.ids import AgentSessionId, CommentId, RunId, TargetKind, TargetRef
from piloop.otel import Span, Tracer
from piloop.plugins.executor.contract import AgentJob, AgentKey, Executor, ExecutorConfig, LeftJob, Resume, Source
from piloop.plugins.harness.contract import SESSION_ID, cleared_env, fill, of_provider, takes_session_id
from piloop.plugins.role.contract import Continuity, RoleConfig, SweepRole
from piloop.plugins.runtime.contract import AgentStart

PROMPT = 'Your brief is in {path}. Read all of it, in parts if a read comes back cut short, then follow it.'
"""An agent's first prompt, on its argv.

It names the brief's file instead of holding the brief. The prompt is one argument, and Linux limits one argument to
128 KiB (``MAX_ARG_STRLEN``). A research brief with a long history is longer than that.
"""


class Launcher:
    """Turns an ``agent_sessions`` row into a job of the executor, and waits for the result that closes the row."""

    def __init__(self, db: Database, tracer: Tracer, home: Path, executor: Executor[ExecutorConfig]) -> None:
        self.db = db
        self.tracer = tracer
        self.home = home
        self.executor = executor
        self.cancelled = threading.Event()
        """Set when the daemon stops. No job is submitted after it, and the results that come in then close their rows
        as stopped (``RunEnd.DAEMON_STOPPED``)."""
        self.lock = threading.Lock()
        """Held from the ``cancelled`` check to the job's submit, and by ``cancel``.

        So after ``cancel`` returns, every agent either is the executor's (and its stop ends it) or never gets to it.
        ``RunController`` shares the lock for the run row and the evaluation queue.
        """
        self.unsubmitted: set[AgentSessionId] = set()
        """The agent sessions that the scheduler took for a free slot and that ``launch`` has not submitted yet. The
        executor's ``capacity`` does not count them, so the scheduler does."""
        self.handover = threading.Event()
        """Set before ``cancelled`` by an in-place restart (``RunController.hand_over``). The agents keep running, and the
        next daemon process waits for them again (``reattach``)."""

    def cancel(self) -> None:
        """Submit no more jobs, and wake the waits that give up on it."""
        with self.lock:
            self.cancelled.set()
        self.db.notify()

    def launch(
        self,
        agent_session_id: AgentSessionId,
        *,
        brief: Cid,
        place: str,
        artifact: Source,
        timeout: float,
        capture: RunId | None = None,
        resumed_brief: Cid | None = None,
        read: Sequence[CommentId] = (),
    ) -> Ended:
        """Run agent session row ``agent_session_id`` to the end on the executor, and return how it ended.

        The steps are: pick the harness and model, fill in the row, submit the agent's job, and wait for its result,
        which closes the row (``piloop.agents.results``). The executor makes the job's ``place`` (its log directory),
        materializes ``artifact`` in it (a worktree), starts the agent, and removes the worktree after. Its log files
        come back into ``place`` under this home. With ``capture``, its work comes back too: ``Ran.work``. ``timeout``
        is the seconds that the agent may run. With ``resumed_brief``, the brief of a resumed session (only what is
        new), the agent continues the harness session of its role's last agent (``resumable``), else it starts anew.
        ``read`` are the notes on the row that the brief carries: they are delivered (``comment.delivered``) as the row
        starts, so they are not sent again.
        """
        with self.tracer.span('piloop.agent', agent_session=agent_session_id) as span:
            with self.lock:
                try:
                    if self.cancelled.is_set():
                        return DaemonStopped()
                    job = self._prepare(
                        agent_session_id, span, brief, place, artifact, timeout, capture, resumed_brief, read
                    )
                    self.executor.submit(job)
                except Exception as e:  # a harness or a runtime that piloop.toml no longer has, an executor's refusal
                    span.error(e)
                    error = f'{type(e).__name__}: {e}'
                    self._fail(agent_session_id, error)
                    return NotStarted(error)
                finally:  # after the submit: the executor counts the job from there
                    self.unsubmitted.discard(agent_session_id)
            return self._await(agent_session_id, span)

    def reattach(self, agent_session_id: AgentSessionId) -> Ended:
        """Like ``launch``, for an agent that an earlier daemon process started and the executor adopted.

        The agent can still run, or it can have ended while no daemon watched.
        """
        with self.tracer.span('piloop.agent', agent_session=agent_session_id, reattached=True) as span:
            return self._await(agent_session_id, span)

    def _await(self, agent_session_id: AgentSessionId, span: Span) -> Ended:
        """Wait for the row to close, and return how its job ended (``ended_of``). A close without one is a stop's."""

        def closed(s: Session) -> bool:
            return s.get_one(AgentSession, agent_session_id).state not in LIVE

        if not self.db.wait_until(closed, give_up=self.handover.is_set):
            span.set(state='handed_over')
            return HandedOver()
        with self.db.session() as s:
            span.set(state=s.get_one(AgentSession, agent_session_id).state)
            ended = ended_of(s, agent_session_id)
        return ended or DaemonStopped()

    def _fail(self, agent_session_id: AgentSessionId, error: str) -> None:
        """Close a row whose job could not be described or submitted: no executor has it."""
        with self.db.session() as s:
            w = s.get_one(AgentSession, agent_session_id)
            w.state, w.ended_at, w.error = AgentSessionState.KILLED, utcnow(), error
            session_finished(s, w)
            s.commit()

    def _prepare(
        self,
        agent_session_id: AgentSessionId,
        span: Span,
        brief: Cid,
        place: str,
        artifact: Source,
        timeout: float,
        capture: RunId | None,
        resumed_brief: Cid | None,
        read: Sequence[CommentId],
    ) -> AgentJob:
        """Fill in the row, mark it ``starting``, emit ``<kind>.started``, and return the agent's job.

        The harness and the model are picked here, and the job describes the agent's start (``AgentJob``). A session
        that it resumes picks the harness: that session's.
        """
        with self.db.session() as s:
            w = s.get_one(AgentSession, agent_session_id)
            campaign = s.get_one(Campaign, w.campaign_id)
            cfg = campaign.config
            source = None if resumed_brief is None else resumable(s, w, cfg, Path(place))
            if isinstance(source, SessionNotResumed):
                emit(s, source, TargetRef(TargetKind.AGENT_SESSION, w.id), System.DAEMON)
                span.set(unresumed=source.reason)
                source = None
            harness = source.harness if source is not None else w.harness or rotate(s, w, cfg.role_harnesses(w.kind))
            model = w.model or cfg.role_models(w.kind)[harness]
            effort = cfg.role(w.kind).effort
            definition = cfg.harness_for(harness)
            runtime_kind = cfg.role_runtime(w.kind)
            session_id = str(uuid4()) if definition.needs_session_id and takes_session_id(definition.headless) else None
            kind, reason = w.kind, w.reason
            span.set(kind=kind, harness=harness, model=model, reason=reason, session_id=session_id)
            prompt = PROMPT.format(path=f'{{log_dir}}/{BRIEF_FILE}')
            # the worker adds its PATH and its piloop (``jobs.Site.env``), and the values of the secrets
            agent = Agent(role=kind, session=agent_session_id)
            env = {
                **definition.literals,
                **artifact.vcs.agent_env(agent),
                'PILOOP_ACTOR': agent.signature,
                **dict.fromkeys(cleared_env() | {*definition.session_env}, ''),
            }
            # {cwd} and {log_dir} stay for the worker, which makes them (``AgentStart.at``)
            argv = [
                fill(a, model=model, effort=effort, prompt=prompt, session_id=session_id or '')
                for a in definition.headless
            ]
            # {prompt} stays for the queued messages, and {session_id} for the id that a harness reports while it runs
            resume = [
                fill(a, model=model, effort=effort, session_id=session_id or SESSION_ID) for a in definition.resume
            ]
            spec = AgentStart(
                executable=definition.executable,
                args=argv,
                env=env,
                session_id_pattern=definition.session_id_pattern,
                resume_args=resume,
            )
            resumes = None
            if source is not None and resumed_brief is not None:
                resumes = Resume(
                    session_id=source.session_id,
                    file=source.file,
                    brief=resumed_brief,
                    args=[
                        fill(a, model=model, effort=effort, prompt=prompt, session_id=source.session_id)
                        for a in definition.resume
                    ],
                )
            job = AgentJob(
                agent_session=agent_session_id,
                runtime=runtime_kind,
                artifact=artifact,
                place=place,
                timeout=timeout,
                brief_cid=brief,
                spec=spec,
                harness=of_provider(definition),
                servers=cfg.role_servers(kind),
                secrets=cfg.role_secrets(kind, harness),
                session_id=session_id,
                capture=capture,
                resume=resumes,
            )
            w.harness, w.model, w.runtime, w.log_dir = harness, model, runtime_kind, str(self.home / place)
            w.state, w.started_at = AgentSessionState.STARTING, utcnow()
            started = AgentSessionStarted(
                role=kind,
                harness=harness,
                model=model,
                reason=reason,
                resumes=None if source is None else source.agent_session,
            )
            on_it = TargetRef(TargetKind.AGENT_SESSION, agent_session_id)
            emit(s, started, on_it, System.DAEMON)
            for note in read:
                emit(s, PromptOutcome(kind='comment.delivered', comment=note), on_it, System.DAEMON)
            learn(s, w, resumes.session_id if resumes is not None else session_id)
            s.commit()
            return job

    @deprecated.until(
        '0.4.0',
        'an agent that a 0.2 daemon started (before leases): delete Launcher.left, Scheduler._left, LeftJob.job and'
        ' LeftJob.handle, and a None AgentJob.spec',
    )
    def left(self, s: Session, w: AgentSession, artifact: Source, place: str, capture: RunId | None) -> LeftJob:
        """The ``LeftJob`` of a running agent, with its job as its row has it: no spec, so it is re-attached, never started.

        Only an agent that a daemon before leases started needs it: an executor finds its own jobs again.
        """
        cfg = s.get_one(Campaign, w.campaign_id).config
        definition = harness_of(s, w)
        assert w.runtime is not None and w.log_dir is not None  # a running row was launched
        job = AgentJob(
            agent_session=w.id,
            runtime=w.runtime,
            artifact=artifact,
            place=place,
            timeout=cfg.role(w.kind).timeout,
            brief_cid=None,
            spec=None,
            harness=None if definition is None else of_provider(definition),
            session_id=w.session_id,
            capture=capture,
        )
        return LeftJob(AgentKey(w.id), job, dict(w.handle) or None, w.started_at)


@dataclass(frozen=True)
class Resumable:
    """An earlier agent's harness session that an agent continues."""

    agent_session: AgentSessionId
    harness: str
    session_id: str
    file: Cid


def resumable(s: Session, w: AgentSession, cfg: CampaignConfig, place: Path) -> Resumable | SessionNotResumed:
    """The earlier agent of ``w``'s role whose harness session ``w`` continues, or why ``w`` starts anew.

    It is the latest one with a session in the CAS, on ``w``'s harness when the request named one. The role's last
    agent resumed a session and did not end well: that session may be what broke it, so ``w`` starts anew.
    """
    earlier = (
        select(AgentSession)
        .where(AgentSession.campaign_id == w.campaign_id, AgentSession.kind == w.kind, AgentSession.id < w.id)
        .order_by(AgentSession.id.desc())
    )
    if (last := s.scalars(earlier.limit(1)).first()) is not None and (failed := _failed_resume(s, last)) is not None:
        return failed
    saved = earlier.where(AgentSession.session_file.is_not(None), AgentSession.session_id.is_not(None))
    if w.harness is not None:
        saved = saved.where(AgentSession.harness == w.harness)
    match s.scalars(saved.limit(1)).first():
        case AgentSession(id=row, harness=str(harness), session_id=str(session_id), session_file=Cid() as file):
            source = Resumable(row, harness, session_id, file)
        case _:
            on = f' on {w.harness}' if w.harness is not None else ''
            return SessionNotResumed(reason=Unresumed.NO_SESSION, detail=f'no earlier {w.kind} left a session{on}')
    try:
        definition = cfg.harness_for(source.harness)
    except KeyError:
        return SessionNotResumed(reason=Unresumed.HARNESS, detail=f'piloop.toml no longer names {source.harness}')
    if not definition.resume or definition.restore(source.session_id, place) is None:
        return SessionNotResumed(
            reason=Unresumed.HARNESS,
            detail=f'{source.harness} has no resume argv, or its provider restores no session file',
        )
    return source


def _failed_resume(s: Session, w: AgentSession) -> SessionNotResumed | None:
    """Why the agent after ``w`` starts anew: ``w`` resumed a session, its file was written back, and ``w`` failed."""
    on = Event.target == TargetRef(TargetKind.AGENT_SESSION, w.id)
    started = latest(s, AgentSessionStarted, on)
    if started is None or started[1].resumes is None or latest(s, SessionNotResumed, on) is not None:
        return None
    match w.state, w.error:
        case (AgentSessionState.KILLED, _) | (AgentSessionState.EXITED, str()):
            detail = f'agent_session#{w.id} resumed a session and ended {w.state}'
            return SessionNotResumed(reason=Unresumed.FAILED, detail=detail)
        case _:
            return None


def turn(s: Session, w: AgentSession, table: RoleConfig, harnesses: list[str]) -> str:
    """Return the harness whose turn it is for the sweep ``w`` of a role whose table is ``table``.

    A role that resumes its sessions keeps the harness of its last session while ``harnesses`` has it, so that the
    next agent can resume it. Any other takes the next one (``rotate``).
    """
    match table:
        case SweepRole(continuity=Continuity.RESUME):
            kept = s.scalar(
                select(AgentSession.harness)
                .where(
                    AgentSession.campaign_id == w.campaign_id,
                    AgentSession.kind == w.kind,
                    AgentSession.id < w.id,
                    AgentSession.session_file.is_not(None),
                    AgentSession.harness.in_(harnesses),
                )
                .order_by(AgentSession.id.desc())
                .limit(1)
            )
            return kept if kept is not None else rotate(s, w, harnesses)
        case _:
            return rotate(s, w, harnesses)


def rotate(s: Session, agent_session: AgentSession, harnesses: list[str]) -> str:
    """Return the harness for an implementer: the n-th agent of a kind in a campaign takes the n-th harness, round robin."""
    earlier = s.scalar(
        select(func.count())
        .select_from(AgentSession)
        .where(
            AgentSession.campaign_id == agent_session.campaign_id,
            AgentSession.kind == agent_session.kind,
            AgentSession.id < agent_session.id,
        )
    )
    return harnesses[(earlier or 0) % len(harnesses)]
