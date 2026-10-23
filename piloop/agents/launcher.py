"""One lifecycle for every agent role (https://piloop.autoresearch.inc/concepts/roles): brief, start, wait, collect, close the ``agent_sessions`` row.

Roles differ in data (``piloop.agents.roles``) and in the content of their brief. ``Launcher.launch`` is the only code
that starts, waits for and closes an agent process. It never raises: a failure goes on the agent session row and its span.
"""

import threading
from pathlib import Path
from uuid import uuid4

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
    ended_of,
    learn,
    session_finished,
)
from piloop.config import BRIEF_FILE, config_of, harness_of
from piloop.db.events import emit
from piloop.db.models import LIVE, AgentSession, AgentSessionState, Campaign, utcnow
from piloop.db.session import Database
from piloop.harnesses import SESSION_ENV, SESSION_ID, fill, takes_session_id
from piloop.ids import AgentSessionId, TargetKind, TargetRef
from piloop.otel import Span, Tracer
from piloop.plugins.executor.contract import AgentJob, AgentKey, Artifact, Commits, Executor, ExecutorConfig, LeftJob
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
        """Submit no more jobs."""
        with self.lock:
            self.cancelled.set()

    def launch(
        self,
        agent_session_id: AgentSessionId,
        *,
        brief: str,
        place: str,
        artifact: Artifact,
        commits: Commits | None = None,
    ) -> Ended:
        """Run agent session row ``agent_session_id`` to the end on the executor, and return how it ended.

        The steps are: pick the harness and model, fill in the row, submit the agent's job, and wait for its result,
        which closes the row (``piloop.agents.results``). The executor makes the job's ``place`` (its log directory),
        materializes ``artifact`` in it (a worktree), starts the agent, and removes the worktree after. Its log files
        come back into ``place`` under this home. With ``commits``, they come back too: ``Ended.commits``.
        """
        with self.tracer.span('piloop.agent', agent_session=agent_session_id) as span:
            with self.lock:
                try:
                    if self.cancelled.is_set():
                        return DaemonStopped()
                    job = self._prepare(agent_session_id, span, brief, place, artifact, commits)
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
        brief: str,
        place: str,
        artifact: Artifact,
        commits: Commits | None,
    ) -> AgentJob:
        """Fill in the row, mark it ``starting``, emit ``<kind>.started``, and return the agent's job.

        The harness and the model are picked here, and the job describes the agent's start (``AgentJob``).
        """
        with self.db.session() as s:
            w = s.get_one(AgentSession, agent_session_id)
            campaign = s.get_one(Campaign, w.campaign_id)
            cfg = config_of(campaign)
            harness = w.harness or rotate(s, w, cfg.role_harnesses(w.kind))
            model = w.model or cfg.role_models(w.kind)[harness]
            effort = cfg.role(w.kind).effort
            definition = cfg.harness_for(harness)
            runtime_kind = cfg.role_runtime(w.kind)
            session_id = str(uuid4()) if definition.captures_session and takes_session_id(definition.headless) else None
            kind, reason = w.kind, w.reason
            span.set(kind=kind, harness=harness, model=model, reason=reason, session_id=session_id)
            prompt = PROMPT.format(path=f'{{log_dir}}/{BRIEF_FILE}')
            # the worker adds its PATH and its piloop (``jobs.Site.env``), and the variables that the MCP servers name
            env = {
                'PILOOP_ACTOR': Agent(role=kind, session=agent_session_id).signature,
                **dict.fromkeys(SESSION_ENV | {*definition.session_env}, ''),
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
            job = AgentJob(
                agent_session=agent_session_id,
                runtime=runtime_kind,
                artifact=artifact,
                place=place,
                timeout=cfg.role(kind).timeout,
                brief=brief,
                spec=spec,
                mcp=definition.mcp,
                servers=cfg.role_servers(kind),
                usage=definition.usage,
                entire_agent=definition.entire_agent,
                captures_session=definition.captures_session,
                session_id=session_id,
                commits=commits,
            )
            w.harness, w.model, w.runtime, w.log_dir = harness, model, runtime_kind, str(self.home / place)
            w.state, w.started_at = AgentSessionState.STARTING, utcnow()
            started = AgentSessionStarted(role=kind, harness=harness, model=model, reason=reason)
            emit(s, started, TargetRef(TargetKind.AGENT_SESSION, agent_session_id), System.DAEMON)
            learn(s, w, session_id)
            s.commit()
            return job

    @deprecated.until(
        '0.3.0',
        'an agent that a daemon before leases started: delete Launcher.left, Scheduler._left, LeftJob.job and'
        ' LeftJob.handle, and a None AgentJob.spec',
    )
    def left(self, s: Session, w: AgentSession, artifact: Artifact, place: str, commits: Commits | None) -> LeftJob:
        """The ``LeftJob`` of a running agent, with its job as its row has it: no spec, so it is re-attached, never started.

        Only an agent that a daemon before leases started needs it: an executor finds its own jobs again.
        """
        cfg = config_of(s.get_one(Campaign, w.campaign_id))
        definition = harness_of(s, w)
        assert w.runtime is not None and w.log_dir is not None  # a running row was launched
        job = AgentJob(
            agent_session=w.id,
            runtime=w.runtime,
            artifact=artifact,
            place=place,
            timeout=cfg.role(w.kind).timeout,
            brief=None,
            spec=None,
            usage=definition.usage if definition is not None else None,
            entire_agent=definition.entire_agent if definition is not None else None,
            captures_session=definition is not None and definition.captures_session,
            session_id=w.session_id,
            commits=commits,
        )
        return LeftJob(AgentKey(w.id), job, dict(w.handle) or None, w.started_at)


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
