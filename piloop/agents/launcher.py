"""One lifecycle for every agent role (https://piloop.autoresearch.inc/concepts/roles): brief, start, wait, collect, close the ``agent_sessions`` row.

Roles differ in data (``piloop.agents.roles``) and in the content of their brief. ``Launcher.launch`` is the only code
that starts, waits for and closes an agent process. It never raises: a failure goes on the agent session row and its span.
"""

import os
import shlex
import threading
from pathlib import Path
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from piloop import deprecated
from piloop.actor import Agent
from piloop.agents.results import STOPPED_ERROR, End, Ended, ended_of
from piloop.config import BRIEF_FILE, config_of, harness_of
from piloop.db.events import emit
from piloop.db.models import LIVE, AgentSession, AgentSessionState, Campaign, Run, TargetKind, utcnow
from piloop.db.session import Database
from piloop.harnesses import SESSION_ENV, SESSION_ID, Invocation, fill, takes_session_id
from piloop.otel import Span, Tracer
from piloop.plugins.contracts.executor import AgentJob, AgentKey, Artifact, Commits, Executor, LeftJob
from piloop.plugins.contracts.runtime import RUNTIMES
from piloop.proc import PILOOP_ARGV, child_env
from piloop.runtime import AgentExit, AgentSpec, AgentStatus
from piloop.text import cell

AGENT_REFS = 'refs/piloop/agents/'
"""Where an agent's head is pinned before its worktree goes, ``refs/piloop/agents/<agent session id>``
(``Commits.ref``). Its run pins the commits under refs of its own, then deletes this one. A refused report's ref stays:
a person can import a commit from it."""
PROMPT = 'Your brief is in {path}. Read all of it, in parts if a read comes back cut short, then follow it.'
"""An agent's first prompt, on its argv or Herdr's (``agent prompt``).

It names the brief's file instead of holding the brief. The prompt is one argument, and Linux limits one argument to
128 KiB (``MAX_ARG_STRLEN``). A research brief with a long history is longer than that.
"""
TAB_TITLE_CHARS = 30
"""How many characters of the idea's title go in an implementer's tab label: enough to tell tabs apart in Herdr."""

CLI_SHIM = '#!/bin/sh\nPILOOP_HOME={home} exec {argv} "$@"\n'
"""``<home>/bin/piloop``: this home's piloop, the daemon's own code on the daemon's home. Agents get it as ``PILOOP_BIN``,
which the briefs use, and its directory first on their PATH, for the prompts' plain ``piloop``. Another install can
come first on the user's PATH (a release next to a build of ``main``), and a harness's shell can run the user's rc
files again, which can put it back in front."""


def own_cli(home: Path) -> Path:
    """Write the ``CLI_SHIM`` for this process's piloop under ``home``, and return its path."""
    shim = home / 'bin' / 'piloop'
    text = CLI_SHIM.format(home=shlex.quote(str(home)), argv=shlex.join(PILOOP_ARGV))
    if not shim.is_file() or shim.read_text() != text:
        shim.parent.mkdir(parents=True, exist_ok=True)
        shim.write_text(text)
        shim.chmod(0o755)
    return shim


class Launcher:
    """Turns an ``agent_sessions`` row into a job of the executor, and waits for the result that closes the row."""

    def __init__(self, db: Database, tracer: Tracer, home: Path, executor: Executor) -> None:
        self.db = db
        self.tracer = tracer
        self.home = home
        self.cli = own_cli(home)
        self.executor = executor
        self.cancelled = threading.Event()
        """Set when the daemon stops. No job is submitted after it, and the results that come in then close their rows
        as stopped (``STOPPED_ERROR``)."""
        self.lock = threading.Lock()
        """Held from the ``cancelled`` check to the job's submit, and by ``cancel``.

        So after ``cancel`` returns, every agent either is the executor's (and its stop ends it) or never gets to it.
        ``RunController`` shares the lock for the run row and the evaluation queue.
        """
        self.handover = threading.Event()
        """Set before ``cancelled`` by an in-place restart (``RunController.hand_over``). The agents keep running, and the
        next daemon process waits for them again (``reattach``)."""

    def cancel(self) -> None:
        """Submit no more jobs."""
        with self.lock:
            self.cancelled.set()

    def launch(
        self,
        agent_session_id: int,
        *,
        brief: str,
        cwd: Path,
        log_dir: Path,
        artifact: Artifact,
        commits: Commits | None = None,
    ) -> Ended:
        """Run agent session row ``agent_session_id`` to the end on the executor, and return how it ended.

        The steps are: pick the harness and model, fill in the row, submit the agent's job, and wait for its result,
        which closes the row (``piloop.agents.results``). The executor materializes ``artifact`` at ``cwd`` (a
        worktree), starts the agent, and removes the worktree after. With ``commits``, they come back: ``Ended.head``.
        """
        with self.tracer.span('piloop.agent', agent_session=agent_session_id) as span:
            with self.lock:
                if self.cancelled.is_set():
                    return Ended(End.STOPPED, AgentExit(AgentStatus.EXITED, 0, error=STOPPED_ERROR))
                try:
                    job = self._prepare(agent_session_id, span, brief, cwd, log_dir, artifact, commits)
                    self.executor.submit(job)
                except Exception as e:  # a harness or a runtime that piloop.toml no longer has, an executor's refusal
                    span.error(e)
                    error = f'{type(e).__name__}: {e}'
                    self._fail(agent_session_id, error)
                    return Ended(End.NOT_STARTED, AgentExit(AgentStatus.EXITED, 0, error=error))
            return self._await(agent_session_id, span)

    def reattach(self, agent_session_id: int) -> Ended:
        """Like ``launch``, for an agent that an earlier daemon process started and the executor adopted.

        The agent can still run, or it can have ended while no daemon watched.
        """
        with self.tracer.span('piloop.agent', agent_session=agent_session_id, reattached=True) as span:
            return self._await(agent_session_id, span)

    def _await(self, agent_session_id: int, span: Span) -> Ended:
        """Wait for the row to close, and return how its job ended (``ended_of``). A close without one is a stop's."""

        def closed(s: Session) -> bool:
            return s.get_one(AgentSession, agent_session_id).state not in LIVE

        if not self.db.wait_until(closed, give_up=self.handover.is_set):
            span.set(state='handed_over')
            return Ended(End.HANDED_OVER, AgentExit(AgentStatus.EXITED, 0))
        with self.db.session() as s:
            span.set(state=s.get_one(AgentSession, agent_session_id).state)
            ended = ended_of(s, agent_session_id)
        return ended or Ended(End.STOPPED, AgentExit(AgentStatus.EXITED, 0, error=STOPPED_ERROR))

    def _fail(self, agent_session_id: int, error: str) -> None:
        """Close a row whose job could not be described or submitted: no executor has it."""
        with self.db.session() as s:
            w = s.get_one(AgentSession, agent_session_id)
            w.state, w.ended_at, w.error = AgentSessionState.KILLED, utcnow(), error
            payload: dict[str, object] = {'reason': w.reason, 'status': AgentStatus.EXITED, 'error': error}
            emit(s, f'{w.kind}.finished', TargetKind.AGENT_SESSION, agent_session_id, payload=payload)
            s.commit()

    def _prepare(
        self,
        agent_session_id: int,
        span: Span,
        brief: str,
        cwd: Path,
        log_dir: Path,
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
            definition = cfg.harness_for(harness)
            runtime_kind = cfg.role_runtime(w.kind)
            invocation = RUNTIMES.get(runtime_kind).invocation
            template = definition.argv(invocation)
            session_id = str(uuid4()) if definition.captures_session and takes_session_id(template) else None
            kind, reason = w.kind, w.reason
            span.set(kind=kind, harness=harness, model=model, reason=reason, session_id=session_id)
            prompt = PROMPT.format(path=log_dir / BRIEF_FILE)
            # A Herdr pane does not inherit the daemon's environment. Pass the daemon's PATH, where the harness CLIs and
            # uv were found, after the daemon's own piloop. The worker adds the variables that the MCP servers name.
            env = {
                # the shim sets it too, but a harness shell that runs the user's rc files can put another install's
                # piloop first on PATH: with the variable, that one still uses this home's database, never the default
                'PILOOP_HOME': str(self.home),
                'PILOOP_ACTOR': Agent(role=kind, session=agent_session_id).signature,
                'PILOOP_BIN': str(self.cli),
                'PATH': f'{self.cli.parent}{os.pathsep}{child_env()["PATH"]}',
                # empty, not unset: a Herdr pane can only override what its server inherited
                **dict.fromkeys(SESSION_ENV | {*definition.session_env}, ''),
            }
            argv = [
                fill(a, model=model, prompt=prompt, log_dir=str(log_dir), session_id=session_id or '', cwd=str(cwd))
                for a in template
            ]
            # {prompt} stays for the queued messages, and {session_id} for the id that a harness reports while it runs
            resume = [
                fill(a, model=model, log_dir=str(log_dir), session_id=session_id or SESSION_ID, cwd=str(cwd))
                for a in (definition.resume if invocation is Invocation.HEADLESS else [])
            ]
            busy, failed, queue, composer = definition.screens
            spec = AgentSpec(
                name=f'{kind}-{agent_session_id}',
                label=_label(s, w),
                executable=definition.executable,
                args=argv,
                prompt=prompt,
                cwd=cwd,
                log_dir=log_dir,
                env=env,
                startup_dialogs=definition.startup_dialogs,
                busy_screens=busy,
                failed_turn_screens=failed,
                queue_key=queue,
                composer=composer,
                session_id_pattern=definition.session_id_pattern,
                resume_args=resume,
            )
            job = AgentJob(
                agent_session=agent_session_id,
                runtime=runtime_kind,
                campaign=campaign.name,
                campaign_path=Path(campaign.path),
                artifact=artifact,
                cwd=cwd,
                log_dir=log_dir,
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
            w.harness, w.model, w.runtime, w.log_dir = harness, model, runtime_kind, str(log_dir)
            w.state, w.started_at, w.session_id = AgentSessionState.STARTING, utcnow(), session_id
            emit(
                s,
                f'{kind}.started',
                TargetKind.AGENT_SESSION,
                agent_session_id,
                payload={'harness': harness, 'model': model, 'reason': reason},
            )
            s.commit()
            return job

    @deprecated.until(
        '0.3.0',
        'an agent that a daemon before leases started: delete Launcher.left, Scheduler._left, LeftJob.job and'
        ' LeftJob.handle, and a None AgentJob.spec',
    )
    def left(self, s: Session, w: AgentSession, artifact: Artifact, cwd: Path, commits: Commits | None) -> LeftJob:
        """The ``LeftJob`` of a running agent, with its job as its row has it: no spec, so it is re-attached, never started.

        Only an agent that a daemon before leases started needs it: an executor finds its own jobs again.
        """
        campaign = s.get_one(Campaign, w.campaign_id)
        cfg = config_of(campaign)
        definition = harness_of(s, w)
        assert w.runtime is not None and w.log_dir is not None  # a running row was launched
        job = AgentJob(
            agent_session=w.id,
            runtime=w.runtime,
            campaign=campaign.name,
            campaign_path=Path(campaign.path),
            artifact=artifact,
            cwd=cwd,
            log_dir=Path(w.log_dir),
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


def _label(s: Session, w: AgentSession) -> str:
    """Return the tab label: ``implementer-12 idea#5 Stacking with a meta-learner…``, or ``research-7`` for a sweep."""
    if w.run_id is None:
        return f'{w.kind}-{w.id}'
    idea = s.get_one(Run, w.run_id).idea
    return f'{w.kind}-{w.id} idea#{idea.id} {cell(idea.title, TAB_TITLE_CHARS)}'
