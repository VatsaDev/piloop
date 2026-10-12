"""One lifecycle for every agent role (https://piloop.autoresearch.inc/concepts/roles): brief, start, wait, collect, close the ``workers`` row.

Roles differ in data (``piloop.agents.roles``) and in the content of their brief. ``Launcher.launch`` is the only code
that starts, waits for and closes an agent process. It never raises: a failure goes on the worker row and its span.
"""

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from piloop import git
from piloop.agents.capture import ENTIRE_INSTALL, MIN_ENTIRE, AttachError, attach_session, attachable, usable
from piloop.agents.refill import usage_of
from piloop.agents.roles import role_of
from piloop.config import BRIEF_FILE, RuntimeKind, config_of, harness_of
from piloop.db.events import emit
from piloop.db.models import Comment, Project, Run, TargetKind, UsageJson, Worker, WorkerState, utcnow
from piloop.db.session import Database
from piloop.harnesses import fill, takes_session_id
from piloop.mcp import RENDERERS, Launch, Rendered, expand, forwarded_env
from piloop.otel import Span, Tracer
from piloop.proc import child_env
from piloop.runtime import Handle, Outcome, Runtime, StartFailed, WorkerSpec, WorkerStatus
from piloop.runtime.factory import RuntimeFactory, handle_for, make_runtime, runtime_for
from piloop.text import cell
from piloop.usage import UsageError

TRANSCRIPT = 'transcript.txt'
TRANSCRIPT_LINES = 2000
START_SCREEN_LINES = 12
"""How many non-blank lines from the end of a failed start's screen go into the row's error."""
STOPPED_ERROR = 'daemon stopped'
STARTUP_SECONDS = 60
"""An agent that ends sooner than this without spending a token did not start to work: a dialog, a crash."""
STARTUP_ANOMALY = 'anomaly.startup'
"""The event on a worker that did not start to work (``Outcome.startup_failed``): an environment fault, not bad work."""
CLEARED_ENV = ('CLAUDE_CODE_CHILD_SESSION',)
"""Variables to clear in an agent's environment.

A Claude Code session sets ``CLAUDE_CODE_CHILD_SESSION`` for every process it runs, so a daemon or Herdr server started
from one passes it on. A Claude Code agent that inherits it saves no transcript, so it has no token usage and no
Entire checkpoint. The value is set to empty, which Claude Code reads as unset, because a Herdr pane can only override
what its server inherited.
"""
PROMPT = 'Your brief is in {path}. Read all of it, in parts if a read comes back cut short, then follow it.'
"""An agent's first prompt, on its argv or Herdr's (``agent prompt``).

It names the brief's file instead of holding the brief. The prompt is one argument, and Linux limits one argument to
128 KiB (``MAX_ARG_STRLEN``). A research brief with a long history is longer than that.
"""
TAB_TITLE_CHARS = 30
"""How many characters of the idea's title go in a worker's tab label: enough to tell tabs apart in Herdr."""


@dataclass(frozen=True)
class Prepared:
    """A worker row marked ``starting``: what the runtime needs to start it, and how long it may run."""

    spec: WorkerSpec
    runtime: Runtime
    timeout: float
    captures_session: bool
    """Whether the session id is needed when the agent ends, for the Entire attach and the token usage."""
    session_id: str | None
    """Created here when the argv takes ``{session_id}``. None when the runtime reports it after the start."""


class Launcher:
    """Turns a ``workers`` row into a process, and the ended process into a closed row, on the calling thread."""

    def __init__(
        self, db: Database, tracer: Tracer, home: Path, runtime_factory: RuntimeFactory = make_runtime
    ) -> None:
        self.db = db
        self.tracer = tracer
        self.home = home
        self.runtime_factory = runtime_factory
        self.cancelled = threading.Event()
        """Set when the daemon stops. Agents not started yet are skipped, and the daemon closes the rows of the rest."""
        self.entire_version: str | None = None
        """What ``entire version`` printed when the daemon started.

        None before that, or without ``entire``. The attach is still tried then, and the event says why it failed.
        Below ``MIN_ENTIRE``, no session is attached.
        """
        self.lock = threading.Lock()
        """Held from the ``cancelled`` check through ``runtime.start`` to the stored handle, and around row writes.

        ``cancel`` sets the flag under this lock. So after ``cancel`` returns, every agent either has its handle stored
        (and the daemon kills it) or never starts, and no thread reopens a row that the daemon closed. ``RunExecutor``
        shares the lock for the run row and the evaluate process.
        """
        self.handover = threading.Event()
        """Set before ``cancelled`` by an in-place restart (``RunExecutor.hand_over``).

        The agents keep running, and the next daemon process re-attaches to them (``reattach``). So a row that
        ``cancelled`` leaves open is handed over, not stopped.
        """
        self.watching: set[int] = set()
        """Workers whose agent this process started or re-attached to and did not close yet.

        A hand-over leaves these to the next daemon, and waits for every other thread.
        """

    def cancel(self) -> None:
        """Start no more agents. The daemon writes the rows of the agents it kills."""
        with self.lock:
            self.cancelled.set()

    def launch(self, worker_id: int, *, brief: str, cwd: Path, log_dir: Path) -> Outcome:
        """Run worker row ``worker_id`` to the end and close it.

        The steps are: pick the harness and model, start, wait, save the transcript, read the usage, attach the
        session, close the row and emit the events. The caller owns ``cwd`` (a worktree) and removes it after this
        returns, unless the agent was handed over (``Outcome.handed_over``). The usage is read and the session is
        attached before that, from inside ``cwd``.
        """
        return self._run(worker_id, cwd, log_dir, lambda span: self._launch(worker_id, span, brief, cwd, log_dir))

    def reattach(self, worker_id: int, *, cwd: Path, log_dir: Path) -> Outcome:
        """Like ``launch``, for an agent that an earlier daemon process started: watch it to its end and close it.

        The agent can still run, or it can have ended while no daemon watched. Its role's timeout counts from its start.
        """
        return self._run(worker_id, cwd, log_dir, lambda span: self._reattach(worker_id, span, log_dir))

    def _run(self, worker_id: int, cwd: Path, log_dir: Path, agent: Callable[[Span], Outcome]) -> Outcome:
        """Run the agent phase (``agent``), then read the usage, check the startup, attach the session and close."""
        started = time.monotonic()
        with self.tracer.span('piloop.agent', worker=worker_id) as span:
            try:
                try:
                    outcome = agent(span)
                    killed = outcome.status is WorkerStatus.TIMED_OUT or outcome.startup_failed
                except Exception as e:
                    span.error(e)
                    outcome = Outcome(WorkerStatus.EXITED, time.monotonic() - started, error=f'{type(e).__name__}: {e}')
                    killed = True
                cancelled = self.cancelled.is_set()
                usage = None if cancelled else self._usage(worker_id, cwd, log_dir, span)
                if not cancelled and not outcome.startup_failed and self._never_worked(worker_id, outcome, usage, cwd):
                    outcome = replace(outcome, startup_failed=True)
                checkpoint = None if cancelled else self._attach(worker_id, cwd, span)
                if self._close(worker_id, outcome, killed, span, checkpoint, usage):
                    outcome = replace(outcome, handed_over=True)
            finally:
                self.watching.discard(worker_id)
        return outcome

    def _launch(self, worker_id: int, span: Span, brief: str, cwd: Path, log_dir: Path) -> Outcome:
        with self.lock:
            if self.cancelled.is_set():
                return Outcome(WorkerStatus.EXITED, 0, error=STOPPED_ERROR)
            prepared = self._prepare(worker_id, span, brief, cwd, log_dir)
            started = time.monotonic()
            try:
                handle = prepared.runtime.start(prepared.spec)
            except Exception as e:  # an unanswered dialog or a lost prompt: the agent did not start to work
                span.error(e)
                error = self._start_error(e, log_dir)
                return Outcome(WorkerStatus.EXITED, time.monotonic() - started, error=error, startup_failed=True)
            with self.db.session() as s:
                w = s.get_one(Worker, worker_id)
                w.handle, w.state = handle.data, WorkerState.RUNNING
                s.commit()
            self.watching.add(worker_id)
        unreported = prepared.captures_session and prepared.session_id is None
        return self._watch(worker_id, span, prepared.runtime, handle, prepared.timeout, log_dir, unreported=unreported)

    def _reattach(self, worker_id: int, span: Span, log_dir: Path) -> Outcome:
        """Watch the agent of a running row through its runtime. Its seconds count from the row's start."""
        self.watching.add(worker_id)
        with self.db.session() as s:
            w = s.get_one(Worker, worker_id)
            assert w.started_at is not None  # only running rows are re-attached
            elapsed = (utcnow() - w.started_at).total_seconds()
            timeout = config_of(s.get_one(Project, w.project_id)).role(w.kind).timeout - elapsed
            definition = harness_of(s, w)
            unreported = definition is not None and definition.captures_session and w.session_id is None
            runtime, handle = runtime_for(s, w, log_dir, self.runtime_factory), handle_for(w)
            span.set(kind=w.kind, harness=w.harness, model=w.model, reason=w.reason, reattached=True)
        outcome = self._watch(worker_id, span, runtime, handle, max(timeout, 0), log_dir, unreported=unreported)
        return replace(outcome, seconds=elapsed + outcome.seconds)

    def _watch(
        self,
        worker_id: int,
        span: Span,
        runtime: Runtime,
        handle: Handle,
        timeout: float,
        log_dir: Path,
        *,
        unreported: bool,
    ) -> Outcome:
        """Wait for a started agent to end, save its transcript and release it.

        When ``unreported``, record the session id that the runtime reports first.
        """
        try:
            if unreported:
                self._reported_session(worker_id, runtime, handle, span)
            return runtime.wait(handle, timeout)
        finally:
            self._save_transcript(runtime, handle, log_dir)
            runtime.kill(handle)

    def _prepare(self, worker_id: int, span: Span, brief: str, cwd: Path, log_dir: Path) -> Prepared:
        """Pick the harness and model, fill in the row, mark it ``starting`` and emit ``<kind>.started``."""
        with self.db.session() as s:
            w = s.get_one(Worker, worker_id)
            project = s.get_one(Project, w.project_id)
            cfg = config_of(project)
            harness = w.harness or rotate(s, w, cfg.role_harnesses(w.kind))
            model = w.model or cfg.role_models(w.kind)[harness]
            definition = cfg.harness_for(harness)
            template = definition.interactive if cfg.runtime is RuntimeKind.HERDR else definition.headless
            session_id = str(uuid4()) if definition.captures_session and takes_session_id(template) else None
            w.harness, w.model, w.runtime, w.log_dir = harness, model, cfg.runtime.value, str(log_dir)
            w.state, w.started_at, w.session_id = WorkerState.STARTING, utcnow(), session_id
            emit(
                s,
                f'{w.kind}.started',
                TargetKind.WORKER,
                worker_id,
                payload={'harness': harness, 'model': model, 'reason': w.reason},
            )
            s.commit()
            kind, reason, project_name, project_path = w.kind, w.reason, project.name, Path(project.path)
            label = _label(s, w)
            actor = f'{role_of(s, w).name}#{worker_id}'
        span.set(kind=kind, harness=harness, model=model, reason=reason, session_id=session_id)
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / BRIEF_FILE).write_text(brief)
        prompt = PROMPT.format(path=log_dir / BRIEF_FILE)
        # A Herdr pane does not inherit the daemon's environment. Pass the daemon's PATH, where the harness CLIs and
        # uv were found, and the variables that the role's MCP servers name.
        servers = cfg.role_servers(kind)
        env = {
            'PILOOP_HOME': str(self.home),
            'PILOOP_ACTOR': actor,
            'PATH': child_env()['PATH'],
            **dict.fromkeys(CLEARED_ENV, ''),
            **forwarded_env(servers, child_env()),
        }
        argv = [
            fill(a, model=model, prompt=prompt, log_dir=str(log_dir), session_id=session_id or '', cwd=str(cwd))
            for a in template
        ]
        mcp = (
            RENDERERS[definition.mcp].render(
                Launch(servers, definition.executable, cwd, log_dir, child_env() | env, argv)
            )
            if definition.mcp is not None
            else Rendered([])
        )
        spec = WorkerSpec(
            name=f'{kind}-{worker_id}',
            label=label,
            executable=definition.executable,
            args=expand(argv, mcp),
            prompt=prompt,
            cwd=cwd,
            log_dir=log_dir,
            env=env | mcp.env,
            startup_dialogs=definition.startup_dialogs,
            session_id_pattern=definition.session_id_pattern,
        )
        runtime = self.runtime_factory(cfg.runtime, project=project_name, project_path=project_path)
        return Prepared(spec, runtime, cfg.role(kind).timeout, definition.captures_session, session_id)

    def _reported_session(self, worker_id: int, runtime: Runtime, handle: Handle, span: Span) -> None:
        """Record the session id that the runtime reports for the started agent.

        Without an id, the session cannot be attached. Then emit an event and leave the row's id NULL.
        """
        session_id = runtime.session_id(handle)
        if session_id is None:
            span.event('worker.session_unknown', worker=worker_id, runtime=runtime.kind)
            return
        span.set(session_id=session_id)
        with self.lock, self.db.session() as s:
            s.get_one(Worker, worker_id).session_id = session_id
            s.commit()

    def _start_error(self, e: Exception, log_dir: Path) -> str:
        """Return the row's error for a failed start.

        A ``StartFailed`` screen becomes the transcript, and its last lines end the error. The pane is gone now, so this
        screen is the only record of what the agent was stuck on.
        """
        if not isinstance(e, StartFailed):
            return f'{type(e).__name__}: {e}'
        (log_dir / TRANSCRIPT).write_text(e.screen)
        tail = [line for line in e.screen.splitlines() if line.strip()][-START_SCREEN_LINES:]
        return '\n'.join([str(e), 'Its screen ended with:', *tail]) if tail else str(e)

    def _save_transcript(self, runtime: Runtime, handle: Handle, log_dir: Path) -> None:
        try:
            (log_dir / TRANSCRIPT).write_text(runtime.read(handle, lines=TRANSCRIPT_LINES))
        except Exception as e:  # the pane can already be gone; a missing transcript is not an error
            self.tracer.event('worker.transcript_lost', error=str(e))

    def _usage(self, worker_id: int, cwd: Path, log_dir: Path, span: Span) -> UsageJson | None:
        """Read the ended agent's tokens as its harness's ``usage`` spec says. A failure is an event, and stays NULL.

        This runs while the worktree still exists, because a session store can be keyed by the agent's cwd. A harness
        without a spec records nothing. An agent that never ran (no harness picked) has nothing to read.
        """
        with self.db.session() as s:
            w = s.get_one(Worker, worker_id)
            if w.harness is None:
                return None
            found = usage_of(s, w, cwd)
        if found is None:
            return None
        if isinstance(found, UsageError):
            span.event('worker.usage_unknown', worker=worker_id, error=str(found))
            return None
        span.set(tokens=found.total)
        return found.as_json()

    def _never_worked(self, worker_id: int, outcome: Outcome, usage: UsageJson | None, cwd: Path) -> bool:
        """Return whether a started agent ended within ``STARTUP_SECONDS`` without a token and left nothing behind.

        Nothing behind means no commit (its checkout's HEAD did not move) and no comment signed by it. Herdr reports
        some first-launch dialogs as idle, so such an agent looks finished. Only a harness whose usage is read can tell
        "spent nothing" from "unknown". The tokens can be unknown for an agent that worked too: Herdr reports no
        session for a harness without Herdr hooks. So what the agent left decides.
        """
        if outcome.status is WorkerStatus.TIMED_OUT or outcome.seconds >= STARTUP_SECONDS:
            return False
        if (usage is not None and usage['total'] > 0) or git.moved(cwd):
            return False
        with self.db.session() as s:
            w = s.get_one(Worker, worker_id)
            definition = harness_of(s, w)
            if definition is None or definition.usage is None:
                return False
            signed = select(Comment.id).where(Comment.author == f'{role_of(s, w).name}#{worker_id}').limit(1)
            return s.scalar(signed) is None

    def _attach(self, worker_id: int, cwd: Path, span: Span) -> str | None:
        """Attach the ended session to Entire from ``cwd``. A failure is an event, and the row's checkpoint stays NULL.

        Nothing retries the attach: the session stays readable in the harness's own store.
        """
        with self.db.session() as s:
            w = s.get_one(Worker, worker_id)
            target = attachable(w, harness_of(s, w))
        if target is None:
            return None
        session_id, agent = target
        if self.entire_version is not None and not usable(self.entire_version):
            error = f'entire {self.entire_version} is older than {MIN_ENTIRE}; upgrade it: {ENTIRE_INSTALL}'
            span.event('worker.session_unattached', worker=worker_id, session_id=session_id, error=error)
            return None
        try:
            attached = attach_session(session_id, agent, cwd)
        except AttachError as e:
            span.event('worker.session_unattached', worker=worker_id, session_id=session_id, error=str(e))
            return None
        span.event(
            'worker.session_attached',
            worker=worker_id,
            session_id=session_id,
            checkpoint=attached.checkpoint_id,
            agent=attached.agent,
            tokens=attached.tokens,
            entire=self.entire_version,
        )
        return attached.checkpoint_id

    def _close(
        self,
        worker_id: int,
        outcome: Outcome,
        killed: bool,
        span: Span,
        checkpoint: str | None,
        usage: UsageJson | None,
    ) -> bool:
        """Close the row and emit ``<kind>.finished``, and ``STARTUP_ANOMALY`` when it applies.

        After ``cancel``, the daemon's stop owns the row. Return True when the row stays open for the next daemon: a
        hand-over came first and the agent had started. Then this thread does not touch the row again, whatever the
        next daemon did with it.
        """
        with self.lock:
            if self.cancelled.is_set():
                if self.handover.is_set() and worker_id in self.watching:
                    span.set(state='handed_over')
                    return True
                span.set(state=WorkerState.KILLED, error=STOPPED_ERROR)
                return False
            with self.db.session() as s:
                w = s.get_one(Worker, worker_id)
                w.state = WorkerState.KILLED if killed else WorkerState.EXITED
                w.ended_at, w.error, w.checkpoint_id, w.usage = utcnow(), outcome.error, checkpoint, usage
                emit(
                    s,
                    f'{w.kind}.finished',
                    TargetKind.WORKER,
                    worker_id,
                    payload={'reason': w.reason, 'status': outcome.status, 'error': outcome.error},
                )
                if outcome.startup_failed:
                    anomaly: dict[str, object] = {
                        'kind': w.kind,
                        'harness': w.harness,
                        'run': w.run_id,
                        'seconds': round(outcome.seconds, 1),
                        'error': outcome.error,
                    }
                    emit(s, STARTUP_ANOMALY, TargetKind.WORKER, worker_id, payload=anomaly)
                    span.event(STARTUP_ANOMALY, worker=worker_id, harness=w.harness, error=outcome.error)
                s.commit()
                span.set(state=w.state, error=outcome.error, checkpoint=checkpoint)
        return False


def rotate(s: Session, worker: Worker, harnesses: list[str]) -> str:
    """Return the harness for a worker: the n-th agent of a kind in a project takes the n-th harness, round robin."""
    earlier = s.scalar(
        select(func.count())
        .select_from(Worker)
        .where(Worker.project_id == worker.project_id, Worker.kind == worker.kind, Worker.id < worker.id)
    )
    return harnesses[(earlier or 0) % len(harnesses)]


def _label(s: Session, w: Worker) -> str:
    """Return the tab label: ``worker-12 idea#5 Stacking with a meta-learner…``, or ``research-7`` for a sweep."""
    if w.run_id is None:
        return f'{w.kind}-{w.id}'
    idea = s.get_one(Run, w.run_id).idea
    return f'{w.kind}-{w.id} idea#{idea.id} {cell(idea.title, TAB_TITLE_CHARS)}'
