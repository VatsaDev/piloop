"""Run one job on this machine: an agent with its runtime, or an evaluation with the evaluator runner.

Any executor can use it (``piloop.plugins.executor.contract``): ``workers`` does, for each job its worker takes, and so
can one that runs jobs inline. What the executor does around a job (track it, stop it, report it) is its
``Control``. Like Airflow's ``run_workload``: one way to run a unit of work, whichever executor placed it.

The steps of an agent's job: make the artifact (a worktree), write the brief, render the MCP servers with the
variables of this machine, start the agent, watch it to its end, save its transcript, read its usage, attach its
session to Entire, and bring its commits back (``Commits``). The worktree stays: the executor removes it
(``remove``) once the report is out, so a worker that restarts in between finds the ended agent there.
"""

import subprocess
import time
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Protocol

from git import Repo

from piloop import git
from piloop.agents.capture import ENTIRE_INSTALL, MIN_ENTIRE, AttachError, attach_session, usable
from piloop.config import BRIEF_FILE, TRANSCRIPT
from piloop.mcp import RENDERERS, Launch, expand, forwarded_env
from piloop.otel import Span
from piloop.plugins.executor.contract import (
    AgentFinished,
    AgentJob,
    AgentNotStarted,
    AgentReport,
    Checkout,
    EvaluationJob,
    EvaluationReport,
)
from piloop.plugins.runtime.contract import AgentExit, AgentSpec, ExitStatus, Handle, Runtime, StartFailed
from piloop.proc import child_env, kill_group
from piloop.scoring.evaluate import SENT, EvalResult, run_evaluate
from piloop.usage import UsageError, read_usage

TRANSCRIPT_LINES = 2000
START_SCREEN_LINES = 12
"""How many non-blank lines from the end of a failed start's screen go into the row's error."""
STOPPED = 'the worker stopped before the job started'


class Control(Protocol):
    """What running one job asks of the executor that runs it."""

    @property
    def entire_version(self) -> str | None:
        """What ``entire version`` printed when the daemon started, or None. Below ``MIN_ENTIRE``, nothing attaches."""
        ...

    def begin(self) -> bool:
        """The agent is about to start. False when the executor stops: start nothing."""
        ...

    def started(self, handle: Handle, session_id: str | None) -> bool:
        """The start went through: record it. False when the job is no longer the executor's: it is killed."""
        ...

    def running(self, kill: Callable[[], None] | None) -> None:
        """The start is over: this is how to stop the job now. None when there is nothing to kill (any more)."""
        ...

    def process(self, proc: subprocess.Popen[bytes]) -> bool:
        """An evaluator started. Return False to kill it at once: a stop, or a job that is no longer the executor's."""
        ...

    def session(self, session_id: str) -> None:
        """The runtime reported the agent's session id."""
        ...

    def asked(self) -> bool:
        """Whether the executor asked the job to stop: an evaluator that it killed is not run again."""
        ...

    def stopping(self) -> bool:
        """Whether the executor stops: the session is not attached to Entire then."""
        ...

    def handing_over(self) -> bool:
        """Whether the daemon restarts in place: the agent keeps running, with its worktree, for the next one."""
        ...


def run_agent(job: AgentJob, runtime: Runtime, control: Control, span: Span) -> AgentReport | None:
    """Make the artifact, start the agent, watch it to its end, and return its report.

    None when there is nothing to report: the job is no longer the executor's, or the daemon hands it over.
    """
    # only an agent that a previous daemon left has neither, and it is re-attached (``watch_agent``)
    assert job.spec is not None and job.brief is not None
    started_at = time.monotonic()
    try:
        if not isinstance(job.artifact, Checkout):
            artifact = job.artifact
            git.add_worktree(git.open_repo(artifact.repo), job.cwd, artifact.commit, artifact.links)
        job.log_dir.mkdir(parents=True, exist_ok=True)
        (job.log_dir / BRIEF_FILE).write_text(job.brief)
        spec = rendered(job, job.spec)
    except Exception as e:  # a full disk, a locked index, a missing variable: a fault of the host, not of the agent
        span.error(e)
        return _unstarted(job, f'{type(e).__name__}: {e}', started_at)
    if not control.begin():
        remove(job)
        return AgentNotStarted(reason=STOPPED)
    handle: Handle | None = None
    try:
        started_at = time.monotonic()
        try:
            handle = runtime.start(spec)
        except Exception as e:  # an unanswered dialog or a lost prompt: the agent did not start to work
            span.error(e)
            return _unstarted(job, _start_error(e, job.log_dir), started_at)
        try:
            recorded = control.started(handle, job.session_id)
        except Exception as e:
            # Not recorded, so nothing would ever watch the agent or wait for its process: end it here.
            span.error(e)
            runtime.kill(handle)
            handle = None
            return _unstarted(job, f'{type(e).__name__}: {e}', started_at)
        if not recorded:  # the job is no longer the executor's: nothing waits for this agent
            runtime.kill(handle)
            handle = None
            remove(job)
            return None
    finally:
        control.running(None if handle is None else _killer(runtime, handle))
    assert handle is not None
    return watch_agent(job, runtime, handle, 0, control, span)


def watch_agent(
    job: AgentJob, runtime: Runtime, handle: Handle, elapsed: float, control: Control, span: Span
) -> AgentFinished | None:
    """Watch a started agent to its end, then return its report. ``elapsed``: the seconds it ran before this.

    None after a hand-over: the next daemon re-attaches, and the job and its worktree stay.
    """
    outcome, session_id = _watch(job, runtime, handle, max(job.timeout - elapsed, 0), control, span)
    if control.handing_over():
        return None
    return _finish(job, replace(outcome, seconds=elapsed + outcome.seconds), session_id, control, span)


def _killer(runtime: Runtime, handle: Handle) -> Callable[[], None]:
    return lambda: runtime.kill(handle)


def _unstarted(job: AgentJob, error: str, started_at: float) -> AgentNotStarted:
    """Remove the worktree of an agent that did not start, and return its report: a failed start."""
    remove(job)
    return AgentNotStarted(reason=error, seconds=time.monotonic() - started_at)


def _watch(
    job: AgentJob, runtime: Runtime, handle: Handle, timeout: float, control: Control, span: Span
) -> tuple[AgentExit, str | None]:
    """Wait for a started agent to end, save its transcript and release it. Return how, and its session id.

    When the harness reports its session id, record it: asked before the wait and again before the agent is released,
    because a harness that is slow to start reports its session late.
    """
    session_id = job.session_id
    reported = not job.captures_session or session_id is not None
    started_at = time.monotonic()
    try:
        if not reported:
            session_id = _session(runtime, handle, control, span)
            reported = session_id is not None
        return runtime.wait(handle, timeout), session_id
    except Exception as e:  # a pane or a server that is gone: the agent ended for the worker
        span.error(e)
        return AgentExit(ExitStatus.EXITED, time.monotonic() - started_at, error=f'{type(e).__name__}: {e}'), session_id
    finally:
        if not reported:
            try:
                if (session_id := _session(runtime, handle, control, span)) is None:
                    span.event('agent_session.session_unknown', agent_session=job.agent_session, runtime=runtime.kind)
            except Exception as e:  # the pane or the server can be gone; releasing the agent comes first
                span.event(
                    'agent_session.session_unknown',
                    agent_session=job.agent_session,
                    runtime=runtime.kind,
                    error=str(e),
                )
        if not control.handing_over():
            try:
                (job.log_dir / TRANSCRIPT).write_text(runtime.read(handle, lines=TRANSCRIPT_LINES))
            except Exception as e:  # the pane can already be gone; a missing transcript is not an error
                span.event('agent_session.transcript_lost', error=str(e))
            runtime.kill(handle)


def _session(runtime: Runtime, handle: Handle, control: Control, span: Span) -> str | None:
    session_id = runtime.session_id(handle)
    if session_id is not None:
        span.set(session_id=session_id)
        control.session(session_id)
    return session_id


def _finish(job: AgentJob, outcome: AgentExit, session_id: str | None, control: Control, span: Span) -> AgentFinished:
    """Read the usage, attach the session, bring the commits back, and return the report."""
    report = AgentFinished(process=outcome, session_id=session_id)
    span.set(session_id=session_id)
    if job.usage is not None:
        try:
            usage = read_usage(job.usage, session_id, job.cwd, job.log_dir)
            report.usage = usage
            span.set(tokens=usage.total if usage is not None else None)
        except Exception as e:  # an unreadable file or a bug in a reader must not stop the report
            error = str(e) if isinstance(e, UsageError) else f'{type(e).__name__}: {e}'
            span.event('agent_session.usage_unknown', agent_session=job.agent_session, error=error)
    links = job.artifact.links if not isinstance(job.artifact, Checkout) else {}
    try:
        ignore = git.linked(job.cwd, links)
        report.changed = git.changed(job.cwd, ignore)
        if not control.stopping():
            report.checkpoint = _attach(job, session_id, control.entire_version, span)
        if job.commits is not None and not isinstance(job.artifact, Checkout):
            report.head = _commits(job, Repo(job.cwd), ignore)
    except Exception as e:  # a worktree that is gone: the report says what is known
        span.error(e)
        report.process = replace(report.process, error=report.process.error or f'{type(e).__name__}: {e}')
    process = report.process
    span.set(status=process.status, error=process.error, head=report.head, checkpoint=report.checkpoint)
    return report


def _commits(job: AgentJob, wt: Repo, ignore: list[str]) -> str | None:
    """Commit what the agent left uncommitted, on its own commit, and pin the head at ``Commits.ref``.

    Return the head, or None when the agent made no commit. The uncommitted work is never amended into the agent's
    last commit, which is scored as the agent made it.
    """
    assert job.commits is not None and not isinstance(job.artifact, Checkout)
    if git.is_dirty(wt, ignore):
        git.commit_all(wt, job.commits.message, ignore)
    head = wt.head.commit.hexsha
    if head == job.artifact.commit:
        return None
    git.pin(git.open_repo(job.artifact.repo), job.commits.ref, head)
    return head


def remove(job: AgentJob | EvaluationJob) -> None:
    """Remove the job's worktree, if it made one."""
    if not isinstance(job.artifact, Checkout):
        git.remove_worktree(git.open_repo(job.artifact.repo), job.cwd)


def _attach(job: AgentJob, session_id: str | None, entire_version: str | None, span: Span) -> str | None:
    """Attach the ended session to Entire from its cwd. A failure is an event, and the checkpoint stays NULL."""
    if job.entire_agent is None or session_id is None:
        return None
    agent_session = job.agent_session
    if entire_version is not None and not usable(entire_version):
        error = f'entire {entire_version} is older than {MIN_ENTIRE}; upgrade it: {ENTIRE_INSTALL}'
        span.event('agent_session.session_unattached', agent_session=agent_session, session_id=session_id, error=error)
        return None
    try:
        attached = attach_session(session_id, job.entire_agent, job.cwd)
    except AttachError as e:
        span.event('agent_session.session_unattached', agent_session=agent_session, session_id=session_id, error=str(e))
        return None
    span.event(
        'agent_session.session_attached',
        agent_session=agent_session,
        session_id=session_id,
        checkpoint=attached.checkpoint_id,
        agent=attached.agent,
        tokens=attached.tokens,
        entire=entire_version,
    )
    return attached.checkpoint_id


def run_evaluation(job: EvaluationJob, control: Control, span: Span) -> EvaluationReport:
    """Run the evaluator on the candidate's tree, and return what it measured.

    An evaluate that another process killed, an agent's ``pkill -f`` say, measured nothing of the candidate, so it
    runs once more, in a new worktree. The executor's own kills (a cancel, its stop) are not run again.
    """
    result = _evaluate_once(job, control, span)
    if result.signal in SENT and not control.asked() and not control.stopping():
        span.event('evaluation.retry', evaluation=job.evaluation, error=result.error)
        result = _evaluate_once(job, control, span)
    span.set(error=result.error)
    return EvaluationReport(metrics=result.metrics, seconds=result.seconds, error=result.error, units=result.units)


def _evaluate_once(job: EvaluationJob, control: Control, span: Span) -> EvalResult:
    """Run the evaluate command in a new worktree, which is removed after. A failure to start is a crash."""
    try:
        job.log_dir.mkdir(parents=True, exist_ok=True)
        job.units_file.parent.mkdir(parents=True, exist_ok=True)
        git.add_worktree(git.open_repo(job.artifact.repo), job.cwd, job.artifact.commit, job.artifact.links)
        return run_evaluate(
            job.command,
            cwd=job.cwd,
            log_dir=job.log_dir,
            timeout=job.timeout,
            required=set(job.required),
            track=lambda proc: _track(control, proc),
            units_file=job.units_file,
        )
    except Exception as e:
        span.error(e)
        return EvalResult({}, 0.0, f'{type(e).__name__}: {e}')
    finally:
        remove(job)


def _track(control: Control, proc: subprocess.Popen[bytes] | None) -> None:
    """Tell the executor how to kill the evaluator while it runs. One that it refuses is killed at once."""
    if proc is None:
        control.running(None)
        return
    if not control.process(proc):
        kill_group(proc)


def rendered(job: AgentJob, spec: AgentSpec) -> AgentSpec:
    """The spec with the values of the servers' variables from this machine's environment, and ``{mcp}`` expanded.

    Raises:
        McpError: a variable is not set here, or the servers cannot be given to this agent.
    """
    env = spec.env | forwarded_env(job.servers, child_env())
    if job.mcp is None:
        return replace(spec, env=env)
    mcp = RENDERERS[job.mcp].render(
        Launch(job.servers, spec.executable, job.cwd, job.log_dir, child_env() | env, spec.args)
    )
    return replace(spec, args=expand(spec.args, mcp), resume_args=expand(spec.resume_args, mcp), env=env | mcp.env)


def _start_error(e: Exception, log_dir: Path) -> str:
    """Return the error of a failed start.

    A ``StartFailed`` screen becomes the transcript, and its last lines end the error. The pane is gone now, so this
    screen is the only record of what the agent was stuck on.
    """
    if not isinstance(e, StartFailed):
        return f'{type(e).__name__}: {e}'
    log_dir.mkdir(parents=True, exist_ok=True)
    (log_dir / TRANSCRIPT).write_text(e.screen)
    tail = [line for line in e.screen.splitlines() if line.strip()][-START_SCREEN_LINES:]
    return '\n'.join([str(e), 'Its screen ended with:', *tail]) if tail else str(e)
