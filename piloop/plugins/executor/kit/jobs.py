"""Run one job on this machine: an agent with its runtime, or an evaluation with the evaluator runner.

Any executor can use it (``piloop.plugins.executor.contract``): ``workers`` does, for each job its worker takes, and so
can one that runs jobs inline. What the executor does around a job (track it, stop it, report it) is its
``Control``; the machine it runs on is its ``Site``. Like Airflow's ``run_workload``: one way to run a unit of work,
whichever executor placed it.

The steps of an agent's job: make its place and its artifact (a worktree, ``Clones``), write the brief, render the MCP
servers with the variables of this machine, start the agent, watch it to its end, save its transcript, read its
usage, and bring its commits back as a bundle. What it made goes back by address: the
report names the bundle, its log files (``Site.files``) and what it wrote in the shared workspace in the CAS. The worktree stays: the executor removes it
(``remove``) once the report is out, so a worker that restarts in between finds the ended agent there.
"""

import os
import shlex
import subprocess
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field, replace
from enum import StrEnum
from functools import partial
from pathlib import Path
from typing import Protocol

from covalence import Cid
from git import Repo

from piloop import git
from piloop.config import BRIEF_FILE, TRANSCRIPT
from piloop.ids import CommentId
from piloop.mcp import RENDERERS, Launch, expand, forwarded_env
from piloop.otel import Span
from piloop.plugins.cas.contract import Cas
from piloop.plugins.executor.contract import (
    DEADLINE_ENV,
    AgentFinished,
    AgentJob,
    AgentNotStarted,
    AgentReport,
    EvaluationJob,
    EvaluationReport,
    Prompt,
    RepoId,
    Sample,
)
from piloop.plugins.executor.kit.clones import Clones, Fetch
from piloop.plugins.executor.kit.relay import RELAY_ARGV
from piloop.plugins.runtime.contract import AgentExit, AgentSpec, ExitStatus, Handle, Runtime, taken
from piloop.proc import PILOOP_ARGV, child_env, kill_group
from piloop.scoring.evaluate import SENT, EvalResult, run_evaluate
from piloop.settings import INPUTS_DIR
from piloop.store import commits
from piloop.store.tree import Materialised, Seen, evict_unlinked, put_tree
from piloop.usage import UsageError, read_usage

TRANSCRIPT_LINES = 2000
STOPPED = 'the worker stopped before the job started'
WORKTREE = 'worktree'
CLONES = 'clones'
"""Where a worker elsewhere keeps its clones, under its home (``clone_of``)."""
"""The worktree of a job's ``GitSource``, in its place."""
INPUTS_REBUILT = 'inputs.rebuilt'
"""An event: a shared copy of a linked input no longer held its blob (a job changed it), and was made again."""
INPUTS_CHANGED = 'inputs.changed'
"""An event: an evaluation changed a linked input that it read, so what it measured is a crash, not a measurement."""
CHANGED_INPUTS = 'evaluate changed its linked inputs'
SHOWN = 10
"""How many paths or blobs an event or an error names."""
CLI_SHIM = '#!/bin/sh\nPILOOP_HOME={home} exec {argv} "$@"\n'
"""``<home>/bin/piloop``: this home's piloop, the worker's own code on the worker's home, or on a worker elsewhere the
relay to the server (``door.main``). Agents get it as ``PILOOP_BIN``, which the briefs use, and its directory first on
their PATH, for the prompts' plain ``piloop``. Another install can come first on the user's PATH (a release next to a
build of ``main``), and a harness's shell can run the user's rc files again, which can put it back in front."""


def own_cli(home: Path, argv: Sequence[str]) -> Path:
    """Write the ``CLI_SHIM`` that runs ``argv`` under ``home``, and return its path."""
    shim = home / 'bin' / 'piloop'
    text = CLI_SHIM.format(home=shlex.quote(str(home)), argv=shlex.join(argv))
    if not shim.is_file() or shim.read_text() != text:
        shim.parent.mkdir(parents=True, exist_ok=True)
        shim.write_text(text)
        shim.chmod(0o755)
    return shim


@dataclass
class Site:
    """The machine that runs jobs: its home, under which each job's place is made, its CAS, and its git repos."""

    home: Path
    cas: Cas
    clones: Clones
    reserve: int
    """Bytes kept free on the disk of ``home`` (``[storage] min_free_gb``): ``evict_inputs`` frees room below it."""
    piloop: Sequence[str] = PILOOP_ARGV
    """What an agent's ``piloop`` runs: this process's piloop, or on a worker elsewhere the relay to the server."""
    cli: Path = field(init=False)
    seen: Seen = field(default_factory=dict, init=False)
    """What ``files`` stored, so that a file it stored before is not read again."""

    def __post_init__(self) -> None:
        self.cli = own_cli(self.home, self.piloop)

    def log_dir(self, job: AgentJob | EvaluationJob) -> Path:
        """The job's place on this machine: its log files, and its worktree."""
        return self.home / job.place

    def cwd(self, job: AgentJob | EvaluationJob) -> Path:
        """Where the job runs: its worktree."""
        return self.log_dir(job) / WORKTREE

    def env(self) -> dict[str, str]:
        """What an agent's environment gets from this machine: its piloop first on the PATH, and its home.

        ``PILOOP_HOME`` too, although the shim sets it: a harness shell that runs the user's rc files can put another
        install's piloop first on PATH, and with the variable that one still uses this home's database.
        """
        path = f'{self.cli.parent}{os.pathsep}{child_env()["PATH"]}'
        return {'PILOOP_HOME': str(self.home), 'PILOOP_BIN': str(self.cli), 'PATH': path}

    def evict_inputs(self) -> int:
        """Under ``reserve``, remove the copies of linked inputs that no worktree links, as ``make_room`` does.

        One eviction (``evict_unlinked``): a worker elsewhere has no archiver to run ``make_room``. Return how many went.
        """
        return evict_unlinked(self.home / INPUTS_DIR, self.reserve)

    def files(self, job: AgentJob | EvaluationJob) -> dict[str, Cid]:
        """Put the files of the job's log directory, its worktree aside, into the CAS, and return them by path."""
        log_dir = self.log_dir(job)
        names = [name for name in os.listdir(log_dir) if name != WORKTREE] if log_dir.is_dir() else []
        stored = put_tree(self.cas, log_dir, names, seen=self.seen)
        return {e.path: e.blob for e in stored.manifest.entries if e.blob is not None}


def local_site(home: Path, cas: Cas, repos: Callable[[RepoId], Path], reserve: int) -> Site:
    """The machine of the daemon's own process: its home and CAS, with the repos of record as its clones.

    A commit that a clone lacks is bundled from the repo of record into the CAS, as the server does for a remote worker.
    """
    return Site(home, cas, Clones(cas, repos, partial(bundle_of_record, cas, repos), home), reserve)


def remote_site(home: Path, cas: Cas, fetch: Fetch, reserve: int) -> Site:
    """The machine of a worker elsewhere: its home and the shared CAS, with clones of its own, filled by ``fetch``.

    Its agents' ``piloop`` is the relay to the server (``door.main``).
    """
    return Site(home, cas, Clones(cas, partial(clone_of, home), fetch, home), reserve, RELAY_ARGV)


def clone_of(home: Path, repo: RepoId) -> Path:
    """The worker's clone of a repo of record, ``<home>/clones/<repo>``: an empty repo at first, filled from bundles."""
    path = home / CLONES / str(repo)
    if not (path / '.git').is_dir():
        Repo.init(path)
    return path


def bundle_of_record(
    cas: Cas, repos: Callable[[RepoId], Path], repo: RepoId, heads: Sequence[str], have: Sequence[str]
) -> Cid | None:
    """What the server does for a worker that lacks a commit: a bundle of it from the repo of record, into the CAS."""
    return commits.bundle(cas, git.open_repo(repos(repo)), heads, have)


class Control(Protocol):
    """What running one job asks of the executor that runs it."""

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
        """Whether the executor stops: an evaluation runs no more of its repeats then."""
        ...

    def handing_over(self) -> bool:
        """Whether the daemon restarts in place: the agent keeps running, with its worktree, for the next one."""
        ...

    def queued(self) -> list[list[Prompt]]:
        """The prompts that the agent's runtime took, one list for each ``Runtime.prompt``, oldest first."""
        ...


def run_agent(job: AgentJob, runtime: Runtime, control: Control, site: Site, span: Span) -> AgentReport | None:
    """Make the artifact, start the agent, watch it to its end, and return its report.

    None when there is nothing to report: the job is no longer the executor's, or the daemon hands it over.
    """
    # only an agent that a previous daemon left has neither, and it is re-attached (``watch_agent``)
    assert job.spec is not None and job.brief is not None
    started_at = time.monotonic()
    log_dir, cwd = site.log_dir(job), site.cwd(job)
    try:
        _rebuilt(site.clones.checkout(job.artifact, cwd), span)
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / BRIEF_FILE).write_text(job.brief)
        spec = rendered(job, job.spec.at(cwd, log_dir, site.env()))
    except Exception as e:  # a full disk, a locked index, a missing variable: a fault of the host, not of the agent
        span.error(e)
        return _unstarted(job, site, f'{type(e).__name__}: {e}', started_at)
    if not control.begin():
        remove(job, site)
        return AgentNotStarted(reason=STOPPED)
    handle: Handle | None = None
    try:
        started_at = time.monotonic()
        try:
            handle = runtime.start(replace(spec, env=spec.env | {DEADLINE_ENV: str(int(time.time() + job.timeout))}))
        except Exception as e:  # a missing executable, a full disk: the agent did not start to work
            span.error(e)
            return _unstarted(job, site, f'{type(e).__name__}: {e}', started_at)
        try:
            recorded = control.started(handle, job.session_id)
        except Exception as e:
            # Not recorded, so nothing would ever watch the agent or wait for its process: end it here.
            span.error(e)
            runtime.kill(handle)
            handle = None
            return _unstarted(job, site, f'{type(e).__name__}: {e}', started_at)
        if not recorded:  # the job is no longer the executor's: nothing waits for this agent
            runtime.kill(handle)
            handle = None
            remove(job, site)
            return None
    finally:
        control.running(None if handle is None else _killer(runtime, handle))
    assert handle is not None
    return watch_agent(job, runtime, handle, 0, control, site, span)


def watch_agent(
    job: AgentJob, runtime: Runtime, handle: Handle, elapsed: float, control: Control, site: Site, span: Span
) -> AgentFinished | None:
    """Watch a started agent to its end, then return its report. ``elapsed``: the seconds it ran before this.

    None after a hand-over: the next daemon re-attaches, and the job and its worktree stay.
    """
    outcome, session_id = _watch(job, runtime, handle, max(job.timeout - elapsed, 0), control, site, span)
    if control.handing_over():
        return None
    return _finish(job, replace(outcome, seconds=elapsed + outcome.seconds), session_id, control, site, span)


def _killer(runtime: Runtime, handle: Handle) -> Callable[[], None]:
    return lambda: runtime.kill(handle)


def _unstarted(job: AgentJob, site: Site, error: str, started_at: float) -> AgentNotStarted:
    """Remove the worktree of an agent that did not start, and return its report: a failed start."""
    remove(job, site)
    return AgentNotStarted(reason=error, seconds=time.monotonic() - started_at, files=site.files(job))


def _watch(
    job: AgentJob, runtime: Runtime, handle: Handle, timeout: float, control: Control, site: Site, span: Span
) -> tuple[AgentExit, str | None]:
    """Wait for a started agent to end, save its transcript and release it. Return how, and its session id.

    When the harness reports its session id, record it: asked before the wait and again before the agent is released,
    because a harness that is slow to start reports its session late.
    """
    session_id = job.session_id
    reported = not job.needs_session_id or session_id is not None
    started_at = time.monotonic()
    try:
        if not reported:
            session_id = _session(runtime, handle, control, span)
            reported = session_id is not None
        outcome = runtime.wait(handle, timeout)
    except Exception as e:  # a pane or a server that is gone: the agent ended for the worker
        span.error(e)
        outcome = AgentExit(ExitStatus.EXITED, time.monotonic() - started_at, error=f'{type(e).__name__}: {e}')
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
                (site.log_dir(job) / TRANSCRIPT).write_text(runtime.read(handle, lines=TRANSCRIPT_LINES))
            except Exception as e:  # the pane can already be gone; a missing transcript is not an error
                span.event('agent_session.transcript_lost', error=str(e))
            runtime.kill(handle)
    return outcome, session_id  # after the last ask: a session reported late goes in the report


def _session(runtime: Runtime, handle: Handle, control: Control, span: Span) -> str | None:
    session_id = runtime.session_id(handle)
    if session_id is not None:
        span.set(session_id=session_id)
        control.session(session_id)
    return session_id


def _finish(
    job: AgentJob, outcome: AgentExit, session_id: str | None, control: Control, site: Site, span: Span
) -> AgentFinished:
    """Read the usage, bring the commits back, and return the report with its log files."""
    report = AgentFinished(process=outcome, session_id=session_id)
    span.set(session_id=session_id)
    log_dir, cwd = site.log_dir(job), site.cwd(job)
    if job.usage is not None:
        try:
            usage = read_usage(job.usage, session_id, cwd, log_dir)
            report.usage = usage
            span.set(tokens=usage.total if usage is not None else None)
        except Exception as e:  # an unreadable file or a bug in a reader must not stop the report
            error = str(e) if isinstance(e, UsageError) else f'{type(e).__name__}: {e}'
            span.event('agent_session.usage_unknown', agent_session=job.agent_session, error=error)
    try:
        report.changed = git.changed(cwd, site.clones.ignored(job.artifact))
        if job.commits is not None:
            report.commits = site.clones.bring_back(job.artifact, cwd, job.commits.message)
        report.workspace = site.clones.changed(job.artifact, cwd)
    except Exception as e:  # a worktree that is gone: the report says what is known
        span.error(e)
        report.process = replace(report.process, error=report.process.error or f'{type(e).__name__}: {e}')
    report.read, report.unread = _reads(control.queued(), log_dir)
    report.files = site.files(job)
    process = report.process
    head = report.commits.head if report.commits is not None else None
    span.set(status=process.status, error=process.error, head=head)
    return report


def _reads(queued: list[list[Prompt]], log_dir: Path) -> tuple[list[CommentId], list[CommentId]]:
    """Split the ``queued`` prompts into those that the agent took from its queue and those that wait in it (``taken``).

    Each ``Runtime.prompt`` is one message of the queue, so the messages that wait are the newest prompts, and the ones
    taken came just before. A runtime without the queue leaves both empty: its prompts stay queued.
    """
    read, waiting = taken(log_dir)
    split = max(len(queued) - waiting, 0)
    return [p.id for b in queued[max(split - read, 0) : split] for p in b], [p.id for b in queued[split:] for p in b]


def remove(job: AgentJob | EvaluationJob, site: Site) -> None:
    """Remove the job's worktree, if it made one."""
    site.clones.remove(job.artifact, site.cwd(job))


def run_evaluation(job: EvaluationJob, control: Control, site: Site, span: Span) -> EvaluationReport:
    """Run the evaluator ``repeats`` times on the candidate's tree, each next to a run on the bar's tree, if any.

    The order is candidate, bar, bar, candidate, candidate, bar (``order``). It stops at the first run that fails: a
    candidate that crashes once crashes again, and a pair needs both sides. The candidate runs first, so each job
    measures it. Each run has its own worktree and logs, under ``<side>-<n>`` in the job's place, and the report names
    the files of all of them.
    """
    samples: dict[Tree, list[Sample]] = {Tree.CANDIDATE: [], Tree.BAR: []}
    for side in order(job.repeats, paired=job.bar is not None):
        n = len(samples[side])
        artifact = job.artifact if side is Tree.CANDIDATE or job.bar is None else job.bar.artifact
        one = job.model_copy(update={'artifact': artifact, 'place': f'{job.place}/{side}-{n}'})
        result = _evaluate(one, control, site, span)
        sample = Sample(metrics=result.metrics, seconds=result.seconds, error=result.error, units=result.units)
        samples[side].append(sample)
        if result.error is not None or control.asked() or control.stopping():
            break
    return EvaluationReport(samples=samples[Tree.CANDIDATE], bar_samples=samples[Tree.BAR], files=site.files(job))


class Tree(StrEnum):
    """The tree that one run of an evaluation measures."""

    CANDIDATE = 'candidate'
    BAR = 'bar'


def order(repeats: int, *, paired: bool) -> list[Tree]:
    """The runs of an evaluation: ``repeats`` of the candidate, and with ``paired``, as many of the bar in ABBA order.

    Linear drift of the load then costs both sides alike. The candidate opens, so a first run that is slower (a cold
    cache) counts against the candidate, never for it.
    """
    if not paired:
        return [Tree.CANDIDATE] * repeats
    pairs = [(Tree.CANDIDATE, Tree.BAR), (Tree.BAR, Tree.CANDIDATE)]
    return [side for i in range(repeats) for side in pairs[i % 2]]


def _evaluate(job: EvaluationJob, control: Control, site: Site, span: Span) -> EvalResult:
    """Run the evaluator once on the job's tree.

    An evaluate that another process killed, an agent's ``pkill -f`` say, measured nothing of the tree, so it runs
    once more, in a new worktree. The executor's own kills (a cancel, its stop) are not run again.
    """
    result = _evaluate_once(job, control, site, span)
    if result.signal in SENT and not control.asked() and not control.stopping():
        span.event('evaluation.retry', evaluation=job.evaluation, error=result.error)
        result = _evaluate_once(job, control, site, span)
    span.set(error=result.error)
    return result


def _evaluate_once(job: EvaluationJob, control: Control, site: Site, span: Span) -> EvalResult:
    """Run the evaluate command in a new worktree, which is removed after. A failure to start is a crash.

    What it measured counts only if the linked inputs that it read still hold their blobs: a crash otherwise.
    """
    log_dir, cwd = site.log_dir(job), site.cwd(job)
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        links = _rebuilt(site.clones.checkout(job.artifact, cwd), span)
        result = run_evaluate(
            job.command,
            cwd=cwd,
            log_dir=log_dir,
            timeout=job.timeout,
            required=set(job.required),
            track=lambda proc: _track(control, proc),
            units_file=log_dir.parent / f'.units-{uuid.uuid4().hex}.json',
        )
        if changed := sorted(rel for rel, copy in links.copies.items() if not copy.holds(cwd / rel)):
            span.event(INPUTS_CHANGED, evaluation=job.evaluation, paths=', '.join(changed[:SHOWN]))
            return replace(result, metrics={}, units={}, error=f'{CHANGED_INPUTS}: {", ".join(changed[:SHOWN])}')
        return result
    except Exception as e:
        span.error(e)
        return EvalResult({}, 0.0, f'{type(e).__name__}: {e}')
    finally:
        remove(job, site)


def _rebuilt(links: Materialised, span: Span) -> Materialised:
    """Record the blobs whose shared copy a job had changed: they were made again for this one."""
    if links.rebuilt:
        span.event(INPUTS_REBUILT, blobs=', '.join(str(blob) for blob in links.rebuilt[:SHOWN]))
    return links


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
        Launch(job.servers, spec.executable, spec.cwd, spec.log_dir, child_env() | env, spec.args)
    )
    return replace(spec, args=expand(spec.args, mcp), resume_args=expand(spec.resume_args, mcp), env=env | mcp.env)
