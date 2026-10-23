"""A worker of the ``workers`` executor: takes leases (``piloop.plugins.executor.kit.leases``) and runs their jobs on this machine (``piloop.plugins.executor.kit.jobs``).

``piloop serve`` runs one (``SERVE``). It reports each job when it ends. It reads no campaign table: everything
comes in the job's description, and everything goes back in the report.

It talks to its control plane only through a ``LeaseClient``: ``Local`` in ``piloop serve``. One thread sends a
heartbeat with a host sample (``Timing.renew``), takes new jobs and resends unsent reports, from ``start`` until the
worker stops. Another reads the worker's messages (a long-poll): a waiting job wakes the first at once, a revoke stops
a job, and a prompt goes to its agent. Each job has a thread of its own.
A worker that hears nothing back for ``Timing.self_stop`` stops its jobs and keeps their reports until it is heard
again: by then the fence refuses them, and a person can still import what they made (``leases.StaleReport``).
"""

import os
import signal
import subprocess
import threading
import time
import uuid
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from functools import partial
from importlib.metadata import version
from pathlib import Path
from typing import assert_never

from piloop.otel import Tracer
from piloop.plugins.executor.contract import AgentJob, AgentReport, EvaluationJob, EvaluationReport
from piloop.plugins.executor.kit import jobs
from piloop.plugins.executor.kit.capacity import sample
from piloop.plugins.executor.kit.client import LeaseClient, WorkerInfo
from piloop.plugins.executor.kit.leases import (
    DEFAULT_TIMING,
    SERVE,
    AgentHandle,
    Answer,
    Grant,
    NotAddressed,
    ProcessHandle,
    PromptMessage,
    Revoke,
    StaleLease,
    Work,
)
from piloop.plugins.runtime.contract import Handle, Runtime
from piloop.proc import alive, kill_group, started
from piloop.runtime.factory import RuntimeFactory, make_runtime
from piloop.scoring.ledger import evaluator_runners

GONE = 'worker lost: the worker restarted and could not find its {what}'
WORKER_ANOMALY = 'anomaly.worker'
"""An event in the daemon's log: the worker's turns fail one after another. It goes on trying."""


FAILING_TURNS = 3
"""How many of the worker's turns fail one after another before it says so (``WORKER_ANOMALY``)."""


@dataclass(frozen=True)
class Unsent:
    """A report to send, with its request id, and the agent's job whose worktree goes once it is out."""

    request_id: str
    report: AgentReport | EvaluationReport
    job: AgentJob | None


@dataclass
class Running:
    """A job that this worker runs now: how to stop it, and what its heartbeat says."""

    kill: Callable[[], None] | None
    """How to stop its process. None while it starts, and after its evaluator ended: nothing to kill yet, or any more."""
    evaluation: bool = False
    session_id: str | None = None
    asked: bool = False
    """The worker asked it to stop: a revoke, the worker's own stop. An evaluation it stopped is not run again."""
    killed: bool = False
    """The kill went through. Until then, each heartbeat that asks again, and each turn of a silent worker, tries it
    again: a kill can fail (a Herdr server that does not answer)."""
    steer: Callable[[str], None] | None = None
    """How to prompt its agent (``Runtime.prompt`` at its handle), once the agent started. None for an evaluation."""


class Worker:
    """Takes leases through ``client``, in ``pool``, and runs their jobs. ``start`` makes it work."""

    def __init__(
        self,
        client: LeaseClient,
        tracer: Tracer,
        home: Path,
        *,
        pool: str = SERVE,
        runtime_factory: RuntimeFactory = make_runtime,
        runners: tuple[str, ...] | None = None,
        entire_version: str | None = None,
    ) -> None:
        self.id: str | None = None
        """The worker's name, which its registration gives."""
        self.pool = pool
        self.home = home
        """Where the worker's host sample reads the free disk."""
        self.client = client
        """The worker's seam to its control plane."""
        self.timing = DEFAULT_TIMING
        """The lease timing, which its registration gives."""
        self._tracer = tracer
        self.runtime_factory = runtime_factory
        self.runners = runners if runners is not None else evaluator_runners()
        self.entire_version = entire_version
        """What ``entire version`` printed on this machine (``Host.entire_version``)."""
        self._lock = threading.RLock()
        """Guards ``_jobs``, ``_threads`` and the flags, briefly: never across a slow call (a start, a kill, the
        database), so a heartbeat never waits for one. Re-entrant.

        A job enters ``_jobs`` under it, with the ``stopping`` check, before its start. So after ``stop``, every job is
        either asked to stop or never starts, and one that was starting kills its agent once the start returns."""
        self._jobs: dict[int, Running] = {}
        self._threads: dict[int, threading.Thread] = {}
        self._unsent: dict[int, Unsent] = {}
        """Reports that could not be sent, by lease: sent again on every turn of the loop."""
        self._loop: threading.Thread | None = None
        self._listener: threading.Thread | None = None
        self._wanted = threading.Event()
        self._registered = False
        self._outcomes: dict[int, str | None] = {}
        """What each prompt past the cursor that this worker handled came to, by message id: a prompt read again,
        because its answer did not land, is answered the same, and not delivered again."""
        self._started = False
        self._stopping = False
        self._handover = False
        self._heard = time.monotonic()
        """When the last heartbeat that came back was sent. The worker stops its jobs ``Timing.self_stop`` after it."""
        self._failures = 0
        """How many turns failed one after another (``FAILING_TURNS``)."""
        self._starting: set[int] = set()
        """The leases of the agents that start now: ``runtime.start`` ran or runs, and their start is not over."""
        self._started_all = threading.Condition(self._lock)
        """Notified when ``_starting`` falls: a hand-over waits for every start in flight to be recorded."""
        self._recovered = threading.Condition(self._lock)
        """Notified when a held job is registered again, or its thread ends: the listener waits for every one."""

    def start(self) -> None:
        """Register, find this worker's leases again, then take jobs and read messages. Later calls do nothing.

        A registration that fails (a locked database) is tried again on each turn of the loop. A job that a restarted
        worker cannot find again is reported gone (``LeaseClient.lost``): an agent whose worktree is gone, an
        evaluation (its evaluator cannot be watched again, so it is stopped and queued again).
        """
        with self._lock:
            if self._started or self._stopping:
                return
            self._started = True
        self._join()
        with self._lock:
            if self._stopping:  # a stop came while it registered: it joined no loop, so none may start
                return
            self._loop = threading.Thread(target=self._run_loop, name=f'worker-{self.pool}', daemon=True)
            self._loop.start()

    def _join(self) -> bool:
        """Register, recover the leases that this worker holds, and start reading messages. Return whether it did."""
        info = WorkerInfo(
            version=version('piloop'), pool=self.pool, runners=list(self.runners), sample=sample(self.home)
        )
        try:
            registration = self.client.register(info)
        except Exception as e:  # the database is locked, say: the next turn tries again
            self._tracer.event('worker.turn_failed', worker=self.id, error=f'{type(e).__name__}: {e}')
            return False
        self.id, self.timing, self._registered = registration.worker, registration.timing, True
        for grant in registration.held:
            self._spawn(grant, partial(self._recover, grant))
        held = [grant.lease for grant in registration.held]
        self._listener = threading.Thread(
            target=self._listen, args=(registration.cursor, held), name=f'messages-{self.id}', daemon=True
        )
        self._listener.start()
        return True

    def _wake(self) -> None:
        """Make the loop turn now."""
        self._wanted.set()

    def _listen(self, after: int, held: list[int]) -> None:
        """Read the messages after the cursor ``after`` until the worker stops: wake the loop, stop jobs, prompt.

        It reads once every ``held`` job is registered again or given up, so a prompt that waited across a restart
        finds its agent. One long-poll is open at a time, after the GitHub Actions runner. Its wait is one heartbeat,
        so a worker that stops ends this thread within one. The cursor moves past a prompt only once its answer
        landed, so the next read, in this process or the next, gets an unanswered prompt again.
        """
        with self._lock:
            self._recovered.wait_for(lambda: all(lease in self._jobs or lease not in self._threads for lease in held))
        while not self._stopping:
            try:
                messages = self.client.messages(after, self.timing.renew)
            except Exception as e:  # the control plane does not answer: the heartbeat's loop decides what stops
                self._tracer.event('worker.unheard', worker=self.id, error=str(e))
                self._wanted.wait(self.timing.renew)
                continue
            prompts: dict[int, list[PromptMessage]] = {}
            for message in messages:
                match message:
                    case Work():
                        self._wake()
                    case Revoke(lease=lease):
                        self._stop_job(lease, 'revoke')
                    case PromptMessage(lease=lease):
                        prompts.setdefault(lease, []).append(message)
                    case never:
                        assert_never(never)
            unanswered = [id_ for lease, batch in prompts.items() for id_ in self._prompt(lease, batch)]
            after = min(unanswered) - 1 if unanswered else max((m.id for m in messages), default=after)
            self._outcomes = {id_: outcome for id_, outcome in self._outcomes.items() if id_ > after}
            if unanswered:  # the control plane does not take answers now: the next read would get them at once
                self._wanted.wait(self.timing.renew)

    def _prompt(self, lease_id: int, messages: list[PromptMessage]) -> list[int]:
        """Deliver the prompts of one agent in one ``Runtime.prompt``, answer each, and return those not answered.

        A prompt handled before is answered as it was, and not delivered again. One prompt, because two prompts typed
        into a busy pane join in its input queue with nothing between them.
        """
        new = [m for m in messages if m.id not in self._outcomes]
        if new:
            with self._lock:
                running = self._jobs.get(lease_id)
                steer = running.steer if running is not None else None
            error: str | None = None
            if steer is None:
                error = 'the agent does not run on this worker'
            else:
                try:
                    steer('\n'.join(m.prompt.text for m in new))
                except (
                    Exception
                ) as e:  # NotSteerable (subprocess), a blocked agent or a closed pane: the worker goes on
                    error = str(e)
            self._outcomes.update(dict.fromkeys((m.id for m in new), error))
        unanswered: list[int] = []
        for message in messages:
            try:
                self.client.delivered(message.id, self._outcomes[message.id])
            except NotAddressed as e:  # not this worker's: no answer of it will land
                self._tracer.event('worker.delivered_refused', worker=self.id, message=message.id, error=str(e))
            except Exception as e:  # the control plane does not answer: the cursor stays, so the next read gets it
                self._tracer.event('worker.delivered_unsent', worker=self.id, message=message.id, error=str(e))
                unanswered.append(message.id)
        return unanswered

    def stop(self) -> None:
        """Take no more jobs, and stop the running ones. Each still reports how it ended.

        A kill that failed is tried again on each call.
        """
        with self._lock:
            self._stopping = True
            jobs = list(self._jobs)
        for lease_id in jobs:
            self._stop_job(lease_id, 'stop')
        self._wake()

    def hand_over(self) -> None:
        """Take no more jobs, and leave the agents running for the next daemon process (an in-place restart).

        Their leases stay live, and the next worker re-attaches to them. Evaluations are stopped: a new worker cannot
        watch an evaluator again, and its lease is reported gone by the next worker, which queues it again.
        """
        with self._lock:
            self._handover = self._stopping = True
            # a start that is not recorded leaves the next worker an agent it cannot find; starts are bounded waits
            self._started_all.wait_for(lambda: not self._starting)
            evaluations = [lease_id for lease_id, running in self._jobs.items() if running.evaluation]
        for lease_id in evaluations:
            self._stop_job(lease_id, 'hand-over')
        self._wanted.set()  # a loop that waits ends at once; one that turns ends after its turn
        if (loop := self._loop) is not None:
            loop.join(self.timing.renew)  # the daemon closes its database next

    def join(self, timeout: float) -> None:
        """Wait up to ``timeout`` seconds for the jobs' threads and the loop to end."""
        deadline = time.monotonic() + timeout
        with self._lock:  # a thread is registered and started under the lock: none of these is unstarted
            threads, loop = list(self._threads.values()), self._loop
        for thread in threads:
            thread.join(max(deadline - time.monotonic(), 0))
        self._wanted.set()  # the loop ends once the jobs are reported
        if loop is not None:
            loop.join(max(deadline - time.monotonic(), 0))

    def _run_loop(self) -> None:
        """Turn every ``Timing.renew``, or at once on a change, until the worker stopped and its jobs reported.

        A turn that fails is tried again after ``Timing.renew``: it never ends the loop, which would let every lease of
        the worker expire.
        """
        # after a hand-over the next daemon's worker has the leases, and this daemon's database may be closed
        while not self._handover:
            self._wanted.clear()  # before the turn: a change during it makes another turn at once
            failed = not (self._registered or self._join()) or not self._guarded_turn()
            with self._lock:
                done = not self._threads and not self._unsent and (not failed or not self._registered)
                if self._stopping and done:  # an unregistered worker has nothing to report
                    return
            self._wanted.wait(self.timing.renew)

    def _guarded_turn(self) -> bool:
        """Run a turn, and return whether it went through. A streak of ``FAILING_TURNS`` failures is an anomaly."""
        try:
            self._turn()
        except Exception as e:  # the database is locked, a job that no longer validates, a bug
            self._failures += 1
            error = f'{type(e).__name__}: {e}'
            self._tracer.event('worker.turn_failed', worker=self.id, failures=self._failures, error=error)
            if self._failures == FAILING_TURNS:
                self._tracer.event(WORKER_ANOMALY, worker=self.id, failures=self._failures, error=error)
            return False
        self._failures = 0
        return True

    def _turn(self) -> None:
        """Send the heartbeat with a host sample, act on its answers, send what is unsent, and take new jobs."""
        host = sample(self.home)
        try:
            with self._lock:  # every lease it holds, a job that is still starting too
                sessions = {
                    lease_id: running.session_id if (running := self._jobs.get(lease_id)) is not None else None
                    for lease_id in self._threads
                }
            sent = time.monotonic()
            answers = self.client.heartbeat(sessions, host)
            # the renewal is stamped inside the call, after ``sent``: a call that waited for the write lock must not
            # move the worker's own deadline past the lease's
            self._heard = sent
        except Exception as e:  # a partition: the control plane does not answer
            self._tracer.event('worker.unheard', worker=self.id, error=str(e))
            if time.monotonic() - self._heard >= self.timing.self_stop:
                self._stop_jobs('silent')
            return
        for lease_id, answer in answers.items():
            if answer is not Answer.RENEWED:
                self._stop_job(lease_id, answer)
        for lease_id, unsent in list(self._unsent.items()):
            self._send(lease_id, unsent)
        while not self._stopping and isinstance(grant := self.client.acquire(host), Grant):
            body = self._agent if isinstance(grant.job, AgentJob) else self._evaluation
            self._spawn(grant, partial(body, grant))

    def _stop_jobs(self, why: str) -> None:
        with self._lock:
            jobs = list(self._jobs)
        for lease_id in jobs:
            self._stop_job(lease_id, why)

    def _stop_job(self, lease_id: int, why: str) -> None:
        """Ask a job to stop, and kill it. Called again after a kill that failed, it tries the kill again."""
        with self._lock:
            running = self._jobs.get(lease_id)
            if running is None or running.killed:
                return
            first, running.asked = not running.asked, True
            kill = running.kill
        if first:
            self._tracer.event('worker.job_stopped', worker=self.id, lease=lease_id, why=why)
        if kill is None:  # it starts: its start kills it once it returns
            return
        try:
            kill()
        except Exception as e:  # a pane that is gone already, or a server that does not answer: try again next time
            self._tracer.event('worker.kill_failed', worker=self.id, lease=lease_id, error=str(e))
            return
        with self._lock:
            running.killed = True

    def _spawn(self, grant: Grant, body: Callable[[], None]) -> None:
        kind = 'agent' if isinstance(grant.job, AgentJob) else 'evaluation'
        thread = threading.Thread(
            target=self._guarded, args=(grant.lease, body), name=f'{kind}-{grant.lease}', daemon=True
        )
        with self._lock:
            self._threads[grant.lease] = thread
            thread.start()

    def _guarded(self, lease_id: int, body: Callable[[], None]) -> None:
        """Run a job's thread. A fault of the worker itself loses the job, so that nothing waits for it forever."""
        try:
            body()
        except Exception as e:
            self._tracer.event('worker.job_failed', worker=self.id, lease=lease_id, error=f'{type(e).__name__}: {e}')
            with self._lock:
                self._jobs.pop(lease_id, None)
            self._lost(lease_id, f'worker lost: {type(e).__name__}: {e}')
        finally:
            with self._lock:
                self._threads.pop(lease_id, None)
                self._recovered.notify_all()

    def _send(self, lease_id: int, unsent: Unsent) -> None:
        """Send a report. Once the control plane accepted or refused it, remove the agent's worktree.

        A refused one is dropped: the control plane keeps it as a stale report. One that cannot be sent (a partition,
        a database locked past its busy timeout) is kept and sent again on the next turn, and its worktree stays: a
        worker killed meanwhile leaves its successor the ended agent in its worktree, which it reports.
        """
        try:
            self.client.report(lease_id, unsent.request_id, unsent.report)
        except StaleLease as e:
            self._tracer.event('worker.report_refused', worker=self.id, lease=lease_id, error=str(e))
        except Exception as e:  # keep it and send it again
            self._tracer.event('worker.report_unsent', worker=self.id, lease=lease_id, error=str(e))
            self._unsent[lease_id] = unsent
            return
        self._unsent.pop(lease_id, None)
        if unsent.job is not None and not self._handover:
            jobs.remove(unsent.job)

    def _report(self, lease_id: int, report: AgentReport | EvaluationReport, job: AgentJob | None = None) -> None:
        """Send the report of a job that ended, then remove ``job``'s worktree. Nothing after a hand-over."""
        with self._lock:
            self._jobs.pop(lease_id, None)
        if self._handover:
            return
        self._send(lease_id, Unsent(uuid.uuid4().hex, report, job))
        self._wake()  # a slot is free

    def _register(self, lease_id: int, kill: Callable[[], None] | None, *, evaluation: bool = False) -> Running:
        running = Running(kill, evaluation)
        self._jobs[lease_id] = running
        return running

    def _recover(self, grant: Grant) -> None:
        """Watch a job of this worker's that an earlier process of it started, or report it gone."""
        job = grant.job
        if isinstance(job, EvaluationJob):
            match grant.handle:
                case ProcessHandle(pgid=pgid, start_time=float(start)) if alive(pgid, start):
                    with suppress(ProcessLookupError):
                        os.killpg(pgid, signal.SIGKILL)
            self._lost(grant.lease, GONE.format(what='evaluator'))
            jobs.remove(job)  # its worktree is this lease's own: no later lease removes it
            return
        if grant.started_at is None or not isinstance(grant.handle, AgentHandle):  # it never started, or unrecorded
            jobs.remove(job)
            self._lost(grant.lease, GONE.format(what='agent'))
            return
        if not job.cwd.is_dir():
            self._lost(grant.lease, GONE.format(what='agent'))
            return
        runtime = self._runtime(job)
        handle = Handle(job.runtime, grant.handle.data)
        elapsed = time.time() - grant.started_at.timestamp()
        with self._lock:
            running = self._register(grant.lease, partial(runtime.kill, handle))
            running.session_id, running.steer = job.session_id, partial(runtime.prompt, handle)
            self._recovered.notify_all()
        with self._tracer.span('piloop.job', lease=grant.lease, agent_session=job.agent_session) as span:
            span.set(reattached=True)
            report = jobs.watch_agent(job, runtime, handle, elapsed, _Control(self, grant.lease, runtime), span)
        if report is not None:  # after the span ends: what waits for the report reads a finished span
            self._report(grant.lease, report, job)

    def _lost(self, lease_id: int, reason: str) -> None:
        try:
            self.client.lost(lease_id, reason)
        except StaleLease as e:
            self._tracer.event('worker.report_refused', worker=self.id, lease=lease_id, error=str(e))

    def _runtime(self, job: AgentJob) -> Runtime:
        return self.runtime_factory(job.runtime, campaign=job.campaign, campaign_path=job.campaign_path)

    def _agent(self, grant: Grant) -> None:
        """Run an agent's job (``jobs.run_agent``), and report it once the job's span ends.

        So what waits for the report reads a finished span.
        """
        job = grant.job
        assert isinstance(job, AgentJob)
        runtime = self._runtime(job)
        with self._tracer.span('piloop.job', lease=grant.lease, agent_session=job.agent_session) as span:
            report = jobs.run_agent(job, runtime, _Control(self, grant.lease, runtime), span)
        if report is not None:
            self._report(grant.lease, report, job)

    def _evaluation(self, grant: Grant) -> None:
        """Run an evaluation's job (``jobs.run_evaluation``), and report what it measured."""
        job = grant.job
        assert isinstance(job, EvaluationJob)
        with self._lock:  # a stop that comes before the evaluator starts kills it when it starts
            self._register(grant.lease, None, evaluation=True)
        with self._tracer.span('piloop.job', lease=grant.lease, evaluation=job.evaluation) as span:
            report = jobs.run_evaluation(job, _Control(self, grant.lease), span)
        self._report(grant.lease, report)


class _Control:
    """``jobs.Control`` for one lease of a worker: the job's start, its kill, and its session id go through here.

    ``runtime`` is an agent's: once the agent started, its prompts go to it there.
    """

    def __init__(self, worker: Worker, lease_id: int, runtime: Runtime | None = None) -> None:
        self._worker = worker
        self._lease = lease_id
        self._runtime = runtime

    @property
    def entire_version(self) -> str | None:
        return self._worker.entire_version

    def begin(self) -> bool:
        w = self._worker
        with w._lock:
            if w._stopping:
                return False
            w._register(self._lease, None)
            w._starting.add(self._lease)
        return True

    def started(self, handle: Handle, session_id: str | None) -> bool:
        w = self._worker
        assert self._runtime is not None  # an agent's control has its runtime
        with w._lock:  # before its row says that it runs: a prompt may come at once
            w._jobs[self._lease].steer = partial(self._runtime.prompt, handle)
        try:
            w.client.started(self._lease, AgentHandle(runtime=handle.runtime, data=handle.data), session_id)
        except StaleLease:
            return False
        return True

    def running(self, kill: Callable[[], None] | None) -> None:
        w = self._worker
        with w._lock:
            running = w._jobs[self._lease]
            if self._lease in w._starting:  # an agent's start is over, recorded or not
                w._starting.discard(self._lease)
                w._started_all.notify_all()
                if kill is None:  # it did not start: there is no job to stop
                    w._jobs.pop(self._lease)
            running.kill = kill
            asked = running.asked and kill is not None
        if asked:  # a stop or a revoke came while it started
            w._stop_job(self._lease, 'asked while it started')

    def process(self, proc: subprocess.Popen[bytes]) -> bool:
        w = self._worker
        with w._lock:
            running = w._jobs[self._lease]
            running.kill = partial(kill_group, proc)
            if running.asked or w._stopping:
                running.asked = running.killed = True
                return False
        try:
            w.client.started(self._lease, ProcessHandle(pgid=proc.pid, start_time=started(proc.pid)), None)
        except StaleLease:
            return False
        return True

    def session(self, session_id: str) -> None:
        w = self._worker
        with w._lock:
            w._jobs[self._lease].session_id = session_id
        w._wake()  # the next heartbeat carries it to the agent's row

    def asked(self) -> bool:
        with self._worker._lock:
            running = self._worker._jobs.get(self._lease)
            return running is not None and running.asked

    def stopping(self) -> bool:
        return self._worker._stopping

    def handing_over(self) -> bool:
        return self._worker._handover
