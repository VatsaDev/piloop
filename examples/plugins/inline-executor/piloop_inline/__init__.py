"""``inline``: a piloop executor that runs each job at once, in a thread of the daemon, with no leases.

Each job runs as the built-in ``workers`` executor's worker runs it (``run_agent``, ``run_evaluation``), and its result
goes to core through ``Host.results``. There is no fence: one thread owns each job, and only
it reports. Nothing survives the daemon: ``adopt`` takes over no job, and ``hand_over`` stops every job.
"""

import subprocess
import threading
from collections.abc import Callable, Sequence
from contextlib import suppress
from datetime import datetime
from functools import partial

from piloop.capabilities import (
    AgentJob,
    AgentKey,
    AgentReport,
    EvaluationJob,
    EvaluationReport,
    Handle,
    Host,
    JobKey,
    LeftJob,
    NoParameters,
    Route,
    Settled,
    key_of,
    kill_group,
    remove,
    run_agent,
    run_evaluation,
)


class InlineExecutor:
    """``Executor``: a thread for each job, started when the job is submitted. It takes no parameters."""

    Parameters = NoParameters

    def __init__(self, host: Host, parameters: object) -> None:
        self.host = host
        self._lock = threading.Lock()
        self._jobs: dict[JobKey, _Job] = {}
        self.stopping = False

    @classmethod
    def capacity(cls, parameters: object) -> int | None:
        """No bound: every evaluation runs as soon as it is submitted."""
        return None

    def submit(self, job: AgentJob | EvaluationJob) -> None:
        running = _Job(self, key_of(job))
        with self._lock:
            if self.stopping:
                raise RuntimeError('the executor stops: it takes no more jobs')
            self._jobs[running.key] = running
        running.thread = threading.Thread(target=self._run, args=(job, running), daemon=True)
        running.thread.start()

    def cancel(self, key: JobKey) -> None:
        with self._lock:
            running = self._jobs.get(key)
        if running is not None:
            running.stop()

    def adopt(self, left: Sequence[LeftJob]) -> list[JobKey]:
        """Take over nothing: a previous daemon's jobs died with its threads."""
        return [job.key for job in left]

    def routes(self) -> Sequence[Route]:
        """None: every job runs in the daemon."""
        return []

    def tick(self, now: datetime) -> datetime | None:
        """Nothing to do: a thread reports its own job."""
        return None

    def stop(self, timeout: float) -> None:
        with self._lock:
            self.stopping = True
            running = list(self._jobs.values())
        for job in running:
            job.stop()
        for job in running:
            if job.thread is not None:
                job.thread.join(timeout)

    def hand_over(self) -> None:
        """Stop every job: a next daemon could not find them again."""
        self.stop(0)

    def close(self) -> None:
        """Nothing to release: the threads end with their jobs."""

    def _run(self, job: AgentJob | EvaluationJob, running: _Job) -> None:
        """Run ``job`` to its end and apply its result, or tell core it is lost when the run itself fails."""
        results, key = self.host.results, running.key
        try:
            with self.host.tracer.span('piloop.job', job=job.kind) as span:
                if isinstance(job, AgentJob):
                    runtime = self.host.runtimes(job.runtime, campaign=job.campaign, campaign_path=job.campaign_path)
                    report: AgentReport | EvaluationReport | None = run_agent(job, runtime, running, span)
                else:
                    report = run_evaluation(job, running, span)
            if report is not None:
                with suppress(Settled):  # its row closed meanwhile: a stop
                    results.finished(job, report)
            if isinstance(job, AgentJob):
                remove(job)
        except Exception as e:  # a fault of the executor: nothing must wait for this job forever
            with suppress(Settled):
                results.lost(key, f'{type(e).__name__}: {e}')
        finally:
            with self._lock:
                self._jobs.pop(key, None)


class _Job:
    """``Control`` for one job: how to stop it, and its start and session id."""

    def __init__(self, executor: InlineExecutor, key: JobKey) -> None:
        self._executor = executor
        self.key = key
        self.thread: threading.Thread | None = None
        self._kill: Callable[[], None] | None = None
        self._asked = False

    def stop(self) -> None:
        self._asked = True
        if (kill := self._kill) is not None:
            kill()

    @property
    def entire_version(self) -> str | None:
        return self._executor.host.entire_version

    def begin(self) -> bool:
        return not self._executor.stopping

    def started(self, handle: Handle, session_id: str | None) -> bool:
        assert isinstance(self.key, AgentKey)
        try:
            self._executor.host.results.started(self.key, handle.data, session_id)
        except Settled:  # its row closed meanwhile: nothing waits for the agent
            return False
        return True

    def running(self, kill: Callable[[], None] | None) -> None:
        self._kill = kill
        if kill is not None and self._asked:  # a cancel came while it started
            kill()

    def process(self, proc: subprocess.Popen[bytes]) -> bool:
        self._kill = partial(kill_group, proc)
        return not self._asked and not self._executor.stopping

    def session(self, session_id: str) -> None:
        assert isinstance(self.key, AgentKey)
        with suppress(Settled):
            self._executor.host.results.session(self.key, session_id)

    def asked(self) -> bool:
        return self._asked

    def stopping(self) -> bool:
        return self._executor.stopping

    def handing_over(self) -> bool:
        return False
