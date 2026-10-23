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
    Capacity,
    EvaluationJob,
    EvaluationReport,
    ExecutorConfig,
    Handle,
    Host,
    JobKey,
    LeftJob,
    Prompt,
    Route,
    Runtime,
    Settled,
    key_of,
    kill_group,
    local_site,
    remove,
    run_agent,
    run_evaluation,
)


class Inline(ExecutorConfig):
    """``inline``: it takes no parameters."""

    def open(self, host: Host) -> InlineExecutor:
        return InlineExecutor(host)


class InlineExecutor:
    """``Executor``: a thread for each job, started when the job is submitted."""

    def __init__(self, host: Host) -> None:
        self.host = host
        self.site = local_site(host.home, host.cas, host.repos, host.reserve)
        self._lock = threading.Lock()
        self._jobs: dict[JobKey, _Job] = {}
        self.stopping = False

    def configure(self, config: ExecutorConfig) -> None:
        """Nothing to change: it takes no parameters."""

    def capacity(self) -> Capacity:
        """No bound: every job runs as soon as it is submitted."""
        return Capacity(agents=None, evaluations=None)

    def submit(self, job: AgentJob | EvaluationJob) -> None:
        running = _Job(self, key_of(job))
        with self._lock:
            if self.stopping:
                raise RuntimeError('the executor stops: it takes no more jobs')
            self._jobs[running.key] = running
        running.thread = threading.Thread(target=self._run, args=(job, running), daemon=True)
        running.thread.start()

    def prompt(self, key: AgentKey, prompt: Prompt) -> None:
        """Type the prompt into the agent's runtime at once, and say how that went."""
        with self._lock:
            running = self._jobs.get(key)
        steer = running.steer if running is not None else None
        error = None if steer is not None else 'the agent is not running'
        if steer is not None and running is not None:
            try:
                steer(prompt.text)
            except Exception as e:  # the runtime refused it (a headless agent): core records why
                error = str(e)
            else:
                running.prompts.append([prompt])
        self.host.results.queued(key, prompt.id, error)

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
                    path = self.site.clones.path(job.artifact)
                    runtime = self.host.runtimes(job.runtime, campaign=job.campaign, campaign_path=path)
                    running.runtime = runtime
                    report: AgentReport | EvaluationReport | None = run_agent(job, runtime, running, self.site, span)
                    running.steer = None  # the agent ended: a prompt from now on is undelivered
                else:
                    report = run_evaluation(job, running, self.site, span)
            if report is not None:
                with suppress(Settled):  # its row closed meanwhile: a stop
                    results.finished(job, report)
            if isinstance(job, AgentJob):
                remove(job, self.site)
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
        self.runtime: Runtime | None = None
        self.steer: Callable[[str], None] | None = None
        """How to prompt the agent, once it started."""
        self.prompts: list[list[Prompt]] = []
        """What its runtime took, one prompt at a time: the report says which ones the agent read."""
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
        if self.runtime is not None:
            self.steer = partial(self.runtime.prompt, handle)
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

    def queued(self) -> list[list[Prompt]]:
        return self.prompts
