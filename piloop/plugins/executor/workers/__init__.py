"""The ``workers`` executor, built in: lease-based workers (``piloop.plugins.executor.kit.leases``), in-process by default.

``[plugins.executor]`` in ``config.toml`` selects it (the default) and sets its ``Parameters``. Its worker in
``piloop serve`` takes every lease. Remote workers are to join it as a configuration of this executor, through the same
lease operations, not as another executor.
"""

from collections.abc import Sequence
from datetime import datetime
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from piloop.plugins.executor.contract import AgentJob, EvaluationJob, Host, JobKey, LeftJob
from piloop.plugins.executor.kit.leases import Leases, Timing
from piloop.plugins.executor.workers.worker import Worker


class WorkersParameters(BaseModel):
    """The keys of ``[plugins.executor]`` for ``workers``.

    A policy beyond these numbers (placement, pools, retries) is another executor's, not more keys here.
    """

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    evaluations: int = Field(default=2, ge=1)
    """How many evaluations run at the same time, across all campaigns (``capacity``). The others wait in the
    evaluation queue (``piloop eval queue``)."""
    renew_seconds: float = Field(default=10, gt=0)
    """How often the worker renews its leases with a heartbeat. Kubernetes: the kubelet renews its node Lease every
    10 s."""
    grace_seconds: float = Field(default=50, gt=0)
    """The silence after which the worker is unreachable (``lease.unreachable``) and gets no new lease until it is
    heard again. Kubernetes: ``--node-monitor-grace-period``, 50 s since 1.32 (40 s before)."""
    toleration_seconds: float = Field(default=300, gt=0)
    """The silence after which its leases expire and their jobs are queued again with a larger fencing token. The
    worker stops its own jobs two heartbeats before. Kubernetes: the ``tolerationSeconds`` of 300 that the
    ``DefaultTolerationSeconds`` admission plugin gives a pod for the unreachable taint."""

    @model_validator(mode='after')
    def _ordered(self) -> Self:
        if not self.renew_seconds < self.grace_seconds < self.toleration_seconds - 2 * self.renew_seconds:
            raise ValueError('renew_seconds < grace_seconds < toleration_seconds - 2 * renew_seconds')
        return self


class WorkersExecutor:
    """``Executor``: every job on a lease (``piloop.plugins.executor.kit.leases``), run by the worker of this process."""

    Parameters = WorkersParameters

    def __init__(self, host: Host, parameters: BaseModel) -> None:
        assert isinstance(parameters, WorkersParameters)
        timing = Timing(parameters.renew_seconds, parameters.grace_seconds, parameters.toleration_seconds)
        self.leases = Leases(host.db, host.results, timing)
        self.worker = Worker(
            self.leases,
            host.tracer,
            runtime_factory=host.runtimes,
            entire_version=host.entire_version,
        )
        """The worker inside ``piloop serve``: every job of this executor runs on it."""

    @classmethod
    def capacity(cls, parameters: BaseModel) -> int | None:
        assert isinstance(parameters, WorkersParameters)
        return parameters.evaluations

    def submit(self, job: AgentJob | EvaluationJob) -> None:
        self.leases.submit(job)

    def cancel(self, key: JobKey) -> None:
        """Revoke the job's live lease: its worker stops it, and it still reports. A queued job is not touched."""
        self.leases.revoke(key)

    def adopt(self, left: Sequence[LeftJob]) -> list[JobKey]:
        """Take back the jobs on a lease, then start the worker. A job that has no lease is returned.

        The worker re-attaches to each agent that still has its worktree, and reports the rest gone. Core fails a
        returned agent, or asks again for a returned evaluation.
        """
        refused = self.leases.adopt(left, self.worker.id)
        self.worker.start()
        return refused

    def tick(self, now: datetime) -> datetime | None:
        """Expire the leases of a silent worker (``Leases.expire``), and return the next expiry."""
        return self.leases.expire(now)

    def stop(self, timeout: float) -> None:
        """End the agent leases that no worker took, stop the worker's jobs, and wait for their reports.

        A kill that failed is tried once more after the wait. An agent's lease that is still live then ends: a report
        that comes later is refused, and the next daemon's worker does not take it.
        """
        self.leases.stop_untaken()
        self.worker.stop()
        self.worker.join(timeout)
        self.worker.stop()
        self.leases.end_unreported()

    def hand_over(self) -> None:
        self.worker.hand_over()

    def close(self) -> None:
        """Nothing to release: the worker's threads end with the daemon process."""
