"""The ``workers`` executor, built in: lease-based workers (``piloop.plugins.executor.kit.leases``), in-process by default.

``[plugins.executor]`` in ``config.toml`` selects it (the default) and sets its ``Parameters``. Its worker in
``piloop serve`` takes every lease. Remote workers are to join it as a configuration of this executor, through the same
lease operations, not as another executor.
"""

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.db.models import LIVE_LEASES, Lease, LeaseKind, LeaseWorker
from piloop.net import Route
from piloop.plugins.executor.contract import AgentJob, Capacity, EvaluationJob, Host, JobKey, LeftJob
from piloop.plugins.executor.kit.capacity import HostSample, Pool
from piloop.plugins.executor.kit.leases import DEFAULT_POOLS, Leases, Timing
from piloop.plugins.executor.workers.worker import Worker


class WorkersParameters(BaseModel):
    """The keys of ``[plugins.executor]`` for ``workers``.

    A policy beyond these numbers (placement, retries) is another executor's, not more keys here.
    """

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    pools: dict[str, Pool] = Field(default=dict(DEFAULT_POOLS))
    """``[plugins.executor.pools.<name>]``: the slots and thresholds of each worker of a pool. ``serve`` is the worker
    inside ``piloop serve``, there with its defaults unless the table names it. Its ``agents = 0`` and
    ``evaluations = 0`` run nothing on the server. The jobs over the slots wait: agents as requested, evaluations in
    the evaluation queue (``piloop eval queue``)."""
    renew_seconds: float = Field(default=10, gt=0)
    """How often the worker renews its leases with a heartbeat. Kubernetes: the kubelet renews its node Lease every
    10 s."""
    grace_seconds: float = Field(default=50, gt=0)
    """The silence after which the worker is unreachable (``lease.unreachable``): its slots count zero until it is
    heard again. Kubernetes: ``--node-monitor-grace-period``, 50 s since 1.32 (40 s before)."""
    toleration_seconds: float = Field(default=300, gt=0)
    """The silence after which its leases expire and their jobs are queued again with a larger fencing token. The
    worker stops its own jobs two heartbeats before. Kubernetes: the ``tolerationSeconds`` of 300 that the
    ``DefaultTolerationSeconds`` admission plugin gives a pod for the unreachable taint."""

    @field_validator('pools', mode='after')
    @classmethod
    def _serve(cls, pools: dict[str, Pool]) -> dict[str, Pool]:
        return {**DEFAULT_POOLS, **pools}

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
        self.leases = Leases(host.db, host.results, timing, parameters.pools)
        self.worker = Worker(
            self.leases,
            host.tracer,
            host.home,
            runtime_factory=host.runtimes,
            entire_version=host.entire_version,
        )
        """The worker inside ``piloop serve``, in the pool ``serve``."""

    def capacity(self) -> Capacity:
        """The free slots of the reachable workers that their pools admit now (``Leases.capacity``)."""
        return self.leases.capacity()

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

    def routes(self) -> Sequence[Route]:
        """None yet: its only worker is the one in ``piloop serve``."""
        return []

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


@dataclass(frozen=True)
class Seen:
    """A worker as ``piloop worker list`` shows it: its row, its last host sample, and its live leases of each kind."""

    worker: LeaseWorker
    host: HostSample
    running: Capacity


def seen(s: Session) -> list[Seen]:
    """Every worker that registered, by name."""
    rows = s.execute(select(Lease.worker, Lease.kind).where(Lease.state.in_(LIVE_LEASES))).all()
    live = Counter((worker, kind) for worker, kind in rows)
    return [
        Seen(
            row,
            HostSample.model_validate(row.sample),
            Capacity(live[row.id, LeaseKind.AGENT], live[row.id, LeaseKind.EVALUATION]),
        )
        for row in s.scalars(select(LeaseWorker).order_by(LeaseWorker.id))
    ]
