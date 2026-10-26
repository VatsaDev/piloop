"""The ``workers`` executor, built in: lease-based workers (``piloop.plugins.executor.kit.leases``), in-process by default.

``[plugins.executor]`` in ``config.toml`` selects it (the default) and sets its fields (``Workers``). Its worker in
``piloop serve`` takes the leases of the pool ``serve``. A worker on another machine (``piloop worker --server``) takes
those of its pool, through the same lease operations over HTTP (``piloop.plugins.executor.kit.remote``). A pool with a
provisioner gets its workers started and stopped on demand (``piloop.plugins.executor.kit.scaling``).
"""

import threading
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Self

import httpx2 as httpx
from pydantic import Field, field_validator, model_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.daemon.loop import Consumer, Turn
from piloop.db.models import LIVE_LEASES, EventModel, Lease, LeaseKind, LeaseWorker, WorkerInstance, WorkerInstanceState
from piloop.net.server import executor_router
from piloop.otel import Tracer
from piloop.plugins.cas.contract import Cas
from piloop.plugins.contribution import Contribution
from piloop.plugins.executor.contract import (
    AgentJob,
    AgentKey,
    Capacity,
    EvaluationJob,
    ExecutorConfig,
    Host,
    JobKey,
    LeftJob,
    Prompt,
)
from piloop.plugins.executor.kit import jobs, scaling
from piloop.plugins.executor.kit.capacity import HostSample, Pool, Provisioner
from piloop.plugins.executor.kit.client import Local
from piloop.plugins.executor.kit.leases import (
    DEFAULT_POOLS,
    SERVE,
    Leases,
    LeaseWorkerJoined,
    LeaseWorkerRelieved,
    Timing,
)
from piloop.plugins.executor.kit.relay import Relay, Runner
from piloop.plugins.executor.kit.remote import Remote, lease_routes
from piloop.plugins.executor.kit.scaling import Scaler
from piloop.plugins.executor.kit.transfers import FirstTransfers
from piloop.plugins.executor.workers.worker import Worker


class Workers(ExecutorConfig):
    """``workers``: the keys of ``[plugins.executor]``.

    A policy beyond these numbers (placement, retries) is another executor's, not more keys here.
    """

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

    def open(self, host: Host) -> WorkersExecutor:
        return WorkersExecutor(host, self)


class WorkersExecutor:
    """``Executor``: every job on a lease (``piloop.plugins.executor.kit.leases``), run by the worker of this process."""

    def __init__(self, host: Host, config: Workers) -> None:
        timing = Timing(config.renew_seconds, config.grace_seconds, config.toleration_seconds)
        self.leases = Leases(host.db, host.results, host.cas, timing, config.pools, host.secrets)
        self._changed = host.db.changed
        self._cas = host.cas
        self._send = FirstTransfers(host.db, host.cas, host.records, host.tracer)
        """What the ``send`` route does: versions from the repo of record, into the CAS."""
        self._cli = Runner(host.db, host.home, host.repos)
        """What the ``cli`` route does: an agent's relayed call, run here."""
        self.worker = Worker(
            Local(self.leases, SERVE),
            host.tracer,
            jobs.local_site(host.home, host.cas, host.repos, host.records, host.reserve),
            runtime_factory=host.runtimes,
        )
        """The worker inside ``piloop serve``, in the pool ``serve``."""
        self.scaler = Scaler.of(
            host.db, lambda: self.leases.pools, host.network, host.tracer, timing, host.home, _worker_argv
        )
        """Starts and stops the workers of the pools that have a provisioner."""

    def configure(self, config: Workers) -> None:
        """Take the pools with the live limits applied: the next ``acquire`` and ``capacity`` read them."""
        self.leases.pools = config.pools

    def capacity(self) -> Capacity:
        """The free slots of the reachable workers that their pools admit now (``Leases.capacity``)."""
        return self.leases.capacity()

    def submit(self, job: AgentJob | EvaluationJob) -> None:
        self.leases.submit(job)

    def prompt(self, key: AgentKey, prompt: Prompt) -> None:
        """Send ``prompt`` to the agent's worker (``Leases.prompt``). The worker answers once it reached the agent."""
        self.leases.prompt(key, prompt)

    def cancel(self, key: JobKey) -> None:
        """Revoke the job's live lease: its worker stops it, and it still reports. A queued job is not touched."""
        self.leases.revoke(key)

    def adopt(self, left: Sequence[LeftJob]) -> list[JobKey]:
        """Take back the jobs on a lease, then start the worker. A job that has no lease is returned.

        The worker re-attaches to each agent that still has its worktree, and reports the rest gone. Core fails a
        returned agent, or asks again for a returned evaluation.
        """
        refused = self.leases.adopt(left, SERVE)
        self.worker.start()
        return refused

    def capacity_events(self) -> frozenset[type[EventModel]]:
        """A worker that joins or comes back, and one whose host is no longer under pressure."""
        return frozenset({LeaseWorkerJoined, LeaseWorkerRelieved})

    def contribution(self) -> Contribution:
        """Its upkeep and its lease routes.

        The upkeep wakes on what the scaler of the provisioned pools acts on (``scaling.WAKES``). Workers on other
        machines call the lease routes (``remote.lease_routes``, ``piloop worker``).
        """
        wakes = frozenset(t.model for t in scaling.WAKES)
        routes = lease_routes(self.leases, self._changed, self._cas, self._send.send, self._cli)
        return Contribution(consumers=(Consumer('executor', wakes, self._upkeep),), router=executor_router(routes))

    def _upkeep(self, turn: Turn) -> datetime | None:
        """Expire the leases of a silent worker (``Leases.expire``), scale the provisioned pools, and return when next."""
        due = [d for d in (self.leases.expire(turn.now, turn.absent), self.scaler.upkeep(turn.now)) if d is not None]
        return min(due, default=None)

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
        self.scaler.join(timeout)

    def hand_over(self) -> None:
        self.worker.hand_over()
        self.scaler.join(self.leases.timing.renew)  # the next daemon process lists the instances again

    def close(self) -> None:
        """Nothing to release: the worker's threads end with the daemon process."""


def _worker_argv(server: str, pool: str, policy: Provisioner) -> list[str]:
    """What a provisioned instance runs: ``piloop worker`` of ``pool``, on ``server``."""
    once = ['--once'] if policy.once else []
    orphan = policy.orphan_exit_seconds
    idle = ['--orphan-exit', f'{orphan:g}'] if orphan is not None else []
    return ['piloop', 'worker', '--server', server, '--pool', pool, *once, *idle]


def remote_worker(
    http: httpx.Client,
    stop: threading.Event,
    tracer: Tracer,
    home: Path,
    cas: Cas,
    pool: str,
    reserve: int,
    relay: Path,
    *,
    once: bool = False,
    orphan_exit: float | None = None,
) -> Worker:
    """A worker on this machine that takes the leases of ``pool`` from a server through ``http`` (``piloop worker``).

    It reads and writes ``cas``, the CAS that the server shares, and keeps clones of its own. After each job it keeps
    ``reserve`` bytes free on its home's disk (``Site.evict_inputs``). Its calls give up trying
    again once ``stop`` is set. Its agents' ``piloop`` calls go to the server through the socket ``relay``. With ``once``, it
    takes one lease and ends once it reported. With ``orphan_exit``, it ends after that many seconds without an answer of the server.
    """
    client = Remote(http, stop, tracer)
    site = jobs.remote_site(home, cas, client.send, reserve)
    return Worker(client, tracer, site, pool=pool, relay=Relay(relay, client.cli), once=once, orphan_exit=orphan_exit)


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


def instances(s: Session) -> Sequence[WorkerInstance]:
    """The workers that a provisioner started and that have not ended, oldest first."""
    live = WorkerInstance.state != WorkerInstanceState.ENDED
    return s.scalars(select(WorkerInstance).where(live).order_by(WorkerInstance.id)).all()
