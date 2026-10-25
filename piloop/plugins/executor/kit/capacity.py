"""The capacity of a worker, after Kubernetes: a pool's allocatable slots, and the thresholds that keep jobs off a host.

A **pool** is the capacity of each worker in it (``Pool``). A worker sends a **host sample** (``HostSample``) on
every heartbeat. ``admits`` checks a pool against a sample and what the worker runs: a host under memory or load
pressure takes no job, as the node conditions of Kubernetes taint a node. Its slots then count zero.
"""

import os
from datetime import datetime
from pathlib import Path
from typing import Annotated

import psutil
from pydantic import BaseModel, ConfigDict, Field, model_validator

from piloop.db.models import LeaseKind, NoLease, utcnow
from piloop.plugins.executor.contract import Capacity
from piloop.plugins.host import Live
from piloop.plugins.provisioner.contract import PROVISIONERS, ProvisionerConfig

GB = 10**9


class HostSample(BaseModel):
    """What a worker's host has now. The worker samples it once per heartbeat."""

    model_config = ConfigDict(frozen=True)

    at: datetime
    cpus: int
    load1: float
    """The 1-minute load average."""
    memory_available: int
    """Bytes."""
    disk_free: int
    """Bytes, on the worker's home."""


def sample(home: Path) -> HostSample:
    """Sample this host, with the free disk of ``home``."""
    return HostSample(
        at=utcnow(),
        cpus=os.cpu_count() or 1,
        load1=os.getloadavg()[0],
        memory_available=psutil.virtual_memory().available,
        disk_free=psutil.disk_usage(str(home)).free,
    )


class Provisioner(BaseModel):
    """``[plugins.executor.pools.<name>.provisioner]``: start the pool's workers on demand, between ``min`` and ``max``.

    These keys are the scaling policy, the same for every provider (the Nomad autoscaler's split of policy and
    target). ``compute`` is the provider that starts and stops the workers.
    """

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    compute: Annotated[ProvisionerConfig, PROVISIONERS.entry]
    """The provisioner and its keys, as a table: ``[plugins.executor.pools.<name>.provisioner.compute]``, with its
    ``provider``."""
    max: Annotated[int, Field(ge=0), Live]
    """The most workers at once: the hard bound of the cost. 0 starts none, and drains those that run."""
    min: Annotated[int, Field(ge=0), Live] = 0
    """The workers that run when no job waits. GitHub's ARC: ``minRunners``."""
    idle_seconds: Annotated[float, Field(gt=0), Live] = 300
    """A worker past ``min`` that held no lease for this long is drained, and stopped. Ray: ``idle_timeout_minutes``
    of 5."""
    max_starting: Annotated[int, Field(ge=1), Live] = 4
    """The most workers that start at once, before they register. Ray: ``max_concurrent_launches``."""
    register_seconds: float = Field(default=600, gt=0)
    """A worker that does not register this long after its start is stopped, and the pool backs off before it starts
    another. Slurm: ``ResumeTimeout``."""
    max_worker_hours_per_day: Annotated[float | None, Field(gt=0), Live] = None
    """The pool starts no worker once its workers ran this many hours in the last 24. No bound when unset."""
    once: bool = False
    """Each worker takes one lease, runs it and exits (``piloop worker --once``): for a platform that bills per call or
    cannot keep a process."""
    orphan_exit_seconds: float | None = Field(default=600, gt=0)
    """A worker that cannot reach its server for this long exits by itself once it holds no job (``piloop worker
    --orphan-exit``): so it never outlives a stopped server. While the server answers, its drain decides. None: never."""

    @model_validator(mode='after')
    def _ordered(self) -> Provisioner:
        if self.min > self.max:
            raise ValueError(f'min = {self.min} is more than max = {self.max}')
        return self


class Pool(BaseModel):
    """The capacity of each worker in a pool. Kubernetes: a node pool's allocatable, and its eviction thresholds."""

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    agents: Annotated[int | None, Field(ge=0), Live] = None
    """How many agents a worker of the pool runs at the same time. No bound when unset."""
    evaluations: Annotated[int, Field(ge=0), Live] = 2
    """How many evaluations a worker of the pool runs at the same time."""
    min_memory_gb: Annotated[float | None, Live] = None
    """A worker with less memory available takes no job, and its slots count zero."""
    max_load: Annotated[float | None, Live] = None
    """The 1-minute load per CPU above which a worker takes no job, and its slots count zero."""
    runners: list[str] = []
    """The evaluator runners that its workers have, before any of them runs: the evaluations that a provisioned pool
    starts a worker for."""
    provisioner: Provisioner | None = None
    """Start and stop its workers on demand. None: they start by hand (``piloop worker``)."""

    @model_validator(mode='after')
    def _runners(self) -> Pool:
        if self.provisioner is not None and self.evaluations and not self.runners:
            raise ValueError('a pool with a provisioner names the evaluator runners of its workers: runners = [...]')
        return self


def slots(capacity: Pool | Capacity, kind: LeaseKind) -> int | None:
    """The slots of ``kind`` in a pool or a capacity. None: no bound."""
    return {LeaseKind.AGENT: capacity.agents, LeaseKind.EVALUATION: capacity.evaluations}[kind]


def pressure(pool: Pool, host: HostSample) -> NoLease | None:
    """Return why the host of a worker of ``pool`` takes no job of any kind now, or None. The first gate gives it."""
    gates = (
        (NoLease.MEMORY, pool.min_memory_gb is not None and host.memory_available < pool.min_memory_gb * GB),
        (NoLease.LOAD, pool.max_load is not None and host.load1 > pool.max_load * host.cpus),
    )
    return next((reason for reason, refused in gates if refused), None)


def admits(pool: Pool, host: HostSample, running: Capacity, kind: LeaseKind) -> NoLease | None:
    """Return why a worker of ``pool`` takes no job of ``kind`` now, or None when it takes one.

    ``running`` counts the worker's live leases of each kind. The host's ``pressure`` comes first, then the slots.
    """
    if (pressed := pressure(pool, host)) is not None:
        return pressed
    bound = slots(pool, kind)
    return NoLease.SLOTS if bound is not None and (slots(running, kind) or 0) >= bound else None


def add(a: int | None, b: int | None) -> int | None:
    """Add two numbers of slots. None, no bound, absorbs."""
    return None if a is None or b is None else a + b


def less(free: int | None, taken: int) -> int | None:
    """``free`` slots less ``taken``, never below zero. None, no bound, stays None."""
    return None if free is None else max(free - taken, 0)


def times(slots: int | None, workers: int) -> int | None:
    """The slots of ``workers`` workers of ``slots`` each. None, no bound, stays None but for no worker."""
    return 0 if workers == 0 else None if slots is None else slots * workers
