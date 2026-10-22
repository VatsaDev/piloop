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
from pydantic import BaseModel, ConfigDict, Field

from piloop.db.models import LeaseKind, NoLease, utcnow
from piloop.plugins.executor.contract import Capacity

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


class Pool(BaseModel):
    """The capacity of each worker in a pool. Kubernetes: a node pool's allocatable, and its eviction thresholds."""

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    agents: Annotated[int | None, Field(ge=0)] = None
    """How many agents a worker of the pool runs at the same time. No bound when unset."""
    evaluations: Annotated[int, Field(ge=0)] = 2
    """How many evaluations a worker of the pool runs at the same time."""
    min_memory_gb: float | None = None
    """A worker with less memory available takes no job, and its slots count zero."""
    max_load: float | None = None
    """The 1-minute load per CPU above which a worker takes no job, and its slots count zero."""


def slots(capacity: Pool | Capacity, kind: LeaseKind) -> int | None:
    """The slots of ``kind`` in a pool or a capacity. None: no bound."""
    return {LeaseKind.AGENT: capacity.agents, LeaseKind.EVALUATION: capacity.evaluations}[kind]


def admits(pool: Pool, host: HostSample, running: Capacity, kind: LeaseKind) -> NoLease | None:
    """Return why a worker of ``pool`` takes no job of ``kind`` now, or None when it takes one.

    ``running`` counts the worker's live leases of each kind. The first gate that refuses gives the reason.
    """
    bound = slots(pool, kind)
    gates = (
        (NoLease.MEMORY, pool.min_memory_gb is not None and host.memory_available < pool.min_memory_gb * GB),
        (NoLease.LOAD, pool.max_load is not None and host.load1 > pool.max_load * host.cpus),
        (NoLease.SLOTS, bound is not None and (slots(running, kind) or 0) >= bound),
    )
    return next((reason for reason, refused in gates if refused), None)
