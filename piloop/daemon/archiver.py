"""The daemon's archiver: at most one archiving or upkeep at a time, on the daemon's pool, never blocking a tick.

What is due, and how it is stored, pruned and restored, is ``piloop.campaign.archive``. This module decides when, by
an ``ArchivePolicy`` that the daemon builds from ``[storage]`` and that tests build as they need.
"""

import math
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import NamedTuple

from piloop.campaign.archive import Job, archive, due, make_room, prune_older
from piloop.db.events import emit
from piloop.db.models import TargetKind, utcnow
from piloop.db.session import Database
from piloop.otel import Tracer
from piloop.settings import GB, StorageSettings
from piloop.store import Store
from piloop.store.tree import Cancelled

DISK_ANOMALY = 'anomaly.disk'
"""An event on a project: an archiving left files unstored because the store's disk was too full."""


@dataclass(frozen=True)
class ArchivePolicy:
    """When the archiver archives, prunes and evicts."""

    reserve: int
    """Bytes kept free on the store's disk (``[storage] min_free_gb``)."""
    prune_after: timedelta = timedelta(days=7)
    """The age of an archiving past which its workspace copies are removed, whatever the free space."""
    retry: timedelta = timedelta(minutes=10)
    """How long nothing is archived after a failed job, or one that the disk was too full for."""
    upkeep_every: timedelta = timedelta(hours=1)
    """How often, while nothing is due, the archiver prunes by age and by disk pressure, and evicts (with a remote)."""
    live: frozenset[str] = frozenset({'NOTES.md', 'research'})
    """Top-level workspace entries never archived: the small knowledge files that agents and research sweeps read
    (``NOTES.md``, the loss tables in ``research/``) stay where they are; only bulk data goes to the store."""

    @classmethod
    def of(cls, storage: StorageSettings) -> ArchivePolicy:
        """The policy that ``[storage]`` sets, with the defaults for the rest."""
        return cls(reserve=int(storage.min_free_gb * GB))


class Archiving(NamedTuple):
    """What the daemon archives into, and when. The scheduler builds its ``Archiver`` from it."""

    store: Store
    policy: ArchivePolicy


class Archiver:
    """Starts the next due archiving once the previous one ended; with nothing due, the upkeep when it is time.

    A failed job, or one that the disk was too full for, makes it wait ``policy.retry``. A job that the disk was too
    full for also writes an ``anomaly.disk`` event on its project, so at most one per wait.
    """

    def __init__(
        self,
        db: Database,
        store: Store,
        home: Path,
        policy: ArchivePolicy,
        tracer: Tracer,
        pool: ThreadPoolExecutor,
        stop: threading.Event,
    ) -> None:
        self._db, self._store, self._home, self._policy, self._tracer = db, store, home, policy, tracer
        self._pool, self._stop = pool, stop
        self._job: Future[None] | None = None
        self._paused_until = -math.inf
        self._upkept_at = -math.inf

    def tick(self) -> None:
        """Start the next due archiving, else the upkeep when it is time. Nothing while a job runs or after a failure."""
        if self._stop.is_set() or (self._job is not None and not self._job.done()):
            return
        now = time.monotonic()
        if now < self._paused_until:
            return
        with self._db.session() as s:
            job = due(s)
        if job is not None:
            self._job = self._pool.submit(self._archive, job)
        elif now - self._upkept_at >= self._policy.upkeep_every.total_seconds():
            self._upkept_at = now
            self._job = self._pool.submit(self._upkeep)

    def _pause(self) -> None:
        self._paused_until = time.monotonic() + self._policy.retry.total_seconds()

    def _archive(self, job: Job) -> None:
        with self._tracer.span('piloop.archive', project=job.project_id, idea=job.idea_id) as span:
            done = self._guarded(
                span.error,
                lambda: archive(
                    self._db,
                    self._store,
                    self._home,
                    job,
                    live=self._policy.live,
                    reserve=self._policy.reserve,
                    stop=self._stop,
                ),
            )
            if done is None:
                return
            rows = done.rows
            span.set(
                archives=len(rows),
                files=sum(r.files for r in rows),
                bytes=sum(r.bytes for r in rows),
                manifest=rows[-1].manifest if rows else None,
            )
            if done.short is None:
                return
            self._pause()
            short = {
                'needed': done.short.needed,
                'free': done.short.free,
                'reserve': done.short.reserve,
                'idea': job.idea_id,
                'retry_seconds': self._policy.retry.total_seconds(),
            }
            span.event(DISK_ANOMALY, **short)
            with self._db.session() as s:
                emit(s, DISK_ANOMALY, TargetKind.PROJECT, job.project_id, payload=dict(short))
                s.commit()

    def _upkeep(self) -> None:
        with self._tracer.span('piloop.store.upkeep') as span:
            older = utcnow() - self._policy.prune_after
            aged = self._guarded(span.error, lambda: prune_older(self._db, self._store, self._home, older))
            pressed = self._guarded(
                span.error, lambda: make_room(self._db, self._store, self._home, self._policy.reserve)
            )
            span.set(pruned_by_age=aged, pruned_by_pressure=pressed)
            if self._store.remote is not None and (done := self._guarded(span.error, self._store.evict)) is not None:
                span.set(uploaded=done.uploaded, evicted=done.evicted, freed=done.freed)

    def _guarded[T](self, error: Callable[[BaseException], None], step: Callable[[], T]) -> T | None:
        """Run one step of a job. A failure pauses the archiver and goes on the span; None then, and on a stop."""
        try:
            return step()
        except Cancelled:
            return None
        except Exception as e:  # the next tick must not retry at once; the span says what failed
            self._pause()
            error(e)
            return None
