"""The daemon's archiver: at most one archiving or upkeep at a time, on the daemon's pool, never blocking the loop.

What is due, and how it is stored, pruned and restored, is ``piloop.campaign.archive``. This module decides when, by
an ``ArchivePolicy`` that the daemon builds from ``[storage]`` and that tests build as they need.
"""

import math
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Literal, NamedTuple

from piloop.actor import System
from piloop.campaign.archive import Job, archive, due, make_room, prune_older
from piloop.db.events import emit
from piloop.db.models import EventModel, utcnow
from piloop.db.session import Database
from piloop.ids import IdeaId, TargetKind, TargetRef
from piloop.otel import Tracer
from piloop.plugins.cas.contract import Cas, Upkeep
from piloop.settings import StorageSettings
from piloop.store.tree import Cancelled


class DiskAnomaly(EventModel):
    """On a campaign: an archiving left files unstored because the store's disk was too full."""

    kind: Literal['anomaly.disk'] = 'anomaly.disk'
    needed: int
    """The bytes that were left."""
    free: int
    reserve: int
    idea: IdeaId | None
    retry_seconds: float
    """When the archiver tries again."""


SKIPPED_EVENT = 'archive.skipped'
"""A span event: the paths that an archiving left out, and why. Only the first ``SKIPPED_SHOWN``."""
SKIPPED_SHOWN = 20
LEFT_EVENT = 'prune.left'
"""A span event: workspace files that a prune left, with why, so their archivings stay unpruned."""


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
    """How often the archiver prunes by age and by disk pressure, and evicts (with a remote), before the next job."""
    live: frozenset[str] = frozenset({'NOTES.md', 'research'})
    """Top-level workspace entries never archived: the small knowledge files that agents and research sweeps read
    (``NOTES.md``, the loss tables in ``research/``) stay where they are; only bulk data goes to the CAS."""

    @classmethod
    def of(cls, storage: StorageSettings) -> ArchivePolicy:
        """The policy that ``[storage]`` sets, with the defaults for the rest."""
        return cls(reserve=storage.reserve)


class Archiving(NamedTuple):
    """What the daemon archives into, and when. The scheduler builds its ``Archiver`` from it."""

    cas: Cas
    policy: ArchivePolicy


class Archiver:
    """Runs the upkeep when it is time, else the next due archiving, one at a time.

    A failed job, or one that the disk was too full for, makes it wait ``policy.retry``. A job that the disk was too
    full for also writes an ``anomaly.disk`` event on its campaign, so at most one per wait.
    """

    def __init__(
        self,
        db: Database,
        cas: Cas,
        home: Path,
        policy: ArchivePolicy,
        tracer: Tracer,
        pool: ThreadPoolExecutor,
        stop: threading.Event,
        ended: Callable[[Future[None]], object],
    ) -> None:
        """``ended``: called when a job ends, so that the daemon runs ``run`` again."""
        self._db, self._cas, self._home, self._policy, self._tracer = db, cas, home, policy, tracer
        self._pool, self._stop, self._ended = pool, stop, ended
        self._job: Future[None] | None = None
        self._paused_until = -math.inf
        self._upkept_at = -math.inf

    def run(self, now: datetime) -> datetime | None:
        """Start the upkeep when it is time, else the next due archiving. Nothing while a job runs or after a failure.

        The upkeep goes first: it is what frees room (pruning, eviction) for a job that the disk was too full for, so a
        job that stays due must not keep it waiting. Returns when to run again: the end of a pause, or the next upkeep.
        A job that ends calls ``ended``, and an idea or a campaign that ends makes an archiving due.
        """
        if self._stop.is_set() or (self._job is not None and not self._job.done()):
            return None
        mono = time.monotonic()
        if mono < self._paused_until:
            return now + timedelta(seconds=self._paused_until - mono)
        upkeep_in = self._upkept_at + self._policy.upkeep_every.total_seconds() - mono
        if upkeep_in <= 0:
            self._submit(self._upkeep)
            return None
        with self._db.session() as s:
            job = due(s)
        if job is not None:
            self._submit(partial(self._archive, job))
            return None
        return now + timedelta(seconds=upkeep_in)

    def _submit(self, body: Callable[[], None]) -> None:
        self._job = self._pool.submit(body)
        self._job.add_done_callback(self._ended)

    def running(self) -> list[Future[None]]:
        """The job in flight, if any: a stopping daemon waits for it before it lets another daemon start."""
        return [self._job] if self._job is not None and not self._job.done() else []

    def _pause(self) -> None:
        self._paused_until = time.monotonic() + self._policy.retry.total_seconds()

    def _archive(self, job: Job) -> None:
        with self._tracer.span('piloop.archive', campaign=job.campaign_id, idea=job.idea_id) as span:
            done = self._guarded(
                span.error,
                lambda: archive(
                    self._db,
                    self._cas,
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
            manifest = rows[-1].manifest if rows else None
            span.set(
                archives=len(rows),
                files=sum(r.files for r in rows),
                bytes=sum(r.bytes for r in rows),
                manifest=None if manifest is None else str(manifest),
                skipped=len(done.skipped),
            )
            if done.skipped:  # left in the workspace: say which and why, so that a human can fix what lasts
                shown = sorted(done.skipped.items())[:SKIPPED_SHOWN]
                span.event(SKIPPED_EVENT, paths='; '.join(f'{path}: {why}' for path, why in shown))
            if done.short is None:
                return
            self._pause()
            self._upkept_at = -math.inf  # after the pause, evict and prune before this job is tried again
            short = DiskAnomaly(
                needed=done.short.needed,
                free=done.short.free,
                reserve=done.short.reserve,
                idea=job.idea_id,
                retry_seconds=self._policy.retry.total_seconds(),
            )
            span.event(short.kind, **short.model_dump(exclude={'kind'}))
            with self._db.session() as s:
                emit(s, short, TargetRef(TargetKind.CAMPAIGN, job.campaign_id), System.DAEMON)
                s.commit()

    def _upkeep(self) -> None:
        try:
            self._prune_and_evict()
        finally:  # counted from the end: a slow upkeep still leaves the next turns to the archive jobs
            self._upkept_at = time.monotonic()

    def _prune_and_evict(self) -> None:
        with self._tracer.span('piloop.store.upkeep') as span:
            older = utcnow() - self._policy.prune_after
            aged = self._guarded(span.error, lambda: prune_older(self._db, self._cas, self._home, older, self._stop))
            pressed = self._guarded(
                span.error, lambda: make_room(self._db, self._cas, self._home, self._policy.reserve, self._stop)
            )
            span.set(
                pruned_by_age=aged.archivings if aged is not None else None,
                pruned_by_pressure=pressed.archivings if pressed is not None else None,
                inputs_evicted=pressed.inputs if pressed is not None else None,
            )
            left = {**(aged.left if aged else {}), **(pressed.left if pressed else {})}
            if left:  # they stay, and their archivings stay unpruned until the next prune can act on them
                shown = sorted(left.items())[:SKIPPED_SHOWN]
                span.event(LEFT_EVENT, paths='; '.join(f'{path}: {why}' for path, why in shown))
            if self._stop.is_set():  # the daemon stops: evicting can wait for the next one
                span.set(stopped=True)
                return
            if (
                isinstance(cas := self._cas, Upkeep)
                and (done := self._guarded(span.error, lambda: cas.flush(evict=True))) is not None
            ):
                span.set(uploaded=done.uploaded, evicted=done.evicted, freed=done.freed)

    def _guarded[T](self, error: Callable[[BaseException], None], step: Callable[[], T]) -> T | None:
        """Run one step of a job. A failure pauses the archiver and goes on the span; None then, and on a stop."""
        try:
            return step()
        except Cancelled:
            return None
        except Exception as e:  # the next pass must not retry at once; the span says what failed
            self._pause()
            error(e)
            return None
