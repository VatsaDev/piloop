"""Archiving the shared workspace into the artifact store, and restoring it: the campaign side of ``piloop.store``.

Agents write to ``<PILOOP_HOME>/projects/<id>/workspace`` and nothing used to delete it: on one machine it grew to
hundreds of gigabytes in millions of files, most of them copies. The daemon now moves it into the store in two moments (``due``):

- an idea is done (its last run judged): every top-level entry that belongs to it (``owner``) goes;
- a project stopped and nothing of it runs any more: everything left in its workspace goes.

``NOTES.md`` and ``research/`` are never archived (``LIVE``). Each archiving stores the files, stores their manifest,
checks that the store holds every blob, records an ``archives`` row, and only then removes the local files that are
unchanged since they were read (``tree.prune``). ``restore`` writes an idea's files, a path or everything back. A top-level entry belongs to an idea when it is named
``idea<id>…`` or after a worker of one of its runs (``w<worker id>…``, ``worker<worker id>…``, ``worker-<worker id>…``),
by the conventions agents use; anything else waits for the campaign to stop.
"""

import math
import re
import threading
import time
from collections.abc import Iterable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from sqlalchemy import Select, exists, func, select
from sqlalchemy.orm import Session

from piloop.campaign.project import WORKSPACE
from piloop.db.models import (
    ENDED,
    OPEN_RUNS,
    Archive,
    Idea,
    IdeaStatus,
    Project,
    ProjectStatus,
    Run,
    Worker,
    WorkerKind,
    count,
)
from piloop.db.session import Database
from piloop.otel import Tracer
from piloop.settings import GB, Settings
from piloop.store import Entry, Manifest, Store
from piloop.store.tree import Cancelled, Materialised, materialise, prune, put_tree

RETRY_SECONDS = 600
"""After a failed archiving the daemon archives nothing for this long: a full disk or an unreachable remote is not
retried every tick."""
EVICT_SECONDS = 3600
"""How often the daemon applies ``[storage]``'s eviction, while no archiving is due: it scans the whole store."""
LIVE = frozenset({'NOTES.md', 'research'})
"""Top-level workspace entries never archived: the small knowledge files agents and research sweeps read
(``NOTES.md``, the loss tables in ``research/``) stay where they are; only bulk data goes to the store."""
IDEA_NAME = re.compile(r'idea(\d+)(?!\d)')
WORKER_NAME = re.compile(r'(?:w|worker-?)(\d+)(?!\d)')


class NotArchived(RuntimeError):
    """The store lacks blobs of a manifest just stored: nothing is removed."""


def open_store(cfg: Settings) -> Store:
    """The store of this PILOOP_HOME as ``[storage]`` configures it."""
    storage = cfg.storage
    remote = None
    if storage.remote is not None:
        from piloop.store.s3 import S3  # here: boto3 is imported only when a remote is configured

        remote = S3(storage.remote, storage_class=storage.storage_class, standard_below=storage.standard_below)
    return Store(
        cfg.store_dir,
        remote,
        evict_after=timedelta(days=storage.evict_after_days) if storage.evict_after_days is not None else None,
        max_local_bytes=int(storage.local_max_gb * GB) if storage.local_max_gb is not None else None,
    )


def workspace_of(home: Path, project_id: int) -> Path:
    """The project's shared workspace, symlinked into every agent's worktree as ``./workspace``."""
    return home / 'projects' / str(project_id) / WORKSPACE


def owners(s: Session, project_id: int, names: Iterable[str]) -> dict[str, int]:
    """The idea each of ``names`` (top-level workspace entries) belongs to; names that belong to none are left out."""
    ideas: dict[str, int] = {}
    workers: dict[str, int] = {}
    for name in names:
        if m := IDEA_NAME.match(name):
            ideas[name] = int(m[1])
        elif m := WORKER_NAME.match(name):
            workers[name] = int(m[1])
    known = set(s.scalars(select(Idea.id).where(Idea.project_id == project_id, Idea.id.in_(set(ideas.values())))).all())
    run_of: Mapping[int, int] = dict(
        s.execute(
            select(Worker.id, Run.idea_id)
            .join(Run, Worker.run_id == Run.id)
            .where(
                Worker.project_id == project_id,
                Worker.kind == WorkerKind.WORKER,
                Worker.id.in_(set(workers.values())),
            )
        ).all()
    )
    found = {name: idea for name, idea in ideas.items() if idea in known}
    return found | {name: run_of[w] for name, w in workers.items() if w in run_of}


@dataclass(frozen=True)
class Job:
    """One archiving: a done idea's entries, or (``idea_id`` None) what a stopped project left."""

    project_id: int
    idea_id: int | None


def _due_ideas() -> Select[int, int]:
    """``(project, idea)`` of every idea due, oldest first.

    Due: ``done``, no run of it open, and its latest run finished after its latest archiving (a re-run idea is archived
    again).
    """
    open_run = exists().where(Run.idea_id == Idea.id, Run.status.in_(OPEN_RUNS))
    last_run = select(func.max(Run.finished_at)).where(Run.idea_id == Idea.id).scalar_subquery()
    archived = select(func.max(Archive.created_at)).where(Archive.idea_id == Idea.id).scalar_subquery()
    return (
        select(Idea.project_id, Idea.id)
        .where(Idea.status == IdeaStatus.DONE, ~open_run, archived.is_(None) | (last_run > archived))
        .order_by(Idea.id)
    )


def _due_projects() -> Select[int]:
    """Every project due, oldest first.

    Due: ``stopped``, none of its agents requested or alive, and no archiving of it newer than its last agent's end (a
    resumed and stopped again campaign is due again).
    """
    live = exists().where(Worker.project_id == Project.id, Worker.state.not_in(ENDED))
    last_end = select(func.max(Worker.ended_at)).where(Worker.project_id == Project.id).scalar_subquery()
    swept = (
        select(func.max(Archive.created_at))
        .where(Archive.project_id == Project.id, Archive.idea_id.is_(None))
        .scalar_subquery()
    )
    return (
        select(Project.id)
        .where(Project.status == ProjectStatus.STOPPED, ~live, swept.is_(None) | (last_end > swept))
        .order_by(Project.id)
    )


def due(s: Session) -> Job | None:
    """The next archiving the store owes, oldest first; a project's own waits until its every done idea is archived.

    Computed from the database alone, so a daemon that stops in the middle of a backlog picks it up where it was: the
    job it cut is done again, and what it had stored is found by hash and not stored twice.
    """
    idea = s.execute(_due_ideas().limit(1)).first()
    if idea is not None:
        return Job(idea.project_id, idea.id)
    project = s.scalar(_due_projects().limit(1))
    return None if project is None else Job(project, None)


@dataclass(frozen=True)
class Backlog:
    """What the store still owes, for ``piloop store status``."""

    ideas: int
    projects: int


def backlog(s: Session) -> Backlog:
    """How many ideas and projects are due."""
    return Backlog(count(s, _due_ideas()), count(s, _due_projects()))


def archive(db: Database, store: Store, home: Path, job: Job, stop: threading.Event | None = None) -> Archive:
    """Archive ``job``'s entries of the workspace: store them, record the row, then remove the local copies."""
    workspace = workspace_of(home, job.project_id)
    names = [entry.name for entry in workspace.iterdir() if entry.name not in LIVE] if workspace.is_dir() else []
    if job.idea_id is not None:
        with db.session() as s:
            names = [name for name, idea in owners(s, job.project_id, names).items() if idea == job.idea_id]
    stored = put_tree(store, workspace, names, stop)
    row = Archive(project_id=job.project_id, idea_id=job.idea_id, files=len(stored.manifest.entries), bytes=stored.size)
    if stored.manifest.entries:
        manifest = store.put_manifest(stored.manifest).hash
        if missing := store.missing([manifest, *stored.manifest.blobs]):
            raise NotArchived(f'the store lacks {len(missing)} blobs of manifest {manifest}, e.g. {missing[0]}')
        row.manifest = manifest
    with db.session() as s:
        s.add(row)
        s.commit()
    prune(workspace, stored)
    return row


@dataclass(frozen=True)
class Restored:
    """What ``restore`` wrote back, from how many archivings."""

    archives: int
    done: Materialised


def restore(s: Session, store: Store, home: Path, project: Project, target: int | str | None) -> Restored:
    """Write archived files back into the project's workspace: an idea's (``target`` an id), a path's, or all.

    A path is workspace-relative: the file itself or everything under it. When several archivings hold a path, the
    latest wins. A file already there with other content is kept, and listed.
    """
    rows = s.scalars(
        select(Archive).where(Archive.project_id == project.id, Archive.manifest.is_not(None)).order_by(Archive.id)
    ).all()
    latest: dict[str, Entry] = {}
    for row in rows:
        assert row.manifest is not None
        latest |= {e.path: e for e in store.manifest(row.manifest).entries}
    entries = list(latest.values())
    if isinstance(target, int):
        idea_of = owners(s, project.id, {e.path.split('/', 1)[0] for e in entries})
        entries = [e for e in entries if idea_of.get(e.path.split('/', 1)[0]) == target]
    elif target is not None:
        prefix = target.strip('/')
        entries = [e for e in entries if e.path == prefix or e.path.startswith(f'{prefix}/')]
    done = materialise(store, Manifest(tuple(entries)), workspace_of(home, project.id))
    return Restored(len(rows), done)


class Archiver:
    """The daemon's side: at most one archiving (or eviction) at a time, on the daemon's pool, never blocking a tick."""

    def __init__(
        self, db: Database, store: Store, home: Path, tracer: Tracer, pool: ThreadPoolExecutor, stop: threading.Event
    ) -> None:
        self._db, self._store, self._home, self._tracer = db, store, home, tracer
        self._pool, self._stop = pool, stop
        self._job: Future[None] | None = None
        self._failed_at = -math.inf
        self._evicted_at = -math.inf

    def tick(self) -> None:
        """Start the next due archiving once the previous one ended; with nothing due, the eviction when it is time."""
        if self._stop.is_set() or (self._job is not None and not self._job.done()):
            return
        now = time.monotonic()
        if now - self._failed_at < RETRY_SECONDS:
            return
        with self._db.session() as s:
            job = due(s)
        if job is not None:
            self._job = self._pool.submit(self._archive, job)
        elif self._store.remote is not None and now - self._evicted_at >= EVICT_SECONDS:
            self._evicted_at = now
            self._job = self._pool.submit(self._evict)

    def _archive(self, job: Job) -> None:
        with self._tracer.span('piloop.archive', project=job.project_id, idea=job.idea_id) as span:
            try:
                row = archive(self._db, self._store, self._home, job, self._stop)
            except Cancelled:
                span.set(cancelled=True)
                return
            except Exception as e:  # the next tick must not retry at once; the span says what failed
                self._failed_at = time.monotonic()
                span.error(e)
                return
            span.set(files=row.files, bytes=row.bytes, manifest=row.manifest)

    def _evict(self) -> None:
        with self._tracer.span('piloop.store.evict') as span:
            try:
                done = self._store.evict()
            except Exception as e:  # as in _archive
                self._failed_at = time.monotonic()
                span.error(e)
                return
            span.set(uploaded=done.uploaded, evicted=done.evicted, freed=done.freed)
