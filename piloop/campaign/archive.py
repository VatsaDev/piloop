"""Archiving the shared workspace into the artifact store, and restoring it: the campaign side of ``piloop.store``.

Agents write to ``<PILOOP_HOME>/projects/<id>/workspace`` and nothing used to delete it: on one machine it grew to
hundreds of gigabytes in millions of files, most of them copies. What the store owes (``due``):

- an idea is done (its last run judged): every top-level entry named ``idea<id>…`` goes into the store;
- a project stopped and nothing of it runs any more: everything left in its workspace goes.

``archive`` stores a job's files, stores their manifest, checks that the store holds every blob, and records an
``archives`` row. It removes nothing: the local copies stay, so an agent or a human that still reads them finds them.
``prune`` removes them later, file by file where the file still holds what was stored: by age, or under disk pressure
(``make_room``). ``restore`` writes an idea's files, a path or everything back. The daemon decides when
(``piloop.daemon.archiver``).
"""

import re
import threading
from collections.abc import Collection, Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from sqlalchemy import Select, exists, func, select
from sqlalchemy.orm import Session

from piloop.campaign.project import workspace_dir
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
    count,
    utcnow,
)
from piloop.db.session import Database
from piloop.store import Entry, Manifest, Store
from piloop.store.tree import Materialised, Walk, materialise, put_walk, remove, walk

IDEA_NAME = re.compile(r'idea(\d+)(?!\d)')
"""A top-level workspace entry of an idea: ``idea12/``, ``idea12-canary.log``. Workers are told to use it."""


class NotArchived(RuntimeError):
    """The store lacks blobs of a manifest just stored: no row is recorded."""


def owners(s: Session, project_id: int, names: Iterable[str]) -> dict[str, int]:
    """The idea each of ``names`` (top-level workspace entries) belongs to; names that belong to none are left out."""
    ideas = {name: int(m[1]) for name in names if (m := IDEA_NAME.match(name))}
    known = set(s.scalars(select(Idea.id).where(Idea.project_id == project_id, Idea.id.in_(set(ideas.values())))))
    return {name: idea for name, idea in ideas.items() if idea in known}


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


def _unpruned() -> Select[Archive]:
    """The archivings whose local copies may still be in the workspace, oldest first."""
    return select(Archive).where(Archive.pruned_at.is_(None)).order_by(Archive.id)


def prune(db: Database, store: Store, home: Path, archive_id: int) -> int:
    """Remove the workspace copies of what an archiving stored, where they still hold what was stored.

    The row records when (``pruned_at``), so an archiving is pruned once. Returns the paths removed.
    """
    with db.session() as s:
        row = s.get_one(Archive, archive_id)
        removed = 0
        if row.manifest is not None:
            removed = remove(workspace_dir(home, row.project_id), store.manifest(row.manifest))
        row.pruned_at = utcnow()
        s.commit()
    return removed


def prune_older(db: Database, store: Store, home: Path, than: datetime) -> int:
    """``prune`` every archiving made before ``than``. Returns the archivings pruned."""
    with db.session() as s:
        ids = s.scalars(_unpruned().where(Archive.created_at < than).with_only_columns(Archive.id)).all()
    for archive_id in ids:
        prune(db, store, home, archive_id)
    return len(ids)


def make_room(db: Database, store: Store, home: Path, reserve: int) -> int:
    """While the store's disk has less than ``reserve`` bytes free, ``prune`` the oldest archiving left.

    The workspaces and the store share the disk of ``PILOOP_HOME``. Returns the archivings pruned.
    """
    pruned = 0
    while store.free() < reserve:
        with db.session() as s:
            oldest = s.scalar(_unpruned().with_only_columns(Archive.id).limit(1))
        if oldest is None:
            break
        prune(db, store, home, oldest)
        pruned += 1
    return pruned


@dataclass(frozen=True)
class Short:
    """The disk was too full to store what was left of a job: its bytes, the bytes free and the reserve."""

    needed: int
    free: int
    reserve: int


@dataclass(frozen=True)
class Archived:
    """What ``archive`` did: one row per batch, and ``short`` when some files did not fit and were not stored."""

    rows: list[Archive]
    short: Short | None = None


def archive(
    db: Database,
    store: Store,
    home: Path,
    job: Job,
    *,
    live: Collection[str],
    reserve: int,
    stop: threading.Event | None = None,
) -> Archived:
    """Store ``job``'s entries of the workspace, except the top-level ``live`` ones, keeping ``reserve`` bytes free.

    Storing writes each file's compressed copy, so the disk needs room for it. Before each batch, ``make_room`` prunes
    older archivings, oldest first, until the disk has room for the rest of the job above ``reserve``, or there is
    nothing left to prune. A batch holds the files whose sizes (what their compressed copies take at most) fit in the
    free space minus ``reserve``. It is stored, gets its manifest and the store check, and its ``archives`` row. A
    workspace that fits is one batch and one row.

    When not one more file fits, the rest is not stored, and ``short`` says so. With no batch done, the job has no row
    and stays due. With some done, the idea is archived as far as it went, and the rest goes when the campaign stops
    (the project's own job takes everything left).
    """
    workspace = workspace_dir(home, job.project_id)
    names = [entry.name for entry in workspace.iterdir() if entry.name not in live] if workspace.is_dir() else []
    if job.idea_id is not None:
        with db.session() as s:
            names = [name for name, idea in owners(s, job.project_id, names).items() if idea == job.idea_id]
    left = walk(workspace, names)
    rows: list[Archive] = []
    while True:
        make_room(db, store, home, reserve + left.size)
        free = store.free()
        batch, left = left.take(free - reserve)
        if left.files and not batch.files:
            return Archived(rows, Short(left.size, free, reserve))
        rows.append(_store_batch(db, store, workspace, job, batch, stop))
        if not left.files:
            return Archived(rows)


def _store_batch(
    db: Database, store: Store, workspace: Path, job: Job, batch: Walk, stop: threading.Event | None
) -> Archive:
    """Store one batch and record its row."""
    stored = put_walk(store, workspace, batch, stop)
    row = Archive(project_id=job.project_id, idea_id=job.idea_id, files=len(stored.manifest.entries), bytes=stored.size)
    if stored.manifest.entries:
        manifest = store.put_manifest(stored.manifest).hash
        if missing := store.missing([manifest, *stored.manifest.blobs]):
            raise NotArchived(f'the store lacks {len(missing)} blobs of manifest {manifest}, e.g. {missing[0]}')
        row.manifest = manifest
    with db.session() as s:
        s.add(row)
        s.commit()
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
    manifests = s.scalars(
        select(Archive.manifest)
        .where(Archive.project_id == project.id, Archive.manifest.is_not(None))
        .order_by(Archive.id)
    ).all()
    latest: dict[str, Entry] = {}
    for manifest in manifests:
        if manifest is not None:
            latest |= {e.path: e for e in store.manifest(manifest).entries}
    entries = list(latest.values())
    if isinstance(target, int):
        idea_of = owners(s, project.id, {e.path.split('/', 1)[0] for e in entries})
        entries = [e for e in entries if idea_of.get(e.path.split('/', 1)[0]) == target]
    elif target is not None:
        prefix = target.strip('/')
        entries = [e for e in entries if e.path == prefix or e.path.startswith(f'{prefix}/')]
    done = materialise(store, Manifest(tuple(entries)), workspace_dir(home, project.id))
    return Restored(len(manifests), done)
