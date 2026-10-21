"""Archiving the shared workspace into the artifact store, and restoring it: the campaign side of ``piloop.store``.

Agents write to ``<PILOOP_HOME>/projects/<id>/workspace`` and nothing used to delete it: on one machine it grew to
hundreds of gigabytes in millions of files, most of them copies. What the store owes (``due``):

- an idea is done (its last run judged): every top-level entry named ``idea<id>…`` goes into the store;
- a campaign stopped and nothing of it runs any more: everything left in its workspace goes.

``archive`` stores a job's files, stores their manifest, checks that the store holds every blob, and records an
``archives`` row. It removes nothing: the local copies stay, so an agent or a human that still reads them finds them.
``prune`` removes them later, file by file where the file still holds what was stored: by age, or under disk pressure
(``make_room``). ``restore`` writes an idea's files, a path or everything back. The daemon decides when
(``piloop.daemon.archiver``).
"""

import re
import threading
from collections.abc import Collection, Iterable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path, PurePosixPath

from sqlalchemy import Select, exists, func, select
from sqlalchemy.orm import Session

from piloop.campaign.campaigns import workspace_dir
from piloop.db.models import (
    ENDED,
    OPEN_RUNS,
    AgentSession,
    Archive,
    Campaign,
    CampaignStatus,
    Idea,
    IdeaStatus,
    Run,
    count,
    utcnow,
)
from piloop.db.session import Database
from piloop.store import Entry, Manifest, Store, hash_file
from piloop.store.tree import Materialised, Removed, Walk, materialise, put_walk, remove, walk

IDEA_NAME = re.compile(r'idea(\d+)(?!\d)')
"""A top-level workspace entry of an idea: ``idea12/``, ``idea12-canary.log``. Implementers are told to use it."""


class NotArchived(RuntimeError):
    """The store lacks blobs of a manifest just stored: no row is recorded."""


def idea_of(name: str) -> int | None:
    """The idea id that a top-level workspace entry is named after, or None."""
    m = IDEA_NAME.match(name)
    return int(m[1]) if m else None


def owners(s: Session, campaign_id: int, names: Iterable[str]) -> dict[str, int]:
    """The idea of ``campaign_id`` that each of ``names`` (top-level workspace entries) belongs to; the rest are left out.

    A name of an idea that is not in the campaign belongs to none, so the campaign's stop takes it.
    """
    ideas = {name: idea for name in names if (idea := idea_of(name)) is not None}
    known = set(s.scalars(select(Idea.id).where(Idea.campaign_id == campaign_id, Idea.id.in_(set(ideas.values())))))
    return {name: idea for name, idea in ideas.items() if idea in known}


@dataclass(frozen=True)
class Job:
    """One archiving: a done idea's entries, or (``idea_id`` None) what a stopped campaign left."""

    campaign_id: int
    idea_id: int | None


def _due_ideas() -> Select[int, int]:
    """``(campaign, idea)`` of every idea due, oldest first.

    Due: ``done``, no run of it open, and its latest run finished after its latest complete archiving (a re-run idea is
    archived again, and a job that stopped short is done again).
    """
    open_run = exists().where(Run.idea_id == Idea.id, Run.status.in_(OPEN_RUNS))
    last_run = select(func.max(Run.finished_at)).where(Run.idea_id == Idea.id).scalar_subquery()
    archived = (
        select(func.max(Archive.created_at)).where(Archive.idea_id == Idea.id, Archive.complete).scalar_subquery()
    )
    return (
        select(Idea.campaign_id, Idea.id)
        .where(Idea.status == IdeaStatus.DONE, ~open_run, archived.is_(None) | (last_run > archived))
        .order_by(Idea.id)
    )


def _due_campaigns() -> Select[int]:
    """Every campaign due, oldest first.

    Due: ``stopped``, none of its agents requested or alive, and no complete archiving of it newer than its last agent's
    end (a resumed and stopped again campaign is due again, and so is a job that stopped short).
    """
    live = exists().where(AgentSession.campaign_id == Campaign.id, AgentSession.state.not_in(ENDED))
    last_end = select(func.max(AgentSession.ended_at)).where(AgentSession.campaign_id == Campaign.id).scalar_subquery()
    swept = (
        select(func.max(Archive.created_at))
        .where(Archive.campaign_id == Campaign.id, Archive.idea_id.is_(None), Archive.complete)
        .scalar_subquery()
    )
    return (
        select(Campaign.id)
        .where(Campaign.status == CampaignStatus.STOPPED, ~live, swept.is_(None) | (last_end > swept))
        .order_by(Campaign.id)
    )


def due(s: Session) -> Job | None:
    """The next archiving the store owes, oldest first; a campaign's own waits until its every done idea is archived.

    Computed from the database alone, so a daemon that stops in the middle of a backlog picks it up where it was: the
    job it cut is done again, and what it had stored is found by hash and not stored twice.
    """
    idea = s.execute(_due_ideas().limit(1)).first()
    if idea is not None:
        return Job(idea.campaign_id, idea.id)
    campaign = s.scalar(_due_campaigns().limit(1))
    return None if campaign is None else Job(campaign, None)


@dataclass(frozen=True)
class Backlog:
    """What the store still owes, for ``piloop store status``."""

    ideas: int
    campaigns: int


def backlog(s: Session) -> Backlog:
    """How many ideas and campaigns are due."""
    return Backlog(count(s, _due_ideas()), count(s, _due_campaigns()))


def _unpruned() -> Select[Archive]:
    """The archivings whose local copies may still be in the workspace, oldest first."""
    return select(Archive).where(Archive.pruned_at.is_(None)).order_by(Archive.id)


def prune(db: Database, store: Store, home: Path, archive_id: int) -> Removed:
    """Remove the workspace copies of what an archiving stored, where they still hold what was stored.

    Each blob is checked in the store first (``Store.check``): a workspace copy of a blob that is missing or corrupt
    there stays. The row records when (``pruned_at``), so an archiving is pruned once, unless a file stayed for such a
    reason or could not be read: then it stays unpruned, and the next prune tries again.
    """
    with db.session() as s:
        row = s.get_one(Archive, archive_id)
        done = Removed()
        if row.manifest is not None:
            manifest = store.manifest(row.manifest)
            # a workspace copy is the last good one of a blob that went bad in the store: it stays
            doubtful = {blob for blob in manifest.blobs if store.check(blob) != 'ok'}
            done = remove(workspace_dir(home, row.campaign_id), manifest, keep=doubtful)
        if not done.left:
            row.pruned_at = utcnow()
        s.commit()
    return done


@dataclass
class Pruned:
    """What a round of ``prune`` did: the archivings pruned, and the paths it left to try again, each with why."""

    archivings: int = 0
    left: dict[str, str] = field(default_factory=dict)

    def add(self, done: Removed) -> None:
        if not done.left:
            self.archivings += 1
        self.left.update(done.left)


def prune_older(db: Database, store: Store, home: Path, than: datetime, stop: threading.Event | None = None) -> Pruned:
    """``prune`` every archiving made before ``than``, until ``stop`` is set."""
    with db.session() as s:
        ids = s.scalars(_unpruned().where(Archive.created_at < than).with_only_columns(Archive.id)).all()
    pruned = Pruned()
    for archive_id in ids:
        if stop is not None and stop.is_set():
            break
        pruned.add(prune(db, store, home, archive_id))
    return pruned


def make_room(db: Database, store: Store, home: Path, reserve: int, stop: threading.Event | None = None) -> Pruned:
    """While the store's disk has less than ``reserve`` bytes free, ``prune`` the next archiving, oldest first.

    The workspaces and the store share the disk of ``PILOOP_HOME``. Each archiving is tried once per call, so one with a
    file it cannot read does not stop the others. It ends early once ``stop`` is set.
    """
    with db.session() as s:
        ids = s.scalars(_unpruned().with_only_columns(Archive.id)).all()
    pruned = Pruned()
    for archive_id in ids:
        if store.free() >= reserve or (stop is not None and stop.is_set()):
            break
        pruned.add(prune(db, store, home, archive_id))
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
    skipped: dict[str, str] = field(default_factory=dict)
    """Workspace path → why it was not stored (``piloop.store.tree.Stored.skipped``). It stays in the workspace."""


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
    workspace that fits is one batch and one row. When no file fits, the files whose content the store holds already
    still go, since they take no room.

    Only the last batch of a job that stored everything is ``complete``. When not one more file fits, the rest is not
    stored, and ``short`` says so. A job that stops short (no room, a failure, a stop of the daemon) has no complete
    row, so it stays due, and its next run stores the rest: what was stored already is found by hash.
    """
    workspace = workspace_dir(home, job.campaign_id)
    names = [entry.name for entry in workspace.iterdir() if entry.name not in live] if workspace.is_dir() else []
    if job.idea_id is not None:
        with db.session() as s:
            names = [name for name, idea in owners(s, job.campaign_id, names).items() if idea == job.idea_id]
    left = walk(workspace, names)
    done = Archived([])
    while True:
        make_room(db, store, home, reserve + left.size)
        free = store.free()
        batch, left = left.take(free - reserve)
        if left.files and not batch.files:
            held, left = _held(store, workspace, left)
            done.skipped.update(held.skipped)
            if not held.files:
                return Archived(done.rows, Short(left.size, free, reserve), done.skipped)
            batch = Walk(files=held.files, links=batch.links, skipped=batch.skipped)
        row, skipped = _store_batch(db, store, workspace, job, batch, stop, complete=not left.files)
        done.rows.append(row)
        done.skipped.update(skipped)
        if not left.files:
            return done


def _held(store: Store, workspace: Path, left: Walk) -> tuple[Walk, Walk]:
    """Split ``left`` into the files whose content the store holds already, which cost no room, and the rest.

    Only for a job that does not fit: it reads every file it has left once, to hash it. A file it cannot read is in
    neither part: it goes into the held part's ``skipped``, with why.
    """
    held, rest = Walk(), Walk()
    for rel, st in left.files:
        try:
            known = store.has(hash_file(workspace / rel).hash)
        except OSError as e:
            held.skipped[rel] = f'not readable ({e.strerror}): fix its permissions'
            continue
        (held if known else rest).files.append((rel, st))
    return held, rest


def _store_batch(
    db: Database, store: Store, workspace: Path, job: Job, batch: Walk, stop: threading.Event | None, *, complete: bool
) -> tuple[Archive, dict[str, str]]:
    """Store one batch and record its row. Returns the row, and the paths left out with why."""
    stored = put_walk(store, workspace, batch, stop)
    row = Archive(
        campaign_id=job.campaign_id,
        idea_id=job.idea_id,
        files=len(stored.manifest.entries),
        bytes=stored.size,
        complete=complete,
    )
    if stored.manifest.entries:
        manifest = store.put_manifest(stored.manifest).hash
        if missing := store.missing([manifest, *stored.manifest.blobs]):
            raise NotArchived(f'the store lacks {len(missing)} blobs of manifest {manifest}, e.g. {missing[0]}')
        row.manifest = manifest
    with db.session() as s:
        s.add(row)
        s.commit()
    return row, stored.skipped


def _parents(path: str) -> list[str]:
    """The directories that a workspace-relative path is in, innermost first: ``a/b/c`` is in ``a/b`` and ``a``."""
    return [str(p) for p in PurePosixPath(path).parents][:-1]


@dataclass(frozen=True)
class Restored:
    """What ``restore`` wrote back, from how many archivings."""

    archives: int
    done: Materialised


def restore(s: Session, store: Store, home: Path, campaign: Campaign, target: int | str | None) -> Restored:
    """Write archived files back into the campaign's workspace: an idea's (``target`` an id), a path's, or all.

    A path is workspace-relative: the file itself or everything under it. When several archivings hold a path, the
    latest wins, and a newer file where an older one had a directory (or the reverse) displaces the older paths. A file
    already there with other content is kept, and listed.
    """
    manifests = s.scalars(
        select(Archive.manifest)
        .where(Archive.campaign_id == campaign.id, Archive.manifest.is_not(None))
        .order_by(Archive.id)
    ).all()
    latest: dict[str, Entry] = {}
    for manifest in manifests:
        assert manifest is not None  # the query has only the archivings that stored something
        newer = {e.path: e for e in store.manifest(manifest).entries}
        dirs = {parent for path in newer for parent in _parents(path)}
        # a newer file where an older path had a directory, or the reverse, displaces the older path
        latest = {
            path: e for path, e in latest.items() if path not in dirs and not any(p in newer for p in _parents(path))
        } | newer
    entries = list(latest.values())
    if isinstance(target, int):  # by name alone: an archive outlives a deleted idea
        entries = [e for e in entries if idea_of(e.path.split('/', 1)[0]) == target]
    elif target is not None:
        prefix = target.strip('/')
        entries = [e for e in entries if e.path == prefix or e.path.startswith(f'{prefix}/')]
    done = materialise(store, Manifest(tuple(entries)), workspace_dir(home, campaign.id))
    return Restored(len(manifests), done)
