"""Archiving the shared workspace into the CAS, and restoring it: the campaign side of ``piloop.store.tree``.

Agents write to ``<PILOOP_HOME>/projects/<id>/workspace`` and nothing used to delete it: on one machine it grew to
hundreds of gigabytes in millions of files, most of them copies. What the store owes (``due``), from the event feed and
core's tables alone:

- a subject settled (``subject.settled``, for example a done idea): every top-level entry named after it goes into the
  store;
- a campaign stopped and nothing of it runs any more: everything left in its workspace goes.

``archive`` stores a job's files, stores their manifest, checks that the store holds every blob, and records an
``archives`` row. It removes nothing: the local copies stay, so an agent or a human that still reads them finds them.
``prune`` removes them later, file by file where the file still holds what was stored: by age, or under disk pressure
(``make_room``). ``restore`` writes a subject's files, a path or everything back. The archiver decides when
(``archiver``).
"""

import re
import threading
from collections.abc import Collection, Iterable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path, PurePosixPath

from sqlalchemy import Select, exists, func, select
from sqlalchemy.orm import Session

from piloop import deprecated
from piloop.campaign.campaigns import workspace_dir
from piloop.db.events import SubjectSettled, has
from piloop.db.models import ENDED, AgentSession, Campaign, Event, count, utcnow
from piloop.db.session import Database
from piloop.ids import CampaignId, CampaignStatus, TargetKind, TargetRef
from piloop.plugins.archive.contract import CampaignArchived, Restored, Summary
from piloop.plugins.archive.local.tables import Archive
from piloop.plugins.cas.contract import Cas, Integrity, integrity
from piloop.settings import INPUTS_DIR
from piloop.store.tree import (
    DRISL,
    Entry,
    Manifest,
    Missing,
    Removed,
    Walk,
    cid_of,
    evict_unlinked,
    free,
    materialise,
    put_walk,
    remove,
    walk,
)

SUBJECT_NAME = re.compile(r'(?P<kind>[a-z_]+)(?P<id>\d+)(?!\d)')
"""A top-level workspace entry of a subject, ``<kind><id>`` not followed by a digit: ``idea12/``,
``idea12-canary.log``. Implementers are told to name their entries so."""


class NotArchived(RuntimeError):
    """The store lacks blobs of a manifest just stored: no row is recorded."""


class ManifestNotMigrated(RuntimeError):
    """The store could not give an archive's JSON manifest back, so its row still names it. The message says which."""


def subject_of(name: str) -> TargetRef | None:
    """The subject that a top-level workspace entry is named after, or None."""
    m = SUBJECT_NAME.match(name)
    return TargetRef(TargetKind(m['kind']), int(m['id'])) if m and m['kind'] in TargetKind else None


def owners(s: Session, campaign_id: CampaignId, names: Iterable[str]) -> dict[str, TargetRef]:
    """The subject of ``campaign_id`` that each of ``names`` (top-level workspace entries) belongs to; the rest are left out.

    A name whose subject did not settle in the campaign belongs to none, so the campaign's own archiving takes it.
    """
    subjects = {name: subject for name in names if (subject := subject_of(name)) is not None}
    settled = s.execute(
        select(Event.target_kind, Event.target_id).where(has(SubjectSettled, campaign=campaign_id)).distinct()
    )
    known = {TargetRef(kind, target_id) for kind, target_id in settled}
    return {name: subject for name, subject in subjects.items() if subject in known}


@dataclass(frozen=True)
class Job:
    """One archiving: a settled subject's entries, or (``subject`` None) what a stopped campaign left."""

    campaign_id: CampaignId
    subject: TargetRef | None


def _due_subjects() -> Select[CampaignId, TargetKind, int]:
    """``(campaign, subject kind, subject id)`` of every subject due, oldest settled first.

    Due: its latest ``subject.settled`` is newer than its latest complete archiving (a re-run idea is archived again,
    and a job that stopped short is done again).
    """
    campaign = func.json_extract(Event.payload, '$.campaign')
    settled = (
        select(
            campaign.label('campaign_id'),
            Event.target_kind,
            Event.target_id,
            func.max(Event.at).label('at'),
            func.max(Event.id).label('last'),
        )
        .where(has(SubjectSettled))
        .group_by(Event.target_kind, Event.target_id, campaign)
        .subquery()
    )
    archived = (
        select(func.max(Archive.created_at))
        .where(
            Archive.subject_kind == settled.c.target_kind, Archive.subject_id == settled.c.target_id, Archive.complete
        )
        .scalar_subquery()
    )
    return (
        select(settled.c.campaign_id, settled.c.target_kind, settled.c.target_id)
        .join(Campaign, Campaign.id == settled.c.campaign_id)  # an event outlives its campaign, an archiving does not
        .where(archived.is_(None) | (settled.c.at > archived))
        .order_by(settled.c.last)
    )


def _due_campaigns() -> Select[CampaignId]:
    """Every campaign due, oldest first.

    Due: ``stopped``, none of its agents requested or alive, and no complete archiving of it newer than its last agent's
    end (a resumed and stopped again campaign is due again, and so is a job that stopped short).
    """
    live = exists().where(AgentSession.campaign_id == Campaign.id, AgentSession.state.not_in(ENDED))
    last_end = select(func.max(AgentSession.ended_at)).where(AgentSession.campaign_id == Campaign.id).scalar_subquery()
    swept = (
        select(func.max(Archive.created_at))
        .where(Archive.campaign_id == Campaign.id, Archive.subject_kind.is_(None), Archive.complete)
        .scalar_subquery()
    )
    return (
        select(Campaign.id)
        .where(
            Campaign.status.in_((CampaignStatus.STOPPED, CampaignStatus.RETIRED)),
            ~live,
            swept.is_(None) | (last_end > swept),
        )
        .order_by(Campaign.id)
    )


def due(s: Session) -> Job | None:
    """The next archiving the store owes, oldest first; a campaign's own waits until its every settled subject's is.

    Computed from the database alone, so a daemon that stops in the middle of a backlog picks it up where it was: the
    job it cut is done again, and what it had stored is found by hash and not stored twice.
    """
    subject = s.execute(_due_subjects().limit(1)).first()
    if subject is not None:
        return Job(CampaignId(subject.campaign_id), TargetRef(subject.target_kind, subject.target_id))
    campaign = s.scalar(_due_campaigns().limit(1))
    return None if campaign is None else Job(campaign, None)


def summary(s: Session) -> Summary:
    """What the archive holds of each campaign that has an archiving, what is due, and the latest archiving's time."""
    rows = s.execute(
        select(Campaign.name, func.count(Archive.id), func.sum(Archive.files), func.sum(Archive.bytes))
        .join(Archive, Archive.campaign_id == Campaign.id)
        .group_by(Campaign.id)
        .order_by(Campaign.id)
    ).all()
    return Summary(
        campaigns=tuple(CampaignArchived(*row) for row in rows),
        subjects_due=count(s, _due_subjects()),
        campaigns_due=count(s, _due_campaigns()),
        latest=s.scalar(select(func.max(Archive.created_at))),
    )


def _unpruned() -> Select[Archive]:
    """The archivings whose local copies may still be in the workspace, oldest first."""
    return select(Archive).where(Archive.pruned_at.is_(None)).order_by(Archive.id)


def prune(db: Database, cas: Cas, home: Path, archive_id: int) -> Removed:
    """Remove the workspace copies of what an archiving stored, where they still hold what was stored.

    Each blob is checked in the CAS first (``integrity``): a workspace copy of a blob that is missing or corrupt there
    stays. The row records when (``pruned_at``), so an archiving is pruned once, unless a file stayed for such a
    reason or could not be read: then it stays unpruned, and the next prune tries again.
    """
    with db.session() as s:
        row = s.get_one(Archive, archive_id)
        done = Removed()
        if row.manifest is not None:
            manifest = Manifest.get(cas, row.manifest)
            # a workspace copy is the last good one of a blob that went bad in the CAS: it stays
            doubtful = {blob for blob in manifest.blobs if integrity(cas, blob) is not Integrity.OK}
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
    inputs: int = 0
    """The shared copies of linked inputs that ``make_room`` removed (``evict_unlinked``)."""

    def add(self, done: Removed) -> None:
        if not done.left:
            self.archivings += 1
        self.left.update(done.left)


def prune_older(db: Database, cas: Cas, home: Path, than: datetime, stop: threading.Event | None = None) -> Pruned:
    """``prune`` every archiving made before ``than``, until ``stop`` is set."""
    with db.session() as s:
        ids = s.scalars(_unpruned().where(Archive.created_at < than).with_only_columns(Archive.id)).all()
    pruned = Pruned()
    for archive_id in ids:
        if stop is not None and stop.is_set():
            break
        pruned.add(prune(db, cas, home, archive_id))
    return pruned


def make_room(db: Database, cas: Cas, home: Path, reserve: int, stop: threading.Event | None = None) -> Pruned:
    """While the store's disk has less than ``reserve`` bytes free, free room, oldest first: inputs, then archivings.

    It removes the shared copies of linked inputs that no worktree links (``evict_unlinked``), then ``prune``s the next
    archiving. The workspaces, the inputs and the store share the disk of ``PILOOP_HOME``. A copy of an input goes
    first: the CAS holds its blob, and the next job that needs it makes it again. Each archiving is tried once per call,
    so one with a file it cannot read does not stop the others. It ends early once ``stop`` is set.
    """

    def enough() -> bool:
        return free(home) >= reserve or (stop is not None and stop.is_set())

    pruned = Pruned(inputs=evict_unlinked(home / INPUTS_DIR, reserve, stop))
    with db.session() as s:
        ids = s.scalars(_unpruned().with_only_columns(Archive.id)).all()
    for archive_id in ids:
        if enough():
            break
        pruned.add(prune(db, cas, home, archive_id))
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
    cas: Cas,
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
    if job.subject is not None:
        with db.session() as s:
            names = [name for name, subject in owners(s, job.campaign_id, names).items() if subject == job.subject]
    left = walk(workspace, names)
    done = Archived([])
    while True:
        make_room(db, cas, home, reserve + left.size)
        room = free(home)
        batch, left = left.take(room - reserve)
        if left.files and not batch.files:
            held, left = _held(cas, workspace, left)
            done.skipped.update(held.skipped)
            if not held.files:
                return Archived(done.rows, Short(left.size, room, reserve), done.skipped)
            batch = Walk(files=held.files, links=batch.links, skipped=batch.skipped)
        row, skipped = _store_batch(db, cas, workspace, job, batch, stop, complete=not left.files)
        done.rows.append(row)
        done.skipped.update(skipped)
        if not left.files:
            return done


def _held(cas: Cas, workspace: Path, left: Walk) -> tuple[Walk, Walk]:
    """Split ``left`` into the files whose content the store holds already, which cost no room, and the rest.

    Only for a job that does not fit: it reads every file it has left once, to hash it. A file it cannot read is in
    neither part: it goes into the held part's ``skipped``, with why.
    """
    held, rest = Walk(), Walk()
    for rel, st in left.files:
        try:
            known = cas.has(cid_of(workspace / rel))
        except OSError as e:
            held.skipped[rel] = f'not readable ({e.strerror}): fix its permissions'
            continue
        (held if known else rest).files.append((rel, st))
    return held, rest


def _store_batch(
    db: Database, cas: Cas, workspace: Path, job: Job, batch: Walk, stop: threading.Event | None, *, complete: bool
) -> tuple[Archive, dict[str, str]]:
    """Store one batch and record its row. Returns the row, and the paths left out with why."""
    stored = put_walk(cas, workspace, batch, stop)
    row = Archive(
        campaign_id=job.campaign_id,
        subject=job.subject,
        files=len(stored.manifest.entries),
        bytes=stored.size,
        complete=complete,
    )
    if stored.manifest.entries:
        manifest = stored.manifest.put(cas)
        if missing := cas.missing([manifest, *stored.manifest.blobs]):
            raise NotArchived(f'the store lacks {len(missing)} blobs of manifest {manifest}, e.g. {missing[0]}')
        row.manifest = manifest
    with db.session() as s:
        s.add(row)
        s.commit()
    return row, stored.skipped


def _parents(path: str) -> list[str]:
    """The directories that a workspace-relative path is in, innermost first: ``a/b/c`` is in ``a/b`` and ``a``."""
    return [str(p) for p in PurePosixPath(path).parents][:-1]


def restore(s: Session, cas: Cas, home: Path, campaign: Campaign, target: TargetRef | str | None) -> Restored:
    """Write archived files back into the campaign's workspace: a subject's (``target`` a ``TargetRef``), a path's, or all.

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
        newer = {e.path: e for e in Manifest.get(cas, manifest).entries}
        dirs = {parent for path in newer for parent in _parents(path)}
        # a newer file where an older path had a directory, or the reverse, displaces the older path
        latest = {
            path: e for path, e in latest.items() if path not in dirs and not any(p in newer for p in _parents(path))
        } | newer
    entries = list(latest.values())
    if isinstance(target, TargetRef):  # by name alone: an archive outlives a deleted subject
        entries = [e for e in entries if subject_of(e.path.split('/', 1)[0]) == target]
    elif target is not None:
        prefix = target.strip('/')
        entries = [e for e in entries if e.path == prefix or e.path.startswith(f'{prefix}/')]
    done = materialise(cas, Manifest(entries=tuple(entries)), workspace_dir(home, campaign.id))
    return Restored(len(manifests), done)


@deprecated.until(
    '0.4.0',
    'JSON manifests of piloop 0.2: delete migrate_manifests, its callers and ManifestNotMigrated, and make 0.4 refuse a'
    ' home whose archives still name a raw CID (the schema revision does not tell)',
)
def migrate_manifests(db: Database, cas: Cas) -> int:
    """Store each archive's JSON manifest again as a DASL one, and point its row at it. Returns how many.

    ``piloop migrate`` and the daemon's start run it, and it skips the rows done already. The JSON manifest stays in the
    store, which still reads it, so a database restored from its backup still works. It stops at a manifest that the
    store cannot give back (``ManifestNotMigrated``): that row keeps its JSON manifest, and the next run tries again.
    """
    with db.session() as s:
        rows = s.execute(select(Archive.id, Archive.manifest).where(Archive.manifest.is_not(None))).all()
    done = 0
    for archive_id, cid in rows:
        if cid is None or cid.codec == DRISL:
            continue
        try:
            dasl = Manifest.get(cas, cid).put(cas)
        except (Missing, OSError, ValueError) as e:
            raise ManifestNotMigrated(
                f'archive {archive_id}: its manifest {cid} could not be read from the store ({type(e).__name__}: {e}).'
                ' Make the CAS reachable ([plugins.cas] and its credentials), then run `piloop migrate` again'
            ) from e
        with db.session() as s:
            s.get_one(Archive, archive_id).manifest = dasl
            s.commit()
        done += 1
    return done
