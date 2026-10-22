import os
import time
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import delete, select

from piloop.campaign import archive as archive_module
from piloop.campaign.archive import (
    Archived,
    Backlog,
    Job,
    NotArchived,
    Pruned,
    Short,
    archive,
    backlog,
    due,
    make_room,
    owners,
    prune,
    prune_older,
    restore,
)
from piloop.campaign.campaigns import workspace_dir
from piloop.db.models import (
    AgentSession,
    AgentSessionState,
    Archive,
    Campaign,
    CampaignStatus,
    Idea,
    IdeaStatus,
    RoleName,
    Run,
    RunStatus,
    utcnow,
)
from piloop.db.session import Database
from piloop.store import Store, hash_file, key
from piloop.store.tree import Removed, Stored, Walk

LIVE = frozenset({'NOTES.md', 'research'})


def add_idea(db: Database, campaign_id: int, title: str, status: IdeaStatus = IdeaStatus.DONE) -> int:
    with db.session() as s:
        idea = Idea(campaign_id=campaign_id, track='score', title=title, hypothesis='h', created_by='t', status=status)
        s.add(idea)
        s.commit()
        return idea.id


def add_run(
    db: Database, idea_id: int, status: RunStatus = RunStatus.SUCCEEDED, kind: RoleName = RoleName.IMPLEMENTER
) -> int:
    """A run of ``idea_id`` and the implementer that ran it; the implementer's id."""
    with db.session() as s:
        idea = s.get_one(Idea, idea_id)
        finished = utcnow() if status not in (RunStatus.QUEUED, RunStatus.RUNNING) else None
        run = Run(
            idea_id=idea_id,
            attempt=len(idea.runs) + 1,
            base_candidate_id=1,
            harness='script',
            model='m',
            status=status,
            finished_at=finished,
        )
        s.add(run)
        s.flush()
        state = AgentSessionState.EXITED if finished is not None else AgentSessionState.RUNNING
        agent_session = AgentSession(
            kind=kind, campaign_id=idea.campaign_id, run_id=run.id, state=state, ended_at=finished, harness='script'
        )
        s.add(agent_session)
        s.commit()
        return agent_session.id


def write(path: Path, data: bytes | str = b'x') -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data if isinstance(data, bytes) else data.encode())
    return path


def files(root: Path) -> dict[str, bytes]:
    return {str(p.relative_to(root)): p.read_bytes() for p in sorted(root.rglob('*')) if p.is_file()}


@pytest.fixture
def home(tmp_path: Path) -> Path:
    return tmp_path / 'piloop-home'


@pytest.fixture
def store(home: Path) -> Store:
    return Store(home / 'store')


def test_an_entry_belongs_to_an_idea_by_its_name(db: Database, campaign: Campaign) -> None:
    idea = add_idea(db, campaign.id, 'one')
    other = add_idea(db, campaign.id, 'two')
    agent_session = add_run(db, idea)
    with db.session() as s:
        alien = Campaign(name='alien', path='/alien', config={})
        s.add(alien)
        s.flush()
        stranger = Idea(campaign_id=alien.id, track='t', title='x', hypothesis='h', created_by='t')
        s.add(stranger)
        s.commit()
        stranger_id = stranger.id
    names = [
        f'idea{idea}',
        f'idea{idea}-canary.log',
        f'idea{other}',
        f'idea{idea}0',
        f'w{agent_session}',
        f'implementer-{agent_session}',
        f'idea{stranger_id}',
        'research-31',
        'NOTES.md',
    ]
    with db.session() as s:
        assert owners(s, campaign.id, names) == {
            f'idea{idea}': idea,
            f'idea{idea}-canary.log': idea,
            f'idea{other}': other,
        }, 'idea<id>0 is another idea, an idea of another campaign is not this one, an implementer name is no idea'


def test_an_idea_is_due_once_done_and_again_after_a_new_run(db: Database, campaign: Campaign) -> None:
    with db.session() as s:
        assert due(s) is None
    first = add_idea(db, campaign.id, 'first')
    add_run(db, first)
    running = add_idea(db, campaign.id, 'running', IdeaStatus.DONE)
    add_run(db, running, RunStatus.RUNNING)
    add_idea(db, campaign.id, 'ready', IdeaStatus.READY)
    with db.session() as s:
        assert due(s) == Job(campaign.id, first), 'an open run holds an idea back, whatever its status'
        s.add(Archive(campaign_id=campaign.id, idea_id=first))
        s.commit()
        assert due(s) is None
    time.sleep(0.01)
    add_run(db, first)
    with db.session() as s:
        assert due(s) == Job(campaign.id, first), 'its new run finished after the archiving'


def test_a_stopped_campaign_is_due_once_its_ideas_are_and_nothing_of_it_runs(db: Database, campaign: Campaign) -> None:
    idea = add_idea(db, campaign.id, 'idea')
    agent_session = add_run(db, idea)
    with db.session() as s:
        s.get_one(Campaign, campaign.id).status = CampaignStatus.STOPPED
        s.add(AgentSession(kind=RoleName.WATCHDOG, campaign_id=campaign.id, reason='campaign stopped'))
        s.commit()
        assert due(s) == Job(campaign.id, idea), 'its done ideas first'
        assert backlog(s) == Backlog(ideas=1, campaigns=0), 'the closing watchdog is still requested'
        s.add(Archive(campaign_id=campaign.id, idea_id=idea))
        s.commit()
        assert due(s) is None, 'the closing watchdog is still requested'
        for w in s.scalars(select(AgentSession).where(AgentSession.id != agent_session)):
            w.state, w.ended_at = AgentSessionState.EXITED, utcnow()
        s.commit()
        assert due(s) == Job(campaign.id, None) and backlog(s) == Backlog(ideas=0, campaigns=1)
        s.add(Archive(campaign_id=campaign.id))
        s.commit()
        assert due(s) is None
    time.sleep(0.01)
    with db.session() as s:
        s.add(
            AgentSession(
                kind=RoleName.WATCHDOG, campaign_id=campaign.id, state=AgentSessionState.EXITED, ended_at=utcnow()
            )
        )
        s.commit()
        assert due(s) == Job(campaign.id, None), 'resumed and stopped again: what it wrote since is archived'


def archive_job(db: Database, store: Store, home: Path, job: Job, reserve: int = 0) -> Archived:
    return archive(db, store, home, job, live=LIVE, reserve=reserve)


def test_an_archiving_stores_and_keeps_the_files_a_prune_removes_them_and_restore_brings_back_any_part(
    db: Database, campaign: Campaign, home: Path, store: Store
) -> None:
    idea = add_idea(db, campaign.id, 'idea')
    parked = add_idea(db, campaign.id, 'parked', IdeaStatus.PARKED)
    add_run(db, idea)
    workspace = workspace_dir(home, campaign.id)
    write(workspace / f'idea{idea}' / 'arrays' / 'a.npy', os.urandom(5000))
    write(workspace / f'idea{idea}' / 'copy.npy', (workspace / f'idea{idea}' / 'arrays' / 'a.npy').read_bytes())
    write(workspace / f'idea{idea}-fresh.log', 'ok\n')
    (workspace / f'idea{idea}' / 'latest').symlink_to('copy.npy')
    write(workspace / f'idea{parked}' / 'notes.md', '# parked\n')
    write(workspace / 'research' / 'losses.md', '# Losses\n')
    write(workspace / 'NOTES.md', '# Notes\n')
    write(workspace / 'boost465' / 'run.bin', b'\x00' * 100)
    before = files(workspace)
    live = {k: v for k, v in before.items() if k.startswith(('NOTES.md', 'research/'))}

    [row] = archive_job(db, store, home, Job(campaign.id, idea)).rows
    assert (row.idea_id, row.files, row.bytes, row.pruned_at) == (idea, 4, 2 * 5000 + 3, None)
    assert row.manifest is not None and not store.missing(store.manifest(row.manifest).blobs)
    assert files(workspace) == before, 'storing removes nothing'
    write(workspace / f'idea{idea}-fresh.log', 'rewritten since\n')
    assert prune(db, store, home, row.id) == Removed(3), 'the rewritten log stays'
    with db.session() as s:
        assert s.get_one(Archive, row.id).pruned_at is not None
    assert sorted(p.name for p in workspace.iterdir()) == [
        'NOTES.md',
        'boost465',
        f'idea{idea}-fresh.log',
        f'idea{parked}',
        'research',
    ]
    write(workspace / f'idea{idea}-fresh.log', 'ok\n')

    [rest] = archive_job(db, store, home, Job(campaign.id, None)).rows
    assert (rest.idea_id, rest.files) == (None, 3)
    prune(db, store, home, rest.id)
    assert files(workspace) == live, 'NOTES.md and research/ are never archived'
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        got = restore(s, store, home, p, idea)
        assert (got.archives, got.done.written) == (2, 4), 'its own files, from every archiving'
        assert files(workspace) == live | {k: v for k, v in before.items() if k.startswith(f'idea{idea}')}
        assert os.readlink(workspace / f'idea{idea}' / 'latest') == 'copy.npy'
        assert restore(s, store, home, p, 'boost465/').done.written == 1
        assert restore(s, store, home, p, 'boost465/run.bin').done.same == 1
        assert restore(s, store, home, p, None).done.written == 1
        assert files(workspace) == before
        assert restore(s, store, home, p, 999).done.written == 0


def test_nothing_to_archive_is_recorded_too(db: Database, campaign: Campaign, home: Path, store: Store) -> None:
    idea = add_idea(db, campaign.id, 'empty')
    add_run(db, idea)
    [row] = archive_job(db, store, home, Job(campaign.id, idea)).rows
    assert (row.manifest, row.files, row.bytes) == (None, 0, 0)
    assert prune(db, store, home, row.id) == Removed(0)
    with db.session() as s:
        assert due(s) is None
        assert restore(s, store, home, s.get_one(Campaign, campaign.id), None).archives == 0


def test_no_row_when_the_store_does_not_hold_every_blob(
    db: Database, campaign: Campaign, home: Path, store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    idea = add_idea(db, campaign.id, 'idea')
    add_run(db, idea)
    path = write(workspace_dir(home, campaign.id) / f'idea{idea}' / 'x.npy')
    monkeypatch.setattr(Store, 'missing', lambda self, hashes: ['lost'])
    with pytest.raises(NotArchived, match='the store lacks 1 blobs of manifest .*, e.g. lost'):
        archive_job(db, store, home, Job(campaign.id, idea))
    assert path.exists()
    with db.session() as s:
        assert s.scalars(select(Archive)).all() == []


def archived_idea(db: Database, campaign: Campaign, home: Path, store: Store, title: str) -> tuple[int, int]:
    """A done idea with one 1000-byte file, archived: its id and its archiving's."""
    idea = add_idea(db, campaign.id, title)
    add_run(db, idea)
    write(workspace_dir(home, campaign.id) / f'idea{idea}' / 'x.npy', os.urandom(1000))
    [row] = archive_job(db, store, home, Job(campaign.id, idea)).rows
    return idea, row.id


def test_local_copies_go_by_age_and_under_disk_pressure_the_oldest_archiving_first(
    db: Database, campaign: Campaign, home: Path, store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, old_row = archived_idea(db, campaign, home, store, 'old')
    mid, _ = archived_idea(db, campaign, home, store, 'mid')
    new, _ = archived_idea(db, campaign, home, store, 'new')
    with db.session() as s:
        s.get_one(Archive, old_row).created_at = utcnow() - timedelta(days=8)
        s.commit()
    workspace = workspace_dir(home, campaign.id)
    assert prune_older(db, store, home, utcnow() - timedelta(days=7)) == Pruned(1)
    assert sorted(p.name for p in workspace.iterdir()) == [f'idea{mid}', f'idea{new}']
    assert prune_older(db, store, home, utcnow() - timedelta(days=7)) == Pruned(0), 'once'

    monkeypatch.setattr(Store, 'free', lambda self: 10_000 - 1000 * len(list(workspace.rglob('*.npy'))))
    assert make_room(db, store, home, reserve=8000) == Pruned(0), '8000 free already'
    assert make_room(db, store, home, reserve=9000) == Pruned(1)
    assert sorted(p.name for p in workspace.iterdir()) == [f'idea{new}'], 'the older of the two went'
    assert make_room(db, store, home, reserve=99_000) == Pruned(1), 'it stops when nothing is left to prune'
    with db.session() as s:
        assert all(a.pruned_at is not None for a in s.scalars(select(Archive)))


def big_idea(db: Database, campaign: Campaign, home: Path) -> tuple[int, dict[str, bytes]]:
    """A done idea whose workspace holds three 4000-byte files in nested directories and a symlink."""
    idea = add_idea(db, campaign.id, 'big')
    add_run(db, idea)
    workspace = workspace_dir(home, campaign.id)
    for name in ('a/one.npy', 'a/b/two.npy', 'three.npy'):
        write(workspace / f'idea{idea}' / name, os.urandom(4000))
    (workspace / f'idea{idea}' / 'latest').symlink_to('three.npy')
    return idea, files(workspace)


def disk(home: Path, size: int) -> int:
    """Free bytes of a disk of ``size`` that holds the workspaces' arrays and the store's blobs."""
    used = sum(
        p.stat().st_size for p in home.rglob('*') if p.is_file() and (p.suffix == '.npy' or p.name == 'bytes.zst')
    )
    return size - used


def test_a_workspace_larger_than_the_room_above_the_reserve_is_stored_in_batches_pruning_in_between(
    db: Database, campaign: Campaign, home: Path, store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    idea, before = big_idea(db, campaign, home)
    workspace = workspace_dir(home, campaign.id)
    monkeypatch.setattr(Store, 'free', lambda self: disk(home, 12_000 + 1000 + 9000))
    on_disk: list[int] = []
    real_put = archive_module.put_walk

    def counting(store: Store, root: Path, found: Walk, stop: object = None) -> Stored:
        on_disk.append(len(list(workspace.rglob('*.npy'))))
        return real_put(store, root, found)

    monkeypatch.setattr(archive_module, 'put_walk', counting)
    done = archive_job(db, store, home, Job(campaign.id, idea), reserve=1000)
    assert done.short is None
    assert [(r.files, r.bytes) for r in done.rows] == [(3, 8000), (1, 4000)], 'two files fit, then the third'
    assert on_disk == [3, 1], 'the first batch is pruned to make room for the second'
    with db.session() as s:
        assert [a.pruned_at is not None for a in s.scalars(select(Archive).order_by(Archive.id))] == [True, False]
        assert due(s) is None
        assert restore(s, store, home, s.get_one(Campaign, campaign.id), idea).done.written == 3, 'two arrays, the link'
    assert files(workspace) == before


def test_what_does_not_fit_with_nothing_left_to_prune_is_short_and_stays_for_the_campaign_stop(
    db: Database, campaign: Campaign, home: Path, store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    idea, before = big_idea(db, campaign, home)
    monkeypatch.setattr(Store, 'free', lambda self: disk(home, 12_000 + 1100))
    done = archive_job(db, store, home, Job(campaign.id, idea), reserve=1000)
    assert done == Archived([], Short(needed=12_000, free=1100, reserve=1000))
    assert files(workspace_dir(home, campaign.id)) == before
    with db.session() as s:
        assert due(s) == Job(campaign.id, idea), 'no batch stored: still due'
        s.get_one(Campaign, campaign.id).status = CampaignStatus.STOPPED
        s.commit()
    monkeypatch.setattr(Store, 'free', lambda self: 1 << 40)
    assert [r.files for r in archive_job(db, store, home, Job(campaign.id, idea), reserve=1000).rows] == [4]


def test_a_job_cut_after_some_batches_stays_due_and_its_next_run_stores_the_rest(
    db: Database, campaign: Campaign, home: Path, store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The first batch of a job marked the whole job as done."""
    idea, before = big_idea(db, campaign, home)
    taken: list[int] = []  # what other programs write meanwhile, in bytes
    monkeypatch.setattr(Store, 'free', lambda self: disk(home, 12_000 + 1000 + 9000) - sum(taken))
    real_put = archive_module.put_walk

    def filling(store: Store, root: Path, found: Walk, stop: object = None) -> Stored:
        stored = real_put(store, root, found)
        taken.append(1 << 30)  # the disk fills up after the first batch
        return stored

    monkeypatch.setattr(archive_module, 'put_walk', filling)
    done = archive_job(db, store, home, Job(campaign.id, idea), reserve=1000)
    assert [(r.files, r.complete) for r in done.rows] == [(3, False)] and done.short is not None
    with db.session() as s:
        assert due(s) == Job(campaign.id, idea), 'one incomplete batch: the job is still due'
    taken.clear()
    monkeypatch.setattr(archive_module, 'put_walk', real_put)
    rest = archive_job(db, store, home, Job(campaign.id, idea), reserve=1000).rows
    assert [r.complete for r in rest] == [True]
    with db.session() as s:
        assert due(s) is None
        restore(s, store, home, s.get_one(Campaign, campaign.id), idea)
    assert files(workspace_dir(home, campaign.id)) == before


def test_an_idea_deleted_since_its_archiving_is_still_restored_by_its_id(
    db: Database, campaign: Campaign, home: Path, store: Store
) -> None:
    """Restore by id asked for a live idea row, and wrote nothing."""
    idea, row = archived_idea(db, campaign, home, store, 'short-lived')
    prune(db, store, home, row)
    with db.session() as s:
        s.execute(delete(Run).where(Run.idea_id == idea))
        s.execute(delete(Idea).where(Idea.id == idea))
        s.commit()
        assert restore(s, store, home, s.get_one(Campaign, campaign.id), idea).done.written == 1


def test_a_newer_archiving_displaces_an_older_file_or_directory_at_the_same_path(
    db: Database, campaign: Campaign, home: Path, store: Store
) -> None:
    """`results` a file in one archiving and a directory in a later one."""
    workspace = workspace_dir(home, campaign.id)
    for body in ('a file', None):
        if body is not None:
            write(workspace / 'results', body)
        else:
            (workspace / 'results').unlink(missing_ok=True)  # pruned already
            write(workspace / 'results' / 'output.npy', 'array')
        [row] = archive_job(db, store, home, Job(campaign.id, None)).rows
        prune(db, store, home, row.id)
        with db.session() as s:  # the next stop of the campaign
            s.add(
                AgentSession(
                    kind=RoleName.WATCHDOG, campaign_id=campaign.id, state=AgentSessionState.EXITED, ended_at=utcnow()
                )
            )
            s.commit()
        time.sleep(0.01)
    with db.session() as s:
        done = restore(s, store, home, s.get_one(Campaign, campaign.id), None).done
    assert done.written == 1 and files(workspace) == {'results/output.npy': b'array'}


def test_a_file_a_prune_cannot_read_keeps_its_archiving_unpruned(
    db: Database, campaign: Campaign, home: Path, store: Store
) -> None:
    """The archiving was marked pruned, and never pruned again."""
    _, row = archived_idea(db, campaign, home, store, 'locked')
    [path] = workspace_dir(home, campaign.id).rglob('*.npy')
    path.chmod(0)
    try:
        assert list(prune(db, store, home, row).left) == [str(path.relative_to(workspace_dir(home, campaign.id)))]
        with db.session() as s:
            assert s.get_one(Archive, row).pruned_at is None, 'tried again next time'
        pruned = prune_older(db, store, home, utcnow() + timedelta(days=1))
        assert (pruned.archivings, len(pruned.left)) == (0, 1), 'not counted as pruned'
    finally:
        path.chmod(0o644)
    assert prune(db, store, home, row) == Removed(1)


def test_content_the_store_holds_already_is_archived_even_when_nothing_else_fits(
    db: Database, campaign: Campaign, home: Path, store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A copy of a stored blob was budgeted by its full size and stayed short."""
    idea, _ = big_idea(db, campaign, home)
    workspace = workspace_dir(home, campaign.id)
    for path in workspace.rglob('*.npy'):
        store.put_path(path)  # an earlier archiving stored the same content
    monkeypatch.setattr(Store, 'free', lambda self: 1000)
    done = archive_job(db, store, home, Job(campaign.id, idea), reserve=1000)
    assert done.short is None and [(r.files, r.complete) for r in done.rows] == [(4, True)], 'three arrays, one link'


def test_a_workspace_copy_of_a_blob_that_went_bad_in_the_store_is_not_pruned(
    db: Database, campaign: Campaign, home: Path, store: Store
) -> None:
    """The intact workspace copy was deleted, leaving only a corrupt blob."""
    _, row = archived_idea(db, campaign, home, store, 'rotting')
    [path] = workspace_dir(home, campaign.id).rglob('*.npy')
    (store.root / key(hash_file(path).hash)).write_bytes(b'rot')
    done = prune(db, store, home, row)
    assert done.removed == 0 and list(done.left.values()) == [
        'its blob in the store is missing or corrupt: run piloop store verify'
    ]
    assert path.exists()
    with db.session() as s:
        assert s.get_one(Archive, row).pruned_at is None


def test_a_file_that_cannot_be_read_when_nothing_fits_is_reported_with_why(
    db: Database, campaign: Campaign, home: Path, store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    idea, _ = big_idea(db, campaign, home)
    [locked, *_] = sorted(workspace_dir(home, campaign.id).rglob('*.npy'))
    locked.chmod(0)
    monkeypatch.setattr(Store, 'free', lambda self: 1000)
    try:
        done = archive_job(db, store, home, Job(campaign.id, idea), reserve=1000)
    finally:
        locked.chmod(0o644)
    rel = str(locked.relative_to(workspace_dir(home, campaign.id)))
    assert done.rows == [] and done.short == Short(needed=8000, free=1000, reserve=1000)
    assert done.skipped == {rel: 'not readable (Permission denied): fix its permissions'}, 'said, with why'
