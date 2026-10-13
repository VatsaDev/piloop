import errno
import math
import os
import threading
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path

import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from sqlalchemy import select

from piloop import settings as settings_module
from piloop.campaign import archive as archive_module
from piloop.campaign.archive import (
    DISK_ANOMALY,
    MAX_RETRY_SECONDS,
    RETRY_SECONDS,
    Archived,
    Archiver,
    Backlog,
    Job,
    NotArchived,
    Short,
    archive,
    backlog,
    due,
    open_store,
    owners,
    restore,
    workspace_of,
)
from piloop.db.models import (
    Archive,
    Event,
    Idea,
    IdeaStatus,
    Project,
    ProjectStatus,
    Run,
    RunStatus,
    TargetKind,
    Worker,
    WorkerKind,
    WorkerState,
    utcnow,
)
from piloop.db.session import Database
from piloop.otel import Tracer
from piloop.settings import GB, Settings, StorageSettings
from piloop.store import Blob, Store
from piloop.store.s3 import S3
from piloop.store.tree import Stored, Walk
from tests.conftest import events
from tests.store.test_store import DirRemote


def add_idea(db: Database, project_id: int, title: str, status: IdeaStatus = IdeaStatus.DONE) -> int:
    with db.session() as s:
        idea = Idea(project_id=project_id, track='score', title=title, hypothesis='h', created_by='t', status=status)
        s.add(idea)
        s.commit()
        return idea.id


def add_run(
    db: Database, idea_id: int, status: RunStatus = RunStatus.SUCCEEDED, kind: WorkerKind = WorkerKind.WORKER
) -> int:
    """A run of ``idea_id`` and the worker that ran it; the worker's id."""
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
        state = WorkerState.EXITED if finished is not None else WorkerState.RUNNING
        worker = Worker(
            kind=kind, project_id=idea.project_id, run_id=run.id, state=state, ended_at=finished, harness='script'
        )
        s.add(worker)
        s.commit()
        return worker.id


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


def test_entries_belong_to_an_idea_by_its_name_or_by_a_worker_of_its_runs(db: Database, project: Project) -> None:
    idea = add_idea(db, project.id, 'one')
    other = add_idea(db, project.id, 'two')
    worker = add_run(db, idea)
    watchdog = add_run(db, other, kind=WorkerKind.WATCHDOG)
    with db.session() as s:
        alien = Project(name='alien', path='/alien', config={})
        s.add(alien)
        s.flush()
        stranger = Idea(project_id=alien.id, track='t', title='x', hypothesis='h', created_by='t')
        s.add(stranger)
        s.commit()
        stranger_id = stranger.id
    names = [
        f'idea{idea}',
        f'idea{idea}-canary.log',
        f'idea{other}',
        f'idea{idea}0',
        f'w{worker}',
        f'w{worker}-control.log',
        f'worker-{worker}',
        f'worker{worker}-compare.py',
        f'w{watchdog}',
        f'idea{stranger_id}',
        'wavelet480',
        'research-31',
        'NOTES.md',
    ]
    with db.session() as s:
        assert owners(s, project.id, names) == {
            f'idea{idea}': idea,
            f'idea{idea}-canary.log': idea,
            f'idea{other}': other,
            f'w{worker}': idea,
            f'w{worker}-control.log': idea,
            f'worker-{worker}': idea,
            f'worker{worker}-compare.py': idea,
        }, 'idea<id>0 is another idea, a watchdog has no idea, an idea of another project is not this one'


def test_an_idea_is_due_once_done_and_again_after_a_new_run(db: Database, project: Project) -> None:
    with db.session() as s:
        assert due(s) is None
    first = add_idea(db, project.id, 'first')
    add_run(db, first)
    running = add_idea(db, project.id, 'running', IdeaStatus.DONE)
    add_run(db, running, RunStatus.RUNNING)
    add_idea(db, project.id, 'ready', IdeaStatus.READY)
    with db.session() as s:
        assert due(s) == Job(project.id, first), 'an open run holds an idea back, whatever its status'
        s.add(Archive(project_id=project.id, idea_id=first))
        s.commit()
        assert due(s) is None
    time.sleep(0.01)
    add_run(db, first)
    with db.session() as s:
        assert due(s) == Job(project.id, first), 'its new run finished after the archiving'


def test_a_stopped_project_is_due_once_its_ideas_are_and_nothing_of_it_runs(db: Database, project: Project) -> None:
    idea = add_idea(db, project.id, 'idea')
    worker = add_run(db, idea)
    with db.session() as s:
        s.get_one(Project, project.id).status = ProjectStatus.STOPPED
        s.add(Worker(kind=WorkerKind.WATCHDOG, project_id=project.id, reason='campaign stopped'))
        s.commit()
        assert due(s) == Job(project.id, idea), 'its done ideas first'
        assert backlog(s) == Backlog(ideas=1, projects=0), 'the closing watchdog is still requested'
        s.add(Archive(project_id=project.id, idea_id=idea))
        s.commit()
        assert due(s) is None, 'the closing watchdog is still requested'
        for w in s.scalars(select(Worker).where(Worker.id != worker)):
            w.state, w.ended_at = WorkerState.EXITED, utcnow()
        s.commit()
        assert due(s) == Job(project.id, None) and backlog(s) == Backlog(ideas=0, projects=1)
        s.add(Archive(project_id=project.id))
        s.commit()
        assert due(s) is None
    time.sleep(0.01)
    with db.session() as s:
        s.add(Worker(kind=WorkerKind.WATCHDOG, project_id=project.id, state=WorkerState.EXITED, ended_at=utcnow()))
        s.commit()
        assert due(s) == Job(project.id, None), 'resumed and stopped again: what it wrote since is archived'


def test_a_done_idea_is_archived_then_the_rest_at_stop_and_restore_brings_back_any_part(
    db: Database, project: Project, home: Path, store: Store
) -> None:
    idea = add_idea(db, project.id, 'idea')
    worker = add_run(db, idea)
    parked = add_idea(db, project.id, 'parked', IdeaStatus.PARKED)
    workspace = workspace_of(home, project.id)
    write(workspace / f'idea{idea}' / 'arrays' / 'a.npy', os.urandom(5000))
    write(workspace / f'idea{idea}' / 'copy.npy', (workspace / f'idea{idea}' / 'arrays' / 'a.npy').read_bytes())
    write(workspace / f'idea{idea}-fresh.log', 'ok\n')
    write(workspace / f'w{worker}' / 'ledger.json', '{"units": []}')
    (workspace / f'w{worker}' / 'latest').symlink_to(f'../idea{idea}/copy.npy')
    write(workspace / f'idea{parked}' / 'notes.md', '# parked\n')
    write(workspace / 'research' / 'losses.md', '# Losses\n')
    write(workspace / 'NOTES.md', '# Notes\n')
    write(workspace / 'boost465' / 'run.bin', b'\x00' * 100)
    before = files(workspace)
    live = {k: v for k, v in before.items() if k.startswith(('NOTES.md', 'research/'))}

    [row] = archive(db, store, home, Job(project.id, idea)).rows
    assert (row.idea_id, row.files, row.bytes) == (idea, 5, 2 * 5000 + 3 + 13)
    assert row.manifest is not None and not store.missing(store.manifest(row.manifest).blobs)
    assert sorted(p.name for p in workspace.iterdir()) == ['NOTES.md', 'boost465', f'idea{parked}', 'research']

    [rest] = archive(db, store, home, Job(project.id, None)).rows
    assert (rest.idea_id, rest.files) == (None, 2)
    assert files(workspace) == live, 'NOTES.md and research/ are never archived'
    with db.session() as s:
        assert [a.id for a in s.scalars(select(Archive))] == [row.id, rest.id]
        p = s.get_one(Project, project.id)
        got = restore(s, store, home, p, idea)
        assert (got.archives, got.done.written) == (2, 5), 'its own files, from every archiving'
        mine = {k: v for k, v in before.items() if k.startswith((f'idea{idea}', f'w{worker}'))}
        assert files(workspace) == live | mine
        assert os.readlink(workspace / f'w{worker}' / 'latest') == f'../idea{idea}/copy.npy'
        assert restore(s, store, home, p, 'boost465/').done.written == 1
        assert restore(s, store, home, p, 'boost465/run.bin').done.same == 1
        assert restore(s, store, home, p, None).done.written == 1
        assert files(workspace) == before
        assert restore(s, store, home, p, 999).done.written == 0


def test_nothing_to_archive_is_recorded_too(db: Database, project: Project, home: Path, store: Store) -> None:
    idea = add_idea(db, project.id, 'empty')
    add_run(db, idea)
    [row] = archive(db, store, home, Job(project.id, idea)).rows
    assert (row.manifest, row.files, row.bytes) == (None, 0, 0)
    with db.session() as s:
        assert due(s) is None
        assert restore(s, store, home, s.get_one(Project, project.id), None).archives == 0


def test_files_stay_when_the_store_does_not_hold_every_blob(
    db: Database, project: Project, home: Path, store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    idea = add_idea(db, project.id, 'idea')
    add_run(db, idea)
    path = write(workspace_of(home, project.id) / f'idea{idea}' / 'x.npy')
    monkeypatch.setattr(Store, 'missing', lambda self, hashes: ['lost'])
    with pytest.raises(NotArchived, match='the store lacks 1 blobs of manifest .*, e.g. lost'):
        archive(db, store, home, Job(project.id, idea))
    assert path.exists()
    with db.session() as s:
        assert s.scalars(select(Archive)).all() == []


def test_the_store_follows_the_storage_settings(tmp_path: Path) -> None:
    local = open_store(Settings(home=tmp_path))
    assert (local.root, local.remote, local.evict_after) == (tmp_path / 'store', None, None)
    storage = StorageSettings(remote='s3://b/p', evict_after_days=0, local_max_gb=2, standard_below=10)
    cfg = Settings(home=tmp_path, storage=storage)
    staging = open_store(cfg)
    assert isinstance(staging.remote, S3) and staging.remote.standard_below == 10
    assert staging.staging and staging.max_local_bytes == 2 * 1024**3


@pytest.fixture
def pool() -> Iterator[ThreadPoolExecutor]:
    with ThreadPoolExecutor(1) as executor:
        yield executor


def settle(archiver: Archiver) -> None:
    job = archiver._job
    assert job is not None
    job.result(timeout=30)


def test_the_archiver_runs_one_job_at_a_time_and_waits_after_a_failure(
    db: Database,
    project: Project,
    home: Path,
    store: Store,
    tracer: Tracer,
    spans: InMemorySpanExporter,
    pool: ThreadPoolExecutor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first, second = add_idea(db, project.id, 'first'), add_idea(db, project.id, 'second')
    add_run(db, first)
    add_run(db, second)
    write(workspace_of(home, project.id) / f'idea{first}' / 'a.txt', 'a')
    archiver = Archiver(db, store, home, tracer, pool, threading.Event())
    gate = threading.Event()
    real = archive_module.archive

    def held(
        db: Database, store: Store, home: Path, job: Job, stop: threading.Event | None = None, *, reserve: int = 0
    ) -> Archived:
        gate.wait(10)
        return real(db, store, home, job, stop, reserve=reserve)

    monkeypatch.setattr(archive_module, 'archive', held)
    archiver.tick()
    archiver.tick()  # the first is still running: no second job
    gate.set()
    settle(archiver)
    archiver.tick()
    settle(archiver)
    done = [s for s in spans.get_finished_spans() if s.name == 'piloop.archive']
    assert [(s.attributes or {}).get('idea') for s in done] == [first, second]
    assert (done[0].attributes or {}).get('files') == 1

    third = add_idea(db, project.id, 'third')
    add_run(db, third)

    def broken(
        db: Database, store: Store, home: Path, job: Job, stop: threading.Event | None = None, *, reserve: int = 0
    ) -> Archived:
        raise OSError('No space left on device')

    monkeypatch.setattr(archive_module, 'archive', broken)
    archiver.tick()
    settle(archiver)
    failed = [s for s in spans.get_finished_spans() if s.name == 'piloop.archive'][-1]
    assert failed.status.description == 'OSError: No space left on device'
    jobs = archiver._job
    archiver.tick()
    assert archiver._job is jobs, 'no retry within RETRY_SECONDS'
    monkeypatch.setattr(archive_module, 'RETRY_SECONDS', 0)
    monkeypatch.setattr(archive_module, 'archive', real)
    archiver.tick()
    settle(archiver)
    with db.session() as s:
        assert due(s) is None


def test_a_stopping_daemon_cancels_the_archiving_in_flight_and_the_next_one_resumes_it(
    db: Database,
    project: Project,
    home: Path,
    store: Store,
    tracer: Tracer,
    spans: InMemorySpanExporter,
    pool: ThreadPoolExecutor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    idea = add_idea(db, project.id, 'idea')
    add_run(db, idea)
    path = write(workspace_of(home, project.id) / f'idea{idea}' / 'a.txt', 'a')
    stop = threading.Event()
    real = archive_module.archive

    def stopping(
        db: Database, store: Store, home: Path, job: Job, stop: threading.Event | None = None, *, reserve: int = 0
    ) -> Archived:
        assert stop is not None
        stop.set()  # the daemon stops while the archiving runs
        return real(db, store, home, job, stop, reserve=reserve)

    monkeypatch.setattr(archive_module, 'archive', stopping)
    archiver = Archiver(db, store, home, tracer, pool, stop)
    archiver.tick()
    settle(archiver)
    assert path.exists() and span_attrs(spans, 'piloop.archive').get('cancelled') is True
    first = archiver._job
    archiver.tick()
    assert archiver._job is first, 'nothing starts while the daemon stops'
    with db.session() as s:
        assert due(s) == Job(project.id, idea), 'a restarted daemon picks the backlog up where it was'
    monkeypatch.setattr(archive_module, 'archive', real)
    restarted = Archiver(db, store, home, tracer, pool, threading.Event())
    restarted.tick()
    settle(restarted)
    assert not path.exists()


def test_with_a_remote_and_nothing_due_the_archiver_evicts_hourly(
    db: Database, home: Path, tracer: Tracer, spans: InMemorySpanExporter, pool, monkeypatch: pytest.MonkeyPatch
) -> None:
    remote = DirRemote(home / 'remote')
    local = Store(home / 'store')
    write(home / 'x.txt', 'early')
    local.put_path(home / 'x.txt')
    store = Store(home / 'store', remote, evict_after=timedelta(days=1))
    archiver = Archiver(db, store, home, tracer, pool, threading.Event())
    archiver.tick()
    settle(archiver)
    assert span_attrs(spans, 'piloop.store.evict') == {'uploaded': 1, 'evicted': 0, 'freed': 0}
    jobs = archiver._job
    archiver.tick()
    assert archiver._job is jobs, 'not again within the hour'

    def broken() -> None:
        raise OSError('remote down')

    monkeypatch.setattr(archive_module, 'EVICT_SECONDS', 0)
    monkeypatch.setattr(store, 'evict', broken)
    archiver.tick()
    settle(archiver)
    assert [s for s in spans.get_finished_spans() if s.name == 'piloop.store.evict'][-1].status.description == (
        'OSError: remote down'
    )
    jobs = archiver._job
    archiver.tick()
    assert archiver._job is jobs, 'a failed eviction waits like a failed archiving'
    Archiver(db, Store(home / 'other'), home, tracer, pool, threading.Event()).tick()  # no remote: nothing to do


def span_attrs(spans: InMemorySpanExporter, name: str) -> dict[str, object]:
    [span] = [s for s in spans.get_finished_spans() if s.name == name]
    return dict(span.attributes or {})


def big_idea(db: Database, project: Project, home: Path) -> tuple[int, dict[str, bytes]]:
    """A done idea whose workspace holds three 4000-byte files in nested directories and a symlink."""
    idea = add_idea(db, project.id, 'big')
    add_run(db, idea)
    workspace = workspace_of(home, project.id)
    for name in ('a/one.npy', 'a/b/two.npy', 'three.npy'):
        write(workspace / f'idea{idea}' / name, os.urandom(4000))
    (workspace / f'idea{idea}' / 'latest').symlink_to('three.npy')
    return idea, files(workspace)


def test_a_workspace_larger_than_the_free_space_above_the_reserve_is_archived_in_batches(
    db: Database, project: Project, home: Path, store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    idea, before = big_idea(db, project, home)
    workspace = workspace_of(home, project.id)
    monkeypatch.setattr(Store, 'free', lambda self: 1000 + 9000)
    left_on_disk: list[int] = []
    real_put = archive_module.put_walk

    def counting(store: Store, root: Path, found: Walk, stop: threading.Event | None = None) -> Stored:
        left_on_disk.append(len(list(workspace.rglob('*.npy'))))
        return real_put(store, root, found, stop)

    monkeypatch.setattr(archive_module, 'put_walk', counting)
    done = archive(db, store, home, Job(project.id, idea), reserve=1000)
    assert done.short is None
    assert [(r.files, r.bytes) for r in done.rows] == [(3, 8000), (1, 4000)], 'two files fit, then the third'
    assert left_on_disk == [3, 1], 'each batch is pruned before the next one is stored'
    assert not (workspace / f'idea{idea}').exists(), 'the last batch removes the directories it empties'
    with db.session() as s:
        assert due(s) is None
        assert restore(s, store, home, s.get_one(Project, project.id), idea).done.written == 4
    assert files(workspace) == before


def test_a_disk_that_fills_between_batches_leaves_the_rest_for_the_campaign_stop(
    db: Database, project: Project, home: Path, store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    idea, before = big_idea(db, project, home)
    frees = iter([1000 + 9000, 1000 + 100])
    monkeypatch.setattr(Store, 'free', lambda self: next(frees))
    done = archive(db, store, home, Job(project.id, idea), reserve=1000)
    assert [r.files for r in done.rows] == [3]
    assert done.short == Short(needed=4000, free=1100, reserve=1000)
    workspace = workspace_of(home, project.id)
    assert [p.name for p in workspace.rglob('*.npy')] == ['three.npy'], 'what did not fit stays'
    with db.session() as s:
        s.get_one(Project, project.id).status = ProjectStatus.STOPPED
        s.commit()
        assert due(s) == Job(project.id, None), 'the idea is archived as far as it went; the stop sweep takes the rest'
    monkeypatch.setattr(Store, 'free', lambda self: 1 << 40)
    assert [r.files for r in archive(db, store, home, Job(project.id, None), reserve=1000).rows] == [1]
    with db.session() as s:
        restore(s, store, home, s.get_one(Project, project.id), idea)
    assert files(workspace) == before


def test_the_archiver_defers_a_job_the_disk_is_too_full_for_and_backs_off_on_each_failure_in_a_row(
    db: Database,
    project: Project,
    home: Path,
    store: Store,
    tracer: Tracer,
    spans: InMemorySpanExporter,
    pool: ThreadPoolExecutor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The incident: a 21 GB idea was stored into a disk with less free, ENOSPC, and stored again every 600 s."""
    idea, before = big_idea(db, project, home)
    workspace = workspace_of(home, project.id)
    monkeypatch.setattr(settings_module, 'settings', Settings(storage=StorageSettings(min_free_gb=1)))
    monkeypatch.setattr(Store, 'free', lambda self: GB + 100)
    archiver = Archiver(db, store, home, tracer, pool, threading.Event())
    assert archiver.retry_seconds == 0
    archiver.tick()
    settle(archiver)
    assert files(workspace) == before, 'not one file fits above the reserve: nothing is touched'
    with db.session() as s:
        assert s.scalars(select(Archive)).all() == [] and due(s) == Job(project.id, idea), 'still due'
        [event] = s.scalars(select(Event).where(Event.kind == DISK_ANOMALY)).all()
        assert (event.target_kind, event.target_id) == (TargetKind.PROJECT, project.id)
        assert event.payload == {
            'needed': 12000,
            'free': GB + 100,
            'reserve': GB,
            'idea': idea,
            'retry_seconds': RETRY_SECONDS,
        }
    assert events(spans, DISK_ANOMALY)[0]['needed'] == 12000
    jobs = archiver._job
    archiver.tick()
    assert archiver._job is jobs, 'it waits'

    def full(*args: object) -> Blob:
        raise OSError(errno.ENOSPC, 'No space left on device')

    put_path = Store.put_path
    monkeypatch.setattr(Store, 'free', lambda self: 1 << 40)
    monkeypatch.setattr(Store, 'put_path', full)
    archiver._failed_at = -math.inf  # the wait is over
    archiver.tick()
    settle(archiver)
    assert archiver.retry_seconds == 2 * RETRY_SECONDS, 'a failure after a deferral waits twice as long'
    for _ in range(10):
        archiver._failed_at = -math.inf
        archiver.tick()
        settle(archiver)
    assert archiver.retry_seconds == MAX_RETRY_SECONDS
    monkeypatch.setattr(Store, 'put_path', put_path)
    archiver._failed_at = -math.inf
    archiver.tick()
    settle(archiver)
    assert archiver.retry_seconds == 0, 'a success ends the backoff'
    assert not (workspace / f'idea{idea}').exists()
