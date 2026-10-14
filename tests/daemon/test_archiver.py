"""The daemon's archiver: one job at a time on the daemon's pool, a pause after a failure, and the hourly upkeep."""

import errno
import os
import threading
from collections.abc import Collection, Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path

import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from sqlalchemy import select

from piloop.campaign.archive import Archived, Job, due
from piloop.campaign.project import workspace_dir
from piloop.daemon import archiver as archiver_module
from piloop.daemon.archiver import DISK_ANOMALY, ArchivePolicy, Archiver
from piloop.db.models import Archive, Event, Project, Run, TargetKind, utcnow
from piloop.db.session import Database
from piloop.otel import Tracer
from piloop.settings import GB, StorageSettings
from piloop.store import Blob, Store
from tests.campaign.test_archive import add_idea, add_run, big_idea, files, write
from tests.conftest import events
from tests.store.test_store import DirRemote

POLICY = ArchivePolicy(reserve=0)
type Stop = threading.Event | None


@pytest.fixture
def home(tmp_path: Path) -> Path:
    return tmp_path / 'piloop-home'


@pytest.fixture
def store(home: Path) -> Store:
    return Store(home / 'store')


@pytest.fixture
def pool() -> Iterator[ThreadPoolExecutor]:
    with ThreadPoolExecutor(1) as executor:
        yield executor


def settle(archiver: Archiver) -> None:
    job = archiver._job
    assert job is not None
    job.result(timeout=30)


def spans_named(spans: InMemorySpanExporter, name: str) -> list[dict[str, object]]:
    return [dict(s.attributes or {}) for s in spans.get_finished_spans() if s.name == name]


def test_the_policy_takes_the_reserve_from_storage_and_defaults_the_rest() -> None:
    policy = ArchivePolicy.of(StorageSettings(min_free_gb=2))
    assert (policy.reserve, policy.prune_after, policy.retry) == (2 * GB, timedelta(days=7), timedelta(minutes=10))
    assert policy.live == {'NOTES.md', 'research'}


def test_the_archiver_runs_one_job_at_a_time_and_pauses_after_a_failure(
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
    write(workspace_dir(home, project.id) / f'idea{first}' / 'a.txt', 'a')
    archiver = Archiver(db, store, home, POLICY, tracer, pool, threading.Event())
    gate = threading.Event()
    real = archiver_module.archive

    def held(
        db: Database, store: Store, home: Path, job: Job, *, live: Collection[str], reserve: int, stop: Stop = None
    ) -> Archived:
        gate.wait(10)
        return real(db, store, home, job, live=live, reserve=reserve, stop=stop)

    monkeypatch.setattr(archiver_module, 'archive', held)
    archiver.tick()
    archiver.tick()  # the first is still running: no second job
    gate.set()
    settle(archiver)
    archiver.tick()
    settle(archiver)
    done = spans_named(spans, 'piloop.archive')
    assert [s.get('idea') for s in done] == [first, second] and done[0].get('files') == 1

    third = add_idea(db, project.id, 'third')
    add_run(db, third)

    def broken(
        db: Database, store: Store, home: Path, job: Job, *, live: Collection[str], reserve: int, stop: Stop = None
    ) -> Archived:
        raise OSError('No space left on device')

    monkeypatch.setattr(archiver_module, 'archive', broken)
    archiver.tick()
    settle(archiver)
    [failed] = [s for s in spans.get_finished_spans() if s.name == 'piloop.archive'][-1:]
    assert failed.status.description == 'OSError: No space left on device'
    jobs = archiver._job
    archiver.tick()
    assert archiver._job is jobs, 'paused for policy.retry'
    monkeypatch.setattr(archiver_module, 'archive', real)
    archiver._policy = ArchivePolicy(reserve=0, retry=timedelta(0))
    archiver._pause()
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
    pool: ThreadPoolExecutor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    idea = add_idea(db, project.id, 'idea')
    add_run(db, idea)
    write(workspace_dir(home, project.id) / f'idea{idea}' / 'a.txt', 'a')
    stop = threading.Event()
    real = archiver_module.archive

    def stopping(
        db: Database, store: Store, home: Path, job: Job, *, live: Collection[str], reserve: int, stop: Stop = None
    ) -> Archived:
        assert stop is not None
        stop.set()  # the daemon stops while the archiving runs
        return real(db, store, home, job, live=live, reserve=reserve, stop=stop)

    monkeypatch.setattr(archiver_module, 'archive', stopping)
    archiver = Archiver(db, store, home, POLICY, tracer, pool, stop)
    archiver.tick()
    settle(archiver)
    first = archiver._job
    archiver.tick()
    assert archiver._job is first, 'nothing starts while the daemon stops'
    with db.session() as s:
        assert due(s) == Job(project.id, idea), 'a restarted daemon picks the backlog up where it was'
    monkeypatch.setattr(archiver_module, 'archive', real)
    restarted = Archiver(db, store, home, POLICY, tracer, pool, threading.Event())
    restarted.tick()
    settle(restarted)
    with db.session() as s:
        assert due(s) is None


def test_with_nothing_due_the_upkeep_prunes_by_age_and_pressure_and_evicts_hourly(
    db: Database,
    project: Project,
    home: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
    pool: ThreadPoolExecutor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    remote = DirRemote(home / 'remote')
    store = Store(home / 'store', remote, evict_after=timedelta(days=1))
    workspace = workspace_dir(home, project.id)
    for title in ('old', 'new'):
        idea = add_idea(db, project.id, title)
        add_run(db, idea)
        write(workspace / f'idea{idea}' / 'x.npy', os.urandom(100))
    archiver = Archiver(db, store, home, POLICY, tracer, pool, threading.Event())
    for _ in range(2):
        archiver.tick()
        settle(archiver)
    with db.session() as s:
        old = s.scalars(select(Archive).order_by(Archive.id)).first()
        assert old is not None
        old.created_at = utcnow() - timedelta(days=8)
        for run in s.scalars(select(Run).where(Run.idea_id == old.idea_id)):
            run.finished_at = utcnow() - timedelta(days=9)  # else the idea is due again
        s.commit()
    monkeypatch.setattr(Store, 'free', lambda self: 1 << 40)
    archiver.tick()
    settle(archiver)
    assert spans_named(spans, 'piloop.store.upkeep') == [
        {'pruned_by_age': 1, 'pruned_by_pressure': 0, 'uploaded': 0, 'evicted': 0, 'freed': 0}
    ]
    assert sorted(p.name for p in workspace.iterdir()) == ['idea2'], 'the older archiving went by age'
    jobs = archiver._job
    archiver.tick()
    assert archiver._job is jobs, 'not again within the hour'

    archiver._policy = ArchivePolicy(reserve=GB, upkeep_every=timedelta(0))
    monkeypatch.setattr(Store, 'free', lambda self: 0)
    monkeypatch.setattr(store, 'evict', lambda: (_ for _ in ()).throw(OSError('remote down')))
    archiver.tick()
    settle(archiver)
    last = [s for s in spans.get_finished_spans() if s.name == 'piloop.store.upkeep'][-1]
    assert dict(last.attributes or {}) == {'pruned_by_age': 0, 'pruned_by_pressure': 1}
    assert last.status.description == 'OSError: remote down'
    assert files(workspace) == {}, 'the disk is short: the newer archiving went too'
    jobs = archiver._job
    archiver.tick()
    assert archiver._job is jobs, 'a failed upkeep pauses like a failed archiving'
    Archiver(db, Store(home / 'other'), home, POLICY, tracer, pool, threading.Event()).tick()  # no remote: no evict


def test_a_job_the_disk_is_too_full_for_writes_one_disk_anomaly_and_pauses(
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
    monkeypatch.setattr(Store, 'free', lambda self: GB + 100)
    archiver = Archiver(db, store, home, ArchivePolicy(reserve=GB), tracer, pool, threading.Event())
    archiver.tick()
    settle(archiver)
    assert files(workspace_dir(home, project.id)) == before
    with db.session() as s:
        assert s.scalars(select(Archive)).all() == [] and due(s) == Job(project.id, idea), 'still due'
        [event] = s.scalars(select(Event).where(Event.kind == DISK_ANOMALY)).all()
        assert (event.target_kind, event.target_id) == (TargetKind.PROJECT, project.id)
        assert event.payload == {'needed': 12000, 'free': GB + 100, 'reserve': GB, 'idea': idea, 'retry_seconds': 600}
    assert events(spans, DISK_ANOMALY)[0]['needed'] == 12000
    jobs = archiver._job
    archiver.tick()
    assert archiver._job is jobs, 'it waits'

    def full(*_args: object) -> Blob:
        raise OSError(errno.ENOSPC, 'No space left on device')

    monkeypatch.setattr(Store, 'free', lambda self: 1 << 40)
    monkeypatch.setattr(Store, 'put_path', full)
    archiver._paused_until = 0  # the wait is over
    archiver.tick()
    settle(archiver)
    assert archiver._paused_until > 0, 'a failure pauses for the same policy.retry, never longer'
