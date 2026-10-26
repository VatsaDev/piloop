"""The archiver of ``local``: one job at a time on its own thread, a pause after a failure, and the hourly upkeep."""

import errno
import os
import threading
import time
from collections.abc import Collection
from datetime import timedelta
from pathlib import Path

import pytest
from covalence import Cid
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from sqlalchemy import select

from piloop.campaign.campaigns import workspace_dir
from piloop.db.events import in_order
from piloop.db.models import Campaign, Event, utcnow
from piloop.db.session import Database
from piloop.ids import TargetKind
from piloop.otel import Tracer
from piloop.plugins.archive.contract import ArchiveHost
from piloop.plugins.archive.local import LocalArchive, archiver as archiver_module, store as archive_module
from piloop.plugins.archive.local.archiver import ArchivePolicy, DiskAnomaly, LocalArchiver
from piloop.plugins.archive.local.store import Archived, Job, Pruned, archive, due, make_room
from piloop.plugins.archive.local.tables import Archive
from piloop.plugins.cas.cached import CachedCas
from piloop.plugins.cas.contract import Cas
from piloop.plugins.cas.disk import DiskCas
from piloop.plugins.cas.kit.objects import ObjectsCas
from piloop.plugins.contribution import Leaving
from piloop.settings import GB
from tests.archive.test_store import add_idea, add_run, big_idea, files, ref, write
from tests.conftest import events

POLICY = ArchivePolicy(reserve=0)
type Stop = threading.Event | None


@pytest.fixture
def home(tmp_path: Path) -> Path:
    return tmp_path / 'piloop-home'


def cached(home: Path, evict_after: timedelta | None = None) -> CachedCas:
    remote = DiskCas(home / 'remote')
    return CachedCas(DiskCas(home / 'store'), home / 'store', remote, 'file://remote', evict_after)


@pytest.fixture
def store(home: Path) -> ObjectsCas:
    return DiskCas(home / 'store')


def nobody() -> None:
    """The archiver's ``wake``: a test runs the archiver itself."""


def opened(db: Database, store: Cas, home: Path, tracer: Tracer, policy: ArchivePolicy = POLICY) -> LocalArchiver:
    return LocalArchiver(ArchiveHost(db, store, home, policy.reserve, tracer, nobody), policy)


def settle(archiver: LocalArchiver) -> None:
    job = archiver._job
    assert job is not None
    job.result(timeout=30)


def upkept(archiver: LocalArchiver) -> LocalArchiver:
    """``archiver`` after its first run, which starts the upkeep: the next run archives."""
    archiver.run(utcnow())
    settle(archiver)
    return archiver


def spans_named(spans: InMemorySpanExporter, name: str) -> list[dict[str, object]]:
    return [dict(s.attributes or {}) for s in spans.get_finished_spans() if s.name == name]


def test_the_policy_takes_the_reserve_from_the_host_and_defaults_the_rest(
    db: Database, home: Path, store: ObjectsCas, tracer: Tracer
) -> None:
    archiver = LocalArchive().open(ArchiveHost(db, store, home, 2 * GB, tracer, nobody))
    policy = archiver._policy
    assert (policy.reserve, policy.prune_after, policy.retry) == (2 * GB, timedelta(days=7), timedelta(minutes=10))
    assert policy.live == {'NOTES.md', 'research'}
    archiver.stop(Leaving.STOP, 0)


def test_the_archiver_runs_one_job_at_a_time_and_pauses_after_a_failure(
    db: Database,
    campaign: Campaign,
    home: Path,
    store: ObjectsCas,
    tracer: Tracer,
    spans: InMemorySpanExporter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first, second = add_idea(db, campaign.id, 'first'), add_idea(db, campaign.id, 'second')
    add_run(db, first)
    add_run(db, second)
    write(workspace_dir(home, campaign.id) / f'idea{first}' / 'a.txt', 'a')
    archiver = upkept(opened(db, store, home, tracer))
    gate = threading.Event()
    real = archiver_module.archive

    def held(
        db: Database, store: ObjectsCas, home: Path, job: Job, *, live: Collection[str], reserve: int, stop: Stop = None
    ) -> Archived:
        gate.wait(10)
        return real(db, store, home, job, live=live, reserve=reserve, stop=stop)

    monkeypatch.setattr(archiver_module, 'archive', held)
    archiver.run(utcnow())
    archiver.run(utcnow())  # the first is still running: no second job
    gate.set()
    settle(archiver)
    archiver.run(utcnow())
    settle(archiver)
    done = spans_named(spans, 'piloop.archive')
    assert [s.get('subject') for s in done] == [f'idea#{first}', f'idea#{second}'] and done[0].get('files') == 1

    third = add_idea(db, campaign.id, 'third')
    add_run(db, third)

    def broken(
        db: Database, store: ObjectsCas, home: Path, job: Job, *, live: Collection[str], reserve: int, stop: Stop = None
    ) -> Archived:
        raise OSError('No space left on device')

    monkeypatch.setattr(archiver_module, 'archive', broken)
    archiver.run(utcnow())
    settle(archiver)
    [failed] = [s for s in spans.get_finished_spans() if s.name == 'piloop.archive'][-1:]
    assert failed.status.description == 'OSError: No space left on device'
    jobs = archiver._job
    archiver.run(utcnow())
    assert archiver._job is jobs, 'paused for policy.retry'
    monkeypatch.setattr(archiver_module, 'archive', real)
    archiver._policy = ArchivePolicy(reserve=0, retry=timedelta(0))
    archiver._pause()
    archiver.run(utcnow())
    settle(archiver)
    with db.session() as s:
        assert due(s) is None


def test_a_stopping_daemon_cancels_the_archiving_in_flight_and_the_next_one_resumes_it(
    db: Database,
    campaign: Campaign,
    home: Path,
    store: ObjectsCas,
    tracer: Tracer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    idea = add_idea(db, campaign.id, 'idea')
    add_run(db, idea)
    write(workspace_dir(home, campaign.id) / f'idea{idea}' / 'a.txt', 'a')
    real = archiver_module.archive

    def stopping(
        db: Database, store: ObjectsCas, home: Path, job: Job, *, live: Collection[str], reserve: int, stop: Stop = None
    ) -> Archived:
        assert stop is not None
        stop.set()  # the daemon stops while the archiving runs
        return real(db, store, home, job, live=live, reserve=reserve, stop=stop)

    monkeypatch.setattr(archiver_module, 'archive', stopping)
    archiver = upkept(opened(db, store, home, tracer))
    archiver.run(utcnow())
    settle(archiver)
    first = archiver._job
    archiver.run(utcnow())
    assert archiver._job is first, 'nothing starts while the daemon stops'
    with db.session() as s:
        assert due(s) == Job(campaign.id, ref(idea)), 'a restarted daemon picks the backlog up where it was'
    monkeypatch.setattr(archiver_module, 'archive', real)
    restarted = upkept(opened(db, store, home, tracer))
    restarted.run(utcnow())
    settle(restarted)
    with db.session() as s:
        assert due(s) is None


def test_with_nothing_due_the_upkeep_prunes_by_age_and_pressure_and_evicts_hourly(
    db: Database,
    campaign: Campaign,
    home: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = cached(home, evict_after=timedelta(days=1))
    workspace = workspace_dir(home, campaign.id)
    for title in ('old', 'new'):
        idea = add_idea(db, campaign.id, title)
        add_run(db, idea)
        write(workspace / f'idea{idea}' / 'x.npy', os.urandom(100))
        archive(db, store, home, Job(campaign.id, ref(idea)), live=POLICY.live, reserve=0)
    with db.session() as s:
        old = s.scalars(select(Archive).order_by(Archive.id)).first()
        assert old is not None
        old.created_at = utcnow() - timedelta(days=8)
        assert old.subject is not None
        for event in s.scalars(select(Event).where(Event.target == old.subject)):
            event.at = utcnow() - timedelta(days=9)  # else the idea is due again
        s.commit()
    monkeypatch.setattr(archive_module, 'free', lambda _: 1 << 40)
    archiver = upkept(opened(db, store, home, tracer))
    assert spans_named(spans, 'piloop.store.upkeep') == [
        {'pruned_by_age': 1, 'pruned_by_pressure': 0, 'inputs_evicted': 0, 'uploaded': 0, 'evicted': 0, 'freed': 0}
    ], 'the first tick of a daemon is the upkeep'
    assert sorted(p.name for p in workspace.iterdir()) == ['idea2'], 'the older archiving went by age'
    archiver.run(utcnow())
    assert archiver._job is not None and archiver._job.done() and len(spans_named(spans, 'piloop.store.upkeep')) == 1

    archiver._policy = ArchivePolicy(reserve=GB, upkeep_every=timedelta(0))
    monkeypatch.setattr(archive_module, 'free', lambda _: 0)
    monkeypatch.setattr(store, 'flush', lambda *, evict: (_ for _ in ()).throw(OSError('remote down')))
    archiver.run(utcnow())
    settle(archiver)
    last = [s for s in spans.get_finished_spans() if s.name == 'piloop.store.upkeep'][-1]
    assert dict(last.attributes or {}) == {'pruned_by_age': 0, 'pruned_by_pressure': 1, 'inputs_evicted': 0}
    assert last.status.description == 'OSError: remote down'
    assert files(workspace) == {}, 'the disk is short: the newer archiving went too'
    jobs = archiver._job
    archiver.run(utcnow())
    assert archiver._job is jobs, 'a failed upkeep pauses like a failed archiving'
    opened(db, DiskCas(home / 'other'), home, tracer).run(utcnow())  # no evict


def test_a_job_the_disk_is_too_full_for_writes_one_disk_anomaly_and_pauses(
    db: Database,
    campaign: Campaign,
    home: Path,
    store: ObjectsCas,
    tracer: Tracer,
    spans: InMemorySpanExporter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The incident: a 21 GB idea was stored into a disk with less free, ENOSPC, and stored again every 600 s."""
    idea, before = big_idea(db, campaign, home)
    monkeypatch.setattr(archive_module, 'free', lambda _: GB + 100)
    archiver = upkept(opened(db, store, home, tracer, ArchivePolicy(reserve=GB)))
    archiver.run(utcnow())
    settle(archiver)
    assert files(workspace_dir(home, campaign.id)) == before
    with db.session() as s:
        assert s.scalars(select(Archive)).all() == [] and due(s) == Job(campaign.id, ref(idea)), 'still due'
        [(event, short)] = in_order(s, DiskAnomaly)
        assert (event.target_kind, event.target_id) == (TargetKind.CAMPAIGN, campaign.id)
        assert short == DiskAnomaly(needed=12000, free=GB + 100, reserve=GB, subject=ref(idea), retry_seconds=600)
    assert events(spans, short.kind)[0]['needed'] == 12000
    jobs = archiver._job
    archiver.run(utcnow())
    assert archiver._job is jobs, 'it waits'

    def full(*_args: object, **_limits: object) -> Cid:
        raise OSError(errno.ENOSPC, 'No space left on device')

    monkeypatch.setattr(archive_module, 'free', lambda _: 1 << 40)
    monkeypatch.setattr(ObjectsCas, 'put_fileobj', full)
    archiver._paused_until = 0  # the wait is over
    archiver.run(utcnow())
    settle(archiver)
    assert len(spans_named(spans, 'piloop.store.upkeep')) == 2, 'after a short job, the upkeep frees room first'
    archiver.run(utcnow())
    settle(archiver)
    assert archiver._paused_until > 0, 'a failure pauses for the same policy.retry, never longer'


def test_what_an_archiving_leaves_out_is_on_its_span_with_why(
    db: Database, campaign: Campaign, home: Path, store: ObjectsCas, tracer: Tracer, spans: InMemorySpanExporter
) -> None:
    """A skipped path went unreported."""
    idea = add_idea(db, campaign.id, 'idea')
    add_run(db, idea)
    write(workspace_dir(home, campaign.id) / f'idea{idea}' / 'a.txt', 'a')
    os.mkfifo(workspace_dir(home, campaign.id) / f'idea{idea}' / 'pipe')
    archiver = upkept(opened(db, store, home, tracer))
    archiver.run(utcnow())
    settle(archiver)
    assert spans_named(spans, 'piloop.archive')[0]['skipped'] == 1
    assert events(spans, 'archive.skipped') == [
        {'paths': f'idea{idea}/pipe: not a regular file, directory or symlink: never stored'}
    ]


def test_the_upkeep_reports_a_file_it_cannot_read_and_counts_from_its_end(
    db: Database,
    campaign: Campaign,
    home: Path,
    store: ObjectsCas,
    tracer: Tracer,
    spans: InMemorySpanExporter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An upkeep slower than its period kept the archive jobs waiting."""
    idea = add_idea(db, campaign.id, 'locked')
    add_run(db, idea)
    path = write(workspace_dir(home, campaign.id) / f'idea{idea}' / 'x.npy', 'x')
    archive(db, store, home, Job(campaign.id, ref(idea)), live=POLICY.live, reserve=0)
    monkeypatch.setattr(archive_module, 'free', lambda _: 0)
    path.chmod(0)
    try:
        archiver = upkept(opened(db, store, home, tracer, ArchivePolicy(reserve=GB)))
    finally:
        path.chmod(0o644)
    assert events(spans, 'prune.left') == [
        {'paths': f'idea{idea}/x.npy: not readable (Permission denied): fix its permissions'}
    ]
    assert time.monotonic() - archiver._upkept_at < 5, 'the period counts from the end of the upkeep'


class Clock:
    """A ``time`` module for the archiver whose ``monotonic`` the test moves."""

    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now


def test_an_upkeep_slower_than_its_period_still_leaves_the_next_tick_to_a_due_job(
    db: Database,
    campaign: Campaign,
    home: Path,
    store: ObjectsCas,
    tracer: Tracer,
    spans: InMemorySpanExporter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With the period counted from the start, a 2-hour upkeep ran again at once."""
    clock = Clock()
    monkeypatch.setattr(archiver_module, 'time', clock)
    idea = add_idea(db, campaign.id, 'waiting')
    add_run(db, idea)
    real = archiver_module.prune_older

    def slow(*args: object, **kwargs: object) -> Pruned:
        clock.now += 2 * 3600  # this upkeep takes two hours, twice its period
        return real(db, store, home, utcnow() - POLICY.prune_after)

    monkeypatch.setattr(archiver_module, 'prune_older', slow)
    archiver = upkept(opened(db, store, home, tracer))
    archiver.run(utcnow())
    settle(archiver)
    assert len(spans_named(spans, 'piloop.store.upkeep')) == 1 and spans_named(spans, 'piloop.archive'), (
        'the next tick archives'
    )


def test_a_stopping_daemon_ends_the_upkeep_between_steps(
    db: Database,
    campaign: Campaign,
    home: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An upkeep went on pruning and evicting after the daemon had stopped."""
    store = cached(home)
    evicted: list[bool] = []
    monkeypatch.setattr(store, 'flush', lambda *, evict: evicted.append(evict))
    with db.session() as s:
        s.add(Archive(campaign_id=campaign.id, created_at=utcnow() - timedelta(days=30)))
        s.commit()
    archiver = opened(db, store, home, tracer)
    archiver.stop(Leaving.STOP, 0)  # the daemon stops while the upkeep runs
    archiver._upkeep()
    assert spans_named(spans, 'piloop.store.upkeep')[0]['stopped'] is True and evicted == []
    with db.session() as s:
        assert s.scalars(select(Archive.pruned_at)).one() is None, 'not even the overdue archiving'
    store.put(b'x')
    monkeypatch.setattr(archive_module, 'free', lambda _: 0)
    assert make_room(db, store, home, reserve=1, stop=archiver._stop) == Pruned(), 'the pressure prune stops too'
