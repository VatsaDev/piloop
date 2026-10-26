"""The daemon archives a done idea's workspace files once its run is judged, and keeps the local copies."""

import threading
import time
from pathlib import Path

import pytest
from sqlalchemy import select

from piloop.campaign.campaigns import workspace_dir
from piloop.daemon.run_controller import RunController
from piloop.daemon.scheduler import Scheduler
from piloop.db.models import Campaign, utcnow
from piloop.db.session import Database
from piloop.otel import Tracer
from piloop.plugins.archive.contract import CONSUMER, ArchiveHost
from piloop.plugins.archive.local import LocalArchive, archiver as archiver_module
from piloop.plugins.archive.local.store import Pruned
from piloop.plugins.archive.local.tables import Archive
from piloop.plugins.cas.disk import DiskCas
from piloop.plugins.executor.workers import Workers
from tests.archive.test_store import files
from tests.conftest import SETTLE, ScriptRuntime, factory, run_all, serving, wait_until, write_toml
from tests.daemon.test_scheduler import add_ideas

HARNESS_LEAVES_AN_ARRAY = (
    '#!/bin/sh\nmkdir -p workspace/idea1\necho array > workspace/idea1/x.npy\n'
    'echo line >> solve.txt\ngit commit -qam "add a line"\n'
)


def scheduled(db: Database, runtime: ScriptRuntime, home: Path, store: DiskCas) -> Scheduler:
    """A scheduler with the ``local`` archiver, which wakes its consumer as the daemon's does."""
    tracer = Tracer(None)
    made: list[Scheduler] = []

    def wake() -> None:
        made[0].loop.wake(CONSUMER)

    archiver = LocalArchive().open(ArchiveHost(db, store, home, 0, tracer, wake))
    controller = serving(RunController(db, tracer, home, factory(runtime), executor=Workers()))
    made.append(Scheduler(db, controller, tracer, home, factory(runtime), archiver=archiver))
    return made[0]


def test_the_tick_archives_an_idea_once_its_run_is_judged_and_leaves_its_files_for_later(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, campaign_path: Path
) -> None:
    write_toml(campaign_path, HARNESS_LEAVES_AN_ARRAY)
    store = DiskCas(tmp_path / 'store')
    scheduler = scheduled(db, runtime, tmp_path, store)
    (idea,) = add_ideas(db, campaign, 'leaves an array')
    assert idea == 1
    workspace = workspace_dir(tmp_path, campaign.id)
    assert run_all(scheduler) == [1]
    scheduler.wait(SETTLE)
    assert files(workspace) == {'idea1/x.npy': b'array\n'}

    def archived() -> bool:
        run_all(scheduler)
        with db.session() as s:
            return s.scalars(select(Archive)).first() is not None

    wait_until(archived)
    scheduler.stop(timeout=1)
    assert files(workspace) == {'idea1/x.npy': b'array\n'}, 'pruned by age or disk pressure, not now'
    with db.session() as s:
        row = s.scalars(select(Archive)).one()
        assert (str(row.subject), row.files, row.bytes) == ('idea#1', 1, 6)


def test_a_stopping_scheduler_waits_for_the_archivers_job(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The daemon released its lock while an upkeep still pruned."""
    store = DiskCas(tmp_path / 'store')
    scheduler = scheduled(db, runtime, tmp_path, store)
    started, finished = threading.Event(), threading.Event()
    real = archiver_module.prune_older

    def slow(*args: object, **kwargs: object) -> Pruned:
        started.set()
        time.sleep(0.5)
        finished.set()
        return real(db, store, tmp_path, utcnow())

    monkeypatch.setattr(archiver_module, 'prune_older', slow)
    run_all(scheduler)  # the first tick starts the upkeep
    assert started.wait(10)
    scheduler.stop(timeout=10)
    assert finished.is_set(), 'stop returned only after the upkeep'
