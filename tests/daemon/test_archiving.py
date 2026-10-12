"""The daemon archives a done idea's workspace files once its run is judged."""

from pathlib import Path

from sqlalchemy import select

from piloop.campaign.archive import workspace_of
from piloop.daemon.runner import RunExecutor
from piloop.daemon.scheduler import Scheduler
from piloop.db.models import Archive, Project
from piloop.db.session import Database
from piloop.otel import Tracer
from piloop.store import Store
from tests.campaign.test_archive import files
from tests.conftest import ScriptRuntime, factory, write_toml
from tests.daemon.test_scheduler import add_ideas
from tests.daemon.test_serve import wait_until

HARNESS_LEAVES_AN_ARRAY = (
    '#!/bin/sh\nmkdir -p workspace/idea1\necho array > workspace/idea1/x.npy\n'
    'echo line >> solve.txt\ngit commit -qam "add a line"\n'
)


def test_the_tick_archives_an_idea_once_its_run_is_judged(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, project_path: Path
) -> None:
    write_toml(project_path, HARNESS_LEAVES_AN_ARRAY)
    tracer = Tracer(None)
    store = Store(tmp_path / 'store')
    scheduler = Scheduler(
        db, RunExecutor(db, tracer, tmp_path, factory(runtime)), tracer, tmp_path, factory(runtime), store=store
    )
    (idea,) = add_ideas(db, project, 'leaves an array')
    assert idea == 1
    workspace = workspace_of(tmp_path, project.id)
    assert scheduler.tick() == [1]
    scheduler.wait(timeout=30)
    assert files(workspace) == {'idea1/x.npy': b'array\n'}

    def archived() -> bool:
        scheduler.tick()
        with db.session() as s:
            return s.scalars(select(Archive)).first() is not None

    wait_until(archived)
    wait_until(lambda: not (workspace / 'idea1').exists())
    scheduler.stop(timeout=1)
    with db.session() as s:
        row = s.scalars(select(Archive)).one()
        assert (row.idea_id, row.files, row.bytes) == (1, 1, 6)
