"""Every command that looks at an agent asks the agent's runtime, through the ``Runtime`` methods, never its name."""

from collections.abc import Iterator
from importlib.metadata import EntryPoint
from pathlib import Path

import pytest

from piloop.cli import agents, common, doctor
from piloop.config import config_of
from piloop.db.models import Project, Worker, WorkerKind, WorkerState, utcnow
from piloop.db.session import Database
from piloop.reports.overview import overview
from piloop.runtime import Check
from piloop.runtime.factory import RUNTIME_GROUP, view_of
from piloop.settings import Settings
from piloop.watch.app import focus
from tests.daemon.test_roles import PLUGIN, forget_loaded, installed

SHOWN = [EntryPoint('shown', f'{PLUGIN}:Shown', RUNTIME_GROUP)]


@pytest.fixture
def shown(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Install a package that adds the ``shown`` runtime (``tests.fixtures.plugin.Shown``)."""
    installed(monkeypatch, SHOWN)
    yield
    forget_loaded()


def test_attach_focus_view_and_doctor_use_the_methods_of_a_runtime_that_a_package_adds(
    db: Database, project: Project, shown: None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with db.session() as s:
        s.get_one(Project, project.id).config = {**project.config, 'runtime': 'shown'}
        worker = Worker(
            kind=WorkerKind.WATCHDOG,
            project_id=project.id,
            harness='script',
            model='m',
            runtime='shown',
            handle={'pid': 7, 'start_time': 0, 'log_dir': str(tmp_path)},
            state=WorkerState.RUNNING,
            log_dir=str(tmp_path),
        )
        s.add(worker)
        s.commit()
        assert view_of(worker) == 'pane 7'
        row = s.get_one(Project, project.id)
        assert ', pane 7, on ' in overview(s, row, config_of(row), utcnow(), lambda w: [])
    assert focus(db, worker.id, tmp_path) == f'worker#{worker.id}: focused pane 7'
    monkeypatch.setattr(common, 'settings', Settings(home=tmp_path))
    printed: list[object] = []
    monkeypatch.setattr(common.console, 'print', printed.append)
    agents.attach(worker.id)
    assert printed == ['attached to pane 7']
    harness = str(tmp_path / 'harness.sh')
    assert doctor.checks(Settings(home=tmp_path))[-1] == Check('shown', f'ready for {harness}')
