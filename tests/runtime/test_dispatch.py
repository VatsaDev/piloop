"""Every command that looks at an agent asks the agent's runtime, through the ``Runtime`` methods, never its name."""

from collections.abc import Iterator
from importlib.metadata import EntryPoint
from pathlib import Path

import pytest

from piloop.cli import agents, common, doctor
from piloop.config import config_of
from piloop.db.models import AgentSession, AgentSessionState, Campaign, RoleName, utcnow
from piloop.db.session import Database
from piloop.plugins.runtime.contract import RUNTIMES, Check
from piloop.reports.overview import overview
from piloop.runtime.factory import view_of
from piloop.settings import Settings
from piloop.watch.app import focus
from tests.daemon.test_roles import PLUGIN, forget_loaded, installed

SHOWN = [EntryPoint('shown', f'{PLUGIN}:Shown', RUNTIMES.group)]


@pytest.fixture
def shown(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Install a package that adds the ``shown`` runtime (``tests.fixtures.plugin.Shown``)."""
    installed(monkeypatch, SHOWN)
    yield
    forget_loaded()


def test_attach_focus_view_and_doctor_use_the_methods_of_a_runtime_that_a_package_adds(
    db: Database, campaign: Campaign, shown: None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with db.session() as s:
        s.get_one(Campaign, campaign.id).config = {**campaign.config, 'runtime': 'shown'}
        agent_session = AgentSession(
            kind=RoleName.WATCHDOG,
            campaign_id=campaign.id,
            harness='script',
            model='m',
            runtime='shown',
            handle={'pid': 7, 'start_time': 0, 'log_dir': str(tmp_path)},
            state=AgentSessionState.RUNNING,
            log_dir=str(tmp_path),
        )
        s.add(agent_session)
        s.commit()
        assert view_of(agent_session) == 'pane 7'
        row = s.get_one(Campaign, campaign.id)
        assert ', pane 7, on ' in overview(s, row, config_of(row), utcnow(), lambda w: [])
    assert focus(db, agent_session.id, tmp_path) == f'agent_session#{agent_session.id}: focused pane 7'
    monkeypatch.setattr(common, 'settings', Settings(home=tmp_path))
    printed: list[object] = []
    monkeypatch.setattr(common.console, 'print', printed.append)
    agents.attach(agent_session.id)
    assert printed == ['attached to pane 7']
    harness = str(tmp_path / 'harness.sh')
    assert {c.name: c for c in doctor.checks(Settings(home=tmp_path))}['shown'] == Check(
        'shown', f'ready for {harness}'
    )
