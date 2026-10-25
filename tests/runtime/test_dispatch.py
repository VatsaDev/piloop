"""Every command that looks at an agent asks the agent's runtime, through the ``Runtime`` methods, never its name."""

from collections.abc import Iterator
from importlib.metadata import EntryPoint
from pathlib import Path

import pytest

from piloop.cli import agents, common
from piloop.db.models import AgentSession, AgentSessionState, Campaign, utcnow
from piloop.db.session import Database
from piloop.plugins.runtime.contract import RUNTIMES
from piloop.settings import Settings
from tests.conftest import WATCHDOG, reconfigured
from tests.daemon.test_roles import PLUGIN, forget_loaded, installed
from tests.reports.test_overview import overview

SHOWN = [EntryPoint('shown', f'{PLUGIN}:Shown', RUNTIMES.group)]


@pytest.fixture
def shown(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Install a package that adds the ``shown`` runtime (``tests.fixtures.plugin.Shown``)."""
    installed(monkeypatch, SHOWN)
    yield
    forget_loaded()


def test_attach_and_status_read_the_output_through_a_runtime_that_a_package_adds(
    db: Database, campaign: Campaign, shown: None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with db.session() as s:
        s.get_one(Campaign, campaign.id).config = reconfigured(campaign.config, runtime='shown')
        agent_session = AgentSession(
            kind=WATCHDOG,
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
        row = s.get_one(Campaign, campaign.id)
        assert ', on ' in (text := overview(s, row, utcnow())) and ': pane 7' in text
    monkeypatch.setattr(common, 'settings', Settings(home=tmp_path))
    printed: list[object] = []
    monkeypatch.setattr(common.console, 'print', lambda line, **_: printed.append(line))
    agents.attach(agent_session.id)
    assert printed == ['pane 7']
