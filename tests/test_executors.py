"""The executor contract: every executor runs a job the same way for core. Each case runs on ``workers`` (built in, on
leases) and on ``inline`` (the example ``examples/plugins/inline-executor``, a thread per job).

A run brings its commits back and is scored, an evaluation records its measurement, a cancel stops a running agent
and it still reports, and a stop closes a running agent as stopped.
"""

import subprocess
import threading
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import func, select

from piloop.agents.results import DaemonStopped, Ended, Ran
from piloop.cli import common, evaluation
from piloop.daemon.main import Daemon
from piloop.daemon.run_controller import RunController
from piloop.db.models import LIVE, AgentSession, AgentSessionState, Campaign, Candidate, Measurement, RunEnd
from piloop.db.session import Database
from piloop.ids import WATCHDOG, AgentSessionId
from piloop.otel import Tracer
from piloop.plugins import host as plugins
from piloop.plugins.executor.contract import EXECUTORS, AgentKey, Checkout
from piloop.plugins.host import PluginTable
from piloop.settings import Settings
from tests.conftest import (
    HARNESS_SLEEPS,
    ScriptRuntime,
    baseline,
    example,
    factory,
    package,
    wait_until,
    write_toml,
)
from tests.daemon.test_run_controller import execute, make_run, reload_campaign, run_row


@pytest.fixture(params=['workers', 'inline'])
def provider(request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """The name of the executor under the contract, installed and selectable."""
    if request.param == 'inline':
        example(tmp_path / 'site', 'inline-executor')
        monkeypatch.syspath_prepend(tmp_path / 'site')
    plugins.close()  # the registry loads again, with the example's entry point
    yield request.param
    plugins.close()


def controller(db: Database, runtime: ScriptRuntime, tmp_path: Path, provider: str) -> RunController:
    return RunController(db, Tracer(None), tmp_path, factory(runtime), executor=PluginTable(provider=provider))


def launch_in_thread(
    ex: RunController, db: Database, campaign: Campaign, tmp_path: Path
) -> tuple[AgentSessionId, list[Ended]]:
    with db.session() as s:
        w = AgentSession(kind=WATCHDOG, campaign_id=campaign.id, reason='contract')
        s.add(w)
        s.commit()
        agent_session_id = w.id
    cwd = tmp_path / 'cwd'
    cwd.mkdir()
    ended: list[Ended] = []

    def launch() -> None:
        ended.append(
            ex.launcher.launch(
                agent_session_id, brief='b', cwd=cwd, log_dir=tmp_path / 'w', artifact=Checkout(path=cwd)
            )
        )

    threading.Thread(target=launch, daemon=True).start()
    return agent_session_id, ended


def test_a_run_brings_its_commits_back_and_is_scored(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, provider: str
) -> None:
    run_id = make_run(db, campaign)
    ex = controller(db, runtime, tmp_path, provider)
    execute(ex, db, run_id)
    assert run_row(db, run_id).status.value == 'succeeded'
    with db.session() as s:
        [candidate] = s.scalars(select(Candidate).where(Candidate.run_id == run_id)).all()
        assert candidate.summary == 'add a line' and candidate.measurements, 'its commit came back and was measured'
    pinned = subprocess.run(
        ['git', 'for-each-ref', 'refs/piloop/agents/'], cwd=campaign.path, capture_output=True, text=True, check=True
    )
    assert pinned.stdout == '', "the agent's ref goes once the run pinned its commits"


def test_an_evaluation_records_its_measurement(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, provider: str
) -> None:
    ex = controller(db, runtime, tmp_path, provider)
    candidate = baseline(db, campaign.id).id
    ex.remeasure(candidate)
    with db.session() as s:
        count = s.scalar(select(func.count()).select_from(Measurement).where(Measurement.candidate_id == candidate))
    assert count == 2, 'the baseline and the new measurement'


def test_a_cancelled_agent_is_stopped_and_still_reports(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, provider: str
) -> None:
    write_toml(Path(campaign.path), HARNESS_SLEEPS)
    reload_campaign(db, campaign)
    ex = controller(db, runtime, tmp_path, provider)
    agent_session_id, ended = launch_in_thread(ex, db, campaign, tmp_path)
    wait_for_agent_session_handle_of(db, agent_session_id)
    ex.executor.cancel(AgentKey(agent_session_id))
    wait_until(lambda: ended)
    assert isinstance(ended[0], Ran), 'its result closed the row'
    with db.session() as s:
        assert s.get_one(AgentSession, agent_session_id).state not in LIVE


def test_a_stop_closes_a_running_agent_as_stopped(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, provider: str
) -> None:
    write_toml(Path(campaign.path), HARNESS_SLEEPS)
    reload_campaign(db, campaign)
    ex = controller(db, runtime, tmp_path, provider)
    agent_session_id, ended = launch_in_thread(ex, db, campaign, tmp_path)
    wait_for_agent_session_handle_of(db, agent_session_id)
    ex.stop(30)
    wait_until(lambda: ended)
    assert ended[0] == DaemonStopped()
    with db.session() as s:
        w = s.get_one(AgentSession, agent_session_id)
        assert (w.state, w.error) == (AgentSessionState.KILLED, RunEnd.DAEMON_STOPPED)


def wait_for_agent_session_handle_of(db: Database, agent_session_id: AgentSessionId) -> None:
    def running() -> bool:
        with db.session() as s:
            w = s.get_one(AgentSession, agent_session_id)
            return w.state is AgentSessionState.RUNNING and bool(w.handle)

    wait_until(running)


def test_config_toml_selects_the_executor_and_the_daemon_refuses_a_parameter_it_does_not_take(
    tmp_path: Path, provider: str
) -> None:
    (tmp_path / 'config.toml').write_text(f'[plugins.executor]\nprovider = "{provider}"\n')
    daemon = Daemon(Settings(home=tmp_path))
    try:
        assert (
            type(daemon.controller.executor).__name__
            == {'workers': 'WorkersExecutor', 'inline': 'InlineExecutor'}[provider]
        )
    finally:
        daemon.shutdown(timeout=5)
    (tmp_path / 'config.toml').write_text(f'[plugins.executor]\nprovider = "{provider}"\nslots = 3\n')
    with pytest.raises(SystemExit, match='slots'):
        Daemon(Settings(home=tmp_path))


def test_a_cancel_of_a_job_that_the_executor_does_not_have_changes_nothing(
    db: Database, runtime: ScriptRuntime, tmp_path: Path, provider: str
) -> None:
    controller(db, runtime, tmp_path, provider).executor.cancel(AgentKey(AgentSessionId(999)))


def test_an_entry_point_that_is_not_an_executor_class_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package(tmp_path / 'bad', 'piloop-bad', '1.0', '[piloop.executors]\nbad = tests.fixtures.plugin:Typed\n')
    monkeypatch.syspath_prepend(tmp_path / 'bad')
    plugins.close()
    try:
        with pytest.raises(plugins.PluginError, match='is not a piloop executor class'):
            EXECUTORS.get('bad')
    finally:
        plugins.close()


def test_eval_queue_shows_the_slots_of_the_selected_executor(
    tmp_path: Path, provider: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / 'config.toml').write_text(f'[plugins.executor]\nprovider = "{provider}"\n')
    monkeypatch.setattr(common, 'settings', Settings(home=tmp_path))
    expected = {'workers': '2 slot(s) (workers evaluations)', 'inline': 'no slot limit (inline)'}
    assert evaluation._slots() == expected[provider]


def test_config_toml_refuses_a_plugins_table_that_no_capability_of_the_home_declares(tmp_path: Path) -> None:
    """``[plugins.<name>]`` of ``config.toml`` is generic: the plugin host checks it, as ``piloop.toml``'s."""
    (tmp_path / 'config.toml').write_text('[plugins.verdict_rule]\nprovider = "bootstrap"\n')
    with pytest.raises(SystemExit, match=r'\[plugins.verdict_rule\]: no capability has that name; there is executor'):
        Daemon(Settings(home=tmp_path))
