"""Contributions: what a provider that the daemon opened, or an installed package, adds to the kernel."""

from datetime import datetime
from importlib.metadata import EntryPoint
from pathlib import Path

import cyclopts
import pytest
from fastapi import APIRouter
from pydantic import BaseModel

from piloop.cli.main import COMMANDS, add_commands
from piloop.daemon.api import Handlers, daemon_state, with_api
from piloop.daemon.loop import Consumer, Turn
from piloop.daemon.main import DECLARED
from piloop.daemon.run_controller import RunController
from piloop.daemon.scheduler import Scheduler
from piloop.db.models import TABLES, TARGET_KINDS, Idea
from piloop.db.session import Database
from piloop.net import Route
from piloop.net.server import app, executor_router
from piloop.net.tokens import Caller, Tokens
from piloop.otel import Tracer
from piloop.plugins.contribution import Contribution, collect
from piloop.plugins.executor.contract import JOB_KINDS, AgentFinished, AgentJob, JobKind
from piloop.plugins.executor.workers import Workers
from piloop.plugins.host import PluginError
from piloop.plugins.network.contract import NetworkHost
from piloop.plugins.network.https import Https
from piloop.plugins.secrets.contract import HomeSecrets
from piloop.plugins.secrets.local import LocalSecrets
from piloop.settings import Settings
from tests.conftest import disk
from tests.daemon.test_roles import installed


def idle(_turn: Turn) -> datetime | None:
    return None


class Archiver:
    def contribution(self) -> Contribution:
        return Contribution(consumers=(Consumer('archiver', frozenset(), idle),))


def test_the_executor_s_upkeep_runs_after_limits_and_before_the_scheduler_s_own(db: Database, tmp_path: Path) -> None:
    controller = RunController(db, Tracer(None), tmp_path, executor=Workers())
    names = [c.name for c in Scheduler(db, controller, Tracer(None), tmp_path).consumers()]
    assert names == ['limits', 'executor', 'evaluations', 'prompts', 'wind_down', 'facts', 'archiver', 'campaigns']


def test_a_consumer_name_that_the_daemon_or_another_contribution_has_is_refused() -> None:
    with pytest.raises(PluginError, match="Archiver contributes the consumer 'archiver', which the daemon has already"):
        collect([Archiver()], ['archiver'])
    with pytest.raises(PluginError, match="Archiver contributes the consumer 'archiver', which Archiver has already"):
        collect([Archiver(), Archiver()], [])


class Ask(BaseModel):
    pass


def nothing(_caller: Caller, _body: Ask) -> None:
    return None


def status() -> None:
    return None


def test_a_route_that_another_router_or_the_api_has_is_refused(db: Database, tmp_path: Path) -> None:
    tokens = Tokens(db)
    network = Https().open(NetworkHost(tokens, HomeSecrets(db, LocalSecrets())))
    echo = executor_router([Route('echo', Ask, nothing)])
    assert app(tokens, network, [echo]).url_path_for('executor echo') == '/v1/executor/echo'
    with pytest.raises(PluginError, match='POST /v1/executor/echo is both executor echo and executor echo'):
        app(tokens, network, [echo, executor_router([Route('echo', Ask, nothing)])])
    shadow = APIRouter(prefix='/v1')
    shadow.add_api_route('/status', status, methods=['GET'], name='shadow')
    api = Handlers(db, Settings(home=tmp_path), daemon_state('now', None), disk(tmp_path))
    with pytest.raises(PluginError, match='GET /v1/status is both shadow and status'):
        with_api(app(tokens, network, [shadow]), api)


def hello() -> str:
    """Say hello."""
    return 'hello'


def test_a_package_adds_a_command_by_its_path_and_one_with_a_name_of_piloop_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    installed(monkeypatch, [EntryPoint('hello', f'{__name__}:hello', COMMANDS.group)])
    cli = cyclopts.App()
    add_commands(cli)
    assert cli(['hello'], result_action='return_value') == 'hello'
    assert 'idea' in cli, "piloop's own commands are entry points of the same group"
    installed(monkeypatch, [EntryPoint('status', f'{__name__}:hello', COMMANDS.group)])
    taken = cyclopts.App()
    taken.command(hello, name='status')
    with pytest.raises(PluginError, match=f"status = {__name__}:hello in piloop.commands: 'status' is a command"):
        add_commands(taken)
    installed(monkeypatch, [EntryPoint('idea', f'{__name__}:hello', COMMANDS.group)])
    with pytest.raises(PluginError, match=r"'idea' is a command already, from piloop \d"):
        add_commands(cyclopts.App())


def check_declared() -> None:
    """What ``preflight`` checks of the packages' declarations."""
    for slot in DECLARED:
        slot.loaded()


def test_a_package_s_table_is_checked_when_the_daemon_starts(monkeypatch: pytest.MonkeyPatch) -> None:
    installed(monkeypatch, [EntryPoint('ideas', 'piloop.db.models:Idea', TABLES.group)])
    check_declared()
    assert TABLES.loaded() == {'ideas': Idea}
    installed(monkeypatch, [EntryPoint('ideas', f'{__name__}:Ask', TABLES.group)])
    with pytest.raises(PluginError, match='piloop.tables is not a table: Input should be a subclass of Base'):
        check_declared()
    installed(monkeypatch, [EntryPoint('notions', 'piloop.db.models:Idea', TABLES.group)])
    with pytest.raises(PluginError, match="in piloop.tables: it names the table 'ideas': the names must be the same"):
        check_declared()


LATTICE = JobKind(AgentJob, AgentFinished)


def test_a_package_s_job_kind_is_checked_when_the_daemon_starts_and_one_of_piloop_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    installed(monkeypatch, [EntryPoint('lattice', f'{__name__}:LATTICE', JOB_KINDS.group)])
    check_declared()
    assert JOB_KINDS.loaded() == {'lattice': LATTICE}
    installed(monkeypatch, [EntryPoint('evaluation', f'{__name__}:LATTICE', JOB_KINDS.group)])
    with pytest.raises(PluginError, match="piloop.job_kinds: piloop runs jobs of the kind 'evaluation' already"):
        check_declared()


def test_a_package_s_target_kind_is_checked_when_the_daemon_starts_and_one_of_piloop_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    installed(monkeypatch, [EntryPoint('notion', 'piloop.db.models:Idea', TARGET_KINDS.group)])
    check_declared()
    assert TARGET_KINDS.loaded() == {'notion': Idea}
    installed(monkeypatch, [EntryPoint('idea', 'piloop.db.models:Idea', TARGET_KINDS.group)])
    with pytest.raises(PluginError, match="piloop.target_kinds: comments and events point at 'idea' of piloop already"):
        check_declared()
