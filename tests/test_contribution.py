"""Contributions: what a provider that the daemon opened, or an installed package, adds to the kernel."""

from collections.abc import Iterator
from datetime import datetime
from importlib.metadata import EntryPoint
from pathlib import Path

import cyclopts
import httpx2 as httpx
import pytest
from fastapi import APIRouter
from pydantic import BaseModel
from sqlalchemy.orm import Session

from piloop.actor import Actor, System
from piloop.api.client import Client, Refused
from piloop.api.contract import Access, Problem
from piloop.cli.main import COMMANDS, add_commands
from piloop.daemon.api import Handlers, daemon_state, openapi, with_api
from piloop.daemon.loop import Consumer, Turn
from piloop.daemon.main import DECLARED
from piloop.daemon.run_controller import RunController
from piloop.daemon.scheduler import Scheduler
from piloop.db.models import TARGET_KINDS, Campaign, Idea
from piloop.db.session import Database
from piloop.net import Route
from piloop.net.server import app, executor_router
from piloop.net.tokens import Caller, Tokens
from piloop.otel import Tracer
from piloop.plugins import contribution
from piloop.plugins.archive.contract import ArchiveHost
from piloop.plugins.archive.local import LocalArchive
from piloop.plugins.contribution import Contribution, collect
from piloop.plugins.executor.contract import JOB_KINDS, AgentFinished, AgentJob, JobKind
from piloop.plugins.executor.workers import Workers
from piloop.plugins.host import PluginError
from piloop.plugins.memory.contract import MEMORY, MemoryProvider, Synced
from piloop.plugins.memory.local import api as local_api
from piloop.plugins.network.contract import NetworkHost
from piloop.plugins.network.https import Https
from piloop.plugins.secrets.contract import HomeSecrets
from piloop.plugins.secrets.local import LocalSecrets
from piloop.settings import Settings
from tests.conftest import disk, serving_api
from tests.daemon.test_roles import forget_loaded, installed
from tests.fixtures.route_plugin import Greeting
from tests.net.test_api import token


def idle(_turn: Turn) -> datetime | None:
    return None


def idle_wake() -> None:
    return None


class Archiver:
    def contribution(self) -> Contribution:
        return Contribution(consumers=(Consumer('archiver', frozenset(), idle),))


def test_the_executor_s_upkeep_and_the_archiver_run_after_limits_and_before_the_scheduler_s_own(
    db: Database, tmp_path: Path
) -> None:
    controller = RunController(db, Tracer(None), tmp_path, executor=Workers())
    archiver = LocalArchive().open(ArchiveHost(db, disk(tmp_path), tmp_path, 0, Tracer(None), idle_wake))
    names = [c.name for c in Scheduler(db, controller, Tracer(None), tmp_path, archiver=archiver).consumers()]
    assert names == ['limits', 'executor', 'archiver', 'evaluations', 'prompts', 'wind_down', 'facts', 'campaigns']


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


NOTES = APIRouter(prefix='/v1')


@NOTES.get('/notes', operation_id='notes')
def notes() -> list[str]:
    """The notes of every campaign."""
    return []


class Notes(MemoryProvider):
    def sync(self, s: Session, campaign: Campaign, actor: Actor) -> Synced:
        return Synced(added=[], unchanged=[], skipped={})

    def contribution(self) -> Contribution:
        return Contribution(router=NOTES)


class Picky(Notes):
    folder: str


@pytest.fixture
def forgets() -> Iterator[None]:
    yield
    forget_loaded()


@pytest.mark.usefixtures('forgets')
def test_each_installed_provider_that_a_campaign_may_choose_serves_and_documents_its_routes(
    db: Database, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    installed(monkeypatch, [EntryPoint('notes', f'{__name__}:Notes', MEMORY.group)])
    controller = RunController(db, Tracer(None), tmp_path, executor=Workers())
    routers = Scheduler(db, controller, Tracer(None), tmp_path).contributions.routers
    assert routers[1:] == (local_api.router, NOTES), "after the executor's, the memory providers', in registry order"
    tokens = Tokens(db)
    network = Https().open(NetworkHost(tokens, HomeSecrets(db, LocalSecrets())))
    assert app(tokens, network, routers).url_path_for('notes') == '/v1/notes'
    paths = openapi()['paths']
    assert isinstance(paths, dict) and paths['/v1/notes']['get']['operationId'] == 'notes'
    assert not any(path.startswith('/v1/executor') for path in paths), "the executor's routes are for its workers"


@pytest.mark.usefixtures('forgets')
def test_a_provider_that_contributes_and_cannot_be_built_with_its_defaults_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    installed(monkeypatch, [EntryPoint('picky', f'{__name__}:Picky', MEMORY.group)])
    with pytest.raises(
        PluginError,
        match="^the memory provider 'picky' contributes, and cannot be built with its defaults: folder: Field required$",
    ):
        contribution.installed()


@pytest.mark.usefixtures('forgets')
def test_a_package_s_route_authenticates_and_addresses_its_campaign_with_the_module_alone(
    db: Database, campaign: Campaign, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    installed(monkeypatch, [EntryPoint('greeter', 'tests.fixtures.route_plugin:Greeter', MEMORY.group)])
    cfg = Settings(home=tmp_path)
    reader = token(db, 'ana', Access.READ)
    with serving_api(cfg), Client.on(cfg.socket_file, System.ORCHESTRATOR) as client:
        path = f'/v1/campaigns/{campaign.name}/greeting'
        assert client.post(path, None, Greeting) == Greeting(campaign=campaign.name, to='orchestrator')
        with pytest.raises(Refused) as unknown:
            client.post('/v1/campaigns/nope/greeting', None, Greeting)
        transport = httpx.HTTPTransport(uds=str(cfg.socket_file))
        with httpx.Client(transport=transport, headers={'Authorization': f'Bearer {reader}'}) as http:
            refused = http.post(f'http://piloop{path}')
    assert unknown.value.error == Problem.UNKNOWN_CAMPAIGN
    assert (refused.status_code, refused.json()) == (403, {'detail': 'ana has read access, and this route needs act'})


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
