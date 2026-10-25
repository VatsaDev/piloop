"""The API: who may call it on each listener, what it answers, how its long-poll waits, and its OpenAPI document."""

import threading
import time
from contextlib import closing
from pathlib import Path

import httpx2 as httpx
import pytest

from piloop.actor import System
from piloop.api.client import Client, NoDaemon, Refused
from piloop.api.contract import (
    ENDPOINTS,
    EVENTS,
    INBOX,
    STATUS,
    Access,
    Endpoint,
    EventsQuery,
    InboxQuery,
    Method,
    Problem,
    Selection,
)
from piloop.api.views import Status
from piloop.daemon.api import endpoints
from piloop.db.events import emit
from piloop.db.models import Campaign, UnknownEvent
from piloop.db.session import Database
from piloop.ids import TargetKind, TargetRef
from piloop.net import Served, tokens
from piloop.net.server import OPENAPI, Server, app, local_listener, openapi
from piloop.net.tokens import Caller, Tokens
from piloop.plugins.network.contract import NetworkHost
from piloop.plugins.network.https import Https
from piloop.settings import Settings
from tests.conftest import serving_api


@pytest.fixture
def cfg(tmp_path: Path) -> Settings:
    return Settings(home=tmp_path)


def token(db: Database, name: str, access: Access) -> str:
    with db.session() as s:
        secret = tokens.create(s, name, access)
        s.commit()
    return secret


def emitted(db: Database, kind: str) -> None:
    with db.session() as s:
        emit(s, UnknownEvent(kind=kind, payload={}), TargetRef(TargetKind.CAMPAIGN, 1), System.DAEMON)
        s.commit()


def test_every_endpoint_of_the_contract_has_one_handler(db: Database, cfg: Settings) -> None:
    assert [s.endpoint for s in endpoints(db, cfg, 'now', None)] == list(ENDPOINTS)


def test_on_the_socket_a_call_without_a_token_is_the_owner_and_a_token_is_its_own_caller(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    worker = token(db, 'gpu1', Access.WORKER)
    with serving_api(cfg), Client(cfg.socket_file) as owner:
        [state] = owner.call(STATUS, Selection(campaign='demo'))['campaigns']
        assert state['name'] == 'demo'
        transport = httpx.HTTPTransport(uds=str(cfg.socket_file))
        with httpx.Client(transport=transport, headers={'Authorization': f'Bearer {worker}'}) as client:
            refused = client.get('http://piloop/v1/status')
    assert (refused.status_code, refused.json()) == (
        403,
        {'detail': 'gpu1 has worker access, and this route needs read'},
    ), 'the token says who calls, wherever it comes from'


def test_on_the_network_a_caller_needs_a_token_whose_access_reads(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    reader = token(db, 'ana', Access.READ)
    with serving_api(cfg, listen='127.0.0.1:0') as [_, tcp]:
        assert httpx.get(tcp.url + STATUS.path).status_code == 401, 'only the socket knows its owner'
        answered = httpx.get(tcp.url + STATUS.path, headers={'Authorization': f'Bearer {reader}'})
        assert answered.status_code == 200 and answered.json()['daemon']['listen'] == tcp.url


def test_a_refusal_says_why_and_what_to_do(db: Database, campaign: Campaign, cfg: Settings) -> None:
    with serving_api(cfg), Client(cfg.socket_file) as client:
        with pytest.raises(Refused, match="^no campaign 'nope' in .*`piloop init <path>` registers one$") as unknown:
            client.call(INBOX, InboxQuery(campaign='nope'))
        assert unknown.value.error == Problem.UNKNOWN_CAMPAIGN
        with pytest.raises(Refused, match='^cursor 99 is ahead of the feed') as ahead:
            client.call(EVENTS, EventsQuery(after=99))
        assert ahead.value.error == Problem.CURSOR_AHEAD
        with pytest.raises(Refused, match='^limit: Input should be less than or equal to 1000$') as invalid:
            client.call(EVENTS, EventsQuery.model_construct(limit=1001))
        assert invalid.value.error is None
        missing = Endpoint('missing', Method.GET, '/v1/missing', Selection, dict, Access.READ, 'Nothing.')
        with pytest.raises(Refused, match=r'^the daemon has no /v1/missing: it runs an older piloop; `piloop restart`'):
            client.call(missing, Selection())


def test_a_relative_path_names_the_campaign_from_where_the_client_runs(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    path = Path(campaign.path)
    with serving_api(cfg), Client(cfg.socket_file) as client:
        named = client.call(STATUS, Selection(campaign=path.name, cwd=str(path.parent)))
        assert [c['id'] for c in named['campaigns']] == [campaign.id]
        inside = client.call(STATUS, Selection(cwd=str(path)))
        assert [c['id'] for c in inside['campaigns']] == [campaign.id]


def test_the_events_long_poll_answers_the_first_event_that_matches_or_its_moved_cursor_at_the_end(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    with serving_api(cfg), Client(cfg.socket_file) as client:
        start = client.call(EVENTS, EventsQuery(after=0, limit=1000))['next']
        emitted(db, 'noise')
        quiet = client.call(EVENTS, EventsQuery(after=start, kind=('wanted',), wait=0.2))
        assert quiet == {'events': [], 'next': start + 1, 'more': False}, 'the end of the wait moves the cursor'
        threading.Timer(0.2, emitted, [db, 'wanted']).start()
        woke = client.call(EVENTS, EventsQuery(after=quiet['next'], kind=('wanted',), wait=20))
        assert [e['kind'] for e in woke['events']] == ['wanted'], 'another session commits: the wait wakes on it'


def test_the_server_serves_the_openapi_document_of_the_contract(cfg: Settings, db: Database) -> None:
    with serving_api(cfg):
        transport = httpx.HTTPTransport(uds=str(cfg.socket_file))
        with httpx.Client(transport=transport) as client:
            assert client.get('http://piloop' + OPENAPI).json() == openapi()
    paths = openapi()['paths']
    assert isinstance(paths, dict) and sorted(paths) == sorted(e.path for e in ENDPOINTS)


def test_without_a_daemon_the_client_says_how_to_start_one(cfg: Settings) -> None:
    with Client(cfg.socket_file) as client, pytest.raises(NoDaemon, match='start it with `piloop serve --detach`'):
        client.call(STATUS, Selection())
    cfg.socket_file.touch()
    with Client(cfg.socket_file) as client, pytest.raises(NoDaemon, match='start it with'):
        client.call(STATUS, Selection())


def test_a_long_poll_answers_its_moved_cursor_at_once_when_the_server_closes(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    with serving_api(cfg) as [server], Client(cfg.socket_file) as client:
        start = client.call(EVENTS, EventsQuery(limit=1000))['next']
        threading.Timer(0.3, server.close).start()
        began = time.monotonic()
        quiet = client.call(EVENTS, EventsQuery(after=start, kind=('wanted',), wait=20))
    assert quiet['events'] == [] and time.monotonic() - began < 15


def test_a_handler_that_crashes_is_an_answer_that_says_so(db: Database, cfg: Settings) -> None:
    def crash(caller: Caller, params: Selection) -> Status:
        raise RuntimeError('a bug')

    tokens_ = Tokens(db)
    api = app(tokens_, Https().open(NetworkHost(tokens_)), (), [Served(STATUS, crash)])
    with closing(Server(api, local_listener(cfg.socket_file))) as server, Client(cfg.socket_file) as client:
        server.start()
        with pytest.raises(Refused, match='^the daemon answered 500: Internal Server Error$'):
            client.call(STATUS, Selection())
