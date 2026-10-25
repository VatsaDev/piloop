"""The API: who may call it on each listener, what it answers, how its long-poll waits, and its OpenAPI document."""

import threading
import time
from contextlib import closing
from pathlib import Path
from typing import Annotated, get_args, get_origin, get_type_hints

import httpx2 as httpx
import pytest
from fastapi.routing import APIRoute

from piloop.actor import System
from piloop.api.client import Client, NoDaemon, Refused
from piloop.api.contract import Access, EventsQuery, InboxQuery, Params, Problem, Selection
from piloop.api.views import Status
from piloop.daemon.api import Handlers, daemon_state, openapi, router, with_api
from piloop.db.events import emit
from piloop.db.models import Campaign, UnknownEvent
from piloop.db.session import Database
from piloop.ids import TargetKind, TargetRef
from piloop.net import tokens
from piloop.net.server import OPENAPI, Server, app, local_listener
from piloop.net.tokens import Tokens
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


def test_on_the_socket_a_call_without_a_token_is_the_owner_and_a_token_is_its_own_caller(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    worker = token(db, 'gpu1', Access.WORKER)
    with serving_api(cfg), Client.on(cfg.socket_file) as owner:
        [state] = owner.status(Selection(campaign='demo')).campaigns
        assert state.name == 'demo'
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
        assert httpx.get(tcp.url + '/v1/status').status_code == 401, 'only the socket knows its owner'
        answered_ = httpx.get(tcp.url + '/v1/status', headers={'Authorization': f'Bearer {reader}'})
        assert answered_.status_code == 200 and answered_.json()['daemon']['listen'] == tcp.url


def test_a_refusal_says_why_and_what_to_do(db: Database, campaign: Campaign, cfg: Settings) -> None:
    with serving_api(cfg), Client.on(cfg.socket_file) as client:
        with pytest.raises(Refused, match="^no campaign 'nope' in .*`piloop init <path>` registers one$") as unknown:
            client.inbox(InboxQuery(campaign='nope'))
        assert unknown.value.error == Problem.UNKNOWN_CAMPAIGN
        with pytest.raises(Refused, match='^cursor 99 is ahead of the feed') as ahead:
            client.events(EventsQuery(after=99))
        assert ahead.value.error == Problem.CURSOR_AHEAD
        with pytest.raises(Refused, match='^limit: Input should be less than or equal to 1000$') as invalid:
            client.events(EventsQuery.model_construct(limit=1001))
        assert invalid.value.error is None
        with pytest.raises(Refused, match=r'^the daemon has no such route \(Not Found\): it runs an older piloop'):
            client._get('/v1/missing', Selection(), Status)


def test_a_relative_path_names_the_campaign_from_where_the_client_runs(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    path = Path(campaign.path)
    with serving_api(cfg), Client.on(cfg.socket_file) as client:
        named = client.status(Selection(campaign=path.name, cwd=str(path.parent)))
        assert [c.id for c in named.campaigns] == [campaign.id]
        inside = client.status(Selection(cwd=str(path)))
        assert [c.id for c in inside.campaigns] == [campaign.id]


def test_the_events_long_poll_answers_the_first_event_that_matches_or_its_moved_cursor_at_the_end(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    with serving_api(cfg), Client.on(cfg.socket_file) as client:
        start = client.events(EventsQuery(limit=1000)).next
        emitted(db, 'noise')
        quiet = client.events(EventsQuery(after=start, kind=('wanted',), wait=0.2))
        assert (quiet.events, quiet.next, quiet.more) == ([], start + 1, False), 'the end of the wait moves the cursor'
        threading.Timer(0.2, emitted, [db, 'wanted']).start()
        woke = client.events(EventsQuery(after=quiet.next, kind=('wanted',), wait=20))
        assert [e.kind for e in woke.events] == ['wanted'], 'another session commits: the wait wakes on it'


def test_a_long_poll_answers_its_moved_cursor_at_once_when_the_server_closes(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    with serving_api(cfg) as [server], Client.on(cfg.socket_file) as client:
        start = client.events(EventsQuery(limit=1000)).next
        threading.Timer(0.3, server.close).start()
        began = time.monotonic()
        quiet = client.events(EventsQuery(after=start, kind=('wanted',), wait=20))
    assert quiet.events == [] and time.monotonic() - began < 15


def test_the_server_serves_the_openapi_document_of_its_routes(cfg: Settings, db: Database) -> None:
    with serving_api(cfg), Client.on(cfg.socket_file) as client:
        assert client._http.get(OPENAPI).json() == openapi()
    paths = openapi()['paths']
    assert isinstance(paths, dict) and sorted(paths) == sorted(
        f'/v1/{r}' for r in ('status', 'overview', 'inbox', 'events')
    )


def test_a_route_that_crashes_is_an_answer_that_says_so(db: Database, cfg: Settings) -> None:
    def crash() -> None:
        raise RuntimeError('a bug')

    tokens_ = Tokens(db)
    server = app(tokens_, Https().open(NetworkHost(tokens_)), ())
    server.add_api_route('/v1/status', crash)
    api = with_api(server, Handlers(db, cfg, daemon_state('now', None)))
    with closing(Server(api, local_listener(cfg.socket_file))) as running:
        running.start()
        with (
            pytest.raises(Refused, match='^the daemon answered 500: Internal Server Error$'),
            Client.on(cfg.socket_file) as client,
        ):
            client.status(Selection())


def test_without_a_daemon_the_client_says_how_to_start_one(cfg: Settings) -> None:
    with pytest.raises(NoDaemon, match='start it with `piloop serve --detach`'), Client.on(cfg.socket_file):
        pass
    cfg.socket_file.touch()
    with pytest.raises(NoDaemon, match='start it with'), Client.on(cfg.socket_file):
        pass


def test_each_method_of_the_client_calls_its_route_with_the_route_s_models() -> None:
    """The client and the routes share their models: a method sends its route's query model to its route's path."""
    sent: list[httpx.Request] = []

    def record(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return httpx.Response(500, text='only the request counts')

    client = Client(httpx.Client(transport=httpx.MockTransport(record), base_url='http://piloop'))
    for route in router.routes:
        assert isinstance(route, APIRoute)
        hints = get_type_hints(getattr(client, route.name))
        query = hints['query']
        assert query in _queries(route) and hints['return'] is route.response_model, route.name
        with pytest.raises(Refused):
            getattr(client, route.name)(query())
        assert ({sent[-1].method}, sent[-1].url.path) == (route.methods, route.path), route.name


def _queries(route: APIRoute) -> set[type]:
    """The query models that the route and its dependencies take."""
    found: set[type] = set()
    for call in (route.endpoint, *(d.call for d in route.dependant.dependencies if d.call is not None)):
        for hint in get_type_hints(call, include_extras=True).values():
            model = get_args(hint)[0] if get_origin(hint) is Annotated else hint
            if isinstance(model, type) and issubclass(model, Params):
                found.add(model)
    return found
