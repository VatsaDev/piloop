"""The API: who may call it on each listener, what it answers, how its long-poll waits, and its OpenAPI document."""

import errno
import os
import threading
import time
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, get_args, get_origin, get_type_hints

import httpx2 as httpx
import pytest
from fastapi.dependencies.models import Dependant
from fastapi.routing import APIRoute
from pytest_mock import MockerFixture
from rich.console import Console
from sqlalchemy import select

from piloop.actor import Agent, System
from piloop.api import client as client_module
from piloop.api.client import Client, NoDaemon, Refused
from piloop.api.contract import (
    Access,
    AckBody,
    EventsQuery,
    IdeaBody,
    InboxQuery,
    NewsQuery,
    Params,
    PauseBody,
    Problem,
    Reasoned,
    ReplyBody,
    Selection,
    StatusBody,
    StopBody,
    defaulted,
)
from piloop.api.views import Status, thread_lines
from piloop.cli import common, main as cli
from piloop.daemon.api import Handlers, daemon_state, openapi, router, with_api
from piloop.db.events import emit
from piloop.db.models import Campaign, Comment, Event, UnknownEvent, utcnow
from piloop.db.session import Database
from piloop.ids import (
    AgentSessionId,
    CampaignStatus,
    CommentId,
    CommentKind,
    IdeaId,
    IdeaStatus,
    LinkKind,
    RoleName,
    TargetKind,
    TargetRef,
)
from piloop.net import tokens
from piloop.net.server import OPENAPI, Server, app, local_listener
from piloop.net.tokens import Tokens
from piloop.plugins.network.contract import NetworkHost
from piloop.plugins.network.https import ACTOR_HEADER, TOKEN_ENV, Https
from piloop.plugins.network.proxy import Proxy
from piloop.plugins.secrets.contract import HomeSecrets
from piloop.plugins.secrets.local import LocalSecrets
from piloop.settings import Settings
from piloop.watch.panels import Context, InboxPanel
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
    with serving_api(cfg), Client.on(cfg.socket_file, System.HUMAN) as owner:
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
    with serving_api(cfg), Client.on(cfg.socket_file, System.HUMAN) as client:
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
    with serving_api(cfg), Client.on(cfg.socket_file, System.HUMAN) as client:
        named = client.status(Selection(campaign=path.name, cwd=str(path.parent)))
        assert [c.id for c in named.campaigns] == [campaign.id]
        inside = client.status(Selection(cwd=str(path)))
        assert [c.id for c in inside.campaigns] == [campaign.id]


def test_the_events_long_poll_answers_the_first_event_that_matches_or_its_moved_cursor_at_the_end(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    with serving_api(cfg), Client.on(cfg.socket_file, System.HUMAN) as client:
        start = client.events(EventsQuery(limit=1000)).next
        emitted(db, 'noise')
        quiet = client.events(EventsQuery(after=start, kind=('wanted',), wait=0.2))
        assert (quiet.events, quiet.next, quiet.more) == ([], start + 1, False), 'the end of the wait moves the cursor'
        threading.Timer(0.2, emitted, [db, 'wanted']).start()
        woke = client.events(EventsQuery(after=quiet.next, kind=('wanted',), wait=20))
        assert [e.kind for e in woke.events] == ['wanted'], 'another session commits: the wait wakes on it'


@pytest.mark.parametrize('poll', ['events', 'news'])
def test_a_long_poll_answers_its_moved_cursor_at_once_when_the_server_closes(
    db: Database, campaign: Campaign, cfg: Settings, poll: str
) -> None:
    with serving_api(cfg) as [server], Client.on(cfg.socket_file, System.HUMAN) as client:
        threading.Timer(0.3, server.close).start()
        began = time.monotonic()
        match poll:
            case 'events':
                assert client.events(EventsQuery(kind=('wanted',), limit=1000, wait=20)).more is False
            case _:
                assert client.news(NewsQuery(wait=20)).items == []
    assert time.monotonic() - began < 15


def test_the_server_serves_the_openapi_document_of_its_routes(cfg: Settings, db: Database) -> None:
    with serving_api(cfg), Client.on(cfg.socket_file, System.HUMAN) as client:
        assert client._http.get(OPENAPI).json() == openapi()
    paths = openapi()['paths']
    assert isinstance(paths, dict) and sorted(paths) == sorted(
        {r.path for r in router.routes if isinstance(r, APIRoute)}
    )


def test_a_route_that_crashes_is_an_answer_that_says_so(db: Database, cfg: Settings) -> None:
    def crash() -> None:
        raise RuntimeError('a bug')

    tokens_ = Tokens(db)
    server = app(tokens_, Https().open(NetworkHost(tokens_, HomeSecrets(db, LocalSecrets()))), ())
    server.add_api_route('/v1/status', crash)
    api = with_api(server, Handlers(db, cfg, daemon_state('now', None)))
    with closing(Server(api, local_listener(cfg.socket_file))) as running:
        running.start()
        with (
            pytest.raises(Refused, match='^the daemon answered 500: Internal Server Error$'),
            Client.on(cfg.socket_file, System.HUMAN) as client,
        ):
            client.status(Selection())


@pytest.mark.parametrize(('code', 'answers'), [(errno.ENOTSOCK, False), (errno.EACCES, None)])
def test_a_file_that_is_no_socket_is_no_daemon_as_macos_says_it(
    tmp_path: Path, mocker: MockerFixture, code: int, answers: bool | None
) -> None:
    """macOS refuses to connect to a file that is no socket with ``ENOTSOCK``, where Linux refuses the connection.
    Another error is raised."""
    mocker.patch.object(client_module.sockets.socket, 'connect', side_effect=OSError(code, os.strerror(code)))
    if answers is None:
        with pytest.raises(PermissionError):
            client_module.answers(tmp_path / 'piloop.sock')
    else:
        assert client_module.answers(tmp_path / 'piloop.sock') is answers


def test_without_a_daemon_the_client_says_how_to_start_one(cfg: Settings) -> None:
    with (
        pytest.raises(NoDaemon, match='start it with `piloop serve --detach`'),
        Client.on(cfg.socket_file, System.HUMAN),
    ):
        pass
    cfg.socket_file.touch()
    with pytest.raises(NoDaemon, match='start it with'), Client.on(cfg.socket_file, System.HUMAN):
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
        answer, taken = hints.pop('return'), list(hints.items())
        path = _path_params(route.dependant)
        assert answer is route.response_model, route.name
        assert {n for n, _ in taken if n in path} == path, f"{route.name}: the route's path parameters"
        assert all(t in _queries(route) for n, t in taken if n not in path), f"{route.name}: the route's query model"
        with pytest.raises(Refused):
            getattr(client, route.name)(*('1' if n in path else t.model_construct() for n, t in taken))
        sent_path = route.path.format(**dict.fromkeys(path, 1))
        assert ({sent[-1].method}, sent[-1].url.path) == (route.methods, sent_path), route.name


def _path_params(dependant: Dependant) -> set[str]:
    """The path parameters that the route and its dependencies take."""
    return {p.name for p in dependant.path_params}.union(*(_path_params(d) for d in dependant.dependencies))


def _queries(route: APIRoute) -> set[type]:
    """The query models that the route and its dependencies take."""
    found: set[type] = set()
    for call in (route.endpoint, *(d.call for d in route.dependant.dependencies if d.call is not None)):
        for hint in get_type_hints(call, include_extras=True).values():
            model = get_args(hint)[0] if get_origin(hint) is Annotated else hint
            if isinstance(model, type) and issubclass(model, Params):
                found.add(model)
    return found


def comment(db: Database, author: str, body: str) -> None:
    with db.session() as s:
        s.add(
            Comment(
                target=TargetRef(TargetKind.CAMPAIGN, 1),
                author=author,
                kind=CommentKind.DECISION,
                priority=2,
                body=body,
            )
        )
        s.commit()


def test_the_news_long_poll_wakes_on_what_someone_else_writes_and_skips_what_the_caller_wrote(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    with serving_api(cfg), Client.on(cfg.socket_file, System.ORCHESTRATOR) as orchestrator:
        start = orchestrator.news(NewsQuery()).next
        comment(db, 'orchestrator', 'my own decision')
        mine = orchestrator.news(NewsQuery(after=start, wait=0.2))
        assert mine.items == [] and mine.next > start, (
            'its own writes are not news to it, and the cursor moves past them'
        )
        threading.Timer(0.2, comment, [db, 'watchdog#2', 'look at this']).start()
        [item] = orchestrator.news(NewsQuery(after=mine.next, wait=20)).items
        assert item.text() == 'comment#2 decision p2 on campaign#1 (demo) by watchdog#2: look at this'
        title, lines = thread_lines(orchestrator.thread(CommentId(2)), datetime.now(UTC))
        assert title.startswith('comment#2 decision on campaign#1 by watchdog#2, ') and lines == ['look at this']
        with pytest.raises(Refused, match='^no comment#99$') as unknown:
            orchestrator.thread(CommentId(99))
        assert unknown.value.error == Problem.UNKNOWN_COMMENT


def test_a_caller_acts_as_itself_or_the_orchestrator_and_only_admin_as_an_agent(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    reader = {'Authorization': f'Bearer {token(db, "ana", Access.READ)}'}
    with serving_api(cfg, listen='127.0.0.1:0') as [_, tcp]:
        news = tcp.url + '/v1/inbox/news'
        for named in (None, 'human', 'human:ana', 'orchestrator'):
            assert httpx.get(news, headers={**reader, **({ACTOR_HEADER: named} if named else {})}).status_code == 200
        agent = httpx.get(news, headers={**reader, ACTOR_HEADER: 'implementer#12'})
        assert agent.json() == {'detail': 'ana has read access, which may not act as implementer#12'}
        other = httpx.get(news, headers={**reader, ACTOR_HEADER: 'human:bob'})
        assert other.json() == {'detail': 'ana has read access, which may not act as human:bob'}
        assert httpx.get(news, headers={**reader, ACTOR_HEADER: 'implementer#'}).status_code == 422
        with Client.on(cfg.socket_file, Agent(role=RoleName('implementer'), session=AgentSessionId(12))) as owner:
            assert owner.news(NewsQuery()).items == [], 'the owner on the socket may name an agent'


PROXY = Proxy.model_validate(
    {
        'listen': '127.0.0.1:0',
        'claims': {'principal': 'x-principal-id'},
        'name_claim': 'principal',
        'rules': [{'when': {}, 'access': 'act'}],
    }
)


def test_a_proxy_caller_s_ack_reply_and_pause_sign_as_the_person_it_names(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    comment(db, 'watchdog#2', 'look at this')
    stamped = {'x-principal-id': 'ana@example.com'}
    with serving_api(cfg, network_config=PROXY) as [_, tcp], httpx.Client(base_url=tcp.url, headers=stamped) as http:
        client = Client(http)
        [acked] = client.ack(AckBody(ids=(CommentId(1),))).results
        replied = client.reply(CommentId(1), ReplyBody(body='go ahead')).comment
        paused = client.pause('demo', PauseBody(reason='waits for comment#2')).campaign
        with httpx.Client(base_url=tcp.url, headers={**stamped, PROXY.actor_header: 'orchestrator'}) as asking:
            acted = Client(asking).reply(CommentId(1), ReplyBody(body='mine')).comment
    me = 'human:ana@example.com'
    assert (acked.acked_by, replied.author, paused.paused) == (me, me, 'launches')
    assert acted.author == 'orchestrator', 'a person may still act as the orchestrator'
    with db.session() as s:
        row = s.get_one(Campaign, campaign.id)
        assert (row.updated_by, s.get_one(Comment, 1).acked_by) == (me, me)
        signed = {(e.kind, e.actor) for e in s.scalars(select(Event).where(Event.id > 1))}
        rows = InboxPanel().items(s, Context(row, row.config, utcnow(), System.HUMAN))
    assert signed == {
        ('campaign.paused', me),
        ('comment.acked_at', me),
        ('comment.decision', me),
        ('comment.decision', 'orchestrator'),
    }, 'the change log signs as the caller'
    assert {r.cells[2] for r in rows} >= {me, 'orchestrator'}, 'watch shows who replied'


def test_a_write_answers_its_cursor_and_refuses_what_its_operation_refuses(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    with serving_api(cfg), Client.on(cfg.socket_file, System.HUMAN) as client:
        added = client.add_idea('demo', IdeaBody(title='try a cache', priority=1))
        assert (added.idea.title, added.idea.status, added.idea.created_by, added.review) == (
            'try a cache',
            'ready',
            'human',
            None,
        )
        assert client.events(EventsQuery(after=added.cursor)).events == [], 'the cursor passes the write itself'
        paused = client.pause('demo', PauseBody(reason='a review'))
        with pytest.raises(Refused, match=r'^campaign demo is paused already') as twice:
            client.pause('demo', PauseBody(reason='again'))
        assert twice.value.error == Problem.REFUSED
        assert client.resume('demo').campaign.paused is None and paused.campaign.paused == 'launches'
        stopped = client.stop('demo', StopBody(reason='done'))
        assert stopped.cursor > paused.cursor
        with pytest.raises(Refused, match='^no campaign .nope.') as unknown:
            client.resume('nope')
        assert unknown.value.error == Problem.UNKNOWN_CAMPAIGN
        with pytest.raises(Refused, match='^no comment#9$') as missing:
            client.reply(CommentId(9), ReplyBody(body='hm'))
        assert missing.value.error == Problem.UNKNOWN_COMMENT
        with pytest.raises(Refused, match='^no idea#9$') as no_idea:
            client.set_status(IdeaId(9), StatusBody(status=IdeaStatus.PARKED, reason='hm'))
        with pytest.raises(Refused, match='^no idea#9$') as no_target:
            client.link(added.idea.id, LinkKind.AFTER, IdeaId(9), Reasoned(reason='hm'))
        assert no_idea.value.error == no_target.value.error == Problem.UNKNOWN_IDEA
        [result] = client.ack(AckBody(ids=(CommentId(9),))).results
        assert result.line() == 'comment#9 is not in the inbox'


def test_a_write_needs_act_access_and_a_caller_whose_name_signs(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    reader = {'Authorization': f'Bearer {token(db, "ana", Access.READ)}'}
    spaced = {'Authorization': f'Bearer {token(db, "two words", Access.ACT)}'}
    with serving_api(cfg, listen='127.0.0.1:0') as [_, tcp]:
        refused = httpx.post(tcp.url + '/v1/campaigns/demo/resume', headers=reader)
        unsigned = httpx.post(tcp.url + '/v1/campaigns/demo/resume', headers=spaced)
    assert refused.json() == {'detail': 'ana has read access, and this route needs act'}
    assert unsigned.json() == {'detail': "'two words' is no name that signs a write: name the caller another way"}


def test_without_a_campaign_named_status_answers_every_campaign(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    with db.session() as s:
        s.add(Campaign(name='other', path='/elsewhere', status=CampaignStatus.STOPPED, config=campaign.config))
        s.commit()
    with serving_api(cfg), Client.on(cfg.socket_file, System.HUMAN) as client:
        listed = client.status(Selection()).campaigns
    assert [c.name for c in listed] == ['demo', 'other'], 'a client that lists campaigns sees them all'
    assert [c.name for c in defaulted(listed)] == ['demo'], 'the CLI shows the only active one'


def test_with_a_server_set_the_commands_call_it_through_the_network_with_its_credential(
    db: Database, campaign: Campaign, cfg: Settings, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv(TOKEN_ENV, token(db, 'laptop', Access.READ))
    with serving_api(cfg, listen='127.0.0.1:0') as [_, tcp]:
        monkeypatch.setattr(common, 'settings', Settings(home=cfg.home / 'elsewhere', server=tcp.url))
        monkeypatch.setattr(common, 'console', Console(width=200, force_terminal=False))
        cli.app(['status', 'demo', '--json'], result_action='return_value', exit_on_error=False)
    assert '"name": "demo"' in capsys.readouterr().out, 'a name resolves on the server; no socket of this home'
