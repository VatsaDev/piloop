"""The API: who may call it on each listener, what it answers, how its long-poll waits, and its OpenAPI document."""

import errno
import os
import subprocess
import threading
import time
from contextlib import closing
from datetime import UTC, datetime, timedelta
from http import HTTPStatus
from pathlib import Path
from types import UnionType
from typing import Annotated, TypeAliasType, get_args, get_origin, get_type_hints

import httpx2 as httpx
import pytest
from fastapi.dependencies.models import Dependant
from fastapi.routing import APIRoute
from pydantic import BaseModel, SecretStr
from pytest_mock import MockerFixture
from rich.console import Console
from sqlalchemy import select, update

from piloop.actor import Agent, System
from piloop.agents.results import PromptOutcome
from piloop.api import client as client_module
from piloop.api.client import SOCKET_ACTOR_HEADER, Client, NoDaemon, Refused
from piloop.api.contract import (
    MAX_POLL_SECONDS,
    Access,
    AckBody,
    CancelBody,
    Checkout,
    Clone,
    EventsQuery,
    IdeaBody,
    InboxQuery,
    LaunchBody,
    LimitBody,
    Located,
    MessageBody,
    NewsQuery,
    Params,
    PauseBody,
    Problem,
    PromptBody,
    Reasoned,
    Remote,
    ReplyBody,
    RetireBody,
    SecretBody,
    Selection,
    StatusBody,
    StopBody,
    TokenBody,
    defaulted,
)
from piloop.api.views import AckOutcome, CampaignState, InvalidConfig, Pending, Queued, Relation, Status, thread_lines
from piloop.campaign import ops
from piloop.campaign.stop import CampaignClosed
from piloop.cli import common, main as cli
from piloop.daemon.api import ROUTERS, Handlers, daemon_state, openapi, with_api
from piloop.db.events import emit
from piloop.db.models import (
    EVENT,
    AgentSession,
    AgentSessionState,
    Campaign,
    Comment,
    Evaluation,
    Event,
    EventModel,
    Pause,
    RunStatus,
    UnknownEvent,
    utcnow,
)
from piloop.db.session import Database
from piloop.db.triggers.changelog import CampaignPausedChanged, CampaignStatusChanged
from piloop.ids import (
    AgentSessionId,
    CampaignStatus,
    CandidateId,
    CommentId,
    CommentKind,
    Ending,
    IdeaId,
    IdeaStatus,
    LinkKind,
    RoleName,
    RunId,
    TargetKind,
    TargetRef,
)
from piloop.net import tokens
from piloop.net.server import OPENAPI, Server, app, local_listener
from piloop.net.tokens import Tokens
from piloop.plugins.contribution import collect, contributions_of, installed
from piloop.plugins.memory.local.api import JournalBody, Journaled
from piloop.plugins.memory.local.tables import Journal
from piloop.plugins.network.contract import NetworkHost
from piloop.plugins.network.https import ACTOR_HEADER, TOKEN_ENV, Https
from piloop.plugins.network.proxy import Proxy, Rule
from piloop.plugins.secrets.contract import HomeSecrets
from piloop.plugins.secrets.local import LocalSecrets
from piloop.plugins.vcs.git import Git
from piloop.scoring.api import CancelledBy, ImportBody, RemeasureBody, RunningSince, ScoringClient
from piloop.scoring.ledger import MeasurementRecorded
from piloop.settings import Settings
from piloop.watch.panels import Context, InboxPanel
from tests.campaign.test_stop import add_run
from tests.conftest import disk, home_cas, measuring, serving_api, store_config


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


@pytest.mark.parametrize('poll', ['events', 'news', 'prompt'])
def test_a_long_poll_answers_its_moved_cursor_at_once_when_the_server_closes(
    db: Database, campaign: Campaign, cfg: Settings, poll: str
) -> None:
    with db.session() as s:
        s.add(AgentSession(kind='research', campaign_id=1, state=AgentSessionState.RUNNING))
        s.commit()
    with serving_api(cfg) as [server], Client.on(cfg.socket_file, System.HUMAN) as client:
        threading.Timer(0.3, server.close).start()
        began = time.monotonic()
        match poll:
            case 'events':
                assert client.events(EventsQuery(kind=('wanted',), limit=1000, wait=20)).more is False
            case 'news':
                assert client.news(NewsQuery(wait=20)).items == []
            case _:
                assert client.prompt(AgentSessionId(1), PromptBody(text='x', wait=20)).delivery == Pending()
    assert time.monotonic() - began < 15


def test_the_server_serves_the_openapi_document_of_its_routes(cfg: Settings, db: Database) -> None:
    with serving_api(cfg), Client.on(cfg.socket_file, System.HUMAN) as client:
        assert client._http.get(OPENAPI).json() == openapi()
    paths = openapi()['paths']
    routers = (*ROUTERS, *collect(contributions_of(installed()), ()).routers)
    assert isinstance(paths, dict) and sorted(paths) == sorted(
        {r.path for routes in routers for r in routes.routes if isinstance(r, APIRoute)}
    )
    assert '/v1/campaigns/{campaign}/journal' in paths, 'the route of local, the default memory provider'


def test_a_route_that_crashes_is_an_answer_that_says_so(db: Database, cfg: Settings) -> None:
    def crash() -> None:
        raise RuntimeError('a bug')

    tokens_ = Tokens(db)
    server = app(tokens_, Https().open(NetworkHost(tokens_, HomeSecrets(db, LocalSecrets()))), ())
    server.add_api_route('/v1/status', crash)
    api = with_api(server, Handlers(db, cfg, daemon_state('now', None), disk(cfg.home)))
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

    client = ScoringClient(httpx.Client(transport=httpx.MockTransport(record), base_url='http://piloop'))
    for route in [r for routes in ROUTERS for r in routes.routes]:
        assert isinstance(route, APIRoute)
        hints = get_type_hints(getattr(client, route.name))
        answer, taken = hints.pop('return'), list(hints.items())
        path = _path_params(route.dependant)
        assert answer is route.response_model, route.name
        assert {n for n, _ in taken if n in path} == path, f"{route.name}: the route's path parameters"
        assert all({*_models(t)} <= _queries(route) for n, t in taken if n not in path), (
            f"{route.name}: the route's query model"
        )
        with pytest.raises(Refused):
            getattr(client, route.name)(*('1' if n in path else _models(t)[0].model_construct() for n, t in taken))
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
            found.update(model for model in _models(hint) if issubclass(model, Params))
    return found


def _models(hint: object) -> list[type[BaseModel]]:
    """The models of a hint: itself, the one that it annotates, or each of a union's, as a type alias names them."""
    match hint:
        case TypeAliasType():
            return _models(hint.__value__)
        case UnionType():
            return [model for arg in get_args(hint) for model in _models(arg)]
        case type() if issubclass(hint, BaseModel):
            return [hint]
        case _ if get_origin(hint) is Annotated:
            return _models(get_args(hint)[0])
        case _:
            return []


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


def test_a_caller_acts_as_itself_or_the_orchestrator_and_only_an_agent_s_token_as_that_agent(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    reader = {'Authorization': f'Bearer {token(db, "ana", Access.READ)}'}
    with serving_api(cfg, listen='127.0.0.1:0') as [_, tcp]:
        news = tcp.url + '/v1/inbox/news'
        for named in (None, 'human', 'human:ana', 'orchestrator'):
            assert httpx.get(news, headers={**reader, **({ACTOR_HEADER: named} if named else {})}).status_code == 200
        agent = httpx.get(news, headers={**reader, ACTOR_HEADER: 'implementer#12'})
        assert agent.json() == {
            'error': 'forbidden',
            'detail': 'ana may not act as implementer#12: only the token of implementer#12 acts as it',
        }
        other = httpx.get(news, headers={**reader, ACTOR_HEADER: 'human:bob'})
        assert other.json() == {'error': 'forbidden', 'detail': 'ana has read access, which may not act as human:bob'}
        assert httpx.get(news, headers={**reader, ACTOR_HEADER: 'implementer#'}).status_code == 422


def test_an_agent_s_token_acts_as_its_agent_only_and_the_owner_on_the_socket_as_anyone_else(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    """The owner acts as the orchestrator with no setup. An agent's token on the socket is that agent, whatever it asks."""
    with db.session() as s:
        live = [AgentSession(kind='watchdog', campaign_id=campaign.id, state=AgentSessionState.RUNNING) for _ in '12']
        s.add_all(live)
        s.flush()
        [bound, older] = [Agent(role=RoleName('watchdog'), session=w.id) for w in live]
        secret = tokens.mint_agent(s, bound).get_secret_value()
        s.commit()
    uds = httpx.HTTPTransport(uds=str(cfg.socket_file))
    with serving_api(cfg), httpx.Client(transport=uds, base_url='http://piloop') as http:

        def journal(text: str, named: str | None, *, bearer: str | None = None) -> httpx.Response:
            headers = {
                **({SOCKET_ACTOR_HEADER: named} if named else {}),
                **({'Authorization': f'Bearer {bearer}'} if bearer else {}),
            }
            return http.post(f'/v1/campaigns/{campaign.name}/journal', json={'line': text}, headers=headers)

        for named in (None, 'human', 'orchestrator', 'human:ana'):
            assert journal(f'owner as {named}', named).status_code == 200, 'the owner needs no setup'
        for named in (None, 'human', str(bound)):
            assert journal(f'agent as {named}', named, bearer=secret).status_code == 200
        for named in ('orchestrator', 'human:ana', str(older)):
            refused = journal('posing', named, bearer=secret)
            assert refused.json() == {'error': 'forbidden', 'detail': f'{bound} acts only as itself, not as {named}'}
        assert journal('posing', str(bound)).json() == {
            'error': 'forbidden',
            'detail': f'owner may not act as {bound}: only the token of {bound} acts as it',
        }, 'without its token, nobody acts as an agent'
        assert journal('an agent of the daemon before', str(older)).status_code == 200, 'until 0.5.0'
        assert journal('no such agent', 'watchdog#99').status_code == 403
    with db.session() as s:
        signed = {(j.line, j.author) for j in s.scalars(select(Journal))}
    assert signed >= {
        ('owner as orchestrator', 'orchestrator'),
        ('owner as None', 'human'),
        ('agent as None', str(bound)),
        ('agent as human', str(bound)),
        (f'agent as {bound}', str(bound)),
        ('an agent of the daemon before', str(older)),
    }
    assert not {line for line, _ in signed} & {'posing', 'no such agent'}


PROXY = Proxy.model_validate(
    {
        'listen': '127.0.0.1:0',
        'claims': {'principal': 'x-principal-id'},
        'name_claims': ['principal'],
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
        with (
            httpx.Client(base_url=tcp.url, headers={**stamped, PROXY.actor_header: 'orchestrator'}) as asking,
            Client.on(cfg.socket_file, System.HUMAN) as local,
            Client.on(cfg.socket_file, System.ORCHESTRATOR) as orchestrator,
        ):
            start = local.news(NewsQuery()).next
            acted = Client(asking).reply(CommentId(1), ReplyBody(body='mine')).comment
            [news] = local.news(NewsQuery(after=start)).items
            assert orchestrator.news(NewsQuery(after=start)).items == [], (
                'the orchestrator at the terminal is the same party as the network orchestrator: nothing waits'
            )
    me = 'human:ana@example.com'
    assert news.text() == f'comment#{acted.id} decision p2 on comment#1 (demo) by orchestrator via {me}: mine', (
        "the person at the terminal sees the network orchestrator's decision, and whose it is"
    )
    assert isinstance(paused, CampaignState)
    assert (acked.acked_by, replied.author, paused.paused) == (me, me, 'launches')
    assert acted.author == f'orchestrator via {me}', 'a person acts as the orchestrator, and the write says who'
    with db.session() as s:
        row = s.get_one(Campaign, campaign.id)
        assert (row.updated_by, s.get_one(Comment, 1).acked_by) == (me, me)
        signed = {(e.kind, e.actor) for e in s.scalars(select(Event).where(Event.id > 1))}
        rows = InboxPanel().items(s, Context(row, row.config, utcnow(), System.HUMAN, home_cas(db)))
    assert signed == {
        ('campaign.paused', me),
        ('comment.acked_at', me),
        ('comment.decision', me),
        ('comment.decision', f'orchestrator via {me}'),
    }, 'the change log signs as the caller'
    assert {r.cells[2] for r in rows} >= {me, f'orchestrator via {me}'}, 'watch shows who replied'


def test_behind_the_proxy_an_agent_s_token_signs_on_the_socket_and_no_piloop_token_passes_the_listener(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    """A relayed call runs ``piloop`` on the server's socket with the agent's token; the proxy fronts the listener."""
    with db.session() as s:
        live, ended = [
            AgentSession(kind='watchdog', campaign_id=campaign.id, state=AgentSessionState.RUNNING) for _ in '12'
        ]
        s.add_all([live, ended])
        s.flush()
        agent, gone = (Agent(role=RoleName('watchdog'), session=w.id) for w in (live, ended))
        [secret, stale] = [tokens.mint_agent(s, a).get_secret_value() for a in (agent, gone)]
        ended.state = AgentSessionState.EXITED
        s.commit()
    person = token(db, 'ana', Access.ADMIN)
    uds = httpx.HTTPTransport(uds=str(cfg.socket_file))
    stamped = {'x-principal-id': 'ana@example.com'}
    with (
        serving_api(cfg, network_config=PROXY) as [_, tcp],
        httpx.Client(transport=uds, base_url='http://piloop') as socket,
        httpx.Client(base_url=tcp.url, headers=stamped) as network,
    ):
        signed = socket.post(
            f'/v1/campaigns/{campaign.name}/journal',
            json={'line': 'relayed'},
            headers={'Authorization': f'Bearer {secret}'},
        )
        assert signed.status_code == 200, signed.text
        late = socket.post(
            f'/v1/campaigns/{campaign.name}/journal',
            json={'line': 'late'},
            headers={'Authorization': f'Bearer {stale}'},
        )
        assert (late.status_code, late.json()) == (401, {'detail': 'the token is unknown or revoked'})
        for bearer in (person, secret):
            refused = network.post(
                f'/v1/campaigns/{campaign.name}/journal',
                json={'line': 'around'},
                headers={'Authorization': f'Bearer {bearer}'},
            )
            assert refused.status_code == 401 and 'takes no piloop token' in refused.json()['detail']
    with db.session() as s:
        assert {(j.line, j.author) for j in s.scalars(select(Journal))} == {('relayed', str(agent))}


def test_a_proxy_caller_that_its_rule_calls_a_service_never_signs_as_a_person(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    comment(db, 'watchdog#2', 'look at this')
    rules = [{'when': {}, 'access': 'act', 'caller': 'service'}]
    network = PROXY.model_copy(update={'rules': [Rule.model_validate(r) for r in rules]})
    stamped = {'x-principal-id': 'key-2'}
    with serving_api(cfg, network_config=network) as [_, tcp]:
        with httpx.Client(base_url=tcp.url, headers=stamped) as http:
            replied = Client(http).reply(CommentId(1), ReplyBody(body='go ahead')).comment
        with httpx.Client(base_url=tcp.url, headers={**stamped, PROXY.actor_header: 'orchestrator'}) as http:
            acted = Client(http).reply(CommentId(1), ReplyBody(body='mine')).comment
    assert (replied.author, acted.author) == ('service:key-2', 'orchestrator via service:key-2')


def test_a_message_signs_as_the_caller_wakes_the_orchestrator_and_its_reply_reaches_the_sender_s_inbox(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    stamped = {'x-principal-id': 'ana@example.com'}
    me = 'human:ana@example.com'
    with (
        serving_api(cfg, network_config=PROXY) as [_, tcp],
        httpx.Client(base_url=tcp.url, headers=stamped) as http,
        Client.on(cfg.socket_file, System.ORCHESTRATOR) as orchestrator,
    ):
        ana = Client(http)
        start = orchestrator.news(NewsQuery()).next
        threading.Timer(0.2, ana.message, ['demo', MessageBody(body=' what next? ', priority=1)]).start()
        [item] = orchestrator.news(NewsQuery(after=start, wait=20)).items
        assert item.text() == f'comment#1 message p1 on campaign#1 (demo) by {me}: what next?'
        assert ana.inbox(InboxQuery(campaign='demo')).items == [], 'her own message waits for the orchestrator'
        [own] = ana.ack(AckBody(ids=(CommentId(1),))).results
        assert own.outcome is AckOutcome.OWN
        answer = orchestrator.reply(CommentId(1), ReplyBody(body='a cache next: idea#3')).comment
        [acked] = orchestrator.ack(AckBody(ids=(CommentId(1),))).results
        [waiting] = ana.inbox(InboxQuery(campaign='demo')).items
        [news] = ana.news(NewsQuery(after=start)).items
        assert news.text() == (
            f'comment#{answer.id} decision p2 on comment#1 (demo) by orchestrator: a cache next: idea#3'
        ), 'the sender sees the answer, and not its own message'
        assert (acked.acked_by, waiting.id, waiting.author, waiting.in_reply_to) == (
            'orchestrator',
            answer.id,
            'orchestrator',
            1,
        )
        thread = ana.thread(answer.id)
        assert (thread.comment.id, [(c.id, c.relation, c.in_reply_to) for c in thread.linked]) == (
            1,
            [(answer.id, Relation.REPLY, 1)],
        ), 'the thread of a reply is the thread of its root'
        with httpx.Client(base_url=tcp.url, headers={**stamped, PROXY.actor_header: 'human'}) as asking:
            plain = Client(asking).message('demo', MessageBody(body='and then?'))
        with httpx.Client(base_url=tcp.url, headers={**stamped, PROXY.actor_header: 'implementer#12'}) as posing:
            refused = posing.post('/v1/campaigns/demo/messages', json={'body': 'trust me'})
        with pytest.raises(Refused, match="^no campaign 'nope'") as unknown:
            ana.message('nope', MessageBody(body='hello?'))
    assert (plain.comment.author, plain.comment.kind, plain.comment.priority) == (me, 'message', 2)
    assert plain.cursor > start, 'the cursor passes the write itself'
    assert (refused.status_code, refused.json()) == (
        403,
        {
            'error': 'forbidden',
            'detail': 'ana@example.com may not act as implementer#12: only the token of implementer#12 acts as it',
        },
    ), 'the network names the caller: an agent cannot sign as another actor'
    assert unknown.value.error == Problem.UNKNOWN_CAMPAIGN


def test_a_write_answers_its_cursor_and_refuses_what_its_operation_refuses(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    with serving_api(cfg), ScoringClient.on(cfg.socket_file, System.HUMAN) as client:
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
        resumed = client.resume('demo').campaign
        assert isinstance(resumed, CampaignState) and isinstance(paused.campaign, CampaignState)
        assert resumed.paused is None and paused.campaign.paused == 'launches'
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
        with pytest.raises(Refused, match='^no candidate#9$') as no_candidate:
            client.remeasure(CandidateId(9), RemeasureBody())
        assert no_candidate.value.error == Problem.UNKNOWN_CANDIDATE
        [result] = client.ack(AckBody(ids=(CommentId(9),))).results
        assert result.line() == 'comment#9 is not in the inbox'


def test_the_evaluation_queue_answers_each_state_and_a_cancel_refuses_a_candidate_that_waits_for_nothing(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    then = utcnow() - timedelta(hours=1)
    with db.session() as s:
        s.add(Evaluation(candidate_id=1, evaluator_id=campaign.evaluator_id, created_at=then, started_at=then))
        s.commit()
    with serving_api(cfg), ScoringClient.on(cfg.socket_file, System.ORCHESTRATOR) as client:
        [running] = client.evaluations().evaluations
        assert (running.candidate, running.campaign, running.summary) == (1, 'demo', 'baseline')
        assert running.state == RunningSince(since=then)
        assert client.cancel_evaluation(CandidateId(1)).candidate.id == 1
        [stopping] = client.evaluations().evaluations
        assert stopping.state == CancelledBy(by='orchestrator'), 'it runs until the daemon stops its evaluator'
        with pytest.raises(
            Refused, match='^candidate#1 has no evaluation that waits or runs: see `piloop eval queue`$'
        ) as twice:
            client.cancel_evaluation(CandidateId(1))
        assert twice.value.error == Problem.REFUSED
        with pytest.raises(Refused, match='^no candidate#9$') as unknown:
            client.cancel_evaluation(CandidateId(9))
        assert unknown.value.error == Problem.UNKNOWN_CANDIDATE
    with db.session() as s:
        assert s.get_one(Campaign, campaign.id).paused is None, 'a baseline that measured before holds nothing'


def test_a_cancelled_evaluation_of_a_baseline_that_never_measured_pauses_its_campaign(
    db: Database, campaign_path: Path, cfg: Settings
) -> None:
    with serving_api(cfg), ScoringClient.on(cfg.socket_file, System.HUMAN) as client:
        added = client.register_campaign(Checkout(path=str(campaign_path)))
        client.remeasure(added.baseline, RemeasureBody())
        client.cancel_evaluation(added.baseline)
    with db.session() as s:
        held = s.get_one(Campaign, added.campaign.id)
        assert (held.paused, held.paused_reason) == (
            Pause.LAUNCHES,
            'the baseline failed to evaluate: evaluation cancelled by human; fix it, then'
            f' `piloop candidate remeasure {added.baseline}` and `piloop resume demo`',
        )


def test_a_limit_is_set_and_unset_by_an_admin_and_refused_on_a_key_that_is_not_live(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    key = 'executor.pools.serve.evaluations'
    with serving_api(cfg, listen='127.0.0.1:0') as [_, tcp], Client.on(cfg.socket_file, System.HUMAN) as client:
        assert {k.key: k.origin for k in client.limits().keys}[key] == 'default'
        set_ = client.set_limit(key, LimitBody(value=1, reason='the GPU is busy'))
        [limit] = [k for k in set_.keys if k.key == key]
        assert (limit.value, limit.origin, limit.actor, limit.reason) == (1, 'limit', 'human', 'the GPU is busy')
        with pytest.raises(Refused, match='^executor.renew_seconds is not a live key') as not_live:
            client.set_limit('executor.renew_seconds', LimitBody(value=5, reason='r'))
        assert not_live.value.error == Problem.REFUSED
        [config] = [k for k in client.unset_limit(key).keys if k.key == key]
        assert (config.value, config.origin) == (2, 'default'), 'config.toml holds again'
        with pytest.raises(Refused, match=f'^{key} has no limit: see `piloop limits`$') as unknown:
            client.unset_limit(key)
        assert unknown.value.error == Problem.UNKNOWN_LIMIT
        actor = {'Authorization': f'Bearer {token(db, "ana", Access.ACT)}'}
        refused = httpx.get(tcp.url + '/v1/limits', headers=actor)
    assert refused.json() == {'detail': 'ana has act access, and this route needs admin'}


def test_an_admin_sets_lists_and_removes_secrets(
    db: Database, campaign: Campaign, cfg: Settings, tmp_path: Path
) -> None:
    with db.session() as s:
        s.add(Campaign(name='other', path=str(tmp_path / 'other'), config=campaign.config))
        s.commit()
    home, demo, other = Located(), Located(campaign='demo'), Located(campaign='other')
    body = SecretBody(value=SecretStr('s3cret'))
    assert 's3cret' not in repr(body), 'a log of the body shows no value'
    with (
        serving_api(cfg, listen='127.0.0.1:0') as [_, tcp],
        Client.on(cfg.socket_file, System.HUMAN) as admin,
    ):
        assert admin.set_secret('KEY', home, body).campaign is None
        assert admin.set_secret('KEY', demo, body).campaign == 'demo'
        assert admin.set_secret('OWN', other, body).campaign == 'other'
        listed = [(f.name, f.campaign, f.set_by) for f in admin.list_secrets().secrets]
        assert listed == [('KEY', None, 'human'), ('KEY', 'demo', 'human'), ('OWN', 'other', 'human')]
        assert admin.remove_secret('KEY', demo).campaign == 'demo'
        with pytest.raises(Refused, match=r'^campaign demo has no secret KEY: see `piloop secret list`$') as unknown:
            admin.remove_secret('KEY', demo)
        assert unknown.value.error == Problem.UNKNOWN_SECRET
        with pytest.raises(Refused) as nowhere:
            admin.set_secret('KEY', Located(campaign='nowhere'), body)
        assert nowhere.value.error == Problem.UNKNOWN_CAMPAIGN
        with pytest.raises(Refused, match='^name: String should match pattern'):
            admin.set_secret('1X', home, body)
        actor = {'Authorization': f'Bearer {token(db, "ana", Access.ACT)}'}
        refused = httpx.get(tcp.url + '/v1/secrets', headers=actor)
    assert refused.json() == {'detail': 'ana has act access, and this route needs admin'}


def bare(checkout: Path, at: Path) -> str:
    """A bare clone of ``checkout`` at ``at``, as a file:// URL."""
    subprocess.run(['git', 'clone', '-q', '--bare', str(checkout), str(at)], check=True)
    return at.as_uri()


def first(client: Client, after: int, *kinds: str) -> EventModel:
    """The payload of the first event of ``kinds`` after the cursor ``after``, once it comes."""
    while not (batch := client.events(EventsQuery(after=after, kind=kinds, wait=MAX_POLL_SECONDS))).events:
        after = batch.next
    return EVENT.validate_python(batch.events[0].payload)


def test_a_checkout_registers_at_once_the_daemon_measures_its_baseline_and_its_piloop_toml_is_read_again(
    db: Database, campaign_path: Path, cfg: Settings
) -> None:
    with serving_api(cfg), Client.on(cfg.socket_file, System.HUMAN) as client:
        added = client.register_campaign(Checkout(path=str(campaign_path)))
        assert (added.campaign.name, added.campaign.path, added.baseline) == (
            'demo',
            str(campaign_path),
            CandidateId(1),
        )
        assert isinstance(added.campaign, CampaignState)
        head = added.campaign.head
        assert head is not None and (head.candidate, [t.value for t in head.tracks]) == (added.baseline, [None]), (
            'the baseline is the head, and nothing measured it yet'
        )
        assert added.documents.line() == 'documents: 0 added, 0 unchanged'
        with measuring(cfg):  # the daemon's next pass, which also follows a restart
            recorded = first(client, added.cursor, *MeasurementRecorded.kinds())
        assert recorded == MeasurementRecorded(
            candidate=added.baseline, metrics={'score': 1.0, 'size': 5.0}, error=None, run=None
        )
        with pytest.raises(Refused, match=f'^{campaign_path} is the checkout of campaign demo #1 already$') as taken:
            client.register_campaign(Checkout(path=str(campaign_path), name='other'))
        assert taken.value.error == Problem.REFUSED
        with pytest.raises(Refused, match='^path: an absolute path, not campaign$'):
            client.register_campaign(Checkout.model_construct(path='campaign'))
        transport = httpx.HTTPTransport(uds=str(cfg.socket_file))
        with httpx.Client(transport=transport, base_url='http://piloop') as raw:
            for neither_or_both in ({}, {'path': str(campaign_path), 'clone': {'url': 'file:///x.git'}}):
                assert raw.post('/v1/campaigns', json=neither_or_both).status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        with db.session() as s:
            s.execute(update(Campaign).values(status=CampaignStatus.STOPPED))
            s.commit()
        (campaign_path / 'piloop.toml').write_text('name = "demo"\n')
        with pytest.raises(Refused, match='is not a valid piloop config') as invalid:
            client.sync_campaign('demo')
        assert invalid.value.error == Problem.REFUSED
        subprocess.run(['git', 'checkout', '-q', 'piloop.toml'], cwd=campaign_path, check=True)
        synced = client.sync_campaign('demo')
        assert (synced.was, synced.campaign.status) == (CampaignStatus.STOPPED, CampaignStatus.STOPPED), (
            'the stop conditions did not change'
        )


def test_a_registration_names_the_campaign_and_its_git_refs_carry_that_name(
    db: Database, campaign_path: Path, cfg: Settings
) -> None:
    toml = campaign_path / 'piloop.toml'
    toml.write_text(toml.read_text().replace('name = "demo"', 'name = "42"'))
    subprocess.run(['git', 'commit', '-qam', 'a name of digits'], cwd=campaign_path, check=True)
    with serving_api(cfg), ScoringClient.on(cfg.socket_file, System.HUMAN) as client:
        with pytest.raises(
            Refused, match="^'42' is no campaign name: a reference of digits names a campaign by its"
        ) as e:
            client.register_campaign(Checkout(path=str(campaign_path)))
        assert e.value.error == Problem.REFUSED
        added = client.register_campaign(Checkout(path=str(campaign_path), name='chosen'))
        assert added.campaign.name == 'chosen'
        (campaign_path / 'solve.txt').write_text('another\n')
        subprocess.run(['git', 'commit', '-qam', 'another tree'], cwd=campaign_path, check=True)
        imported = client.import_candidate('chosen', ImportBody(version='HEAD')).candidate
    refs = subprocess.run(
        ['git', 'for-each-ref', '--format=%(refname)'], cwd=campaign_path, capture_output=True, text=True
    )
    assert f'refs/piloop/chosen/reference/{imported.id}' in refs.stdout.split(), 'not the name of piloop.toml'


def test_a_stopped_campaign_retires_once_its_stop_is_over_and_its_name_registers_again(
    db: Database, campaign: Campaign, campaign_path: Path, cfg: Settings
) -> None:
    with serving_api(cfg), Client.on(cfg.socket_file, System.HUMAN) as client:
        with pytest.raises(Refused, match=r'^demo is active: stop it first, with `piloop campaign stop demo`$') as e:
            client.retire('demo', RetireBody(reason='done'))
        assert e.value.error == Problem.REFUSED
        with db.session() as s:
            s.get_one(Campaign, campaign.id).status = CampaignStatus.STOPPED
            s.add(AgentSession(kind='research', campaign_id=campaign.id, state=AgentSessionState.RUNNING))
            s.add(
                Comment(
                    target=TargetRef(TargetKind.CAMPAIGN, campaign.id),
                    author='w',
                    kind='escalation',
                    body='?',
                    priority=1,
                )
            )
            s.commit()
        with pytest.raises(Refused, match=r'^demo is still stopping; agents #1 are not over: retire it once its') as e:
            client.retire('demo', RetireBody(reason='done'))
        assert e.value.error == Problem.STILL_STOPPING
        with db.session() as s:
            s.execute(update(AgentSession).values(state=AgentSessionState.EXITED))
            s.commit()
        with pytest.raises(
            Refused, match=r'^demo is still stopping: retire it once its closing review and runs are over'
        ):
            client.retire('demo', RetireBody(reason='done'))
        with db.session() as s:
            emit(s, CampaignClosed(), TargetRef(TargetKind.CAMPAIGN, campaign.id), System.DAEMON)
            s.commit()
        with pytest.raises(Refused, match='^a reason is required'):
            client.retire('demo', RetireBody(reason=' '))
        assert client.inbox(InboxQuery()).items, 'the escalation waits'
        retired = client.retire('demo', RetireBody(reason='the e2e ended')).campaign
        assert (retired.id, retired.status) == (campaign.id, CampaignStatus.RETIRED)
        assert client.inbox(InboxQuery()).items == [], 'a retired campaign asks nothing more'
        assert client.status(Selection()).campaigns == []
        [listed] = client.status(Selection(status=(CampaignStatus.RETIRED,))).campaigns
        [by_id] = client.status(Selection(campaign=str(campaign.id))).campaigns
        assert listed.name == by_id.name == 'demo'
        with pytest.raises(Refused, match=f'^campaign #{campaign.id} \\(demo\\) is retired$') as changed:
            client.pause(str(campaign.id), PauseBody(reason='no'))
        assert changed.value.error == Problem.REFUSED
        with pytest.raises(Refused, match='^no campaign .demo.') as gone:
            client.pause('demo', PauseBody(reason='no'))
        assert gone.value.error == Problem.UNKNOWN_CAMPAIGN
        again = client.register_campaign(Checkout(path=str(campaign_path))).campaign
        assert (again.name, again.path, again.status) == ('demo', str(campaign_path), CampaignStatus.ACTIVE)
        assert again.id != campaign.id
        [here] = client.status(Selection(cwd=str(campaign_path))).campaigns
        assert here.id == again.id, 'the retired campaign holds no path'
    with db.session() as s:
        notes = s.scalars(select(Comment.body).where(Comment.kind == CommentKind.NOTE)).all()
        retirements = s.scalars(select(Event.payload).where(Event.kind == 'campaign.status')).all()
        assert notes == ['retired: the e2e ended']
        assert [p.new for p in retirements if isinstance(p, CampaignStatusChanged)][-1] is CampaignStatus.RETIRED
        with pytest.raises(ops.Refused, match=f'^campaign #{campaign.id} \\(demo\\) is retired already$'):
            ops.retire(s, campaign.id, System.HUMAN, reason='twice')


def test_a_baseline_that_fails_to_evaluate_registers_and_then_pauses_its_campaign(
    db: Database, campaign_path: Path, cfg: Settings
) -> None:
    toml = campaign_path / 'piloop.toml'
    toml.write_text(toml.read_text().replace('"sh", "-c"', '"false", "-c"'))
    with serving_api(cfg), measuring(cfg), Client.on(cfg.socket_file, System.HUMAN) as client:
        added = client.register_campaign(Checkout(path=str(campaign_path)))
        paused = first(client, added.cursor, *CampaignPausedChanged.kinds())
    logs = cfg.home / 'projects' / str(added.campaign.id) / 'measure' / f'{added.baseline}-e1'
    assert isinstance(paused, CampaignPausedChanged) and paused.new is Pause.LAUNCHES
    assert paused.paused_reason == (
        f'the baseline failed to evaluate: evaluate exit 1: ; its logs are in {logs}; fix it, then'
        f' `piloop candidate remeasure {added.baseline}` and `piloop resume demo`'
    )
    with db.session() as s:
        assert s.get_one(Campaign, added.campaign.id).name == 'demo', 'it stays registered'


def test_a_git_url_is_cloned_under_the_home_with_the_home_s_secret_and_then_registered_as_often_as_asked(
    db: Database, campaign_path: Path, cfg: Settings, tmp_path: Path, mocker: MockerFixture
) -> None:
    url = bare(campaign_path, tmp_path / 'demo.git')
    cloned = mocker.spy(Git, 'clone')
    with serving_api(cfg), Client.on(cfg.socket_file, System.HUMAN) as client:
        with pytest.raises(Refused, match=r'^the home has no secret TOKEN: see `piloop secret list`$') as unknown:
            client.register_campaign(Clone(clone=Remote(url=url, secret='TOKEN')))
        assert unknown.value.error == Problem.REFUSED and not (cfg.home / 'repos').exists()
        client.set_secret('TOKEN', Located(), SecretBody(value=SecretStr('tok')))
        added = client.register_campaign(Clone(clone=Remote(url=url, secret='TOKEN')))
        assert added.campaign.path == str(cfg.home / 'repos' / str(added.campaign.id))
        [(_, _, path, credential)] = [c.args for c in cloned.call_args_list]
        assert (path.parent, credential.get_secret_value()) == (cfg.home / 'repos', 'tok')
        again = client.register_campaign(Clone(clone=Remote(url=url), name='again'))
        assert (again.campaign.name, again.campaign.path) == ('again', str(cfg.home / 'repos' / str(again.campaign.id)))
        (cfg.home / 'repos' / str(again.campaign.id + 1)).mkdir()
        with pytest.raises(
            Refused, match=f'^{cfg.home / "repos" / str(again.campaign.id + 1)} exists: move it away'
        ) as e:
            client.register_campaign(Clone(clone=Remote(url=url), name='third'))
        assert e.value.error == Problem.REFUSED
    assert sorted(p.name for p in (cfg.home / 'repos').iterdir()) == ['1', '2', '3'], 'the refused clone is gone'


def test_a_clone_that_fails_or_does_not_register_leaves_nothing_under_the_home(
    db: Database, campaign_path: Path, cfg: Settings, tmp_path: Path
) -> None:
    url = bare(campaign_path, tmp_path / 'taken.git')
    with serving_api(cfg), Client.on(cfg.socket_file, System.HUMAN) as client:
        with pytest.raises(Refused, match='^git clone of file:///nowhere.git failed: ') as failed:
            client.register_campaign(Clone(clone=Remote(url='file:///nowhere.git')))
        assert failed.value.error == Problem.REFUSED
        client.register_campaign(Checkout(path=str(campaign_path)))
        with pytest.raises(Refused, match="^campaign name 'demo' is already used") as taken:
            client.register_campaign(Clone(clone=Remote(url=url)))
        assert taken.value.error == Problem.REFUSED
    assert list((cfg.home / 'repos').iterdir()) == [], 'a retry clones again'


def test_an_admin_makes_a_token_whose_secret_only_its_answer_carries_and_revokes_it(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    with serving_api(cfg, listen='127.0.0.1:0') as [_, tcp], Client.on(cfg.socket_file, System.HUMAN) as client:
        made = client.create_token(TokenBody(name='gpu1', access=Access.WORKER))
        secret = made.secret.get_secret_value()
        assert secret not in repr(made), 'a log of the answer shows no secret'
        assert Tokens(db).verify(secret) == tokens.Caller('gpu1', Access.WORKER)
        with pytest.raises(Refused, match="^a token is called 'gpu1' already") as taken:
            client.create_token(TokenBody(name='gpu1', access=Access.READ))
        assert taken.value.error == Problem.REFUSED
        [listed] = client.list_tokens().tokens
        assert (listed.name, listed.access, listed.created_at) == ('gpu1', Access.WORKER, made.created_at)
        assert client.revoke_token('gpu1').tokens == []
        with pytest.raises(Refused, match="^no token is called 'gpu1'$") as unknown:
            client.revoke_token('gpu1')
        assert unknown.value.error == Problem.UNKNOWN_TOKEN
        actor = {'Authorization': f'Bearer {token(db, "ana", Access.ACT)}'}
        refused = httpx.post(tcp.url + '/v1/tokens', headers=actor, json={'name': 'mine', 'access': 'admin'})
    assert refused.json() == {'detail': 'ana has act access, and this route needs admin'}


def test_an_agent_is_requested_of_a_sweeping_role_only(db: Database, campaign: Campaign, cfg: Settings) -> None:
    with serving_api(cfg), Client.on(cfg.socket_file, System.ORCHESTRATOR) as client:
        asked = client.launch('demo', LaunchBody(role=RoleName('research'), model='opus', reason='look again'))
        with pytest.raises(
            Refused, match="^demo has no sweeping role 'janitor'; it has orchestrator, research, "
        ) as refused:
            client.launch('demo', LaunchBody(role=RoleName('janitor'), reason='sweep'))
    assert refused.value.error == Problem.REFUSED
    with db.session() as s:
        agent = s.get_one(AgentSession, asked.agent)
        assert (asked.role, agent.kind, agent.model, agent.reason) == ('research', 'research', 'opus', 'look again')


def outcome(db: Database, comment: CommentId) -> None:
    with db.session() as s:
        queued = PromptOutcome(kind='comment.queued', comment=comment)
        emit(s, queued, TargetRef(TargetKind.AGENT_SESSION, 1), System.DAEMON)
        s.commit()


def test_a_prompt_waits_for_what_came_of_its_note_and_refuses_an_agent_that_does_not_run(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    with db.session() as s:
        s.add_all(
            [
                AgentSession(kind='research', campaign_id=1, state=AgentSessionState.RUNNING),
                AgentSession(kind='research', campaign_id=1, state=AgentSessionState.EXITED),
            ]
        )
        s.commit()
    with serving_api(cfg), Client.on(cfg.socket_file, System.ORCHESTRATOR) as client:
        unanswered = client.prompt(AgentSessionId(1), PromptBody(text='look at #3'))
        assert (unanswered.comment, unanswered.delivery) == (1, Pending()), 'no wait: it answers at once'
        threading.Timer(0.2, outcome, [db, CommentId(2)]).start()
        started = time.monotonic()
        queued = client.prompt(AgentSessionId(1), PromptBody(text='and #4', wait=20))
        assert (queued.comment, queued.delivery) == (2, Queued()) and time.monotonic() - started < 10
        with pytest.raises(Refused, match='^agent_session#2 is not running$') as ended:
            client.prompt(AgentSessionId(2), PromptBody(text='x'))
        assert ended.value.error == Problem.REFUSED
        with pytest.raises(Refused, match='^no agent_session#9$') as unknown:
            client.prompt(AgentSessionId(9), PromptBody(text='x'))
        assert unknown.value.error == Problem.UNKNOWN_AGENT
    with db.session() as s:
        assert s.get_one(Comment, 1).author == 'orchestrator'


def test_a_cancel_asks_the_daemon_to_end_an_open_run_or_a_live_agent_and_refuses_a_closed_one(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    add_run(db, campaign, status=RunStatus.RUNNING, measured=False)
    add_run(db, campaign, measured=False)
    with db.session() as s:
        s.add_all(
            [
                AgentSession(kind='research', campaign_id=1, state=AgentSessionState.REQUESTED),
                AgentSession(kind='research', campaign_id=1, state=AgentSessionState.EXITED),
            ]
        )
        s.commit()
    with serving_api(cfg), Client.on(cfg.socket_file, System.ORCHESTRATOR) as client:
        agent = client.cancel_agent(AgentSessionId(1), CancelBody())
        run = client.cancel_run(RunId(1), CancelBody(ending=Ending.WRAP_UP))
        assert (agent.agent, agent.role, run.run) == (1, 'research', 1)
        with pytest.raises(Refused, match='^agent_session#2 is not requested, starting or running$') as ended:
            client.cancel_agent(AgentSessionId(2), CancelBody())
        with pytest.raises(Refused, match='^run#2 is not queued or running$') as closed:
            client.cancel_run(RunId(2), CancelBody())
        assert ended.value.error == closed.value.error == Problem.REFUSED
        with pytest.raises(Refused, match='^no agent_session#9$') as no_agent:
            client.cancel_agent(AgentSessionId(9), CancelBody())
        assert no_agent.value.error == Problem.UNKNOWN_AGENT
        with pytest.raises(Refused, match='^no run#9$') as no_run:
            client.cancel_run(RunId(9), CancelBody())
        assert no_run.value.error == Problem.UNKNOWN_RUN
    with db.session() as s:
        asked = [
            (e.kind, e.target_id, e.actor) for e in s.scalars(select(Event)) if e.kind.endswith('cancel_requested')
        ]
    assert asked == [('agent_session.cancel_requested', 1, 'orchestrator'), ('run.cancel_requested', 1, 'orchestrator')]


def test_a_journal_line_is_the_campaign_s_signed_by_the_caller_s_actor_and_the_server_refuses_one_too_long(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    path = f'/v1/campaigns/{campaign.name}/journal'
    with serving_api(cfg), Client.on(cfg.socket_file, System.ORCHESTRATOR) as client:
        written = client.post(path, JournalBody(line=' parked idea#4:\n dup of #2 '), Journaled)
        with pytest.raises(Refused, match='^line: a journal line is 1 to 280 bytes, not 281: say less$') as long:
            client.post(path, JournalBody.model_construct(line='x' * 281), Journaled)
        with pytest.raises(Refused) as unknown:
            client.post('/v1/campaigns/nope/journal', JournalBody(line='parked idea#4'), Journaled)
    assert long.value.error is None
    assert unknown.value.error == Problem.UNKNOWN_CAMPAIGN
    with db.session() as s:
        entry = s.get_one(Journal, written.journal)
        assert (entry.campaign_id, entry.author, entry.line) == (
            campaign.id,
            'orchestrator',
            'parked idea#4: dup of #2',
        )


def test_a_write_needs_act_access_and_a_caller_whose_name_signs(
    db: Database, campaign: Campaign, cfg: Settings
) -> None:
    reader = {'Authorization': f'Bearer {token(db, "ana", Access.READ)}'}
    spaced = {'Authorization': f'Bearer {token(db, "two words", Access.ACT)}'}
    with serving_api(cfg, listen='127.0.0.1:0') as [_, tcp]:
        refused = httpx.post(tcp.url + '/v1/campaigns/demo/resume', headers=reader)
        unsigned = httpx.post(tcp.url + '/v1/campaigns/demo/resume', headers=spaced)
    assert refused.json() == {'detail': 'ana has read access, and this route needs act'}
    assert unsigned.json() == {
        'error': 'forbidden',
        'detail': "'two words' is no name that signs a write: name the caller another way",
    }


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


def test_a_campaign_whose_stored_config_is_invalid_is_listed_as_such_and_the_others_as_ever(
    db: Database,
    campaign: Campaign,
    cfg: Settings,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A stored copy that no longer validates, as after an upgrade, is no 500 for every campaign: status and the
    overview list it with what is wrong and how to fix it, and every other campaign in full."""
    with db.session() as s:
        broken = Campaign(name='broken', path='/elsewhere', config=campaign.config)
        s.add(broken)
        s.commit()
        broken_id = broken.id
    store_config(db, broken_id, {})
    with serving_api(cfg), Client.on(cfg.socket_file, System.HUMAN) as client:
        listed = client.status(Selection()).campaigns
        shown = client.overview(Selection()).campaigns
        monkeypatch.setattr(common, 'settings', cfg)
        monkeypatch.setattr(common, 'console', Console(width=200, force_terminal=False))
        cli.app(['status'], result_action='return_value', exit_on_error=False)
        cli.app(['pause', 'broken', '--reason', 'r'], result_action='return_value', exit_on_error=False)
    for entries in (listed, shown):
        ok, invalid = entries
        assert not isinstance(ok, InvalidConfig) and ok.name == 'demo'
        assert isinstance(invalid, InvalidConfig) and (invalid.id, invalid.name) == (broken_id, 'broken')
        assert invalid.fix == '`piloop init /elsewhere`'
        assert invalid.problem.startswith('the stored config is not a valid piloop config'), invalid.problem
    status, paused = capsys.readouterr().out.split('campaign broken paused')
    assert 'demo #1' in status and 'broken #2' in status and 'fix: `piloop init /elsewhere`' in status, status
    assert paused.startswith('; its stored config is invalid, fix it with `piloop init /elsewhere`;'), paused


def test_with_a_server_set_the_commands_call_it_through_the_network_with_its_credential(
    db: Database, campaign: Campaign, cfg: Settings, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv(TOKEN_ENV, token(db, 'laptop', Access.READ))
    with serving_api(cfg, listen='127.0.0.1:0') as [_, tcp]:
        monkeypatch.setattr(common, 'settings', Settings(home=cfg.home / 'elsewhere', server=tcp.url))
        monkeypatch.setattr(common, 'console', Console(width=200, force_terminal=False))
        cli.app(['status', 'demo', '--json'], result_action='return_value', exit_on_error=False)
    assert '"name": "demo"' in capsys.readouterr().out, 'a name resolves on the server; no socket of this home'
