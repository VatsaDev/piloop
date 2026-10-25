"""The server of ``piloop serve --listen``: who may call it, TLS, and an executor's routes."""

import http.client
import json
import logging
import socket
import ssl
import time
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

import httpx2 as httpx
import pytest
from covalence import Cid
from pydantic import BaseModel, ValidationError

from piloop.api.contract import Access, Polled
from piloop.daemon.main import preflight
from piloop.db.models import Campaign
from piloop.db.session import Database
from piloop.net import Route, cas_server, tokens
from piloop.net.cas import route
from piloop.net.server import App, Server, app
from piloop.net.tokens import Caller, Tokens
from piloop.plugins import host as plugins
from piloop.plugins.cas.contract import BLAKE3
from piloop.plugins.cas.disk import DiskCas
from piloop.plugins.network.contract import (
    NETWORKS,
    Address,
    Enrollment,
    Listener,
    Network,
    NetworkHost,
    NotEnrolled,
    Peer,
    Refused,
)
from piloop.plugins.network.https import Https
from piloop.settings import Settings
from tests.conftest import Dial, package
from tests.net.conftest import Serving


class Ask(BaseModel):
    text: str


class Answer(BaseModel):
    text: str
    caller: str


def _echo(caller: Caller, ask: Ask) -> Answer | None:
    return Answer(text=ask.text, caller=caller.name) if ask.text else None


ECHO = Route('echo', Ask, _echo)
ECHOED = '/v1/executor/echo'
HI = {'text': 'hi'}
DROPPED = (httpx.RemoteProtocolError, httpx.ReadError, httpx.WriteError)
"""How a client sees a server that drops its connection unanswered: a close (FIN), or a reset (RST) when the server
closed with some of the request still unread, which depends on when the rest of the request arrived."""


def test_a_caller_without_a_known_token_gets_401_and_a_revoked_token_stops_working(
    serving: Serving, token: Callable[..., str], db: Database, dial: Dial
) -> None:
    secret = token()
    with serving([ECHO]) as server, dial(server.url, secret) as client, httpx.Client() as anonymous:
        assert client.post(ECHOED, json=HI).status_code == 200
        refused = anonymous.post(server.url + ECHOED, json=HI)
        assert (refused.status_code, refused.headers['www-authenticate']) == (401, 'Bearer')
        with dial(server.url, 'plt_guessed') as guessed:
            assert guessed.post(ECHOED, json=HI).status_code == 401
        with db.session() as s:
            tokens.revoke(s, 'gpu1')
            s.commit()
        revoked = client.post(ECHOED, json=HI)
        assert (revoked.status_code, revoked.json()) == (401, {'detail': 'the token is unknown or revoked'})


def test_core_verifies_a_piloop_token_before_the_network_and_a_token_of_another_access_gets_403(
    serving: Serving, token: Callable[..., str], dial: Dial
) -> None:
    with serving([ECHO]) as server:
        with dial(server.url, token('ana', Access.READ)) as reader:
            refused = reader.post(ECHOED, json=HI)
            assert (refused.status_code, refused.json()) == (
                403,
                {'detail': 'ana has read access, and this route needs worker'},
            )
        with dial(server.url, 'hil_someone_elses') as foreign:
            unknown = foreign.post(ECHOED, json=HI)
            assert unknown.json() == {
                'detail': 'the credential is not a piloop token (plt_…): send Authorization: Bearer <token>'
            }, 'https knows no other credential'


def test_an_executor_route_takes_its_model_and_answers_with_its_model_or_204(
    serving: Serving, token: Callable[..., str], dial: Dial
) -> None:
    with serving([ECHO]) as server, dial(server.url, token()) as client:
        answered = client.post('/v1/executor/echo', json={'text': 'hi'})
        assert (answered.status_code, answered.json()) == (200, {'text': 'hi', 'caller': 'gpu1'})
        assert client.post('/v1/executor/echo', json={'text': ''}).status_code == 204
        invalid = client.post('/v1/executor/echo', json={'words': 'hi'})
        assert invalid.status_code == 422
        assert invalid.json()['detail'][0]['loc'] == ['text']
        assert client.post('/v1/executor/other', json={}).status_code == 404


def test_tls_with_a_certificate_that_the_client_trusts_through_ssl_cert_file(
    serving: Serving, token: Callable[..., str], certificate: dict[str, str], dial: Dial
) -> None:
    with serving([ECHO], tls=certificate) as server, dial(server.url, token()) as client:
        assert server.url.startswith('https://127.0.0.1:')
        assert client.post(ECHOED, json=HI).status_code == 200
        with pytest.raises(DROPPED):
            httpx.post(server.url.replace('https', 'http') + ECHOED, json=HI)


class Recording:
    """A network that admits every caller as ``gpu1`` and records the peer of each request."""

    def __init__(self, tls: ssl.SSLContext | None) -> None:
        self.tls = tls
        self.peers: list[Peer] = []

    def listener(self) -> Listener:
        return Listener(socket.create_server(('127.0.0.1', 0)), self.tls)

    def identify(self, headers: Mapping[str, str], peer: Peer) -> Caller:
        self.peers.append(peer)
        return Caller('gpu1', Access.WORKER)

    def enroll(self, principal: str, access: Access) -> Enrollment:
        raise NotEnrolled('it admits every caller')

    def revoke(self, principal: str) -> None:
        raise NotEnrolled('it admits every caller')


@dataclass(frozen=True)
class Served:
    """A server's app over a network, and a request that the network must admit, with the status that answers it."""

    app: Callable[[Tokens, Network, Path], App]
    method: str
    path: str
    status: int
    body: str | None = None


def _control_plane(tokens: Tokens, network: Network, tmp: Path) -> App:
    return app(tokens, network, [ECHO], ())


def _cas(tokens: Tokens, network: Network, tmp: Path) -> App:
    return cas_server.app(tokens, network, DiskCas(tmp / 'cas'), tmp / 'scratch')


SERVED = {
    'piloop serve': Served(_control_plane, 'POST', ECHOED, 200, json.dumps(HI)),
    'piloop cas serve': Served(_cas, 'HEAD', route(Cid.from_parts('raw', BLAKE3, b'\0' * 32)), 404),
}


def _peer_of(served: Served, tokens: Tokens, network: Recording, tmp: Path, client: ssl.SSLContext | None) -> Address:
    """Send ``served``'s request through a server over ``network``, and return the address that it came from."""
    with closing(Server(served.app(tokens, network, tmp), network.listener())) as server:
        server.start()
        host, port = server.listener.socket.getsockname()[:2]
        connection = (
            http.client.HTTPConnection(host, port)
            if client is None
            else http.client.HTTPSConnection(host, port, context=client)
        )
        connection.connect()
        sent = Address(host=connection.sock.getsockname()[0], port=connection.sock.getsockname()[1])
        connection.request(served.method, served.path, served.body, {'Content-Type': 'application/json'})
        assert connection.getresponse().status == served.status
        connection.close()
    return sent


@pytest.mark.parametrize('served', SERVED.values(), ids=SERVED.keys())
def test_the_server_tells_the_network_the_peer_address_and_its_tls_certificate(
    served: Served, certificate: dict[str, str], tmp_path: Path, db: Database
) -> None:
    plain = Recording(None)
    sent = _peer_of(served, Tokens(db), plain, tmp_path, None)
    assert plain.peers == [Peer(address=sent, certificate=None)]

    tls = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    tls.load_cert_chain(certificate['cert'], certificate['key'])
    tls.verify_mode = ssl.CERT_OPTIONAL
    tls.load_verify_locations(certificate['cert'])
    mutual = Recording(tls)
    client = ssl.create_default_context(cafile=certificate['cert'])
    client.load_cert_chain(certificate['cert'], certificate['key'])
    sent = _peer_of(served, Tokens(db), mutual, tmp_path, client)
    der = ssl.PEM_cert_to_DER_cert(Path(certificate['cert']).read_text())
    assert mutual.peers == [Peer(address=sent, certificate=der)]


@pytest.mark.parametrize(
    ('keys', 'refused'),
    [
        ({'listen': '0.0.0.0:8443'}, True),
        ({'listen': '[::]:8443'}, True),
        ({'listen': 'piloop.example.com:8443'}, True),
        ({'listen': '0.0.0.0:8443', 'tls': 'proxy'}, False),
        ({'listen': '127.0.0.1:8443'}, False),
        ({'listen': '[::1]:8443'}, False),
        ({'listen': 'localhost:8443'}, False),
        ({}, False),
    ],
)
def test_an_address_that_is_not_loopback_needs_tls(keys: dict[str, str], refused: bool) -> None:
    if refused:
        with pytest.raises(ValidationError, match='is not loopback, and without tls its tokens would cross'):
            Https.model_validate(keys)
    else:
        Https.model_validate(keys)


def test_a_certificate_makes_any_address_acceptable(certificate: dict[str, str]) -> None:
    Https.model_validate({'listen': '0.0.0.0:8443', 'tls': certificate})
    with pytest.raises(ValidationError, match='key'):
        Https.model_validate({'listen': '0.0.0.0:8443', 'tls': {'cert': certificate['cert']}})


@pytest.mark.parametrize('text', ['8443', ':8443', '127.0.0.1:', '127.0.0.1:http', '127.0.0.1:70000'])
def test_an_address_is_host_colon_port(text: str) -> None:
    with pytest.raises(ValidationError):
        Address.model_validate(text)


def test_an_address_prints_as_it_parses() -> None:
    assert [str(Address.model_validate(a)) for a in ('[::1]:1', 'localhost:0')] == ['[::1]:1', 'localhost:0']


def test_an_entry_point_that_is_not_a_network_class_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    package(tmp_path / 'bad', 'piloop-bad', '1.0', '[piloop.networks]\nbad = tests.fixtures.plugin:Typed\n')
    monkeypatch.syspath_prepend(tmp_path / 'bad')
    plugins.close()
    try:
        with pytest.raises(
            plugins.PluginError, match='is not a subclass of piloop.plugins.network.contract.NetworkConfig'
        ):
            NETWORKS.get('bad')
    finally:
        plugins.close()


def test_config_toml_selects_the_network_and_listen_overrides_its_address(tmp_path: Path) -> None:
    (tmp_path / 'config.toml').write_text(
        '[plugins.network]\nprovider = "https"\nlisten = "0.0.0.0:8443"\ntls = "proxy"\n'
    )
    assert preflight(Settings(home=tmp_path)) == Https(listen=Address(host='0.0.0.0', port=8443), tls='proxy')
    served = preflight(Settings(home=tmp_path), Address(host='127.0.0.1', port=0))
    assert served == Https(listen=Address(host='127.0.0.1', port=0), tls='proxy')
    (tmp_path / 'config.toml').write_text('[plugins.network]\nprovider = "https"\nlisten = "0.0.0.0:8443"\n')
    with pytest.raises(plugins.PluginError, match='is not loopback'):
        preflight(Settings(home=tmp_path))


class Poll(Polled):
    name: str


def renamed_to(db: Database) -> Callable[[Caller, Poll], Answer | None]:
    """A long-poll's handler: the campaign's name once it is the asked one, else nothing yet."""

    def handle(caller: Caller, poll: Poll) -> Answer | None:
        with db.session() as s:
            name = s.get_one(Campaign, 1).name
        return Answer(text=name, caller=caller.name) if name == poll.name else None

    return handle


def test_more_long_polls_than_pool_threads_hold_none_and_a_quick_route_still_answers_at_once(
    serving: Serving, token: Callable[..., str], db: Database, campaign: Campaign, dial: Dial
) -> None:
    """Starlette's pool has 40 threads. A long-poll holds one only while it reads, then awaits a commit."""
    polls = 50
    route = Route.long_poll('renamed', Poll, renamed_to(db), db.changed)
    with serving([ECHO, route]) as server, dial(server.url, token()) as client:
        with ThreadPoolExecutor(polls) as pool:
            waiting = [
                pool.submit(client.post, '/v1/executor/renamed', json={'name': 'renamed', 'wait': 20})
                for _ in range(polls)
            ]
            time.sleep(1)  # every long-poll is parked
            began = time.monotonic()
            assert client.post(ECHOED, json=HI).status_code == 200
            assert time.monotonic() - began < 1, 'a heartbeat waits for no long-poll'
            with db.session() as s:
                s.get_one(Campaign, campaign.id).name = 'renamed'
                s.commit()
            began = time.monotonic()
            answers = [w.result(timeout=15) for w in waiting]
        assert {(a.status_code, a.json()['text']) for a in answers} == {(200, 'renamed')}
        assert time.monotonic() - began < 10, 'the commit woke them, long before their wait ended'
        quiet = client.post('/v1/executor/renamed', json={'name': 'other', 'wait': 0.2})
        assert quiet.status_code == 204, 'nothing by the end of its wait'
        assert client.post('/v1/executor/renamed', json={'name': 'x', 'wait': 26}).status_code == 422


def test_a_close_ends_a_parked_long_poll_at_once_with_204_and_logs_no_error(
    serving: Serving,
    token: Callable[..., str],
    db: Database,
    campaign: Campaign,
    dial: Dial,
    caplog: pytest.LogCaptureFixture,
) -> None:
    route = Route.long_poll('renamed', Poll, renamed_to(db), db.changed)
    with serving([route]) as server, dial(server.url, token()) as client, ThreadPoolExecutor(1) as pool:
        parked = pool.submit(client.post, '/v1/executor/renamed', json={'name': 'renamed', 'wait': 20})
        time.sleep(0.5)  # the long-poll is parked
        began = time.monotonic()
        server.close()
        assert time.monotonic() - began < 1, 'the close waits for no long-poll'
        assert parked.result(timeout=5).status_code == 204
    assert [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR] == []


def test_https_enrolls_a_machine_with_a_token_that_its_client_dials_with_until_it_is_revoked(
    db: Database, serving: Serving, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(NotEnrolled, match='set url'):
        Https().open(NetworkHost(Tokens(db))).enroll('gpu-1', Access.WORKER)
    with serving([ECHO]) as server:
        network = Https(url=server.url).open(NetworkHost(Tokens(db)))
        enrolled = network.enroll('gpu-1', Access.WORKER)
        for name, value in enrolled.env.items():
            monkeypatch.setenv(name, value.get_secret_value())
        with Https().client(enrolled.server) as client:
            assert client.post(ECHOED, json=HI).status_code == 200
            network.revoke('gpu-1')
            network.revoke('gpu-1')
            assert client.post(ECHOED, json=HI).status_code == 401


class TrustedHeaders:
    """A network behind an edge that checks its own keys and stamps who calls: ``x-user`` and ``x-access``."""

    def __init__(self) -> None:
        self.asked: list[str] = []

    def listener(self) -> Listener:
        return Listener(socket.create_server(('127.0.0.1', 0)), None)

    def identify(self, headers: Mapping[str, str], peer: Peer) -> Caller | Refused:
        self.asked.append(headers.get('authorization', ''))
        if (user := headers.get('x-user')) is None:
            return Refused('the edge stamped no x-user')
        return Caller(user, Access(headers.get('x-access', 'read')))

    def enroll(self, principal: str, access: Access) -> Enrollment:
        raise NotEnrolled('the edge gives keys')

    def revoke(self, principal: str) -> None:
        raise NotEnrolled('the edge gives keys')


def test_a_bearer_that_is_not_a_piloop_token_reaches_the_network_which_identifies_the_caller(db: Database) -> None:
    network = TrustedHeaders()
    with closing(Server(app(Tokens(db), network, [ECHO], ()), network.listener())) as server:
        server.start()
        stamped = {'Authorization': 'Bearer hil_abc', 'x-user': 'gpu7', 'x-access': 'worker'}
        answered = httpx.post(server.url + ECHOED, json=HI, headers=stamped)
        assert (answered.status_code, answered.json()) == (200, {'text': 'hi', 'caller': 'gpu7'})
        unstamped = httpx.post(server.url + ECHOED, json=HI, headers={'Authorization': 'Bearer hil_abc'})
        assert (unstamped.status_code, unstamped.json()) == (401, {'detail': 'the edge stamped no x-user'})
        reader = httpx.post(server.url + ECHOED, json=HI, headers={**stamped, 'x-access': 'read'})
        assert reader.status_code == 403, 'core checks the access of whoever the network names'
        forged = httpx.post(server.url + ECHOED, json=HI, headers={**stamped, 'Authorization': 'Bearer plt_guessed'})
        assert (forged.status_code, forged.json()) == (401, {'detail': 'the token is unknown or revoked'})
    assert network.asked == ['Bearer hil_abc'] * 3, (
        'core verifies only a piloop token, and leaves every other call to the network'
    )
