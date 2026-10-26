"""``proxy``: the caller is what the fronting proxy stamped, and a worker gets the home's credential."""

import socket

import httpx2 as httpx
import pytest
from pydantic import SecretStr, ValidationError

from piloop.actor import System
from piloop.api.contract import Access
from piloop.db.session import Database
from piloop.net.server import Server, app, executor_router
from piloop.net.tokens import Caller, Tokens
from piloop.plugins.network.contract import NETWORKS, NetworkHost, NoCredential, NotEnrolled, Peer, Refused
from piloop.plugins.network.proxy import (
    CREDENTIAL_ENV,
    NAME_ENV,
    Proxy,
    ProxyNetwork,
)
from piloop.plugins.secrets.contract import HomeSecrets, set_secret
from piloop.plugins.secrets.local import LocalSecrets
from tests.net.test_server import ECHO, ECHOED, HI

PEER = Peer(address=None, certificate=None)
KEYS = {
    'url': 'https://piloop.test',
    'claims': {'org': 'X-Org-Id', 'kind': 'x-subject-kind', 'principal': 'x-principal-id'},
    'name_claim': 'principal',
    'rules': [
        {'when': {'org': 'acme', 'kind': 'service', 'principal': 'key-1'}, 'access': 'worker'},
        {'when': {'org': 'acme', 'kind': ['user', 'service']}, 'access': 'act'},
    ],
}
STAMPED = {
    'x-org-id': 'acme',
    'x-subject-kind': 'service',
    'x-principal-id': 'key-1',
    'x-piloop-worker': 'gpu-0123abcd',
}


def opened(db: Database, **keys: object) -> ProxyNetwork:
    config = NETWORKS.validated({'provider': 'proxy', 'listen': '127.0.0.1:0', **KEYS, **keys})
    assert isinstance(config, Proxy)
    return config.open(NetworkHost(Tokens(db), HomeSecrets(db, LocalSecrets())))


def test_the_first_rule_that_the_claims_match_gives_the_access(db: Database) -> None:
    network = opened(db)
    assert network.identify(STAMPED, PEER) == Caller('gpu-0123abcd', Access.WORKER), 'a worker, by its header'
    person = STAMPED | {'x-subject-kind': 'user', 'x-principal-id': 'user-7'}
    assert network.identify(person, PEER) == Caller('user-7', Access.ACT), 'anyone else, by its name claim'
    another_key = STAMPED | {'x-principal-id': 'key-2'}
    assert network.identify(another_key, PEER) == Caller('key-2', Access.ACT), 'the next rule that matches'


@pytest.mark.parametrize(
    'headers',
    [
        {},
        STAMPED | {'x-org-id': 'other'},
        STAMPED | {'x-subject-kind': 'robot'},
        {k: v for k, v in STAMPED.items() if k != 'x-piloop-worker'},
    ],
    ids=['unstamped', 'another org', 'no rule for the kind', 'an unnamed worker'],
)
def test_a_caller_that_no_rule_admits_is_refused(db: Database, headers: dict[str, str]) -> None:
    assert isinstance(opened(db).identify(headers, PEER), Refused)


def test_workers_alone_need_no_name_claim(db: Database) -> None:
    rules = [{'when': {'org': 'acme', 'principal': 'key-1'}, 'access': 'worker'}]
    network = opened(db, rules=rules, name_claim=None)
    assert network.identify(STAMPED, PEER) == Caller('gpu-0123abcd', Access.WORKER)
    assert isinstance(network.identify(STAMPED | {'x-principal-id': 'user-7'}, PEER), Refused)


@pytest.mark.parametrize(
    ('keys', 'error'),
    [
        ({'claims': {}}, 'claims names no header for kind, org, principal'),
        ({'rules': []}, 'needs rules'),
        ({'name_claim': None}, 'name_claim names them'),
    ],
)
def test_a_server_config_names_its_claims_rules_and_names(keys: dict[str, object], error: str) -> None:
    with pytest.raises(ValidationError, match=error):
        NETWORKS.validated({'provider': 'proxy', 'listen': '0.0.0.0:8443', **KEYS, **keys})
    assert isinstance(NETWORKS.validated({'provider': 'proxy'}), Proxy), 'a worker names none'


def test_a_provisioned_worker_gets_the_home_secret_and_its_name(db: Database) -> None:
    network = opened(db)
    with pytest.raises(NotEnrolled, match='piloop secret set PILOOP_WORKER_CREDENTIAL'):
        network.enroll('gpu-0123abcd', Access.WORKER)
    with db.session() as s:
        set_secret(s, LocalSecrets(), System.HUMAN, None, 'PILOOP_WORKER_CREDENTIAL', SecretStr('proxy-key'))
        s.commit()
    enrollment = network.enroll('gpu-0123abcd', Access.WORKER)
    assert enrollment.server == 'https://piloop.test'
    assert {k: v.get_secret_value() for k, v in enrollment.env.items()} == {
        CREDENTIAL_ENV: 'proxy-key',
        NAME_ENV: 'gpu-0123abcd',
    }
    with pytest.raises(NotEnrolled, match='url'):
        opened(db, url=None).enroll('gpu-0123abcd', Access.WORKER)


def test_a_worker_client_sends_its_credential_and_name(monkeypatch: pytest.MonkeyPatch) -> None:
    config = Proxy(worker_header='x-who')
    monkeypatch.setenv(CREDENTIAL_ENV, 'proxy-key')
    monkeypatch.setenv(NAME_ENV, 'gpu-0123abcd')
    client = config.client('https://piloop.test')
    assert (client.headers['authorization'], client.headers['x-who']) == ('Bearer proxy-key', 'gpu-0123abcd')
    assert 'x-piloop-actor' not in client.headers
    assert config.client('https://piloop.test', System.ORCHESTRATOR).headers['x-piloop-actor'] == 'orchestrator'
    monkeypatch.delenv(NAME_ENV)
    with pytest.raises(NoCredential):
        config.client('https://piloop.test')


def test_a_served_request_is_known_by_its_stamps_alone(db: Database) -> None:
    network = opened(db)
    listener = network.listener()
    assert listener is not None and listener.tls is None, 'TLS ends at the proxy'
    server = Server(app(Tokens(db), network, [executor_router([ECHO])]), listener)
    server.start()
    try:
        stamped = httpx.post(server.url + ECHOED, headers=STAMPED, json=HI)
        bare = httpx.post(server.url + ECHOED, json=HI)
    finally:
        server.close()
    assert bare.status_code == 401
    assert stamped.json() == {'text': 'hi', 'caller': 'gpu-0123abcd'}


def test_the_proxy_network_makes_core_refuse_piloop_tokens(db: Database) -> None:
    assert opened(db).takes_tokens is False


def test_a_network_without_listen_binds_nothing_and_one_on_ipv6_binds_ipv6(db: Database) -> None:
    host = NetworkHost(Tokens(db), HomeSecrets(db, LocalSecrets()))
    assert Proxy().open(host).listener() is None, 'a worker dials; it listens nowhere'
    listener = opened(db, listen='[::1]:0').listener()
    assert listener is not None
    with listener.socket:
        assert listener.socket.family == socket.AF_INET6
