"""The server of ``piloop serve --listen``: who may call it, TLS, and an executor's routes."""

import subprocess
from collections.abc import Callable
from pathlib import Path

import httpx2 as httpx
import pytest
from pydantic import BaseModel, ValidationError

from piloop.daemon.main import preflight
from piloop.db.models import TokenScope
from piloop.db.session import Database
from piloop.net import Route, tokens
from piloop.net.tokens import Caller
from piloop.plugins import host as plugins
from piloop.plugins.network.contract import NETWORKS, Address
from piloop.plugins.network.https import Https, HttpsNetwork
from piloop.settings import Settings
from tests.conftest import package
from tests.net.conftest import Serving


class Ask(BaseModel):
    text: str


class Answer(BaseModel):
    text: str
    caller: str


def _echo(caller: Caller, ask: Ask) -> Answer | None:
    return Answer(text=ask.text, caller=caller.name) if ask.text else None


ECHO = Route('echo', TokenScope.WORKER, Ask, _echo)
ECHOED = '/v1/executor/echo'
HI = {'text': 'hi'}


def test_a_caller_without_a_known_token_gets_401_and_a_revoked_token_stops_working(
    serving: Serving, network: HttpsNetwork, token: Callable[..., str], db: Database
) -> None:
    secret = token()
    with serving([ECHO]) as server, network.client(server.url, secret) as client, httpx.Client() as anonymous:
        assert client.post(ECHOED, json=HI).status_code == 200
        refused = anonymous.post(server.url + ECHOED, json=HI)
        assert (refused.status_code, refused.headers['www-authenticate']) == (401, 'Bearer')
        with network.client(server.url, 'plt_guessed') as guessed:
            assert guessed.post(ECHOED, json=HI).status_code == 401
        with db.session() as s:
            tokens.revoke(s, 'gpu1')
            s.commit()
        revoked = client.post(ECHOED, json=HI)
        assert (revoked.status_code, revoked.json()) == (401, {'detail': 'the token is unknown or revoked'})


def test_a_route_refuses_a_token_of_another_scope_with_403(
    serving: Serving, network: HttpsNetwork, token: Callable[..., str]
) -> None:
    secret = token('agent7', TokenScope.LEASE)
    with serving([ECHO]) as server, network.client(server.url, secret) as client:
        refused = client.post(ECHOED, json=HI)
        assert refused.status_code == 403
        assert refused.json() == {'detail': 'agent7 has a lease token, and this route takes a worker token'}


def test_an_executor_route_takes_its_model_and_answers_with_its_model_or_204(
    serving: Serving, network: HttpsNetwork, token: Callable[..., str]
) -> None:
    with serving([ECHO]) as server, network.client(server.url, token()) as client:
        answered = client.post('/v1/executor/echo', json={'text': 'hi'})
        assert (answered.status_code, answered.json()) == (200, {'text': 'hi', 'caller': 'gpu1'})
        assert client.post('/v1/executor/echo', json={'text': ''}).status_code == 204
        invalid = client.post('/v1/executor/echo', json={'words': 'hi'})
        assert invalid.status_code == 422
        assert invalid.json()['detail'][0]['loc'] == ['text']
        assert client.post('/v1/executor/other', json={}).status_code == 404


@pytest.fixture
def certificate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """A self-signed certificate for 127.0.0.1, which clients trust through ``SSL_CERT_FILE``."""
    cert, key = tmp_path / 'cert.pem', tmp_path / 'key.pem'
    subprocess.run(
        ['openssl', 'req', '-x509', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:prime256v1', '-nodes', '-days',
         '1', '-subj', '/CN=127.0.0.1', '-addext', 'subjectAltName=IP:127.0.0.1', '-keyout', str(key), '-out',
         str(cert)],
        check=True,
        capture_output=True,
    )  # fmt: skip
    monkeypatch.setenv('SSL_CERT_FILE', str(cert))
    return {'cert': str(cert), 'key': str(key)}


def test_tls_with_a_certificate_that_the_client_trusts_through_ssl_cert_file(
    serving: Serving, network: HttpsNetwork, token: Callable[..., str], certificate: dict[str, str]
) -> None:
    with serving([ECHO], tls=certificate) as server, network.client(server.url, token()) as client:
        assert server.url.startswith('https://127.0.0.1:')
        assert client.post(ECHOED, json=HI).status_code == 200
        with pytest.raises(httpx.RemoteProtocolError):
            httpx.post(server.url.replace('https', 'http') + ECHOED, json=HI)


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
