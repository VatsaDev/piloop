"""The daemon's server: it starts on the network's listener, and its shutdown closes it."""

import socket
from pathlib import Path

import httpx2 as httpx
import pytest
from pydantic import ValidationError

from piloop.cli import common
from piloop.daemon.main import Daemon
from piloop.plugins.network.contract import Address
from piloop.settings import Settings
from tests.cli.test_cli import run_cli

UNROUTED = '/v1/executor/none'
"""A path with no route: the server answers it with 404, a closed port not at all."""


def test_the_daemon_serves_on_listen_and_its_shutdown_closes_the_port(tmp_path: Path) -> None:
    daemon = Daemon(Settings(home=tmp_path / 'home'), listen=Address(host='127.0.0.1', port=0))
    with daemon.tracer.span('test') as span:
        daemon._listen(span)
    assert daemon._server is not None
    url = daemon._server.url
    assert httpx.post(url + UNROUTED).status_code == 404
    daemon.shutdown(timeout=5)
    with pytest.raises(httpx.ConnectError):
        httpx.post(url + UNROUTED)


def test_a_daemon_without_listen_serves_nothing(tmp_path: Path) -> None:
    daemon = Daemon(Settings(home=tmp_path / 'home'))
    with daemon.tracer.span('test') as span:
        daemon._listen(span)
    assert daemon._server is None
    daemon.shutdown(timeout=5)


def test_a_port_in_use_stops_the_start_with_the_reason(tmp_path: Path) -> None:
    with socket.create_server(('127.0.0.1', 0)) as taken:
        port = taken.getsockname()[1]
        daemon = Daemon(Settings(home=tmp_path / 'home'), listen=Address(host='127.0.0.1', port=port))
        with (
            daemon.tracer.span('test') as span,
            pytest.raises(SystemExit, match='the daemon does not listen: .*in use'),
        ):
            daemon._listen(span)
    daemon.shutdown(timeout=5)


def test_listen_must_be_host_colon_port(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(common, 'settings', Settings(home=tmp_path))
    with pytest.raises(SystemExit, match="'8443' is not HOST:PORT"):
        run_cli(['serve', '--listen', '8443'])


def test_a_tls_file_that_does_not_exist_is_a_validation_error_at_its_key(tmp_path: Path) -> None:
    (tmp_path / 'config.toml').write_text(
        '[plugins.network]\nlisten = "0.0.0.0:8443"\ntls = { cert = "missing.pem", key = "missing.key" }\n'
    )
    with pytest.raises(
        ValidationError, match=r'for \[plugins\]\nnetwork\.tls\.Certificate\.cert\n  Path does not point to a file'
    ):
        _ = Settings(home=tmp_path).providers
