"""The daemon's server: it starts on the home's Unix socket and the network's listener, and its shutdown closes them."""

import os
import re
import socket
import tempfile
from collections.abc import Iterator
from pathlib import Path

import httpx2 as httpx
import pytest
from pydantic import ValidationError

from piloop import settings as settings_module
from piloop.api.client import Client
from piloop.api.contract import Selection
from piloop.cli import common
from piloop.daemon.main import Daemon
from piloop.net.server import local_listener
from piloop.plugins.network.contract import Address
from piloop.settings import Settings
from tests.cli.test_cli import run_cli

UNROUTED = '/v1/executor/none'
"""A path with no route: the server answers it with 404, a closed port not at all."""


def test_the_daemon_serves_on_listen_and_its_shutdown_closes_the_port_and_the_socket(tmp_path: Path) -> None:
    cfg = Settings(home=tmp_path / 'home')
    daemon = Daemon(cfg, listen=Address(host='127.0.0.1', port=0))
    with daemon.tracer.span('test') as span:
        daemon._listen(span)
    url = daemon._servers[-1].url
    assert httpx.post(url + UNROUTED).status_code == 404
    assert cfg.socket_file.stat().st_mode & 0o777 == 0o600, 'only the user of the home calls as its owner'
    with Client.on(cfg.socket_file) as client:
        assert client.status(Selection()).daemon.listen == url
    daemon.shutdown(timeout=5)
    with pytest.raises(httpx.ConnectError):
        httpx.post(url + UNROUTED)
    assert not cfg.socket_file.exists()


def test_a_daemon_on_a_fixed_port_says_where_it_listens_and_restarts_on_that_port(tmp_path: Path) -> None:
    with socket.create_server(('127.0.0.1', 0)) as free:
        port = free.getsockname()[1]
    cfg = Settings(home=tmp_path / 'home')
    daemon = Daemon(cfg, listen=Address(host='127.0.0.1', port=port))
    argv = list(daemon._argv)
    with daemon.tracer.span('test') as span:
        daemon._listen(span)
    assert (daemon._argv, daemon._servers[-1].url) == (argv, f'http://127.0.0.1:{port}')
    daemon.shutdown(timeout=5)


def test_a_daemon_without_listen_serves_only_its_socket_and_replaces_one_that_a_killed_daemon_left(
    tmp_path: Path,
) -> None:
    cfg = Settings(home=tmp_path / 'home')
    daemon = Daemon(cfg)
    stale = socket.socket(socket.AF_UNIX)
    stale.bind(str(cfg.socket_file))
    stale.close()
    with daemon.tracer.span('test') as span:
        daemon._listen(span)
    assert [server.listener.socket.family for server in daemon._servers] == [socket.AF_UNIX]
    with Client.on(cfg.socket_file) as client:
        assert client.status(Selection()).daemon.listen is None
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


@pytest.fixture
def short_sockets(monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """macOS's limit of 104 bytes for a socket's path, on any system, and a directory of short paths of this test."""
    monkeypatch.setattr(settings_module, 'SUN_PATH', 104)
    with tempfile.TemporaryDirectory(dir='/tmp') as short:
        monkeypatch.setattr(settings_module, 'SHORT_SOCKETS', Path(short))
        yield Path(short)


def test_a_home_whose_socket_does_not_fit_serves_on_a_short_path_that_its_clients_find(
    tmp_path: Path, short_sockets: Path
) -> None:
    """A home deep in a temporary directory, as pytest's are on macOS: ``<home>/piloop.sock`` needs 104 bytes or more."""
    home = tmp_path / ('h' * max(1, 105 - len(f'{tmp_path}//piloop.sock')))
    cfg = Settings(home=home)
    assert len(str(home / 'piloop.sock')) >= 104, 'too long for macOS; on Linux, short enough for its own 108'
    assert cfg.socket_file.parent == short_sockets / f'piloop-{os.getuid()}'
    assert len(str(cfg.socket_file)) < 104 and Settings(home=home).socket_file == cfg.socket_file, 'one path per home'
    assert Settings(home=tmp_path / 'other' / home.name).socket_file != cfg.socket_file
    daemon = Daemon(cfg)
    with daemon.tracer.span('test') as span:
        daemon._listen(span)
    assert cfg.socket_file.parent.stat().st_mode & 0o777 == 0o700, 'only the user of the home reaches it'
    with Client.on(cfg.socket_file) as client:
        assert client.status(Selection()).campaigns == [], 'a client finds the daemon from the home alone'
    daemon.shutdown(timeout=5)
    assert not cfg.socket_file.exists()


def test_a_socket_path_that_the_system_refuses_stops_the_start_with_the_reason(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings_module, 'SUN_PATH', 1000)  # no short path: the system's own limit refuses it
    cfg = Settings(home=tmp_path / ('h' * 120))
    daemon = Daemon(cfg)
    with (
        daemon.tracer.span('test') as span,
        pytest.raises(SystemExit, match=f'the daemon does not listen on {re.escape(str(cfg.socket_file))}: '),
    ):
        daemon._listen(span)
    daemon.shutdown(timeout=5)


def test_a_short_socket_directory_that_others_reach_is_refused(tmp_path: Path, short_sockets: Path) -> None:
    cfg = Settings(home=tmp_path / ('h' * 120))
    cfg.socket_file.parent.mkdir(mode=0o755)
    cfg.socket_file.parent.chmod(0o755)
    with pytest.raises(PermissionError, match='is not a directory that only this user reaches'):
        local_listener(cfg.socket_file)
    cfg.socket_file.parent.rmdir()
    (tmp_path / 'elsewhere').mkdir(mode=0o700)
    cfg.socket_file.parent.symlink_to(tmp_path / 'elsewhere')
    with pytest.raises(PermissionError, match='is not a directory that only this user reaches'):
        local_listener(cfg.socket_file)
