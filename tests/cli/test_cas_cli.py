"""``piloop cas serve``: a process of its own that serves the home's CAS, and what stops it from starting."""

import signal
import socket
import subprocess
import sys
from pathlib import Path

import httpx2 as httpx
import pytest
import uvicorn
from pytest_mock import MockerFixture
from rich.console import Console

from piloop.cli import common
from piloop.plugins.cas.cached import Cached, CachedCas
from piloop.plugins.cas.disk import Disk
from piloop.plugins.cas.http import Http, HttpCas
from piloop.plugins.cas.kit.conformance import check_cas
from piloop.plugins.network.https import TOKEN_ENV
from piloop.settings import Settings
from tests.cli.test_cli import run_cli
from tests.conftest import at_head
from tests.daemon.test_serve import env_for

PILOOP = [sys.executable, '-m', 'piloop']


@pytest.fixture
def out(tmp_path: Path, schema: Path, monkeypatch: pytest.MonkeyPatch):
    at_head(schema, tmp_path / 'piloop.db')
    monkeypatch.setattr(common, 'settings', Settings(home=tmp_path, uv_tool_dir=tmp_path / 'uv-tools'))
    console = Console(record=True, width=200, force_terminal=False)
    monkeypatch.setattr(common, 'console', console)
    return lambda: console.export_text(clear=True)


def test_cas_serve_prints_its_url_and_serves_until_it_stops(out, mocker: MockerFixture) -> None:
    ran = mocker.patch.object(uvicorn.Server, 'run', side_effect=KeyboardInterrupt)  # a Ctrl-C, once it shut down
    run_cli(['cas', 'serve', '--listen', '127.0.0.1:0'])
    (listener,) = ran.call_args.kwargs['sockets']
    assert listener.fileno() == -1, 'the socket is closed'
    assert out().startswith('cas serving on http://127.0.0.1:')


@pytest.mark.parametrize(
    ('listen', 'refused'),
    [
        ('8443', "'8443' is not HOST:PORT"),
        ('0.0.0.0:8443', 'is not loopback, and without tls'),
    ],
)
def test_cas_serve_refuses_an_address_before_it_binds(out, listen: str, refused: str) -> None:
    with pytest.raises(SystemExit, match=refused):
        run_cli(['cas', 'serve', '--listen', listen])


def test_cas_serve_says_why_it_cannot_listen(out) -> None:
    with socket.create_server(('127.0.0.1', 0)) as taken:
        port = taken.getsockname()[1]
        with pytest.raises(SystemExit, match='the CAS does not listen: .*in use'):
            run_cli(['cas', 'serve', '--listen', f'127.0.0.1:{port}'])


def test_cas_serve_is_its_own_process_that_keeps_the_contract_for_a_worker(
    tmp_path: Path, schema: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = tmp_path / 'home'
    at_head(schema, home / 'piloop.db')
    env = env_for(home)
    secret = subprocess.run(
        [*PILOOP, 'token', 'create', 'gpu1'], env=env, capture_output=True, text=True, check=True
    ).stdout.strip()
    monkeypatch.setenv(TOKEN_ENV, secret)
    with subprocess.Popen(
        [*PILOOP, 'cas', 'serve', '--listen', '127.0.0.1:0'], env=env, stdout=subprocess.PIPE, text=True
    ) as served:
        try:
            assert served.stdout is not None
            url = served.stdout.readline().removeprefix('cas serving on ').strip()
            remote = Http.model_validate({'url': url})
            worker = tmp_path / 'worker'
            for provider in (remote, Cached(remote=remote, local=Disk(path=worker / 'store'))):
                match provider.open(worker):
                    case (HttpCas(http=http) | CachedCas(remote=HttpCas(http=http))) as store:
                        with http:  # the client lives as long as the process, which is longer than this test
                            check_cas(store, tmp_path / 'scratch')
                    case other:
                        pytest.fail(f'{provider} opened {other}')
            assert len(list((home / 'store' / 'blake3').iterdir())) > 3, 'the home holds the blobs'
            with pytest.raises(httpx.HTTPStatusError):
                httpx.head(url + '/v1/cas/x').raise_for_status()
            served.send_signal(signal.SIGTERM)
            assert served.wait(timeout=30) == -signal.SIGTERM
        finally:
            served.kill()  # a no-op once it has stopped; else `with` would wait for it
