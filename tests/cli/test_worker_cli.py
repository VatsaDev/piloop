"""``piloop worker --server URL --pool NAME``: a worker on this machine, of a server on another."""

import os
import signal
import threading
from collections.abc import Callable, Generator
from pathlib import Path

import pytest

from piloop.cli import common
from piloop.db.models import LeaseWorker, TokenScope
from piloop.db.session import Database
from piloop.net import tokens
from piloop.net.server import Server, app
from piloop.net.tokens import Tokens
from piloop.plugins.executor.kit.capacity import Pool
from piloop.plugins.executor.kit.leases import Leases
from piloop.plugins.executor.kit.remote import lease_routes
from piloop.plugins.network.contract import NetworkHost
from piloop.plugins.network.https import Https
from piloop.settings import Settings
from tests.cli.test_cli import run_cli
from tests.conftest import wait_until
from tests.test_lease import Recorder


@pytest.fixture
def server(db: Database) -> Generator[str]:
    """The URL of a server whose only pool is ``gpu``, on the test's database."""
    network = Https.model_validate({'listen': '127.0.0.1:0'}).open(NetworkHost(Tokens(db)))
    listener = network.listener()
    assert listener is not None
    server = Server(app(network, lease_routes(Leases(db, Recorder(db), pools={'gpu': Pool()}), db.changed)), listener)
    server.start()
    yield server.url
    server.close()


@pytest.fixture
def as_worker(tmp_path: Path, db: Database, monkeypatch: pytest.MonkeyPatch) -> Generator[Callable[[], None]]:
    """Give this process a worker's home and a token called ``gpu1``. Its signal handlers come back after the test."""
    handlers = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)}

    def token() -> None:
        with db.session() as s:
            secret = tokens.create(s, 'gpu1', TokenScope.WORKER)
            s.commit()
        monkeypatch.setattr(common, 'settings', Settings(home=tmp_path / 'worker', token=secret))

    yield token
    for sig, handler in handlers.items():
        signal.signal(sig, handler)


def test_a_worker_without_a_token_says_how_to_get_one(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(common, 'settings', Settings(home=tmp_path))
    with pytest.raises(SystemExit, match=r'set PILOOP_TOKEN to the secret that `piloop token create <name>` printed'):
        run_cli(['worker', '--server', 'http://127.0.0.1:9', '--pool', 'gpu'])


def test_a_worker_in_a_pool_that_the_server_lacks_exits_with_why(server: str, as_worker: Callable[[], None]) -> None:
    as_worker()
    with pytest.raises(SystemExit, match="no pool 'cpu': the pools are gpu"):
        run_cli(['worker', '--server', server, '--pool', 'cpu'])


def test_a_worker_registers_with_its_tokens_name_and_stops_on_sigterm(
    server: str, as_worker: Callable[[], None], db: Database, tmp_path: Path
) -> None:
    as_worker()

    def stop_once_registered() -> None:
        def registered() -> bool:
            with db.session() as s:
                return s.get(LeaseWorker, 'gpu1') is not None

        wait_until(registered)
        os.kill(os.getpid(), signal.SIGTERM)

    threading.Thread(target=stop_once_registered, daemon=True).start()
    run_cli(['worker', '--server', server, '--pool', 'gpu'])
    assert (tmp_path / 'worker').is_dir(), 'its home, made for its spool and its free disk'
