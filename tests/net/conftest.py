"""A live server of ``piloop serve --listen`` in the test's process, on a free loopback port."""

import subprocess
from collections.abc import Callable, Generator, Sequence
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path

import pytest

from piloop.api.contract import Access
from piloop.db.session import Database
from piloop.net import Route, tokens
from piloop.net.server import Server, app, executor_router
from piloop.net.tokens import Tokens
from piloop.plugins.network.contract import NetworkHost
from piloop.plugins.network.https import Https, HttpsNetwork
from piloop.plugins.secrets.contract import HomeSecrets
from piloop.plugins.secrets.local import LocalSecrets

type Serving = Callable[..., AbstractContextManager[Server]]


@pytest.fixture
def network(db: Database) -> HttpsNetwork:
    return Https.model_validate({'listen': '127.0.0.1:0'}).open(
        NetworkHost(Tokens(db), HomeSecrets(db, LocalSecrets()))
    )


@pytest.fixture
def serving(db: Database) -> Serving:
    """Serve ``routes`` with ``https`` and the given keys, until the block ends."""

    @contextmanager
    def serve(routes: Sequence[Route] = (), **keys: object) -> Generator[Server]:
        network = Https.model_validate({'listen': '127.0.0.1:0', **keys}).open(
            NetworkHost(Tokens(db), HomeSecrets(db, LocalSecrets()))
        )
        listener = network.listener()
        assert listener is not None
        server = Server(app(Tokens(db), network, [executor_router(routes)]), listener)
        server.start()
        try:
            yield server
        finally:
            server.close()

    return serve


@pytest.fixture
def token(db: Database) -> Callable[..., str]:
    """Make a token, a worker's unless ``access`` says otherwise, and return its secret."""

    def make(name: str = 'gpu1', access: Access = Access.WORKER) -> str:
        with db.session() as s:
            secret = tokens.create(s, name, access)
            s.commit()
        return secret

    return make


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
