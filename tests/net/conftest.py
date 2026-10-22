"""A live server of ``piloop serve --listen`` in the test's process, on a free loopback port."""

from collections.abc import Callable, Generator, Sequence
from contextlib import AbstractContextManager, contextmanager

import pytest

from piloop.db.models import TokenScope
from piloop.db.session import Database
from piloop.net import Route, tokens
from piloop.net.server import Server, app
from piloop.net.tokens import Tokens
from piloop.plugins.network.contract import NetworkHost
from piloop.plugins.network.https import Https, HttpsParameters

type Serving = Callable[..., AbstractContextManager[Server]]


@pytest.fixture
def network(db: Database) -> Https:
    return Https(NetworkHost(Tokens(db)), HttpsParameters.model_validate({'listen': '127.0.0.1:0'}))


@pytest.fixture
def serving(db: Database) -> Serving:
    """Serve ``routes`` with ``https`` and the given keys, until the block ends."""

    @contextmanager
    def serve(routes: Sequence[Route] = (), **keys: object) -> Generator[Server]:
        network = Https(NetworkHost(Tokens(db)), HttpsParameters.model_validate({'listen': '127.0.0.1:0', **keys}))
        listener = network.listener()
        assert listener is not None
        server = Server(app(network, routes), listener)
        server.start()
        try:
            yield server
        finally:
            server.close()

    return serve


@pytest.fixture
def token(db: Database) -> Callable[..., str]:
    """Make a token, a worker's unless ``scope`` says otherwise, and return its secret."""

    def make(name: str = 'gpu1', scope: TokenScope = TokenScope.WORKER) -> str:
        with db.session() as s:
            secret = tokens.create(s, name, scope)
            s.commit()
        return secret

    return make
