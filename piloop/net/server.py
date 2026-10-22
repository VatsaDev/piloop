"""The server of ``piloop serve --listen``: FastAPI on uvicorn, in a thread of the daemon, on the network's socket.

Every route checks its caller with a dependency: the network identifies it (401 when it cannot), and the route's
scope must be the caller's (403). FastAPI runs a route that blocks, a long-poll say, in its thread pool.
"""

import ssl
import threading
from collections.abc import Awaitable, Callable, Sequence
from typing import Annotated

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.exceptions import RequestValidationError

from piloop.db.models import TokenScope
from piloop.net import InvalidBody, Route
from piloop.net.tokens import Caller
from piloop.plugins.network.contract import Listener, Network, Refused

GRACE_SECONDS = 5
"""How long ``Server.close`` lets the requests in flight finish."""


def authorized(network: Network, scope: TokenScope) -> Callable[[Request], Caller]:
    """The dependency that admits a caller of ``scope`` only."""

    def caller(request: Request) -> Caller:
        match network.identify(request.headers):
            case Refused(reason=reason):
                raise HTTPException(401, reason, headers={'WWW-Authenticate': 'Bearer'})
            case known if known.scope != scope:
                raise HTTPException(
                    403, f'{known.name} has a {known.scope} token, and this route takes a {scope} token'
                )
            case known:
                return known

    return caller


def app(network: Network, routes: Sequence[Route]) -> FastAPI:
    """``routes`` under ``/v1/executor``."""
    server = FastAPI(openapi_url=None)
    for route in routes:
        server.add_api_route(f'/v1/executor/{route.path}', _endpoint(network, route), methods=['POST'])
    return server


def _endpoint(network: Network, route: Route) -> Callable[[Request, Caller], Awaitable[Response]]:
    """The handler of ``route``: its body is read here and validated by the route, in the thread that handles it."""
    scoped = authorized(network, route.scope)

    async def endpoint(request: Request, caller: Annotated[Caller, Depends(scoped)]) -> Response:
        body = await request.body()
        try:
            answer = await run_in_threadpool(route.call, caller, body)
        except InvalidBody as e:
            raise RequestValidationError(e.error.errors()) from e
        if answer is None:
            return Response(status_code=204)
        return Response(answer.model_dump_json(), media_type='application/json')

    return endpoint


class Server:
    """uvicorn serving ``app`` on ``listener``, in a thread of this process, from ``start`` until ``close``."""

    def __init__(self, app: FastAPI, listener: Listener) -> None:
        self.listener = listener
        config = uvicorn.Config(
            app,
            log_config=None,
            access_log=False,
            lifespan='off',
            timeout_graceful_shutdown=GRACE_SECONDS,
            ssl_context_factory=_tls(listener.tls) if listener.tls is not None else None,
        )
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(
            target=self._server.run, kwargs={'sockets': [listener.socket]}, name='piloop-server', daemon=True
        )

    @property
    def url(self) -> str:
        """The URL that a client on this machine calls: the scheme, and the bound address with its port."""
        host, port = self.listener.socket.getsockname()[:2]
        scheme = 'https' if self.listener.tls is not None else 'http'
        return f'{scheme}://[{host}]:{port}' if ':' in host else f'{scheme}://{host}:{port}'

    def start(self) -> None:
        """Serve. The socket listens already, so a request that comes before uvicorn accepts waits in its backlog."""
        self._thread.start()

    def close(self) -> None:
        """Stop accepting, let the requests in flight finish for up to ``GRACE_SECONDS``, then close the socket."""
        self._server.should_exit = True
        self._thread.join()
        self.listener.socket.close()


def _tls(context: ssl.SSLContext) -> Callable[[uvicorn.Config, Callable[[], ssl.SSLContext]], ssl.SSLContext]:
    """Uvicorn's ``ssl_context_factory`` that returns ``context``, the network's own."""

    def factory(config: uvicorn.Config, default: Callable[[], ssl.SSLContext]) -> ssl.SSLContext:
        return context

    return factory
