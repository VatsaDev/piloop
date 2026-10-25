"""The server of ``piloop serve --listen``: FastAPI on uvicorn, in a thread of the daemon, on the network's socket.

Every route checks its caller with a dependency: the network identifies it (401 when it cannot), and the route's
scope must be the caller's (403). A handler runs in the thread pool of Starlette, 40 threads; a long-poll awaits its
next call outside it, so many long-polls never starve a heartbeat.
"""

import asyncio
import socket
import ssl
import threading
import time
from collections.abc import Awaitable, Callable, Sequence
from typing import Annotated

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from uvicorn.protocols.http.h11_impl import H11Protocol

from piloop.net import InvalidBody, Refusal, Route
from piloop.net.tokens import Caller
from piloop.plugins.network.contract import Address, Listener, Network, Peer, Refused

GRACE_SECONDS = 5
"""How long ``Server.close`` lets the requests in flight finish."""


def authorized(network: Network) -> Callable[[Request], Caller]:
    """The dependency that admits a caller that the network knows. Every token has the scope ``worker``."""

    def caller(request: Request) -> Caller:
        match network.identify(request.headers, _peer(request)):
            case Refused(reason=reason):
                raise HTTPException(401, reason, headers={'WWW-Authenticate': 'Bearer'})
            case known:
                return known

    return caller


def _peer(request: Request) -> Peer:
    """The other end of ``request``'s connection."""
    client = request.client
    return Peer(
        address=None if client is None else Address(host=client.host, port=client.port),
        certificate=request.scope['state'].get(_CERTIFICATE),
    )


class App(FastAPI):
    """FastAPI, with the event that its ``Server`` sets when it shuts down."""

    def __init__(self) -> None:
        super().__init__(openapi_url=None)
        self.closing = asyncio.Event()
        """Set at the start of the shutdown: each long-poll that waits answers 204 at once, as at the end of its wait."""


def app(network: Network, routes: Sequence[Route]) -> App:
    """``routes`` under ``/v1/executor``."""
    server = App()
    for route in routes:
        server.add_api_route(f'/v1/executor/{route.path}', _endpoint(network, route, server.closing), methods=['POST'])
    return server


def _endpoint(
    network: Network, route: Route, closing: asyncio.Event
) -> Callable[[Request, Caller], Awaitable[Response]]:
    """The handler of ``route``: its body is read and validated here, and the route's handler runs in the thread pool.

    A long-poll calls it again after each wake (``Route.changed``) while it answers None, until the body's wait ends
    or the server closes.
    """
    scoped = authorized(network)

    async def endpoint(request: Request, caller: Annotated[Caller, Depends(scoped)]) -> Response:
        body = await request.body()
        try:
            call = route.bind(caller, body)
        except InvalidBody as e:
            raise RequestValidationError(e.error.errors()) from e
        deadline, seen = time.monotonic() + call.wait, 0
        try:
            while (answer := await run_in_threadpool(call.handle)) is None and route.changed is not None:
                if (left := deadline - time.monotonic()) <= 0:
                    break
                if (changed := await _unless_closing(route.changed(seen, left), closing)) is None:
                    break
                seen = changed
        except Refusal as e:
            return JSONResponse({'error': e.error, 'detail': str(e)}, status_code=e.status)
        if answer is None:
            return Response(status_code=204)
        return Response(answer.model_dump_json(), media_type='application/json')

    return endpoint


async def _unless_closing(changed: Awaitable[int], closing: asyncio.Event) -> int | None:
    """What ``changed`` returns, or None once ``closing`` is set."""
    waits = asyncio.ensure_future(changed), asyncio.ensure_future(closing.wait())
    try:
        await asyncio.wait(waits, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for wait in waits:
            wait.cancel()
    return None if closing.is_set() else waits[0].result()


class _Uvicorn(uvicorn.Server):
    """uvicorn that sets ``closing`` when it shuts down, before it waits for the requests in flight."""

    def __init__(self, config: uvicorn.Config, closing: asyncio.Event) -> None:
        super().__init__(config)
        self._closing = closing

    async def shutdown(self, sockets: list[socket.socket] | None = None) -> None:
        self._closing.set()
        await super().shutdown(sockets)


class Server:
    """uvicorn serving ``app`` on ``listener``: in this thread (``run``), or in its own from ``start`` until ``close``."""

    def __init__(self, app: App, listener: Listener) -> None:
        self.listener = listener
        config = uvicorn.Config(
            app,
            log_config=None,
            access_log=False,
            lifespan='off',
            http=_Protocol,
            timeout_graceful_shutdown=GRACE_SECONDS,
            ssl_context_factory=_tls(listener.tls) if listener.tls is not None else None,
        )
        self._server = _Uvicorn(config, app.closing)
        self._thread = threading.Thread(target=self.run, name='piloop-server', daemon=True)

    @property
    def url(self) -> str:
        """The URL that a client on this machine calls: the scheme, and the bound address with its port."""
        host, port = self.listener.socket.getsockname()[:2]
        scheme = 'https' if self.listener.tls is not None else 'http'
        return f'{scheme}://[{host}]:{port}' if ':' in host else f'{scheme}://{host}:{port}'

    def run(self) -> None:
        """Serve in this thread, then close the socket. In the main thread, SIGINT and SIGTERM stop it.

        uvicorn raises the signal again once it has shut down, so a Ctrl-C ends as ``KeyboardInterrupt``.
        """
        try:
            self._server.run(sockets=[self.listener.socket])
        finally:
            self.listener.socket.close()

    def start(self) -> None:
        """Serve in a thread. The socket listens already, so a request that comes before uvicorn accepts waits."""
        self._thread.start()

    def close(self) -> None:
        """Stop accepting, end the long-polls, let the other requests in flight finish for up to ``GRACE_SECONDS``.

        ``run`` closes the socket.
        """
        self._server.should_exit = True
        self._thread.join()


def _tls(context: ssl.SSLContext) -> Callable[[uvicorn.Config, Callable[[], ssl.SSLContext]], ssl.SSLContext]:
    """Uvicorn's ``ssl_context_factory`` that returns ``context``, the network's own."""

    def factory(config: uvicorn.Config, default: Callable[[], ssl.SSLContext]) -> ssl.SSLContext:
        return context

    return factory


_CERTIFICATE = 'piloop.peer_certificate'


class _Protocol(H11Protocol):
    """uvicorn's HTTP/1.1, which puts the peer's TLS certificate in each request's ``scope['state']``.

    uvicorn sets no ASGI ``tls`` extension in the scope, and this is the one per-connection seam that reaches it.
    """

    def connection_made(self, transport: asyncio.Transport) -> None:
        tls = transport.get_extra_info('ssl_object')
        self.app_state = {**self.app_state, _CERTIFICATE: None if tls is None else tls.getpeercert(binary_form=True)}
        super().connection_made(transport)
