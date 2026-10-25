"""The server of ``piloop serve``: FastAPI on uvicorn, in threads of the daemon, on the Unix socket and the network.

Every route checks its caller with a dependency: core verifies a piloop token, a call on the Unix socket without one is
the home's owner, and the network identifies any other caller (401 when nobody knows it). The caller's access must
grant the route's (403). A handler runs in the thread pool of Starlette, 40 threads; a long-poll awaits its
next call outside it, so many long-polls never starve a heartbeat.
"""

import asyncio
import socket
import ssl
import threading
import time
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import Annotated

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from uvicorn.protocols.http.h11_impl import H11Protocol

from piloop.api.contract import GRANTS, Access
from piloop.net import Changed, InvalidBody, Refusal, Route
from piloop.net.tokens import PREFIX, Caller, Tokens
from piloop.plugins.network.contract import Address, Listener, Network, Peer, Refused
from piloop.settings import socket_dir

GRACE_SECONDS = 5
"""How long ``Server.close`` lets the requests in flight finish."""
OWNER = Caller('owner', Access.ADMIN)
"""Who calls on the home's Unix socket without a token: only the home's user can open it (Docker's socket)."""
OPENAPI = '/v1/openapi.json'


def authorized(access: Access) -> Callable[[Request], Caller]:
    """The dependency that admits a caller whose access grants ``access``: 401 for an unknown caller, 403 for another."""

    def caller(request: Request) -> Caller:
        server = served(request)
        known = _identified(server.tokens, server.network, request)
        if isinstance(known, Refused):
            raise HTTPException(401, known.reason, headers={'WWW-Authenticate': 'Bearer'})
        if access not in GRANTS[known.access]:
            raise HTTPException(403, f'{known.name} has {known.access} access, and this route needs {access}')
        return known

    return caller


def _identified(tokens: Tokens, network: Network, request: Request) -> Caller | Refused:
    """The caller of a piloop token (``plt_``), on any network, or else whom the network knows."""
    scheme, _, secret = request.headers.get('authorization', '').partition(' ')
    if scheme.lower() == 'bearer' and secret.startswith(PREFIX):
        if not network.takes_tokens:
            return Refused('this server takes no piloop token: its network authenticates every caller')
        return tokens.verify(secret) or Refused('the token is unknown or revoked')
    if request.scope['state'].get(_LOCAL):
        return OWNER
    return network.identify(request.headers, _peer(request))


def _peer(request: Request) -> Peer:
    """The other end of ``request``'s connection."""
    client = request.client
    return Peer(
        address=None if client is None else Address(host=client.host, port=client.port),
        certificate=request.scope['state'].get(_CERTIFICATE),
    )


class App(FastAPI):
    """FastAPI that knows who may call it, with the event that its ``Server`` sets when it shuts down.

    One ``Server`` serves one ``App``. A route's ``Refusal`` answers its status with ``{"error", "detail"}``.
    """

    def __init__(self, tokens: Tokens, network: Network, *, openapi_url: str | None = None) -> None:
        super().__init__(title='piloop', version='1', openapi_url=openapi_url)
        self.tokens = tokens
        self.network = network
        self.closing = asyncio.Event()
        """Set at the start of the shutdown: each long-poll that waits answers at once, as at the end of its wait."""

        @self.exception_handler(Refusal)
        def refused(request: Request, e: Refusal) -> Response:
            return JSONResponse({'error': e.error, 'detail': str(e)}, status_code=e.status)


def served(request: Request) -> App:
    """The ``App`` that serves ``request``."""
    found = request.app
    assert isinstance(found, App)  # every server of piloop serves one
    return found


def app(tokens: Tokens, network: Network, routes: Sequence[Route]) -> App:
    """The server's app, with the executor's ``routes`` under ``/v1/executor``, for workers. The daemon adds the API."""
    server = App(tokens, network, openapi_url=OPENAPI)
    workers = authorized(Access.WORKER)
    for route in routes:
        endpoint = _endpoint(workers, route, server.closing)
        server.add_api_route(f'/v1/executor/{route.path}', endpoint, methods=['POST'], include_in_schema=False)
    return server


async def changed(request: Request, waited: Changed, seen: int, timeout: float) -> int | None:
    """What ``waited`` returns, or None once the server of ``request`` closes: a long-poll then answers at once."""
    return await _unless_closing(waited(seen, timeout), served(request).closing)


def _endpoint(
    scoped: Callable[[Request], Caller], route: Route, closing: asyncio.Event
) -> Callable[[Request, Caller], Awaitable[Response]]:
    """The handler of ``route``: its body is read and validated here, and the route's handler runs in the thread pool.

    A long-poll calls it again after each wake (``Route.changed``) while it answers None, until the body's wait ends
    or the server closes.
    """

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
        """``url`` of its listener."""
        return url(self.listener)

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


def url(listener: Listener) -> str:
    """The URL that a client on this machine calls on a TCP listener: the scheme, and the address with its port."""
    host, port = listener.socket.getsockname()[:2]
    scheme = 'https' if listener.tls is not None else 'http'
    return f'{scheme}://[{host}]:{port}' if ':' in host else f'{scheme}://{host}:{port}'


def _tls(context: ssl.SSLContext) -> Callable[[uvicorn.Config, Callable[[], ssl.SSLContext]], ssl.SSLContext]:
    """Uvicorn's ``ssl_context_factory`` that returns ``context``, the network's own."""

    def factory(config: uvicorn.Config, default: Callable[[], ssl.SSLContext]) -> ssl.SSLContext:
        return context

    return factory


_CERTIFICATE = 'piloop.peer_certificate'
_LOCAL = 'piloop.local'


class _Protocol(H11Protocol):
    """uvicorn's HTTP/1.1, which marks each request's ``scope['state']``: the peer's TLS certificate, the Unix socket.

    uvicorn sets no ASGI ``tls`` extension in the scope, and this is the one per-connection seam that reaches it.
    """

    def connection_made(self, transport: asyncio.Transport) -> None:
        tls = transport.get_extra_info('ssl_object')
        self.app_state = {
            **self.app_state,
            _CERTIFICATE: None if tls is None else tls.getpeercert(binary_form=True),
            _LOCAL: transport.get_extra_info('socket').family == socket.AF_UNIX,
        }
        super().connection_made(transport)


def local_listener(path: Path) -> Listener:
    """Listen on the Unix socket ``path``, which only this user may open, in place of one that an earlier daemon left.

    The caller holds the daemon's lock, so a socket that is there is stale. Its directory is the home, of mode 0700, or
    a short one that only this user reaches (``socket_dir``).
    """
    socket_dir(path)
    path.unlink(missing_ok=True)
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.bind(str(path))
        path.chmod(0o600)
        sock.listen()
    except OSError:
        sock.close()
        raise
    return Listener(sock, None)
