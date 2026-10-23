"""A TCP proxy that injects network faults between a client and a server on this machine: Toxiproxy's toxics.

```python
with Proxy(('127.0.0.1', server_port)) as proxy:
    proxy.toxic = Latency(0.2)
    client = httpx.Client(base_url=f'http://127.0.0.1:{proxy.port}')
```

Each toxic holds for every connection, open or new, until it is replaced (``proxy.toxic = None`` heals), except
``CutResponse``, which fires once.
"""

import contextlib
import socket
import struct
import threading
import time
from dataclasses import dataclass
from types import TracebackType
from typing import Self

CHUNK = 64 * 1024


@dataclass(frozen=True)
class Latency:
    """Delay every chunk, in both directions, by ``seconds``."""

    seconds: float


@dataclass(frozen=True)
class Blackhole:
    """Accept and read, but forward nothing: a partition that sends no RST, so the client only times out."""


@dataclass(frozen=True)
class Reset:
    """Reset every connection (RST) at its next chunk, and each new one at once."""


@dataclass(frozen=True)
class CutResponse:
    """Forward the request, then close the client's connection instead of sending it the answer. Once.

    It cuts the first answer that contains ``answer``, to a request that contains ``request``: ``request=b'POST
    /v1/executor/report'`` cuts a report's answer and lets the heartbeats through.
    """

    request: bytes = b''
    answer: bytes = b''


type Toxic = Latency | Blackhole | Reset | CutResponse


class Proxy:
    """Listens on ``127.0.0.1:<port>`` and forwards each connection to ``upstream``, through ``toxic``."""

    def __init__(self, upstream: tuple[str, int]) -> None:
        self.upstream = upstream
        self.toxic: Toxic | None = None
        self.cuts = 0
        """How many answers ``CutResponse`` dropped."""
        self._lock = threading.Lock()
        self._sockets: set[socket.socket] = set()
        self._listener = socket.create_server(('127.0.0.1', 0))
        self.port: int = self._listener.getsockname()[1]
        threading.Thread(target=self._accept, daemon=True).start()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, kind: type[BaseException] | None, error: BaseException | None, tb: TracebackType | None) -> None:
        self.close()

    def close(self) -> None:
        """Stop listening, and close every connection."""
        self._listener.close()
        with self._lock:
            open_now, self._sockets = list(self._sockets), set()
        for s in open_now:
            _close(s)

    def _accept(self) -> None:
        while True:
            try:
                client, _ = self._listener.accept()
            except OSError:  # the listener closed
                return
            if isinstance(self.toxic, Reset):
                _reset(client)
                continue
            try:
                server = socket.create_connection(self.upstream)
            except OSError:
                _reset(client)
                continue
            with self._lock:
                self._sockets |= {client, server}
            exchange = _Exchange()
            threading.Thread(target=self._pump, args=(client, server, False, exchange), daemon=True).start()
            threading.Thread(target=self._pump, args=(server, client, True, exchange), daemon=True).start()

    def _pump(self, src: socket.socket, dst: socket.socket, answer: bool, exchange: _Exchange) -> None:
        """Copy ``src`` to ``dst`` through the toxic until either end closes. ``answer``: server to client."""
        try:
            while chunk := src.recv(CHUNK):
                if not answer:  # kept whatever the toxic: a request sent before ``CutResponse`` was set is one too
                    exchange.request = chunk if exchange.answered else (exchange.request + chunk)[:CHUNK]
                match self.toxic:
                    case Latency(seconds=seconds):
                        time.sleep(seconds)
                    case Blackhole():
                        continue
                    case Reset():
                        _reset(src)
                        _reset(dst)
                        return
                    case CutResponse(request=request, answer=wanted) if (
                        answer and request in exchange.request and wanted in chunk and self._cut()
                    ):
                        _reset(dst)
                        _close(src)
                        return
                    case CutResponse() | None:
                        pass
                exchange.answered = answer
                dst.sendall(chunk)
        except OSError:  # the other pump, close or a reset closed a socket: the connection is over
            pass
        _close(dst)

    def _cut(self) -> bool:
        """Take the one cut that ``CutResponse`` allows: True for the first answer that asks."""
        with self._lock:
            if not isinstance(self.toxic, CutResponse):
                return False
            self.toxic = None
            self.cuts += 1
            return True


@dataclass
class _Exchange:
    """Where a connection is in its request and answer, for ``CutResponse``. HTTP/1.1 clients send one request at a
    time, so a chunk from the client after an answer starts the next request."""

    request: bytes = b''
    """The start of the request that the server answers now."""
    answered: bool = False
    """The last chunk was the server's."""


def _reset(s: socket.socket) -> None:
    """Close with an RST: a zero linger time drops what is unsent.

    ``SHUT_RD`` wakes the other pump's ``recv`` on the socket and sends nothing; while a ``recv`` still holds it, a
    close would not reset the connection. A ``SHUT_WR`` would send a FIN instead of the RST.
    """
    with contextlib.suppress(OSError):
        s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        s.shutdown(socket.SHUT_RD)
    s.close()


def _close(s: socket.socket) -> None:
    with contextlib.suppress(OSError):
        s.shutdown(socket.SHUT_RDWR)
    s.close()
