"""The fault proxy of ``tests/fixtures/netfault.py``: each toxic does what the network tests rely on."""

import socket
import socketserver
import threading
import time
from collections.abc import Iterator

import pytest

from tests.conftest import wait_until
from tests.fixtures.netfault import Blackhole, CutResponse, Latency, Proxy, Reset


class _EchoServer(socketserver.ThreadingTCPServer):
    """Answers each chunk with itself, and keeps what came."""

    daemon_threads = True

    def __init__(self) -> None:
        super().__init__(('127.0.0.1', 0), _Echo)
        self.received: list[bytes] = []


class _Echo(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        assert isinstance(self.server, _EchoServer)
        while data := self.request.recv(1024):
            self.server.received.append(data)
            self.request.sendall(data)


@pytest.fixture
def echo() -> Iterator[_EchoServer]:
    server = _EchoServer()
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield server
    server.shutdown()
    server.server_close()


@pytest.fixture
def proxy(echo: _EchoServer) -> Iterator[Proxy]:
    with Proxy(('127.0.0.1', echo.server_address[1])) as proxy:
        yield proxy


def _connect(proxy: Proxy) -> socket.socket:
    return socket.create_connection(('127.0.0.1', proxy.port), timeout=5)


def _round_trip(proxy: Proxy, data: bytes = b'ping') -> bytes:
    with _connect(proxy) as client:
        client.sendall(data)
        return client.recv(1024)


def test_without_a_toxic_it_forwards_both_ways(proxy: Proxy) -> None:
    assert _round_trip(proxy) == b'ping'


def test_latency_delays_the_request_and_the_answer(proxy: Proxy) -> None:
    proxy.toxic = Latency(0.2)
    start = time.monotonic()
    assert _round_trip(proxy) == b'ping'
    assert time.monotonic() - start >= 0.4


def test_a_blackhole_forwards_nothing_and_sends_no_reset_until_it_heals(proxy: Proxy, echo: _EchoServer) -> None:
    with _connect(proxy) as client:
        client.sendall(b'before')
        assert client.recv(1024) == b'before'
        proxy.toxic = Blackhole()
        client.sendall(b'lost')
        client.settimeout(0.3)
        with pytest.raises(TimeoutError):
            client.recv(1024)
    assert echo.received == [b'before']
    proxy.toxic = None
    assert _round_trip(proxy) == b'ping'


def test_reset_breaks_open_and_new_connections(proxy: Proxy) -> None:
    with _connect(proxy) as client:
        client.sendall(b'before')  # an open connection: the proxy accepted it before the toxic, and forwards it
        assert client.recv(1024) == b'before'
        proxy.toxic = Reset()
        client.sendall(b'x')
        with pytest.raises(ConnectionResetError):
            client.recv(1024)
    with pytest.raises(ConnectionResetError), _connect(proxy) as client:  # at the connect or the first read
        client.recv(1024)


def test_cut_response_lets_the_request_through_and_drops_one_answer(proxy: Proxy, echo: _EchoServer) -> None:
    proxy.toxic = CutResponse()
    with _connect(proxy) as client:
        client.sendall(b'request')
        with pytest.raises(ConnectionResetError):
            client.recv(1024)
    wait_until(lambda: echo.received == [b'request'])
    assert (proxy.cuts, proxy.toxic) == (1, None)
    assert _round_trip(proxy) == b'ping'
