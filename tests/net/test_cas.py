"""``piloop cas serve`` and the ``http`` provider: the CAS contract over the wire, through faults, tokens and TLS."""

import io
import os
from collections.abc import Callable, Generator, Iterator
from contextlib import ExitStack, contextmanager
from datetime import timedelta
from pathlib import Path

import httpx2 as httpx
import pytest
from covalence import Cid, IntegrityError, StoreError

from piloop.db.session import Database
from piloop.net import cas_server
from piloop.net.cas import route
from piloop.net.server import Server
from piloop.net.tokens import Tokens
from piloop.plugins.cas.cached import CachedCas
from piloop.plugins.cas.contract import UNBOUNDED, Cas, Writer
from piloop.plugins.cas.disk import Disk, DiskCas
from piloop.plugins.cas.http import Http, HttpCas
from piloop.plugins.cas.kit.conformance import check_cas
from piloop.plugins.network.contract import NetworkHost, NoCredential
from piloop.plugins.network.https import TOKEN_ENV, Https
from piloop.plugins.secrets.contract import HomeSecrets
from piloop.plugins.secrets.local import LocalSecrets
from tests.conftest import Dial
from tests.fixtures.netfault import CutResponse, Latency, Proxy

PAYLOAD = os.urandom(3 << 20)
"""Incompressible and several chunks long, so a cut can land inside a transfer."""


@pytest.fixture
def served(tmp_path: Path) -> DiskCas:
    """The CAS behind the service."""
    return DiskCas(tmp_path / 'served')


@contextmanager
def serving(db: Database, store: Cas, scratch: Path, **keys: object) -> Generator[Server]:
    """``piloop cas serve`` over ``store``, in a thread of the test, with ``https`` and the given keys."""
    network = Https.model_validate({'listen': '127.0.0.1:0', **keys}).open(
        NetworkHost(Tokens(db), HomeSecrets(db, LocalSecrets()))
    )
    listener = network.listener()
    assert listener is not None
    server = Server(cas_server.app(Tokens(db), network, store, scratch), listener)
    server.start()
    try:
        yield server
    finally:
        server.close()


@pytest.fixture
def server(db: Database, served: DiskCas, tmp_path: Path) -> Iterator[Server]:
    with serving(db, served, tmp_path / 'server-tmp') as running:
        yield running


type Connect = Callable[[str, str], HttpCas]


@pytest.fixture
def connect(tmp_path: Path, dial: Dial) -> Iterator[Connect]:
    """Make a client of the service at a URL with a token's secret; each closes when the test ends."""
    with ExitStack() as clients:

        def make(url: str, secret: str) -> HttpCas:
            return HttpCas(clients.enter_context(dial(url, secret)), tmp_path / 'client-tmp')

        yield make


@pytest.fixture
def proxy(server: Server) -> Iterator[Proxy]:
    with Proxy(server.listener.socket.getsockname()[:2]) as running:
        yield running


@pytest.fixture
def remote(proxy: Proxy, token: Callable[..., str], connect: Connect) -> HttpCas:
    """The service as a worker sees it: through the fault proxy, with its token."""
    return connect(f'http://127.0.0.1:{proxy.port}', token('gpu1'))


@pytest.mark.parametrize('tier', ['http', 'cached', 'staging'])
def test_http_and_a_cache_over_it_keep_the_contract_with_latency(
    remote: HttpCas, proxy: Proxy, tier: str, tmp_path: Path
) -> None:
    proxy.toxic = Latency(0.01)
    local = Disk(path=tmp_path / 'local')
    match tier:
        case 'http':
            store: Cas = remote
        case _:
            evict = timedelta(0) if tier == 'staging' else None
            store = CachedCas(local.tier(tmp_path), tmp_path / 'local', remote, 'http://cas', evict)
    check_cas(store, tmp_path / 'scratch')


def test_a_cut_answer_to_an_upload_is_retried_and_the_blob_is_stored_once(
    remote: HttpCas, proxy: Proxy, served: DiskCas
) -> None:
    proxy.toxic = CutResponse()
    cid = remote.put(PAYLOAD)
    assert proxy.cuts == 1
    assert served.get(cid) == PAYLOAD


def test_a_cut_download_is_retried_from_the_start(
    remote: HttpCas, proxy: Proxy, served: DiskCas, tmp_path: Path
) -> None:
    cid = served.put(PAYLOAD)
    proxy.toxic = CutResponse()
    assert remote.get_file(cid, tmp_path / 'fetched') == len(PAYLOAD)
    assert proxy.cuts == 1
    assert (tmp_path / 'fetched').read_bytes() == PAYLOAD


def test_a_workers_token_reads_and_writes_and_an_unknown_one_does_not(
    server: Server, served: DiskCas, token: Callable[..., str], connect: Connect
) -> None:
    cid = served.put(b'stored')
    worker = connect(server.url, token('gpu1'))
    assert worker.get(cid) == b'stored'
    assert worker.get(worker.put(b'from a worker')) == b'from a worker'
    anonymous = connect(server.url, 'plt_unknown')
    with pytest.raises(StoreError, match='401: .*"unauthenticated".*the token is unknown or revoked'):
        anonymous.get(cid)


def test_tls_ends_at_the_service_with_a_certificate_that_the_client_trusts(
    db: Database,
    served: DiskCas,
    token: Callable[..., str],
    certificate: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(TOKEN_ENV, token('gpu1'))
    with serving(db, served, tmp_path / 'server-tmp', tls=certificate) as running:
        assert running.url.startswith('https://127.0.0.1:')
        store = Http.model_validate({'url': running.url}).open(tmp_path)
        assert isinstance(store, HttpCas)
        with store.http:
            cid = store.put(b'over tls')
        assert served.get(cid) == b'over tls'
        with pytest.raises(httpx.RemoteProtocolError):
            httpx.head(running.url.replace('https', 'http') + route(cid))


def test_http_dials_with_the_credential_of_the_homes_network(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(TOKEN_ENV, raising=False)
    provider = Http.model_validate({'url': 'https://cas.example:7701'})
    assert provider.where(tmp_path) == 'https://cas.example:7701'
    with pytest.raises(NoCredential, match=f'set {TOKEN_ENV}'):
        provider.open(tmp_path)


def test_the_wire_answers_as_covalences_http_cas_does(
    server: Server, served: DiskCas, token: Callable[..., str]
) -> None:
    cid = served.put(b'hello')
    secret = token('gpu2')
    with httpx.Client(base_url=server.url, headers={'Authorization': f'Bearer {secret}'}) as http:
        found = http.head(route(cid))
        assert found.status_code == 200 and 'content-length' not in found.headers
        got = http.get(route(cid))
        assert (got.status_code, got.headers['content-type'], got.headers['content-length'], got.content) == (
            200, 'application/octet-stream', '5', b'hello'
        )  # fmt: skip
        absent = Cid.of(b'absent')
        assert http.head(route(absent)).status_code == 404
        assert http.get(route(absent)).json() == {'code': 'not_found', 'message': f'no blob {absent}'}
        assert http.put(route(cid), content=b'hello').status_code == 201, 'a blob stored already is stored'
        wrong = http.put(route(absent), content=b'not absent')
        assert (wrong.status_code, wrong.json()['code']) == (400, 'cid_mismatch')
        assert not served.has(Cid.of(b'not absent')) and not served.has(absent)
        chunked = http.put(route(cid), content=iter([b'hello']))
        assert (chunked.status_code, chunked.json()['code']) == (411, 'length_required')
        invalid = http.get('/v1/cas/xyz')
        assert (invalid.status_code, invalid.json()['code']) == (400, 'invalid_cid')
        sha256 = 'bafkreibm6jg3ux5qumhcn2b3flc3tyu6dmlb4xa7u5bf44yegnrjhc4yeq'
        assert http.get(f'/v1/cas/{sha256}').json()['code'] == 'invalid_cid'
        other = http.post(route(cid))
        assert (other.status_code, other.json()['code']) == (405, 'invalid_request')
    assert not list((served.root / 'tmp').iterdir()), 'no upload is left behind'


class Flaky(DiskCas):
    """A CAS whose storage fails once, for a moment, then a second time for good."""

    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.calls = 0

    def get_to(self, cid: Cid, writer: Writer, *, max_bytes: int, expected_length: int | None = None) -> int | None:
        self.calls += 1
        match self.calls:
            case 1:
                raise StoreError('the bucket is slow')
            case 2:
                return super().get_to(cid, writer, max_bytes=max_bytes, expected_length=expected_length)
            case _:
                raise RuntimeError('a bug')


def test_a_store_that_fails_for_a_moment_is_tried_again_after_retry_after(
    db: Database, token: Callable[..., str], connect: Connect, tmp_path: Path
) -> None:
    flaky = Flaky(tmp_path / 'flaky')
    with serving(db, flaky, tmp_path / 'server-tmp') as running:
        store = connect(running.url, token('gpu1'))
        absent = Cid.of(b'absent')
        assert store.get(absent) is None
        with pytest.raises(StoreError, match='500: {"code":"internal","message":"internal error"}'):
            store.get(absent)
        assert flaky.calls == 3, 'a 500 is not tried again'


def test_a_corrupt_blob_never_leaves_the_service(
    server: Server, served: DiskCas, token: Callable[..., str], connect: Connect
) -> None:
    cid = served.put(PAYLOAD)
    blob = next((served.root / 'blake3').iterdir()) / 'bytes.zst'
    blob.write_bytes(blob.read_bytes()[:-100])
    store = connect(server.url, token('gpu1'))
    with pytest.raises(StoreError, match='500: {"code":"internal"'):
        store.get(cid)


class Changing(io.BytesIO):
    """A source whose bytes change once they were read to the end, as a hash reads them: then they are ``then``."""

    def __init__(self, first: bytes, then: bytes) -> None:
        super().__init__(first)
        self.then: bytes | None = then

    def read(self, size: int | None = -1, /) -> bytes:
        chunk = super().read(size)
        if not chunk and self.then is not None:
            self.seek(0)
            self.truncate()
            self.write(self.then)
            self.then = None
        return chunk


@pytest.mark.parametrize(
    ('then', 'refused'),
    [(b'other bytes', 'the service refused its bytes'), (b'short', 'the source is shorter than the 11 bytes')],
)
def test_a_source_that_changes_while_it_is_uploaded_is_refused(
    remote: HttpCas, served: DiskCas, then: bytes, refused: str
) -> None:
    source = Changing(b'first bytes', then)
    with pytest.raises(IntegrityError, match=refused):
        remote.put_fileobj(source, max_bytes=UNBOUNDED)
    assert not served.has(Cid.of(b'first bytes')) and not served.has(Cid.of(then))


def answering(scratch: Path, *responses: httpx.Response) -> HttpCas:
    """A client of a stand-in service that answers each request with the next of ``responses``."""
    answers = iter(responses)
    transport = httpx.MockTransport(lambda request: next(answers))
    return HttpCas(httpx.Client(base_url='http://cas', transport=transport), scratch)


def test_a_service_that_sends_other_bytes_is_caught(tmp_path: Path) -> None:
    store = answering(tmp_path, httpx.Response(200, content=b'other'))
    with pytest.raises(IntegrityError, match='the service sent bytes that hash to'):
        store.get(Cid.of(b'asked'))


def test_a_busy_proxy_is_waited_for_and_its_page_is_no_refusal(tmp_path: Path) -> None:
    busy = httpx.Response(503, headers={'Retry-After': 'Wed, 21 Oct 2026 07:28:00 GMT'}, text='<html>busy</html>')
    store = answering(tmp_path, busy, httpx.Response(200), httpx.Response(400, text='<html>bad</html>'))
    assert store.has(Cid.of(b'x'))
    with pytest.raises(StoreError, match='400: <html>bad</html>'):
        store.has(Cid.of(b'y'))


def test_a_service_that_stays_down_fails_once_the_retries_are_over(tmp_path: Path) -> None:
    store = answering(tmp_path, *[httpx.Response(503, headers={'Retry-After': '1'})] * 2)
    store.retry_for = 0.5
    with pytest.raises(StoreError, match='503'):
        store.has(Cid.of(b'x'))


def test_a_refused_upload_whose_page_is_not_the_services_is_no_integrity_error(tmp_path: Path) -> None:
    store = answering(tmp_path, httpx.Response(404), httpx.Response(400, text='<html>bad</html>'))
    with pytest.raises(StoreError, match='PUT /v1/cas/.* answered 400: <html>bad</html>'):
        store.put(b'x')


def test_a_service_that_cannot_be_reached_fails_with_the_connection_error(tmp_path: Path) -> None:
    def unreachable(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError('connection refused', request=request)

    store = HttpCas(httpx.Client(base_url='http://cas', transport=httpx.MockTransport(unreachable)), tmp_path, 0.2)
    with pytest.raises(StoreError, match='the CAS at http://cas: ConnectError: connection refused'):
        store.has(Cid.of(b'x'))
