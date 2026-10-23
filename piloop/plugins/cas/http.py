"""``http``: a CAS service over HTTP, ``piloop cas serve`` or another server of the same routes.

```toml
[plugins.cas]
provider = "cached"
remote = { provider = "http", url = "https://cas.example:7701" }
```

It speaks covalence's HTTP CAS (``piloop.net.cas``) through a client of this home's network (``[plugins.network]``),
which brings this machine's credential. A connection error, a 502, 503 or 504 is tried again with backoff for up to a
minute: each call is idempotent, because a blob is named by its content. A download goes to a temporary file, so that
a retry starts it over, and is checked against its CID.

Retirement: deleted when covalence's ``Cas.http`` ships and takes the network's client, and ``http`` opens it instead.
"""

import tempfile
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import BinaryIO

import httpx2 as httpx
from covalence import Cid, IntegrityError, StoreError
from pydantic import HttpUrl, ValidationError

from piloop.net.cas import Code, ErrorBody, route
from piloop.plugins.cas.contract import Cas, CasProvider, Writer
from piloop.plugins.cas.kit.objects import StreamCas, stream, write_all
from piloop.plugins.network.contract import NETWORKS
from piloop.settings import Settings

CHUNK = 1 << 20
RETRY_SECONDS = 60.0
FIRST_BACKOFF = 0.1
MAX_BACKOFF = 5.0
RETRIED = frozenset({httpx.codes.BAD_GATEWAY, httpx.codes.SERVICE_UNAVAILABLE, httpx.codes.GATEWAY_TIMEOUT})
"""The statuses of a server or a proxy in front of it that is down or busy for now."""
TMP = 'tmp'
"""``<PILOOP_HOME>/tmp``: where a download waits until it is checked."""


class Http(CasProvider):
    """``[plugins.cas] provider = "http"``: a CAS service, dialed through this home's network."""

    url: HttpUrl
    """The service: ``https://host:port``."""

    def open(self, home: Path) -> Cas:
        """Raises ``NoCredential`` when the network has no credential on this machine."""
        network = NETWORKS.selected(Settings(home=home).providers)
        return HttpCas(network.client(str(self.url)), home / TMP)

    def where(self, home: Path) -> str:
        return str(self.url).rstrip('/')


@dataclass
class HttpCas(StreamCas):
    """``Cas``: the blobs of the service that ``http`` calls, with downloads spooled in ``scratch``."""

    http: httpx.Client
    scratch: Path
    retry_for: float = RETRY_SECONDS

    def has(self, cid: Cid) -> bool:
        return self._retried(partial(self._head, cid))

    def put_fileobj(self, file: BinaryIO, *, max_bytes: int, expected_length: int | None = None) -> Cid:
        file.seek(0)
        hashed = stream(file, _skip, max_bytes=max_bytes, expected_length=expected_length)
        if not self.has(hashed.cid):
            self._retried(partial(self._upload, hashed.cid, _Source(file, hashed.size)))
        return hashed.cid

    def get_to(self, cid: Cid, writer: Writer, *, max_bytes: int, expected_length: int | None = None) -> int | None:
        self.scratch.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryFile(dir=self.scratch) as spool:
            if not self._retried(partial(self._download, cid, spool)):
                return None
            spool.seek(0)
            got = stream(spool, write_all(writer), max_bytes=max_bytes, expected_length=expected_length)
        if got.cid.digest != cid.digest:
            raise IntegrityError(f'{cid}: the service sent bytes that hash to {got.cid}')
        return got.size

    def _head(self, cid: Cid) -> bool:
        response = self.http.head(route(cid))
        if response.status_code == httpx.codes.NOT_FOUND:
            return False
        response.raise_for_status()
        return True

    def _upload(self, cid: Cid, source: _Source) -> None:
        response = self.http.put(route(cid), content=source.chunks(), headers={'Content-Length': str(source.length)})
        if response.status_code == httpx.codes.BAD_REQUEST and _code(response) == Code.CID_MISMATCH:
            raise IntegrityError(f'{cid}: the source changed while it was stored, and the service refused its bytes')
        response.raise_for_status()

    def _download(self, cid: Cid, spool: BinaryIO) -> bool:
        """Download the blob into ``spool``, over what a try before wrote; False when the service lacks it."""
        spool.seek(0)
        spool.truncate()
        with self.http.stream('GET', route(cid)) as response:
            if response.status_code == httpx.codes.NOT_FOUND:
                return False
            if response.is_error:
                response.read()  # for the message of the error
            response.raise_for_status()
            for chunk in response.iter_bytes(CHUNK):
                spool.write(chunk)
        return True

    def _retried[T](self, call: Callable[[], T]) -> T:
        """What ``call`` returns, tried again after a transient error with a doubling backoff, for ``retry_for``.

        Raises:
            StoreError: The last error, once it is not transient or ``retry_for`` is over.
        """
        deadline = time.monotonic() + self.retry_for
        backoff = FIRST_BACKOFF
        while True:
            try:
                return call()
            except httpx.HTTPError as e:
                wait = _wait(e, backoff)
                if wait is None or time.monotonic() + wait > deadline:
                    raise StoreError(f'the CAS at {self.http.base_url}: {_why(e)}') from e
            time.sleep(wait)
            backoff = min(2 * backoff, MAX_BACKOFF)


@dataclass(frozen=True)
class _Source:
    """The first ``length`` bytes of a seekable file: the bytes that were hashed, sent again from the start each try."""

    file: BinaryIO
    length: int

    def chunks(self) -> Iterator[bytes]:
        self.file.seek(0)
        left = self.length
        while left:
            if not (chunk := self.file.read(min(CHUNK, left))):
                raise IntegrityError(f'the source is shorter than the {self.length} bytes that were hashed')
            left -= len(chunk)
            yield chunk


def _skip(chunk: bytes) -> None:
    """A sink of ``stream`` that only hashes."""


def _wait(e: httpx.HTTPError, backoff: float) -> float | None:
    """How long to wait before the same request again, or None when it cannot succeed later."""
    match e:
        case httpx.HTTPStatusError(response=response) if response.status_code in RETRIED:
            after = response.headers.get('retry-after', '')
            return max(backoff, int(after)) if after.isdigit() else backoff  # an HTTP date: our own backoff
        case httpx.TransportError():
            return backoff
        case _:
            return None


def _code(response: httpx.Response) -> str | None:
    """The ``code`` of an error's body, or None when the body is not the service's: a proxy's page, say."""
    try:
        return ErrorBody.model_validate_json(response.content).code
    except ValidationError:
        return None


def _why(e: httpx.HTTPError) -> str:
    match e:
        case httpx.HTTPStatusError(request=request, response=response):
            return f'{request.method} {request.url.path} answered {response.status_code}: {response.text}'
        case _:
            return f'{type(e).__name__}: {e}'
