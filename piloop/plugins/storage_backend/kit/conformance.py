"""The contract of a storage backend: one check that every backend passes, built in or added by a package.

piloop's tests run it against each built-in backend, and a package runs it in its own tests.
``check_storage_backend`` drives a ``StorageBackend`` through what the store asks of it, then a ``Store`` that mirrors
to it and keeps nothing local. Point it at a scratch bucket or folder: it writes objects under ``piloop-contract/<random>/``, and the contract has no delete.
A broken rule raises ``ContractBroken``, which says which one.
"""

import io
import os
import uuid
from datetime import timedelta
from pathlib import Path

from piloop.plugins.storage_backend.contract import StorageBackend
from piloop.store import Store, key


class ContractBroken(AssertionError):
    """A storage backend does not do what the store relies on. The message says what."""


def _require(ok: bool, rule: str) -> None:
    if not ok:
        raise ContractBroken(rule)


def check_storage_backend(backend: StorageBackend, scratch: Path) -> None:
    """Check ``backend`` against the contract, with ``scratch`` as an empty local directory for files and a store."""
    scratch.mkdir(parents=True, exist_ok=True)
    base = f'piloop-contract/{uuid.uuid4().hex}'
    _require(
        isinstance(backend.url, str) and backend.url == backend.url.rstrip('/'), 'url is one spelling, no trailing /'
    )
    missing = f'{base}/blake3/missing/bytes.zst'
    _require(not backend.has(missing), 'has() is False for a key that was never uploaded')
    _require(not backend.download(missing, scratch / 'missing'), 'download() is False for a missing key')
    _require(not (scratch / 'missing').exists(), 'a failed download leaves no file')

    payload = os.urandom(3000)
    source = scratch / 'source'
    source.write_bytes(payload)
    name = f'{base}/blake3/some/bytes.zst'
    backend.upload(name, source)
    backend.upload(name, source)
    _require(backend.has(name), 'has() is True once a key is uploaded, again or not')
    _require(backend.download(name, scratch / 'fetched'), 'download() is True for an uploaded key')
    _require((scratch / 'fetched').read_bytes() == payload, 'download() writes the bytes that upload() read')

    prefixed = _Prefixed(backend, f'{base}/')
    store = Store(scratch / 'store', prefixed, evict_after=timedelta(0))
    blob = store.put(io.BytesIO(payload))
    _require(prefixed.has(key(blob.hash)), 'a blob that a store puts is on the backend')
    _require(not (scratch / 'store' / key(blob.hash)).exists(), 'a staging store keeps no local bytes once uploaded')
    out = io.BytesIO()
    store.get(blob.hash, out)
    _require(out.getvalue() == payload, 'a store reads back through the backend what it put')


class _Prefixed:
    """``remote`` with every key under ``prefix``, so that the store's objects stay in the scratch folder."""

    def __init__(self, remote: StorageBackend, prefix: str) -> None:
        self._remote, self._prefix = remote, prefix
        self.url = f'{remote.url}/{prefix.rstrip("/")}'

    def upload(self, key: str, path: Path) -> None:
        self._remote.upload(self._prefix + key, path)

    def download(self, key: str, path: Path) -> bool:
        return self._remote.download(self._prefix + key, path)

    def has(self, key: str) -> bool:
        return self._remote.has(self._prefix + key)
