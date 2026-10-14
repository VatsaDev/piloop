"""The contract of a remote backend: one check that every backend passes, built in or added by a package.

piloop's tests run it against each built-in backend, and a package runs it in its own tests. ``check_remote`` drives
a ``Remote`` through what the store asks of it, then a ``Store`` that mirrors to it and keeps nothing local. Point it
at a scratch bucket or folder: it writes objects under ``piloop-contract/<random>/``, and the contract has no delete.
A broken rule raises ``ContractBroken``, which says which one.
"""

import io
import os
import uuid
from datetime import timedelta
from pathlib import Path

from piloop.store import Remote, Store, key


class ContractBroken(AssertionError):
    """A remote does not do what the store relies on. The message says what."""


def _require(ok: bool, rule: str) -> None:
    if not ok:
        raise ContractBroken(rule)


def check_remote(remote: Remote, scratch: Path) -> None:
    """Check ``remote`` against the contract, with ``scratch`` as an empty local directory for files and a store."""
    scratch.mkdir(parents=True, exist_ok=True)
    base = f'piloop-contract/{uuid.uuid4().hex}'
    _require(isinstance(remote.url, str) and remote.url == remote.url.rstrip('/'), 'url is one spelling, no trailing /')
    missing = f'{base}/blake3/missing/bytes.zst'
    _require(not remote.has(missing), 'has() is False for a key that was never uploaded')
    _require(not remote.download(missing, scratch / 'missing'), 'download() is False for a missing key')
    _require(not (scratch / 'missing').exists(), 'a failed download leaves no file')

    payload = os.urandom(3000)
    source = scratch / 'source'
    source.write_bytes(payload)
    name = f'{base}/blake3/some/bytes.zst'
    remote.upload(name, source)
    remote.upload(name, source)
    _require(remote.has(name), 'has() is True once a key is uploaded, again or not')
    _require(remote.download(name, scratch / 'fetched'), 'download() is True for an uploaded key')
    _require((scratch / 'fetched').read_bytes() == payload, 'download() writes the bytes that upload() read')

    prefixed = _Prefixed(remote, f'{base}/')
    store = Store(scratch / 'store', prefixed, evict_after=timedelta(0))
    blob = store.put(io.BytesIO(payload))
    _require(prefixed.has(key(blob.hash)), 'a blob that a store puts is on the remote')
    _require(not (scratch / 'store' / key(blob.hash)).exists(), 'a staging store keeps no local bytes once uploaded')
    out = io.BytesIO()
    store.get(blob.hash, out)
    _require(out.getvalue() == payload, 'a store reads back through the remote what it put')


class _Prefixed:
    """``remote`` with every key under ``prefix``, so that the store's objects stay in the scratch folder."""

    def __init__(self, remote: Remote, prefix: str) -> None:
        self._remote, self._prefix = remote, prefix
        self.url = f'{remote.url}/{prefix.rstrip("/")}'

    def upload(self, key: str, path: Path) -> None:
        self._remote.upload(self._prefix + key, path)

    def download(self, key: str, path: Path) -> bool:
        return self._remote.download(self._prefix + key, path)

    def has(self, key: str) -> bool:
        return self._remote.has(self._prefix + key)
