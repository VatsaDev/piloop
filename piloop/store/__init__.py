"""The artifact store: a content-addressed store of blobs and the manifests that name them.

Generic: it knows files and bytes, nothing of campaigns, ideas or workspaces (``piloop.campaign.archive`` does). A blob
is addressed by the BLAKE3 hash of its whole content and kept zstd-compressed under ``blake3/<hash>/bytes.zst``, where
``<hash>`` is the 32-byte digest in lowercase unpadded base32: covalence's ``KvCas`` layout (``blake3/<digest>/bytes``)
with the compression in the name. A manifest is a blob too: a tree of paths, each with its blob, size and mode.

The contract callers use is ``Store``'s: ``put``/``put_path``, ``open``/``get``/``get_path``, ``has``/``missing``,
``put_manifest``/``manifest``, and ``check``, ``usage`` and ``evict`` for upkeep. It never hands out a key or a path, so a backend that keeps many small
blobs in one pack file (a hash → pack, offset, length index) fits behind it without changing a caller. Reads and
writes stream in ``CHUNK``-sized pieces: a blob of any size is never held in memory.

The store's own directory always holds what is local; a ``Remote`` (``piloop.store.s3``) mirrors it. Every blob put
while a remote is set is uploaded and marked ``blake3/<hash>/remote``; ``evict`` uploads what is still unmarked, then
drops the local bytes of marked blobs by age and by the store's local size. Evicting at age 0 makes the directory a
staging area: a blob's bytes are dropped as soon as they are uploaded, and a read fetches them back.

Append-only: there is no delete and no garbage collection.

Retirement: this package (this module, ``tree`` and ``s3``) is deleted once piloop can depend on covalence
(github.com/hiloopai/covalence) for the same contract: a local-disk backend, streaming (multipart) reads and writes,
list and delete, and its S3 store, all reachable from Python. ``Store``'s methods map onto its CAS trait (``has``,
``missing``, ``put``, ranged ``get``), and its ``KvCas`` keeps a block under ``blake3/<digest>/bytes`` with the same
digest spelling.
"""

import base64
import io
import json
import os
import shutil
import tempfile
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Generator, Iterable, Iterator
from compression.zstd import ZstdCompressor, ZstdError, ZstdFile
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Protocol

from blake3 import blake3
from pydantic import BaseModel, ConfigDict

CHUNK = 1 << 20
"""Bytes read, hashed and compressed at a time."""
BYTES = 'bytes.zst'
REMOTE = 'remote'
"""An empty file beside a blob's bytes: the remote holds the blob. It stays when the bytes are evicted."""
TMP = 'tmp'
BOUND = 'remote.url'
"""In the store's directory: the remote that its blobs' ``remote`` markers mean, written at the first upload."""
LOCKS = 64
"""Stripes of the per-blob lock ``_push`` takes: blobs, not threads, so a few threads rarely wait on each other."""
MANIFEST_VERSION = 1


class BlobMissing(KeyError):
    """Neither the store's directory nor its remote holds the blob."""


class BlobCorrupt(ValueError):
    """A blob's bytes no longer hash to its address."""


class RemoteChanged(ValueError):
    """The store's blobs were uploaded to one remote, and it is opened with another one, or with none."""


class Changed(RuntimeError):
    """A file changed while it was being put: what was compressed is not what was hashed."""


class Reader(Protocol):
    """A binary stream to read from: a file, a ``ZstdFile``, an ``io.BytesIO``."""

    def read(self, size: int = -1, /) -> bytes:
        """Up to ``size`` bytes; empty at the end."""
        ...


class Writer(Protocol):
    """A binary stream to write to."""

    def write(self, data: bytes, /) -> int:
        """Write ``data``; how many bytes were written."""
        ...


class Remote(Protocol):
    """Where blobs are mirrored: whole compressed objects by key. ``RemoteConfig.open`` makes one."""

    url: str

    def upload(self, key: str, path: Path) -> None:
        """Store the file at ``path`` under ``key``, streaming it."""
        ...

    def download(self, key: str, path: Path) -> bool:
        """Write the object under ``key`` to ``path``, streaming it; False when there is none."""
        ...

    def has(self, key: str) -> bool:
        """Whether an object is stored under ``key``."""
        ...


class RemoteConfig(BaseModel, ABC):
    """The settings of a remote backend, ``[storage.remote]``: each backend subclasses it with its own keys.

    ``kind`` names the backend (``piloop.store.config.remotes``). ``open`` makes the ``Remote`` that the settings
    describe. ``piloop.store.s3.S3Config`` and ``piloop.store.gcs.GcsConfig`` are the built-in ones.
    """

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    kind: str
    """The backend: the name it is registered under."""

    @abstractmethod
    def open(self) -> Remote:
        """The remote these settings describe."""


@dataclass(frozen=True)
class Blob:
    """A stored content: its address and its size uncompressed."""

    hash: str
    size: int


@dataclass(frozen=True)
class Entry:
    """One path of a manifest: a file's blob, size and mode, or a symlink's target (``hash`` None)."""

    path: str
    """Relative, ``/``-separated."""
    mode: int
    """Permission bits (``st_mode & 0o7777``)."""
    size: int = 0
    hash: str | None = None
    link: str | None = None


@dataclass(frozen=True)
class Manifest:
    """A tree as stored: its entries by path."""

    entries: tuple[Entry, ...]

    def to_bytes(self) -> bytes:
        rows = [{k: v for k, v in vars(e).items() if v is not None} for e in sorted(self.entries, key=lambda e: e.path)]
        return json.dumps({'version': MANIFEST_VERSION, 'entries': rows}, separators=(',', ':')).encode()

    @classmethod
    def from_bytes(cls, data: bytes) -> Manifest:
        doc = json.loads(data)
        if doc.get('version') != MANIFEST_VERSION:
            raise ValueError(f'manifest version {doc.get("version")!r}, expected {MANIFEST_VERSION}')
        return cls(tuple(Entry(**row) for row in doc['entries']))

    @property
    def blobs(self) -> set[str]:
        return {e.hash for e in self.entries if e.hash is not None}


class _Sink:
    """A stream that drops what it is given: ``check`` reads a blob for its hash only."""

    def write(self, data: bytes, /) -> int:
        return len(data)


def address(digest: bytes) -> str:
    """A digest as a blob's address: lowercase unpadded base32, as covalence spells it."""
    return base64.b32encode(digest).decode().rstrip('=').lower()


def hash_file(path: Path) -> Blob:
    """The blob ``path`` would be, read in chunks (not mapped: a file truncated under a mapping is a SIGBUS)."""
    hasher, size = blake3(), 0
    with path.open('rb') as src:
        while chunk := src.read(CHUNK):
            hasher.update(chunk)
            size += len(chunk)
    return Blob(address(hasher.digest()), size)


def key(hash: str) -> str:
    """The key of a blob's compressed bytes, here and on the remote."""
    return f'blake3/{hash}/{BYTES}'


@dataclass
class Evicted:
    """What ``evict`` did."""

    uploaded: int = 0
    evicted: int = 0
    freed: int = 0
    """Compressed bytes dropped from the store's directory."""


@dataclass
class Usage:
    """The store's directory: blobs it knows, how many have their bytes here, how much those take, how many are remote."""

    blobs: int = 0
    local: int = 0
    local_bytes: int = 0
    remote: int = 0


@dataclass
class Store:
    """Blobs by BLAKE3 hash under ``root``, mirrored to ``remote`` when there is one.

    ``evict_after``: the age past which ``evict`` drops the local bytes of a blob the remote holds; zero drops them as
    soon as they are uploaded (the directory is a staging area). ``max_local_bytes``: ``evict`` then drops the oldest
    such blobs until the local bytes fit. Both need a remote; without one nothing is ever evicted.
    """

    root: Path
    remote: Remote | None = None
    evict_after: timedelta | None = None
    max_local_bytes: int | None = None
    level: int = 3
    """zstd level: 3 is zstd's default, ~10x on the workspace's JSON ledgers (measured on a 5.5 GB sample)."""
    _locks: list[threading.Lock] = field(
        default_factory=lambda: [threading.Lock() for _ in range(LOCKS)], init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        if self.remote is None and (self.evict_after is not None or self.max_local_bytes is not None):
            raise ValueError('eviction needs a remote: without one the local copy is the only one')
        bound = self.root / BOUND
        was = bound.read_text() if bound.is_file() else None
        if was is not None and was != (self.remote.url if self.remote is not None else None):
            raise RemoteChanged(
                f'{self.root} mirrors to {was}, and its blobs marked as uploaded are there, but the store is opened with'
                f' {self.remote.url if self.remote is not None else "no remote"}: point [storage.remote] back at {was},'
                f' or copy its objects to the new remote and then write the new URL into {bound}'
            )

    @property
    def staging(self) -> bool:
        """Local bytes are dropped once uploaded."""
        return self.remote is not None and self.evict_after == timedelta(0)

    def free(self) -> int:
        """Bytes free on the filesystem of the store's directory, or of its nearest parent before the first put."""
        path = self.root
        while not path.exists():
            path = path.parent
        return shutil.disk_usage(path).free

    def _dir(self, hash: str) -> Path:
        return self.root / 'blake3' / hash

    def scratch(self) -> Path:
        """A directory of the store for temporary files, on the store's disk and outside every workspace."""
        tmp = self.root / TMP
        tmp.mkdir(parents=True, exist_ok=True)
        return tmp

    def _tmp(self) -> Path:
        fd, name = tempfile.mkstemp(dir=self.scratch())
        os.close(fd)
        return Path(name)

    def has(self, hash: str) -> bool:
        """Whether the store holds the blob, here or (as last confirmed) on the remote. No network."""
        d = self._dir(hash)
        return (d / BYTES).is_file() or (d / REMOTE).is_file()

    def missing(self, hashes: Iterable[str]) -> list[str]:
        """Those of ``hashes`` the store does not hold, in order."""
        return [h for h in hashes if not self.has(h)]

    def put(self, src: Reader) -> Blob:
        """Store what ``src`` yields to its end, hashing and compressing it in one pass."""
        tmp = self._tmp()
        try:
            hasher, size = self._compress(src, tmp)
            return self._commit(tmp, Blob(address(hasher.digest()), size))
        finally:
            tmp.unlink(missing_ok=True)

    def put_path(self, path: Path) -> Blob:
        """Store the file at ``path``: hashed first, so content the store holds is never compressed again.

        Raises ``Changed`` when the file changed between the hash and the compression.
        """
        blob = hash_file(path)
        if self.has(blob.hash):
            if self.remote is not None:
                self._push(blob.hash)
            return blob
        tmp = self._tmp()
        try:
            with path.open('rb') as src:
                again, _ = self._compress(src, tmp)
            if address(again.digest()) != blob.hash:
                raise Changed(f'{path} changed while it was stored')
            return self._commit(tmp, blob)
        finally:
            tmp.unlink(missing_ok=True)

    def _compress(self, src: Reader, dst: Path) -> tuple[blake3, int]:
        hasher, size = blake3(), 0
        compressor = ZstdCompressor(self.level)
        with dst.open('wb') as out:
            while chunk := src.read(CHUNK):
                hasher.update(chunk)
                size += len(chunk)
                out.write(compressor.compress(chunk))
            out.write(compressor.flush())
        return hasher, size

    def _commit(self, tmp: Path, blob: Blob) -> Blob:
        """Move compressed bytes into place unless the store holds them already; upload them when there is a remote."""
        d = self._dir(blob.hash)
        if not self.has(blob.hash):
            d.mkdir(parents=True, exist_ok=True)
            os.replace(tmp, d / BYTES)
        if self.remote is not None:
            self._push(blob.hash)
        return blob

    def _push(self, hash: str) -> bool:
        """Upload a blob the remote is not known to hold; drop its local bytes when staging. True if it uploaded.

        Under the blob's lock: when staging, a thread putting the same content would otherwise drop the bytes while
        another one's upload still reads them.
        """
        assert self.remote is not None
        d = self._dir(hash)
        uploaded = False
        with self._locks[int(hash[:4], 36) % LOCKS]:  # base32 digits are base-36 digits
            if not (d / REMOTE).is_file():
                self.remote.upload(key(hash), d / BYTES)
                (self.root / BOUND).write_text(self.remote.url)
                (d / REMOTE).touch()
                uploaded = True
            if self.staging:
                (d / BYTES).unlink(missing_ok=True)
        return uploaded

    @contextmanager
    def open(self, hash: str) -> Generator[Reader]:
        """The blob's content as a stream, unchecked: from the directory, else fetched from the remote.

        A fetched blob is put back in the directory unless it is a staging area; there it is read from a temporary
        file that goes when the stream closes.
        """
        local = self._dir(hash) / BYTES
        try:
            stream = ZstdFile(local, 'rb')
        except FileNotFoundError:
            stream = None
        if stream is not None:
            with stream:
                yield stream
            return
        tmp = self._fetch(hash)
        try:
            # opened before it is moved or removed: an eviction of the moved file meanwhile cannot pull it away
            with tmp.open('rb') as raw, ZstdFile(raw, 'rb') as stream:
                if not self.staging:
                    os.replace(tmp, local)
                yield stream
        finally:
            tmp.unlink(missing_ok=True)

    def _fetch(self, hash: str) -> Path:
        if self.remote is None or not (self._dir(hash) / REMOTE).is_file():
            raise BlobMissing(hash)
        tmp, fetched = self._tmp(), False
        try:
            if not self.remote.download(key(hash), tmp):
                raise BlobMissing(hash)
            fetched = True
            return tmp
        finally:
            if not fetched:  # a missing object, or a download that failed half-way
                tmp.unlink(missing_ok=True)

    def get(self, hash: str, dst: Writer) -> int:
        """Copy the blob's content into ``dst``, checking it against its address; the bytes copied.

        Raises ``BlobCorrupt`` after the copy when the content does not hash to ``hash``: the caller discards ``dst``.
        """
        hasher, size = blake3(), 0
        with self.open(hash) as src:
            while chunk := src.read(CHUNK):
                hasher.update(chunk)
                size += len(chunk)
                dst.write(chunk)
        if address(hasher.digest()) != hash:
            raise BlobCorrupt(hash)
        return size

    def get_path(self, hash: str, path: Path, mode: int = 0o644) -> int:
        """Write the blob to a new ``path`` with ``mode``: a checked copy beside it, linked into place.

        Raises:
            FileExistsError: Something is at ``path`` already, maybe written while the blob was read; it stays.
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(dir=path.parent, prefix=f'.{path.name}.')
        tmp = Path(name)
        try:
            with os.fdopen(fd, 'wb') as out:
                size = self.get(hash, out)
            tmp.chmod(mode)
            os.link(tmp, path)  # unlike a rename, a link never replaces what is there
            return size
        finally:
            tmp.unlink(missing_ok=True)

    def check(self, hash: str) -> str:
        """``ok``, ``missing`` or ``corrupt``: local bytes are read and hashed, an evicted blob is asked of the remote."""
        local = self._dir(hash) / BYTES
        if local.is_file():
            try:
                self.get(hash, _Sink())
            except BlobCorrupt, ZstdError:
                return 'corrupt'
            return 'ok'
        if self.remote is not None and (self._dir(hash) / REMOTE).is_file():
            return 'ok' if self.remote.has(key(hash)) else 'missing'
        return 'missing'

    def put_manifest(self, manifest: Manifest) -> Blob:
        """Store a manifest; its address names the tree."""
        return self.put(io.BytesIO(manifest.to_bytes()))

    def manifest(self, hash: str) -> Manifest:
        """The manifest stored under ``hash``, checked."""
        out = io.BytesIO()
        self.get(hash, out)
        return Manifest.from_bytes(out.getvalue())

    def usage(self) -> Usage:
        """Count the blobs in the directory: one scan of it."""
        usage = Usage()
        for _, _, stat, remote in self._scan():
            usage.blobs += 1
            usage.remote += remote
            if stat is not None:
                usage.local += 1
                usage.local_bytes += stat.st_size
        return usage

    def _scan(self) -> Iterator[tuple[str, Path, os.stat_result | None, bool]]:
        """Every blob of the directory: its hash, bytes path, the bytes' stat (None when evicted), whether remote."""
        with suppress(FileNotFoundError), os.scandir(self.root / 'blake3') as it:
            for entry in it:
                d = Path(entry.path)
                try:
                    stat: os.stat_result | None = (d / BYTES).stat()
                except FileNotFoundError:
                    stat = None
                remote = (d / REMOTE).is_file()
                if stat is not None or remote:
                    yield entry.name, d / BYTES, stat, remote

    def evict(self) -> Evicted:
        """Upload every blob the remote is not known to hold, then drop local bytes the policy says to.

        Dropped: every remote blob whose bytes are older than ``evict_after`` (by modification time: when they were
        put or fetched), then the oldest remaining until the local bytes are at most ``max_local_bytes``. Nothing
        without a remote.
        """
        done = Evicted()
        if self.remote is None:
            return done
        held: list[tuple[float, int, Path]] = []
        for hash, bytes_path, stat, remote in self._scan():
            if stat is None:
                continue
            if not remote:
                done.uploaded += self._push(hash)
                if self.staging:
                    done.evicted, done.freed = done.evicted + 1, done.freed + stat.st_size
                    continue
            held.append((stat.st_mtime, stat.st_size, bytes_path))
        held.sort()
        total = sum(size for _, size, _ in held)
        cutoff = time.time() - self.evict_after.total_seconds() if self.evict_after is not None else None
        for mtime, size, bytes_path in held:
            too_old = cutoff is not None and mtime <= cutoff
            too_big = self.max_local_bytes is not None and total > self.max_local_bytes
            if not too_old and not too_big:
                break
            bytes_path.unlink(missing_ok=True)
            total -= size
            done.evicted, done.freed = done.evicted + 1, done.freed + size
        return done
