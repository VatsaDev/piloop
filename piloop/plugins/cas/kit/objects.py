"""A CAS of zstd-compressed blobs, one object each, under ``blake3/<digest>/bytes.zst``: on a disk or in a bucket.

``ObjectsCas`` is the store; ``Objects`` is where its objects go: ``Dir`` for a directory, a client for a bucket. The
key is covalence's ``KvCas`` layout (``blake3/<digest>/bytes``, the digest in lowercase unpadded base32) with the
compression in the name, and the object is one zstd frame of the blob. The store hashes a file before it compresses
it, so content that it holds is never compressed again. Reads and writes stream in pieces of 1 MiB.

Retirement: deleted when covalence's ``Cas.kv`` takes a directory and compression from Python (``Kv.directory``,
``compression='zstd'``), and ``disk``, ``s3`` and ``gcs`` open that instead.
"""

import base64
import binascii
import io
import os
import tempfile
from abc import ABC, abstractmethod
from collections.abc import Callable, Generator, Sequence
from compression.zstd import ZstdCompressor, ZstdError, ZstdFile
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from os import PathLike
from pathlib import Path
from typing import BinaryIO, Protocol

from covalence import Cid, IntegrityError

from piloop.plugins.cas.contract import BLAKE3, UNBOUNDED, Writer

BYTES = 'bytes.zst'
DIGEST = 32


def name(cid: Cid) -> str:
    """The name of a blob's directory: its BLAKE3 digest in lowercase unpadded base32, as covalence spells it."""
    if cid.hash != BLAKE3:
        raise ValueError(f'{cid} is hashed with {cid.hash.name}: the CAS addresses by BLAKE3')
    return base64.b32encode(cid.digest).decode().rstrip('=').lower()


def named(text: str) -> Cid:
    """The raw CID of the blob whose directory ``name`` is ``text``.

    Raises:
        ValueError: ``text`` is no such name.
    """
    try:
        digest = base64.b32decode(text.upper() + '=' * (-len(text) % 8))
    except binascii.Error as e:
        raise ValueError(f'{text!r} is no base32 BLAKE3 digest') from e
    if len(digest) != DIGEST or name(cid := Cid.from_parts('raw', BLAKE3, digest)) != text:
        raise ValueError(f'{text!r} is no base32 BLAKE3 digest')
    return cid


def key(cid: Cid) -> str:
    """The key of a blob's compressed bytes."""
    return f'blake3/{name(cid)}/{BYTES}'


class Reader(Protocol):
    """A binary stream to read from."""

    def read(self, size: int = -1, /) -> bytes:
        """Up to ``size`` bytes; empty at the end."""
        ...


class _Once(io.RawIOBase, BinaryIO):
    """``src`` as covalence's ``Cid.of_fileobj`` reads it, once from its start; each chunk also goes to ``sink``.

    One pass then both hashes a stream and writes it elsewhere: covalence has no incremental hasher.
    """

    def __init__(self, src: Reader, sink: Callable[[bytes], object]) -> None:
        self._src, self._sink, self.size = src, sink, 0

    def seek(self, offset: int, whence: int = io.SEEK_SET, /) -> int:
        if (offset, whence, self.size) != (0, io.SEEK_SET, 0):
            raise io.UnsupportedOperation('a stream is read once, from its start')
        return 0

    def read(self, size: int = -1, /) -> bytes:
        chunk = self._src.read(size)
        self.size += len(chunk)
        self._sink(chunk)
        return chunk


@dataclass(frozen=True)
class Streamed:
    """What ``stream`` passed on: the CID of the bytes, and how many."""

    cid: Cid
    size: int


def stream(src: Reader, sink: Callable[[bytes], object], *, max_bytes: int, expected_length: int | None) -> Streamed:
    """Pass what ``src`` yields to its end on to ``sink``, hashing it in the same pass.

    Raises:
        ValueError: More than ``max_bytes``, or other than ``expected_length``.
    """
    once = _Once(src, sink)
    return Streamed(Cid.of_fileobj(once, max_bytes=max_bytes, expected_length=expected_length), once.size)


def write_all(writer: Writer) -> Callable[[bytes], None]:
    """``writer.write``, repeated over what a short write left."""

    def write(data: bytes) -> None:
        view = memoryview(data)
        while view:
            view = view[writer.write(view.tobytes()) :]

    return write


class StreamCas(ABC):
    """The methods of ``Cas`` that follow from ``has``, ``put_fileobj`` and ``get_to``."""

    @abstractmethod
    def has(self, cid: Cid) -> bool:
        """``Cas.has``."""

    @abstractmethod
    def put_fileobj(self, file: BinaryIO, *, max_bytes: int, expected_length: int | None = None) -> Cid:
        """``Cas.put_fileobj``."""

    @abstractmethod
    def get_to(self, cid: Cid, writer: Writer, *, max_bytes: int, expected_length: int | None = None) -> int | None:
        """``Cas.get_to``."""

    def missing(self, cids: Sequence[Cid]) -> list[Cid]:
        return [c for c in cids if not self.has(c)]

    def put(self, data: bytes) -> Cid:
        return self.put_fileobj(io.BytesIO(data), max_bytes=len(data))

    def get(self, cid: Cid) -> bytes | None:
        out = io.BytesIO()
        return None if self.get_to(cid, out, max_bytes=UNBOUNDED) is None else out.getvalue()

    def put_file(self, path: str | PathLike[str]) -> Cid:
        with open(path, 'rb') as file:
            return self.put_fileobj(file, max_bytes=UNBOUNDED)

    def get_file(self, cid: Cid, path: str | PathLike[str]) -> int | None:
        target = Path(path)
        fd, tmp = tempfile.mkstemp(dir=target.parent, prefix=f'.{target.name}.')
        try:
            with os.fdopen(fd, 'wb') as out:
                size = self.get_to(cid, out, max_bytes=UNBOUNDED)
            if size is not None:
                os.replace(tmp, target)
            return size
        finally:
            Path(tmp).unlink(missing_ok=True)


class Objects(Protocol):
    """Where an ``ObjectsCas`` keeps its objects: whole files by key. It never lists, changes or deletes one."""

    def has(self, key: str) -> bool:
        """Whether an object is under ``key``."""
        ...

    def put(self, key: str, path: Path) -> None:
        """Store the file at ``path`` under ``key``. The file is the caller's to drop after, so it may be moved."""
        ...

    def open(self, key: str) -> AbstractContextManager[BinaryIO | None]:
        """The object under ``key`` as a stream, or None when there is none."""
        ...


@dataclass(frozen=True)
class Dir:
    """Objects as files under ``root``: a put moves the file into place, a read opens it."""

    root: Path

    def has(self, key: str) -> bool:
        return (self.root / key).is_file()

    def put(self, key: str, path: Path) -> None:
        target = self.root / key
        target.parent.mkdir(parents=True, exist_ok=True)
        os.replace(path, target)

    @contextmanager
    def open(self, key: str) -> Generator[BinaryIO | None]:
        try:
            file = (self.root / key).open('rb')
        except FileNotFoundError:
            yield None
            return
        with file:
            yield file


@dataclass
class ObjectsCas(StreamCas):
    """Blobs as zstd-compressed objects of ``objects``, with temporary files in ``scratch``."""

    objects: Objects
    scratch: Path
    """On the disk of ``objects`` for a ``Dir``, so that a put is a rename."""
    level: int = 3
    """zstd level: 3 is zstd's default, ~10x on the workspace's JSON ledgers (measured on a 5.5 GB sample)."""

    def has(self, cid: Cid) -> bool:
        return self.objects.has(key(cid))

    def put_fileobj(self, file: BinaryIO, *, max_bytes: int, expected_length: int | None = None) -> Cid:
        cid = Cid.of_fileobj(file, max_bytes=max_bytes, expected_length=expected_length)
        if self.has(cid):
            return cid
        self.scratch.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(dir=self.scratch)
        tmp = Path(name)
        try:
            compressor = ZstdCompressor(self.level)
            with os.fdopen(fd, 'wb') as out:
                file.seek(0)
                again = stream(
                    file,
                    lambda chunk: out.write(compressor.compress(chunk)),
                    max_bytes=max_bytes,
                    expected_length=expected_length,
                )
                out.write(compressor.flush())
            if again.cid != cid:
                raise IntegrityError(f'the source changed while it was stored: {cid}, then {again.cid}')
            self.objects.put(key(cid), tmp)
            return cid
        finally:
            tmp.unlink(missing_ok=True)

    def get_to(self, cid: Cid, writer: Writer, *, max_bytes: int, expected_length: int | None = None) -> int | None:
        with self.objects.open(key(cid)) as raw:
            if raw is None:
                return None
            try:
                with ZstdFile(raw, 'rb') as src:
                    got = stream(src, write_all(writer), max_bytes=max_bytes, expected_length=expected_length)
            except ZstdError as e:
                raise IntegrityError(f'{cid}: its object is no zstd frame: {e}') from e
        if got.cid.digest != cid.digest:
            raise IntegrityError(f'{cid}: its bytes hash to {got.cid}')
        return got.size
