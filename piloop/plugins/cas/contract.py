"""The CAS capability: the content-addressed store that archived files, manifests and job data live in.

A blob's address is a covalence ``Cid``: BLAKE3, hash only. The contract, ``Cas``, is a subset of covalence's Python
``Cas`` with the same names and signatures, so a ``covalence.Cas`` is a provider's store as it is. A missing blob reads
as None. A blob whose bytes do not match its CID raises covalence's ``IntegrityError``, and a failure of the storage
underneath its ``StoreError``.

Host tier: a provider does I/O. ``[plugins.cas] provider`` in ``config.toml`` selects one (``disk`` by default), and
the other keys of the table are its parameters: a provider is its parameters, a ``CasProvider`` whose ``open`` returns
the ``Cas``. A store that keeps blobs in tiers also implements ``Upkeep``.
"""

from abc import ABC, abstractmethod
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from enum import StrEnum
from os import PathLike
from pathlib import Path
from typing import Annotated, BinaryIO, Protocol, runtime_checkable

from covalence import Cid, Hash, IntegrityError
from pydantic import PlainSerializer, PlainValidator, WithJsonSchema

from piloop.plugins.host import Capability, Choice, Parameters, Scope

BLAKE3 = Hash('blake3')
UNBOUNDED = (1 << 64) - 1
"""``max_bytes`` for a blob of any size."""


class Writer(Protocol):
    """A binary stream to write to: what ``Cas.get_to`` takes (``_typeshed.SupportsWrite[bytes]``)."""

    def write(self, data: bytes, /) -> int:
        """Write ``data``; how many bytes were written."""
        ...


class Cas(Protocol):
    """A content-addressed store: the methods of covalence's ``Cas`` that piloop calls, with their signatures."""

    def has(self, cid: Cid) -> bool:
        """Whether the store holds the blob, without checking its bytes."""
        ...

    def missing(self, cids: Sequence[Cid]) -> list[Cid]:
        """Those of ``cids`` that the store lacks, in order."""
        ...

    def put(self, data: bytes) -> Cid:
        """Store ``data`` and return its raw BLAKE3 CID."""
        ...

    def get(self, cid: Cid) -> bytes | None:
        """The bytes under ``cid``, checked, or None if missing."""
        ...

    def put_file(self, path: str | PathLike[str]) -> Cid:
        """Store the file at ``path``, read twice (to hash, to store), and return its raw BLAKE3 CID.

        A file that changed in between raises ``IntegrityError`` and stores nothing.
        """
        ...

    def get_file(self, cid: Cid, path: str | PathLike[str]) -> int | None:
        """Write the blob to ``path``, checked in a temporary file beside it that then replaces it; its length.

        None if missing, and ``path`` stays as it was.
        """
        ...

    def put_fileobj(self, file: BinaryIO, *, max_bytes: int, expected_length: int | None = None) -> Cid:
        """Store a seekable file object, read twice from its start, and return its raw BLAKE3 CID.

        A source longer than ``max_bytes``, or not ``expected_length`` long, raises ``ValueError``; one that changed
        between the reads raises ``IntegrityError``. Either way nothing is stored.
        """
        ...

    def get_to(self, cid: Cid, writer: Writer, *, max_bytes: int, expected_length: int | None = None) -> int | None:
        """Write the blob to ``writer`` and return its length, or None if missing.

        ``IntegrityError`` after the write when the bytes do not match ``cid``: the caller discards what it wrote. A
        length outside ``max_bytes`` or other than ``expected_length`` raises ``ValueError``.
        """
        ...


def _address(value: object) -> Cid:
    cid = Cid(value) if isinstance(value, str) else value
    if not isinstance(cid, Cid):
        raise ValueError(f'an address is a CID or its text, not {type(value).__name__}')
    if cid.hash != BLAKE3:
        raise ValueError(f'{cid} is hashed with {cid.hash.name}: the CAS addresses by BLAKE3')
    return cid


type BlobAddress = Annotated[
    Cid, PlainValidator(_address), PlainSerializer(str, when_used='json'), WithJsonSchema({'type': 'string'})
]
"""A blob's address: a BLAKE3 CID, hash only. JSON holds its text."""


class Integrity(StrEnum):
    """What ``integrity`` found of a blob."""

    OK = 'ok'
    MISSING = 'missing'
    CORRUPT = 'corrupt'


@dataclass(frozen=True)
class TierUsage:
    """A tiered store: the blobs it knows, how many have their bytes here and how much those take, how many remote."""

    blobs: int = 0
    local: int = 0
    local_bytes: int = 0
    remote: int = 0
    where: str = ''
    """The remote tier, in one spelling: ``s3://bucket/prefix``."""


@dataclass(frozen=True)
class Flushed:
    """What ``Upkeep.flush`` did."""

    uploaded: int = 0
    evicted: int = 0
    freed: int = 0
    """Bytes dropped from the local tier."""


@runtime_checkable
class Upkeep(Protocol):
    """What a store of a local tier over a remote one also does. Optional: ``piloop store`` and the daemon check for it.

    It is not part of ``Cas``, because covalence's ``Cas`` has none of it.
    """

    def usage(self) -> TierUsage:
        """Count the blobs of the local tier: one scan of it."""
        ...

    def flush(self, *, evict: bool) -> Flushed:
        """Upload what the remote tier lacks; with ``evict``, then drop the local copies that the policy says to."""
        ...

    def integrity(self, cid: Cid) -> Integrity:
        """Read and hash a local copy; ask the remote tier about an evicted blob, without fetching it."""
        ...


@dataclass(frozen=True)
class Held:
    """A blob whose bytes a local tier holds: what an eviction policy sees (covalence's ``EvictionCandidate``)."""

    cid: Cid
    stored_size: int
    """Bytes on the tier, compressed or not."""
    modified_at: float
    """When the bytes were put or fetched, in seconds since the epoch."""


@runtime_checkable
class Evictable(Protocol):
    """What a store over a local tier needs of it to evict: every blob held, and dropping one."""

    def held(self) -> Iterator[Held]:
        """Every blob whose bytes the tier holds: one scan of it."""
        ...

    def release(self, cid: Cid) -> int:
        """Drop the tier's bytes of ``cid``; the bytes freed, 0 when it held none."""
        ...


class LocalCas(Cas, Evictable, Protocol):
    """A store that can be the local tier of another: a ``Cas`` that is ``Evictable``."""


class _Sink:
    def write(self, data: bytes, /) -> int:
        return len(data)


def integrity(cas: Cas, cid: Cid) -> Integrity:
    """Whether ``cas`` holds the blob intact: ``Upkeep.integrity``, else a read of the whole blob."""
    if isinstance(cas, Upkeep):
        return cas.integrity(cid)
    try:
        found = cas.get_to(cid, _Sink(), max_bytes=UNBOUNDED)
    except IntegrityError:
        return Integrity.CORRUPT
    return Integrity.MISSING if found is None else Integrity.OK


class CasProvider(Parameters, ABC):
    """A CAS provider: its parameters, the keys of ``[plugins.cas]`` besides ``provider``, and how to open its store."""

    @abstractmethod
    def open(self, home: Path) -> Cas:
        """The store that these parameters describe, for the PILOOP_HOME ``home``."""

    @abstractmethod
    def where(self, home: Path) -> str:
        """Where its blobs are, in one spelling (``s3://bucket/prefix``): a cache over it records which store it fills."""


class LocalProvider(CasProvider):
    """A provider whose store can be the local tier of another (``cached``'s ``local``)."""

    @abstractmethod
    def tier(self, home: Path) -> LocalCas:
        """The store as a local tier. The store over it owns what it writes beside the blobs, which ``open`` refuses."""

    @abstractmethod
    def root(self, home: Path) -> Path:
        """The directory where a store over this tier keeps its own files."""


CAS = Capability('cas', CasProvider, choice=Choice(Scope.HOME, default='disk'), group='piloop.cas', noun='CAS provider')
"""Every CAS provider by name. ``disk``, built in and the default, keeps the blobs in ``<PILOOP_HOME>/store``."""
