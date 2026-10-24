"""``cached``: a local tier over a remote CAS, as a cache that keeps every blob it holds on the remote too.

The local tier is any ``LocalProvider`` (``disk`` by default), the remote any provider. Each blob put is stored in both
and marked ``blake3/<digest>/remote`` under the local tier's root, beside its bytes on a ``disk``: the layout of
piloop 0.2, whose homes open in place. ``remote.url`` there names the remote that the markers mean. ``flush`` (``Upkeep``) uploads what is not
marked yet, then drops the local bytes of marked blobs by age and by the directory's size. A read of a dropped blob
fetches it back, and so does a read of a blob that another store put on the remote: a server and its workers share
one bucket, each through a store of its own. With ``evict_after_days = 0`` the local tier is a staging area: a put
goes to the remote alone, and a read streams from it.

Blobs move between the tiers through the ``Cas`` protocol, as content: each upload and each fetch decompresses and
compresses again, through a temporary file of the blob's full size. A flush uploaded a 512 MiB JSON blob (145 MiB
stored) in 14 s over a local ``disk`` remote, where a copy of its object takes 0.1 s. That cost goes with this module.

Retirement: deleted when covalence's ``Cas.cached`` ships with eviction, usage and this marker layout, and ``cached``
opens it instead.
"""

import os
import tempfile
import threading
import time
from collections.abc import Generator, Sequence
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Annotated, BinaryIO

from covalence import Cid
from pydantic import Field

from piloop.plugins.cas.contract import (
    CAS,
    Cas,
    CasProvider,
    Flushed,
    Held,
    Integrity,
    LocalCas,
    LocalProvider,
    TierUsage,
    Upkeep,
    Writer,
    integrity,
)
from piloop.plugins.cas.disk import BOUND, BoundElsewhere, Disk
from piloop.plugins.cas.kit.objects import StreamCas, name, named, stream, write_all
from piloop.settings import GB

REMOTE = 'remote'
"""An empty file beside a blob's bytes: the remote holds the blob. It stays when the bytes are evicted."""
LOCKS = 64
"""Stripes of the per-blob lock of a put: blobs, not threads, so a few threads rarely wait on each other."""


class Cached(CasProvider):
    """``[plugins.cas] provider = "cached"``: a local tier over ``remote``, evicted by age and by size."""

    remote: Annotated[CasProvider, CAS.entry]
    """The CAS that holds every blob: a table with its ``provider`` and parameters, ``{ provider = "s3", url = … }``."""
    local: Annotated[LocalProvider, CAS.entry] = Disk()
    """The local tier: a table of a provider that can be one, ``{ provider = "disk", path = … }``. Unset: ``disk``."""
    evict_after_days: float | None = Field(default=None, ge=0)
    """Drop the local copy of a blob once it is this old (since it was stored or fetched); 0 keeps no local copy, so
    every read fetches. Unset: by age never."""
    local_max_gb: float | None = Field(default=None, ge=0)
    """Drop the local copies of the oldest blobs until the local tier is at most this size."""

    def open(self, home: Path) -> Cas:
        return CachedCas(
            self.local.tier(home),
            self.local.root(home),
            self.remote.open(home),
            self.remote.where(home),
            evict_after=timedelta(days=self.evict_after_days) if self.evict_after_days is not None else None,
            max_local_bytes=int(self.local_max_gb * GB) if self.local_max_gb is not None else None,
        )

    def where(self, home: Path) -> str:
        return self.remote.where(home)


@dataclass
class CachedCas(StreamCas):
    """``local``, whose markers are under ``root``, over ``remote``, which ``url`` names."""

    local: LocalCas
    root: Path
    remote: Cas
    url: str
    evict_after: timedelta | None = None
    max_local_bytes: int | None = None
    _locks: list[threading.Lock] = field(
        default_factory=lambda: [threading.Lock() for _ in range(LOCKS)], init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        bound = self.root / BOUND
        if bound.is_file() and (was := bound.read_text()) != self.url:
            raise BoundElsewhere(
                f'{self.root} mirrors to {was}, and its blobs marked as uploaded are there, but it is opened over'
                f' {self.url}: point the remote back at {was}, or copy its objects to the new remote and then write'
                f' the new URL into {bound}'
            )

    @property
    def staging(self) -> bool:
        """No local copy is kept."""
        return self.evict_after == timedelta(0)

    def _marker(self, cid: Cid) -> Path:
        return self.root / 'blake3' / name(cid) / REMOTE

    def _mark(self, cid: Cid) -> None:
        if not (self.root / BOUND).is_file():
            self.root.mkdir(parents=True, exist_ok=True)
            (self.root / BOUND).write_text(self.url)
        marker = self._marker(cid)
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.touch()

    def has(self, cid: Cid) -> bool:
        """Here, or (as last confirmed) on the remote. No network."""
        return self.local.has(cid) or self._marker(cid).is_file()

    def missing(self, cids: Sequence[Cid]) -> list[Cid]:
        """Those that neither tier holds: one that this store does not know is asked of the remote, and marked."""
        unknown = [cid for cid in cids if not self.has(cid)]
        lacking = self.remote.missing(unknown) if unknown else []
        for cid in set(unknown) - set(lacking):
            self._mark(cid)
        return lacking

    def put_fileobj(self, file: BinaryIO, *, max_bytes: int, expected_length: int | None = None) -> Cid:
        cid = Cid.of_fileobj(file, max_bytes=max_bytes, expected_length=expected_length)
        with self._locks[cid.digest[0] % LOCKS]:  # threads that put one content upload it once
            if self.has(cid):
                return cid
            if not self.staging:
                self.local.put_fileobj(file, max_bytes=max_bytes, expected_length=expected_length)
            self.remote.put_fileobj(file, max_bytes=max_bytes, expected_length=expected_length)
            self._mark(cid)
        return cid

    def get_to(self, cid: Cid, writer: Writer, *, max_bytes: int, expected_length: int | None = None) -> int | None:
        if (size := self.local.get_to(cid, writer, max_bytes=max_bytes, expected_length=expected_length)) is not None:
            return size
        if self.staging:
            size = self.remote.get_to(cid, writer, max_bytes=max_bytes, expected_length=expected_length)
            if size is not None:
                self._mark(cid)
            return size
        with self._scratch() as fetched:
            if self.remote.get_file(cid, fetched) is None:
                return None
            self.local.put_file(fetched)
            self._mark(cid)
            # from the fetched file: an eviction may drop the local copy before it is read
            with fetched.open('rb') as src:
                return stream(src, write_all(writer), max_bytes=max_bytes, expected_length=expected_length).size

    def _upload(self, cid: Cid) -> None:
        """Upload a local blob, checked on its way through a temporary file, and mark it."""
        with self._scratch() as tmp:
            if self.local.get_file(cid, tmp) is not None:
                self.remote.put_file(tmp)
                self._mark(cid)

    @contextmanager
    def _scratch(self) -> Generator[Path]:
        """A temporary file under ``root``, gone after."""
        self.root.mkdir(parents=True, exist_ok=True)
        fd, path = tempfile.mkstemp(dir=self.root, prefix='.cached-')
        os.close(fd)
        try:
            yield Path(path)
        finally:
            Path(path).unlink(missing_ok=True)

    def integrity(self, cid: Cid) -> Integrity:
        if self.local.has(cid):
            found = integrity(self.local, cid)
            if found is not Integrity.MISSING:  # else evicted since
                return found
        if isinstance(self.remote, Upkeep):  # a store of tiers itself: it checks without fetching too
            return self.remote.integrity(cid)
        return Integrity.OK if self.remote.has(cid) else Integrity.MISSING

    def _marked(self) -> set[Cid]:
        """Every blob marked as on the remote: one scan of ``root``."""
        found: set[Cid] = set()
        with suppress(FileNotFoundError), os.scandir(self.root / 'blake3') as it:
            for entry in it:
                try:
                    cid = named(entry.name)
                except ValueError:  # not a blob's directory
                    continue
                if os.path.isfile(os.path.join(entry.path, REMOTE)):
                    found.add(cid)
        return found

    def usage(self) -> TierUsage:
        """The local tier's blobs and bytes, and the marked ones; a blob in both tiers is one blob."""
        held, marked = list(self.local.held()), self._marked()
        blobs = len({h.cid for h in held} | marked)
        return TierUsage(blobs, len(held), sum(h.stored_size for h in held), len(marked), self.url)

    def flush(self, *, evict: bool) -> Flushed:
        """Upload every blob the remote is not known to hold; with ``evict``, then drop local bytes as the policy says.

        Dropped: when staging, every uploaded blob; else every one older than ``evict_after`` (since it was put or
        fetched), then the oldest remaining until the local bytes are at most ``max_local_bytes``. A remote that is a
        store of tiers itself flushes after, so that what this one uploaded moves on.
        """
        held, marked = sorted(self.local.held(), key=lambda h: h.modified_at), self._marked()
        uploaded = 0
        for h in held:
            if h.cid not in marked:
                self._upload(h.cid)
                uploaded += 1
        done = Flushed(uploaded, *(self._evict(held) if evict else (0, 0)))
        if isinstance(self.remote, Upkeep):
            self.remote.flush(evict=evict)
        return done

    def _evict(self, held: list[Held]) -> tuple[int, int]:
        """Drop the oldest of ``held`` (oldest first) as the policy says; how many and their bytes."""
        total = sum(h.stored_size for h in held)
        cutoff = time.time() - self.evict_after.total_seconds() if self.evict_after is not None else None
        evicted = freed = 0
        for h in held:
            too_old = cutoff is not None and h.modified_at <= cutoff
            too_big = self.max_local_bytes is not None and total > self.max_local_bytes
            if not too_old and not too_big:
                break
            released = self.local.release(h.cid)
            total, evicted, freed = total - h.stored_size, evicted + 1, freed + released
        return evicted, freed
