"""``cached``: a local directory over a remote CAS, with markers, eviction by age then size, and a staging mode."""

import os
import threading
import time
from datetime import timedelta
from pathlib import Path
from typing import BinaryIO

import pytest
from covalence import Cas as Covalence, Cid, IntegrityError

from piloop.plugins.cas.cached import REMOTE, CachedCas
from piloop.plugins.cas.contract import Flushed, Integrity, TierUsage, Writer
from piloop.plugins.cas.disk import BOUND, BoundElsewhere, Disk, DiskCas
from piloop.plugins.cas.kit.objects import ObjectsCas, StreamCas, key, name

URL = 'memory://remote'


class Counted(StreamCas):
    """A remote that counts its puts, over covalence's store in memory."""

    def __init__(self) -> None:
        self.cas = Covalence.memory()
        self.puts: list[Cid] = []

    def has(self, cid: Cid) -> bool:
        return self.cas.has(cid)

    def put_fileobj(self, file: BinaryIO, *, max_bytes: int, expected_length: int | None = None) -> Cid:
        cid = self.cas.put_fileobj(file, max_bytes=max_bytes, expected_length=expected_length)
        self.puts.append(cid)
        return cid

    def get_to(self, cid: Cid, writer: Writer, *, max_bytes: int, expected_length: int | None = None) -> int | None:
        return self.cas.get_to(cid, writer, max_bytes=max_bytes, expected_length=expected_length)


def cached(
    tmp_path: Path, remote: Counted, evict_after: timedelta | None = None, max_local_bytes: int | None = None
) -> CachedCas:
    local = DiskCas(tmp_path / 'store')
    return CachedCas(local, tmp_path / 'store', remote, URL, evict_after, max_local_bytes)


def local_bytes(tmp_path: Path, cid: Cid) -> Path:
    return tmp_path / 'store' / key(cid)


def test_a_mirror_uploads_each_blob_once_and_evicts_by_age_then_by_size(tmp_path: Path) -> None:
    remote = Counted()
    cas = cached(tmp_path, remote, timedelta(days=7), 60)
    blobs = [cas.put(os.urandom(40)) for _ in range(3)]
    cas.put(b'x')
    cas.put(b'x')
    assert len(remote.puts) == 4 and all(remote.cas.has(c) for c in blobs)
    old, older, new = (local_bytes(tmp_path, c) for c in blobs)
    nine_days, one_day = time.time() - 9 * 86400, time.time() - 86400
    os.utime(older, (nine_days, nine_days))
    os.utime(old, (one_day, one_day))
    done = cas.flush(evict=True)
    assert (done.uploaded, done.evicted) == (0, 2), 'the 9-day-old one by age, the 1-day-old one by size'
    assert not older.exists() and not old.exists() and new.exists()
    usage = cas.usage()
    assert (usage.blobs, usage.local, usage.remote, usage.where) == (4, 2, 4, URL) and usage.local_bytes <= 60
    assert all(cas.has(c) for c in blobs), 'evicted blobs are still held: by the remote'
    assert cas.get(blobs[1]) is not None and older.exists(), 'a read fetches it back into the directory'
    assert cas.integrity(blobs[0]) is Integrity.OK, 'evicted: asked of the remote'
    assert cas.integrity(blobs[2]) is Integrity.OK and cas.integrity(Cid.of(b'never')) is Integrity.MISSING
    gone = cached(tmp_path, Counted())
    assert gone.integrity(blobs[0]) is Integrity.MISSING and gone.get(blobs[0]) is None, 'the remote lost it'


def test_blobs_stored_before_the_remote_was_set_are_uploaded_by_a_flush(tmp_path: Path) -> None:
    early = Disk(path=tmp_path / 'store').open(tmp_path).put(b'early')
    remote = Counted()
    cas = cached(tmp_path, remote)
    assert cas.flush(evict=False) == Flushed(uploaded=1) and remote.puts == [early]
    assert cas.flush(evict=True) == Flushed()
    assert (tmp_path / 'store' / 'blake3' / name(early) / REMOTE).is_file()


def test_staging_keeps_nothing_local_and_reads_through(tmp_path: Path) -> None:
    early = Disk(path=tmp_path / 'store').open(tmp_path).put(b'from before')
    remote = Counted()
    cas = cached(tmp_path, remote, evict_after=timedelta(0))
    assert cas.staging
    staged = cas.put(b'staged')
    assert not local_bytes(tmp_path, staged).exists() and cas.has(staged) and remote.cas.has(staged)
    size = local_bytes(tmp_path, early).stat().st_size
    assert cas.flush(evict=True) == Flushed(uploaded=1, evicted=1, freed=size)
    assert cas.get(early) == b'from before' and not local_bytes(tmp_path, early).exists(), 'read through, not cached'
    assert cas.usage() == TierUsage(blobs=2, local=0, local_bytes=0, remote=2, where=URL)


@pytest.mark.parametrize('evict_after', [None, timedelta(0)])
def test_a_blob_that_another_store_put_on_the_remote_is_found_read_and_marked(
    tmp_path: Path, evict_after: timedelta | None
) -> None:
    remote = Counted()
    server, worker = cached(tmp_path / 'server', remote, evict_after), cached(tmp_path / 'worker', remote, evict_after)
    put, never = worker.put(b'a log file'), Cid.of(b'never put')
    assert server.missing([put, never]) == [never] and server.has(put), 'asked of the remote, then marked'
    other = worker.put(b'a bundle')
    assert server.get(other) == b'a bundle' and server.has(other)
    assert server.integrity(worker.put(b'more')) is Integrity.OK and server.get(never) is None


def test_a_put_of_a_blob_the_remote_holds_does_not_bring_back_its_local_copy(tmp_path: Path) -> None:
    remote = Counted()
    cas = cached(tmp_path, remote, max_local_bytes=0)
    cid = cas.put(b'already remote')
    cas.flush(evict=True)
    assert cas.put(b'already remote') == cid and not local_bytes(tmp_path, cid).exists() and remote.puts == [cid]


def test_threads_putting_one_content_upload_it_once(tmp_path: Path) -> None:
    remote = Counted()
    cas = cached(tmp_path, remote, evict_after=timedelta(0))
    threads = [threading.Thread(target=cas.put, args=(b'same array',)) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert remote.puts == [Cid.of(b'same array')]


def test_a_store_stays_bound_to_the_remote_it_uploaded_to(tmp_path: Path) -> None:
    """A new bucket in the settings left the old markers saying 'uploaded'."""
    cached(tmp_path, Counted())  # nothing uploaded yet: not bound
    cached(tmp_path, Counted()).put(b'x')
    assert (tmp_path / 'store' / BOUND).read_text() == URL
    local = Disk(path=tmp_path / 'store').open
    with pytest.raises(BoundElsewhere, match=f'mirrors to {URL}.*opened over file://elsewhere'):
        CachedCas(DiskCas(tmp_path / 'store'), tmp_path / 'store', Counted(), 'file://elsewhere')
    with pytest.raises(BoundElsewhere, match=f'is a cache over {URL}'):
        local(tmp_path)


def test_a_fetched_blob_evicted_as_it_is_put_back_is_still_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An eviction between the put-back and the read raised FileNotFoundError."""
    cas = cached(tmp_path, Counted(), max_local_bytes=0)
    cid = cas.put(b'archived')
    cas.flush(evict=True)
    real = ObjectsCas.put_file

    def evicted_at_once(self: ObjectsCas, path: Path) -> Cid:
        put = real(self, path)
        local_bytes(tmp_path, put).unlink()  # the daemon's eviction, between the put-back and the read
        return put

    monkeypatch.setattr(ObjectsCas, 'put_file', evicted_at_once)
    assert cas.get(cid) == b'archived'


def test_a_local_copy_that_rotted_is_corrupt_and_a_lost_remote_is_missing(tmp_path: Path) -> None:
    cas = cached(tmp_path, Counted())
    right, wrong = cas.put(b'right'), cas.put(b'wrong')
    local_bytes(tmp_path, right).write_bytes(local_bytes(tmp_path, wrong).read_bytes())
    assert cas.integrity(right) is Integrity.CORRUPT
    with pytest.raises(IntegrityError):
        cas.get(right)


def test_a_flush_skips_what_is_not_a_blob_and_what_went_since_the_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    remote = Counted()
    early = DiskCas(tmp_path / 'store').put(b'early')
    (tmp_path / 'store' / 'blake3' / 'not-a-digest').mkdir()
    (tmp_path / 'store' / 'blake3' / name(Cid.of(b'half-written'))).mkdir()
    cas = cached(tmp_path, remote)
    assert cas.usage().blobs == 1
    monkeypatch.setattr(ObjectsCas, 'get_file', lambda self, cid, path: None)  # evicted by another process meanwhile
    cas.flush(evict=False)
    assert remote.puts == [] and not (tmp_path / 'store' / 'blake3' / name(early) / REMOTE).exists()


def test_a_local_copy_evicted_while_it_is_checked_is_asked_of_the_remote(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cas = cached(tmp_path, Counted())
    cid = cas.put(b'checked')
    monkeypatch.setattr(ObjectsCas, 'get_to', lambda self, cid, writer, **limits: None)
    assert cas.integrity(cid) is Integrity.OK


def test_a_cache_over_a_cache_asks_and_flushes_the_inner_one(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The outer cache uploads into the inner one, which moves it on; integrity asks the inner one, without a fetch."""
    farthest = Counted()
    inner = cached(tmp_path / 'inner', farthest, max_local_bytes=0)
    outer = CachedCas(DiskCas(tmp_path / 'outer'), tmp_path / 'outer', inner, 'file://inner', None, 0)
    cid = outer.put(b'through both')
    assert outer.flush(evict=True).evicted == 1
    assert not (tmp_path / 'outer' / key(cid)).exists() and not (tmp_path / 'inner' / 'store' / key(cid)).exists()
    assert farthest.puts == [cid] and outer.get(cid) == b'through both'
    asked: list[Cid] = []
    monkeypatch.setattr(inner, 'integrity', lambda cid: asked.append(cid) or Integrity.OK)
    outer.local.release(cid)
    assert outer.integrity(cid) is Integrity.OK and asked == [cid]
