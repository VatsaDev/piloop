import io
import os
import shutil
import time
import tracemalloc
from datetime import timedelta
from pathlib import Path

import pytest
from blake3 import blake3

from piloop import store as store_module
from piloop.store import (
    BOUND,
    Blob,
    BlobCorrupt,
    BlobMissing,
    Changed,
    Entry,
    Manifest,
    RemoteChanged,
    Store,
    address,
    hash_file,
    key,
)


class DirRemote:
    """A remote that is a directory: what ``S3`` does, minus the network."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.url = f'file://{root}'
        self.uploads: list[str] = []

    def upload(self, key: str, path: Path) -> None:
        self.uploads.append(key)
        target = self.root / key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(path.read_bytes())

    def download(self, key: str, path: Path) -> bool:
        source = self.root / key
        if not source.is_file():
            return False
        path.write_bytes(source.read_bytes())
        return True

    def has(self, key: str) -> bool:
        return (self.root / key).is_file()

    def delete(self, key: str) -> None:
        (self.root / key).unlink(missing_ok=True)


def blob_of(data: bytes) -> str:
    return address(blake3(data).digest())


def test_a_blob_is_addressed_by_the_base32_blake3_of_its_content_and_kept_compressed(tmp_path: Path) -> None:
    store = Store(tmp_path / 'store')
    data = b'{"metric": 1}\n' * 10_000
    blob = store.put(io.BytesIO(data))
    assert blob == Blob(blob_of(data), len(data))
    assert len(blob.hash) == 52 and blob.hash == blob.hash.lower() and '=' not in blob.hash, 'covalence spells it so'
    stored = tmp_path / 'store' / key(blob.hash)
    assert key(blob.hash) == f'blake3/{blob.hash}/bytes.zst'
    assert stored.stat().st_size < len(data) / 50, 'zstd at rest'
    out = io.BytesIO()
    assert store.get(blob.hash, out) == len(data) and out.getvalue() == data
    assert store.has(blob.hash) and store.missing([blob.hash, blob_of(b'other')]) == [blob_of(b'other')]
    assert store.usage().blobs == 1
    (tmp_path / 'store' / 'blake3' / 'half-written').mkdir()
    assert store.usage().blobs == 1, 'a directory without a blob is none'
    assert not list((tmp_path / 'store' / 'tmp').iterdir()), 'no temporary file left'


def test_equal_content_is_one_blob_and_a_known_file_is_not_compressed_again(tmp_path: Path) -> None:
    store = Store(tmp_path / 'store')
    a, b = tmp_path / 'a.py', tmp_path / 'b.py'
    a.write_text('print(1)\n')
    b.write_text('print(1)\n')
    first = store.put_path(a)
    stored = tmp_path / 'store' / key(first.hash)
    os.utime(stored, (1, 1))
    assert store.put_path(b) == first == hash_file(b)
    assert stored.stat().st_mtime == 1, 'untouched: the hash alone found it'
    assert store.put(io.BytesIO(b'print(1)\n')) == first
    assert store.usage().blobs == 1


def test_an_empty_file_is_a_blob_too(tmp_path: Path) -> None:
    store = Store(tmp_path / 'store')
    empty = tmp_path / 'empty'
    empty.touch()
    blob = store.put_path(empty)
    assert blob.size == 0 and blob.hash == blob_of(b'')
    target = tmp_path / 'out' / 'empty'
    assert store.get_path(blob.hash, target, 0o600) == 0
    assert target.read_bytes() == b'' and target.stat().st_mode & 0o777 == 0o600


def test_a_file_that_changes_while_it_is_stored_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = Store(tmp_path / 'store')
    path = tmp_path / 'log.txt'
    path.write_text('after the change\n')
    monkeypatch.setattr(store_module, 'hash_file', lambda _: Blob(blob_of(b'before the change\n'), 18))
    with pytest.raises(Changed, match='log.txt changed while it was stored'):
        store.put_path(path)
    assert store.usage().blobs == 0 and not list((tmp_path / 'store' / 'tmp').iterdir())


def test_a_multi_gigabyte_file_streams_through_put_and_get_in_bounded_memory(tmp_path: Path) -> None:
    store = Store(tmp_path / 'store')
    big = tmp_path / 'big.npy'
    size = 3 * 1024**3
    with big.open('wb') as f:  # sparse: no disk, but every byte is read
        f.truncate(size - 4)
        f.seek(size - 4)
        f.write(b'tail')

    class Count:
        written = 0

        def write(self, data: bytes, /) -> int:
            self.written += len(data)
            return len(data)

    tracemalloc.start()
    try:
        blob = store.put_path(big)
        sink = Count()
        assert store.get(blob.hash, sink) == size == sink.written
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert blob.size == size
    assert peak < 64 * 1024**2, f'peak {peak / 1024**2:.0f} MiB: the file was held in memory'


def test_a_blob_whose_bytes_rotted_reads_as_corrupt(tmp_path: Path) -> None:
    store = Store(tmp_path / 'store')
    blob = store.put(io.BytesIO(b'right'))
    other = store.put(io.BytesIO(b'wrong'))
    stored = tmp_path / 'store' / key(blob.hash)
    stored.write_bytes((tmp_path / 'store' / key(other.hash)).read_bytes())
    with pytest.raises(BlobCorrupt):
        store.get(blob.hash, io.BytesIO())
    target = tmp_path / 'out.txt'
    with pytest.raises(BlobCorrupt):
        store.get_path(blob.hash, target)
    assert not target.exists() and not list(tmp_path.glob('.out.txt.*')), 'nothing written in place'
    assert store.check(blob.hash) == 'corrupt' and store.check(other.hash) == 'ok'
    stored.write_bytes(b'not zstd at all')
    assert store.check(blob.hash) == 'corrupt'
    assert store.check(blob_of(b'never stored')) == 'missing'
    with pytest.raises(BlobMissing):
        store.get(blob_of(b'never stored'), io.BytesIO())


def test_a_manifest_is_a_blob_naming_a_tree(tmp_path: Path) -> None:
    store = Store(tmp_path / 'store')
    manifest = Manifest((Entry('b/x.npy', 0o644, 3, blob_of(b'abc')), Entry('a/link', 0o777, link='../b/x.npy')))
    blob = store.put_manifest(manifest)
    again = store.manifest(blob.hash)
    assert again.entries == tuple(sorted(manifest.entries, key=lambda e: e.path)), 'sorted by path'
    assert again.blobs == {blob_of(b'abc')}
    assert store.put_manifest(Manifest(tuple(reversed(manifest.entries)))) == blob, 'one tree, one address'
    with pytest.raises(ValueError, match='manifest version 2, expected 1'):
        Manifest.from_bytes(b'{"version": 2, "entries": []}')


REAL_FREE = Store.free
"""Captured before ``conftest.plenty_of_disk`` replaces it for every test."""


def test_free_space_is_measured_where_the_store_is_or_will_be(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    asked: list[Path] = []

    def usage(path: Path) -> shutil._ntuple_diskusage:
        asked.append(path)
        return shutil._ntuple_diskusage(100, 40, 60)

    monkeypatch.setattr(store_module.shutil, 'disk_usage', usage)
    store = Store(tmp_path / 'not' / 'yet')
    assert REAL_FREE(store) == 60
    (tmp_path / 'not' / 'yet').mkdir(parents=True)
    REAL_FREE(store)
    assert asked == [tmp_path, tmp_path / 'not' / 'yet']


def test_eviction_needs_a_remote(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match='eviction needs a remote'):
        Store(tmp_path, evict_after=timedelta(days=1))
    store = Store(tmp_path / 'store')
    store.put(io.BytesIO(b'x'))
    assert store.evict() == store_module.Evicted(), 'no remote: nothing is uploaded or dropped'
    assert store.usage() == store_module.Usage(blobs=1, local=1, local_bytes=store.usage().local_bytes, remote=0)


def test_a_mirror_uploads_each_blob_once_and_evicts_by_age_then_by_size(tmp_path: Path) -> None:
    remote = DirRemote(tmp_path / 'remote')
    store = Store(tmp_path / 'store', remote, evict_after=timedelta(days=7), max_local_bytes=60)
    blobs = [store.put(io.BytesIO(os.urandom(40))) for _ in range(3)]
    store.put(io.BytesIO(b'x'))
    store.put(io.BytesIO(b'x'))
    assert len(remote.uploads) == 4 and all(remote.has(key(b.hash)) for b in blobs)
    old, older, new = (tmp_path / 'store' / key(b.hash) for b in blobs)

    def days_ago(n: int) -> tuple[float, float]:
        return time.time() - n * 86400, time.time() - n * 86400

    os.utime(older, days_ago(9))
    os.utime(old, days_ago(1))
    done = store.evict()
    assert (done.uploaded, done.evicted) == (0, 2), 'the 9-day-old one by age, the 1-day-old one by size'
    assert not older.exists() and not old.exists() and new.exists()
    usage = store.usage()
    assert (usage.blobs, usage.local, usage.remote) == (4, 2, 4) and usage.local_bytes <= 60
    assert all(store.has(b.hash) for b in blobs), 'evicted blobs are still held: by the remote'
    out = io.BytesIO()
    store.get(blobs[1].hash, out)
    assert out.getvalue() and older.exists(), 'a read fetches it back into the directory'
    assert store.check(blobs[0].hash) == 'ok', 'evicted: asked of the remote'
    remote.delete(key(blobs[0].hash))
    assert store.check(blobs[0].hash) == 'missing'
    with pytest.raises(BlobMissing):
        store.get(blobs[0].hash, io.BytesIO())


def test_blobs_stored_before_the_remote_was_configured_are_uploaded_by_evict(tmp_path: Path) -> None:
    local = Store(tmp_path / 'store')
    blob = local.put(io.BytesIO(b'early'))
    remote = DirRemote(tmp_path / 'remote')
    mirror = Store(tmp_path / 'store', remote)
    assert mirror.evict() == store_module.Evicted(uploaded=1)
    assert remote.uploads == [key(blob.hash)] and mirror.evict() == store_module.Evicted()
    again = local.put(io.BytesIO(b'again'))
    path = tmp_path / 'again.txt'
    path.write_bytes(b'again')
    assert mirror.put_path(path) == again and remote.uploads[-1] == key(again.hash), 'a put leaves it on the remote'
    assert mirror.put_path(path) == again and len(remote.uploads) == 2, 'once'


def test_staging_keeps_nothing_local_once_uploaded_and_reads_through_a_temporary_file(tmp_path: Path) -> None:
    remote = DirRemote(tmp_path / 'remote')
    early = Store(tmp_path / 'store').put(io.BytesIO(b'from before'))
    store = Store(tmp_path / 'store', remote, evict_after=timedelta(0))
    assert store.staging
    blob = store.put(io.BytesIO(b'staged'))
    assert not (tmp_path / 'store' / key(blob.hash)).exists() and store.has(blob.hash)
    size = (tmp_path / 'store' / key(early.hash)).stat().st_size
    assert store.evict() == store_module.Evicted(uploaded=1, evicted=1, freed=size)
    out = io.BytesIO()
    store.get(early.hash, out)
    assert out.getvalue() == b'from before'
    assert not (tmp_path / 'store' / key(early.hash)).exists(), 'read through, not cached'
    assert not list((tmp_path / 'store' / 'tmp').iterdir())
    assert store.usage() == store_module.Usage(blobs=2, local=0, local_bytes=0, remote=2)
    remote.delete(key(blob.hash))
    with pytest.raises(BlobMissing):
        store.get(blob.hash, io.BytesIO())
    assert not list((tmp_path / 'store' / 'tmp').iterdir())


def test_a_store_stays_bound_to_the_remote_it_uploaded_to(tmp_path: Path) -> None:
    """A new bucket in the settings left the old markers saying 'uploaded'."""
    root = tmp_path / 'store'
    Store(root, DirRemote(tmp_path / 'a'))  # nothing uploaded yet: not bound
    Store(root, DirRemote(tmp_path / 'b')).put(io.BytesIO(b'x'))
    assert (root / BOUND).read_text() == f'file://{tmp_path / "b"}'
    Store(root, DirRemote(tmp_path / 'b'))
    with pytest.raises(RemoteChanged, match=f'mirrors to file://{tmp_path / "b"}.*opened with file://{tmp_path / "a"}'):
        Store(root, DirRemote(tmp_path / 'a'))
    with pytest.raises(RemoteChanged, match='opened with no remote'):
        Store(root)


def test_a_fetched_blob_evicted_as_it_is_put_back_is_still_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An eviction between the put-back and the read raised FileNotFoundError."""
    store = Store(tmp_path / 'store', DirRemote(tmp_path / 'remote'), max_local_bytes=0)
    blob = store.put(io.BytesIO(b'archived'))
    store.evict()
    real = os.replace

    def evicted_at_once(src: Path, dst: Path) -> None:
        real(src, dst)
        Path(dst).unlink()  # the daemon's eviction, between the put-back and the read

    monkeypatch.setattr(store_module.os, 'replace', evicted_at_once)
    out = io.BytesIO()
    store.get(blob.hash, out)
    assert out.getvalue() == b'archived'
