"""``disk``: compressed objects in a directory, by the BLAKE3 digest of their content (``ObjectsCas`` over ``Dir``)."""

import io
import os
import tracemalloc
from pathlib import Path

import pytest
from covalence import Cid, IntegrityError
from pydantic import TypeAdapter, ValidationError

from piloop.plugins.cas.contract import UNBOUNDED, BlobAddress, Held, Integrity, integrity
from piloop.plugins.cas.disk import BOUND, BoundElsewhere, Disk
from piloop.plugins.cas.kit import objects
from piloop.plugins.cas.kit.objects import ObjectsCas, key, name, named


def disk(tmp_path: Path) -> ObjectsCas:
    found = Disk(path=tmp_path / 'store').open(tmp_path)
    assert isinstance(found, ObjectsCas)
    return found


def test_a_blob_is_addressed_by_the_raw_blake3_cid_of_its_content_and_kept_compressed(tmp_path: Path) -> None:
    cas = disk(tmp_path)
    data = b'{"metric": 1}\n' * 10_000
    cid = cas.put(data)
    assert cid == Cid.of(data) and cid.codec.name == 'raw'
    digest = name(cid)
    assert len(digest) == 52 and digest == digest.lower() and '=' not in digest, 'covalence spells it so'
    assert named(digest) == cid and key(cid) == f'blake3/{digest}/bytes.zst'
    stored = tmp_path / 'store' / key(cid)
    assert stored.stat().st_size < len(data) / 50, 'zstd at rest'
    assert cas.get(cid) == data and cas.has(cid) and cas.missing([cid, Cid.of(b'other')]) == [Cid.of(b'other')]
    assert not list((tmp_path / 'store' / 'tmp').iterdir()), 'no temporary file left'


def test_equal_content_is_one_blob_and_a_known_file_is_not_compressed_again(tmp_path: Path) -> None:
    cas = disk(tmp_path)
    a, b = tmp_path / 'a.py', tmp_path / 'b.py'
    a.write_text('print(1)\n')
    b.write_text('print(1)\n')
    first = cas.put_file(a)
    stored = tmp_path / 'store' / key(first)
    os.utime(stored, (1, 1))
    assert cas.put_file(b) == first == cas.put(b'print(1)\n')
    assert stored.stat().st_mtime == 1, 'untouched: the hash alone found it'


def test_a_source_that_changes_while_it_is_stored_is_refused(tmp_path: Path) -> None:
    class Changing(io.BytesIO):
        """Other bytes from the second read on: the file an agent writes while it is stored."""

        reads = 0

        def seek(self, offset: int, whence: int = 0, /) -> int:
            self.reads += 1
            if self.reads == 2:
                self.write(b'after')
            return super().seek(offset, whence)

    cas = disk(tmp_path)
    with pytest.raises(IntegrityError, match='changed while it was stored'):
        cas.put_fileobj(Changing(b'before'), max_bytes=UNBOUNDED)
    assert not (tmp_path / 'store' / 'blake3').exists() and not list((tmp_path / 'store' / 'tmp').iterdir())


def test_a_multi_gigabyte_file_streams_through_put_and_get_in_bounded_memory(tmp_path: Path) -> None:
    cas = disk(tmp_path)
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
        cid = cas.put_file(big)
        sink = Count()
        assert cas.get_to(cid, sink, max_bytes=UNBOUNDED) == size == sink.written
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert peak < 64 * 1024**2, f'peak {peak / 1024**2:.0f} MiB: the file was held in memory'


def test_a_writer_that_takes_part_of_each_piece_gets_all_of_it(tmp_path: Path) -> None:
    cas = disk(tmp_path)
    data = os.urandom(3000)
    cid = cas.put(data)

    class Partial:
        out = b''

        def write(self, data: bytes, /) -> int:
            self.out += data[:7]
            return min(7, len(data))

    partial = Partial()
    assert cas.get_to(cid, partial, max_bytes=UNBOUNDED) == len(data) and partial.out == data


def test_a_blob_whose_bytes_rotted_reads_as_corrupt(tmp_path: Path) -> None:
    cas = disk(tmp_path)
    right, wrong = cas.put(b'right'), cas.put(b'wrong')
    stored = tmp_path / 'store' / key(right)
    stored.write_bytes((tmp_path / 'store' / key(wrong)).read_bytes())
    with pytest.raises(IntegrityError, match='its bytes hash to'):
        cas.get(right)
    target = tmp_path / 'out.txt'
    with pytest.raises(IntegrityError):
        cas.get_file(right, target)
    assert not target.exists() and not list(tmp_path.glob('.out.txt.*')), 'nothing written in place'
    assert integrity(cas, right) is Integrity.CORRUPT and integrity(cas, wrong) is Integrity.OK
    stored.write_bytes(b'not zstd at all')
    with pytest.raises(IntegrityError, match='no zstd frame'):
        cas.get(right)
    assert integrity(cas, right) is Integrity.CORRUPT and integrity(cas, Cid.of(b'never stored')) is Integrity.MISSING


def test_a_directory_that_a_cache_filled_is_refused_as_a_disk(tmp_path: Path) -> None:
    (tmp_path / 'store').mkdir()
    (tmp_path / 'store' / BOUND).write_text('s3://b/p')
    with pytest.raises(BoundElsewhere, match='is a cache over s3://b/p.*provider = "cached"'):
        Disk().open(tmp_path)


def test_a_stream_is_hashed_in_one_pass_from_its_start() -> None:
    once = objects._Once(io.BytesIO(b'abc'), lambda _: None)
    assert once.seek(0) == 0 and once.read(2) == b'ab'
    with pytest.raises(io.UnsupportedOperation, match='read once, from its start'):
        once.seek(0)


def test_an_address_is_a_blake3_cid_and_json_holds_its_text() -> None:
    address = TypeAdapter(BlobAddress)
    assert address.dump_json(Cid.of(b'x')).decode() == f'"{Cid.of(b"x")}"'
    assert address.validate_json(f'"{Cid.of(b"x")}"') == Cid.of(b'x')
    with pytest.raises(ValidationError, match='hashed with sha256'):
        address.validate_python(Cid.of(b'x', 'sha256'))
    with pytest.raises(ValidationError, match='not int'):
        address.validate_python(1)
    with pytest.raises(ValueError, match='hashed with sha256'):
        name(Cid.of(b'x', 'sha256'))
    for text in ('not base32!', 'aaaa'):
        with pytest.raises(ValueError, match='no base32 BLAKE3 digest'):
            named(text)


def test_a_disk_as_a_local_tier_lists_and_drops_the_blobs_it_holds(tmp_path: Path) -> None:
    tier = Disk(path=tmp_path / 'store').tier(tmp_path)
    cid = tier.put(b'held')
    (tmp_path / 'store' / 'blake3' / 'not-a-digest').mkdir()
    (tmp_path / 'store' / 'blake3' / name(Cid.of(b'evicted'))).mkdir()
    [held] = tier.held()
    stored = (tmp_path / 'store' / key(cid)).stat()
    assert held == Held(cid, stored.st_size, stored.st_mtime)
    assert tier.release(cid) == stored.st_size and not tier.has(cid) and list(tier.held()) == []
    assert tier.release(cid) == 0, 'already gone'
