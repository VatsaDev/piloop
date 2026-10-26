"""Files as chunks and a manifest (``piloop.store.chunks``): cuts at line ends that appends never move."""

import random
import threading
from pathlib import Path

import pytest
from covalence import Cid, Value

from piloop.plugins.cas.disk import DiskCas
from piloop.store.chunks import AVERAGE_CHUNK, MAX_CHUNK, MIN_CHUNK, Chunked, cuts, read
from piloop.store.tree import RAW, Missing
from tests.conftest import disk


def lines(n: int, seed: int = 7, sizes: tuple[int, ...] = (40, 400, 4000, 40_000)) -> bytes:
    """``n`` JSONL lines of mixed lengths, as a session's records are: short messages and long tool results."""
    rng = random.Random(seed)
    return b''.join(b'{"x":"' + rng.randbytes(rng.choice(sizes) // 2).hex().encode() + b'"}\n' for _ in range(n))


def test_a_cut_is_a_line_end_within_the_bounds_and_appends_never_move_one() -> None:
    data = lines(400)
    ends = list(cuts(data))
    for start, end in zip([0, *ends], ends, strict=False):
        last_line = data.rfind(b'\n', start, end - 1) + 1 or start
        assert data[end - 1 : end] == b'\n', 'each chunk is whole lines'
        assert end - start >= MIN_CHUNK and last_line - start < MAX_CHUNK, 'cut at the first line end past the max'
    assert list(cuts(data + lines(50, seed=8)))[: len(ends)] == ends, 'a cut depends only on the bytes before it'
    assert list(cuts(data + b'{"cut short')) == ends, 'a line without its end closes no chunk'
    long = b'"' + b'y' * (2 * MAX_CHUNK) + b'"\n'
    assert list(cuts(long + b'{}\n')) == [len(long)], 'a line past the maximum is a chunk of its own'


def test_the_average_chunk_is_near_its_target() -> None:
    data = lines(3000, seed=3, sizes=(40, 400, 4000))
    size = len(data) / len(list(cuts(data)))
    assert AVERAGE_CHUNK / 2 < size < AVERAGE_CHUNK * 2, size


def test_read_joins_the_parts_of_a_chunked_file_nested_or_not(tmp_path: Path) -> None:
    cas = disk(tmp_path)
    a, b, c = cas.put(b'{"a":1}\n'), cas.put(b'{"b":2}\n'), cas.put(b'{"c":3}\n')
    group = Chunked(chunks=(a, b)).put(cas)
    assert read(cas, Chunked(chunks=(group, c)).put(cas)) == b'{"a":1}\n{"b":2}\n{"c":3}\n', 'a part can be a group'
    assert read(cas, a) == b'{"a":1}\n', 'a blob reads as stored'
    assert read(cas, Cid.of(b'never put')) is None
    gone = Chunked(chunks=(a, Cid.of(b'never put either'))).put(cas)
    with pytest.raises(Missing, match=f'the part {Cid.of(b"never put either")} of {gone}'):
        read(cas, gone)
    tree = Value({'entries': []})
    cas.put(tree.encode())
    with pytest.raises(ValueError, match='chunks'):
        read(cas, tree.cid())


def test_read_fetches_the_parts_of_a_manifest_at_once(tmp_path: Path) -> None:
    both = threading.Barrier(2, timeout=10)

    class Slow(DiskCas):
        """A CAS whose gets of the two chunks each wait for the other: one at a time, the barrier breaks."""

        def get(self, cid: Cid) -> bytes | None:
            if cid.codec == RAW:
                both.wait()
            return super().get(cid)

    cas = Slow(tmp_path / 'store')
    file = Chunked(chunks=(cas.put(b'{"a":1}\n'), cas.put(b'{"b":2}\n'))).put(cas)
    assert read(cas, file) == b'{"a":1}\n{"b":2}\n'
