"""The contract of a CAS: one check that every provider's store passes, piloop's, covalence's or a package's.

piloop's tests run it against each built-in provider and covalence's ``Cas.memory()``; a package runs it in its own
tests. ``check_cas`` drives a store through what piloop asks of it. Point it at an empty store: the contract has no
delete, so it leaves its blobs there. A broken rule raises ``ContractBroken``, which says which one.
"""

import io
import os
from collections.abc import Callable
from pathlib import Path

from covalence import Cid

from piloop.plugins.cas.contract import UNBOUNDED, Cas
from piloop.plugins.host import ContractBroken


def _require(ok: bool, rule: str) -> None:
    if not ok:
        raise ContractBroken(rule)


def _refuses(call: Callable[[], object], rule: str) -> None:
    try:
        call()
    except ValueError:
        return
    raise ContractBroken(rule)


def check_cas(cas: Cas, scratch: Path) -> None:
    """Integrity ``cas`` against the contract, with ``scratch`` as an empty local directory for files."""
    scratch.mkdir(parents=True, exist_ok=True)
    salt = os.urandom(16)  # a store that outlives one run, a bucket, has none of these blobs yet
    data = salt + b'{"metric": 1}\n' * 1000
    absent = Cid.of(salt + b'never stored')
    _require(not cas.has(absent), 'has() is False for a blob never stored')
    _require(cas.get(absent) is None, 'get() is None for a missing blob')
    _require(cas.get_to(absent, io.BytesIO(), max_bytes=UNBOUNDED) is None, 'get_to() is None for a missing blob')
    _require(cas.get_file(absent, scratch / 'absent') is None, 'get_file() is None for a missing blob')
    _require(not (scratch / 'absent').exists(), 'get_file() of a missing blob writes nothing')

    cid = cas.put(data)
    _require(cid == Cid.of(data), 'put() returns the raw BLAKE3 CID of the bytes')
    _require(cas.put(data) == cid, 'a second put() of the same bytes returns the same CID')
    _require(cas.has(cid) and cas.get(cid) == data, 'put() then get() gives the bytes back')
    _require(cas.missing([absent, cid, absent]) == [absent, absent], 'missing() keeps the order and the repeats')
    drisl = Cid.from_parts('drisl', 'blake3', cid.digest)
    _require(cas.has(drisl) and cas.get(drisl) == data, 'a CID of another codec with the same digest finds the bytes')

    empty = cas.put(b'')
    _require(empty == Cid.of(b'') and cas.get(empty) == b'', 'an empty blob is a blob too')

    big = salt + os.urandom(3 << 20)  # past one 1 MiB piece
    source = scratch / 'big'
    source.write_bytes(big)
    blob = cas.put_file(source)
    _require(blob == Cid.of(big), 'put_file() returns the CID of the file')
    target = scratch / 'out'
    target.write_bytes(b'what was there')
    _require(cas.get_file(blob, target) == len(big), 'get_file() returns the length')
    _require(target.read_bytes() == big, 'get_file() replaces the file with the blob')
    out = io.BytesIO()
    _require(cas.get_to(blob, out, max_bytes=len(big), expected_length=len(big)) == len(big), 'get_to() its length')
    _require(out.getvalue() == big, 'get_to() writes the bytes')
    _refuses(lambda: cas.get_to(blob, io.BytesIO(), max_bytes=len(big) - 1), 'get_to() refuses past max_bytes')
    _refuses(
        lambda: cas.get_to(blob, io.BytesIO(), max_bytes=UNBOUNDED, expected_length=1), 'get_to() checks the length'
    )

    other = salt + b'other bytes'
    _refuses(
        lambda: cas.put_fileobj(io.BytesIO(other), max_bytes=UNBOUNDED, expected_length=len(other) + 1),
        'put_fileobj() refuses a source of another length',
    )
    _refuses(lambda: cas.put_fileobj(io.BytesIO(other), max_bytes=1), 'put_fileobj() refuses past max_bytes')
    _require(not cas.has(Cid.of(other)), 'a refused put_fileobj() stores nothing')
    stored = cas.put_fileobj(io.BytesIO(other), max_bytes=len(other), expected_length=len(other))
    _require(stored == Cid.of(other) and cas.get(stored) == other, 'put_fileobj() stores the source')
