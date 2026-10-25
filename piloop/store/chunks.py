"""Files in a CAS as chunks and a manifest: the snapshots of a growing file share all but their last chunks.

The cut points are content-defined, as FastCDC's are, with a minimum, an average and a maximum size, but only at line
ends: each chunk holds whole lines, and a cut depends only on the bytes before it. So a file that grows by appends
keeps every chunk it had except its last, and a snapshot stores only what is new. A manifest is a DASL value that
links its parts, stored as its DRISL encoding under its DRISL CID, as a tree's manifest is (``piloop.store.tree``). A
part is a chunk, or a chunked file of its own: a long file keeps its closed chunks in groups, so the manifest of each
snapshot stays small (a B-tree of two levels).
"""

import zlib
from collections.abc import Iterator

from covalence import Cid, Value
from pydantic import BaseModel, ConfigDict

from piloop.plugins.cas.contract import BlobAddress, Cas
from piloop.store.tree import RAW, Missing

MIN_CHUNK = 2 * 1024
"""No cut before a chunk holds this many bytes."""
AVERAGE_CHUNK = 8 * 1024
"""The size a chunk has on average, when lines are short next to it."""
MAX_CHUNK = 32 * 1024
"""A cut at the first line end past this many bytes. A longer line is a chunk of its own."""
GROUP = 32
"""How many closed chunks a group holds."""


class Chunked(BaseModel):
    """A file stored as parts: its bytes are its parts' bytes, joined in order. A part is a chunk or a chunked file."""

    model_config = ConfigDict(frozen=True, extra='forbid')

    chunks: tuple[BlobAddress, ...]

    def put(self, cas: Cas) -> Cid:
        """Store the manifest; its DRISL CID names the file. Its chunks must be in ``cas`` already."""
        value = Value({'chunks': list(self.chunks)})
        cas.put(value.encode())
        return value.cid()


def cuts(data: bytes) -> Iterator[int]:
    """The ends of the chunks of ``data`` that are closed: each a line end that the rule picks. The rest is open.

    After at least ``MIN_CHUNK`` bytes, a line of ``n`` bytes ends the chunk when its CRC-32 falls under ``n`` in
    ``AVERAGE_CHUNK``: on average one cut per ``AVERAGE_CHUNK`` bytes, whatever the lengths of the lines.
    """
    start = at = 0
    while (end := data.find(b'\n', at)) != -1:
        end += 1
        size, line = end - start, end - at
        if size >= MAX_CHUNK or (size >= MIN_CHUNK and zlib.crc32(data[at:end]) * AVERAGE_CHUNK < line << 32):
            yield end
            start = end
        at = end


def read(cas: Cas, cid: Cid) -> bytes | None:
    """The bytes of the file at ``cid``: a blob as stored, or a chunked file's parts joined. None if missing.

    Raises:
        Missing: a part of the file is not in the CAS.
        ValueError: ``cid`` names a value that is not a chunked file.
    """
    data = cas.get(cid)
    if data is None or cid.codec == RAW:
        return data
    parts: list[bytes] = []
    for part in Chunked.model_validate(Value.decode(data).to_native()).chunks:
        if (found := read(cas, part)) is None:
            raise Missing(f'the part {part} of {cid} is not in the CAS')
        parts.append(found)
    return b''.join(parts)
