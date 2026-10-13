"""Baseline: zlib at level 9. Workers edit this file."""

import zlib


def compress(data: bytes) -> bytes:
    """Return a compressed form of ``data``."""
    return zlib.compress(data, 9)


def decompress(blob: bytes) -> bytes:
    """Return the exact bytes that ``compress`` received."""
    return zlib.decompress(blob)
