"""``disk``: the default CAS, a directory of the local disk, ``<PILOOP_HOME>/store`` unless ``path`` says otherwise.

Its layout is ``blake3/<digest>/bytes.zst`` (``piloop.plugins.cas.kit.objects``), with temporary files in ``tmp``. A
directory that a ``cached`` store filled is refused: its evicted blobs are only on the remote that it names.
"""

import os
from collections.abc import Iterator
from contextlib import suppress
from pathlib import Path

from covalence import Cid

from piloop.plugins.cas.contract import Cas, Held, LocalCas, LocalProvider
from piloop.plugins.cas.kit.objects import BYTES, Dir, ObjectsCas, key, named
from piloop.settings import STORE_DIR

BOUND = 'remote.url'
"""In a ``cached`` store's directory: the remote that its blobs' ``remote`` markers mean."""
TMP = 'tmp'


class BoundElsewhere(ValueError):
    """The directory's blobs were uploaded to one remote, and it is opened over another one, or over none."""


class DiskCas(ObjectsCas):
    """The store of a directory, which can be a local tier: it lists and drops the blobs that it holds."""

    def __init__(self, root: Path) -> None:
        super().__init__(Dir(root), root / TMP)
        self.root = root

    def held(self) -> Iterator[Held]:
        with suppress(FileNotFoundError), os.scandir(self.root / 'blake3') as it:
            for entry in it:
                try:
                    cid = named(entry.name)
                    stat = os.stat(os.path.join(entry.path, BYTES))
                except ValueError, FileNotFoundError:  # not a blob's directory, or its bytes are gone
                    continue
                yield Held(cid, stat.st_size, stat.st_mtime)

    def release(self, cid: Cid) -> int:
        path = self.root / key(cid)
        try:
            size = path.stat().st_size
            path.unlink()
        except FileNotFoundError:
            return 0
        return size


class Disk(LocalProvider):
    """``[plugins.cas] provider = "disk"``: the blobs in a directory."""

    path: Path | None = None
    """The directory. Unset: ``<PILOOP_HOME>/store``."""

    def root(self, home: Path) -> Path:
        return self.path if self.path is not None else home / STORE_DIR

    def open(self, home: Path) -> Cas:
        root = self.root(home)
        if (bound := root / BOUND).is_file():
            raise BoundElsewhere(
                f'{root} is a cache over {bound.read_text()}, which holds the blobs evicted from it: select'
                ' [plugins.cas] provider = "cached" with that remote'
            )
        return DiskCas(root)

    def tier(self, home: Path) -> LocalCas:
        return DiskCas(self.root(home))

    def where(self, home: Path) -> str:
        return f'file://{self.root(home)}'
