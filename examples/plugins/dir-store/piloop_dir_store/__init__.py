"""``dir``: a piloop storage backend that mirrors the artifact store to a directory, such as a mounted share."""

import shutil
from pathlib import Path
from typing import Literal

from piloop.store import StorageBackend, StorageBackendConfig


class DirConfig(StorageBackendConfig):
    """``[storage.backend]`` with ``kind = "dir"``: a directory that the objects go under."""

    kind: Literal['dir'] = 'dir'
    root: Path
    """The directory. It is created when the first object is stored."""

    def open(self) -> StorageBackend:
        return Dir(self.root)


class Dir:
    """A remote of whole objects by key, as files under ``root``. It never lists, deletes or changes an object."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.url = f'file://{root}'.rstrip('/')

    def upload(self, key: str, path: Path) -> None:
        target = self.root / key
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, target)

    def download(self, key: str, path: Path) -> bool:
        if not self.has(key):
            return False
        shutil.copyfile(self.root / key, path)
        return True

    def has(self, key: str) -> bool:
        return (self.root / key).is_file()
