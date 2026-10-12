"""Directory trees in and out of the artifact store: put a tree, remove what was put, materialise a manifest.

Generic, like ``piloop.store``: paths and files only. Regular files and symlinks are stored; empty directories and
special files (sockets, fifos) are not, and the latter stay where they are.
"""

import os
import shutil
import stat
import tempfile
import threading
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

from piloop.store import Changed, Entry, Manifest, Store, hash_file

WORKERS = 8
"""Files ``put_tree`` stores, and blobs ``materialise`` reads, at once: on a remote each is a request."""


class Cancelled(RuntimeError):
    """``put_tree`` was asked to stop; what it stored so far stays, nothing is removed."""


@dataclass
class Stored:
    """What ``put_tree`` stored: the manifest, and what each path looked like when it was read."""

    manifest: Manifest
    seen: dict[str, tuple[int, int]] = field(default_factory=dict)
    """Relative path → ``(st_size, st_mtime_ns)`` as stored: ``prune`` removes a path only if it still matches."""
    dirs: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    """Paths left out, and left in place: special files, files that cannot be read, files that changed while they were
    stored."""

    @property
    def size(self) -> int:
        return sum(e.size for e in self.manifest.entries)


def _signature(st: os.stat_result) -> tuple[int, int]:
    return st.st_size, st.st_mtime_ns


def put_tree(
    store: Store, root: Path, names: Iterable[str], stop: threading.Event | None = None, *, workers: int = WORKERS
) -> Stored:
    """Store every file and symlink under ``root/<name>`` for each of ``names`` (files or directories).

    The tree is walked first, then its files are put ``workers`` at a time: on a remote, each blob is its own upload,
    and one at a time spent most of a measured archiving waiting on S3. Raises ``Cancelled`` once ``stop`` is set,
    before the next file starts.
    """
    entries: list[Entry] = []
    files: list[tuple[str, os.stat_result]] = []
    stored = Stored(Manifest(()))

    def visit(rel: str) -> None:
        path = root / rel
        st = path.lstat()
        if stat.S_ISLNK(st.st_mode):
            entries.append(Entry(rel, stat.S_IMODE(st.st_mode), link=os.readlink(path)))
            stored.seen[rel] = _signature(st)
        elif stat.S_ISREG(st.st_mode):
            files.append((rel, st))
        elif stat.S_ISDIR(st.st_mode):
            try:
                children = sorted(os.listdir(path))
            except PermissionError:
                stored.skipped.append(rel)
                return
            stored.dirs.append(rel)
            for child in children:
                visit(f'{rel}/{child}')
        else:
            stored.skipped.append(rel)

    def put(rel: str, st: os.stat_result) -> Entry | None:
        if stop is not None and stop.is_set():
            raise Cancelled(rel)
        try:
            blob = store.put_path(root / rel)
        except Changed, PermissionError:
            return None
        return Entry(rel, stat.S_IMODE(st.st_mode), blob.size, blob.hash)

    for name in sorted(names):
        visit(name)
    pool = ThreadPoolExecutor(workers, thread_name_prefix='put-tree')
    try:
        for (rel, st), entry in zip(
            files, pool.map(put, [rel for rel, _ in files], [st for _, st in files]), strict=True
        ):
            if entry is None:
                stored.skipped.append(rel)
                continue
            entries.append(entry)
            stored.seen[rel] = _signature(st)
    finally:
        pool.shutdown(cancel_futures=True)
    stored.manifest = Manifest(tuple(entries))
    return stored


def prune(root: Path, stored: Stored) -> int:
    """Remove what ``stored`` holds, path by path, where it is unchanged since; then the directories left empty.

    A path written meanwhile stays, and so does its directory. Returns the paths removed.
    """
    removed = 0
    for rel, signature in stored.seen.items():
        path = root / rel
        try:
            unchanged = _signature(path.lstat()) == signature
        except FileNotFoundError:
            continue
        if unchanged:
            path.unlink()
            removed += 1
    for rel in sorted(stored.dirs, key=lambda d: d.count('/'), reverse=True):
        path = root / rel
        if path.is_dir() and not any(path.iterdir()):
            path.rmdir()
    return removed


@dataclass
class Materialised:
    """What ``materialise`` did: files and links written, already there as stored, and kept because they differ."""

    written: int = 0
    bytes: int = 0
    same: int = 0
    kept: list[str] = field(default_factory=list)


def materialise(
    store: Store,
    manifest: Manifest,
    root: Path,
    wanted: Callable[[Entry], bool] = lambda _: True,
    *,
    workers: int = WORKERS,
) -> Materialised:
    """Write the ``wanted`` entries of ``manifest`` under ``root``; a path that exists with other content is kept.

    Each file is written beside its target and renamed into place once its content checks against its hash. Blobs are
    read ``workers`` at a time, each once however many paths it has.
    """
    done = Materialised()
    missing: dict[str, list[Entry]] = {}
    for entry in manifest.entries:
        if not wanted(entry):
            continue
        path = root / entry.path
        if entry.link is not None:
            if not path.is_symlink() and not path.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                path.symlink_to(entry.link)
                done.written += 1
            elif path.is_symlink() and os.readlink(path) == entry.link:
                done.same += 1
            else:
                done.kept.append(entry.path)
            continue
        assert entry.hash is not None
        if path.is_symlink() or path.exists():
            same = path.is_file() and not path.is_symlink() and hash_file(path).hash == entry.hash
            if same:
                done.same += 1
            else:
                done.kept.append(entry.path)
            continue
        missing.setdefault(entry.hash, []).append(entry)

    def write(entries: list[Entry]) -> int:
        """One blob, read once from the store (a download on a remote), then copied to its other paths."""
        first, *copies = entries
        assert first.hash is not None
        size = store.get_path(first.hash, root / first.path, first.mode)
        for entry in copies:
            _copy(root / first.path, root / entry.path, entry.mode)
        return size * len(entries)

    pool = ThreadPoolExecutor(workers, thread_name_prefix='materialise')
    try:
        done.bytes += sum(pool.map(write, missing.values()))
    finally:
        pool.shutdown(cancel_futures=True)
    done.written += sum(len(entries) for entries in missing.values())
    return done


def _copy(src: Path, path: Path, mode: int) -> None:
    """``src`` to ``path`` with ``mode``, renamed into place."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=path.parent, prefix=f'.{path.name}.')
    os.close(fd)
    tmp = Path(name)
    try:
        shutil.copyfile(src, tmp)
        tmp.chmod(mode)
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)
