"""Directory trees in and out of the artifact store: put a tree, remove what a manifest holds, materialise a manifest.

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
    """What ``put_tree`` stored: the manifest, and the paths it left out."""

    manifest: Manifest
    skipped: list[str] = field(default_factory=list)
    """Paths left out, and left in place: special files, files that cannot be read, files that changed while they were
    stored."""

    @property
    def size(self) -> int:
        return sum(e.size for e in self.manifest.entries)


@dataclass
class Walk:
    """What ``walk`` found under some names: the files to store, and what stores at no cost (symlinks).

    ``take`` splits it into the part that fits a byte budget and the files left over.
    """

    files: list[tuple[str, os.stat_result]] = field(default_factory=list)
    links: list[Entry] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)

    @property
    def size(self) -> int:
        """The bytes of its files, uncompressed: what storing them can take at most (zstd hardly grows any input)."""
        return sum(st.st_size for _, st in self.files)

    def take(self, budget: int) -> tuple[Walk, Walk]:
        """Split off what fits in ``budget`` bytes; return it and a walk of the files left over.

        The part taken has the links and skipped paths, and the files that fit, in order, skipping each that does not.
        """
        taken = Walk(links=self.links, skipped=self.skipped)
        left = Walk()
        for rel, st in self.files:
            fits = st.st_size <= budget
            (taken if fits else left).files.append((rel, st))
            budget -= st.st_size if fits else 0
        return taken, left


def walk(root: Path, names: Iterable[str]) -> Walk:
    """Find every file and symlink under ``root/<name>`` for each of ``names`` (files or directories)."""
    found = Walk()

    def visit(rel: str) -> None:
        path = root / rel
        st = path.lstat()
        if stat.S_ISLNK(st.st_mode):
            found.links.append(Entry(rel, stat.S_IMODE(st.st_mode), link=os.readlink(path)))
        elif stat.S_ISREG(st.st_mode):
            found.files.append((rel, st))
        elif stat.S_ISDIR(st.st_mode):
            try:
                children = sorted(os.listdir(path))
            except PermissionError:
                found.skipped.append(rel)
                return
            for child in children:
                visit(f'{rel}/{child}')
        else:
            found.skipped.append(rel)

    for name in sorted(names):
        visit(name)
    return found


def put_tree(
    store: Store, root: Path, names: Iterable[str], stop: threading.Event | None = None, *, workers: int = WORKERS
) -> Stored:
    """Store every file and symlink under ``root/<name>`` for each of ``names``: ``put_walk`` of ``walk``."""
    return put_walk(store, root, walk(root, names), stop, workers=workers)


def put_walk(
    store: Store, root: Path, found: Walk, stop: threading.Event | None = None, *, workers: int = WORKERS
) -> Stored:
    """Store the files and symlinks of ``found`` (paths relative to ``root``).

    Files are put ``workers`` at a time: on a remote, each blob is its own upload, and one at a time spent most of a
    measured archiving waiting on S3. Raises ``Cancelled`` once ``stop`` is set, before the next file starts.
    """
    stored = Stored(Manifest(()), skipped=list(found.skipped))
    entries = list(found.links)

    def put(rel: str, st: os.stat_result) -> Entry | None:
        if stop is not None and stop.is_set():
            raise Cancelled(rel)
        try:
            blob = store.put_path(root / rel)
        except Changed, PermissionError:
            return None
        return Entry(rel, stat.S_IMODE(st.st_mode), blob.size, blob.hash)

    files = found.files
    pool = ThreadPoolExecutor(workers, thread_name_prefix='put-tree')
    try:
        for (rel, _), entry in zip(
            files, pool.map(put, [rel for rel, _ in files], [st for _, st in files]), strict=True
        ):
            if entry is None:
                stored.skipped.append(rel)
                continue
            entries.append(entry)
    finally:
        pool.shutdown(cancel_futures=True)
    stored.manifest = Manifest(tuple(entries))
    return stored


def remove(root: Path, manifest: Manifest) -> int:
    """Remove the paths of ``manifest`` under ``root`` that still hold what it stored; then the directories left empty.

    A file whose content changed since (its size or its hash differs) or that cannot be read, or a symlink that points
    elsewhere, stays, and so does its directory. Returns the paths removed.
    """
    removed = 0
    for entry in manifest.entries:
        path = root / entry.path
        try:
            st = path.lstat()
        except FileNotFoundError:
            continue
        if entry.link is not None:
            same = stat.S_ISLNK(st.st_mode) and os.readlink(path) == entry.link
        else:
            same = stat.S_ISREG(st.st_mode) and st.st_size == entry.size and _hash(path) == entry.hash
        if same:
            path.unlink()
            removed += 1
    parents = {parent for e in manifest.entries for parent in Path(e.path).parents if parent != Path()}
    for rel in sorted(parents, key=lambda d: len(d.parts), reverse=True):
        path = root / rel
        if path.is_dir() and not path.is_symlink() and not any(path.iterdir()):
            path.rmdir()
    return removed


def _hash(path: Path) -> str | None:
    """The blob address of a file, or None when it cannot be read now: then it is not removed."""
    try:
        return hash_file(path).hash
    except OSError:
        return None


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
