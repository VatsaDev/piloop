"""Directory trees in and out of the artifact store: put a tree, remove what a manifest holds, materialise a manifest.

Generic, like ``piloop.store``: paths and files only. Regular files and symlinks are stored; empty directories and
special files (sockets, fifos) are not, and the latter stay where they are.
"""

import os
import shutil
import stat
import tempfile
import threading
from collections.abc import Callable, Collection, Iterable
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
    """What ``put_tree`` stored: the manifest, and the paths it left out, each with why."""

    manifest: Manifest
    skipped: dict[str, str] = field(default_factory=dict)
    """Relative path → why it was left out (and left in place): a special file, a file or directory it cannot read, a
    file that changed while it was stored, or one that went since the walk."""

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
    skipped: dict[str, str] = field(default_factory=dict)
    """Relative path → why the walk left it out: a special file, or a directory it cannot read."""

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
            except PermissionError as e:
                found.skipped[rel] = f'directory not readable ({e.strerror}): fix its permissions'
                return
            for child in children:
                visit(f'{rel}/{child}')
        else:
            found.skipped[rel] = 'not a regular file, directory or symlink: never stored'

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
    stored = Stored(Manifest(()), skipped=dict(found.skipped))
    entries = list(found.links)

    def put(rel: str, st: os.stat_result) -> Entry | str:
        """The entry of a stored file, or why it was left out: each is a reason for the caller to report, not a fault."""
        if stop is not None and stop.is_set():
            raise Cancelled(rel)
        try:
            blob = store.put_path(root / rel)
        except Changed:
            return 'changed while it was read: the next archiving takes it'
        except PermissionError as e:
            return f'not readable ({e.strerror}): fix its permissions'
        except FileNotFoundError:
            return 'gone since the walk: an agent or a prune removed it'
        return Entry(rel, stat.S_IMODE(st.st_mode), blob.size, blob.hash)

    files = found.files
    pool = ThreadPoolExecutor(workers, thread_name_prefix='put-tree')
    try:
        for (rel, _), entry in zip(
            files, pool.map(put, [rel for rel, _ in files], [st for _, st in files]), strict=True
        ):
            if isinstance(entry, str):
                stored.skipped[rel] = entry
                continue
            entries.append(entry)
    finally:
        pool.shutdown(cancel_futures=True)
    stored.manifest = Manifest(tuple(entries))
    return stored


@dataclass
class Removed:
    """What ``remove`` did: the paths it removed, and the ones it could not read to compare, each with why."""

    removed: int = 0
    left: dict[str, str] = field(default_factory=dict)


def remove(root: Path, manifest: Manifest, keep: Collection[str] = ()) -> Removed:
    """Remove the paths of ``manifest`` under ``root`` that still hold what it stored; then the directories left empty.

    A file whose content changed since (its size or its hash differs), or a symlink that points elsewhere, stays, and
    so does its directory. A path below a symlinked directory stays too: removing through it would remove a file
    outside ``root``, and so does a path below a file where the manifest had a directory. A file it cannot read, or
    whose blob is in ``keep`` (one that the store cannot vouch for), stays and is listed in ``left`` with why, so the
    caller can try again.
    """
    done = Removed()
    for entry in manifest.entries:
        path = root / entry.path
        if not _within(root, Path(entry.path)):
            continue
        try:
            st = path.lstat()
        except FileNotFoundError, NotADirectoryError:  # gone, or a file stands where its directory was
            continue
        if entry.link is not None:
            same = stat.S_ISLNK(st.st_mode) and os.readlink(path) == entry.link
        elif not stat.S_ISREG(st.st_mode) or st.st_size != entry.size:
            same = False
        elif entry.hash in keep:
            done.left[entry.path] = 'its blob in the store is missing or corrupt: run piloop store verify'
            continue
        else:
            try:
                same = hash_file(path).hash == entry.hash
            except OSError as e:
                done.left[entry.path] = f'not readable ({e.strerror}): fix its permissions'
                continue
        if same:
            path.unlink()
            done.removed += 1
    parents = {parent for e in manifest.entries for parent in Path(e.path).parents if parent != Path()}
    for rel in sorted(parents, key=lambda d: len(d.parts), reverse=True):
        path = root / rel
        if _within(root, rel) and path.is_dir() and not path.is_symlink() and not any(path.iterdir()):
            path.rmdir()
    return done


def _blocked(root: Path, rel: Path) -> bool:
    """Whether something other than a directory stands where ``rel`` needs a directory: its parents cannot be made."""
    return any((root / parent).exists() and not (root / parent).is_dir() for parent in rel.parents if parent != Path())


def _within(root: Path, rel: Path) -> bool:
    """Whether the directory of ``root / rel`` is the real one under ``root``: no symlink on the way leads elsewhere.

    It is checked right before each removal or write. An agent that swaps a directory for a symlink in between can
    still race it, but it runs as the same user and could remove those files itself.
    """
    return Path(os.path.realpath(root / rel.parent)) == Path(os.path.realpath(root)) / rel.parent


@dataclass
class Materialised:
    """What ``materialise`` did: files and links written, already there as stored, and kept.

    ``kept`` lists the paths left alone: they hold other content, or they sit below a symlinked directory.
    """

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

    Each blob is read once (a download on a remote), into a checked file of the store's own, ``workers`` at a time.
    Each path then gets a copy of that file, linked into place, which never replaces what someone wrote meanwhile. A
    path below a symlinked directory, or below a file where the manifest has a directory, is kept, not written.
    """
    done = Materialised()
    missing: dict[str, list[Entry]] = {}
    for entry in manifest.entries:
        if not wanted(entry):
            continue
        path = root / entry.path
        if not _within(root, Path(entry.path)) or _blocked(root, Path(entry.path)):
            done.kept.append(entry.path)
            continue
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

    def write(entries: list[Entry]) -> tuple[int, int, list[str]]:
        """One blob, read once into a checked file outside ``root``, then copied to each of its paths.

        Returns the paths written, their bytes, and the paths that something took or blocked meanwhile (kept).
        """
        assert entries[0].hash is not None
        written, kept = 0, []
        with tempfile.TemporaryDirectory(dir=store.scratch()) as scratch:
            source = Path(scratch) / 'blob'
            size = store.get_path(entries[0].hash, source)
            for entry in entries:
                try:
                    _copy(source, root / entry.path, entry.mode)
                except FileExistsError, NotADirectoryError:  # someone wrote there meanwhile: theirs wins
                    kept.append(entry.path)
                    continue
                written += 1
        return written, size * written, kept

    pool = ThreadPoolExecutor(workers, thread_name_prefix='materialise')
    try:
        for written, size, kept in pool.map(write, missing.values()):
            done.written += written
            done.bytes += size
            done.kept += kept
    finally:
        pool.shutdown(cancel_futures=True)
    return done


def _copy(src: Path, path: Path, mode: int) -> None:
    """``src`` to a new ``path`` with ``mode``, linked into place. Raises ``FileExistsError`` when ``path`` is taken."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=path.parent, prefix=f'.{path.name}.')
    os.close(fd)
    tmp = Path(name)
    try:
        shutil.copyfile(src, tmp)
        tmp.chmod(mode)
        os.link(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)
