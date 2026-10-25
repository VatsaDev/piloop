import errno
import os
import shutil
import threading
from pathlib import Path
from typing import BinaryIO

import pytest
from covalence import Cid, IntegrityError, Value
from pydantic import ValidationError

from piloop.plugins.cas.contract import Writer
from piloop.plugins.cas.disk import DiskCas
from piloop.plugins.cas.kit.objects import ObjectsCas
from piloop.store import tree as tree_module
from piloop.store.tree import (
    Cancelled,
    Copies,
    Entry,
    Manifest,
    Missing,
    Removed,
    changed,
    materialise,
    merge,
    put_tree,
    put_walk,
    remove,
    stage,
    walk,
    write_files,
)


def disk(tmp_path: Path) -> ObjectsCas:
    return DiskCas(tmp_path / 'store')


def tree(root: Path) -> dict[str, bytes | str]:
    """Every file (its bytes) and symlink (its target) under ``root``, by relative path."""
    found: dict[str, bytes | str] = {}
    for path in sorted(root.rglob('*')):
        rel = str(path.relative_to(root))
        if path.is_symlink():
            found[rel] = os.readlink(path)
        elif path.is_file():
            found[rel] = path.read_bytes()
    return found


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    root = tmp_path / 'workspace'
    (root / 'idea7' / 'arrays').mkdir(parents=True)
    (root / 'idea7' / 'arrays' / 'x.npy').write_bytes(b'\x93NUMPY' + bytes(1000))
    (root / 'idea7' / 'run.sh').write_text('#!/bin/sh\n')
    (root / 'idea7' / 'run.sh').chmod(0o755)
    (root / 'idea7' / 'copy.npy').write_bytes(b'\x93NUMPY' + bytes(1000))
    (root / 'idea7' / 'latest').symlink_to('arrays/x.npy')
    (root / 'idea7' / 'empty').mkdir()
    (root / 'idea7.log').write_text('done\n')
    (root / 'NOTES.md').write_text('# Notes\n')
    return root


def test_a_tree_is_stored_removed_and_materialised_back_byte_for_byte(tmp_path: Path, workspace: Path) -> None:
    store = disk(tmp_path)
    before = tree(workspace)
    stored = put_tree(store, workspace, ['idea7', 'idea7.log'])
    assert sorted(e.path for e in stored.manifest.entries) == [
        'idea7.log',
        'idea7/arrays/x.npy',
        'idea7/copy.npy',
        'idea7/latest',
        'idea7/run.sh',
    ]
    assert len(stored.manifest.blobs) == 3, 'the two arrays are one blob'
    assert stored.size == 2 * 1006 + 10 + 5 and stored.skipped == {}
    assert remove(workspace, stored.manifest) == Removed(5)
    assert tree(workspace) == {'NOTES.md': b'# Notes\n'}
    assert [p.name for p in (workspace / 'idea7').iterdir()] == ['empty'], 'a directory that it emptied goes'
    done = materialise(store, Manifest.get(store, stored.manifest.put(store)), workspace)
    assert (done.written, done.bytes, done.same, done.kept) == (5, 2 * 1006 + 10 + 5, 0, [])
    assert tree(workspace) == before
    assert (workspace / 'idea7' / 'run.sh').stat().st_mode & 0o777 == 0o755
    again = materialise(store, stored.manifest, workspace)
    assert (again.written, again.same) == (0, 5), 'what is there already is left alone'


def test_materialise_keeps_what_differs_and_writes_only_what_is_wanted(tmp_path: Path, workspace: Path) -> None:
    store = disk(tmp_path)
    stored = put_tree(store, workspace, ['idea7'])
    remove(workspace, stored.manifest)
    (workspace / 'idea7' / 'empty').rmdir()
    (workspace / 'idea7').rmdir()
    (workspace / 'idea7').mkdir()
    (workspace / 'idea7' / 'run.sh').write_text('changed\n')
    (workspace / 'idea7' / 'latest').symlink_to('elsewhere')
    (workspace / 'idea7' / 'copy.npy').symlink_to('run.sh')
    done = materialise(store, stored.manifest, workspace, lambda e: not e.path.startswith('idea7/arrays'))
    assert sorted(done.kept) == ['idea7/copy.npy', 'idea7/latest', 'idea7/run.sh']
    assert done.written == 0 and not (workspace / 'idea7' / 'arrays').exists()
    assert (workspace / 'idea7' / 'run.sh').read_text() == 'changed\n'


def test_remove_leaves_what_changed_since_it_was_stored(tmp_path: Path, workspace: Path) -> None:
    """Days can pass between the archiving and the removal: the content decides, not a timestamp."""
    stored = put_tree(disk(tmp_path), workspace, ['idea7'])
    (workspace / 'idea7' / 'run.sh').write_text('#!/bin/sh\necho more\n')
    (workspace / 'idea7' / 'arrays' / 'new.npy').write_bytes(b'new')
    (workspace / 'idea7' / 'copy.npy').unlink()
    (workspace / 'idea7' / 'latest').unlink()
    (workspace / 'idea7' / 'latest').symlink_to('elsewhere')
    same_size = workspace / 'idea7.log'
    same_size.write_text('dome\n')
    assert remove(workspace, stored.manifest) == Removed(1), 'only arrays/x.npy still holds what was stored'
    assert tree(workspace / 'idea7') == {
        'arrays/new.npy': b'new',
        'latest': 'elsewhere',
        'run.sh': b'#!/bin/sh\necho more\n',
    }


def test_special_files_and_files_changing_under_the_store_are_skipped(
    tmp_path: Path, workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    os.mkfifo(workspace / 'idea7' / 'pipe')
    real = ObjectsCas.put_fileobj

    def flaky(self: ObjectsCas, file: BinaryIO, *, max_bytes: int, expected_length: int | None = None) -> Cid:
        if file.name.endswith('run.sh'):
            raise IntegrityError(f'{file.name} changed while it was stored')
        return real(self, file, max_bytes=max_bytes, expected_length=expected_length)

    monkeypatch.setattr(ObjectsCas, 'put_fileobj', flaky)
    stored = put_tree(disk(tmp_path), workspace, ['idea7'])
    assert stored.skipped == {
        'idea7/pipe': 'not a regular file, directory or symlink: never stored',
        'idea7/run.sh': 'changed while it was read: the next archiving takes it',
    }
    remove(workspace, stored.manifest)
    assert sorted(p.name for p in (workspace / 'idea7').iterdir()) == ['empty', 'pipe', 'run.sh']


def test_put_tree_stops_between_two_files_when_asked(tmp_path: Path, workspace: Path) -> None:
    stop = threading.Event()
    stop.set()
    with pytest.raises(Cancelled):
        put_tree(disk(tmp_path), workspace, ['idea7'], stop)
    assert tree(workspace)['idea7.log'] == b'done\n'


def test_what_cannot_be_read_is_skipped_and_left_in_place(tmp_path: Path, workspace: Path) -> None:
    secret, locked = workspace / 'idea7' / 'run.sh', workspace / 'idea7' / 'arrays'
    secret.chmod(0)
    locked.chmod(0)
    try:
        stored = put_tree(disk(tmp_path), workspace, ['idea7'])
        assert stored.skipped == {
            'idea7/arrays': 'directory not readable (Permission denied): fix its permissions',
            'idea7/run.sh': 'not readable (Permission denied): fix its permissions',
        }
        remove(workspace, stored.manifest)
        assert sorted(p.name for p in (workspace / 'idea7').iterdir()) == ['arrays', 'empty', 'run.sh']
    finally:
        secret.chmod(0o644)
        locked.chmod(0o755)


def test_remove_leaves_a_file_it_cannot_read(tmp_path: Path, workspace: Path) -> None:
    stored = put_tree(disk(tmp_path), workspace, ['idea7'])
    (workspace / 'idea7' / 'run.sh').chmod(0)
    try:
        assert remove(workspace, stored.manifest) == Removed(
            3, {'idea7/run.sh': 'not readable (Permission denied): fix its permissions'}
        )
        assert (workspace / 'idea7' / 'run.sh').exists()
    finally:
        (workspace / 'idea7' / 'run.sh').chmod(0o644)


def test_a_file_gone_between_the_walk_and_its_put_is_left_out(tmp_path: Path, workspace: Path) -> None:
    """A prune between the walk and the put raised FileNotFoundError."""
    found = walk(workspace, ['idea7'])
    (workspace / 'idea7' / 'copy.npy').unlink()
    stored = put_walk(disk(tmp_path), workspace, found)
    assert stored.skipped == {'idea7/copy.npy': 'gone since the walk: an agent or a prune removed it'}
    assert 'idea7/copy.npy' not in {e.path for e in stored.manifest.entries}


def test_nothing_is_removed_or_written_through_a_symlinked_directory(tmp_path: Path, workspace: Path) -> None:
    """An archived directory moved away and replaced by a symlink to it."""
    store = disk(tmp_path)
    stored = put_tree(store, workspace, ['idea7'])
    outside = tmp_path / 'outside'
    (workspace / 'idea7').rename(outside)
    (workspace / 'idea7').symlink_to(outside)
    assert remove(workspace, stored.manifest) == Removed(0)
    assert (outside / 'arrays' / 'x.npy').is_file() and (outside / 'run.sh').is_file(), 'the files outside stay'
    (outside / 'run.sh').unlink()
    done = materialise(store, stored.manifest, workspace)
    assert done.written == 0 and 'idea7/run.sh' in done.kept and not (outside / 'run.sh').exists()


def test_a_restore_never_replaces_what_someone_wrote_while_the_blob_was_read(
    tmp_path: Path, workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A file an agent wrote during the download was overwritten."""
    store = disk(tmp_path)
    stored = put_tree(store, workspace, ['idea7'])
    remove(workspace, stored.manifest)
    real = ObjectsCas.get_to

    def racing(
        self: ObjectsCas, cid: Cid, dst: Writer, *, max_bytes: int, expected_length: int | None = None
    ) -> int | None:
        for name in ('x.npy', 'copy.npy'):  # an agent writes both paths of the shared blob meanwhile
            path = workspace / 'idea7' / 'arrays' / name if name == 'x.npy' else workspace / 'idea7' / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('agent')
        return real(self, cid, dst, max_bytes=max_bytes, expected_length=expected_length)

    monkeypatch.setattr(ObjectsCas, 'get_to', racing)
    done = materialise(store, stored.manifest, workspace, lambda e: e.path.endswith('.npy'))
    assert sorted(done.kept) == ['idea7/arrays/x.npy', 'idea7/copy.npy'] and done.written == 0
    assert (workspace / 'idea7' / 'copy.npy').read_text() == 'agent'


def test_each_path_of_a_shared_blob_gets_the_checked_bytes_whatever_an_agent_does_to_another(
    tmp_path: Path, workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Later copies were made from the first restored path, which an agent edited."""
    store = disk(tmp_path)
    stored = put_tree(store, workspace, ['idea7'])
    original = (workspace / 'idea7' / 'copy.npy').read_bytes()
    remove(workspace, stored.manifest)
    real = tree_module._copy
    written: list[Path] = []

    def editing(src: Path, path: Path, mode: int, mtime_ns: int | None) -> None:
        real(src, path, mode, mtime_ns)
        written.append(path)
        written[0].write_bytes(b'edited by an agent')  # the first restored path changes before the next copy

    monkeypatch.setattr(tree_module, '_copy', editing)
    materialise(store, stored.manifest, workspace, lambda e: e.path.endswith('.npy'))
    assert len(written) == 2 and written[1].read_bytes() == original


def test_a_file_where_the_manifest_has_a_directory_keeps_its_entries_and_the_rest_is_restored(
    tmp_path: Path, workspace: Path
) -> None:
    """NotADirectoryError ended the restore."""
    store = disk(tmp_path)
    stored = put_tree(store, workspace, ['idea7', 'idea7.log'])
    remove(workspace, stored.manifest)
    (workspace / 'idea7' / 'empty').rmdir()
    (workspace / 'idea7').rmdir()
    (workspace / 'idea7').write_text('a file now')
    done = materialise(store, stored.manifest, workspace)
    assert sorted(done.kept) == ['idea7/arrays/x.npy', 'idea7/copy.npy', 'idea7/latest', 'idea7/run.sh']
    assert done.written == 1 and (workspace / 'idea7.log').read_text() == 'done\n'


def test_remove_leaves_a_path_below_a_file_that_replaced_its_directory(tmp_path: Path, workspace: Path) -> None:
    """NotADirectoryError ended the whole prune."""
    stored = put_tree(disk(tmp_path), workspace, ['idea7', 'idea7.log'])
    shutil.rmtree(workspace / 'idea7')
    (workspace / 'idea7').write_text('a file now')
    assert remove(workspace, stored.manifest) == Removed(1), 'idea7.log; the rest is not there to remove'
    assert (workspace / 'idea7').read_text() == 'a file now'


def test_a_manifest_is_a_dasl_value_that_links_its_blobs(tmp_path: Path) -> None:
    cas = disk(tmp_path)
    entries = (
        Entry(path='b/x.npy', mode=0o644, size=3, blob=Cid.of(b'abc')),
        Entry(path='a/link', mode=0o777, link='../b/x.npy'),
    )
    cid = Manifest(entries=entries).put(cas)
    assert cid.codec.name == 'dag-cbor' and cid == Manifest(entries=entries).value().cid() and cas.has(cid)
    again = Manifest.get(cas, cid)
    assert again.entries == tuple(sorted(entries, key=lambda e: e.path)), 'sorted by path'
    assert again.blobs == {Cid.of(b'abc')} and again.value().links() == [Cid.of(b'abc')]
    assert Manifest(entries=entries[::-1]).put(cas) == cid, 'one tree, one address'
    junk = Cid.from_parts(
        'drisl', 'blake3', cas.put(Value({'entries': [{'path': 'x', 'colour': 'red'}]}).encode()).digest
    )
    with pytest.raises(ValidationError, match='colour'):
        Manifest.get(cas, junk)
    with pytest.raises(Missing, match='is not in the CAS'):
        Manifest.get(cas, Cid.of(b'never'))
    with pytest.raises(ValueError, match='names no manifest'):
        Manifest.get(cas, cas.put(b'{"version": 2}'))


def test_a_blob_that_the_cas_lost_stops_a_restore(tmp_path: Path, workspace: Path) -> None:
    stored = put_tree(disk(tmp_path), workspace, ['idea7.log'])
    remove(workspace, stored.manifest)
    with pytest.raises(Missing, match='of idea7.log is not in the CAS'):
        materialise(disk(tmp_path / 'other'), stored.manifest, workspace)


@pytest.fixture
def no_reflink(monkeypatch: pytest.MonkeyPatch) -> None:
    """A filesystem without copy-on-write clones (ext4): a tree gets hard links to the shared copies."""

    def unsupported(fd: int, request: int, arg: int) -> None:
        raise OSError(errno.EOPNOTSUPP, 'Operation not supported')

    monkeypatch.setattr(tree_module.fcntl, 'ioctl', unsupported)


@pytest.fixture
def reflink(monkeypatch: pytest.MonkeyPatch) -> None:
    """A filesystem with copy-on-write clones (btrfs, XFS): a clone is a copy of its own, which this fakes."""

    def clone(fd: int, request: int, src: int) -> None:
        assert request == tree_module.FICLONE
        data = memoryview(os.pread(src, os.fstat(src).st_size, 0))  # not sendfile: macOS sends to a socket only
        while data:
            data = data[os.write(fd, data) :]

    monkeypatch.setattr(tree_module.fcntl, 'ioctl', clone)


def copies(tmp_path: Path) -> Path:
    return tmp_path / 'inputs'


def shared(cas: ObjectsCas, tmp_path: Path) -> Copies:
    return Copies(cas, copies(tmp_path))


@pytest.mark.usefixtures('no_reflink')
def test_without_reflinks_trees_share_hard_links_to_read_only_copies(tmp_path: Path, workspace: Path) -> None:
    cas = disk(tmp_path)
    into = shared(cas, tmp_path)
    stored = put_tree(cas, workspace, ['idea7'])
    first, second = tmp_path / 'first', tmp_path / 'second'
    for root in (first, second):
        done = materialise(cas, stored.manifest, root, into=into)
        assert (done.written, done.kept, done.rebuilt, len(done.copies)) == (4, [], [], 3)
    assert tree(first) == tree(second) == {rel: v for rel, v in tree(workspace).items() if rel.startswith('idea7/')}
    x = first / 'idea7' / 'arrays' / 'x.npy'
    assert x.stat().st_ino == (second / 'idea7' / 'arrays' / 'x.npy').stat().st_ino, 'one copy, linked into both'
    assert oct(x.stat().st_mode & 0o777) == oct(0o444) and (first / 'idea7' / 'run.sh').stat().st_mode & 0o777 == 0o555
    with pytest.raises(PermissionError):
        x.write_bytes(b'a change in place')
    (first / 'idea7' / 'arrays' / 'new.npy').write_bytes(b'a new file')  # the directories are the tree's own
    assert not (second / 'idea7' / 'arrays' / 'new.npy').exists()


@pytest.mark.parametrize('clones', ['no_reflink', 'reflink'])
@pytest.mark.parametrize('shares', [True, False])
def test_materialised_files_keep_their_modification_times(
    tmp_path: Path, workspace: Path, clones: str, shares: bool, request: pytest.FixtureRequest
) -> None:
    """A cache that checks its source's time (Python's bytecode, make) stays current in a copy: the arrays are one blob
    with two times, so each tree's copy of it is its own when they would share an inode."""
    request.getfixturevalue(clones)
    arrays, other = workspace / 'idea7' / 'arrays' / 'x.npy', workspace / 'idea7' / 'copy.npy'
    os.utime(arrays, ns=(1_700_000_000_000_000_000, 1_700_000_000_000_000_000))
    os.utime(other, ns=(1_600_000_000_000_000_000, 1_600_000_000_000_000_000))
    cas = disk(tmp_path)
    stored = put_tree(cas, workspace, ['idea7'])
    into = shared(cas, tmp_path) if shares else None
    materialise(cas, Manifest.get(cas, stored.manifest.put(cas)), tmp_path / 'tree', into=into)
    for source in (arrays, other):
        copy = tmp_path / 'tree' / source.relative_to(workspace)
        assert copy.stat().st_mtime_ns == source.stat().st_mtime_ns


@pytest.mark.usefixtures('no_reflink')
def test_a_copy_that_a_job_changed_through_its_hard_link_is_refused_and_made_again(
    tmp_path: Path, workspace: Path
) -> None:
    """Read-only is a mode, and a job that runs as piloop's user can change it: the next use finds the change."""
    cas = disk(tmp_path)
    into = shared(cas, tmp_path)
    stored = put_tree(cas, workspace, ['idea7.log'])
    done = materialise(cas, stored.manifest, tmp_path / 'first', into=into)
    linked = tmp_path / 'first' / 'idea7.log'
    linked.chmod(0o644)
    linked.write_text('cooked\n')
    assert not done.copies['idea7.log'].holds(linked)
    again = materialise(cas, stored.manifest, tmp_path / 'second', into=into)
    assert again.rebuilt == [stored.manifest.entries[0].blob]
    assert (tmp_path / 'second' / 'idea7.log').read_text() == 'done\n', 'made again from the CAS'


@pytest.mark.usefixtures('no_reflink')
def test_a_copy_touched_but_not_changed_is_read_again_and_kept(tmp_path: Path, workspace: Path) -> None:
    cas = disk(tmp_path)
    into = shared(cas, tmp_path)
    stored = put_tree(cas, workspace, ['idea7.log'])
    done = materialise(cas, stored.manifest, tmp_path / 'first', into=into)
    linked = tmp_path / 'first' / 'idea7.log'
    os.utime(linked, (0, 0))
    assert not done.copies['idea7.log'].unchanged(linked) and done.copies['idea7.log'].holds(linked)
    assert materialise(cas, stored.manifest, tmp_path / 'second', into=into).rebuilt == []
    linked.unlink()
    assert not done.copies['idea7.log'].holds(linked) and not done.copies['idea7.log'].unchanged(linked)


@pytest.mark.usefixtures('reflink')
def test_with_reflinks_a_change_to_one_tree_stays_in_it(tmp_path: Path, workspace: Path) -> None:
    cas = disk(tmp_path)
    into = shared(cas, tmp_path)
    stored = put_tree(cas, workspace, ['idea7.log'])
    materialise(cas, stored.manifest, tmp_path / 'first', into=into)
    linked = tmp_path / 'first' / 'idea7.log'
    assert linked.stat().st_nlink == 1 and linked.stat().st_mode & 0o777 == 0o444, 'a clone of its own'
    linked.chmod(0o644)
    linked.write_text('cooked\n')
    again = materialise(cas, stored.manifest, tmp_path / 'second', into=into)
    assert again.rebuilt == [] and (tmp_path / 'second' / 'idea7.log').read_text() == 'done\n'


def test_a_copy_removed_between_its_check_and_its_link_is_made_again(
    tmp_path: Path, workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cas = disk(tmp_path)
    into = shared(cas, tmp_path)
    stored = put_tree(cas, workspace, ['idea7.log'])
    real = tree_module._reflink
    calls: list[Path] = []

    def removed_once(source: Path, path: Path, mode: int, mtime_ns: int | None) -> None:
        calls.append(source)
        if len(calls) == 1:
            source.unlink()
        real(source, path, mode, mtime_ns)

    monkeypatch.setattr(tree_module, '_reflink', removed_once)
    done = materialise(cas, stored.manifest, tmp_path / 'out', into=into)
    assert len(calls) == 2 and done.written == 1 and (tmp_path / 'out' / 'idea7.log').read_text() == 'done\n'


def test_a_shared_copy_of_a_blob_the_cas_lost_stops_the_tree(tmp_path: Path, workspace: Path) -> None:
    stored = put_tree(disk(tmp_path), workspace, ['idea7.log'])
    with pytest.raises(Missing):
        materialise(disk(tmp_path), stored.manifest, tmp_path / 'out', into=shared(disk(tmp_path / 'empty'), tmp_path))


@pytest.mark.usefixtures('no_reflink')
def test_a_copy_on_another_filesystem_is_a_read_only_copy_not_a_link(
    tmp_path: Path, workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    link = os.link

    def cross_device(src: Path, dst: Path) -> None:
        if Path(src).parent == copies(tmp_path) != Path(dst).parent:
            raise OSError(errno.EXDEV, 'Invalid cross-device link')
        link(src, dst)

    cas = disk(tmp_path)
    into = shared(cas, tmp_path)
    stored = put_tree(cas, workspace, ['idea7.log'])
    monkeypatch.setattr(tree_module.os, 'link', cross_device)
    done = materialise(cas, stored.manifest, tmp_path / 'out', into=into)
    out = tmp_path / 'out' / 'idea7.log'
    assert done.written == 1 and out.read_text() == 'done\n' and out.stat().st_mode & 0o777 == 0o444


def test_a_link_or_a_reflink_that_fails_otherwise_is_an_error(
    tmp_path: Path, workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refused(*args: object) -> None:
        raise OSError(errno.EPERM, 'Operation not permitted')

    cas = disk(tmp_path)
    into = shared(cas, tmp_path)
    stored = put_tree(cas, workspace, ['idea7.log'])
    monkeypatch.setattr(tree_module.fcntl, 'ioctl', refused)
    with pytest.raises(PermissionError):
        materialise(cas, stored.manifest, tmp_path / 'out', into=into)
    assert not (tmp_path / 'out' / 'idea7.log').exists(), 'no half-made file'
    link = os.link

    def no_link(src: Path, dst: Path) -> None:
        if Path(src).parent == copies(tmp_path) != Path(dst).parent:
            raise OSError(errno.EPERM, 'Operation not permitted')
        link(src, dst)

    def no_reflink(*args: object) -> None:
        raise OSError(errno.ENOTTY, 'Inappropriate ioctl for device')

    monkeypatch.setattr(tree_module.fcntl, 'ioctl', no_reflink)
    monkeypatch.setattr(tree_module.os, 'link', no_link)
    with pytest.raises(PermissionError):
        materialise(cas, stored.manifest, tmp_path / 'out', into=into)


@pytest.mark.usefixtures('no_reflink')
def test_a_path_taken_meanwhile_is_kept_when_placing(
    tmp_path: Path, workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cas = disk(tmp_path)
    into = shared(cas, tmp_path)
    stored = put_tree(cas, workspace, ['idea7.log'])
    real = Copies.place

    def taken(self: Copies, blob: Cid, mode: int, mtime_ns: int | None, path: Path) -> object:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('written meanwhile')
        return real(self, blob, mode, mtime_ns, path)

    monkeypatch.setattr(Copies, 'place', taken)
    done = materialise(cas, stored.manifest, tmp_path / 'out', into=into)
    assert (done.written, done.kept) == (0, ['idea7.log'])
    assert (tmp_path / 'out' / 'idea7.log').read_text() == 'written meanwhile'


def test_a_file_stored_before_with_the_same_stat_is_not_read_again(
    tmp_path: Path, workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cas, seen = disk(tmp_path), {}
    first = put_tree(cas, workspace, ['idea7'], seen=seen)
    puts: list[str] = []
    real = DiskCas.put_fileobj

    def counting(self: DiskCas, file: BinaryIO, *, max_bytes: int, expected_length: int | None = None) -> Cid:
        puts.append(file.name)
        return real(self, file, max_bytes=max_bytes, expected_length=expected_length)

    monkeypatch.setattr(DiskCas, 'put_fileobj', counting)
    (workspace / 'idea7' / 'run.sh').write_text('#!/bin/sh\necho changed\n')
    again = put_tree(cas, workspace, ['idea7'], seen=seen)
    assert puts == [str(workspace / 'idea7' / 'run.sh')], 'only the file whose stat changed'
    assert first.manifest != again.manifest


def test_files_are_written_under_a_root_and_replace_other_content(tmp_path: Path) -> None:
    cas = disk(tmp_path)
    same, other = cas.put(b'same'), cas.put(b'new')
    root = tmp_path / 'root'
    (root / 'logs').mkdir(parents=True)
    (root / 'same.txt').write_bytes(b'same')
    (root / 'logs' / 'eval.stdout').write_bytes(b'old')
    before = (root / 'same.txt').stat().st_ino
    write_files(cas, {'same.txt': same, 'logs/eval.stdout': other, 'new/brief.md': other}, root)
    assert tree(root) == {'logs/eval.stdout': b'new', 'new/brief.md': b'new', 'same.txt': b'same'}
    assert (root / 'same.txt').stat().st_ino == before, 'what holds the content already stays'
    with pytest.raises(Missing):
        write_files(disk(tmp_path / 'empty'), {'lost.txt': other}, root)


def test_a_merge_writes_each_path_that_still_holds_its_base_and_puts_the_others_beside(
    tmp_path: Path, workspace: Path
) -> None:
    """A compare-and-swap for each path, on the blob that the job started from: the first writer of a path wins."""
    cas = disk(tmp_path)
    base = put_tree(cas, workspace, ['NOTES.md', 'idea7.log', 'idea7']).manifest
    [same] = [e.blob for e in base.entries if e.path == 'idea7/run.sh' and e.blob is not None]
    (workspace / 'idea7.log').write_text('another agent wrote this meanwhile\n')
    (workspace / 'idea7' / 'blocked').mkdir()
    files = {
        'NOTES.md': cas.put(b'# Notes\nmine\n'),
        'idea7.log': cas.put(b'mine\n'),
        'idea7/new.txt': cas.put(b'new\n'),
        'idea7/blocked': cas.put(b'x'),
        'idea7/latest': cas.put(b'not through a symlink'),
        '../outside': cas.put(b'x'),
        'idea7/run.sh': same,
        'idea7/copy.npy': None,
        'idea7.gone': None,
    }
    (workspace / 'NOTES.md.removed').write_text('x')
    merged = merge(workspace, base, stage(cas, files, tmp_path / 'scratch'), 'implementer-3')
    assert sorted(merged.written) == ['NOTES.md', 'idea7/new.txt']
    assert merged.removed == ['idea7/copy.npy'], 'a removal is a write: it holds what the job started from'
    assert merged.beside == {'idea7.log': 'idea7.log.implementer-3'}
    assert sorted(merged.blocked) == ['../outside', 'idea7/blocked', 'idea7/latest']
    assert (workspace / 'NOTES.md').read_text() == '# Notes\nmine\n'
    assert (workspace / 'idea7.log').read_text() == 'another agent wrote this meanwhile\n'
    assert (workspace / 'idea7.log.implementer-3').read_text() == 'mine\n'
    assert (workspace / 'idea7' / 'new.txt').read_text() == 'new\n'
    assert not (workspace / 'idea7' / 'copy.npy').exists()
    assert not (tmp_path / 'outside').exists()
    (workspace / 'NOTES.md.implementer-4').mkdir()
    (workspace / 'NOTES.md').write_text('a third version\n')
    staged = stage(cas, {'NOTES.md': cas.put(b'x'), 'idea7/run.sh': None}, tmp_path / 'scratch')
    again = merge(workspace, base, staged, 'implementer-4')
    assert (again.blocked, again.removed) == (['NOTES.md'], ['idea7/run.sh'])
    (workspace / 'idea7.log').write_text('changed again\n')
    assert merge(workspace, base, {'idea7.log': None}, 'implementer-5').kept == ['idea7.log'], (
        'another agent changed it since: it stays'
    )
    with pytest.raises(Missing):
        stage(cas, {'gone.txt': Cid.of(b'never stored')}, tmp_path / 'scratch')


def test_nothing_changed_where_no_tree_is(tmp_path: Path) -> None:
    assert changed(disk(tmp_path), tmp_path / 'never-made', Manifest(entries=()), {}) == {}
