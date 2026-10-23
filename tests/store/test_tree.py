import os
import shutil
import threading
import time
from datetime import timedelta
from pathlib import Path

import pytest
from covalence import Cid

from piloop.store import Blob, Changed, Store, Writer, tree as tree_module
from piloop.store.tree import Cancelled, Removed, materialise, put_tree, put_walk, remove, walk
from tests.store.test_store import DirRemote


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
    store = Store(tmp_path / 'store')
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
    done = materialise(store, store.manifest(store.put_manifest(stored.manifest)), workspace)
    assert (done.written, done.bytes, done.same, done.kept) == (5, 2 * 1006 + 10 + 5, 0, [])
    assert tree(workspace) == before
    assert (workspace / 'idea7' / 'run.sh').stat().st_mode & 0o777 == 0o755
    again = materialise(store, stored.manifest, workspace)
    assert (again.written, again.same) == (0, 5), 'what is there already is left alone'


def test_materialise_keeps_what_differs_and_writes_only_what_is_wanted(tmp_path: Path, workspace: Path) -> None:
    store = Store(tmp_path / 'store')
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
    stored = put_tree(Store(tmp_path / 'store'), workspace, ['idea7'])
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
    real = Store.put_path

    def flaky(self: Store, path: Path) -> Blob:
        if path.name == 'run.sh':
            raise Changed(str(path))
        return real(self, path)

    monkeypatch.setattr(Store, 'put_path', flaky)
    stored = put_tree(Store(tmp_path / 'store'), workspace, ['idea7'])
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
        put_tree(Store(tmp_path / 'store'), workspace, ['idea7'], stop)
    assert tree(workspace)['idea7.log'] == b'done\n'


def test_what_cannot_be_read_is_skipped_and_left_in_place(tmp_path: Path, workspace: Path) -> None:
    secret, locked = workspace / 'idea7' / 'run.sh', workspace / 'idea7' / 'arrays'
    secret.chmod(0)
    locked.chmod(0)
    try:
        stored = put_tree(Store(tmp_path / 'store'), workspace, ['idea7'])
        assert stored.skipped == {
            'idea7/arrays': 'directory not readable (Permission denied): fix its permissions',
            'idea7/run.sh': 'not readable (Permission denied): fix its permissions',
        }
        remove(workspace, stored.manifest)
        assert sorted(p.name for p in (workspace / 'idea7').iterdir()) == ['arrays', 'empty', 'run.sh']
    finally:
        secret.chmod(0o644)
        locked.chmod(0o755)


def test_threads_putting_one_content_to_a_staging_remote_upload_it_once(tmp_path: Path) -> None:
    class Slow(DirRemote):
        def upload(self, key: str, path: Path) -> None:
            time.sleep(0.05)  # like s3transfer, the file is opened only once the request goes out
            super().upload(key, path)

    root = tmp_path / 'workspace'
    root.mkdir()
    for n in range(8):
        (root / f'copy{n}.npy').write_bytes(b'same array')
    remote = Slow(tmp_path / 'remote')
    stored = put_tree(Store(tmp_path / 'store', remote, evict_after=timedelta(0)), root, os.listdir(root))
    assert len(stored.manifest.entries) == 8 and len(remote.uploads) == 1


def test_remove_leaves_a_file_it_cannot_read(tmp_path: Path, workspace: Path) -> None:
    stored = put_tree(Store(tmp_path / 'store'), workspace, ['idea7'])
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
    stored = put_walk(Store(tmp_path / 'store'), workspace, found)
    assert stored.skipped == {'idea7/copy.npy': 'gone since the walk: an agent or a prune removed it'}
    assert 'idea7/copy.npy' not in {e.path for e in stored.manifest.entries}


def test_nothing_is_removed_or_written_through_a_symlinked_directory(tmp_path: Path, workspace: Path) -> None:
    """An archived directory moved away and replaced by a symlink to it."""
    store = Store(tmp_path / 'store')
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
    store = Store(tmp_path / 'store')
    stored = put_tree(store, workspace, ['idea7'])
    remove(workspace, stored.manifest)
    real = Store.get

    def racing(self: Store, cid: Cid, dst: Writer) -> int:
        for name in ('x.npy', 'copy.npy'):  # an agent writes both paths of the shared blob meanwhile
            path = workspace / 'idea7' / 'arrays' / name if name == 'x.npy' else workspace / 'idea7' / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('agent')
        return real(self, cid, dst)

    monkeypatch.setattr(Store, 'get', racing)
    done = materialise(store, stored.manifest, workspace, lambda e: e.path.endswith('.npy'))
    assert sorted(done.kept) == ['idea7/arrays/x.npy', 'idea7/copy.npy'] and done.written == 0
    assert (workspace / 'idea7' / 'copy.npy').read_text() == 'agent'


def test_each_path_of_a_shared_blob_gets_the_checked_bytes_whatever_an_agent_does_to_another(
    tmp_path: Path, workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Later copies were made from the first restored path, which an agent edited."""
    store = Store(tmp_path / 'store')
    stored = put_tree(store, workspace, ['idea7'])
    original = (workspace / 'idea7' / 'copy.npy').read_bytes()
    remove(workspace, stored.manifest)
    real = tree_module._copy
    written: list[Path] = []

    def editing(src: Path, path: Path, mode: int) -> None:
        real(src, path, mode)
        written.append(path)
        written[0].write_bytes(b'edited by an agent')  # the first restored path changes before the next copy

    monkeypatch.setattr(tree_module, '_copy', editing)
    materialise(store, stored.manifest, workspace, lambda e: e.path.endswith('.npy'))
    assert len(written) == 2 and written[1].read_bytes() == original


def test_a_file_where_the_manifest_has_a_directory_keeps_its_entries_and_the_rest_is_restored(
    tmp_path: Path, workspace: Path
) -> None:
    """NotADirectoryError ended the restore."""
    store = Store(tmp_path / 'store')
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
    stored = put_tree(Store(tmp_path / 'store'), workspace, ['idea7', 'idea7.log'])
    shutil.rmtree(workspace / 'idea7')
    (workspace / 'idea7').write_text('a file now')
    assert remove(workspace, stored.manifest) == Removed(1), 'idea7.log; the rest is not there to remove'
    assert (workspace / 'idea7').read_text() == 'a file now'
