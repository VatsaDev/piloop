import os
import threading
import time
from datetime import timedelta
from pathlib import Path

import pytest

from piloop.store import Blob, Changed, Store
from piloop.store.tree import Cancelled, materialise, prune, put_tree
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


def test_a_tree_is_stored_pruned_and_materialised_back_byte_for_byte(tmp_path: Path, workspace: Path) -> None:
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
    assert stored.size == 2 * 1006 + 10 + 5 and stored.skipped == []
    assert prune(workspace, stored) == 5
    assert tree(workspace) == {'NOTES.md': b'# Notes\n'} and not (workspace / 'idea7').exists()
    done = materialise(store, store.manifest(store.put_manifest(stored.manifest).hash), workspace)
    assert (done.written, done.bytes, done.same, done.kept) == (5, 2 * 1006 + 10 + 5, 0, [])
    assert tree(workspace) == before
    assert (workspace / 'idea7' / 'run.sh').stat().st_mode & 0o777 == 0o755
    again = materialise(store, stored.manifest, workspace)
    assert (again.written, again.same) == (0, 5), 'what is there already is left alone'


def test_materialise_keeps_what_differs_and_writes_only_what_is_wanted(tmp_path: Path, workspace: Path) -> None:
    store = Store(tmp_path / 'store')
    stored = put_tree(store, workspace, ['idea7'])
    prune(workspace, stored)
    (workspace / 'idea7').mkdir()
    (workspace / 'idea7' / 'run.sh').write_text('changed\n')
    (workspace / 'idea7' / 'latest').symlink_to('elsewhere')
    (workspace / 'idea7' / 'copy.npy').symlink_to('run.sh')
    done = materialise(store, stored.manifest, workspace, lambda e: not e.path.startswith('idea7/arrays'))
    assert sorted(done.kept) == ['idea7/copy.npy', 'idea7/latest', 'idea7/run.sh']
    assert done.written == 0 and not (workspace / 'idea7' / 'arrays').exists()
    assert (workspace / 'idea7' / 'run.sh').read_text() == 'changed\n'


def test_prune_leaves_what_changed_after_it_was_read(tmp_path: Path, workspace: Path) -> None:
    stored = put_tree(Store(tmp_path / 'store'), workspace, ['idea7'])
    (workspace / 'idea7' / 'run.sh').write_text('#!/bin/sh\necho more\n')
    (workspace / 'idea7' / 'arrays' / 'new.npy').write_bytes(b'new')
    (workspace / 'idea7' / 'copy.npy').unlink()
    assert prune(workspace, stored) == 2
    assert tree(workspace / 'idea7') == {'arrays/new.npy': b'new', 'run.sh': b'#!/bin/sh\necho more\n'}


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
    assert sorted(stored.skipped) == ['idea7/pipe', 'idea7/run.sh']
    prune(workspace, stored)
    assert sorted(p.name for p in (workspace / 'idea7').iterdir()) == ['pipe', 'run.sh']


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
        assert stored.skipped == ['idea7/arrays', 'idea7/run.sh']
        prune(workspace, stored)
        assert sorted(p.name for p in (workspace / 'idea7').iterdir()) == ['arrays', 'run.sh']
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
