"""The research pack's workspace listing comes from a bounded walk, never from the whole workspace."""

import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from piloop.db.models import Project
from piloop.db.session import Database
from piloop.reports import pack as pack_module
from piloop.reports.pack import _file_lines, _recent
from tests.reports.test_pack import JAN_1, render, write


def test_the_walk_opens_the_most_recently_changed_directories_first_and_stops_at_its_budget(tmp_path: Path) -> None:
    workspace = tmp_path / 'workspace'
    for n in range(5):
        write(workspace / f'idea{n}' / 'notes.md', '# n\n', n)
        os.utime(workspace / f'idea{n}', (JAN_1 + 60 * n, JAN_1 + 60 * n))
    (workspace / 'idea0' / 'dangling').symlink_to('gone')
    os.utime(workspace / 'idea0', (JAN_1, JAN_1))
    os.utime(workspace, (JAN_1, JAN_1))
    files, cut = _recent(workspace, 100)
    assert len(files) == 5 and not cut, 'a dangling link is no file'
    files, cut = _recent(workspace, 7)
    assert cut and [p.parent.name for p in files] == ['idea4', 'idea3'], 'five directories, then the two newest'


def test_a_directory_or_file_archived_during_the_walk_is_skipped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = tmp_path / 'workspace'
    write(workspace / 'idea1' / 'a.md', '# a\n', 1)
    write(workspace / 'idea2' / 'b.md', '# b\n', 2)
    real = os.scandir

    def vanishing(path: str) -> Iterator[os.DirEntry[str]]:
        if path.endswith('idea1'):
            raise FileNotFoundError(path)
        return real(path)

    monkeypatch.setattr(pack_module.os, 'scandir', vanishing)
    files, _ = _recent(workspace, 100)
    assert [p.name for p in files] == ['b.md']
    lines = _file_lines({'gone': workspace / 'gone.md', 'b': workspace / 'idea2' / 'b.md'}, 10)
    assert [str(line).split(' (')[0] for line in lines] == ['- b']


def test_a_directory_archived_between_the_listing_and_its_stat_is_skipped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = tmp_path / 'workspace'
    write(workspace / 'idea1' / 'a.md', '# a\n', 1)
    write(workspace / 'b.md', '# b\n', 2)
    real = os.scandir

    def racing(path: str) -> Iterator[os.DirEntry[str]]:
        entries = list(real(path))
        if (workspace / 'idea1').exists():  # os.scandir is patched: no rmtree
            os.remove(workspace / 'idea1' / 'a.md')
            os.rmdir(workspace / 'idea1')
        return iter(entries)

    monkeypatch.setattr(pack_module.os, 'scandir', racing)
    files, _ = _recent(workspace, 100)
    assert [p.name for p in files] == ['b.md']


def test_the_pack_says_when_its_listing_was_cut(
    db: Database, project: Project, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = tmp_path / 'workspace'
    for n in range(3):
        write(workspace / f'f{n}.md', '# f\n', n)
    monkeypatch.setattr(pack_module, 'WALK_ENTRIES', 2)
    text = render(db, project, workspace)
    assert '- (listed from the 2 entries of the most recently changed directories)' in text
