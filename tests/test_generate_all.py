"""scripts/generate_all.py: the modes, the stale-file check and its diff, and that every generated file is current.

The diff tests follow the tests of ``generated_file_diff`` in ruff's ``crates/ruff_dev/src/generate_all.rs`` (MIT).
"""

from pathlib import Path

import pytest

from scripts import generate_all
from scripts.generate import REGENERATE_ALL_COMMAND, ROOT, Mode, emit, generated_file_diff

WARNING = f'{{/* WARNING: This file is auto-generated ({REGENERATE_ALL_COMMAND}). Update '


def test_short_diff() -> None:
    assert generated_file_diff('unchanged\nold\n', 'unchanged\nnew\n') == '@@ -1,2 +1,2 @@\n unchanged\n-old\n+new\n'


@pytest.mark.parametrize('line_count', [98, 99])
def test_diff_within_limit(line_count: int) -> None:
    diff = generated_file_diff('', 'new\n' * line_count)
    assert diff == f'@@ -0,0 +1,{line_count} @@\n' + '+new\n' * line_count


@pytest.mark.parametrize('line_count', [100, 200])
def test_truncated_diff(line_count: int) -> None:
    diff = generated_file_diff('', 'new\n' * line_count)
    assert diff == f'@@ -0,0 +1,{line_count} @@\n' + '+new\n' * 99 + '… diff truncated after 100 lines\n'


def test_diff_after_unchanged_prefix() -> None:
    prefix = 'unchanged\n' * 150
    diff = generated_file_diff(f'{prefix}old\n', f'{prefix}new\n')
    assert diff == '@@ -148,4 +148,4 @@\n unchanged\n unchanged\n unchanged\n-old\n+new\n'


@pytest.fixture
def target(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr('scripts.generate.ROOT', tmp_path)
    return tmp_path / 'docs' / 'snippets' / 'generated' / 'x.mdx'


def test_write_creates_then_reports_up_to_date(target: Path, capsys: pytest.CaptureFixture[str]) -> None:
    emit(target, 'a\n', Mode.WRITE)
    emit(target, 'a\n', Mode.WRITE)
    assert target.read_text() == 'a\n'
    out = capsys.readouterr().out
    assert out == 'Updating: docs/snippets/generated/x.mdx\nUp-to-date: docs/snippets/generated/x.mdx\n'


def test_check_fails_on_a_stale_file_with_its_diff(target: Path) -> None:
    target.parent.mkdir(parents=True)
    target.write_text('old\n')
    with pytest.raises(SystemExit, match=r'(?s)`docs/snippets/generated/x\.mdx` changed, please run .*-old\n\+new'):
        emit(target, 'new\n', Mode.CHECK)
    assert target.read_text() == 'old\n'


def test_check_fails_on_a_missing_file(target: Path) -> None:
    with pytest.raises(SystemExit, match='`docs/snippets/generated/x.mdx` not found, please run'):
        emit(target, 'new\n', Mode.CHECK)
    assert not target.exists()


def test_dry_run_prints_and_writes_nothing(target: Path, capsys: pytest.CaptureFixture[str]) -> None:
    emit(target, 'new', Mode.DRY_RUN)
    assert capsys.readouterr().out == 'new\n'
    assert not target.exists()


def test_every_generated_file_is_current(capsys: pytest.CaptureFixture[str]) -> None:
    generate_all.main(Mode.CHECK)
    out = capsys.readouterr().out.splitlines()
    assert len(out) == len(generate_all.GENERATORS)
    assert all(line.startswith('Up-to-date: ') for line in out)


def test_each_generated_mdx_file_starts_with_the_warning() -> None:
    """After the front matter of a page, if any, the first line names the command and the source to change."""
    for generator in generate_all.GENERATORS:
        if generator.PATH.suffix == '.mdx':
            text = generator.generate()
            body = text.split('\n---\n', 1)[1].lstrip('\n') if text.startswith('---\n') else text
            assert body.startswith(WARNING) and body.splitlines()[0].endswith(' */}'), generator.PATH.relative_to(ROOT)
