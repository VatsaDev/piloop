"""The control commands: thin wrappers that print what the operation did, or exit with why it refused."""

from pathlib import Path

import pytest
from rich.console import Console

from piloop.campaign import project as project_module
from piloop.cli import common
from piloop.settings import Settings
from tests.cli.test_cli import research_files, run_cli


@pytest.fixture(autouse=True)
def isolated_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Console:
    monkeypatch.setattr(common, 'settings', Settings(home=tmp_path, uv_tool_dir=tmp_path / 'uv-tools'))
    monkeypatch.setattr(project_module, 'settings', common.settings)
    console = Console(record=True, width=200, force_terminal=False)
    monkeypatch.setattr(common, 'console', console)
    return console


@pytest.fixture
def out(isolated_home: Console):
    def read() -> str:
        return isolated_home.export_text(clear=True)

    return read


def test_pause_and_resume_hold_the_project_say_so_in_status_and_refuse_twice(project_path: Path, out) -> None:
    run_cli(['init', str(project_path)])
    out()
    run_cli(['pause', 'demo', '--reason', 'evaluator v2 lands at 14:00'])
    assert out().strip() == (
        'project demo paused (launches): evaluator v2 lands at 14:00; running work goes on;'
        ' `piloop resume demo` ends it'
    )
    run_cli(['status', 'demo'])
    assert '- Campaign: active, paused (launches): evaluator v2 lands at 14:00. Progress:' in out()
    with pytest.raises(SystemExit, match=r'project demo is paused already \(launches\)'):
        run_cli(['pause', 'demo', '--reason', 'again'])
    run_cli(['pause', 'demo', '--evals', '--reason', 'the box is busy'])
    assert out().startswith('project demo paused (launches and evaluations): the box is busy;')
    run_cli(['resume', 'demo'])
    assert out().strip() == 'project demo resumed; it held all'
    with pytest.raises(SystemExit, match='project demo is not paused'):
        run_cli(['resume', 'demo'])
    with pytest.raises(SystemExit, match='a reason is required'):
        run_cli(['pause', 'demo', '--reason', ''])


def test_a_sweep_files_its_ideas_for_review_while_the_project_is_paused(
    project_path: Path, tmp_path: Path, out, monkeypatch: pytest.MonkeyPatch
) -> None:
    research_files(project_path, 3)
    run_cli(['init', str(project_path)])
    run_cli(['idea', 'seed', 'demo', 'a hunch', '--from', 'run#2'])
    run_cli(['pause', 'demo', '--reason', 'review what research files'])
    out()

    def as_(actor: str, *argv: str) -> None:
        monkeypatch.setattr(common, 'settings', Settings(home=tmp_path, uv_tool_dir=tmp_path / 'uv-tools', actor=actor))
        run_cli(list(argv))

    as_('research#8', 'idea', 'add', 'demo', 'during the pause')
    define = ['--hypothesis', 'h', '--evidence', 'e', '--check', 'c', '--metric', 'score', '--effect', '+1']
    as_('research#8', 'idea', 'develop', '1', *define)
    as_('orchestrator', 'idea', 'add', 'demo', 'the orchestrator paused it')
    assert out().splitlines() == [
        'idea#2 proposed p2: during the pause (project demo is paused: it waits for a review)',
        'idea#1 proposed p2: a hunch (project demo is paused: it waits for a review)',
        'idea#3 ready p2: the orchestrator paused it',
    ]
