"""The control commands: thin wrappers that print what the operation did, or exit with why it refused."""

import json
import re
from contextlib import closing
from pathlib import Path

import pytest
from rich.console import Console
from sqlalchemy import select

from piloop.campaign import project as project_module
from piloop.cli import common
from piloop.db.models import Candidate, Event, Measurement, Run, Verdict
from piloop.db.session import Database
from piloop.scoring.ledger import REJUDGE_REQUESTED
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


def database() -> closing[Database]:
    return closing(Database(common.settings.database_url, actor='test'))


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


def test_candidate_verdict_overrides_with_the_reason_and_refuses_what_is_not_a_judgement(
    project_path: Path, out
) -> None:
    run_cli(['init', str(project_path)])
    out()
    run_cli(['candidate', 'verdict', '1', 'interesting', '--reason', 'a lucky seed'])
    assert out().strip() == 'candidate#1 keep → interesting: a lucky seed'
    with database() as db, db.session() as s:
        c = s.get_one(Candidate, 1)
        assert (c.verdict, c.verdict_by, c.verdict_reason) == (Verdict.INTERESTING, 'human', 'a lucky seed')
    with pytest.raises(SystemExit, match='pending is not a judgement'):
        run_cli(['candidate', 'verdict', '1', 'pending', '--reason', 'why'])
    with pytest.raises(SystemExit, match='no candidate#9'):
        run_cli(['candidate', 'verdict', '9', 'keep', '--reason', 'why'])


def crashed(project_path: Path) -> int:
    """A daemon ``crash`` of a run of a new idea, as candidate 2."""
    run_cli(['init', str(project_path)])
    run_cli(['idea', 'add', 'demo', 'tried'])
    with database() as db, db.session() as s:
        run = Run(idea_id=1, attempt=1, base_candidate_id=1, harness='script', model='m')
        s.add(run)
        s.flush()
        c = Candidate(
            project_id=1,
            run_id=run.id,
            parent_id=1,
            tree_sha='t',
            commit_sha='c',
            verdict=Verdict.CRASH,
            verdict_by='daemon',
        )
        s.add(c)
        s.commit()
        return c.id


def test_measurement_add_records_a_report_and_asks_for_a_judgement_again_unless_the_verdict_stays(
    project_path: Path, tmp_path: Path, out
) -> None:
    c = crashed(project_path)
    out()
    report = tmp_path / 'report.json'
    report.write_text(json.dumps({'metrics': {'score': 3, 'size': 7.5}, 'seconds': 2500}))
    run_cli(['measurement', 'add', str(c), '--report', str(report)])
    assert out().strip() == f'measurement#2 of candidate#{c}: the daemon judges it again with all its measurements'
    report.write_text('{"metrics": {"score": 1, "size": 4}}')
    run_cli(['measurement', 'add', '1', '--report', str(report)])
    assert (
        out().strip()
        == 'measurement#3 of candidate#1: its keep stays; the runs that start from it must beat its new mean'
    )
    with database() as db, db.session() as s:
        rows = s.scalars(select(Measurement).order_by(Measurement.id)).all()
        assert [(m.candidate_id, m.metrics, m.seconds) for m in rows[1:]] == [
            (c, {'score': 3.0, 'size': 7.5}, 2500.0),
            (1, {'score': 1.0, 'size': 4.0}, None),
        ]
        [ask] = s.scalars(select(Event).where(Event.kind == REJUDGE_REQUESTED)).all()
        assert (ask.actor, ask.target_id) == ('human', c)
    report.write_text('{"metrics": {"score": "high"}}')
    with pytest.raises(SystemExit, match=re.escape('the report is not {"metrics": {…}, "seconds": …, "error": …}')):
        run_cli(['measurement', 'add', str(c), '--report', str(report)])
    report.write_text('{"metrics": {"score": 1}}')
    with pytest.raises(SystemExit, match='the report has no size'):
        run_cli(['measurement', 'add', str(c), '--report', str(report)])
