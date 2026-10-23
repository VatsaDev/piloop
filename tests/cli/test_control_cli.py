"""The control commands: thin wrappers that print what the operation did, or exit with why it refused."""

import json
import re
from contextlib import closing
from pathlib import Path

import pytest
from rich.console import Console
from sqlalchemy import select

from piloop.actor import System
from piloop.campaign import campaigns as campaign_module
from piloop.cli import common
from piloop.db.events import has
from piloop.db.models import Campaign, Candidate, Event, Idea, IdeaLink, IdeaStatus, Measurement, Run, Verdict
from piloop.db.session import Database
from piloop.scoring.ledger import RejudgeRequested
from piloop.settings import Settings
from tests.cli.test_cli import research_files, run_cli
from tests.conftest import at_head


@pytest.fixture(autouse=True)
def isolated_home(tmp_path: Path, schema: Path, monkeypatch: pytest.MonkeyPatch) -> Console:
    at_head(schema, tmp_path / 'piloop.db')
    monkeypatch.setattr(common, 'settings', Settings(home=tmp_path, uv_tool_dir=tmp_path / 'uv-tools'))
    monkeypatch.setattr(campaign_module, 'settings', common.settings)
    console = Console(record=True, width=200, force_terminal=False)
    monkeypatch.setattr(common, 'console', console)
    return console


@pytest.fixture
def out(isolated_home: Console):
    def read() -> str:
        return isolated_home.export_text(clear=True)

    return read


def database() -> closing[Database]:
    return closing(Database(common.settings.database_url, actor=System.HUMAN))


def test_pause_and_resume_hold_the_campaign_say_so_in_status_and_refuse_twice(campaign_path: Path, out) -> None:
    run_cli(['init', str(campaign_path)])
    out()
    run_cli(['pause', 'demo', '--reason', 'evaluator v2 lands at 14:00'])
    assert out().strip() == (
        'campaign demo paused (launches): evaluator v2 lands at 14:00; running work goes on;'
        ' `piloop resume demo` ends it'
    )
    run_cli(['status', 'demo'])
    assert '- Campaign: active, paused (launches): evaluator v2 lands at 14:00. Progress:' in out()
    with pytest.raises(SystemExit, match=r'campaign demo is paused already \(launches\)'):
        run_cli(['pause', 'demo', '--reason', 'again'])
    run_cli(['pause', 'demo', '--evals', '--reason', 'the box is busy'])
    assert out().startswith('campaign demo paused (launches and evaluations): the box is busy;')
    run_cli(['resume', 'demo'])
    assert out().strip() == 'campaign demo resumed; it held all'
    with pytest.raises(SystemExit, match='campaign demo is not paused'):
        run_cli(['resume', 'demo'])
    with pytest.raises(SystemExit, match='a reason is required'):
        run_cli(['pause', 'demo', '--reason', ''])


def test_a_sweep_files_its_ideas_for_review_while_the_campaign_is_paused(
    campaign_path: Path, tmp_path: Path, out, monkeypatch: pytest.MonkeyPatch
) -> None:
    research_files(campaign_path, 3)
    run_cli(['init', str(campaign_path)])
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
        'idea#2 proposed p2: during the pause (campaign demo is paused: it waits for a review)',
        'idea#1 proposed p2: a hunch (campaign demo is paused: it waits for a review)',
        'idea#3 ready p2: the orchestrator paused it',
    ]


def test_candidate_verdict_overrides_with_the_reason_and_refuses_what_is_not_a_judgement(
    campaign_path: Path, out
) -> None:
    run_cli(['init', str(campaign_path)])
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


def crashed(campaign_path: Path) -> int:
    """A daemon ``crash`` of a run of a new idea, as candidate 2."""
    run_cli(['init', str(campaign_path)])
    run_cli(['idea', 'add', 'demo', 'tried'])
    with database() as db, db.session() as s:
        run = Run(idea_id=1, attempt=1, base_candidate_id=1, harness='script', model='m')
        s.add(run)
        s.flush()
        c = Candidate(
            campaign_id=1,
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
    campaign_path: Path, tmp_path: Path, out
) -> None:
    c = crashed(campaign_path)
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
        [ask] = s.scalars(select(Event).where(has(RejudgeRequested))).all()
        assert (ask.actor, ask.target_id) == ('human', c)
    report.write_text('{"metrics": {"score": "high"}}')
    with pytest.raises(SystemExit, match=re.escape('the report is not {"metrics": {…}, "seconds": …, "error": …}')):
        run_cli(['measurement', 'add', str(c), '--report', str(report)])
    report.write_text('{"metrics": {"score": 1}}')
    with pytest.raises(SystemExit, match='the report has no size'):
        run_cli(['measurement', 'add', str(c), '--report', str(report)])


def test_idea_status_link_unlink_base_and_track_change_an_idea_and_say_what_they_did(campaign_path: Path, out) -> None:
    run_cli(['init', str(campaign_path)])
    run_cli(['idea', 'add', 'demo', 'first'])
    run_cli(['idea', 'add', 'demo', 'second'])
    out()
    run_cli(['idea', 'link', '2', 'after', '1', '--reason', 'first lands the parser'])
    run_cli(['idea', 'status', '1', 'parked', '--reason', 'wait for evaluator v2'])
    run_cli(['idea', 'unlink', '2', 'after', '1', '--reason', 'idea#1 parked'])
    run_cli(['idea', 'base', '2', '1', '--reason', 'from the baseline'])
    run_cli(['idea', 'base', '2', 'head', '--reason', 'follow the head'])
    assert out().splitlines() == [
        'idea#2 after idea#1',
        'idea#1 ready → parked: wait for evaluator v2',
        'idea#2 no longer after idea#1',
        'idea#2 starts from candidate#1',
        'idea#2 starts from the head',
    ]
    with database() as db, db.session() as s:
        assert s.get_one(Idea, 1).status is IdeaStatus.PARKED
        assert s.scalars(select(IdeaLink)).all() == []
        assert s.get_one(Idea, 2).base_candidate_id is None
        kinds = [e.kind for e in s.scalars(select(Event).where(Event.target_id == 2).order_by(Event.id))]
        assert [k for k in kinds if k.startswith('idea.')] == [
            'idea.created',
            'idea.linked',
            'idea.unlinked',
            'idea.base_candidate_id',
            'idea.base_candidate_id',
        ]
    with pytest.raises(SystemExit, match='idea#2 is on track score already'):
        run_cli(['idea', 'track', '2', 'score', '--reason', 'no-op'])
    with pytest.raises(SystemExit, match="there is no track 'speed'"):
        run_cli(['idea', 'track', '2', 'speed', '--reason', 'faster'])
    run_cli(['idea', 'link', '2', 'after', '1', '--reason', 'first lands the parser'])
    with pytest.raises(SystemExit, match='a cycle: idea#2 already links after idea#1'):
        run_cli(['idea', 'link', '1', 'after', '2', '--reason', 'the other way round'])
    with pytest.raises(SystemExit, match="there is no track 'speed'"):
        run_cli(['idea', 'add', 'demo', 'faster', '--track', 'speed'])
    with pytest.raises(SystemExit, match='no candidate#9 in campaign demo'):
        run_cli(['idea', 'seed', 'demo', 'a hunch', '--from', 'run#1', '--base', '9'])
    with pytest.raises(SystemExit, match='scheduled is not set by hand'):
        run_cli(['idea', 'status', '2', 'scheduled', '--reason', 'why'])


def test_idea_track_moves_an_idea_to_another_track(campaign_path: Path, out) -> None:
    toml = campaign_path / 'piloop.toml'
    text = toml.read_text().replace('role = "primary"\n', 'role = "primary"\nmargin = 1\n')
    text = text.replace('direction = "lower"\n', 'direction = "lower"\nrole = "primary"\nmargin = 1\n')
    toml.write_text(text + '\n[tracks.score]\nshare = 0.5\n\n[tracks.size]\nshare = 0.5\n')
    run_cli(['init', str(campaign_path)])
    run_cli(['idea', 'add', 'demo', 'smaller', '--track', 'score'])
    out()
    run_cli(['idea', 'track', '1', 'size', '--reason', 'it shrinks the output'])
    assert out().strip() == 'idea#1 track score → size'


def test_a_seed_developed_with_a_link_that_closes_a_cycle_is_refused_whole(campaign_path: Path, out) -> None:
    """Every way to link ideas is validated: here a seed that an idea runs after would also run after that idea."""
    run_cli(['init', str(campaign_path)])
    run_cli(['idea', 'seed', 'demo', 'a hunch', '--from', 'run#1'])
    run_cli(['idea', 'add', 'demo', 'after the seed', '--after', '1'])
    define = ['--hypothesis', 'h', '--evidence', 'e', '--check', 'c', '--metric', 'score', '--effect', '+1']
    with pytest.raises(SystemExit, match='a cycle: idea#2 already links after idea#1'):
        run_cli(['idea', 'develop', '1', *define, '--after', '2'])
    with closing(Database(common.settings.database_url, actor=System.HUMAN)) as db, db.session() as s:
        assert s.get_one(Idea, 1).status is IdeaStatus.SEED, 'nothing was written'
    with pytest.raises(SystemExit, match='no idea#9'):
        run_cli(['idea', 'add', 'demo', 'after nothing', '--after', '9'])


def test_campaign_parent_puts_a_campaign_under_another_and_campaign_tree_shows_them(campaign_path: Path, out) -> None:
    run_cli(['init', str(campaign_path)])
    with database() as db, db.session() as s:
        s.add_all(Campaign(name=name, path=f'/{name}', config={}) for name in ('umbrella', 'side'))
        s.commit()
    out()
    run_cli(['campaign', 'parent', 'demo', 'umbrella'])
    run_cli(['campaign', 'parent', 'side', 'demo'])
    assert out().splitlines() == ['demo #1 is under umbrella #2', 'side #3 is under demo #1']
    with pytest.raises(SystemExit, match='^umbrella cannot be under side: .*its descendants'):
        run_cli(['campaign', 'parent', 'umbrella', 'side'])
    with pytest.raises(SystemExit, match='^name the parent, or --clear$'):
        run_cli(['campaign', 'parent', 'side'])
    with pytest.raises(SystemExit, match='^name the parent or --clear, not both$'):
        run_cli(['campaign', 'parent', 'side', 'demo', '--clear'])
    run_cli(['campaign', 'tree'])
    assert out().splitlines() == [
        'umbrella #2  optimization  active',
        '  demo #1  optimization  active',
        '    side #3  optimization  active',
    ]
    run_cli(['status', 'demo'])
    assert f' demo #1  {campaign_path}  under umbrella #2 ' in out()
    run_cli(['campaign', 'parent', 'side', '--clear'])
    assert out().strip() == 'side #3 is a root'
