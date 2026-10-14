"""The scout role: its timer, its brief and signature, its library and its watch list."""

import shlex
import sys
import time
from datetime import timedelta
from pathlib import Path

from sqlalchemy import select

from piloop.agents.scout import MIN_WINDOW, WATCH_RUNS, WINDOW, sources, watch_list, window
from piloop.campaign.ideas import yields
from piloop.config import ScoutRole, load_project_config
from piloop.db.models import (
    Candidate,
    Comment,
    CommentKind,
    Idea,
    IdeaStatus,
    Project,
    Run,
    RunStatus,
    Technique,
    Verdict,
    Worker,
    WorkerKind,
    WorkerState,
    utcnow,
)
from piloop.db.session import Database
from piloop.reports.history import history
from piloop.reports.overview import overview
from tests.conftest import ScriptRuntime, write_toml
from tests.daemon.test_triggers import add_ideas, backdate, configure, occupy, scheduler, workers

PYTHON = shlex.quote(sys.executable)
FAKE_SCOUT = f"""#!/bin/sh
# a scout stand-in: signs as its PILOOP_ACTOR, files one seed and one library technique, may not add an idea
project=$(cat ../brief.md | sed -n "s/.*for project#\\([0-9]*\\).*/\\1/p")
name=$(cat ../brief.md | sed -n "s/.*for project#[0-9]* (\\([a-z]*\\)).*/\\1/p")
cat ../brief.md > workspace/brief-seen.txt
printf %s "$PILOOP_ACTOR" > workspace/actor.txt
{PYTHON} -m piloop idea seed "$name" 'CTC beam search with a digit lexicon' \\
  --from 'https://x.com/someone/status/1; targets digit confusions, 41 of 212 errors; -18% CER in a table'
{PYTHON} -m piloop sql "INSERT INTO techniques (project_id, title, mechanism, url, created_by) VALUES ($project, 'value prior', 'amount grammar, +2 pts', 'https://arxiv.org/abs/2610.00001', '$PILOOP_ACTOR')"
{PYTHON} -m piloop idea add "$name" 'a defined idea' --hypothesis h 2> workspace/refused.txt
{PYTHON} -m piloop sql "INSERT INTO comments (target_kind, target_id, author, kind, priority, body) VALUES ('project', $project, '$PILOOP_ACTOR', 'finding', 3, 'Problem statement: digits')"
"""


def scout_due(db: Database, project: Project, seconds: float) -> None:
    with db.session() as s:
        s.get_one(Project, project.id).created_at = utcnow() - timedelta(seconds=seconds)
        s.commit()


def test_the_scout_is_off_until_it_has_an_interval(project: Project) -> None:
    scout = load_project_config(Path(project.path)).roles.scout
    assert (scout.interval, scout.seeds, scout.files) == (0, ScoutRole().seeds, 'seeds')
    on = load_project_config(Path(project.path), roles={'scout': {'interval': 3600}}).roles.scout
    assert (on.interval, [t.kind for t in on.triggers]) == (3600, ['cron'])


def test_a_scout_sweep_fires_on_its_interval_as_a_row_of_its_own_beside_research(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    configure(db, project, research={'max_ideas': 0}, scout={'interval': 60})
    sched = scheduler(db, runtime, tmp_path)
    occupy(db, project)
    assert sched.tick() == [], 'registered a moment ago: not due'
    scout_due(db, project, 61)
    add_ideas(db, project, 1, by='watchdog#9', status=IdeaStatus.SEED)
    assert sorted(sched.tick()) == [1, 2], 'a seed sweep and a scout sweep, each in a slot of its own role'
    assert workers(db, WorkerKind.RESEARCH)[0][2] == '1 new seeds to develop'
    assert workers(db, WorkerKind.SCOUT)[0][2] == 'timer: 60s'
    sched.wait(10)
    assert sched.tick() == [], 'the last scout started a moment ago'
    backdate(db, 2, 61)
    assert sched.tick() == [3]
    sched.wait(10)
    sched.stop(timeout=5)


def test_a_scout_sweep_reads_its_context_signs_as_a_scout_and_files_only_seeds_and_techniques(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    configure(db, project, FAKE_SCOUT, research={'max_ideas': 0}, scout={'interval': 60, 'seeds': 2})
    occupy(db, project)
    scout_due(db, project, 61)
    sched = scheduler(db, runtime, tmp_path)
    assert sched.tick() == [1]
    sched.wait(30)
    sched.stop(timeout=5)

    workspace = tmp_path / 'projects' / str(project.id) / 'workspace'
    brief = (workspace / 'brief-seen.txt').read_text()
    assert brief.startswith('# Scout sweep\n')
    assert '# Research sweep' not in brief, 'the scout brief replaces the research brief'
    assert "- Sign what you write as 'scout#1'" in brief and '- You are worker#1, the scout for project#1' in brief
    assert '- Seeds you may file: at most 2; every other technique worth keeping goes to the library.' in brief
    assert f'- Search window: since {(utcnow() - WINDOW).strftime("%Y-%m-%d")}' in brief
    assert '## Scout: watch list (sources whose seeds research developed; query each directly)\n- Empty' in brief
    assert '## Scout: technique library (0 techniques, newest first)\n- Empty.' in brief
    assert 'INSERT INTO techniques (project_id, title, mechanism, url, created_by)' in brief
    assert '## Research pack: metrics' in brief, 'the scout reads the research pack like any sweep'
    assert (workspace / 'actor.txt').read_text() == 'scout#1'
    assert 'scout#1 files seeds, not ideas' in (workspace / 'refused.txt').read_text()
    with db.session() as s:
        assert s.get_one(Worker, 1).state is WorkerState.EXITED
        [seed] = s.scalars(select(Idea).where(Idea.status == IdeaStatus.SEED)).all()
        assert (seed.created_by, seed.title) == ('scout#1', 'CTC beam search with a digit lexicon')
        [technique] = s.scalars(select(Technique)).all()
        assert (technique.created_by, technique.title) == ('scout#1', 'value prior')
        [finding] = s.scalars(select(Comment).where(Comment.kind == CommentKind.FINDING)).all()
        assert (finding.author, finding.priority) == ('scout#1', 3)
        assert 'scout 1 ideas, 0 ran' in '; '.join(y.line() for y in yields(s, project.id))
        project_row = s.get_one(Project, project.id)
        cfg = load_project_config(Path(project.path))
        assert '0 research, 1 scout; 0 of 0 ended research sweeps defined no idea' in overview(
            s, project_row, cfg, utcnow(), lambda _: []
        )
        assert 'scout#1: Problem statement: digits' in history(s, project_row, cfg)


def test_sources_are_x_authors_and_github_repos() -> None:
    text = (
        'https://x.com/a_b/status/1 and https://twitter.com/a_b/status/2; https://github.com/o-1/r.x.git,'
        ' https://github.com/o-1/r.x/issues; https://arxiv.org/abs/2610.1 https://x.com/search?q=1'
    )
    assert sources(text) == ['@a_b', 'o-1/r.x']


def developed_seed(db: Database, project: Project, title: str, pointers: str, *, kept: bool = False) -> int:
    with db.session() as s:
        idea = Idea(
            track='score',
            project_id=project.id,
            title=title,
            hypothesis=title,
            provenance=pointers,
            created_by='scout#1',
            status=IdeaStatus.SEED,
        )
        s.add(idea)
        s.commit()
        idea.status = IdeaStatus.READY  # the change log records the develop
        s.commit()
        if kept:
            run = Run(
                idea_id=idea.id, attempt=1, base_candidate_id=1, harness='h', model='m', status=RunStatus.SUCCEEDED
            )
            s.add(run)
            s.flush()
            s.add(
                Candidate(
                    project_id=project.id,
                    run_id=run.id,
                    parent_id=1,
                    commit_sha='a' * 40,
                    tree_sha='b' * 40,
                    verdict=Verdict.KEEP,
                )
            )
            s.commit()
        return idea.id


def add_scouts(db: Database, project: Project, n: int) -> None:
    with db.session() as s:
        for _ in range(n):
            time.sleep(0.01)
            s.add(Worker(kind=WorkerKind.SCOUT, project_id=project.id, state=WorkerState.EXITED, started_at=utcnow()))
        s.commit()


def test_a_developed_source_joins_the_watch_list_and_ages_out_unless_it_kept(db: Database, project: Project) -> None:
    with db.session() as s:
        s.add(
            Idea(
                track='score',
                project_id=project.id,
                title='undeveloped',
                hypothesis='h',
                provenance='https://x.com/c/status/3',
                created_by='scout#1',
                status=IdeaStatus.SEED,
            )
        )
        s.commit()
    developed = developed_seed(db, project, 'developed', 'https://x.com/a/status/1 https://github.com/o/r')
    kept = developed_seed(db, project, 'kept', 'https://x.com/b/status/2', kept=True)
    with db.session() as s:
        assert [(w.source, w.ideas, w.kept) for w in watch_list(s, project.id, WorkerKind.SCOUT)] == [
            ('@b', [kept], True),
            ('@a', [developed], False),
            ('o/r', [developed], False),
        ]
        assert watch_list(s, project.id, WorkerKind.SCOUT)[1].line() == (
            f'- @a (developed, {WATCH_RUNS} more scout sweeps unless one pays off; idea#{developed})'
        )
        assert watch_list(s, project.id, WorkerKind.SCOUT)[0].line() == f'- @b (kept; idea#{kept})'
    add_scouts(db, project, WATCH_RUNS)
    with db.session() as s:
        assert [w.source for w in watch_list(s, project.id, WorkerKind.SCOUT)] == ['@b'], 'what never kept ages out'


def test_the_search_window_starts_at_the_previous_scout_at_least_a_week_back(db: Database, project: Project) -> None:
    now = utcnow()
    with db.session() as s:
        assert window(s, project.id, WorkerKind.SCOUT, 99, now) == now - WINDOW
    add_scouts(db, project, 1)
    with db.session() as s:
        assert window(s, project.id, WorkerKind.SCOUT, 99, now) == now - MIN_WINDOW, (
            'a scout an hour ago still looks a week back'
        )
        started = s.get_one(Worker, 1).started_at = now - timedelta(days=12)
        s.commit()
        assert window(s, project.id, WorkerKind.SCOUT, 99, now) == started


def test_a_scout_requested_by_hand_while_its_timer_is_off_files_the_default_number_of_seeds(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    write_toml(Path(project.path), '#!/bin/sh\ncat ../brief.md > workspace/brief-seen.txt\n')
    configure(db, project, '#!/bin/sh\ncat ../brief.md > workspace/brief-seen.txt\n', research={'max_ideas': 0})
    occupy(db, project)
    with db.session() as s:
        s.add(Worker(kind=WorkerKind.SCOUT, project_id=project.id, reason='by hand'))
        s.commit()
    sched = scheduler(db, runtime, tmp_path)
    assert sched.tick() == [1]
    sched.wait(30)
    sched.stop(timeout=5)
    brief = (tmp_path / 'projects' / str(project.id) / 'workspace' / 'brief-seen.txt').read_text()
    assert f'- Seeds you may file: at most {ScoutRole().seeds};' in brief
