"""Research sweeps end to end: a short backlog requests a sweep, which runs on the head and inserts ideas with SQL."""

import re
import shlex
import sys
from pathlib import Path

from git import Repo
from sqlalchemy import select

from piloop.agents.roles import PILOOP_HINT as PYTHON_HINT
from piloop.campaign.ideas import defined_by
from piloop.config import load_project_config
from piloop.daemon.runner import RunExecutor
from piloop.daemon.scheduler import Scheduler
from piloop.db.models import (
    Candidate,
    Comment,
    CommentKind,
    Idea,
    IdeaStatus,
    Project,
    TargetKind,
    Worker,
    WorkerKind,
    WorkerState,
)
from piloop.db.session import Database
from piloop.otel import Tracer
from tests.conftest import ScriptRuntime, factory
from tests.daemon.test_triggers import add_ideas, configure, finish_a_run, last_reason, occupy

PYTHON = shlex.quote(sys.executable)
FAKE_RESEARCHER = f"""#!/bin/sh
# a research harness stand-in: proposes through `idea add` (signed by its PILOOP_ACTOR), reports through SQL
sig=$(cat ../brief.md | sed -n "s/.*Sign what you write as '\\([a-z]*#[0-9]*\\)'.*/\\1/p")
project=$(cat ../brief.md | sed -n "s/.*for project#\\([0-9]*\\).*/\\1/p")
name=$(cat ../brief.md | sed -n "s/.*for project#[0-9]* (\\([a-z]*\\)).*/\\1/p")
cat ../brief.md > workspace/brief-seen.txt
git rev-parse HEAD > workspace/head.txt
echo note >> solve.txt
git commit -qam "a sweep may try things"
{PYTHON} -m piloop idea add "$name" 'target encoding' --hypothesis 'categoricals carry signal' --plan 'encode, refit' \\
  --priority 1 --base 1 --family encoding --from 'loss table workspace/research/losses.md'
{PYTHON} -m piloop sql "INSERT INTO comments (target_kind, target_id, author, kind, body) VALUES ('project', $project, '$sig', 'finding', 'proposed target encoding; rejected bigger trees (tried in evaluation#1)')"
"""


def test_research_sweep_inserts_ideas_signed_with_its_worker_id(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    configure(db, project, FAKE_RESEARCHER, research={'max_ideas': 2})
    executor = RunExecutor(db, Tracer(None), tmp_path, factory(runtime))
    scheduler = Scheduler(db, executor, Tracer(None), tmp_path, factory(runtime))

    assert scheduler.tick() == [1], 'a free worker slot, no idea for it: low water requests a research sweep'
    scheduler.wait(30)
    scheduler.stop(timeout=5)

    with db.session() as s:
        worker = s.get_one(Worker, 1)
        assert (worker.kind, worker.state, worker.error) == (
            WorkerKind.RESEARCH,
            WorkerState.EXITED,
            None,
        )
        [idea] = s.scalars(select(Idea)).all()
        assert (idea.created_by, idea.status, idea.priority, idea.base_candidate_id, idea.title) == (
            'research#1',
            IdeaStatus.READY,
            1,
            1,
            'target encoding',
        )
        assert (idea.family, idea.provenance) == ('encoding', 'loss table workspace/research/losses.md')
        [finding] = s.scalars(select(Comment).where(Comment.target_kind == TargetKind.PROJECT)).all()
        assert (finding.target_id, finding.author, finding.kind) == (project.id, 'research#1', CommentKind.FINDING)
    workspace = tmp_path / 'projects' / str(project.id) / 'workspace'
    brief = (workspace / 'brief-seen.txt').read_text()
    assert brief.startswith('# Research sweep') and '- Seeds to develop first: 0, listed in the research pack' in brief
    assert '- Ideas of your own after the seeds: propose 2 (the backlog holds 0 of ' in brief
    assert 'Fewer only when what is left duplicates queued work; name that work in your finding.' in brief
    assert re.search(
        r'^- Finished runs since the last keep: 0, of the \d+ that mark a stall \(two rounds of', brief, re.M
    )
    assert 'stalled, so one of your own ideas pivots' not in brief
    assert (workspace / 'head.txt').read_text().strip() == Repo(project.path).head.commit.hexsha, (
        'the sweep ran in a checkout of the baseline, the only candidate'
    )
    assert (
        '- Your cwd is a detached checkout of candidate#1, the head (score=1.0): read and run the code, try'
        ' things. Nothing you commit here is scored or kept; it goes with the checkout when you end.' in brief
    )
    assert (
        not (tmp_path / 'workers' / '1' / 'worktree').exists()
        and Repo(project.path).git.worktree('list').count('\n') == 0
    ), 'the worktree is gone with the sweep'
    with db.session() as s:
        assert s.scalars(select(Candidate)).all() == [s.get_one(Candidate, 1)], "the sweep's commit is not scored"
    assert re.search(r'^- Sweep started at: \d{4}-\d\d-\d\d \d\d:\d\d:\d\d \(UTC', brief, re.MULTILINE), (
        'the sweep needs its start time to query for ideas concurrent sweeps inserted after the history was read'
    )
    tail = brief[brief.index('- Seeds to develop first: 0') :]
    assert [line.split('|')[1].strip() for line in tail.splitlines() if line.startswith('| #')] == ['#1']
    assert '## Research pack: metrics' in tail and '## History: kept lineage' in tail
    assert tail.rstrip().endswith('## Research pack: open escalations (unacknowledged, newest first)\n- None.'), (
        'the sweep reads the research pack from its brief instead of querying for it'
    )
    assert '## History: ideas (0 of 0)' in tail and '## History: kept lineage' in tail
    assert brief == (tmp_path / 'workers' / '1' / 'brief.md').read_text()
    assert (tmp_path / 'workers' / '1' / 'transcript.txt').is_file()


FAKE_DEVELOPER = f"""#!/bin/sh
# a research harness stand-in on a seed sweep: develops the oldest seed, rejects the other with a note
cat ../brief.md > workspace/brief-seen.txt
{PYTHON} -m piloop idea develop 1 --hypothesis 'a deeper tree fits the interactions' --evidence 'loss table l.md' \\
  --check 'depth 6 on the dev split; stop below +0.01' --metric score --effect '+0.02: half the interaction loss' \\
  --family trees
{PYTHON} -m piloop sql "UPDATE ideas SET status = 'rejected' WHERE id = 2"
"""


def test_a_seed_sweep_develops_one_seed_and_rejects_the_other_however_full_the_backlog(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A seed starts a research sweep even when the backlog is at its target.

    The brief lists the seeds and asks for no new ideas. The developed seed is ready with its definition, and the
    sweep counts as one that defined an idea.
    """
    configure(db, project, FAKE_DEVELOPER, worker={'max_concurrent': 1}, research={'buffer': 0, 'max_ideas': 2})
    with db.session() as s:
        for title, by, pointers in (
            ('deeper trees', 'orchestrator', 'run#3'),
            ('trees, deeper', 'watchdog#8', 'run#4'),
        ):
            s.add(
                Idea(
                    track='score',
                    project_id=project.id,
                    title=title,
                    hypothesis=title,
                    status=IdeaStatus.SEED,
                    created_by=by,
                    provenance=pointers,
                    base_candidate_id=1 if by.startswith('watchdog') else None,
                )
            )
        s.commit()
    occupy(db, project)
    add_ideas(db, project, 1)
    finish_a_run(db, project, 'tried once')
    finish_a_run(db, project, 'tried twice')
    executor = RunExecutor(db, Tracer(None), tmp_path, factory(runtime))
    scheduler = Scheduler(db, executor, Tracer(None), tmp_path, factory(runtime))

    assert scheduler.tick() == [1], 'the one slot is busy and the backlog is at its target, but two seeds wait'
    assert last_reason(db) == '2 new seeds to develop'
    scheduler.wait(30)
    scheduler.stop(timeout=5)

    brief = (tmp_path / 'workers' / '1' / 'brief.md').read_text()
    assert '- Seeds to develop first: 2, listed in the research pack oldest first.' in brief
    assert '- Ideas of your own after the seeds: propose 0 (the backlog holds 1 of 1 runnable ideas' in brief
    assert (
        '- Finished runs since the last keep: 2, of the 2 that mark a stall (two rounds of [roles.worker]'
        ' max_concurrent = 1): stalled, so one of your own ideas pivots (section 3).' in brief
    )
    assert '- idea#1 by orchestrator: deeper trees — pointers: run#3' in brief
    assert '- idea#2 by watchdog#8 on candidate#1: trees, deeper — pointers: run#4' in brief
    assert f'{PYTHON_HINT} idea develop <seed id>' in brief
    with db.session() as s:
        developed, rejected = s.get_one(Idea, 1), s.get_one(Idea, 2)
        assert (developed.status, developed.created_by, developed.family) == (IdeaStatus.READY, 'orchestrator', 'trees')
        assert developed.hypothesis == 'a deeper tree fits the interactions\n\nSeed (orchestrator): deeper trees'
        assert developed.plan == (
            'First check, stop if it fails: depth 6 on the dev split; stop below +0.01\n'
            'Success: score; expected effect: +0.02: half the interaction loss'
        )
        assert developed.provenance == 'loss table l.md; seed pointers: run#3'
        assert rejected.status is IdeaStatus.REJECTED
        assert defined_by(s, project.id) >= {'research#1'}, 'the sweep defined an idea: no back-off'


def test_with_several_tracks_research_reads_them_and_which_are_short_of_their_share(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    configure(db, project, '#!/bin/sh\ncat ../brief.md > workspace/brief-seen.txt\n', research={'max_ideas': 1})
    with db.session() as s:
        tracks = {
            'metrics': [
                {'name': 'score', 'direction': 'higher', 'role': 'primary', 'track': 'accuracy', 'margin': 1},
                {'name': 'size', 'direction': 'lower', 'role': 'primary', 'track': 'cost', 'margin': 5},
            ],
            'tracks': {'accuracy': {'share': 0.5}, 'cost': {'share': 0.5}},
            'roles': {'research': {'max_ideas': 1, 'timeout': 20}, 'worker': {'max_concurrent': 2}},
        }
        s.get_one(Project, project.id).config = load_project_config(Path(project.path), **tracks).model_dump(
            mode='json'
        )
        s.commit()
    executor = RunExecutor(db, Tracer(None), tmp_path, factory(runtime))
    scheduler = Scheduler(db, executor, Tracer(None), tmp_path, factory(runtime))
    assert scheduler.tick() == [1]
    scheduler.wait(30)
    scheduler.stop(timeout=5)
    brief = (tmp_path / 'projects' / str(project.id) / 'workspace' / 'brief-seen.txt').read_text()
    assert '- Your cwd is a detached checkout of candidate#1, the head (score=1.0, size=5.0)' in brief
    assert (
        '- Tracks short of their share of the backlog: accuracy 2 (0 of 2 runnable), cost 2 (0 of 2 runnable).'
    ) in brief
    assert '- Tracks: accuracy (primary score, higher is better; share 0.5); cost (primary size, lower' in brief
    assert '### Tracks\n- accuracy: score 1 at the head (+0 on the start); 0 trunk keeps;' in brief
