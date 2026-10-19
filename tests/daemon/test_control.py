"""The daemon's side of the control operations: a pause holds the tick."""

from pathlib import Path

from git import Repo
from sqlalchemy import select

from piloop.campaign import ops
from piloop.config import load_project_config
from piloop.db.models import Comment, IdeaStatus, Project, Worker, WorkerState
from piloop.db.session import Database
from piloop.scoring.ledger import import_reference
from tests.conftest import ScriptRuntime
from tests.daemon.test_scheduler import add_ideas, make_scheduler, measured_by, request_watchdog, statuses


def pause(db: Database, *, evals: bool) -> None:
    with db.session() as s:
        ops.pause(s, 1, evals=evals, reason='wait for the new evaluator')
        s.commit()


def test_a_paused_project_launches_nothing_requests_nothing_and_escalates_nothing_until_resumed(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    sched = make_scheduler(db, runtime, tmp_path)
    add_ideas(db, project, 'more lines')
    watchdog = request_watchdog(db, project)
    pause(db, evals=False)
    assert sched.tick() == [] and sched.tick() == []
    assert statuses(db) == [IdeaStatus.READY], 'no run is queued'
    with db.session() as s:
        workers = s.scalars(select(Worker)).all()
        assert [(w.id, w.state) for w in workers] == [(watchdog, WorkerState.REQUESTED)], 'nothing launches or is asked'
        assert s.scalars(select(Comment)).all() == [], 'a pause raises no escalation'
        ops.resume(s, 1)
        s.commit()
    launched = sched.tick()
    assert watchdog in launched and len(launched) == 2, 'the request and the run of the idea'
    assert statuses(db) == [IdeaStatus.SCHEDULED]
    sched.stop(timeout=10)


def test_a_pause_still_measures_what_the_evaluator_needs_unless_it_holds_the_evaluations_too(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    repo = Repo(project.path)
    repo.git.checkout('-qb', 'elsewhere')
    (Path(project.path) / 'solve.txt').write_text('a\nb\nc\n')
    repo.git.commit('-qam', 'three lines')
    repo.git.checkout('-q', 'main')
    with db.session() as s:
        proj = s.get_one(Project, project.id)
        cfg = load_project_config(Path(project.path))
        reference = import_reference(s, proj, cfg, repo, 'elsewhere', summary=None, actor='orchestrator').id
        s.commit()
    sched = make_scheduler(db, runtime, tmp_path)
    pause(db, evals=True)
    sched.tick()
    sched.wait(30)
    assert measured_by(db, reference) == [], 'held'
    pause(db, evals=False)
    sched.tick()
    sched.wait(30)
    assert measured_by(db, reference) == [(1, 3.0, None)], 'a pause of the launches only still measures'
    sched.stop(timeout=5)
