"""The evaluation queue: one evaluation per tree, a cap on the evaluations that run, a fair order, and cancels."""

import threading
import time
from collections.abc import Callable
from pathlib import Path

from pytest_mock import MockerFixture
from sqlalchemy import delete, select

from piloop.daemon.runner import RunExecutor
from piloop.db.models import Candidate, Evaluation, Project, Run, RunStatus, Verdict
from piloop.db.session import Database
from piloop.otel import Tracer
from piloop.scoring import queue
from piloop.scoring.ledger import measured
from tests.conftest import ScriptRuntime, baseline, factory, write_script
from tests.daemon.test_runner import execute, make_run, set_config
from tests.daemon.test_scheduler import add_ideas, make_scheduler

HARNESS_OWN_LINE = '#!/bin/sh\nbasename "$(dirname "$PWD")" >> solve.txt\ngit commit -qam "own line"\n'
"""Each run appends its own id (``runs/<id>/worktree``), so each run makes a tree of its own."""
SCORE = 'echo METRIC score=$(wc -l < solve.txt); echo METRIC size=$(wc -c < solve.txt)'


def held_evaluate(tmp_path: Path, then: str = SCORE) -> list[str]:
    """An evaluate command that logs its commit in ``evaluated``, waits while ``hold`` exists, then runs ``then``."""
    log, hold = tmp_path / 'evaluated', tmp_path / 'hold'
    return ['sh', '-c', f'git rev-parse HEAD >> {log}; while [ -f {hold} ]; do sleep 0.05; done; {then}']


def until(check: Callable[[], bool], what: str) -> None:
    deadline = time.monotonic() + 20
    while not check():
        assert time.monotonic() < deadline, f'timed out waiting for {what}'
        time.sleep(0.02)


def rows(db: Database) -> list[tuple[int, bool, str | None]]:
    """``(candidate, started, cancelled_by)`` of each evaluation in the queue, by id."""
    with db.session() as s:
        found = s.scalars(select(Evaluation).order_by(Evaluation.id)).all()
        return [(e.candidate_id, e.started_at is not None, e.cancelled_by) for e in found]


def in_thread(fn: Callable[[], object]) -> threading.Thread:
    thread = threading.Thread(target=fn)
    thread.start()
    return thread


def evaluated(tmp_path: Path) -> list[str]:
    path = tmp_path / 'evaluated'
    return path.read_text().split() if path.exists() else []


def run_candidates(db: Database, run_id: int) -> list[Candidate]:
    with db.session() as s:
        return list(s.scalars(select(Candidate).where(Candidate.run_id == run_id).order_by(Candidate.id)))


def queued_executor(db: Database, runtime: ScriptRuntime, tmp_path: Path, slots: int) -> RunExecutor:
    return RunExecutor(db, Tracer(None), tmp_path, factory(runtime), max_evaluations=slots)


def errors(db: Database, candidate_id: int) -> tuple[list[str | None], Verdict]:
    with db.session() as s:
        c = s.get_one(Candidate, candidate_id)
        return [m.error for m in c.measurements], c.verdict


def test_two_runs_that_reach_one_tree_share_one_evaluation_and_its_crash(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """A tree that a second run reaches while it is evaluated is not evaluated again, not even after a crash."""
    (tmp_path / 'hold').touch()
    set_config(db, project, evaluate=held_evaluate(tmp_path, then='exit 7'))
    ex = queued_executor(db, runtime, tmp_path, slots=2)
    waits = mocker.spy(ex, '_wait')
    first, second = make_run(db, project), make_run(db, project, title='the same line')
    one = in_thread(lambda: execute(ex, db, first))
    until(lambda: len(evaluated(tmp_path)) == 1, 'the first evaluation')
    two = in_thread(lambda: execute(ex, db, second))
    until(lambda: waits.call_count == 2, 'the second request')
    (tmp_path / 'hold').unlink()
    one.join(30)
    two.join(30)
    [candidate] = run_candidates(db, first)
    assert run_candidates(db, second) == [], 'the second run reached the tree of the first'
    assert errors(db, candidate.id) == (['evaluate exit 7: '], Verdict.CRASH)
    assert len(evaluated(tmp_path)) == 1, 'evaluated once'
    assert rows(db) == [], 'the queue is empty'


def test_a_shared_evaluation_that_cannot_start_is_a_crash_for_every_request(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """The request that waits for another's evaluation gets that evaluation's crash, never an empty result."""
    ex = queued_executor(db, runtime, tmp_path, slots=2)
    waits = mocker.spy(ex, '_wait')

    def cannot_check_out(*_: object) -> None:
        until(lambda: waits.call_count == 2, 'the second request')
        raise RuntimeError('cannot check out the commit')

    mocker.patch.object(ex, '_evaluate', side_effect=cannot_check_out)
    first, second = make_run(db, project), make_run(db, project, title='the same line')
    one = in_thread(lambda: execute(ex, db, first))
    until(lambda: len(rows(db)) == 1, 'the first request')
    two = in_thread(lambda: execute(ex, db, second))
    one.join(30)
    two.join(30)
    [candidate] = run_candidates(db, first)
    assert errors(db, candidate.id) == (['RuntimeError: cannot check out the commit'], Verdict.CRASH)
    with db.session() as s:
        assert [s.get_one(Run, r).status for r in (first, second)] == [RunStatus.SUCCEEDED] * 2
    assert rows(db) == []


def test_a_cancelled_evaluation_is_a_crash_that_names_who_cancelled_it_at_once(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A cancel records the crash itself, so it holds even if no daemon sees it; a running evaluator is stopped."""
    write_script(Path(project.path).parent / 'harness.sh', HARNESS_OWN_LINE)
    (tmp_path / 'hold').touch()
    set_config(db, project, evaluate=held_evaluate(tmp_path))
    ex = queued_executor(db, runtime, tmp_path, slots=1)
    first, second = make_run(db, project), make_run(db, project, title='another line')
    one = in_thread(lambda: execute(ex, db, first))
    until(lambda: len(evaluated(tmp_path)) == 1, 'the first evaluation')
    two = in_thread(lambda: execute(ex, db, second))
    until(lambda: len(rows(db)) == 2, 'the second evaluation to wait')
    [running] = run_candidates(db, first)
    [waiting] = run_candidates(db, second)
    with db.session() as s:
        assert [e.candidate_id for e in queue.cancel(s, waiting.id, 'orchestrator')] == [waiting.id]
        assert [e.candidate_id for e in queue.cancel(s, running.id, 'human')] == [running.id]
        s.commit()
    assert rows(db) == [(running.id, True, 'human')], 'the waiting one left the queue; the running one is stopping'
    two.join(30)
    ex.stop_cancelled()
    one.join(30)
    assert errors(db, running.id) == (['evaluation cancelled by human'], Verdict.CRASH)
    assert errors(db, waiting.id) == (['evaluation cancelled by orchestrator'], Verdict.CRASH)
    assert len(evaluated(tmp_path)) == 1, 'the waiting evaluation never started'
    assert rows(db) == []


def test_a_cancel_holds_when_the_daemon_restarts_before_it_sees_it(db: Database, project: Project) -> None:
    with db.session() as s:
        evaluator = s.get_one(Project, project.id).evaluator_id
        assert evaluator is not None
        base = baseline(db, project.id).id
        other = Candidate(project_id=project.id, tree_sha='t', commit_sha='c')
        s.add(other)
        s.flush()
        running, _ = queue.request(s, base, evaluator)
        assert queue.start(s, running, 2)
        queue.request(s, other.id, evaluator)
        queue.cancel(s, base, 'human')
        queue.cancel(s, other.id, 'human')
        s.commit()
        s.execute(delete(Evaluation))  # what the next daemon's reap does
        s.commit()
        for candidate in (s.get_one(Candidate, base), s.get_one(Candidate, other.id)):
            assert measured(candidate, evaluator), 'its crash is recorded: resumed scoring does not evaluate it again'
            assert candidate.measurements[-1].error == 'evaluation cancelled by human'


def test_a_stop_ends_the_waiting_evaluations_and_records_nothing(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    write_script(Path(project.path).parent / 'harness.sh', HARNESS_OWN_LINE)
    (tmp_path / 'hold').touch()
    set_config(db, project, evaluate=held_evaluate(tmp_path))
    ex = queued_executor(db, runtime, tmp_path, slots=1)
    first, second = make_run(db, project), make_run(db, project, title='another line')
    threads = [in_thread(lambda: execute(ex, db, first))]
    until(lambda: len(evaluated(tmp_path)) == 1, 'the first evaluation')
    threads.append(in_thread(lambda: execute(ex, db, second)))
    until(lambda: len(rows(db)) == 2, 'the second evaluation to wait')
    ex.cancel()
    for thread in threads:
        thread.join(30)
    with db.session() as s:
        assert [c.measurements for c in s.scalars(select(Candidate).where(Candidate.run_id.is_not(None)))] == [[], []]
    assert rows(db) == [], 'each request left the queue; the next daemon asks again'


def add_project(db: Database, name: str, trees: int) -> list[int]:
    """A second project with ``trees`` candidates, none kept."""
    with db.session() as s:
        other = Project(name=name, path=f'/{name}', config={})
        s.add(other)
        s.flush()
        made = [Candidate(project_id=other.id, tree_sha=f'{name}{i}', commit_sha=f'{name}{i}') for i in range(trees)]
        s.add_all(made)
        s.commit()
        return [c.id for c in made]


def test_a_busy_project_never_holds_back_another_project(db: Database, project: Project) -> None:
    """A free slot goes to the project with the fewest running evaluations, then to the oldest request.

    A strict class order let one project's stream of run evaluations hold back another project's re-measure for ever.
    """
    busy = add_project(db, 'busy', 8)
    [third] = add_project(db, 'third', 1)
    with db.session() as s:
        evaluator = s.get_one(Project, project.id).evaluator_id
        assert evaluator is not None

        def ask(candidate_id: int) -> Evaluation:
            return queue.request(s, candidate_id, evaluator)[0]

        def finish(evaluation: Evaluation) -> None:
            s.delete(evaluation)
            s.flush()

        candidate = Candidate(project_id=project.id, tree_sha='t', commit_sha='c')
        s.add(candidate)
        s.flush()
        first = ask(busy[0])
        assert queue.start(s, first, 1)
        older = ask(busy[1])
        quiet = ask(candidate.id)
        newer = [ask(c) for c in busy[2:5]]
        assert [e.id for e in queue.waiting(s)] == [quiet.id, older.id, *(e.id for e in newer)], (
            'while the busy project runs one, the quiet project is next'
        )
        finish(first)
        assert queue.start(s, older, 1), 'the request before the quiet project: its turn first'
        finish(older)
        assert not queue.start(s, newer[0], 1)
        assert queue.start(s, quiet, 1), 'the busy project asked again meanwhile, but the quiet one is next'
        assert not queue.start(s, newer[0], 1), 'one slot, taken'
        assert queue.start(s, newer[0], 2)
        late = ask(busy[5])
        mine = ask(third)
        assert [e.id for e in queue.waiting(s)] == [mine.id, newer[1].id, newer[2].id, late.id], (
            'two slots: the project with nothing running first, then the oldest'
        )
        assert [e.id for e in queue.listing(s)] == [quiet.id, newer[0].id, mine.id, newer[1].id, newer[2].id, late.id]


def test_a_project_takes_its_head_before_its_older_requests(db: Database, project: Project) -> None:
    with db.session() as s:
        evaluator = s.get_one(Project, project.id).evaluator_id
        assert evaluator is not None
        other = Candidate(project_id=project.id, tree_sha='t', commit_sha='c')
        s.add(other)
        s.flush()
        older = queue.request(s, other.id, evaluator)[0]
        trunk, created = queue.request(s, baseline(db, project.id).id, evaluator)
        assert created and queue.request(s, trunk.candidate_id, evaluator) == (trunk, False), 'one row per tree'
        assert [e.id for e in queue.waiting(s)] == [trunk.id, older.id]


def test_an_evaluation_id_is_never_reused(db: Database, project: Project) -> None:
    """A request that waits for an evaluation by id never mistakes a later one for it."""
    with db.session() as s:
        evaluator = s.get_one(Project, project.id).evaluator_id
        assert evaluator is not None
        base = baseline(db, project.id).id
        gone = queue.request(s, base, evaluator)[0].id
        s.commit()
        s.execute(delete(Evaluation))
        s.commit()
        assert queue.request(s, base, evaluator)[0].id > gone


def test_evaluations_that_wait_for_a_slot_never_hold_back_an_agent_launch(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A run waits in the evaluation queue on its pool thread, so the pool must not run out while many wait."""
    scheduler = make_scheduler(db, runtime, tmp_path)
    release = threading.Event()
    waiting = [scheduler._pool.submit(release.wait, 30) for _ in range(64)]  # more than any default pool size
    add_ideas(db, project, 'one line')
    try:
        assert len(scheduler.tick()) == 1
        with db.session() as s:
            [run_id] = s.scalars(select(Run.id)).all()
        until(lambda: run_status(db, run_id) is not RunStatus.QUEUED, 'the agent to start')
    finally:
        release.set()
    assert all(f.result(5) for f in waiting)
    scheduler.wait(30)
    scheduler.stop(timeout=5)


def run_status(db: Database, run_id: int) -> RunStatus:
    with db.session() as s:
        return s.get_one(Run, run_id).status


def test_a_remeasure_whose_repo_is_gone_is_a_crash(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    Path(project.path).rename(tmp_path / 'moved')
    base = baseline(db, project.id).id
    queued_executor(db, runtime, tmp_path, slots=1).remeasure(base)
    found, verdict = errors(db, base)
    assert found[-1] is not None and found[-1].startswith('NoSuchPathError'), 'it cannot start: a crash, asked again'
    assert verdict is Verdict.KEEP, "a keep's verdict stays"
    assert rows(db) == []
