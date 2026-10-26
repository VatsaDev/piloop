"""The evaluation queue: one evaluation per tree, a cap on the evaluations that run, a fair order, and cancels."""

import threading
from collections.abc import Callable
from pathlib import Path

from git import Repo
from pytest_mock import MockerFixture
from sqlalchemy import delete, select

from piloop.actor import System
from piloop.daemon.run_controller import RunController
from piloop.db.models import Campaign, Candidate, Evaluation, Run, RunStatus, utcnow
from piloop.db.session import Database
from piloop.ids import CandidateId, RunId
from piloop.otel import Tracer
from piloop.plugins.executor.workers import Workers
from piloop.plugins.vcs.git import commands as git
from piloop.plugins.verdict_rule.contract import Outcome
from piloop.scoring import queue
from piloop.scoring.ledger import measured, request_remeasure
from tests.conftest import (
    SETTLE,
    ScriptRuntime,
    bare_config,
    baseline,
    factory,
    run_all,
    serving,
    wait_until,
    workers_of,
    write_script,
)
from tests.daemon.test_run_controller import execute, make_run, set_config
from tests.daemon.test_scheduler import add_ideas, make_scheduler

HARNESS_OWN_LINE = '#!/bin/sh\nbasename "$(dirname "$PWD")" >> solve.txt\ngit commit -qam "own line"\n'
"""Each run appends its own id (``runs/<id>/worktree``), so each run makes a tree of its own."""
SCORE = 'echo METRIC score=$(wc -l < solve.txt); echo METRIC size=$(wc -c < solve.txt)'


def held_evaluate(tmp_path: Path, then: str = SCORE) -> list[str]:
    """An evaluate command that logs its commit in ``evaluated``, waits while ``hold`` exists, then runs ``then``."""
    log, hold = tmp_path / 'evaluated', tmp_path / 'hold'
    return ['sh', '-c', f'git rev-parse HEAD >> {log}; while [ -f {hold} ]; do sleep 0.05; done; {then}']


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


def run_candidates(db: Database, run_id: RunId) -> list[Candidate]:
    with db.session() as s:
        return list(s.scalars(select(Candidate).where(Candidate.run_id == run_id).order_by(Candidate.id)))


def queued_executor(db: Database, runtime: ScriptRuntime, tmp_path: Path, slots: int) -> RunController:
    executor = Workers.model_validate({'pools': {'serve': {'evaluations': slots}}})
    return serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=executor))


def errors(db: Database, candidate_id: CandidateId) -> tuple[list[str | None], Outcome | None]:
    with db.session() as s:
        c = s.get_one(Candidate, candidate_id)
        return [m.error for m in c.measurements], c.verdict


def test_two_runs_that_reach_one_tree_share_one_evaluation_and_its_crash(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """A tree that a second run reaches while it is evaluated is not evaluated again, not even after a crash."""
    (tmp_path / 'hold').touch()
    set_config(db, campaign, evaluate=held_evaluate(tmp_path, then='exit 7'))
    ex = queued_executor(db, runtime, tmp_path, slots=2)
    waits = mocker.spy(queue, 'request')
    first, second = make_run(db, campaign), make_run(db, campaign, title='the same line')
    one = in_thread(lambda: execute(ex, db, first))
    wait_until(lambda: len(evaluated(tmp_path)) == 1, 'the first evaluation')
    two = in_thread(lambda: execute(ex, db, second))
    wait_until(lambda: waits.call_count == 2, 'the second request')
    (tmp_path / 'hold').unlink()
    one.join(30)
    two.join(30)
    [candidate] = run_candidates(db, first)
    assert run_candidates(db, second) == [], 'the second run reached the tree of the first'
    assert errors(db, candidate.id) == (['evaluate exit 7: '], Outcome.CRASH)
    assert len(evaluated(tmp_path)) == 1, 'evaluated once'
    assert rows(db) == [], 'the queue is empty'


def test_a_shared_evaluation_that_cannot_start_is_a_crash_for_every_request(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """The request that waits for another's evaluation gets that evaluation's crash, never an empty result."""
    ex = queued_executor(db, runtime, tmp_path, slots=2)
    waits = mocker.spy(queue, 'request')
    add_worktree = git.add_worktree

    def cannot_check_out(repo: Repo, path: Path, sha: str, links: dict[str, Path]) -> Repo:
        if 'eval-' not in str(path):  # the implementer's worktree
            return add_worktree(repo, path, sha, links)
        wait_until(lambda: waits.call_count == 2, 'the second request')
        raise RuntimeError('cannot check out the commit')

    mocker.patch('piloop.plugins.vcs.git.commands.add_worktree', side_effect=cannot_check_out)
    first, second = make_run(db, campaign), make_run(db, campaign, title='the same line')
    one = in_thread(lambda: execute(ex, db, first))
    wait_until(lambda: len(rows(db)) == 1, 'the first request')
    two = in_thread(lambda: execute(ex, db, second))
    one.join(30)
    two.join(30)
    [candidate] = run_candidates(db, first)
    assert errors(db, candidate.id) == (['RuntimeError: cannot check out the commit'], Outcome.CRASH)
    with db.session() as s:
        assert [s.get_one(Run, r).status for r in (first, second)] == [RunStatus.SUCCEEDED] * 2
    assert rows(db) == []


def test_a_cancelled_evaluation_is_a_crash_that_names_who_cancelled_it_at_once(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A cancel records the crash itself, so it holds even if no daemon sees it; a running evaluator is stopped."""
    write_script(Path(campaign.path).parent / 'harness.sh', HARNESS_OWN_LINE)
    (tmp_path / 'hold').touch()
    set_config(db, campaign, evaluate=held_evaluate(tmp_path))
    ex = queued_executor(db, runtime, tmp_path, slots=1)
    first, second = make_run(db, campaign), make_run(db, campaign, title='another line')
    one = in_thread(lambda: execute(ex, db, first))
    wait_until(lambda: len(evaluated(tmp_path)) == 1, 'the first evaluation')
    two = in_thread(lambda: execute(ex, db, second))
    wait_until(lambda: len(rows(db)) == 2, 'the second evaluation to wait')
    [running] = run_candidates(db, first)
    [waiting] = run_candidates(db, second)
    with db.session() as s:
        assert queue.cancel(s, waiting.id, System.ORCHESTRATOR) == 1
        assert queue.cancel(s, running.id, System.HUMAN) == 1
        s.commit()
    assert rows(db) == [(running.id, True, 'human')], 'the waiting one left the queue; the running one is stopping'
    two.join(30)
    ex.stop_cancelled()
    one.join(30)
    assert errors(db, running.id) == (['evaluation cancelled by human'], Outcome.CRASH)
    assert errors(db, waiting.id) == (['evaluation cancelled by orchestrator'], Outcome.CRASH)
    assert len(evaluated(tmp_path)) == 1, 'the waiting evaluation never started'
    assert rows(db) == []


def test_a_cancel_holds_when_the_daemon_restarts_before_it_sees_it(db: Database, campaign: Campaign) -> None:
    with db.session() as s:
        evaluator = s.get_one(Campaign, campaign.id).evaluator_id
        assert evaluator is not None
        base = baseline(db, campaign.id).id
        other = Candidate(campaign_id=campaign.id, tree_sha='t', revision='c')
        s.add(other)
        s.flush()
        running, _ = queue.request(s, base, evaluator)
        running.started_at = utcnow()  # a worker took it
        queue.request(s, other.id, evaluator)
        queue.cancel(s, base, System.HUMAN)
        queue.cancel(s, other.id, System.HUMAN)
        s.commit()
        s.execute(delete(Evaluation))  # what the next daemon's reap does
        s.commit()
        for candidate in (s.get_one(Candidate, base), s.get_one(Candidate, other.id)):
            assert measured(candidate, evaluator), 'its crash is recorded: resumed scoring does not evaluate it again'
            assert candidate.measurements[-1].error == 'evaluation cancelled by human'


def test_a_stop_ends_the_waiting_evaluations_and_records_nothing(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    write_script(Path(campaign.path).parent / 'harness.sh', HARNESS_OWN_LINE)
    (tmp_path / 'hold').touch()
    set_config(db, campaign, evaluate=held_evaluate(tmp_path))
    ex = queued_executor(db, runtime, tmp_path, slots=1)
    first, second = make_run(db, campaign), make_run(db, campaign, title='another line')
    threads = [in_thread(lambda: execute(ex, db, first))]
    wait_until(lambda: len(evaluated(tmp_path)) == 1, 'the first evaluation')
    threads.append(in_thread(lambda: execute(ex, db, second)))
    wait_until(lambda: len(rows(db)) == 2, 'the second evaluation to wait')
    ex.stop(30)
    for thread in threads:
        thread.join(30)
    workers_of(ex).worker.join(30)
    with db.session() as s:
        assert [c.measurements for c in s.scalars(select(Candidate).where(Candidate.run_id.is_not(None)))] == [[], []]
    [_, waiting] = run_candidates(db, first) + run_candidates(db, second)
    assert rows(db) == [(waiting.id, False, None)], (
        "the stopped one left the queue; the waiting one stays queued for the next daemon's worker"
    )


def add_campaign(db: Database, name: str, trees: int) -> list[CandidateId]:
    """A second campaign with ``trees`` candidates, none kept."""
    with db.session() as s:
        other = Campaign(name=name, path=f'/{name}', config=bare_config(name))
        s.add(other)
        s.flush()
        made = [Candidate(campaign_id=other.id, tree_sha=f'{name}{i}', revision=f'{name}{i}') for i in range(trees)]
        s.add_all(made)
        s.commit()
        return [c.id for c in made]


def test_a_busy_campaign_never_holds_back_another_campaign(db: Database, campaign: Campaign) -> None:
    """A free slot goes to the campaign with the fewest running evaluations, then to the oldest request.

    A strict class order let one campaign's stream of run evaluations hold back another campaign's re-measure for ever.
    """
    busy = add_campaign(db, 'busy', 8)
    [third] = add_campaign(db, 'third', 1)
    with db.session() as s:
        evaluator = s.get_one(Campaign, campaign.id).evaluator_id
        assert evaluator is not None

        def ask(candidate_id: CandidateId) -> Evaluation:
            return queue.request(s, candidate_id, evaluator)[0]

        def finish(evaluation: Evaluation) -> None:
            s.delete(evaluation)
            s.flush()

        candidate = Candidate(campaign_id=campaign.id, tree_sha='t', revision='c')
        s.add(candidate)
        s.flush()

        def take() -> Evaluation:
            """What a worker with a free slot does: it starts the next one (``Leases.acquire``)."""
            evaluation = queue.waiting(s)[0]
            evaluation.started_at = utcnow()
            s.flush()
            return evaluation

        first = ask(busy[0])
        assert take() == first
        older = ask(busy[1])
        quiet = ask(candidate.id)
        newer = [ask(c) for c in busy[2:5]]
        assert [e.id for e in queue.waiting(s)] == [quiet.id, older.id, *(e.id for e in newer)], (
            'while the busy campaign runs one, the quiet campaign is next'
        )
        finish(first)
        assert take() == older, 'the request before the quiet campaign: its turn first'
        finish(older)
        assert take() == quiet, 'the busy campaign asked again meanwhile, but the quiet one is next'
        assert take() == newer[0]
        late = ask(busy[5])
        mine = ask(third)
        assert [e.id for e in queue.waiting(s)] == [mine.id, newer[1].id, newer[2].id, late.id], (
            'two slots: the campaign with nothing running first, then the oldest'
        )
        assert [e.id for e in queue.listing(s)] == [quiet.id, newer[0].id, mine.id, newer[1].id, newer[2].id, late.id]


def test_a_campaign_takes_its_head_before_its_older_requests(db: Database, campaign: Campaign) -> None:
    with db.session() as s:
        evaluator = s.get_one(Campaign, campaign.id).evaluator_id
        assert evaluator is not None
        other = Candidate(campaign_id=campaign.id, tree_sha='t', revision='c')
        s.add(other)
        s.flush()
        older = queue.request(s, other.id, evaluator)[0]
        trunk, created = queue.request(s, baseline(db, campaign.id).id, evaluator)
        assert created and queue.request(s, trunk.candidate_id, evaluator) == (trunk, False), 'one row per tree'
        assert [e.id for e in queue.waiting(s)] == [trunk.id, older.id]


def test_an_evaluation_id_is_never_reused(db: Database, campaign: Campaign) -> None:
    """A request that waits for an evaluation by id never mistakes a later one for it."""
    with db.session() as s:
        evaluator = s.get_one(Campaign, campaign.id).evaluator_id
        assert evaluator is not None
        base = baseline(db, campaign.id).id
        gone = queue.request(s, base, evaluator)[0].id
        s.commit()
        s.execute(delete(Evaluation))
        s.commit()
        assert queue.request(s, base, evaluator)[0].id > gone


def test_evaluations_that_wait_for_a_slot_never_hold_back_an_agent_launch(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A run waits in the evaluation queue on its pool thread, so the pool must not run out while many wait."""
    scheduler = make_scheduler(db, runtime, tmp_path)
    release = threading.Event()
    waiting = [scheduler._pool.submit(release.wait, 30) for _ in range(64)]  # more than any default pool size
    add_ideas(db, campaign, 'one line')
    try:
        assert len(run_all(scheduler)) == 1
        with db.session() as s:
            [run_id] = s.scalars(select(Run.id)).all()
        wait_until(lambda: run_status(db, run_id) is not RunStatus.QUEUED, 'the agent to start')
    finally:
        release.set()
    assert all(f.result(5) for f in waiting)
    scheduler.wait(SETTLE)
    scheduler.stop(timeout=5)


def run_status(db: Database, run_id: RunId) -> RunStatus:
    with db.session() as s:
        return s.get_one(Run, run_id).status


def test_a_remeasure_whose_repo_is_gone_is_a_crash(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    Path(campaign.path).rename(tmp_path / 'moved')
    base = baseline(db, campaign.id).id
    queued_executor(db, runtime, tmp_path, slots=1).remeasure(base)
    found, verdict = errors(db, base)
    assert found[-1] is not None and found[-1].startswith('NotARepository'), 'it cannot start: a crash, asked again'
    assert verdict is Outcome.KEEP, "a keep's verdict stays"
    assert rows(db) == []


def killed_first(tmp_path: Path, times: int, sig: str = 'TERM') -> list[str]:
    """An evaluate command that logs each try in ``tries`` and kills itself with ``sig`` on its first ``times`` tries."""
    tries = tmp_path / 'tries'
    return ['sh', '-c', f'echo x >> {tries}; [ $(wc -l < {tries}) -gt {times} ] || kill -{sig} $$; {SCORE}']


def tries(tmp_path: Path) -> int:
    return len((tmp_path / 'tries').read_text().split())


def test_an_evaluation_that_another_process_kills_is_run_once_more(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """An implementer's ``pkill -f`` measures nothing of the candidate, so the daemon evaluates it again, once."""
    set_config(db, campaign, evaluate=killed_first(tmp_path, times=1))
    base = baseline(db, campaign.id).id
    queued_executor(db, runtime, tmp_path, slots=1).remeasure(base)
    assert tries(tmp_path) == 2
    with db.session() as s:
        last = s.get_one(Candidate, base).measurements[-1]
        assert (last.error, last.metrics) == (None, {'score': 1.0, 'size': 5.0})
    assert rows(db) == []


def test_an_evaluation_killed_twice_is_a_crash_that_names_the_signal(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    set_config(db, campaign, evaluate=killed_first(tmp_path, times=2))
    base = baseline(db, campaign.id).id
    queued_executor(db, runtime, tmp_path, slots=1).remeasure(base)
    assert tries(tmp_path) == 2, 'one more try, not a loop'
    assert errors(db, base)[0][-1] == 'evaluate killed by SIGTERM (exit -15), not by its own exit: '


def test_an_evaluate_that_crashes_on_a_signal_of_its_own_is_not_run_again(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A segfault is the candidate's fault, and a measurement of it."""
    set_config(db, campaign, evaluate=['sh', '-c', f'ulimit -c 0; {killed_first(tmp_path, 1, "SEGV")[2]}'])
    base = baseline(db, campaign.id).id
    queued_executor(db, runtime, tmp_path, slots=1).remeasure(base)
    assert tries(tmp_path) == 1
    assert errors(db, base)[0][-1] == 'evaluate killed by SIGSEGV (exit -11), not by its own exit: '


def test_a_requested_remeasure_is_listed_as_waiting_before_the_daemon_queues_it(
    db: Database, campaign: Campaign
) -> None:
    """The daemon queues one remeasure per campaign at a time, so the others would be invisible while they wait."""
    with db.session() as s:
        evaluator = s.get_one(Campaign, campaign.id).evaluator_id
        assert evaluator is not None
        base = s.get_one(Candidate, baseline(db, campaign.id).id)
        assert queue.requested(s) == []
        request_remeasure(s, base, System.HUMAN)
        s.commit()
        assert [c.id for c, _ in queue.requested(s)] == [base.id], 'listed from the moment it is accepted'
        evaluation, _ = queue.request(s, base.id, evaluator)
        assert queue.requested(s) == [], 'once queued, it is listed as its evaluation, not twice'
        evaluation.started_at = utcnow()  # a worker took it
        queue.cancel(s, base.id, System.HUMAN)
        request_remeasure(s, base, System.HUMAN)
        s.commit()
        assert [c.id for c, _ in queue.requested(s)] == [base.id], 'a cancelled evaluation does not answer it'
        s.delete(evaluation)
        s.flush()
        assert queue.cancel(s, base.id, System.ORCHESTRATOR) == 1, 'a request that is not queued yet can be cancelled'
        s.commit()
        assert queue.requested(s) == []
        assert base.measurements[-1].error == 'evaluation cancelled by orchestrator'


def test_a_waiting_evaluation_that_this_daemon_did_not_ask_for_is_never_submitted(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A row that another daemon left has no job here: it waits for the reap, and holds back no request of this one."""
    base = baseline(db, campaign.id)
    with db.session() as s:
        other = Candidate(campaign_id=campaign.id, tree_sha='t2', revision=base.revision, parent_id=base.id)
        s.add(other)
        s.flush()
        s.add(Evaluation(candidate_id=other.id, evaluator_id=s.get_one(Campaign, campaign.id).evaluator_id))
        s.commit()
        other_id = other.id
    ex = queued_executor(db, runtime, tmp_path, slots=2)  # a free slot that the foreign row does not take
    ex.remeasure(base.id)
    assert rows(db) == [(other_id, False, None)], 'measured, and the foreign row still waits'
