"""The daemon's side of the control operations: a pause holds the tick, and a measurement by hand is judged again."""

import sqlite3
from contextlib import closing
from pathlib import Path

import pytest
from git import Repo
from pytest_mock import MockerFixture
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.actor import System
from piloop.campaign import ops
from piloop.daemon.run_controller import RunController
from piloop.db.events import in_order
from piloop.db.models import ENDED, AgentSession, AgentSessionState, Campaign, Candidate, Comment, Origin
from piloop.db.session import Database
from piloop.ids import CampaignId, CampaignStatus, IdeaStatus, TargetKind
from piloop.plugins.campaign_type.optimization.planner import OptimizationPlanner
from piloop.plugins.vcs.git import PILOOP, GitRepository
from piloop.plugins.verdict_rule.contract import Outcome
from piloop.scoring.ledger import RejudgeFailed, head, import_reference, rejudges, request_remeasure
from tests.conftest import HARNESS_WORSE, SETTLE, Gate, ScriptRuntime, run_all, write_script
from tests.daemon.test_run_controller import candidates, controller, execute, keep_elsewhere, make_run
from tests.daemon.test_scheduler import add_ideas, make_scheduler, measured_by, request_watchdog, statuses


def pause(db: Database, *, evals: bool) -> None:
    with db.session() as s:
        ops.pause(s, CampaignId(1), evals=evals, reason='wait for the new evaluator')
        s.commit()


def test_a_paused_campaign_launches_nothing_requests_nothing_and_escalates_nothing_until_resumed(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    sched = make_scheduler(db, runtime, tmp_path)
    add_ideas(db, campaign, 'more lines')
    watchdog = request_watchdog(db, campaign)
    pause(db, evals=False)
    assert run_all(sched) == [] and run_all(sched) == []
    assert statuses(db) == [IdeaStatus.READY], 'no run is queued'
    with db.session() as s:
        agent_sessions = s.scalars(select(AgentSession)).all()
        assert [(w.id, w.state) for w in agent_sessions] == [(watchdog, AgentSessionState.REQUESTED)], (
            'nothing launches or is asked'
        )
        assert s.scalars(select(Comment)).all() == [], 'a pause raises no escalation'
        ops.resume(s, CampaignId(1))
        s.commit()
    launched = run_all(sched)
    assert watchdog in launched and len(launched) == 2, 'the request and the run of the idea'
    assert statuses(db) == [IdeaStatus.SCHEDULED]
    sched.stop(timeout=10)


def test_a_pause_still_measures_what_the_evaluator_needs_unless_it_holds_the_evaluations_too(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    repo = Repo(campaign.path)
    repo.git.checkout('-qb', 'elsewhere')
    (Path(campaign.path) / 'solve.txt').write_text('a\nb\nc\n')
    repo.git.commit('-qam', 'three lines')
    repo.git.checkout('-q', 'main')
    with db.session() as s:
        proj = s.get_one(Campaign, campaign.id)
        reference = import_reference(
            s, proj, GitRepository(repo, PILOOP), 'elsewhere', summary=None, actor=System.ORCHESTRATOR
        ).id
        s.commit()
    sched = make_scheduler(db, runtime, tmp_path)
    pause(db, evals=True)
    run_all(sched)
    sched.wait(SETTLE)
    assert measured_by(db, reference) == [], 'held'
    pause(db, evals=False)
    run_all(sched)
    sched.wait(SETTLE)
    assert measured_by(db, reference) == [(1, 3.0, None)], 'a pause of the launches only still measures'
    sched.stop(timeout=5)


def test_the_daemon_judges_a_discard_again_with_a_measurement_by_hand(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
) -> None:
    write_script(Path(campaign.path).parent / 'harness.sh', HARNESS_WORSE)
    run_id = make_run(db, campaign)
    execute(controller(db, runtime, tmp_path), db, run_id)
    [worse] = candidates(db, run_id)
    assert worse.verdict is Outcome.DISCARD
    with db.session() as s:
        again = ops.add_measurement(
            s, worse.id, ops.Report(metrics={'score': 5.0, 'size': 1.0}), actor=System.ORCHESTRATOR
        ).rejudged
        s.commit()
    assert again
    sched = make_scheduler(db, runtime, tmp_path)
    gate, asked = Gate(), mocker.spy(RunController, 'rejudge')
    monkeypatch.setattr(sched._controller, 'rejudge', gate.wrap(sched._controller.rejudge))
    run_all(sched)
    assert gate.entered.wait(10)
    run_all(sched)
    gate.release.set()
    sched.wait(SETTLE)
    assert asked.call_count == 1, 'one judgement at a time in the campaign slot'
    with db.session() as s:
        c = s.get_one(Candidate, worse.id)
        assert (c.verdict, c.verdict_by) == (Outcome.KEEP, System.DAEMON), 'a mean of 2.5 against the 1 of the baseline'
        p = s.get_one(Campaign, campaign.id)
        trunk = head(s, p)
        assert trunk is not None and trunk.id == worse.id
        assert rejudges(s, p) == [], 'judged since the request'
    sched.stop(timeout=5)


SKIP = '#!/bin/sh\necho a >> solve.txt\ngit commit -qam scaffold -m "Piloop-Eval: skip"\n'
"""A run whose one commit is marked skip, so the daemon never measures it."""


def test_the_daemon_judges_a_skipped_commit_with_a_measurement_by_hand(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A tree committed with ``Piloop-Eval: skip`` that an orchestrator measured by hand was recorded, never judged."""
    write_script(Path(campaign.path).parent / 'harness.sh', SKIP)
    run_id = make_run(db, campaign)
    execute(controller(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        scaffold_id, origin, verdict = s.execute(
            select(Candidate.id, Candidate.origin, Candidate.verdict).where(Candidate.run_id == run_id)
        ).one()
        assert (origin, verdict) == (Origin.SKIPPED, None)
        added = ops.add_measurement(
            s, scaffold_id, ops.Report(metrics={'score': 5.0, 'size': 1.0}), actor=System.ORCHESTRATOR
        )
        s.commit()
    assert added.rejudged and 'judges it' in added.effect
    sched = make_scheduler(db, runtime, tmp_path)
    run_all(sched)
    sched.wait(SETTLE)
    with db.session() as s:
        judged = s.get_one(Candidate, scaffold_id)
        assert (judged.origin, judged.verdict) == (Origin.RUN, Outcome.KEEP), (
            'against the baseline, as its run would judge it'
        )
    sched.stop(timeout=5)


def test_a_skipped_commit_is_measured_on_request_and_judged(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """``candidate remeasure`` refused a skipped commit, so isolating a regression to it took a measurement by hand."""
    write_script(Path(campaign.path).parent / 'harness.sh', SKIP)
    run_id = make_run(db, campaign)
    execute(controller(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        scaffold = s.scalars(select(Candidate).where(Candidate.run_id == run_id)).one()
        request_remeasure(s, scaffold, System.ORCHESTRATOR, repeats=1)
        s.commit()
        scaffold_id = scaffold.id
    sched = make_scheduler(db, runtime, tmp_path)
    run_all(sched)
    sched.wait(SETTLE)
    with db.session() as s:
        again = s.get_one(Candidate, scaffold_id)
        assert (again.origin, again.verdict, len(again.measurements)) == (Origin.RUN, Outcome.KEEP, 1), (
            'next to its bar, the baseline'
        )
    sched.stop(timeout=5)


def test_the_daemon_judges_the_merge_onto_the_head_of_a_hand_keep_off_the_head(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A hand keep off the head was refused, and the orchestrator re-landed the change on the head by hand."""
    write_script(Path(campaign.path).parent / 'harness.sh', HARNESS_WORSE)
    run_id = make_run(db, campaign)
    execute(controller(db, runtime, tmp_path), db, run_id)
    [worse] = candidates(db, run_id)
    trunk = keep_elsewhere(db, campaign, 'notes', {'notes.txt': 'n\n'}, {'score': 0.5, 'size': 5.0})
    with db.session() as s:
        done = ops.set_verdict(
            s, worse.id, Outcome.KEEP, reason='the score misses what it fixes', actor=System.ORCHESTRATOR
        )
        s.commit()
    assert done.rebased is not None and done.rebased.rebased is not None
    merged = done.rebased.rebased
    sched = make_scheduler(db, runtime, tmp_path)
    run_all(sched)
    sched.wait(SETTLE)
    with db.session() as s:
        c = s.get_one(Candidate, merged)
        assert (c.verdict, c.verdict_by, len(c.measurements)) == (Outcome.DISCARD, System.DAEMON, 1), (
            'measured and judged'
        )
        assert c.verdict_reason is not None and c.verdict_reason.startswith(f'against candidate#{trunk}:')
    sched.stop(timeout=5)


def test_a_judgement_again_that_fails_answers_the_request_with_the_error(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    write_script(Path(campaign.path).parent / 'harness.sh', HARNESS_WORSE)
    run_id = make_run(db, campaign)
    ex = controller(db, runtime, tmp_path)
    execute(ex, db, run_id)
    [worse] = candidates(db, run_id)
    with db.session() as s:
        ops.add_measurement(s, worse.id, ops.Report(metrics={'score': 5.0, 'size': 1.0}), actor=System.ORCHESTRATOR)
        s.commit()
    mocker.patch('piloop.plugins.vcs.git.commands.open_repo', side_effect=OSError('the repo is gone'))
    ex.rejudge(worse.id)
    with db.session() as s:
        [(failed, payload)] = in_order(s, RejudgeFailed)
        assert (failed.target_kind, failed.target_id, payload) == (
            TargetKind.CANDIDATE,
            worse.id,
            RejudgeFailed(error='OSError: the repo is gone'),
        )
        assert s.get_one(Candidate, worse.id).verdict is Outcome.DISCARD
        assert rejudges(s, s.get_one(Campaign, campaign.id)) == [], 'not tried on every pass'


def test_an_agent_requested_while_a_retire_checks_either_stops_the_retire_or_never_lands(
    db: Database, campaign: Campaign, mocker: MockerFixture
) -> None:
    """A retire and a launch race: the retire must never pass while an agent of its campaign is open."""
    with db.session() as s:
        s.get_one(Campaign, campaign.id).status = CampaignStatus.STOPPED
        s.commit()
    path = db.engine.url.database
    assert path is not None
    landed: list[bool] = []

    def scheduler_requests(*_: object) -> bool:
        """The daemon's pass commits a request on its own connection, between the retire's check and its change."""
        with closing(sqlite3.connect(path, timeout=0.2, isolation_level=None)) as con:
            try:
                con.execute("INSERT INTO agent_sessions (kind, campaign_id) VALUES ('research', ?)", (campaign.id,))
                landed.append(True)
            except sqlite3.OperationalError as e:
                assert 'database is locked' in str(e), e
                landed.append(False)
        return False

    mocker.patch.object(ops, 'unclosed', side_effect=scheduler_requests)
    with db.session() as s:
        try:
            ops.retire(s, campaign.id, System.HUMAN, reason='done')
            s.commit()
        except ops.Refused:
            s.rollback()
    with db.session() as s:
        retired = s.get_one(Campaign, campaign.id).status is CampaignStatus.RETIRED
        open_ = s.scalars(select(AgentSession.id).where(AgentSession.state.not_in(ENDED))).all()
    assert landed and not (retired and open_), (landed, retired, open_)


def test_a_campaign_retired_while_the_daemon_takes_its_request_launches_nothing(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """The second guard: a pass that read the campaign before a retire committed never launches its rows."""
    with db.session() as s:
        s.get_one(Campaign, campaign.id).status = CampaignStatus.STOPPED
        s.commit()
    asked = request_watchdog(db, campaign)
    scheduler = make_scheduler(db, runtime, tmp_path)
    path = db.engine.url.database
    assert path is not None
    tidy = OptimizationPlanner.tidy

    def retired_first(planner: OptimizationPlanner, s: Session, read: Campaign) -> None:
        """The retire commits on its own connection, after the pass read the campaign and before it writes."""
        with closing(sqlite3.connect(path, timeout=0.2, isolation_level=None)) as con:
            con.execute("UPDATE campaigns SET status = 'retired' WHERE id = ?", (read.id,))
        tidy(planner, s, read)

    mocker.patch.object(OptimizationPlanner, 'tidy', autospec=True, side_effect=retired_first)
    assert run_all(scheduler) == []
    with db.session() as s:
        row = s.get_one(AgentSession, asked)
        assert (row.state, row.error) == (AgentSessionState.KILLED, 'its campaign is retired')
