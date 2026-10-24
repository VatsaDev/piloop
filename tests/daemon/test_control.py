"""The daemon's side of the control operations: a pause holds the tick, and a measurement by hand is judged again."""

from pathlib import Path

import pytest
from git import Repo
from pytest_mock import MockerFixture
from sqlalchemy import select

from piloop.actor import System
from piloop.campaign import ops
from piloop.config import load_campaign_config
from piloop.daemon.run_controller import RunController
from piloop.db.events import in_order
from piloop.db.models import AgentSession, AgentSessionState, Campaign, Candidate, Comment, IdeaStatus, Origin
from piloop.db.session import Database
from piloop.ids import CampaignId, TargetKind
from piloop.plugins.vcs.git import GitRepository
from piloop.plugins.verdict_rule.contract import Outcome
from piloop.scoring.ledger import RejudgeFailed, head, import_reference, rejudges, request_remeasure
from tests.conftest import HARNESS_WORSE, Gate, ScriptRuntime, write_script
from tests.daemon.test_run_controller import candidates, controller, execute, make_run
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
    assert sched.tick() == [] and sched.tick() == []
    assert statuses(db) == [IdeaStatus.READY], 'no run is queued'
    with db.session() as s:
        agent_sessions = s.scalars(select(AgentSession)).all()
        assert [(w.id, w.state) for w in agent_sessions] == [(watchdog, AgentSessionState.REQUESTED)], (
            'nothing launches or is asked'
        )
        assert s.scalars(select(Comment)).all() == [], 'a pause raises no escalation'
        ops.resume(s, CampaignId(1))
        s.commit()
    launched = sched.tick()
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
        cfg = load_campaign_config(Path(campaign.path))
        reference = import_reference(
            s, proj, cfg, GitRepository(repo), 'elsewhere', summary=None, actor=System.ORCHESTRATOR
        ).id
        s.commit()
    sched = make_scheduler(db, runtime, tmp_path)
    pause(db, evals=True)
    sched.tick()
    sched.wait()
    assert measured_by(db, reference) == [], 'held'
    pause(db, evals=False)
    sched.tick()
    sched.wait()
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
    sched.tick()
    assert gate.entered.wait(10)
    sched.tick()
    gate.release.set()
    sched.wait()
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
    sched.tick()
    sched.wait()
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
    sched.tick()
    sched.wait()
    with db.session() as s:
        again = s.get_one(Candidate, scaffold_id)
        assert (again.origin, again.verdict, len(again.measurements)) == (Origin.RUN, Outcome.KEEP, 1), (
            'next to its bar, the baseline'
        )
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
        assert rejudges(s, s.get_one(Campaign, campaign.id)) == [], 'not tried on every tick'
