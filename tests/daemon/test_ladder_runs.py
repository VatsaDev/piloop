"""The evaluation ladder: a run's own version climbs it and may stop at a rung; everything else climbs every rung.

Everything else is a baseline, a head, a bar, a reference, a requested remeasure, and the later samples of a paired
evaluation.
"""

from collections.abc import Iterator
from importlib.metadata import EntryPoint
from pathlib import Path

import pytest
from inline_snapshot import snapshot
from pytest_mock import MockerFixture

from piloop.actor import System
from piloop.config import CampaignConfig
from piloop.db.models import Campaign, Candidate
from piloop.db.session import Database
from piloop.ids import CampaignId, CandidateId, EvaluatorId, Revision, RunId
from piloop.plugins.verdict_rule.contract import Outcome
from piloop.scoring import queue
from piloop.scoring.ledger import due, request_remeasure, requested
from piloop.scoring.verdict import write
from tests.conftest import ScriptRuntime, baseline, wait_until
from tests.daemon.test_evaluation_queue import evaluated, in_thread, queued_executor, rows, run_candidates
from tests.daemon.test_roles import PLUGIN, forget_loaded, installed
from tests.daemon.test_run_controller import controller, execute, make_run, set_config

CLIMB = (
    'n=$(wc -l < solve.txt); echo climb >> {log};'
    ' printf \'{{"rung": "screen", "metrics": {{"score": %d, "cost": 1}}}}\\n\' $((3 - n)) >> "$PILOOP_RESULT_FILE";'
    ' printf \'{{"metrics": {{"score": %d, "cost": 2}}}}\\n\' $n >> "$PILOOP_RESULT_FILE"'
)
"""An evaluate that logs each climb to ``log``, and reports a screen that disagrees with the full panel: a line more
scores 1 less at the screen, and 1 more in full."""
SCORE = {'name': 'score', 'direction': 'higher', 'role': 'primary'}
COST = {'name': 'cost', 'direction': 'lower'}
SCREEN = {'name': 'screen', 'stop': {'score': {'margin': 0.5}}}


def laddered(db: Database, campaign: Campaign, tmp_path: Path, *, hold: bool = False, **overrides: object) -> Path:
    """Give the campaign the ``CLIMB`` evaluate and a screen, held while ``tmp_path/hold`` exists. Return its log."""
    log = tmp_path / 'climbs'
    held = f'git rev-parse HEAD >> {tmp_path / "evaluated"}; while [ -f {tmp_path / "hold"} ]; do sleep 0.05; done; '
    script = (held if hold else '') + CLIMB.format(log=log)
    set_config(
        db, campaign, **{'evaluate': ['sh', '-c', script], 'metrics': [SCORE, COST], 'ladder': [SCREEN]} | overrides
    )
    return log


def climbs(log: Path) -> int:
    return len(log.read_text().split()) if log.exists() else 0


def rungs_of(db: Database, candidate_id: CandidateId) -> list[tuple[str | None, str | None, dict[str, float]]]:
    with db.session() as s:
        return [(m.rung, m.stopped, m.metrics) for m in s.get_one(Candidate, candidate_id).measurements]


@pytest.mark.parametrize('repeats', [None, 3])
def test_a_run_stops_its_version_at_a_rung_into_a_discard_and_a_remeasure_climbs_in_full(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, repeats: int | None
) -> None:
    """A paired track runs the bar after the candidate's first sample, so a stop there ends the job alone."""
    log = laddered(db, campaign, tmp_path, metrics=[SCORE | ({'repeats': repeats} if repeats else {}), COST])
    ex = controller(db, runtime, tmp_path)
    ex.remeasure(baseline(db, campaign.id).id)  # the new evaluator measures the head first, as the daemon does
    assert rungs_of(db, baseline(db, campaign.id).id)[-2:] == [
        ('screen', None, {'score': 2.0, 'cost': 1.0}),
        (None, None, {'score': 1.0, 'cost': 2.0}),
    ], 'a reference climbs every rung, whatever it measures there'
    run_id = make_run(db, campaign)
    execute(ex, db, run_id)
    assert climbs(log) == 2, 'the head, then one climb that stopped: no bar ran next to it'
    [c] = run_candidates(db, run_id)
    assert rungs_of(db, c.id) == [('screen', 'score 1 vs 2 (-1; margin 0.5)', {'score': 1.0, 'cost': 1.0})]
    with db.session() as s:
        candidate = s.get_one(Candidate, c.id)
        assert (candidate.verdict, candidate.verdict_reason) == (
            Outcome.DISCARD,
            'stopped at screen: score 1 vs 2 (-1; margin 0.5)',
        )
        request_remeasure(s, candidate, System.HUMAN, repeats=1)
        s.commit()
    ex.remeasure(c.id)
    assert [(rung, stopped) for rung, stopped, _ in rungs_of(db, c.id)][1:] == [('screen', None), (None, None)], (
        'what someone asked for climbs every rung'
    )
    with db.session() as s:
        candidate = s.get_one(Candidate, c.id)
        assert (candidate.verdict, candidate.verdict_reason) == snapshot(
            (Outcome.KEEP, 'against candidate#1: score 2 vs 1 (+1; 95% worst case +1 over 1 pairs)')
        )
        assert requested(s, s.get_one(Campaign, campaign.id)) == []


def test_a_rung_with_its_own_command_runs_first_and_its_stop_spares_the_evaluate_command(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    smoke = {
        'name': 'smoke',
        'evaluate': ['sh', '-c', 'echo \'{"metrics": {"score": 0, "cost": 0.5}}\' >> "$PILOOP_RESULT_FILE"'],
        'stop': {'score': {'floor': 1}},
    }
    log = laddered(db, campaign, tmp_path)
    ex = controller(db, runtime, tmp_path)
    ex.remeasure(baseline(db, campaign.id).id)
    laddered(db, campaign, tmp_path, ladder=[smoke, SCREEN])
    ex.remeasure(baseline(db, campaign.id).id)
    assert climbs(log) == 2, 'a reference runs past a stop that holds'
    run_id = make_run(db, campaign)
    execute(ex, db, run_id)
    [c] = run_candidates(db, run_id)
    assert climbs(log) == 2, 'the smoke stopped the climb: the evaluate command never ran'
    assert rungs_of(db, c.id) == [('smoke', 'score 0 past its floor 1', {'score': 0.0, 'cost': 0.5})]


def test_a_keep_that_lands_during_the_climb_stays_and_is_measured_in_full_as_a_head(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    laddered(db, campaign, tmp_path)
    ex = controller(db, runtime, tmp_path)
    ex.remeasure(baseline(db, campaign.id).id)
    measure = ex._measure

    def kept_meanwhile(
        run: RunId | None,
        campaign_id: CampaignId,
        cfg: CampaignConfig,
        sha: Revision,
        candidate_id: CandidateId,
        evaluator_id: EvaluatorId,
        eval_dir: Path,
        *,
        bar: CandidateId | None,
        repeats: int,
        gate: CandidateId | None,
    ) -> bool:
        args = (run, campaign_id, cfg, sha, candidate_id, evaluator_id, eval_dir)
        done = measure(*args, bar=bar, repeats=repeats, gate=gate)
        with db.session() as s:
            write(s.get_one(Candidate, candidate_id), Outcome.KEEP, cause=None, reason='kept by hand', by=System.HUMAN)
            s.commit()
        return done

    monkeypatch.setattr(ex, '_measure', kept_meanwhile)
    run_id = make_run(db, campaign)
    execute(ex, db, run_id)
    [c] = run_candidates(db, run_id)
    with db.session() as s:
        candidate = s.get_one(Candidate, c.id)
        assert (candidate.verdict, candidate.verdict_reason) == (Outcome.KEEP, 'kept by hand')
        assert [m.rung for m in candidate.measurements] == ['screen']
        assert candidate in due(s, s.get_one(Campaign, campaign.id)), 'a head with only a stopped climb is measured'


def test_two_runs_that_reach_one_tree_share_one_climb(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    laddered(db, campaign, tmp_path, hold=True)
    ex = queued_executor(db, runtime, tmp_path, slots=2)
    ex.remeasure(baseline(db, campaign.id).id)
    (tmp_path / 'hold').touch()
    waits = mocker.spy(queue, 'request')
    first, second = make_run(db, campaign), make_run(db, campaign, title='the same line')
    one = in_thread(lambda: execute(ex, db, first))
    wait_until(lambda: len(evaluated(tmp_path)) == 2, 'the first run climbs')
    two = in_thread(lambda: execute(ex, db, second))
    wait_until(lambda: waits.call_count == 2, 'the second request')
    (tmp_path / 'hold').unlink()
    one.join(30)
    two.join(30)
    [candidate] = run_candidates(db, first)
    with db.session() as s:
        shared = s.get_one(Candidate, candidate.id)
        assert ([m.rung for m in shared.measurements], shared.verdict) == (['screen'], Outcome.DISCARD)
    assert len(evaluated(tmp_path)) == 2, 'the head, then the tree once'
    assert rows(db) == []


def test_a_remeasure_that_joins_a_stopped_climb_is_asked_again_and_then_climbs_in_full(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """A run that reaches a stopped tree again climbs it again. A remeasure that joins that evaluation gets the stop,
    which leaves the request open, so the daemon asks again, and that climb goes to the full panel."""
    laddered(db, campaign, tmp_path, hold=True)
    ex = queued_executor(db, runtime, tmp_path, slots=2)
    ex.remeasure(baseline(db, campaign.id).id)
    run_id = make_run(db, campaign)
    execute(ex, db, run_id)
    [c] = run_candidates(db, run_id)
    with db.session() as s:
        current = s.get_one(Campaign, campaign.id)
        cfg, evaluator = current.config, current.evaluator_id
        assert evaluator is not None
    (tmp_path / 'hold').touch()
    waits = mocker.spy(queue, 'request')
    bar = baseline(db, campaign.id).id
    again = in_thread(
        lambda: ex._measure(run_id, campaign.id, cfg, c.revision, c.id, evaluator, tmp_path / 'again', gate=bar)
    )
    wait_until(lambda: len(evaluated(tmp_path)) == 3, 'the climb again')
    with db.session() as s:
        request_remeasure(s, s.get_one(Candidate, c.id), System.HUMAN, repeats=1)
        s.commit()
    joined = in_thread(lambda: ex.remeasure(c.id))
    wait_until(lambda: waits.call_count == 2, 'the remeasure joins')
    (tmp_path / 'hold').unlink()
    again.join(30)
    joined.join(30)
    with db.session() as s:
        assert [m.rung for m in s.get_one(Candidate, c.id).measurements] == ['screen', 'screen']
        assert [r.id for r, _ in requested(s, s.get_one(Campaign, campaign.id))] == [c.id], 'still asked'
    ex.remeasure(c.id)
    with db.session() as s:
        assert [m.rung for m in s.get_one(Candidate, c.id).measurements][-2:] == ['screen', None]
        assert requested(s, s.get_one(Campaign, campaign.id)) == []


@pytest.fixture
def stop_rule(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    installed(monkeypatch, [EntryPoint('stop_is_interesting', f'{PLUGIN}:StopIsInteresting', 'piloop.verdict_rules')])
    yield
    forget_loaded()


@pytest.mark.usefixtures('stop_rule')
def test_the_verdict_rule_decides_what_a_stop_means(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    laddered(db, campaign, tmp_path, plugins={'verdict_rule': {'provider': 'stop_is_interesting'}})
    ex = controller(db, runtime, tmp_path)
    ex.remeasure(baseline(db, campaign.id).id)
    run_id = make_run(db, campaign)
    execute(ex, db, run_id)
    [c] = run_candidates(db, run_id)
    assert (c.verdict, c.verdict_reason) == (
        Outcome.INTERESTING,
        'stopped at screen, worth a full look: score 1 vs 2 (-1; margin 0.5)',
    )
