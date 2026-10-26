"""A ratio guardrail end to end: the evaluate command reports its parts, and piloop computes and judges the ratio."""

from pathlib import Path

from piloop.db.models import Campaign, Candidate
from piloop.db.session import Database
from piloop.plugins.verdict_rule.contract import Outcome
from piloop.scoring.ledger import metrics
from tests.conftest import ScriptRuntime, baseline
from tests.daemon.test_evaluation_queue import run_candidates
from tests.daemon.test_run_controller import controller, execute, make_run, set_config

TASKS = 40
EVALUATE = f"""
import json, os
lines = len(open('solve.txt').read().splitlines())
solved = {{f't{{i}}': float(i < 10 * lines) for i in range({TASKS})}}
cost = {{f't{{i}}': 0.5 * lines * lines for i in range({TASKS})}}
result = {{'metrics': {{'solved': sum(solved.values()), 'cost_usd': sum(cost.values())}},
          'units': {{'solved': solved, 'cost_usd': cost}}}}
open(os.environ['PILOOP_RESULT_FILE'], 'a').write(json.dumps(result) + '\\n')
"""
"""Each line of solve.txt solves 10 more of 40 tasks, and costs more per task than the one before."""
METRICS = [
    {'name': 'solved', 'direction': 'higher', 'role': 'primary'},
    {'name': 'cost_usd', 'direction': 'lower'},
    {
        'name': 'cost_per_solved',
        'direction': 'lower',
        'role': 'guardrail',
        'fraction': 0.5,
        'ratio': {'numerator': 'cost_usd', 'denominator': 'solved'},
    },
]


def test_a_ratio_guardrail_is_computed_from_its_parts_and_holds_back_a_costlier_keep(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    set_config(db, campaign, evaluate=['python3', '-c', EVALUATE], metrics=METRICS)
    ex = controller(db, runtime, tmp_path)
    ex.remeasure(baseline(db, campaign.id).id)
    run_id = make_run(db, campaign, track='solved')
    execute(ex, db, run_id)
    [c] = run_candidates(db, run_id)
    with db.session() as s:
        candidate = s.get_one(Candidate, c.id)
        evaluator = s.get_one(Campaign, campaign.id).evaluator_id
        assert metrics(candidate, evaluator) == {'solved': 20.0, 'cost_usd': 80.0, 'cost_per_solved': 4.0}
        assert 'cost_per_solved' not in candidate.measurements[-1].metrics, 'piloop computes it; nothing stores it'
        assert candidate.verdict is Outcome.INTERESTING
        assert candidate.verdict_reason is not None
        assert 'cost_per_solved 4 vs 2 (+2; 95% worst case' in candidate.verdict_reason
        assert 'beyond its fraction 0.5' in candidate.verdict_reason
