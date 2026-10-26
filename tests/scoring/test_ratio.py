"""Ratio metrics: piloop computes ``numerator / denominator`` from what the evaluate command reports, and resamples it."""

import re
from pathlib import Path

import pytest
from inline_snapshot import snapshot

from piloop.config import ConfigError, Ratio, load_campaign_config
from piloop.plugins.metric import Direction
from piloop.plugins.verdict_rule import bootstrap
from piloop.plugins.verdict_rule.bootstrap import Bootstrap, ratio_bound
from piloop.plugins.verdict_rule.contract import Case, Criterion, Outcome, Ratio as Parts, Side
from piloop.scoring.ledger import derive

SOLVED = {'name': 'solved', 'direction': 'higher', 'role': 'primary'}
COST = {'name': 'cost_usd', 'direction': 'lower'}
PER_SOLVED = {
    'name': 'cost_per_solved',
    'direction': 'lower',
    'role': 'guardrail',
    'fraction': 0.05,
    'ratio': {'numerator': 'cost_usd', 'denominator': 'solved'},
}


def test_a_ratio_is_computed_not_reported_and_its_parts_are_required(campaign_path: Path) -> None:
    cfg = load_campaign_config(campaign_path, metrics=[SOLVED, COST, PER_SOLVED])
    assert (cfg.reported, cfg.required) == (['solved', 'cost_usd'], ['solved', 'cost_usd'])
    assert cfg.evaluator_definition['judging'] == [
        {'name': 'cost_per_solved', 'direction': 'lower', 'ratio': {'numerator': 'cost_usd', 'denominator': 'solved'}},
        {'name': 'solved', 'direction': 'higher'},
    ]
    other = {**PER_SOLVED, 'ratio': {'numerator': 'cost_usd', 'denominator': 'tasks'}}
    tasks = {'name': 'tasks', 'direction': 'higher'}
    assert load_campaign_config(campaign_path, metrics=[SOLVED, COST, tasks, other]).evaluator_key != cfg.evaluator_key


@pytest.mark.parametrize(
    ('metrics', 'error'),
    [
        ([SOLVED, PER_SOLVED], "'cost_per_solved': ratio of ['cost_usd'], which are not metrics that evaluate reports"),
        (
            [SOLVED, COST, {**PER_SOLVED, 'ratio': {'numerator': 'cost_usd', 'denominator': 'cost_usd'}}],
            "'cost_per_solved': ratio of 'cost_usd' by itself",
        ),
        (
            [SOLVED, COST, {**PER_SOLVED, 'role': 'watch', 'fraction': None}],
            "'cost_per_solved': ratio applies to primaries and guardrails only",
        ),
    ],
)
def test_a_ratio_divides_two_reported_metrics(
    campaign_path: Path, metrics: list[dict[str, object]], error: str
) -> None:
    with pytest.raises(ConfigError, match=re.escape(error)):
        load_campaign_config(campaign_path, metrics=metrics)


def test_no_stop_of_the_ladder_reads_a_ratio(campaign_path: Path) -> None:
    ladder = [{'name': 'screen', 'stop': {'cost_per_solved': {'margin': 0.1}}}]
    with pytest.raises(ConfigError, match=re.escape("ladder.screen.stop: ['cost_per_solved'] are ratios")):
        load_campaign_config(campaign_path, metrics=[SOLVED, COST, PER_SOLVED], ladder=ladder)


def test_a_ratio_is_the_ratio_of_the_means_and_needs_a_denominator() -> None:
    found = {'cost_per_solved': Ratio(numerator='cost_usd', denominator='solved')}
    assert derive({'cost_usd': 3.0, 'solved': 6.0}, found) == {'cost_usd': 3.0, 'solved': 6.0, 'cost_per_solved': 0.5}
    assert derive({'cost_usd': 3.0, 'solved': 0.0}, found) == {'cost_usd': 3.0, 'solved': 0.0}
    assert derive({'cost_usd': 3.0}, found) == {'cost_usd': 3.0}


CRITERION = Criterion('cost_per_solved', Direction.LOWER, fraction=0.05, ratio=Parts('cost_usd', 'solved'))


def side(name: str, cost: list[float], solved: list[float]) -> Side:
    """A side with per-task units: each task's cost, and whether it was solved."""
    units = {'cost_usd': {f't{i}': c for i, c in enumerate(cost)}, 'solved': {f't{i}': s for i, s in enumerate(solved)}}
    means = derive(
        {'cost_usd': sum(cost), 'solved': sum(solved)},
        {'cost_per_solved': Ratio(numerator='cost_usd', denominator='solved')},
    )
    return Side(name, means, units)


def test_a_ratio_is_resampled_over_the_units_of_its_parts() -> None:
    bar = side('bar', [1.0] * 40, [1.0] * 20 + [0.0] * 20)
    cheaper = side('candidate', [0.8] * 40, [1.0] * 20 + [0.0] * 20)
    c = ratio_bound(CRITERION, Parts('cost_usd', 'solved'), cheaper, bar, 0.95)
    assert c is not None and (c.mine, c.theirs, c.count) == (1.6, 2.0, 40) and c.worst > 0
    pricier = side('candidate', [1.0] * 40, [1.0] * 18 + [0.0] * 22)
    c = ratio_bound(CRITERION, Parts('cost_usd', 'solved'), pricier, bar, 0.95)
    assert c is not None and c.worst < 0


def test_a_ratio_without_units_of_both_parts_falls_back_to_the_aggregates() -> None:
    bar = side('bar', [1.0] * 4, [1.0] * 4)
    bare = Side('candidate', bar.means, {'cost_usd': bar.units['cost_usd']})
    assert ratio_bound(CRITERION, Parts('cost_usd', 'solved'), bare, bar, 0.95) is None
    shifted = Side('candidate', bar.means, {**bar.units, 'solved': {'other': 4.0}})
    assert ratio_bound(CRITERION, Parts('cost_usd', 'solved'), shifted, bar, 0.95) is None


def test_a_resample_with_nothing_solved_counts_as_the_worst_gain(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(bootstrap, 'RESAMPLES', 20)
    bar = side('bar', [1.0, 1.0], [1.0, 1.0])
    rare = side('candidate', [1.0, 1.0], [1.0, 0.0])
    c = ratio_bound(CRITERION, Parts('cost_usd', 'solved'), rare, bar, 0.95)
    assert c is not None and c.worst == float('-inf')


def test_a_ratio_guardrail_holds_a_keep_back_on_its_resampled_worst_case() -> None:
    primary = Criterion('solved', Direction.HIGHER)
    bar = side('candidate#1', [1.0] * 40, [1.0] * 20 + [0.0] * 20)
    more = side('candidate#2', [1.8] * 40, [1.0] * 30 + [0.0] * 10)
    judged = Bootstrap().decide(Case(primary, (CRITERION,), more, bar))
    assert (judged.verdict, judged.reason) == snapshot(
        (
            Outcome.INTERESTING,
            'against candidate#1: solved 30 vs 20 (+10; 95% worst case +6 over 40 units); guardrails: cost_per_solved 2.4 vs 2 (+0.4; 95% worst case +0.7692 over 40 units) beyond its fraction 0.05',
        )
    )
