"""The verdict rule on numbers alone: primary superiority by paired cluster bootstrap, guardrail non-inferiority.

Each verdict goes through the host's side of the contract (``ledger.decide``): the track and the confidence of the
config become a ``Case`` of plain data, and the built-in rule's ``Judgement`` becomes the verdict of the database.
"""

import ast
import json
import sys
from pathlib import Path

from inline_snapshot import snapshot
from piloop_min_gain import MIN_GAIN

from piloop.config import CampaignConfig, Metric, MetricRole, Track, TrackConfig
from piloop.db.models import Verdict
from piloop.plugins.host import data_errors
from piloop.plugins.metric import Direction
from piloop.plugins.verdict_rule import bootstrap as verdict_module
from piloop.plugins.verdict_rule.bootstrap import BOOTSTRAP, compare, lower_bound
from piloop.plugins.verdict_rule.contract import Outcome, Side, VerdictRule
from piloop.scoring import ledger
from piloop.scoring.evaluate import Units
from piloop.scoring.ledger import criterion, same

BAR = 'candidate#73'


def decide(
    track: Track,
    confidence: float,
    mine: dict[str, float],
    theirs: dict[str, float],
    mine_units: Units,
    theirs_units: Units,
    *,
    bar: str,
) -> tuple[Verdict, str]:
    """The built-in rule's verdict on a candidate of ``track`` against ``bar``, as the daemon asks for it."""
    return ledger.decide(
        BOOTSTRAP, track, confidence, Side('candidate#74', mine, mine_units), Side(bar, theirs, theirs_units)
    )


def config(*guardrails: Metric, confidence: float = 0.95) -> CampaignConfig:
    return CampaignConfig(
        name='v',
        metrics=[Metric(name='accuracy', direction=Direction.HIGHER, role=MetricRole.PRIMARY), *guardrails],
        evaluate=['true'],
        model={'pi': 'm'},
        confidence=confidence,
    )


def rule(cfg: CampaignConfig) -> tuple[Track, float]:
    """What ``decide`` takes of a one-track config: its track and the confidence."""
    return cfg.all_tracks[0], cfg.confidence


def records(diffs: list[int], base: int = 1) -> tuple[dict[str, float], dict[str, float]]:
    """Per-record values of the bar and of a candidate that differs from it by ``diffs``."""
    theirs = {f'r{i:02}': float(base) for i in range(len(diffs))}
    mine = {f'r{i:02}': base + float(d) for i, d in enumerate(diffs)}
    return mine, theirs


def scored(mine: dict[str, float], theirs: dict[str, float]) -> tuple[dict[str, float], dict[str, float], Units, Units]:
    return (
        {'accuracy': sum(mine.values())},
        {'accuracy': sum(theirs.values())},
        {'accuracy': mine},
        {'accuracy': theirs},
    )


CONCENTRATED = [7, 6, 6] + [2] * 16 + [-2] * 14 + [1] + [0] * 35
"""A gain of +24 on 69 records: 19 of it from 3 records, and the other 5 left after gains and losses cancel out."""
BROAD = [2] * 16 + [1] * 4 + [-1] * 12 + [0] * 37
"""The same +24 on 69 records, spread over 20 of them."""


def test_a_gain_carried_by_three_units_is_interesting_not_keep() -> None:
    assert (len(CONCENTRATED), sum(CONCENTRATED), sum(CONCENTRATED[:3])) == (69, 24, 19)
    verdict, reason = decide(*rule(config()), *scored(*records(CONCENTRATED)), bar=BAR)
    assert verdict is Verdict.INTERESTING
    assert reason == snapshot(
        'against candidate#73: accuracy 93 vs 69 (+24; 95% worst case -1 over 69 units): the gain is not significant'
    )


def test_the_same_gain_spread_over_many_units_keeps() -> None:
    assert (len(BROAD), sum(BROAD)) == (69, 24)
    verdict, reason = decide(*rule(config()), *scored(*records(BROAD)), bar=BAR)
    assert verdict is Verdict.KEEP
    assert reason == snapshot('against candidate#73: accuracy 93 vs 69 (+24; 95% worst case +10 over 69 units)')


def test_the_bootstrap_is_seeded_so_a_verdict_is_reproducible() -> None:
    assert [lower_bound([float(d) for d in CONCENTRATED], 0.95) for _ in range(3)] == [-1.0, -1.0, -1.0]
    assert lower_bound([float(d) for d in CONCENTRATED], 0.80) > lower_bound([float(d) for d in CONCENTRATED], 0.95)
    verdict, _ = decide(*rule(config(confidence=0.8)), *scored(*records(CONCENTRATED)), bar=BAR)
    assert verdict is Verdict.KEEP, 'a laxer confidence admits the concentrated gain'


def test_without_units_the_aggregates_decide_and_the_reason_says_so() -> None:
    assert decide(*rule(config()), {'accuracy': 93}, {'accuracy': 69}, {}, {}, bar=BAR) == snapshot(
        (
            Verdict.KEEP,
            'against candidate#73: accuracy 93 vs 69 (+24; aggregate: no per-unit results)',
        )
    )
    mine, theirs = records(CONCENTRATED)
    assert decide(*rule(config()), {'accuracy': 93}, {'accuracy': 69}, {'accuracy': mine}, {}, bar=BAR)[1] == snapshot(
        'against candidate#73: accuracy 93 vs 69 (+24; aggregate: no per-unit results for the bar)'
    )
    assert decide(*rule(config()), {'accuracy': 93}, {'accuracy': 69}, {}, {'accuracy': theirs}, bar=BAR)[1] == (
        'against candidate#73: accuracy 93 vs 69 (+24; aggregate: no per-unit results for the candidate)'
    )
    del mine['r00']
    assert decide(
        *rule(config()), {'accuracy': 93}, {'accuracy': 69}, {'accuracy': mine}, {'accuracy': theirs}, bar=BAR
    )[1] == snapshot('against candidate#73: accuracy 93 vs 69 (+24; aggregate: the unit sets differ)')


def test_no_gain_or_a_loss_discards() -> None:
    assert decide(*rule(config()), {'accuracy': 69}, {'accuracy': 69}, {}, {}, bar=BAR) == snapshot(
        (Verdict.DISCARD, 'against candidate#73: accuracy 69 vs 69 (+0; aggregate: no per-unit results)')
    )
    assert decide(*rule(config()), *scored(*records([-1, 0, 0])), bar=BAR) == snapshot(
        (Verdict.DISCARD, 'against candidate#73: accuracy 2 vs 3 (-1; 95% worst case -2 over 3 units)')
    )


def test_a_crash_and_an_unscored_bar() -> None:
    assert decide(*rule(config()), {}, {'accuracy': 1}, {}, {}, bar=BAR) == (Verdict.CRASH, 'no successful measurement')
    assert decide(*rule(config()), {'accuracy': 1}, {}, {}, {}, bar=BAR) == snapshot(
        (Verdict.KEEP, 'candidate#73 has no score under the current evaluator')
    )


COVERAGE = Metric(name='coverage', direction=Direction.HIGHER, role=MetricRole.GUARDRAIL, margin=10, floor=1000)
ERRORS = Metric(name='errors', direction=Direction.LOWER, role=MetricRole.GUARDRAIL, margin=50, floor=450)


def test_a_floor_breach_discards_whatever_the_primary_did() -> None:
    """A guardrail past its floor discards the candidate, even when the primary gained a lot."""
    mine = {'accuracy': 200.0, 'coverage': 990.0, 'errors': 745.0}
    theirs = {'accuracy': 80.0, 'coverage': 1000.0, 'errors': 300.0}
    assert decide(*rule(config(COVERAGE, ERRORS)), mine, theirs, {}, {}, bar=BAR) == snapshot(
        (Verdict.DISCARD, 'floor: coverage 990 past its floor 1000; errors 745 past its floor 450')
    )
    on_floor = {'accuracy': 200.0, 'coverage': 1000.0, 'errors': 450.0}
    assert decide(*rule(config(COVERAGE, ERRORS)), on_floor, {}, {}, {}, bar=BAR)[0] is Verdict.KEEP, (
        'the floor itself is allowed, and floors apply even against an unscored bar'
    )


def test_a_guardrail_regression_within_its_margin_keeps_beyond_it_is_interesting() -> None:
    theirs = {'accuracy': 80.0, 'coverage': 1020.0, 'errors': 300.0}
    within = {'accuracy': 90.0, 'coverage': 1010.0, 'errors': 350.0}
    assert decide(*rule(config(COVERAGE, ERRORS)), within, theirs, {}, {}, bar=BAR) == snapshot(
        (
            Verdict.KEEP,
            'against candidate#73: accuracy 90 vs 80 (+10; aggregate: no per-unit results); guardrails: coverage 1010 vs 1020 (-10; aggregate: no per-unit results) within its margin 10; errors 350 vs 300 (+50; aggregate: no per-unit results) within its margin 50',
        )
    )
    beyond = {'accuracy': 90.0, 'coverage': 1009.0, 'errors': 351.0}
    assert decide(*rule(config(COVERAGE, ERRORS)), beyond, theirs, {}, {}, bar=BAR) == snapshot(
        (
            Verdict.INTERESTING,
            'against candidate#73: accuracy 90 vs 80 (+10; aggregate: no per-unit results); guardrails: coverage 1009 vs 1020 (-11; aggregate: no per-unit results) beyond its margin 10; errors 351 vs 300 (+51; aggregate: no per-unit results) beyond its margin 50',
        )
    )


def test_a_guardrail_with_units_is_tested_on_its_bootstrap_worst_case() -> None:
    """The point regression is inside the margin, but one unit carries it all: the worst case is beyond."""
    loose = Metric(name='errors', direction=Direction.LOWER, role=MetricRole.GUARDRAIL, margin=6)
    mine_accuracy, theirs_accuracy = records([1] * 20)
    mine_errors = {f'r{i:02}': (5.0 if i == 0 else 0.0) for i in range(20)}
    theirs_errors = {f'r{i:02}': 0.0 for i in range(20)}
    verdict, reason = decide(
        *rule(config(loose)),
        {'accuracy': 40, 'errors': 5},
        {'accuracy': 20, 'errors': 0},
        {'accuracy': mine_accuracy, 'errors': mine_errors},
        {'accuracy': theirs_accuracy, 'errors': theirs_errors},
        bar=BAR,
    )
    assert verdict is Verdict.INTERESTING
    assert reason == snapshot(
        'against candidate#73: accuracy 40 vs 20 (+20; 95% worst case +20 over 20 units); guardrails: errors 5 vs 0 (+5; 95% worst case +15 over 20 units) beyond its margin 6'
    )
    untouched = decide(
        *rule(config(loose)),
        {'accuracy': 40, 'errors': 0},
        {'accuracy': 20, 'errors': 0},
        {'accuracy': mine_accuracy, 'errors': theirs_errors},
        {'accuracy': theirs_accuracy, 'errors': theirs_errors},
        bar=BAR,
    )
    assert untouched[0] is Verdict.KEEP, 'paired: units that did not move add no noise'


def test_a_guardrail_neither_side_measured_holds_the_keep_back() -> None:
    assert decide(*rule(config(ERRORS)), {'accuracy': 2}, {'accuracy': 1}, {}, {}, bar=BAR) == snapshot(
        (
            Verdict.INTERESTING,
            'against candidate#73: accuracy 2 vs 1 (+1; aggregate: no per-unit results); guardrails: errors unmeasured',
        )
    )


def test_a_lower_is_better_primary_reads_in_its_own_direction() -> None:
    cfg = CampaignConfig(
        name='v',
        metrics=[Metric(name='loss', direction=Direction.LOWER, role=MetricRole.PRIMARY)],
        evaluate=['true'],
        model={'pi': 'm'},
    )
    theirs = {f'u{i}': 1.0 for i in range(30)}
    mine = {f'u{i}': 0.5 for i in range(30)}
    assert decide(*rule(cfg), {'loss': 15}, {'loss': 30}, {'loss': mine}, {'loss': theirs}, bar=BAR) == snapshot(
        (Verdict.KEEP, 'against candidate#73: loss 15 vs 30 (-15; 95% worst case -15 over 30 units)')
    )
    assert compare(criterion(cfg.all_tracks[0].primary), 15, 30, mine, theirs, 0.95).gain == 15


def test_a_lower_is_better_metric_that_did_not_move_reads_plus_zero() -> None:
    loss = Metric(name='loss', direction=Direction.LOWER, role=MetricRole.GUARDRAIL, margin=1)
    same_units = {f'u{i}': 1.0 for i in range(3)}
    assert (
        compare(criterion(loss), 3, 3, None, None, 0.95).text(0.95)
        == 'loss 3 vs 3 (+0; aggregate: no per-unit results)'
    )
    assert compare(criterion(loss), 3, 3, same_units, same_units, 0.95).text(0.95) == (
        'loss 3 vs 3 (+0; 95% worst case +0 over 3 units)'
    )


def test_same_means_every_deciding_metric_equal_and_ignores_watch_metrics() -> None:
    """Only the primary and the guardrails decide: a watch metric such as ``eval_seconds`` can differ."""
    guard = Metric(name='errors', direction=Direction.LOWER, role=MetricRole.GUARDRAIL, margin=1)
    watch = Metric(name='eval_seconds', direction=Direction.LOWER, role=MetricRole.WATCH)
    cfg = config(guard, watch)
    parent = {'accuracy': 10.0, 'errors': 2.0, 'eval_seconds': 63.1}
    assert same(cfg.all_tracks[0], {'accuracy': 10.0, 'errors': 2.0, 'eval_seconds': 613.1}, parent)
    assert not same(cfg.all_tracks[0], {'accuracy': 10.0, 'errors': 3.0, 'eval_seconds': 63.1}, parent), (
        'a guardrail moved'
    )
    assert not same(cfg.all_tracks[0], {'accuracy': 11.0, 'errors': 2.0}, parent), 'the primary moved'
    assert not same(cfg.all_tracks[0], {}, {}), 'nothing measured is no no-op: it is a crash or a skip'


def test_every_other_track_primary_is_a_guardrail_with_its_margin_and_floor() -> None:
    """A serving candidate must hold accuracy within its margin and above its floor; accuracy must hold cost too."""
    cfg = CampaignConfig(
        name='v',
        metrics=[
            Metric(
                name='acc', direction=Direction.HIGHER, role=MetricRole.PRIMARY, track='accuracy', margin=2, floor=90
            ),
            Metric(name='cost', direction=Direction.LOWER, role=MetricRole.PRIMARY, track='serving', margin=1),
            ERRORS,
        ],
        tracks={'accuracy': TrackConfig(share=0.75), 'serving': TrackConfig(share=0.25)},
        evaluate=['true'],
        model={'pi': 'm'},
    )
    serving, accuracy = cfg.track('serving'), cfg.track('accuracy')
    assert ([m.name for m in serving.guardrails], [m.name for m in accuracy.guardrails]) == (
        ['errors', 'acc'],
        ['errors', 'cost'],
    ), 'the campaign guardrails first, then the other tracks primaries'
    theirs = {'acc': 95.0, 'cost': 10.0, 'errors': 300.0}
    neutral = {'acc': 94.0, 'cost': 8.0, 'errors': 300.0}
    assert decide(serving, 0.95, neutral, theirs, {}, {}, bar=BAR) == snapshot(
        (
            Verdict.KEEP,
            'against candidate#73: cost 8 vs 10 (-2; aggregate: no per-unit results); guardrails: errors 300 vs 300 (+0; aggregate: no per-unit results) within its margin 50; acc 94 vs 95 (-1; aggregate: no per-unit results) within its margin 2',
        )
    )
    costly = {'acc': 92.0, 'cost': 8.0, 'errors': 300.0}
    assert decide(serving, 0.95, costly, theirs, {}, {}, bar=BAR)[0] is Verdict.INTERESTING, 'accuracy beyond margin'
    floored = {'acc': 89.0, 'cost': 1.0, 'errors': 300.0}
    assert decide(serving, 0.95, floored, {**theirs, 'acc': 89.5}, {}, {}, bar=BAR) == snapshot(
        (Verdict.DISCARD, 'floor: acc 89 past its floor 90')
    )
    pricier = {'acc': 99.0, 'cost': 11.5, 'errors': 300.0}
    assert decide(accuracy, 0.95, pricier, theirs, {}, {}, bar=BAR)[0] is Verdict.INTERESTING, 'cost beyond margin'
    assert decide(accuracy, 0.95, {**pricier, 'cost': 10.5}, theirs, {}, {}, bar=BAR)[0] is Verdict.KEEP
    assert same(serving, {**theirs, 'eval_seconds': 3.0}, theirs)
    assert not same(serving, {**theirs, 'acc': 96.0}, theirs), "another track's primary decides too"


def test_every_guardrail_past_its_margin_is_in_the_reason_whatever_the_primary_did() -> None:
    """A flat primary, a gain that is not significant or a floor discard still names each guardrail past its margin.

    Otherwise the reason of a discard or an ``interesting`` hides a large regression of another metric.
    """
    cfg = config(COVERAGE, ERRORS)
    theirs = {'accuracy': 80.0, 'coverage': 1020.0, 'errors': 300.0}
    flat = {'accuracy': 80.0, 'coverage': 1005.0, 'errors': 420.0}
    assert decide(*rule(cfg), flat, theirs, {}, {}, bar=BAR) == snapshot(
        (
            Verdict.DISCARD,
            'against candidate#73: accuracy 80 vs 80 (+0; aggregate: no per-unit results); guardrails past their margin: coverage 1005 vs 1020 (-15; aggregate: no per-unit results) beyond its margin 10; errors 420 vs 300 (+120; aggregate: no per-unit results) beyond its margin 50',
        )
    )
    mine_accuracy, theirs_accuracy = records(CONCENTRATED)
    noisy = decide(
        *rule(cfg),
        {'accuracy': 93.0, 'coverage': 1020.0, 'errors': 420.0},
        {'accuracy': 69.0, 'coverage': 1020.0, 'errors': 300.0},
        {'accuracy': mine_accuracy},
        {'accuracy': theirs_accuracy},
        bar=BAR,
    )
    assert noisy == snapshot(
        (
            Verdict.INTERESTING,
            'against candidate#73: accuracy 93 vs 69 (+24; 95% worst case -1 over 69 units): the gain is not significant; guardrails past their margin: errors 420 vs 300 (+120; aggregate: no per-unit results) beyond its margin 50',
        )
    )
    floored = {'accuracy': 90.0, 'coverage': 990.0, 'errors': 420.0}
    assert decide(*rule(cfg), floored, theirs, {}, {}, bar=BAR) == snapshot(
        (
            Verdict.DISCARD,
            'floor: coverage 990 past its floor 1000; guardrails past their margin: errors 420 vs 300 (+120; aggregate: no per-unit results) beyond its margin 50',
        )
    )
    assert decide(*rule(cfg), floored, {}, {}, {}, bar=BAR) == snapshot(
        (Verdict.DISCARD, 'floor: coverage 990 past its floor 1000')
    ), 'an unscored bar has no margins to compare'
    within = {'accuracy': 80.0, 'coverage': 1015.0, 'errors': 320.0}
    assert decide(*rule(cfg), within, theirs, {}, {}, bar=BAR)[1] == snapshot(
        'against candidate#73: accuracy 80 vs 80 (+0; aggregate: no per-unit results)'
    ), 'a guardrail within its margin adds nothing to a discard'


def test_the_contract_of_a_rule_is_data_in_data_out_and_its_verdicts_are_verdicts() -> None:
    """A rule can move out of process or to a Wasm component: nothing live crosses its contract."""
    assert data_errors(VerdictRule) == []
    assert {Verdict(o) for o in Outcome} == {Verdict.KEEP, Verdict.INTERESTING, Verdict.DISCARD, Verdict.CRASH}, (
        'a rule judges; pending, reference, unmeasured and recovered are not judgements'
    )


def test_the_built_in_rule_imports_only_the_standard_library_its_contract_and_the_metric_types() -> None:
    """``bootstrap`` is a pure provider like a plugin's: it reads nothing of piloop's state."""
    tree = ast.parse(Path(verdict_module.__file__).read_text())
    imported = {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names} | {
        n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module is not None
    }
    outside = {m for m in imported if m.split('.')[0] not in sys.stdlib_module_names}
    assert outside == {'piloop.plugins.verdict_rule.contract', 'piloop.plugins.metric'}


def test_the_example_rule_min_gain_delegates_to_bootstrap_and_holds_back_a_small_gain() -> None:
    """``examples/plugins/min-gain``: it calls the built-in rule and changes one thing, through the host's contract."""
    track = rule(config(COVERAGE))[0]
    least = json.dumps({'min_gain': 2})

    def judged(mine: dict[str, float], theirs: dict[str, float], with_rule: VerdictRule) -> tuple[Verdict, str]:
        return ledger.decide(with_rule, track, 0.95, Side('candidate#74', mine, {}), Side(BAR, theirs, {}), least)

    bar = {'accuracy': 10.0, 'coverage': 1000.0}
    for mine, theirs in [
        ({'accuracy': 12.0, 'coverage': 1000.0}, bar),
        ({'accuracy': 9.0, 'coverage': 1000.0}, bar),
        ({'accuracy': 99.0, 'coverage': 990.0}, bar),
        ({'accuracy': 1.0}, {}),
        ({}, bar),
    ]:
        assert judged(mine, theirs, MIN_GAIN) == judged(mine, theirs, BOOTSTRAP), 'the built-in verdict stays'
    assert judged({'accuracy': 11.0, 'coverage': 1000.0}, bar, MIN_GAIN) == (
        Verdict.INTERESTING,
        'against candidate#73: accuracy 11 vs 10 (+1; aggregate: no per-unit results); guardrails: coverage 1000 vs'
        ' 1000 (+0; aggregate: no per-unit results) within its margin 10; the gain is below min_gain 2',
    )
