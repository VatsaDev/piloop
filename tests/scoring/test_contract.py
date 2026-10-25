"""Pin the public behaviour of piloop 0.1.0: the verdict's bootstrap and the evaluate command's output protocol.

Other tools and ports reproduce these values. Change a test here only together with a CHANGELOG entry.
"""

import json
import math
from pathlib import Path

import pytest

from piloop.plugins.metric import Direction
from piloop.plugins.verdict_rule.bootstrap import RESAMPLES, SEED, Over, compare, lower_bound
from piloop.plugins.verdict_rule.contract import Criterion
from piloop.scoring.evaluate import UNITS_SUM_TOLERANCE, Failed, Measured, parse_metrics, read_units, run_evaluate

DIFFS = [0.137, -0.052, 0.311, 0.009, -0.274, 0.198, 0.066, -0.013, 0.245, 0.102, -0.087, 0.029]


def test_the_bootstrap_draws_from_cpython_mersenne_twister_with_seed_0() -> None:
    """The lower bound is the sorted sum at index ``int((1 - confidence) * 4000)`` of ``Random(0).choices`` draws."""
    assert (RESAMPLES, SEED) == (4000, 0)
    assert lower_bound(DIFFS, 0.95) == -0.22500000000000003
    assert lower_bound(DIFFS, 0.9) == -0.030000000000000054
    assert lower_bound([1.5], 0.95) == 1.5, 'one unit: every resample is that unit'


def test_the_bootstrap_pairs_units_by_sorted_name_and_measures_gain_in_the_metric_direction() -> None:
    errors = Criterion('errors', Direction.LOWER)
    mine = {'b': 3.0, 'a': 5.0, 'd': 7.0, 'c': 0.0}
    theirs = {'a': 6.0, 'b': 2.0, 'c': 1.0, 'd': 9.0}
    c = compare(errors, 15.0, 18.0, mine, theirs, 0.95)
    assert (c.gain, c.worst, c.count, c.over) == (3.0, lower_bound([1.0, -1.0, 1.0, 2.0], 0.95), 4, Over.UNITS)
    assert c.worst == -1.0


def test_metric_lines_are_name_equals_value_and_everything_else_is_ignored() -> None:
    stdout = '\n'.join(
        [
            'METRIC accuracy=0.83',
            '  METRIC errors = 12  ',
            'METRIC bytes=3.69e6',
            'METRIC seconds=1',
            'METRIC seconds=0.52',
            'training done',
            'METRICS other=1',
            'metric lower=1',
            'METRIC broken=12%',
            'METRIC noequals 3',
        ]
    )
    assert parse_metrics(stdout) == {'accuracy': 0.83, 'errors': 12.0, 'bytes': 3690000.0, 'seconds': 0.52}, (
        'spaces around the name and the value are allowed; the last line of a name wins; a bad line is skipped'
    )


def test_metric_lines_with_a_non_finite_value_or_an_empty_name_are_skipped() -> None:
    stdout = '\n'.join(
        [
            'METRIC errors=3',
            'METRIC errors=nan',
            'METRIC seconds=inf',
            'METRIC bytes=-Infinity',
            'METRIC =1',
            'METRIC   = 2',
        ]
    )
    assert parse_metrics(stdout) == {'errors': 3.0}, (
        'nan and inf are not measurements, and a metric needs a name: such a line is skipped like any bad line'
    )


def test_a_failed_or_incomplete_evaluation_gives_an_error(tmp_path: Path) -> None:
    def evaluate(script: str) -> tuple[dict[str, float], str | None]:
        r = run_evaluate(
            ['sh', '-c', script], cwd=tmp_path, log_dir=tmp_path, timeout=5, required={'errors', 'seconds'}
        )
        match r:
            case Measured(metrics=metrics):
                return metrics, None
            case Failed() as failed:
                return failed.printed, failed.error

    assert evaluate('echo METRIC errors=3; echo METRIC seconds=2') == ({'errors': 3.0, 'seconds': 2.0}, None)
    assert evaluate('echo METRIC errors=3; echo boom >&2; exit 1') == ({'errors': 3.0}, 'evaluate exit 1: boom'), (
        'a non-zero exit keeps its METRIC lines next to the error: a failed evaluation has a cost too'
    )
    assert evaluate('echo METRIC errors=3') == ({'errors': 3.0}, 'evaluate printed no METRIC line for: seconds')


def test_a_units_file_maps_each_metric_to_finite_unit_values_that_sum_to_its_metric_line(tmp_path: Path) -> None:
    path = tmp_path / 'units.json'

    def units(content: object, metrics: dict[str, float]) -> dict[str, dict[str, float]]:
        path.write_text(json.dumps(content))
        return read_units(path, metrics)

    assert UNITS_SUM_TOLERANCE == 1e-3
    assert units({'errors': {'r1': 2, 'r2': 1.0004}}, {'errors': 3.0, 'seconds': 1.0}) == {
        'errors': {'r1': 2.0, 'r2': 1.0004}
    }, 'a relative difference up to 1e-3 is rounding; a metric without units is fine'
    with pytest.raises(ValueError, match="units of 'errors' sum to 3.004, its METRIC line says 3"):
        units({'errors': {'r1': 2, 'r2': 1.004}}, {'errors': 3.0})
    with pytest.raises(ValueError, match="units for 'seconds', which printed no METRIC line"):
        units({'seconds': {'r1': 1}}, {'errors': 3.0})
    with pytest.raises(ValueError, match="no units for 'errors'"):
        units({'errors': {}}, {'errors': 3.0})
    for bad in (math.nan, math.inf, '1', True):
        with pytest.raises(ValueError, match='expected'):
            units({'errors': {'r1': bad}}, {'errors': 1.0})
