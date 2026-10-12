import os
import time
from pathlib import Path

import pytest
from inline_snapshot import snapshot

from piloop.scoring.evaluate import EvalResult, parse_metrics, run_evaluate


def test_parse_metrics() -> None:
    out = 'noise\nMETRIC acc=0.83\nMETRIC loss = -1.5e-3\nMETRIC big=1E+20\nMETRICS bad=1\nMETRIC nan=x\nMETRIC=3\n'
    assert parse_metrics(out) == snapshot({'acc': 0.83, 'loss': -0.0015, 'big': 1e20})


def test_evaluate_ok(tmp_path: Path) -> None:
    r = run_evaluate(
        ['sh', '-c', 'echo METRIC a=1; echo METRIC b=2'], cwd=tmp_path, log_dir=tmp_path, timeout=5, required={'a'}
    )
    assert (r.metrics, r.error) == ({'a': 1.0, 'b': 2.0}, None)
    assert (tmp_path / 'eval.stdout').read_text() == 'METRIC a=1\nMETRIC b=2\n'


def test_evaluate_nonzero_exit_drops_metrics(tmp_path: Path) -> None:
    r = run_evaluate(
        ['sh', '-c', 'echo METRIC a=1; echo boom >&2; exit 2'],
        cwd=tmp_path,
        log_dir=tmp_path,
        timeout=5,
        required={'a'},
    )
    assert r.metrics == {}
    assert r.error == snapshot('evaluate exit 2: boom')


def test_evaluate_missing_metric(tmp_path: Path) -> None:
    r = run_evaluate(['sh', '-c', 'echo METRIC a=1'], cwd=tmp_path, log_dir=tmp_path, timeout=5, required={'a', 'b'})
    assert r.error == snapshot('evaluate printed no METRIC line for: b')


def test_evaluate_timeout_kills_grandchildren_and_keeps_logs(tmp_path: Path) -> None:
    cmd = ['sh', '-c', 'echo partial; sleep 30 & echo $! > child.pid; wait']
    r = run_evaluate(cmd, cwd=tmp_path, log_dir=tmp_path, timeout=0.3, required=set())
    assert r.error == snapshot('evaluate timeout after 0.3s')
    assert (tmp_path / 'eval.stdout').read_text() == 'partial\n'
    assert_dead(int((tmp_path / 'child.pid').read_text()))


def assert_dead(pid: int) -> None:
    time.sleep(0.2)  # let init reap the orphan
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


UNITS_SCRIPT = 'echo METRIC a=3; echo METRIC b=1; printf %s "$UNITS" > "$PILOOP_UNITS_FILE"'


def evaluate_with_units(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, units: str) -> tuple[EvalResult, Path]:
    monkeypatch.setenv('UNITS', units)
    path = tmp_path / 'u.pending'
    r = run_evaluate(
        ['sh', '-c', UNITS_SCRIPT], cwd=tmp_path, log_dir=tmp_path, timeout=5, required={'a', 'b'}, units_file=path
    )
    return r, path


def test_the_daemon_evaluation_gets_a_units_file_and_takes_it_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    r, path = evaluate_with_units(tmp_path, monkeypatch, '{"a": {"x": 1, "y": 2.0}}')
    assert (r.metrics, r.error, r.units) == ({'a': 3.0, 'b': 1.0}, None, {'a': {'x': 1.0, 'y': 2.0}})
    assert not path.exists(), 'the file never outlives the evaluation'


def test_no_units_file_written_or_asked_for_is_no_units(tmp_path: Path) -> None:
    script = ['sh', '-c', 'echo METRIC a=1; echo "${PILOOP_UNITS_FILE:-unset}" > seen']
    r = run_evaluate(script, cwd=tmp_path, log_dir=tmp_path, timeout=5, required={'a'}, units_file=tmp_path / 'u')
    assert (r.error, r.units) == (None, {})
    assert (tmp_path / 'seen').read_text().strip() == str(tmp_path / 'u')
    r = run_evaluate(script, cwd=tmp_path, log_dir=tmp_path, timeout=5, required={'a'})
    assert (r.error, r.units) == (None, {})
    assert (tmp_path / 'seen').read_text().strip() == 'unset', 'a worker running evaluate itself gets no path'


@pytest.mark.parametrize(
    ('units', 'error'),
    [
        ('not json', 'expected {"<metric>": {"<unit>": <number>}}'),
        ('{"a": {"x": "3"}}', 'expected {"<metric>": {"<unit>": <number>}}'),
        ('{"a": {"x": true}}', 'expected {"<metric>": {"<unit>": <number>}}'),
        ('{"a": {"x": NaN}}', 'expected {"<metric>": {"<unit>": <number>}}'),
        ('{"c": {"x": 1}}', "units for 'c', which printed no METRIC line"),
        ('{"a": {}}', "no units for 'a'"),
        ('{"a": {"x": 1, "y": 1}}', "units of 'a' sum to 2, its METRIC line says 3"),
    ],
)
def test_a_bad_units_file_is_an_evaluation_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, units: str, error: str
) -> None:
    r, path = evaluate_with_units(tmp_path, monkeypatch, units)
    assert r.error is not None and r.error.startswith(f'evaluate wrote a bad PILOOP_UNITS_FILE: {error}')
    assert (r.metrics, r.units) == ({'a': 3.0, 'b': 1.0}, {})
    assert not path.exists()


def test_a_rounded_metric_line_still_matches_its_units(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    thirds = ', '.join(f'"r{i}": {1 / 3}' for i in range(9))  # 3.0000000000000004 in floats; the line says 3
    r, _ = evaluate_with_units(tmp_path, monkeypatch, f'{{"a": {{{thirds}}}}}')
    assert r.error is None and len(r.units['a']) == 9
