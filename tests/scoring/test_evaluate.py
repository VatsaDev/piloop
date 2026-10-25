import os
import signal
import subprocess
import time
from pathlib import Path

import pytest
from inline_snapshot import snapshot

from piloop.scoring.evaluate import (
    BadUnits,
    EvalResult,
    Exited,
    Failed,
    Killed,
    Measured,
    MissingMetrics,
    TimedOut,
    parse_metrics,
    run_evaluate,
)


def test_parse_metrics() -> None:
    out = 'noise\nMETRIC acc=0.83\nMETRIC loss = -1.5e-3\nMETRIC big=1E+20\nMETRICS bad=1\nMETRIC nan=x\nMETRIC=3\n'
    assert parse_metrics(out) == snapshot({'acc': 0.83, 'loss': -0.0015, 'big': 1e20})


def test_an_evaluate_that_its_tracker_refuses_is_killed_and_reaped(tmp_path: Path) -> None:
    """A tracker that raises ends the evaluate command at once, and it is reaped: no zombie, no Popen left running."""
    procs: list[subprocess.Popen[bytes]] = []

    def refuse(proc: subprocess.Popen[bytes] | None) -> None:
        if proc is not None:
            procs.append(proc)
            raise RuntimeError('refused')

    with pytest.raises(RuntimeError, match='refused'):
        run_evaluate(['sleep', '30'], cwd=tmp_path, log_dir=tmp_path, timeout=30, required=set(), track=refuse)
    assert [proc.returncode for proc in procs] == [-signal.SIGKILL]


def test_evaluate_ok(tmp_path: Path) -> None:
    r = run_evaluate(
        ['sh', '-c', 'echo METRIC a=1; echo METRIC b=2'], cwd=tmp_path, log_dir=tmp_path, timeout=5, required={'a'}
    )
    assert isinstance(r, Measured) and (r.metrics, r.units) == ({'a': 1.0, 'b': 2.0}, {})
    assert (tmp_path / 'eval.stdout').read_text() == 'METRIC a=1\nMETRIC b=2\n'


def test_evaluate_nonzero_exit_keeps_what_it_printed(tmp_path: Path) -> None:
    """A failed evaluation costs money too: its METRIC lines, the cost among them, go into the measurement."""
    r = run_evaluate(
        ['sh', '-c', 'echo METRIC a=1; echo boom >&2; exit 2'],
        cwd=tmp_path,
        log_dir=tmp_path,
        timeout=5,
        required={'a'},
    )
    assert isinstance(r, Failed) and r.why == Exited(2, 'boom')
    assert (r.error, r.printed) == (snapshot('evaluate exit 2: boom'), {'a': 1.0})


def test_evaluate_killed_by_a_signal_names_the_signal(tmp_path: Path) -> None:
    """A ``pkill`` from another process ends the evaluate with a signal: the error says so, not just ``exit -15``."""
    r = run_evaluate(
        ['sh', '-c', 'echo METRIC a=1; echo boom >&2; kill -TERM $$'],
        cwd=tmp_path,
        log_dir=tmp_path,
        timeout=5,
        required={'a'},
    )
    assert isinstance(r, Failed) and r.why == Killed(signal.SIGTERM, 'boom')
    assert isinstance(r.why, Killed) and r.why.sent, 'another process sent it: the evaluate measured nothing'
    assert r.error == snapshot('evaluate killed by SIGTERM (exit -15), not by its own exit: boom')
    assert r.printed == {'a': 1.0}


@pytest.mark.skipif(not hasattr(signal, 'SIGRTMIN'), reason='macOS has no real-time signals: each signal has a name')
def test_evaluate_killed_by_a_signal_with_no_name_gives_its_number(tmp_path: Path) -> None:
    unnamed = signal.SIGRTMIN + 1
    r = run_evaluate(['sh', '-c', f'kill -{unnamed} $$'], cwd=tmp_path, log_dir=tmp_path, timeout=5, required=set())
    assert isinstance(r, Failed) and isinstance(r.why, Killed) and not r.why.sent
    assert r.error == f'evaluate killed by signal {unnamed} (exit -{unnamed}), not by its own exit: '


def test_evaluate_missing_metric(tmp_path: Path) -> None:
    r = run_evaluate(['sh', '-c', 'echo METRIC a=1'], cwd=tmp_path, log_dir=tmp_path, timeout=5, required={'a', 'b'})
    assert isinstance(r, Failed) and r.why == MissingMetrics(('b',))
    assert (r.error, r.printed) == (snapshot('evaluate printed no METRIC line for: b'), {'a': 1.0})


def test_evaluate_timeout_kills_grandchildren_and_keeps_logs(tmp_path: Path) -> None:
    cmd = ['sh', '-c', 'echo METRIC a=1; sleep 30 & echo $! > child.pid; wait']
    r = run_evaluate(cmd, cwd=tmp_path, log_dir=tmp_path, timeout=0.3, required=set())
    assert isinstance(r, Failed) and r.why == TimedOut(0.3)
    assert (r.error, r.printed) == (snapshot('evaluate timeout after 0.3s'), {'a': 1.0})
    assert (tmp_path / 'eval.stdout').read_text() == 'METRIC a=1\n'
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
    assert isinstance(r, Measured) and (r.metrics, r.units) == ({'a': 3.0, 'b': 1.0}, {'a': {'x': 1.0, 'y': 2.0}})
    assert not path.exists(), 'the file never outlives the evaluation'


def test_no_units_file_written_or_asked_for_is_no_units(tmp_path: Path) -> None:
    script = ['sh', '-c', 'echo METRIC a=1; echo "${PILOOP_UNITS_FILE:-unset}" > seen']
    r = run_evaluate(script, cwd=tmp_path, log_dir=tmp_path, timeout=5, required={'a'}, units_file=tmp_path / 'u')
    assert isinstance(r, Measured) and (r.metrics, r.units) == ({'a': 1.0}, {})
    assert (tmp_path / 'seen').read_text().strip() == str(tmp_path / 'u')
    r = run_evaluate(script, cwd=tmp_path, log_dir=tmp_path, timeout=5, required={'a'})
    assert isinstance(r, Measured) and (r.metrics, r.units) == ({'a': 1.0}, {})
    assert (tmp_path / 'seen').read_text().strip() == 'unset', 'an implementer running evaluate itself gets no path'


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
    assert isinstance(r, Failed) and isinstance(r.why, BadUnits)
    assert r.error.startswith(f'evaluate wrote a bad PILOOP_UNITS_FILE: {error}')
    assert r.printed == {'a': 3.0, 'b': 1.0}, 'the measurement keeps what it printed, and no units'
    assert not path.exists()


def test_a_rounded_metric_line_still_matches_its_units(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    thirds = ', '.join(f'"r{i}": {1 / 3}' for i in range(9))  # 3.0000000000000004 in floats; the line says 3
    r, _ = evaluate_with_units(tmp_path, monkeypatch, f'{{"a": {{{thirds}}}}}')
    assert isinstance(r, Measured) and len(r.units['a']) == 9
