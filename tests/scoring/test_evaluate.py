import json
import os
import signal
import subprocess
import time
from pathlib import Path

import pytest
from inline_snapshot import snapshot

from piloop.proc import Channel
from piloop.scoring.evaluate import (
    BadResult,
    EvalResult,
    Exited,
    Failed,
    Killed,
    Measured,
    MissingMetrics,
    TimedOut,
    Track,
    no_track,
    parse_metrics,
    run_evaluate,
)

KNOWN = frozenset({'a', 'b', 'cost'})


def evaluate(
    tmp_path: Path,
    script: str,
    *,
    required: frozenset[str] = frozenset({'a'}),
    timeout: float = 5,
    cap: int = 1000,
    track: Track = no_track,
) -> EvalResult:
    return run_evaluate(
        ['sh', '-c', script],
        cwd=tmp_path,
        log_dir=tmp_path,
        timeout=timeout,
        required=set(required),
        known=set(KNOWN),
        max_evidence_bytes=cap,
        track=track,
    )


def write(result: object) -> str:
    """A shell line that appends ``result``, as JSON, to the result pipe."""
    return f"printf '%s\\n' '{json.dumps(result)}' >> \"$PILOOP_RESULT_FILE\""


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
        evaluate(tmp_path, 'sleep 30', timeout=30, track=refuse)
    assert [proc.returncode for proc in procs] == [-signal.SIGKILL]
    assert not (tmp_path / '.result').exists(), 'the pipe never outlives the evaluation'


def test_a_result_is_measured(tmp_path: Path) -> None:
    r = evaluate(tmp_path, f'echo working; {write({"metrics": {"a": 1, "b": 2.5}, "units": {"a": {"x": 1}}})}')
    assert r == Measured({'a': 1.0, 'b': 2.5}, r.seconds, {'a': {'x': 1.0}}, Channel.RESULT)
    assert (tmp_path / 'eval.stdout').read_text() == 'working\n', 'stdout is a log'
    assert not (tmp_path / '.result').exists()


def test_a_watch_metric_may_be_left_out(tmp_path: Path) -> None:
    r = evaluate(tmp_path, write({'metrics': {'a': 1}}))
    assert isinstance(r, Measured) and r.metrics == {'a': 1.0}


def test_a_result_written_in_parts_and_without_a_final_newline_is_read(tmp_path: Path) -> None:
    script = 'f="$PILOOP_RESULT_FILE"; printf \'{"metrics": \' >> "$f"; sleep 0.1; printf \'{"a": 3}}\' >> "$f"'
    r = evaluate(tmp_path, script)
    assert isinstance(r, Measured) and r.metrics == {'a': 3.0}


def test_a_large_result_does_not_block_the_command(tmp_path: Path) -> None:
    """The pipe holds about 64 KB: the reader drains it while the command writes."""
    units = {f'u{i}': 1 / 20000 for i in range(20000)}
    (tmp_path / 'result.json').write_text(json.dumps({'metrics': {'a': 1}, 'units': {'a': units}}) + '\n')
    script = 'cat result.json >> "$PILOOP_RESULT_FILE"'
    r = evaluate(tmp_path, script)
    assert isinstance(r, Measured) and len(r.units['a']) == 20000


def test_a_nonzero_exit_keeps_what_it_reported(tmp_path: Path) -> None:
    """A failed evaluation costs money too: what it reported, the cost among it, goes into the measurement."""
    r = evaluate(tmp_path, f'{write({"metrics": {"a": 1, "cost": 0.4}})}; echo boom >&2; exit 2')
    assert isinstance(r, Failed) and r.why == Exited(2, 'boom')
    assert (r.error, r.printed) == (snapshot('evaluate exit 2: boom'), {'a': 1.0, 'cost': 0.4})


def test_evaluate_killed_by_a_signal_names_the_signal(tmp_path: Path) -> None:
    """A ``pkill`` from another process ends the evaluate with a signal: the error says so, not just ``exit -15``."""
    r = evaluate(tmp_path, f'{write({"metrics": {"a": 1}})}; echo boom >&2; kill -TERM $$')
    assert isinstance(r, Failed) and r.why == Killed(signal.SIGTERM, 'boom')
    assert isinstance(r.why, Killed) and r.why.sent, 'another process sent it: the evaluate measured nothing'
    assert r.error == snapshot('evaluate killed by SIGTERM (exit -15), not by its own exit: boom')
    assert r.printed == {'a': 1.0}


@pytest.mark.skipif(not hasattr(signal, 'SIGRTMIN'), reason='macOS has no real-time signals: each signal has a name')
def test_evaluate_killed_by_a_signal_with_no_name_gives_its_number(tmp_path: Path) -> None:
    unnamed = signal.SIGRTMIN + 1
    r = evaluate(tmp_path, f'kill -{unnamed} $$')
    assert isinstance(r, Failed) and isinstance(r.why, Killed) and not r.why.sent
    assert r.error == f'evaluate killed by signal {unnamed} (exit -{unnamed}), not by its own exit: '


def test_a_missing_required_metric_fails(tmp_path: Path) -> None:
    r = evaluate(tmp_path, write({'metrics': {'a': 1}}), required=frozenset({'a', 'b'}))
    assert isinstance(r, Failed) and r.why == MissingMetrics(('b',))
    assert (r.error, r.printed) == (snapshot('evaluate reported no value for: b'), {'a': 1.0})


def test_evaluate_timeout_kills_grandchildren_and_keeps_logs(tmp_path: Path) -> None:
    script = f'echo started; {write({"metrics": {"a": 1}})}; sleep 30 & echo $! > child.pid; wait'
    r = evaluate(tmp_path, script, timeout=0.3)
    assert isinstance(r, Failed) and r.why == TimedOut(0.3)
    assert (r.error, r.printed) == (snapshot('evaluate timeout after 0.3s'), {'a': 1.0})
    assert (tmp_path / 'eval.stdout').read_text() == 'started\n'
    assert_dead(int((tmp_path / 'child.pid').read_text()))


def test_a_child_left_holding_the_pipe_does_not_hold_the_evaluation(tmp_path: Path) -> None:
    script = f'{write({"metrics": {"a": 1}})}; (exec 3>>"$PILOOP_RESULT_FILE"; sleep 30) & echo $! > child.pid'
    started = time.monotonic()
    r = evaluate(tmp_path, script)
    assert isinstance(r, Measured) and time.monotonic() - started < 5
    os.kill(int((tmp_path / 'child.pid').read_text()), signal.SIGKILL)


def assert_dead(pid: int) -> None:
    time.sleep(0.2)  # let init reap the orphan
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


@pytest.mark.parametrize(
    ('script', 'error'),
    [
        ('true', 'it wrote no result to PILOOP_RESULT_FILE'),
        (
            f'{write({"metrics": {"a": 1}})}; {write({"metrics": {"a": 2}})}',
            'it wrote 2 results to PILOOP_RESULT_FILE; one run writes one line',
        ),
        (
            f'echo METRIC a=1; {write({"metrics": {"a": 1}})}',
            'it wrote a result to PILOOP_RESULT_FILE and printed METRIC lines: drop the METRIC lines',
        ),
        (
            'printf \'{\\n  "metrics": {"a": 1}\\n}\\n\' >> "$PILOOP_RESULT_FILE"',
            'it wrote 3 results to PILOOP_RESULT_FILE; one run writes one line',
        ),
        (
            'printf \'not json\\n\' >> "$PILOOP_RESULT_FILE"',
            'expected one JSON object of the result schema: Invalid JSON: expected ident at line 1 column 2',
        ),
        (
            write({'metrics': {'a': '1'}}),
            'expected one JSON object of the result schema: metrics.a: Input should be a valid number',
        ),
        (
            write({'metrics': {'a': True}}),
            'expected one JSON object of the result schema: metrics.a: Input should be a valid number',
        ),
        (
            write({'metrics': {'a': None}}),
            'expected one JSON object of the result schema: metrics.a: Input should be a valid number',
        ),
        (
            'printf \'{"metrics": {"a": NaN}}\\n\' >> "$PILOOP_RESULT_FILE"',
            'expected one JSON object of the result schema: metrics.a: Input should be a finite number',
        ),
        (
            write({'metric': {'a': 1}}),
            'expected one JSON object of the result schema: metric: Extra inputs are not permitted; metrics: Field required',
        ),
        (
            write({'metrics': {'a': 1}, 'notes': 'x'}),
            'expected one JSON object of the result schema: notes: Extra inputs are not permitted',
        ),
        (write({'metrics': {'a': 1, 'acc': 2}}), 'unknown metrics acc; [[metrics]] has a, b, cost'),
        (write({'metrics': {'a': 1}, 'units': {'z': {'x': 1}}}), 'unknown metrics z; [[metrics]] has a, b, cost'),
        (write({'metrics': {'a': 1}, 'units': {'b': {'x': 1}}}), "units for 'b', which has no value"),
        (write({'metrics': {'a': 1}, 'units': {'a': {}}}), "no units for 'a'"),
        (write({'metrics': {'a': 3}, 'units': {'a': {'x': 1, 'y': 1}}}), "units of 'a' sum to 2, its value is 3"),
        (
            write({'metrics': {'a': 1}, 'evidence': {'../up': 'x'}}),
            "expected one JSON object of the result schema: evidence.../up.[key]: String should match pattern '^[A-Za-z0-9][A-Za-z0-9_.-]*$'",
        ),
    ],
)
def test_a_bad_result_is_a_crash_that_says_why(tmp_path: Path, script: str, error: str) -> None:
    r = evaluate(tmp_path, script)
    assert isinstance(r, Failed) and r.why == BadResult(error), r
    assert r.error == f'evaluate wrote a bad result: {error}'
    assert r.printed == {}, 'a refused result counts nothing'


def test_a_rounded_value_still_matches_its_units(tmp_path: Path) -> None:
    thirds = {f'r{i}': 1 / 3 for i in range(9)}  # 3.0000000000000004 in floats; the value says 3
    r = evaluate(tmp_path, write({'metrics': {'a': 3}, 'units': {'a': thirds}}))
    assert isinstance(r, Measured) and len(r.units['a']) == 9


def test_evidence_is_kept_with_the_logs(tmp_path: Path) -> None:
    script = f'mkdir -p out; echo "3 errors" > out/errors.md; {write({"metrics": {"a": 1}, "evidence": {"errors": "out/errors.md"}})}'
    r = evaluate(tmp_path, script)
    assert isinstance(r, Measured)
    assert (tmp_path / 'evidence' / 'errors.md').read_text() == '3 errors\n'


@pytest.mark.parametrize(
    ('script', 'error'),
    [
        (
            write({'metrics': {'a': 1}, 'evidence': {'e': 'missing.md'}}),
            "evidence 'e': 'missing.md' is not a file in the worktree",
        ),
        (
            write({'metrics': {'a': 1}, 'evidence': {'e': '/etc/hostname'}}),
            "evidence 'e': '/etc/hostname' is not a file in the worktree",
        ),
        (
            write({'metrics': {'a': 1}, 'evidence': {'e': '../outside'}}),
            "evidence 'e': '../outside' is not a file in the worktree",
        ),
        (
            f'head -c 1001 /dev/zero > big; {write({"metrics": {"a": 1}, "evidence": {"e": "big"}})}',
            'its evidence is 1001 bytes, over the 1000 of [evaluation] max_evidence_mb',
        ),
    ],
)
def test_bad_evidence_is_a_crash(tmp_path: Path, script: str, error: str) -> None:
    worktree = tmp_path / 'worktree'
    worktree.mkdir()
    (tmp_path / 'outside').write_text('x')
    r = run_evaluate(
        ['sh', '-c', script],
        cwd=worktree,
        log_dir=tmp_path,
        timeout=5,
        required={'a'},
        known=set(KNOWN),
        max_evidence_bytes=1000,
    )
    assert isinstance(r, Failed) and r.why == BadResult(error)
    assert r.printed == {'a': 1.0}
    assert not (tmp_path / 'evidence').exists()


def test_metric_lines_are_read_until_0_4_0(tmp_path: Path) -> None:
    """The protocol before the result pipe: METRIC lines, and units in their own file. Any name is kept, as before."""
    script = 'echo METRIC a=3; echo METRIC other=1; printf \'{"a": {"x": 1, "y": 2}}\' > "$PILOOP_UNITS_FILE"'
    r = evaluate(tmp_path, script)
    assert r == Measured({'a': 3.0, 'other': 1.0}, r.seconds, {'a': {'x': 1.0, 'y': 2.0}}, Channel.METRIC_LINES)
    assert not (tmp_path / '.units.json').exists(), 'the units file never outlives the evaluation'


def test_a_bad_units_file_of_metric_lines_is_a_crash(tmp_path: Path) -> None:
    r = evaluate(tmp_path, 'echo METRIC a=3; printf \'{"a": {"x": 1}}\' > "$PILOOP_UNITS_FILE"')
    assert isinstance(r, Failed) and r.why == BadResult("PILOOP_UNITS_FILE: units of 'a' sum to 1, its value is 3")
    r = evaluate(tmp_path, 'echo METRIC a=3; echo nope > "$PILOOP_UNITS_FILE"')
    assert isinstance(r, Failed) and isinstance(r.why, BadResult)
    assert r.why.why.startswith('PILOOP_UNITS_FILE: expected {"<metric>": {"<unit>": <number>}}')


def test_metric_lines_of_a_failed_command_are_kept(tmp_path: Path) -> None:
    r = evaluate(tmp_path, 'echo METRIC a=1; exit 2')
    assert isinstance(r, Failed) and r.printed == {'a': 1.0}
