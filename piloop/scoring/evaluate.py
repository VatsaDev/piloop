"""Run the evaluate command in a worktree. A timeout kills the whole process group."""

import math
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated

from pydantic import Field, TypeAdapter, ValidationError

from piloop.proc import STDERR_IN_ERROR, UNITS_ENV, child_env, kill_group

UNITS_SUM_TOLERANCE = 1e-3
"""The allowed relative difference between a metric's per-unit sum and its METRIC line, which can be rounded."""

type Units = dict[str, dict[str, float]]
"""Per-unit results: metric name → unit id → the unit's contribution. A metric's contributions sum to its value."""
UNITS = TypeAdapter(dict[str, dict[str, Annotated[float, Field(strict=True, allow_inf_nan=False)]]])
"""Validates a units file, because the evaluate command is on the other side of a trust boundary."""

type Track = Callable[[subprocess.Popen[bytes] | None], None]
"""Called with the child process when it starts and with None after it is reaped, so another thread can kill it."""


def no_track(_proc: subprocess.Popen[bytes] | None) -> None:
    """The default ``Track``, for when no other thread needs to kill the child."""


@dataclass(frozen=True)
class Completed:
    """A command that finished or was killed."""

    returncode: int | None
    """None when the command was killed at its timeout."""
    seconds: float


def _run(
    cmd: list[str], *, cwd: Path, log_dir: Path, prefix: str, timeout: float, track: Track, env: dict[str, str]
) -> Completed:
    """Run ``cmd`` in its own session, with stdout and stderr in ``log_dir/<prefix>.stdout`` and ``.stderr``."""
    started = time.monotonic()
    with (log_dir / f'{prefix}.stdout').open('w') as out, (log_dir / f'{prefix}.stderr').open('w') as err:
        proc = subprocess.Popen(
            cmd, cwd=cwd, stdout=out, stderr=err, stdin=subprocess.DEVNULL, env=env, start_new_session=True
        )
        track(proc)
        try:
            rc: int | None = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            kill_group(proc)
            proc.wait()
            rc = None
        finally:
            track(None)
    return Completed(rc, time.monotonic() - started)


@dataclass(frozen=True)
class EvalResult:
    """The result of one run of the evaluate command."""

    metrics: dict[str, float]
    seconds: float
    error: str | None
    units: Units = field(default_factory=dict)
    """The per-unit results the evaluate wrote to ``UNITS_ENV``. Empty when it wrote none or was not asked to."""


def run_evaluate(
    cmd: list[str],
    *,
    cwd: Path,
    log_dir: Path,
    timeout: float,
    required: set[str],
    track: Track = no_track,
    units_file: Path | None = None,
) -> EvalResult:
    """Run the evaluate command and parse its ``METRIC name=value`` lines. A non-zero exit gives no metrics.

    With ``units_file``, which must not exist yet, the command also gets ``UNITS_ENV`` set to that path. What it writes
    there is validated into ``EvalResult.units``. The file is then removed, so it never outlives the call.
    """
    env = child_env() | ({UNITS_ENV: str(units_file)} if units_file is not None else {})
    try:
        done = _run(cmd, cwd=cwd, log_dir=log_dir, prefix='eval', timeout=timeout, track=track, env=env)
        if done.returncode is None:
            return EvalResult({}, done.seconds, f'evaluate timeout after {timeout:g}s')
        if done.returncode != 0:
            stderr = (log_dir / 'eval.stderr').read_text(errors='replace').strip()[-STDERR_IN_ERROR:]
            return EvalResult({}, done.seconds, f'evaluate exit {done.returncode}: {stderr}')
        metrics = parse_metrics((log_dir / 'eval.stdout').read_text(errors='replace'))
        missing = required - metrics.keys()
        if missing:
            error = f'evaluate printed no METRIC line for: {", ".join(sorted(missing))}'
            return EvalResult(metrics, done.seconds, error)
        if units_file is None or not units_file.exists():
            return EvalResult(metrics, done.seconds, None)
        try:
            units = read_units(units_file, metrics)
        except ValueError as e:
            return EvalResult(metrics, done.seconds, f'evaluate wrote a bad {UNITS_ENV}: {e}')
        return EvalResult(metrics, done.seconds, None, units)
    finally:
        if units_file is not None:
            units_file.unlink(missing_ok=True)


def read_units(path: Path, metrics: dict[str, float]) -> Units:
    """Validate a units file against the METRIC lines of the same evaluation.

    Raises:
        ValueError: the file is not ``{metric: {unit: number}}``, names a metric with no METRIC line, lists a metric
            with no units, or has contributions that do not sum to the metric's value.
    """
    try:
        units = UNITS.validate_json(path.read_bytes())
    except ValidationError as e:
        raise ValueError(f'expected {{"<metric>": {{"<unit>": <number>}}}}: {e}') from e
    for name, values in units.items():
        if name not in metrics:
            raise ValueError(f'units for {name!r}, which printed no METRIC line')
        if not values:
            raise ValueError(f'no units for {name!r}')
        total = math.fsum(values.values())
        if not math.isclose(total, metrics[name], rel_tol=UNITS_SUM_TOLERANCE, abs_tol=1e-9):
            raise ValueError(f'units of {name!r} sum to {total:g}, its METRIC line says {metrics[name]:g}')
    return units


def parse_metrics(stdout: str) -> dict[str, float]:
    """Parse ``METRIC name=value`` lines into floats. Skip malformed lines. The caller checks that none is missing."""
    metrics: dict[str, float] = {}
    for line in stdout.splitlines():
        head, sep, value = line.strip().partition('=')
        if not sep or not head.startswith('METRIC '):
            continue
        name = head.removeprefix('METRIC ').strip()
        try:
            metrics[name] = float(value.strip())
        except ValueError:
            continue
    return metrics
