"""Run the evaluate command in a worktree. A timeout kills the whole process group."""

import math
import signal
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, assert_never

from pydantic import Field, TypeAdapter, ValidationError

from piloop.proc import STDERR_IN_ERROR, UNITS_ENV, child_env, kill_group

SENT = frozenset({signal.SIGHUP, signal.SIGINT, signal.SIGTERM, signal.SIGKILL})
"""The signals that another process sends, a ``pkill`` say: an evaluate that one ends measured nothing of the candidate.

The others, SIGSEGV or SIGABRT, are the evaluate's own fault, and a measurement like a non-zero exit.
"""

CHANGED_INPUTS = 'evaluate changed its linked inputs'
"""How the error of an evaluation that changed a linked input that it read starts (``InputsChanged``)."""
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
class Measured:
    """The evaluate command exited cleanly and printed every metric."""

    metrics: dict[str, float]
    seconds: float
    units: Units = field(default_factory=dict)
    """The per-unit results the evaluate wrote to ``UNITS_ENV``. Empty when it wrote none or was not asked to."""


@dataclass(frozen=True)
class TimedOut:
    """The evaluate command ran past its timeout, and was killed."""

    timeout: float


@dataclass(frozen=True)
class Exited:
    """The evaluate command exited with a non-zero code."""

    code: int
    stderr: str


@dataclass(frozen=True)
class Killed:
    """A signal ended the evaluate command."""

    signal: int
    stderr: str

    @property
    def sent(self) -> bool:
        """Whether another process sent it (``SENT``): then the evaluate measured nothing of the candidate."""
        return self.signal in SENT


@dataclass(frozen=True)
class MissingMetrics:
    """The evaluate command printed no ``METRIC`` line for some metrics."""

    names: tuple[str, ...]
    printed: dict[str, float]
    """The metrics that it printed."""


@dataclass(frozen=True)
class BadUnits:
    """The evaluate command wrote per-unit results that do not fit its ``METRIC`` lines (``read_units``)."""

    why: str
    printed: dict[str, float]


@dataclass(frozen=True)
class InputsChanged:
    """The evaluate command changed a linked input that it read, so what it measured does not count."""

    paths: tuple[str, ...]


@dataclass(frozen=True)
class NotRun:
    """The evaluate command could not run: its worktree or its inputs could not be made."""

    error: str


type Why = TimedOut | Exited | Killed | MissingMetrics | BadUnits | InputsChanged | NotRun


@dataclass(frozen=True)
class Failed:
    """An evaluation that measured nothing that counts, and why."""

    why: Why
    seconds: float

    @property
    def error(self) -> str:
        """The error that the measurement stores."""
        match self.why:
            case TimedOut(timeout=timeout):
                return f'evaluate timeout after {timeout:g}s'
            case Exited(code=code, stderr=stderr):
                return f'evaluate exit {code}: {stderr}'
            case Killed(signal=signum, stderr=stderr):
                return f'evaluate killed by {_name(signum)} (exit {-signum}), not by its own exit: {stderr}'
            case MissingMetrics(names=names):
                return f'evaluate printed no METRIC line for: {", ".join(names)}'
            case BadUnits(why=why):
                return f'evaluate wrote a bad {UNITS_ENV}: {why}'
            case InputsChanged(paths=paths):
                return f'{CHANGED_INPUTS}: {", ".join(paths)}'
            case NotRun(error=error):
                return error
            case _:
                assert_never(self.why)

    @property
    def printed(self) -> dict[str, float]:
        """What the evaluate command printed before it failed. The measurement keeps it, next to the error."""
        match self.why:
            case MissingMetrics(printed=printed) | BadUnits(printed=printed):
                return printed
            case _:
                return {}


type EvalResult = Measured | Failed
"""The result of one run of the evaluate command."""


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
    there is validated into ``Measured.units``. The file is then removed, so it never outlives the call.
    """
    env = child_env() | ({UNITS_ENV: str(units_file)} if units_file is not None else {})
    try:
        done = _run(cmd, cwd=cwd, log_dir=log_dir, prefix='eval', timeout=timeout, track=track, env=env)
        match done.returncode:
            case None:
                return Failed(TimedOut(timeout), done.seconds)
            case 0:
                pass
            case code:
                stderr = (log_dir / 'eval.stderr').read_text(errors='replace').strip()[-STDERR_IN_ERROR:]
                return Failed(Exited(code, stderr) if code > 0 else Killed(-code, stderr), done.seconds)
        metrics = parse_metrics((log_dir / 'eval.stdout').read_text(errors='replace'))
        if missing := required - metrics.keys():
            return Failed(MissingMetrics(tuple(sorted(missing)), metrics), done.seconds)
        if units_file is None or not units_file.exists():
            return Measured(metrics, done.seconds)
        try:
            return Measured(metrics, done.seconds, read_units(units_file, metrics))
        except ValueError as e:
            return Failed(BadUnits(str(e), metrics), done.seconds)
    finally:
        if units_file is not None:
            units_file.unlink(missing_ok=True)


def _name(signum: int) -> str:
    """``SIGTERM`` for 15. A number with no name, a real-time signal say, stays a number."""
    try:
        return signal.Signals(signum).name
    except ValueError:
        return f'signal {signum}'


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
    """Parse ``METRIC name=value`` lines into floats. The caller checks that none is missing.

    Skip a malformed line: one with no name, a value that is not a number, or a value that is NaN or infinite.
    """
    metrics: dict[str, float] = {}
    for line in stdout.splitlines():
        head, sep, value = line.strip().partition('=')
        name = head.removeprefix('METRIC ').strip()
        if not sep or not head.startswith('METRIC ') or not name:
            continue
        try:
            number = float(value.strip())
        except ValueError:
            continue
        if math.isfinite(number):
            metrics[name] = number
    return metrics
