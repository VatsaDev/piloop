"""Run the evaluate command in a worktree, and read its result. A timeout kills the whole process group.

The command writes its result to ``RESULT_ENV`` as one JSON object (``Result``), which is checked against the
campaign's metrics. Until 0.4.0 it may print ``METRIC <name>=<value>`` lines instead (``Channel.METRIC_LINES``).
"""

import math
import shutil
import signal
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, assert_never

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, TypeAdapter, ValidationError

from piloop import deprecated
from piloop.proc import RESULT_ENV, STDERR_IN_ERROR, UNITS_ENV, Channel, ResultPipe, child_env, kill_group

SENT = frozenset({signal.SIGHUP, signal.SIGINT, signal.SIGTERM, signal.SIGKILL})
"""The signals that another process sends, a ``pkill`` say: an evaluate that one ends measured nothing of the candidate.

The others, SIGSEGV or SIGABRT, are the evaluate's own fault, and a measurement like a non-zero exit.
"""

CHANGED_INPUTS = 'evaluate changed its linked inputs'
"""How the error of an evaluation that changed a linked input that it read starts (``InputsChanged``)."""
UNITS_SUM_TOLERANCE = 1e-3
"""The allowed relative difference between a metric's per-unit sum and its value, which can be rounded."""
RESULT_PIPE = '.result'
"""The pipe in the log directory that ``RESULT_ENV`` names while the command runs."""
UNITS_FILE = '.units.json'
"""The file in the log directory that ``UNITS_ENV`` names while the command runs."""
EVIDENCE_DIR = 'evidence'
"""Where the evidence files that a result names are copied, in the log directory: ``evidence/<label><suffix>``."""

type Number = Annotated[float, Field(strict=True, allow_inf_nan=False)]
"""A finite number: an int or a float, never a bool, a string or null."""
type Units = dict[str, dict[str, float]]
"""Per-unit results: metric name → unit id → the unit's contribution. A metric's contributions sum to its value."""
UNITS = TypeAdapter(dict[str, dict[str, Number]])
"""Validates a units file, because the evaluate command is on the other side of a trust boundary."""
type Label = Annotated[str, StringConstraints(pattern=r'^[A-Za-z0-9][A-Za-z0-9_.-]*$', max_length=64)]
"""The name of an evidence file: letters, digits, ``_``, ``.`` and ``-``."""


class Result(BaseModel):
    """What one run of the evaluate command writes to ``RESULT_ENV``, as one line of JSON.

    Example: ``{"metrics": {"accuracy": 0.83}, "units": {"accuracy": {"q-1": 0.002}}, "evidence": {"errors": "out/errors.md"}}``.
    """

    model_config = ConfigDict(extra='forbid', frozen=True)

    metrics: dict[str, Number]
    """The value of each metric, by its name in ``[[metrics]]``."""
    units: dict[str, dict[str, Number]] = {}
    """Optional. Each metric's contribution per unit (one record, example or test case), by unit id. The contributions
    of a metric sum to its value. piloop then tests a gain with a paired bootstrap over the units."""
    evidence: dict[Label, str] = {}
    """Optional. Files of the worktree that explain the result, by a label: piloop keeps them with the evaluation's
    logs, as ``evidence/<label><suffix>``."""


RESULT = TypeAdapter(Result)

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
        rc: int | None = None
        try:
            track(proc)
            rc = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            pass
        finally:
            if proc.returncode is None:  # timed out, or ``track`` raised: kill it, and reap it on every path
                kill_group(proc)
                proc.wait()
            track(None)
    return Completed(rc, time.monotonic() - started)


@dataclass(frozen=True)
class Measured:
    """The evaluate command exited cleanly and reported every required metric."""

    metrics: dict[str, float]
    seconds: float
    units: Units = field(default_factory=dict)
    """The per-unit results that it reported. Empty when it reported none."""
    channel: Channel = Channel.RESULT
    """How it reported them."""


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
    """The evaluate command reported no value for some required metrics."""

    names: tuple[str, ...]


@dataclass(frozen=True)
class BadResult:
    """The evaluate command reported a result that piloop refuses (``read_result``)."""

    why: str


@dataclass(frozen=True)
class InputsChanged:
    """The evaluate command changed a linked input that it read, so what it measured does not count."""

    paths: tuple[str, ...]


@dataclass(frozen=True)
class NotRun:
    """The evaluate command could not run: its worktree or its inputs could not be made."""

    error: str


type Why = TimedOut | Exited | Killed | MissingMetrics | BadResult | InputsChanged | NotRun


@dataclass(frozen=True)
class Failed:
    """An evaluation that measured nothing that counts, and why."""

    why: Why
    seconds: float
    printed: dict[str, float] = field(default_factory=dict)
    """What the evaluate command reported, whatever its exit: a failed evaluation has a cost too. The measurement keeps
    it, next to the error, and a measurement with an error never scores. Empty when its result was refused."""

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
                return f'evaluate reported no value for: {", ".join(names)}'
            case BadResult(why=why):
                return f'evaluate wrote a bad result: {why}'
            case InputsChanged(paths=paths):
                return f'{CHANGED_INPUTS}: {", ".join(paths)}'
            case NotRun(error=error):
                return error
            case _:
                assert_never(self.why)


type EvalResult = Measured | Failed
"""The result of one run of the evaluate command."""


def printed(result: EvalResult) -> dict[str, float]:
    """The metrics that one run of the evaluate command reported, whatever its exit."""
    match result:
        case Measured(metrics=metrics) | Failed(printed=metrics):
            return metrics
        case _:
            assert_never(result)


@dataclass(frozen=True)
class Reported:
    """A result that piloop accepts, before it checks the required metrics and copies the evidence."""

    metrics: dict[str, float]
    units: Units
    evidence: dict[str, str]
    channel: Channel


def run_evaluate(
    cmd: list[str],
    *,
    cwd: Path,
    log_dir: Path,
    timeout: float,
    required: set[str],
    known: set[str],
    max_evidence_bytes: int,
    track: Track = no_track,
) -> EvalResult:
    """Run the evaluate command, and read the result that it wrote to ``RESULT_ENV``, whatever its exit.

    ``required`` are the metrics that it must report, and ``known`` every metric that it may report. Its evidence goes
    into ``log_dir/evidence``, at most ``max_evidence_bytes`` of it. The pipe and the units file are in ``log_dir``
    while it runs, and removed after, so neither outlives the call.
    """
    units_file = log_dir / UNITS_FILE
    try:
        with ResultPipe(log_dir / RESULT_PIPE) as results:
            env = child_env() | {RESULT_ENV: str(results.path), UNITS_ENV: str(units_file)}
            done = _run(cmd, cwd=cwd, log_dir=log_dir, prefix='eval', timeout=timeout, track=track, env=env)
        stdout = (log_dir / 'eval.stdout').read_text(errors='replace')
        reported = read_result(results.close(), stdout, units_file, known)
        kept = reported.metrics if isinstance(reported, Reported) else {}
        match done.returncode:
            case None:
                return Failed(TimedOut(timeout), done.seconds, kept)
            case 0:
                pass
            case code:
                stderr = (log_dir / 'eval.stderr').read_text(errors='replace').strip()[-STDERR_IN_ERROR:]
                return Failed(Exited(code, stderr) if code > 0 else Killed(-code, stderr), done.seconds, kept)
        match reported:
            case BadResult():
                return Failed(reported, done.seconds)
            case Reported(metrics=metrics, units=units, evidence=evidence, channel=channel):
                if missing := required - metrics.keys():
                    return Failed(MissingMetrics(tuple(sorted(missing))), done.seconds, metrics)
                if (bad := keep_evidence(evidence, cwd, log_dir / EVIDENCE_DIR, max_evidence_bytes)) is not None:
                    return Failed(bad, done.seconds, metrics)
                return Measured(metrics, done.seconds, units, channel)
            case _:
                assert_never(reported)
    finally:
        units_file.unlink(missing_ok=True)


def read_result(lines: list[str], stdout: str, units_file: Path, known: set[str]) -> Reported | BadResult:
    """Read the one result that the command wrote to ``RESULT_ENV``, or, until 0.4.0, its ``METRIC`` lines.

    The result must be a ``Result``. Each metric that it names, in ``metrics`` or ``units``, must be in ``known``, and
    each metric's units must sum to its value. A command that wrote both a result and ``METRIC`` lines is refused: it
    reports one way.
    """
    lines_printed = parse_metrics(stdout)
    match lines:
        case []:
            if not lines_printed:
                return BadResult(f'it wrote no result to {RESULT_ENV}')
            deprecated.until('0.4.0', 'METRIC lines on stdout: delete this branch, parse_metrics and UNITS_ENV')
            try:
                units = read_units(units_file, lines_printed) if units_file.exists() else {}
            except ValueError as e:
                return BadResult(f'{UNITS_ENV}: {e}')
            return Reported(lines_printed, units, {}, Channel.METRIC_LINES)
        case [line]:
            if lines_printed:
                return BadResult(f'it wrote a result to {RESULT_ENV} and printed METRIC lines: drop the METRIC lines')
            try:
                result = RESULT.validate_json(line)
            except ValidationError as e:
                return BadResult(f'expected one JSON object of the result schema: {_errors(e)}')
            if unknown := sorted((result.metrics.keys() | result.units.keys()) - known):
                return BadResult(f'unknown metrics {", ".join(unknown)}; [[metrics]] has {", ".join(sorted(known))}')
            try:
                _check_units(result.units, result.metrics)
            except ValueError as e:
                return BadResult(str(e))
            return Reported(
                dict(result.metrics),
                {m: dict(u) for m, u in result.units.items()},
                dict(result.evidence),
                Channel.RESULT,
            )
        case _:
            return BadResult(f'it wrote {len(lines)} results to {RESULT_ENV}; one run writes one line')


def keep_evidence(evidence: dict[str, str], cwd: Path, into: Path, max_bytes: int) -> BadResult | None:
    """Copy each evidence file of the worktree ``cwd`` into ``into``, as ``<label><suffix>``, or say why not.

    A path must be relative, stay inside the worktree, and name a file. All of them together must fit ``max_bytes``.
    """
    root = cwd.resolve()
    found: dict[str, Path] = {}
    for label, rel in evidence.items():
        path = (root / rel).resolve()
        if not path.is_relative_to(root) or not path.is_file():
            return BadResult(f'evidence {label!r}: {rel!r} is not a file in the worktree')
        found[label] = path
    if (total := sum(path.stat().st_size for path in found.values())) > max_bytes:
        return BadResult(f'its evidence is {total} bytes, over the {max_bytes} of [evaluation] max_evidence_mb')
    for label, path in found.items():
        into.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, into / f'{label}{path.suffix}')
    return None


def _errors(error: ValidationError) -> str:
    """Each error of a validation as ``where: what``, joined, cut to ``STDERR_IN_ERROR`` characters."""
    found = (
        '.'.join(str(part) for part in e['loc']) + ': ' + e['msg'] if e['loc'] else e['msg'] for e in error.errors()
    )
    return '; '.join(found)[:STDERR_IN_ERROR]


def _name(signum: int) -> str:
    """``SIGTERM`` for 15. A number with no name, a real-time signal say, stays a number."""
    try:
        return signal.Signals(signum).name
    except ValueError:
        return f'signal {signum}'


@deprecated.until('0.4.0', 'the units file of METRIC lines: delete read_units, UNITS and UNITS_ENV')
def read_units(path: Path, metrics: dict[str, float]) -> Units:
    """Validate a units file against the METRIC lines of the same evaluation.

    Raises:
        ValueError: the file is not ``{metric: {unit: number}}``, or does not fit the metrics (``_check_units``).
    """
    try:
        units = UNITS.validate_json(path.read_bytes())
    except ValidationError as e:
        raise ValueError(f'expected {{"<metric>": {{"<unit>": <number>}}}}: {e}') from e
    _check_units(units, metrics)
    return units


def _check_units(units: Units, metrics: dict[str, float]) -> None:
    """Check per-unit results against the values of the same result.

    Raises:
        ValueError: a metric with units has no value or no units, or its contributions do not sum to its value.
    """
    for name, values in units.items():
        if name not in metrics:
            raise ValueError(f'units for {name!r}, which has no value')
        if not values:
            raise ValueError(f'no units for {name!r}')
        total = math.fsum(values.values())
        if not math.isclose(total, metrics[name], rel_tol=UNITS_SUM_TOLERANCE, abs_tol=1e-9):
            raise ValueError(f'units of {name!r} sum to {total:g}, its value is {metrics[name]:g}')


@deprecated.until('0.4.0', 'METRIC lines on stdout: delete parse_metrics; a result goes to RESULT_ENV')
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
