"""Run the evaluate command in a worktree, and read its result. A timeout kills the whole process group.

The command writes its result to ``RESULT_ENV`` as one JSON object (``Result``), which is checked against the
campaign's metrics. Until 0.4.0 it may print ``METRIC <name>=<value>`` lines instead (``Channel.METRIC_LINES``).
"""

import contextlib
import math
import os
import shutil
import signal
import subprocess
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, assert_never

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, TypeAdapter, ValidationError

from piloop import deprecated
from piloop.config import Rung
from piloop.ids import RungName
from piloop.proc import RESULT_ENV, STDERR_IN_ERROR, UNITS_ENV, Channel, ResultPipe, child_env, kill_group
from piloop.scoring.ladder import Gate

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
    """One line that the evaluate command writes to ``RESULT_ENV``: its full result, or a rung of the ladder.

    Example: ``{"metrics": {"accuracy": 0.83}, "units": {"accuracy": {"q-1": 0.002}}, "evidence": {"errors": "out/errors.md"}}``.
    """

    model_config = ConfigDict(extra='forbid', frozen=True)

    rung: RungName | None = None
    """The rung of ``[[ladder]]`` that the line reports, before the last line. None on the last line."""
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
    """A line of the evaluate command that piloop accepts: the full panel after a clean exit, or a rung of the ladder."""

    metrics: dict[str, float]
    seconds: float
    units: Units = field(default_factory=dict)
    """The per-unit results that it reported. Empty when it reported none."""
    channel: Channel = Channel.RESULT
    """How it reported them."""
    rung: RungName | None = None
    """The rung that it measured. None for the full panel."""


@dataclass(frozen=True)
class StoppedAt:
    """A rung where the climb stopped (``Rung.stop``): what the rung measured, and why it stopped there."""

    rung: RungName
    reason: str
    metrics: dict[str, float]
    seconds: float
    units: Units = field(default_factory=dict)


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
    """The evaluate command reported a result that piloop refuses (``_Lines``)."""

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
    """What the evaluate command reported as its last line, whatever its exit: a failed evaluation has a cost too. The
    measurement keeps it, next to the error, and a measurement with an error never scores. Empty when it was refused."""
    rung: RungName | None = None
    """The rung that failed. None for the full panel."""

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


type EvalResult = Measured | StoppedAt | Failed
"""What one line of the evaluate command came to, or how its run ended."""


def printed(result: EvalResult) -> dict[str, float]:
    """The metrics that one line of the evaluate command reported, whatever its exit."""
    match result:
        case Measured(metrics=metrics) | StoppedAt(metrics=metrics) | Failed(printed=metrics):
            return metrics
        case _:
            assert_never(result)


@dataclass(frozen=True)
class Step:
    """One process of a climb: the rungs that it reports first, each on a line that names it, then its last line."""

    rungs: tuple[Rung, ...] = ()
    """The rungs of the ladder without a command of their own, in order."""
    last: Rung | None = None
    """The rung that its last line reports, when it is that rung's own command. None: the full panel."""

    def label(self, done: int) -> RungName | None:
        """The rung of the line after ``done`` lines: a rung, then the last line's."""
        if done < len(self.rungs):
            return self.rungs[done].name
        return None if self.last is None else self.last.name


@dataclass(frozen=True)
class Reported:
    """The last line of a run, before its exit decides what it counts for."""

    metrics: dict[str, float]
    units: Units
    channel: Channel
    seconds: float


type Read = Measured | StoppedAt | Reported | BadResult | MissingMetrics


class _Lines:
    """The lines of one run of the evaluate command, checked as they come.

    Each line must be a ``Result`` that names only ``known`` metrics, with units that sum to their values. The lines
    of ``step.rungs`` come first, in order, each naming its rung and reporting what its ``stop`` reads; then the last
    line, which names none. With a ``gate``, a rung whose ``stop`` holds stops the climb, and a bad line ends it: piloop
    sends the run's group SIGTERM, then SIGKILL ``grace`` seconds later, and reads no more lines.
    """

    def __init__(self, step: Step, checks: Checks, gate: Gate | None, grace: float) -> None:
        self.step, self.checks, self.gate, self.grace = step, checks, gate, grace
        self.rungs: list[Measured | StoppedAt] = []
        self.last: Reported | None = None
        self.bad: BadResult | MissingMetrics | None = None
        self.count = 0
        self._at = time.monotonic()
        self._lock = threading.Lock()
        self._proc: subprocess.Popen[bytes] | None = None
        self._ended = False
        self._kill: threading.Timer | None = None

    @property
    def stopped(self) -> bool:
        """Whether a rung stopped the climb."""
        return bool(self.rungs) and isinstance(self.rungs[-1], StoppedAt)

    def take(self, line: str) -> None:
        """Check one line, keep what it measured, and end the run at a stop or a bad line."""
        with self._lock:
            if self._ended:
                return
        now = time.monotonic()
        read = self._read(line, now - self._at)
        self._at = now
        self.count += 1
        match read:
            case Measured() | StoppedAt():
                self.rungs.append(read)
            case Reported():
                self.last = read
            case BadResult() | MissingMetrics():
                self.bad = read
            case _:
                assert_never(read)
        if isinstance(read, StoppedAt | BadResult | MissingMetrics):
            self._end()

    def track(self, proc: subprocess.Popen[bytes] | None) -> None:
        """Learn the run's process when it starts, and forget it once it is reaped (``Track``)."""
        with self._lock:
            self._proc = proc
            if proc is None and self._kill is not None:
                self._kill.cancel()
            if proc is not None and self._ended:
                self._signal(proc)

    def _end(self) -> None:
        with self._lock:
            self._ended = True
            if self._proc is not None:
                self._signal(self._proc)

    def _signal(self, proc: subprocess.Popen[bytes]) -> None:
        """SIGTERM to the group, then SIGKILL after the grace. The caller holds the lock."""
        with contextlib.suppress(ProcessLookupError, PermissionError):  # gone, or exiting already
            os.killpg(proc.pid, signal.SIGTERM)
        self._kill = threading.Timer(self.grace, kill_group, (proc,))
        self._kill.daemon = True
        self._kill.start()

    def _read(self, line: str, seconds: float) -> Read:
        done, step, checks = self.count, self.step, self.checks
        if self.last is not None:
            return BadResult('it wrote a line after its last one')
        try:
            result = RESULT.validate_json(line)
        except ValidationError as e:
            return BadResult(f'expected one JSON object of the result schema: {_errors(e)}')
        if unknown := sorted((result.metrics.keys() | result.units.keys()) - checks.known):
            return BadResult(f'unknown metrics {", ".join(unknown)}; [[metrics]] has {", ".join(sorted(checks.known))}')
        try:
            _check_units(result.units, result.metrics)
        except ValueError as e:
            return BadResult(str(e))
        streamed = step.rungs[done] if done < len(step.rungs) else None
        if result.rung != (None if streamed is None else streamed.name):
            return BadResult(f'it reported rung {result.rung!r} where {step.label(done)!r} comes')
        if streamed is not None and (missing := set(streamed.stop) - result.metrics.keys()):
            return MissingMetrics(tuple(sorted(missing)))
        label = step.label(done)
        into = checks.evidence if label is None else checks.evidence / label
        if (bad := keep_evidence(result.evidence, checks.cwd, into, checks.max_evidence_bytes)) is not None:
            return bad
        metrics, units = dict(result.metrics), {m: dict(u) for m, u in result.units.items()}
        if streamed is None:
            return Reported(metrics, units, Channel.RESULT, seconds)
        return self._judged(streamed, metrics, units, seconds)

    def _judged(self, rung: Rung, metrics: dict[str, float], units: Units, seconds: float) -> Measured | StoppedAt:
        """``rung``'s line as measured, or as where the climb stops when the gate says so."""
        reason = None if self.gate is None else self.gate.stop_reason(rung, metrics)
        if reason is None:
            return Measured(metrics, seconds, units, Channel.RESULT, rung.name)
        return StoppedAt(rung.name, reason, metrics, seconds, units)


@dataclass(frozen=True)
class Checks:
    """What every line of a run is checked against, and where its evidence goes."""

    required: set[str]
    """The metrics that the full panel must report: the primaries and the guardrails."""
    known: set[str]
    """Every metric that a line may report: those of ``[[metrics]]``."""
    cwd: Path
    """The worktree that the evidence comes from."""
    evidence: Path
    """Where the evidence goes: ``<log dir>/evidence``, and ``<log dir>/evidence/<rung>`` for a rung's."""
    max_evidence_bytes: int
    """The evidence of one line, all together."""


FULL = Step()
"""A run that reports the full panel alone."""


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
    step: Step = FULL,
    gate: Gate | None = None,
    stop_grace: float = 0,
    prefix: str = 'eval',
) -> list[EvalResult]:
    """Run the evaluate command, and read the lines that it writes to ``RESULT_ENV`` while it runs (``_Lines``).

    Return a result for each rung that it reported, then how it ended: the full panel (or ``step.last``) measured, or
    failed. A climb that stopped ends at its ``StoppedAt`` rung. ``prefix`` names its logs in ``log_dir``. The pipe and
    the units file are in ``log_dir`` while it runs, and removed after, so neither outlives the call.
    """
    units_file = log_dir / UNITS_FILE
    checks = Checks(required, known, cwd, log_dir / EVIDENCE_DIR, max_evidence_bytes)
    lines = _Lines(step, checks, gate, stop_grace)

    def both(proc: subprocess.Popen[bytes] | None) -> None:
        lines.track(proc)
        track(proc)

    try:
        with ResultPipe(log_dir / RESULT_PIPE, lines.take) as results:
            env = child_env() | {RESULT_ENV: str(results.path), UNITS_ENV: str(units_file)}
            done = _run(cmd, cwd=cwd, log_dir=log_dir, prefix=prefix, timeout=timeout, track=both, env=env)
        stdout = (log_dir / f'{prefix}.stdout').read_text(errors='replace')
        rungs: list[EvalResult] = list(lines.rungs)
        if lines.stopped:
            return rungs
        seconds = max(done.seconds - sum(r.seconds for r in lines.rungs), 0.0)
        label = step.label(len(lines.rungs))
        last = lines.last if lines.bad is None else lines.bad
        if last is None:
            last = _unreported(lines, stdout, units_file)
        kept = last.metrics if isinstance(last, Reported) else {}
        match done.returncode:
            case None:
                return [*rungs, Failed(TimedOut(timeout), seconds, kept, label)]
            case 0:
                pass
            case code if lines.bad is None:
                stderr = (log_dir / f'{prefix}.stderr').read_text(errors='replace').strip()[-STDERR_IN_ERROR:]
                why = Exited(code, stderr) if code > 0 else Killed(-code, stderr)
                return [*rungs, Failed(why, seconds, kept, label)]
            case _:
                pass
        match last:
            case BadResult() | MissingMetrics():
                return [*rungs, Failed(last, seconds, rung=label)]
            case Reported(metrics=metrics, units=units, channel=channel):
                if missing := (required if step.last is None else set(step.last.stop)) - metrics.keys():
                    return [*rungs, Failed(MissingMetrics(tuple(sorted(missing))), seconds, metrics, label)]
                if lines.count and parse_metrics(stdout):
                    bad = BadResult(
                        f'it wrote a result to {RESULT_ENV} and printed METRIC lines: drop the METRIC lines'
                    )
                    return [*rungs, Failed(bad, seconds, rung=label)]
                if step.last is not None and gate is not None and (why := gate.stop_reason(step.last, metrics)):
                    return [*rungs, StoppedAt(step.last.name, why, metrics, seconds, units)]
                return [*rungs, Measured(metrics, seconds, units, channel, label)]
            case _:
                assert_never(last)
    finally:
        units_file.unlink(missing_ok=True)


def _unreported(lines: _Lines, stdout: str, units_file: Path) -> Reported | BadResult:
    """What a run that wrote no last line reported: its ``METRIC`` lines until 0.4.0, when it wrote no line at all."""
    if lines.count:
        return BadResult(f'it stopped after rung {lines.rungs[-1].rung}, before its last line')
    lines_printed = parse_metrics(stdout)
    if not lines_printed:
        return BadResult(f'it wrote no result to {RESULT_ENV}')
    if lines.step.rungs:
        return BadResult(f'a ladder reports its rungs on {RESULT_ENV}, not as METRIC lines')
    deprecated.until('0.4.0', 'METRIC lines on stdout: delete this branch, parse_metrics and UNITS_ENV')
    try:
        units = read_units(units_file, lines_printed) if units_file.exists() else {}
    except ValueError as e:
        return BadResult(f'{UNITS_ENV}: {e}')
    return Reported(lines_printed, units, Channel.METRIC_LINES, 0.0)


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
