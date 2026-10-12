"""The spend guard: the daemon polls a project's ``[spend] command``, posts notices and enforces a hard limit.

piloop does not know any vendor. The project's command prints the total that the campaign has spent so far, in USD,
for example a GPU cloud's bill for the project's environments. The daemon keeps the latest reading on the project
row. The daemon enforces the limit, not an agent, so nothing launches past the limit even when no agent is active.
"""

import math
import subprocess
from datetime import timedelta
from pathlib import Path
from subprocess import PIPE

from sqlalchemy.orm import Session

from piloop.config import ProjectConfig, Spend
from piloop.db.models import Comment, CommentKind, Project, TargetKind, utcnow
from piloop.db.triggers import DAEMON_ACTOR
from piloop.proc import STDERR_IN_ERROR, child_env, kill_group

TIMEOUT = 60
"""The seconds that the spend command can take. The daemon's tick waits for it, so a hung command fails."""
NOTICE_PRIORITY = 1
ESCALATION_PRIORITY = 0
"""An escalation from the guard stops all launches, so it is the most urgent item in the inbox."""


class SpendError(Exception):
    """The spend command did not print a spend. It failed, timed out or printed something else."""


def due(project: Project, spend: Spend) -> bool:
    """Return True if the command never ran, or last ran ``interval`` or more seconds ago."""
    return project.spend_at is None or utcnow() - project.spend_at >= timedelta(seconds=spend.interval)


def read(command: list[str], cwd: Path) -> float:
    """Run the spend command and parse its output as one finite, non-negative number of USD.

    Raises:
        SpendError: The command did not start, timed out, exited non-zero or printed something else.
    """
    try:
        proc = subprocess.Popen(command, cwd=cwd, env=child_env(), stdout=PIPE, stderr=PIPE, start_new_session=True)
    except OSError as e:
        raise SpendError(f'could not start: {e}') from e
    with proc:
        try:
            out, err = proc.communicate(timeout=TIMEOUT)
        except subprocess.TimeoutExpired as e:
            # Kill the children too. A child that holds the pipe open blocks the read below.
            kill_group(proc)
            proc.communicate()
            raise SpendError(f'timed out after {TIMEOUT}s') from e
    stdout, stderr = out.decode(errors='replace'), err.decode(errors='replace')
    if proc.returncode != 0:
        raise SpendError(f'exit {proc.returncode}: {stderr.strip()[-STDERR_IN_ERROR:]}')
    try:
        usd = float(stdout)
    except ValueError as e:
        raise SpendError(f'printed {stdout.strip()[:STDERR_IN_ERROR]!r}, not a number of USD') from e
    if not math.isfinite(usd) or usd < 0:
        raise SpendError(f'printed {usd}, not a spend')
    return usd


def record(s: Session, project: Project, spend: Spend, reading: float | SpendError) -> None:
    """Store a reading on the project row and post the notes and escalations that it causes. The caller commits.

    A reading posts a note for each ``notices`` threshold that it crossed since the previous reading. It posts an
    escalation if it crossed ``limit``. The first reading crosses every threshold below it. Because only a crossing
    posts, a raised limit escalates again when the spend reaches it. A failed reading escalates only if the previous
    reading did not fail.
    """
    project.spend_at = utcnow()
    if isinstance(reading, SpendError):
        if project.spend_error is None:
            _post(s, project, CommentKind.ESCALATION, ESCALATION_PRIORITY, _failed(reading))
        project.spend_error = str(reading)
        return
    before = project.spend_usd if project.spend_usd is not None else -math.inf
    project.spend_usd, project.spend_error = reading, None
    for threshold in spend.notices:
        if before < threshold <= reading:
            body = f'Spend notice: ${reading:,.2f} spent, past the ${threshold:,.2f} notice. The limit is ${spend.limit:,.2f}.'
            _post(s, project, CommentKind.NOTE, NOTICE_PRIORITY, body)
    if before < spend.limit <= reading:
        _post(s, project, CommentKind.ESCALATION, ESCALATION_PRIORITY, _limited(reading, spend.limit))


def _failed(error: SpendError) -> str:
    return (
        f'Spend unknown: the [spend] command failed ({error}). Nothing launches and no evaluation starts until it'
        ' prints a spend again. Runs in progress finish. Fix the command or its environment. It runs again every'
        ' interval.'
    )


def _limited(usd: float, limit: float) -> str:
    return (
        f'Spend limit reached: ${usd:,.2f} spent, limit ${limit:,.2f}. Nothing launches and no evaluation starts.'
        ' Runs in progress finish and are scored. To resume, raise [spend] limit in piloop.toml and run `piloop init`.'
    )


def _post(s: Session, project: Project, kind: CommentKind, priority: int, body: str) -> None:
    s.add(
        Comment(
            target_kind=TargetKind.PROJECT,
            target_id=project.id,
            author=DAEMON_ACTOR,
            kind=kind,
            priority=priority,
            body=body,
        )
    )


def held(project: Project, cfg: ProjectConfig) -> str | None:
    """Return why the guard holds launches and evaluations now.

    Returns None if there is no ``[spend]`` table, or if the spend is known and below the limit.
    """
    spend = cfg.spend
    if spend is None:
        return None
    if project.spend_error is not None:
        return f'spend unknown: {project.spend_error}'
    if project.spend_usd is None:
        return 'spend not read yet'
    if project.spend_usd >= spend.limit:
        return 'spend limit reached'
    return None


def progress(project: Project, cfg: ProjectConfig) -> str | None:
    """Return ``$412.30/$1,000.00 spend`` and, if launches are held, why. Return None if there is no ``[spend]``."""
    spend = cfg.spend
    if spend is None:
        return None
    usd = '$?' if project.spend_usd is None else f'${project.spend_usd:,.2f}'
    hold = held(project, cfg)
    return f'{usd}/${spend.limit:,.2f} spend' + (f' (launches held: {hold})' if hold is not None else '')
