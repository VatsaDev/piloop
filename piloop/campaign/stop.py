"""Stop conditions: decide when a project's ``[stop]`` table ends the campaign.

The scheduler checks every active project once per tick. The reason goes into ``projects.stopped_reason`` and into
the trigger of the closing watchdog.
"""

from datetime import datetime, timedelta

from sqlalchemy import ColumnElement, exists, func
from sqlalchemy.orm import Session

from piloop.agents.launcher import STOPPED_ERROR
from piloop.config import Direction, ProjectConfig
from piloop.db.models import (
    FINISHED_RUNS,
    OPEN_RUNS,
    Candidate,
    Idea,
    Measurement,
    Project,
    Run,
    Verdict,
    project_runs,
    utcnow,
)
from piloop.scoring.ledger import head, keeps, score
from piloop.usage import project_tokens

STALE_ERROR = 'daemon exited while the run was in progress'
CAMPAIGN_STOPPED_ERROR = 'campaign stopped'
NOT_ATTEMPTS = (STOPPED_ERROR, STALE_ERROR, CAMPAIGN_STOPPED_ERROR)
"""Run errors that mean the daemon stopped the run, not that the idea was tried.

The causes are a daemon stop or restart (``piloop upgrade``), a daemon crash, or a campaign stop while the run was
queued. Such a run is not counted as an attempt at its idea. It does not count toward ``max_runs`` or ``plateau``,
and it is not an anomaly."""


def attempt() -> ColumnElement[bool]:
    """Select the runs that were an attempt at their idea: all runs except those with a ``NOT_ATTEMPTS`` error."""
    return Run.error.is_(None) | Run.error.not_in(NOT_ATTEMPTS)


def counted() -> ColumnElement[bool]:
    """Select the runs that ``max_runs`` and ``plateau`` count.

    These are attempts that are still open, or that have a candidate that is measured or waiting for a verdict. A run
    that stopped with no commit, or with only ``Piloop-Eval: skip`` commits, does not count. Its cost is limited by
    ``[spend]`` and ``max_tokens`` instead. Many runs end this way, so counting them would use up ``max_runs`` fast.
    """
    scored = exists().where(
        Candidate.run_id == Run.id,
        (Candidate.verdict == Verdict.PENDING) | exists().where(Measurement.candidate_id == Candidate.id),
    )
    return attempt() & (Run.status.in_(OPEN_RUNS) | scored)


def attempts(idea: Idea) -> int:
    """Count the runs of ``idea`` that were an attempt at it. ``ideas.max_runs`` caps this number."""
    return sum(run.error not in NOT_ATTEMPTS for run in idea.runs)


def stop_reason(s: Session, project: Project, cfg: ProjectConfig) -> str | None:
    """Return the first ``[stop]`` condition that holds for ``project`` as a reason, or None."""
    stop = cfg.stop
    checks = (
        _target(s, project, cfg, stop.target) if stop.target is not None else None,
        _max_runs(s, project, stop.max_runs) if stop.max_runs is not None else None,
        _max_hours(s, project, stop.max_hours) if stop.max_hours is not None else None,
        _max_tokens(s, project, stop.max_tokens) if stop.max_tokens is not None else None,
        _plateau(s, project, stop.plateau) if stop.plateau is not None else None,
    )
    return next((reason for reason in checks if reason is not None), None)


def _target(s: Session, project: Project, cfg: ProjectConfig, target: float) -> str | None:
    """Stop when the head reaches ``target`` on the primary metric.

    Config validation rejects ``target`` for a project with more than one track.
    """
    primary = cfg.all_tracks[0].primary
    trunk = head(s, project)
    value = score(trunk, project.evaluator_id or 0, primary) if trunk is not None else None
    if value is None or (value != target and not primary.better(value, target)):
        return None
    sign = '≥' if primary.direction is Direction.HIGHER else '≤'
    return f'target: {primary.name} {value:.4f} {sign} {target:g}'


def _over_runs[T](s: Session, project: Project, aggregate: ColumnElement[T], *where: ColumnElement[bool]) -> T | None:
    return s.scalar(project_runs(project.id).with_only_columns(aggregate).where(*where))


def runs(s: Session, project: Project) -> int:
    """Count the project's worker runs that count toward ``max_runs`` (see ``counted``)."""
    return _over_runs(s, project, func.count(Run.id), counted()) or 0


def hours(s: Session, project: Project, now: datetime) -> float | None:
    """Return the hours from the start of the project's first run to ``now``, or None if no run started."""
    first = _over_runs(s, project, func.min(Run.started_at))
    return None if first is None else (now - first) / timedelta(hours=1)


def runs_without_keep(s: Session, project: Project) -> int:
    """Count the runs (see ``counted``) that finished after the run of the latest keep. ``plateau`` uses this number."""
    last_keep = s.scalar(
        keeps(project).join(Run, Candidate.run_id == Run.id).with_only_columns(func.max(Run.finished_at))
    )
    since = (Run.finished_at > last_keep,) if last_keep is not None else ()
    return _over_runs(s, project, func.count(Run.id), Run.status.in_(FINISHED_RUNS), counted(), *since) or 0


def stall(cfg: ProjectConfig) -> tuple[int, str]:
    """Return the number of runs without a keep after which research must pivot, and how it was derived.

    The number is half of ``plateau``, so half of the campaign's patience remains after the pivot. Without
    ``plateau``, it is two rounds of the worker slots.
    """
    if cfg.stop.plateau is not None:
        return -(-cfg.stop.plateau // 2), f'half of [stop] plateau = {cfg.stop.plateau}'
    slots = cfg.roles.worker.max_concurrent
    return 2 * slots, f'two rounds of [roles.worker] max_concurrent = {slots}'


def _max_runs(s: Session, project: Project, limit: int) -> str | None:
    count = runs(s, project)
    return f'max_runs: {count} runs' if count >= limit else None


def _max_hours(s: Session, project: Project, limit: float) -> str | None:
    elapsed = hours(s, project, utcnow())
    if elapsed is None or elapsed < limit:
        return None
    return f'max_hours: {elapsed:.1f}h since the first run'


def _max_tokens(s: Session, project: Project, limit: int) -> str | None:
    """Count the tokens of all agents, as recorded when they ended. An agent without a session file counts 0."""
    total = project_tokens(s, project.id)
    return f'max_tokens: {total} tokens spent' if total >= limit else None


def _plateau(s: Session, project: Project, limit: int) -> str | None:
    count = runs_without_keep(s, project)
    return f'plateau: {count} runs without a keep' if count >= limit else None
