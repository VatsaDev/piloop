"""Stop conditions: decide when a project's ``[stop]`` table ends the campaign.

The scheduler checks every active project once per tick. The reason goes into ``projects.stopped_reason`` and into
the trigger of the closing watchdog.
"""

from datetime import datetime, timedelta

from sqlalchemy import ColumnElement, exists, func, select
from sqlalchemy.orm import Session

from piloop.agents.launcher import STOPPED_ERROR
from piloop.config import Direction, ProjectConfig
from piloop.db.models import (
    FINISHED_RUNS,
    OPEN_RUNS,
    Candidate,
    Event,
    Idea,
    Measurement,
    Project,
    ProjectStatus,
    Run,
    TargetKind,
    Verdict,
    project_runs,
    utcnow,
)
from piloop.scoring.ledger import head, keeps, score
from piloop.text import metric
from piloop.usage import project_tokens

STALE_ERROR = 'daemon exited while the run was in progress'
CAMPAIGN_STOPPED_ERROR = 'campaign stopped'
ROLE_REMOVED = 'its role is no longer in piloop.toml'
NOT_STARTED = 'no agent of the run started to work'
"""The error of a run whose every agent failed to start (``launcher.STARTUP_ANOMALY`` on each): a fault of the host or
the harness. The errors of its workers say what failed."""
NOT_ATTEMPTS = (STOPPED_ERROR, STALE_ERROR, CAMPAIGN_STOPPED_ERROR, ROLE_REMOVED, NOT_STARTED)
"""Run errors that mean the daemon stopped the run, or that no agent started to work on it, not that the idea was tried.

The causes are a daemon stop or restart (``piloop upgrade``), a daemon crash, a campaign stop while the run was
queued, a ``piloop init`` that removed the role of a queued run, or agents that failed to start. Such a run is not
counted as an attempt at its idea. It does not count toward ``max_runs`` or ``plateau``, and it is not an anomaly: the
failed starts are."""
BLOCKED = 'blocked on escalation'
"""How the error of a run starts when its worker stopped while its own escalation waited for an answer.

The run is an attempt that failed, but not an anomaly: the escalation is in the inbox already. Its idea is parked
until someone answers the escalation and sets the idea ``ready``."""


def attempt() -> ColumnElement[bool]:
    """Select the runs that were an attempt at their idea: all runs except those with a ``NOT_ATTEMPTS`` error."""
    return Run.error.is_(None) | Run.error.not_in(NOT_ATTEMPTS)


def blocked() -> ColumnElement[bool]:
    """Select the runs that ended blocked on their worker's escalation (``BLOCKED``)."""
    return Run.error.is_not(None) & Run.error.startswith(BLOCKED, autoescape=True)


def scored() -> ColumnElement[bool]:
    """Select the runs that have a candidate that is measured or waiting for a verdict."""
    return exists().where(
        Candidate.run_id == Run.id,
        (Candidate.verdict == Verdict.PENDING) | exists().where(Measurement.candidate_id == Candidate.id),
    )


def unsettled() -> ColumnElement[bool]:
    """Select the runs whose outcome can still change: the open runs, and the runs that are scored now (``scoring``)."""
    return Run.status.in_(OPEN_RUNS) | Run.scoring


def counted() -> ColumnElement[bool]:
    """Select the runs that count toward the run limits, ``max_runs`` and ``plateau``: those that reached the evaluator.

    Such a run has a candidate that is measured or waiting for its verdict (``scored``). A run that stopped with no
    commit, or with only ``Piloop-Eval: skip`` commits, does not count: it stopped at a cheap check, or a harness or
    host fault ended it before it committed. It says nothing about the idea's effect on the metric. ``[spend]``,
    ``max_tokens`` and ``max_hours`` limit what such runs cost. This is the one rule for both limits.
    """
    return attempt() & scored()


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
    return f'target: {primary.name} {metric(value)} {sign} {metric(target)}'


def _over_runs[T](s: Session, project: Project, aggregate: ColumnElement[T], *where: ColumnElement[bool]) -> T | None:
    return s.scalar(project_runs(project.id).with_only_columns(aggregate).where(*where))


def runs(s: Session, project: Project) -> int:
    """Count the project's runs, of every role, that hold a place under ``max_runs``.

    These are the runs that count (``counted``) and the unsettled attempts, which may still count. So the queue never
    starts more runs than ``max_runs``.
    """
    return _over_runs(s, project, func.count(Run.id), counted() | (attempt() & unsettled())) or 0


def hours(s: Session, project: Project, now: datetime) -> float | None:
    """Return the hours from the start of the project's first run to ``now``, or None if no run started."""
    first = _over_runs(s, project, func.min(Run.started_at))
    return None if first is None else (now - first) / timedelta(hours=1)


def runs_without_keep(s: Session, project: Project) -> int:
    """Count the finished runs that count (``counted``) since the latest keep or reactivation. ``plateau`` uses this.

    The window starts when the run of the latest keep finished, or when the project was last set ``active`` again
    after a stop (``piloop init``, or an UPDATE), whichever is later. A reactivated campaign gets a fresh window, so
    a ``plateau`` stop does not fire again on the next tick. An evaluator change alone does not reset the window.
    """
    last_keep = s.scalar(
        keeps(project).join(Run, Candidate.run_id == Run.id).with_only_columns(func.max(Run.finished_at))
    )
    reactivated = s.scalar(
        select(func.max(Event.at)).where(
            Event.kind == 'project.status',
            Event.target_kind == TargetKind.PROJECT,
            Event.target_id == project.id,
            Event.payload['new'].as_string() == ProjectStatus.ACTIVE.value,
        )
    )
    since = max((t for t in (last_keep, reactivated) if t is not None), default=None)
    after = (Run.finished_at > since,) if since is not None else ()
    return _over_runs(s, project, func.count(Run.id), Run.status.in_(FINISHED_RUNS), counted(), *after) or 0


def stall(cfg: ProjectConfig) -> tuple[int, str]:
    """Return the number of runs without a keep after which research must pivot, and how it was derived.

    The number is half of ``plateau``, so half of the campaign's patience remains after the pivot. Without
    ``plateau``, it is two rounds of the run slots (``ProjectConfig.run_slots``).
    """
    if cfg.stop.plateau is not None:
        return -(-cfg.stop.plateau // 2), f'half of [stop] plateau = {cfg.stop.plateau}'
    slots = ' + '.join(f'[roles.{name}] max_concurrent' for name in cfg.roles.doing_runs())
    return 2 * cfg.run_slots, f'two rounds of {slots} = {cfg.run_slots}'


def _max_runs(s: Session, project: Project, limit: int) -> str | None:
    """Stop when ``limit`` runs count (``runs``) and none of them is unsettled.

    An unsettled run counts, so the queue starts no more than ``limit`` runs. It can still end without a scored
    candidate, and then it does not count. A stop at that moment would end the run early, and the count would drop
    below ``limit``.
    """
    count = runs(s, project)
    unsettled_runs = _over_runs(s, project, func.count(Run.id), attempt(), unsettled())
    return f'max_runs: {count} runs' if count >= limit and not unsettled_runs else None


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
