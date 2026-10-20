"""Stop conditions: decide when a project's ``[stop]`` table, or a request, ends the campaign.

The scheduler checks every active project once per tick. The reason goes into ``projects.stopped_reason`` and into
the trigger of the closing watchdog. A stop or a cancel that someone requests is an event that the daemon acts on, so
any client of the database can make one.
"""

from datetime import datetime, timedelta

from sqlalchemy import ColumnElement, exists, func, select
from sqlalchemy.orm import Session

from piloop.agents.launcher import STOPPED_ERROR
from piloop.config import Direction, ProjectConfig
from piloop.db.events import emit
from piloop.db.models import (
    FINISHED_RUNS,
    LIVE,
    OPEN_RUNS,
    Candidate,
    Comment,
    CommentKind,
    Event,
    Idea,
    IdeaStatus,
    Measurement,
    Project,
    ProjectStatus,
    Run,
    TargetKind,
    Verdict,
    Worker,
    WorkerState,
    project_runs,
    utcnow,
)
from piloop.db.triggers import DAEMON_ACTOR
from piloop.scoring.ledger import head, keeps, score
from piloop.text import metric
from piloop.usage import project_tokens

STALE_ERROR = 'daemon exited while the run was in progress'
CAMPAIGN_STOPPED_ERROR = 'campaign stopped'
ROLE_REMOVED = 'its role is no longer in piloop.toml'
NOT_STARTED = 'no agent of the run started to work'
"""The error of a run whose every agent failed to start (``launcher.STARTUP_ANOMALY`` on each): a fault of the host or
the harness. The errors of its workers say what failed."""
CANCELLED_ERROR = 'cancelled'
"""The error of a run, and of an agent, that someone cancelled (``CANCEL_REQUESTED``). The run's idea is parked."""
NOT_ATTEMPTS = (STOPPED_ERROR, STALE_ERROR, CAMPAIGN_STOPPED_ERROR, ROLE_REMOVED, NOT_STARTED, CANCELLED_ERROR)
"""Run errors that mean the run was stopped, or that no agent started to work on it, not that the idea was tried.

The causes are a daemon stop or restart (``piloop upgrade``), a daemon crash, a campaign stop, a ``piloop init`` that
removed the role of a queued run, agents that failed to start, or a cancel. Such a run is not counted as an attempt at
its idea. It does not count toward ``max_runs`` or ``plateau``, and it is not an anomaly: the failed starts are."""
STOP_REQUESTED = 'project.stop_requested'
"""Event on a project that someone asked to stop (``piloop project stop``), with ``reason`` and ``kill``.

On its next tick, the daemon stops the campaign as for a ``[stop]`` condition (``stop_reason``). With ``kill``, it
ends the project's workers at once, without the wrap-up prompt. A request made before the project was last set
``active`` is spent."""
CANCELLABLE = (WorkerState.REQUESTED, *LIVE)
"""The states of an agent that a cancel acts on: requested, starting (once it runs) and running."""
CANCEL_REQUESTED = ('run.cancel_requested', 'worker.cancel_requested')
"""Events on a run (``piloop run cancel``) and on an agent (``piloop agent cancel``) that someone asked to cancel, with
``wrap_up``.

The daemon closes a requested row with its queued run. It ends a running agent with everything it started, at once,
or with ``wrap_up`` after the wrap-up prompt and its grace period. A starting agent waits until it runs, and a run
whose agent then fails to start is not launched again. The run closes with ``CANCELLED_ERROR`` unless its worker left
its finding, and its commits are scored."""
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


def request_stop(s: Session, project: Project, actor: str, *, reason: str, kill: bool) -> None:
    """Ask the daemon to stop ``project`` (``STOP_REQUESTED``). The caller commits."""
    emit(s, STOP_REQUESTED, TargetKind.PROJECT, project.id, actor=actor, payload={'reason': reason, 'kill': kill})


def requested_stop(s: Session, project: Project) -> Event | None:
    """Return the latest stop request since the project was last set ``active``, or None."""
    reactivated = s.scalar(
        select(func.max(Event.id)).where(
            Event.kind == 'project.status',
            Event.target_kind == TargetKind.PROJECT,
            Event.target_id == project.id,
            Event.payload['new'].as_string() == ProjectStatus.ACTIVE.value,
        )
    )
    return s.scalars(
        select(Event)
        .where(Event.kind == STOP_REQUESTED, Event.target_kind == TargetKind.PROJECT, Event.target_id == project.id)
        .where(Event.id > (reactivated or 0))
        .order_by(Event.id.desc())
        .limit(1)
    ).first()


def request_cancel(s: Session, target: Run | Worker, actor: str, *, wrap_up: bool) -> None:
    """Ask the daemon to cancel a run or an agent (``CANCEL_REQUESTED``). The caller commits."""
    kind, on = (
        (CANCEL_REQUESTED[0], TargetKind.RUN) if isinstance(target, Run) else (CANCEL_REQUESTED[1], TargetKind.WORKER)
    )
    emit(s, kind, on, target.id, actor=actor, payload={'wrap_up': wrap_up})


def run_cancel(s: Session, run_id: int) -> Event | None:
    """Return the latest cancel of a run: requested on the run, or on any of its agents, or None.

    A cancel on an agent holds for its run, so a worker that a relaunch puts in the place of one that failed to start
    is cancelled too.
    """
    on_run = (Event.target_kind == TargetKind.RUN) & (Event.target_id == run_id)
    agents = select(Worker.id).where(Worker.run_id == run_id)
    on_agent = (Event.target_kind == TargetKind.WORKER) & Event.target_id.in_(agents)
    return _latest_cancel(s, on_run | on_agent)


def agent_cancel(s: Session, worker: Worker) -> Event | None:
    """Return the latest cancel of an agent, or None: its run's (``run_cancel``) for a worker, its own for a sweep."""
    if worker.run_id is not None:
        return run_cancel(s, worker.run_id)
    return _latest_cancel(s, (Event.target_kind == TargetKind.WORKER) & (Event.target_id == worker.id))


def _latest_cancel(s: Session, target: ColumnElement[bool]) -> Event | None:
    query = select(Event).where(Event.kind.in_(CANCEL_REQUESTED), target).order_by(Event.id.desc()).limit(1)
    return s.scalars(query).first()


def park_cancelled(s: Session, run: Run) -> None:
    """Park the idea of a cancelled run, with a note on the idea: a run is cancelled because it is not wanted now."""
    cancel = run_cancel(s, run.id)
    run.idea.status = IdeaStatus.PARKED
    by = f' by {cancel.actor}' if cancel is not None else ''
    body = f'parked: run#{run.id} was cancelled{by}; set the idea ready to run it again'
    s.add(
        Comment(
            target_kind=TargetKind.IDEA, target_id=run.idea_id, author=DAEMON_ACTOR, kind=CommentKind.NOTE, body=body
        )
    )


def stop_reason(s: Session, project: Project, cfg: ProjectConfig) -> str | None:
    """Return why ``project`` stops now, or None: a stop request (``requested_stop``), else the first condition."""
    if (request := requested_stop(s, project)) is not None:
        why = request.payload.get('reason')
        return f'stopped by {request.actor}: {why}' if why else f'stopped by {request.actor}'
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
    """Return the hours from the start of the project's first run to ``now``, or None if no run started.

    The time that the project was paused (``piloop pause``) does not count, so a pause does not use up ``max_hours``.
    The pauses are the ``project.paused`` events of the change log.
    """
    first = _over_runs(s, project, func.min(Run.started_at))
    return None if first is None else (now - first - paused_for(s, project, first, now)) / timedelta(hours=1)


def paused_for(s: Session, project: Project, since: datetime, now: datetime) -> timedelta:
    """Return how long the project was paused between ``since`` and ``now``."""
    changes = s.execute(
        select(Event.at, Event.payload['new'].as_string())
        .where(Event.kind == 'project.paused', Event.target_kind == TargetKind.PROJECT, Event.target_id == project.id)
        .order_by(Event.id)
    ).all()
    total, start = timedelta(), None
    for at, new in changes:
        if new is not None and start is None:
            start = at
        elif new is None and start is not None:
            total += max(at - max(start, since), timedelta())
            start = None
    if start is not None:
        total += max(now - max(start, since), timedelta())
    return total


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
