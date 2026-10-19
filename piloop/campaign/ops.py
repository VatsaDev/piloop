"""The control operations: the changes that the orchestrator or a human asks of a campaign, besides the daemon's own.

Each one is a typed function. It validates its input, makes the change and writes its event in the caller's session,
and the caller commits once. ``Refused`` says why an operation changed nothing. The CLI is a thin wrapper over these
functions, and any other client (an API, a remote controller) calls the same ones, so nothing here assumes the caller
is on the daemon's machine.

A change to a column that the change log covers (``piloop.db.triggers.changelog``) gets its event from the trigger,
signed by the connection's actor, so an operation writes no second event for it. A reason that no column holds goes
into a ``note`` on the row. An operation emits the events of the changes that no trigger logs.
"""

from dataclasses import dataclass
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.orm import Session

from piloop.campaign import spend
from piloop.config import ProjectConfig, config_of
from piloop.db.events import emit
from piloop.db.models import Base, Candidate, Measurement, Pause, Project, ProjectStatus, TargetKind, Verdict
from piloop.scoring.ledger import REJUDGE_REQUESTED, head, rejudgeable


class Refused(Exception):
    """An operation refused its input and changed nothing. The message says why."""


def _get[M: Base](s: Session, model: type[M], key: int, name: str) -> M:
    found = s.get(model, key)
    if found is None:
        raise Refused(f'no {name}#{key}')
    return found


def _reason(reason: str) -> str:
    if not reason.strip():
        raise Refused('a reason is required: say why, with ids')
    return reason.strip()


def pause(s: Session, project_id: int, *, evals: bool, reason: str) -> Project:
    """Pause the project: nothing launches, and with ``evals`` the daemon starts no measurement either.

    No run is queued, no role is requested and no requested row launches, so research files no ideas. A sweep that
    runs already files its ideas as ``proposed`` (``holds_ideas``). Running work goes on and its commits are scored.
    Nothing is escalated. Without ``evals``, the daemon still measures what a new evaluator or a request needs, so the
    pause can end on a measured head. A pause is state in the database, so the daemon holds wherever the caller is.

    Raises:
        Refused: the reason is blank, or the project is paused that way already.
    """
    project = _get(s, Project, project_id, 'project')
    why = _reason(reason)
    scope = Pause.ALL if evals else Pause.LAUNCHES
    if project.paused is scope:
        raise Refused(f'project {project.name} is paused already ({scope}): {project.paused_reason}')
    project.paused, project.paused_reason = scope, why
    return project


def resume(s: Session, project_id: int) -> Pause:
    """Resume a paused project, and return what it held. The daemon schedules it again on its next tick.

    Raises:
        Refused: the project is not paused.
    """
    project = _get(s, Project, project_id, 'project')
    if project.paused is None:
        raise Refused(f'project {project.name} is not paused')
    held = project.paused
    project.paused, project.paused_reason = None, None
    return held


def holds_ideas(project: Project, actor: str) -> bool:
    """Return whether a new or developed idea of ``actor`` waits for a review (``proposed``) because of a pause.

    Only an agent of a sweeping role waits. The orchestrator and humans paused the project themselves.
    """
    return project.paused is not None and actor.partition('#')[0] in config_of(project).roles.sweeping()


OVERRIDES = (Verdict.KEEP, Verdict.INTERESTING, Verdict.DISCARD, Verdict.CRASH)
"""The judgements: what ``set_verdict`` sets, and what it overrides. The other verdicts are states: ``pending`` (the
daemon judges it now), ``reference`` (an import), ``unmeasured`` and ``recovered`` (nothing was measured)."""


def set_verdict(s: Session, candidate_id: int, verdict: Verdict, *, reason: str, actor: str) -> Verdict:
    """Override a candidate's judgement with ``actor``'s, and return the verdict it had.

    The verdict is ``actor``'s now, so the daemon never changes it. The ``candidate.verdict`` event (a trigger) has the
    reason. A keep of a trunk run must build on the head (``builds_on_head``): the head is the newest trunk keep, and a
    keep that lacks the head's changes would drop them from the trunk. A keep of a branch run never moves the head.

    Raises:
        Refused: the reason is blank, the old or the new verdict is not a judgement, the candidate has that verdict
            already, or it would be a trunk keep that does not build on the head.
    """
    candidate = _get(s, Candidate, candidate_id, 'candidate')
    why = _reason(reason)
    if verdict not in OVERRIDES:
        raise Refused(f'{verdict} is not a judgement: give {", ".join(OVERRIDES)}')
    if candidate.verdict is Verdict.PENDING:
        raise Refused(f'candidate#{candidate.id} is pending: the daemon judges it now; override it once it is judged')
    if candidate.verdict not in OVERRIDES:
        raise Refused(
            f'candidate#{candidate.id} is {candidate.verdict}, not a judgement: only a judgement is overridden'
        )
    if candidate.verdict is verdict:
        raise Refused(f'candidate#{candidate.id} is {verdict} already ({candidate.verdict_reason or "no reason"})')
    if verdict is Verdict.KEEP and (trunk := missing_head(s, candidate)) is not None:
        raise Refused(
            f'candidate#{candidate.id} does not build on the head candidate#{trunk.id}: as a keep it would drop what'
            ' the head kept. Mark it interesting, or add an idea that merges it onto the head'
        )
    old = candidate.verdict
    candidate.verdict, candidate.verdict_by, candidate.verdict_reason = verdict, actor, why
    return old


def missing_head(s: Session, candidate: Candidate) -> Candidate | None:
    """Return the head if a keep of ``candidate`` would be a trunk keep without it, else None.

    A candidate builds on the head when the head is among its ancestors (``parent_id``): a commit of a run that
    started from it, or a merge onto it. A candidate of a branch run is a branch keep, which never moves the head.
    """
    if candidate.run is not None and candidate.run.branch:
        return None
    trunk = head(s, s.get_one(Project, candidate.project_id))
    ancestor: Candidate | None = candidate
    while ancestor is not None:
        if trunk is None or ancestor.id == trunk.id:
            return None
        ancestor = ancestor.parent
    return trunk


class Report(BaseModel):
    """A measurement made outside the daemon, for example by running the evaluate command by hand."""

    model_config = ConfigDict(extra='forbid')

    metrics: dict[str, Annotated[float, Field(strict=True, allow_inf_nan=False)]]
    """Each metric's value, as the evaluate command's ``METRIC`` lines would give it."""
    seconds: Annotated[float, Field(ge=0, allow_inf_nan=False)] | None = None
    """How long the evaluation took."""
    error: str | None = None
    """Why the evaluation failed. A failed measurement may lack metrics."""


@dataclass(frozen=True)
class Added:
    """What ``add_measurement`` recorded, and what it does to the candidate's verdict."""

    measurement: Measurement
    rejudged: bool
    """Whether the daemon judges the candidate again (``REJUDGE_REQUESTED``)."""
    effect: str
    """What happens to the verdict, in one line: when the daemon judges it again, or why the verdict stays."""


def add_measurement(s: Session, candidate_id: int, report: Report, *, actor: str) -> Added:
    """Record ``report`` as a measurement of the candidate under the project's current evaluator.

    The daemon judges the candidate again when its verdict is the daemon's ``crash``, ``discard`` or ``interesting`` on
    a run (``ledger.rejudgeable``), with every measurement under the current evaluator. It does that when it measures
    the project again: not while the project is stopped, paused with its evaluations, or held by the spend guard. Any
    other verdict stays, and a keep's mean, which later runs from it must beat, includes the new measurement. Emits
    ``measurement.recorded``, shaped like the daemon's, and the request (``REJUDGE_REQUESTED``).

    Raises:
        Refused: the project has no evaluator yet, the candidate is pending (the daemon is measuring and judging it),
            the report names a metric that ``piloop.toml`` does not, or a report without an error lacks one.
    """
    candidate = _get(s, Candidate, candidate_id, 'candidate')
    project = s.get_one(Project, candidate.project_id)
    if project.evaluator_id is None:
        raise Refused(f'project {project.name} has no evaluator yet: run `piloop init`')
    if candidate.verdict is Verdict.PENDING:
        raise Refused(f'candidate#{candidate.id} is pending: the daemon judges it now; add the measurement after that')
    cfg = config_of(project)
    names = {m.name for m in cfg.metrics}
    if unknown := sorted(report.metrics.keys() - names):
        raise Refused(f'piloop.toml has no metric {", ".join(unknown)}: its metrics are {", ".join(sorted(names))}')
    missing = sorted(names - report.metrics.keys())
    if report.error is None and missing:
        raise Refused(f'the report has no {", ".join(missing)}: give every metric, or the error')
    measurement = Measurement(
        candidate_id=candidate.id,
        evaluator_id=project.evaluator_id,
        metrics=report.metrics,
        error=report.error,
        seconds=report.seconds,
    )
    s.add(measurement)
    s.flush()
    payload: dict[str, object] = {
        'candidate': candidate.id,
        'metrics': report.metrics,
        'error': report.error,
        'run': None,
    }
    emit(s, 'measurement.recorded', TargetKind.MEASUREMENT, measurement.id, actor=actor, payload=payload)
    if not rejudgeable(candidate):
        return Added(measurement, False, _stays(candidate))
    emit(s, REJUDGE_REQUESTED, TargetKind.CANDIDATE, candidate.id, actor=actor, payload={'measurement': measurement.id})
    hold = _measuring_held(project, cfg)
    later = f' once {hold}' if hold is not None else ''
    return Added(measurement, True, f'the daemon judges it again with all its measurements{later}')


def _stays(candidate: Candidate) -> str:
    if candidate.verdict is Verdict.KEEP:
        return 'its keep stays; the runs that start from it must beat its new mean'
    return f'its verdict stays: {candidate.verdict}, set by {candidate.verdict_by or "nobody"}'


def _measuring_held(project: Project, cfg: ProjectConfig) -> str | None:
    """Return when the daemon measures the project again if it does not now, as the scheduler's tick decides."""
    if project.status is ProjectStatus.STOPPED:
        return f'project {project.name} is active again (it stopped: {project.stopped_reason})'
    if project.paused is Pause.ALL:
        return f'project {project.name} is resumed (its evaluations are paused)'
    if (why := spend.held(project, cfg)) is not None:
        return f'the spend guard lets it ({why})'
    return None
