"""The control operations: the changes that the orchestrator or a human asks of a campaign, besides the daemon's own.

Each one is a typed function. It validates its input, makes the change and writes its event in the caller's session,
and the caller commits once. ``Refused`` says why an operation changed nothing. The CLI is a thin wrapper over these
functions, and any other client (an API, a remote controller) calls the same ones, so nothing here assumes the caller
is on the daemon's machine.

A change to a column that the change log covers (``piloop.db.triggers.changelog``) gets its event from the trigger,
signed by the connection's actor, so an operation writes no second event for it. A reason that no column holds goes
into a ``note`` on the row. An operation emits the events of the changes that no trigger logs.
"""

from sqlalchemy.orm import Session

from piloop.config import config_of
from piloop.db.models import Base, Pause, Project


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
