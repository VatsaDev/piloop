"""Pick the runtime a project configured."""

from collections.abc import Callable
from pathlib import Path

from sqlalchemy.orm import Session

from piloop.config import RuntimeKind
from piloop.db.models import Project, Worker
from piloop.runtime import Handle, Runtime
from piloop.runtime.herdr import HerdrRuntime
from piloop.runtime.subprocess import SubprocessRuntime

RUNTIMES: dict[RuntimeKind, Callable[[str, Path], Runtime]] = {
    RuntimeKind.HERDR: HerdrRuntime,
    RuntimeKind.SUBPROCESS: SubprocessRuntime,
}
"""The constructor for each kind, called as ``(project, project_path)``. A new runtime adds one entry and one enum
member."""


def make_runtime(kind: RuntimeKind, *, project: str, project_path: Path) -> Runtime:
    """Create the runtime of ``kind``.

    Herdr uses the project to name its workspace. The subprocess runtime ignores it.
    """
    return RUNTIMES[kind](project, project_path)


type RuntimeFactory = Callable[..., Runtime]
"""The signature of ``make_runtime``: ``(kind, *, project, project_path) -> Runtime``. Tests substitute their own."""


def handle_for(worker: Worker) -> Handle:
    """Return the runtime address of a launched worker row. A requested row has none yet."""
    if worker.runtime is None:
        raise ValueError(f'worker#{worker.id} is {worker.state}: not launched yet')
    return Handle(worker.runtime, worker.handle)


def runtime_for(s: Session, worker: Worker, fallback_path: Path, factory: RuntimeFactory = make_runtime) -> Runtime:
    """Return the runtime that owns a launched worker row. Its project names the Herdr workspace."""
    project = s.get(Project, worker.project_id) if worker.project_id else None
    return factory(
        RuntimeKind(handle_for(worker).runtime),
        project=project.name if project else 'piloop',
        project_path=Path(project.path) if project else fallback_path,
    )
