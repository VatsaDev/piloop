"""The runtime registry, and the runtime of a project or of a launched worker row."""

from collections.abc import Callable
from functools import cache
from importlib.metadata import entry_points
from pathlib import Path
from typing import TypeIs

from sqlalchemy.orm import Session

from piloop.db.models import Project, Worker
from piloop.harnesses import Invocation
from piloop.runtime import Handle, Runtime
from piloop.runtime.herdr import HerdrRuntime
from piloop.runtime.subprocess import SubprocessRuntime

RUNTIME_GROUP = 'piloop.runtimes'
"""The entry point group of the runtimes that packages add. Each entry point names a ``Runtime`` class whose ``kind``
is the name of the entry point."""
BUILTIN_RUNTIMES: dict[str, type[Runtime]] = {
    HerdrRuntime.kind: HerdrRuntime,
    SubprocessRuntime.kind: SubprocessRuntime,
}


def _is_runtime(loaded: object, name: str) -> TypeIs[type[Runtime]]:
    """Check the class attributes of a runtime. The methods are checked by the type checker of the package."""
    return (
        isinstance(loaded, type)
        and getattr(loaded, 'kind', None) == name
        and isinstance(getattr(loaded, 'invocation', None), Invocation)
    )


@cache
def runtimes() -> dict[str, type[Runtime]]:
    """Return every runtime class by name: the built-in ones, then the ones that packages add (``RUNTIME_GROUP``).

    Raises:
        TypeError: An entry point does not name a runtime class with that ``kind`` and an ``invocation``.
        ValueError: An entry point reuses the name of a built-in runtime.
    """
    found = dict(BUILTIN_RUNTIMES)
    for ep in entry_points(group=RUNTIME_GROUP):
        if ep.name in found:
            raise ValueError(f'entry point {ep.name} = {ep.value} in {RUNTIME_GROUP}: {ep.name!r} is a runtime already')
        loaded = ep.load()
        if not _is_runtime(loaded, ep.name):
            raise TypeError(
                f'entry point {ep.name} = {ep.value} in {RUNTIME_GROUP} is not a piloop runtime class: it needs'
                f' kind = {ep.name!r} and an invocation ({", ".join(Invocation)})'
            )
        found[ep.name] = loaded
    return found


def make_runtime(kind: str, *, project: str, project_path: Path) -> Runtime:
    """Create the runtime called ``kind``.

    Herdr uses the project to name its workspace. The subprocess runtime ignores it.
    """
    return runtimes()[kind](project, project_path)


type RuntimeFactory = Callable[..., Runtime]
"""The signature of ``make_runtime``: ``(kind, *, project, project_path) -> Runtime``. Tests substitute their own."""


def handle_for(worker: Worker) -> Handle:
    """Return the runtime address of a launched worker row. A requested row has none yet."""
    if worker.runtime is None:
        raise ValueError(f'worker#{worker.id} is {worker.state}: not launched yet')
    return Handle(worker.runtime, worker.handle)


def view_of(worker: Worker) -> str | None:
    """Return where you can see a launched agent (``Runtime.view``), or None.

    None also for a row not launched yet, and for a runtime that is no longer installed: this only labels the agent.
    """
    runtime = runtimes().get(worker.runtime) if worker.runtime is not None else None
    return None if runtime is None else runtime.view(handle_for(worker))


def runtime_for(s: Session, worker: Worker, fallback_path: Path, factory: RuntimeFactory = make_runtime) -> Runtime:
    """Return the runtime that owns a launched worker row. Its project names the Herdr workspace."""
    project = s.get(Project, worker.project_id) if worker.project_id else None
    return factory(
        handle_for(worker).runtime,
        project=project.name if project else 'piloop',
        project_path=Path(project.path) if project else fallback_path,
    )
