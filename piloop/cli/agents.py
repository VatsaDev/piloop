"""The commands that act on agents: ``launch``, ``prompt`` and ``attach``."""

from typing import Annotated

import cyclopts
from sqlalchemy import select

from piloop.agents.launcher import TRANSCRIPT
from piloop.cli import common
from piloop.cli.common import HARNESS_HELP, ProjectRef
from piloop.config import config_of
from piloop.db.models import Worker, WorkerState
from piloop.runtime import Handle, NotAttachable, NotSteerable, Runtime, RuntimeFailure
from piloop.runtime.factory import handle_for, runtime_for


def launch(
    ref: ProjectRef,
    role: Annotated[
        str,
        cyclopts.Parameter(
            help='The role of the agent: a sweeping role of the project (`[roles.<name>]`). The daemon launches the'
            ' agents of a role that does runs by itself, one for each queued run.'
        ),
    ],
    /,
    *,
    harness: Annotated[str | None, cyclopts.Parameter(help=HARNESS_HELP)] = None,
    model: Annotated[str | None, cyclopts.Parameter(help='The model id, as the harness names it.')] = None,
    reason: Annotated[
        str, cyclopts.Parameter(help="Why you request the agent. The agent's brief includes it.")
    ] = 'requested from the CLI',
) -> None:
    """Ask the daemon for one more agent of a role.

    The daemon launches it on its next tick, within the role's `max_concurrent`.
    """
    with common.session() as s:
        project = common.project(s, ref)
        roles = config_of(project).roles
        sweeping = roles.sweeping()
        if role not in sweeping:
            workers = (
                ' The daemon launches workers by itself, one for each queued run.' if role in roles.doing_runs() else ''
            )
            raise SystemExit(f'{project.name} has no sweeping role {role!r}; it has {", ".join(sweeping)}.{workers}')
        worker = Worker(kind=role, project_id=project.id, harness=harness, model=model, reason=reason)
        s.add(worker)
        s.commit()
        common.console.print(f'worker#{worker.id} {role} requested for {project.name}: {reason}')


def prompt(
    worker_id: Annotated[int, cyclopts.Parameter(help='The id of the agent, as `piloop status` lists it.')],
    text: Annotated[str, cyclopts.Parameter(help='The message.')],
    /,
) -> None:
    """Queue a message for the next turn of a running agent of any role.

    The runtime of the agent defines how the message reaches it. When it cannot, the command says why.
    """
    try:
        runtime, handle, _ = _running(worker_id)
        runtime.prompt(handle, text)
    except (NotSteerable, RuntimeFailure) as e:
        raise SystemExit(str(e)) from e
    common.console.print(f'prompted worker#{worker_id}')


def attach(worker_id: Annotated[int, cyclopts.Parameter(help='The id of the agent.')], /) -> None:
    """Show a running agent in this terminal.

    The runtime of the agent defines how. When it cannot, the command says so and where to look instead.
    """
    try:
        runtime, handle, log_dir = _running(worker_id)
        common.console.print(runtime.attach(handle))
    except NotAttachable as e:
        raise SystemExit(f'worker#{worker_id}: {e}; once it ends, its transcript is {log_dir}/{TRANSCRIPT}') from e
    except RuntimeFailure as e:
        raise SystemExit(str(e)) from e


def _running(worker_id: int) -> tuple[Runtime, Handle, str | None]:
    """Return the runtime, the handle and the log directory of a running agent."""
    with common.session() as s:
        worker = s.scalars(select(Worker).where(Worker.id == worker_id, Worker.state == WorkerState.RUNNING)).first()
        if worker is None:
            raise SystemExit(f'worker#{worker_id} is not running')
        return runtime_for(s, worker, common.settings.home), handle_for(worker), worker.log_dir
