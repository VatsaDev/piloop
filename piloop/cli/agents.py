"""The commands that act on agents: ``launch``, ``prompt`` and ``attach``."""

import os
import sys
from typing import Annotated

import cyclopts
from sqlalchemy import select

from piloop.agents.scout import SCOUT_EVENT
from piloop.cli import common
from piloop.cli.common import HARNESS_HELP, ProjectRef
from piloop.config import config_of
from piloop.db.events import emit
from piloop.db.models import TargetKind, Worker, WorkerKind, WorkerState
from piloop.runtime import NotSteerable
from piloop.runtime.factory import handle_for, runtime_for
from piloop.runtime.herdr import HERDR, HerdrError, HerdrRuntime, focus_tab, session_socket
from piloop.runtime.subprocess import STDOUT


def launch(
    ref: ProjectRef,
    role: Annotated[
        str,
        cyclopts.Parameter(
            help='The role of the agent: watchdog, research or a role that piloop.toml defines. The daemon launches'
            ' workers by itself, one for each queued run.'
        ),
    ],
    /,
    *,
    harness: Annotated[str | None, cyclopts.Parameter(help=HARNESS_HELP)] = None,
    model: Annotated[str | None, cyclopts.Parameter(help='The model id, as the harness names it.')] = None,
    reason: Annotated[
        str, cyclopts.Parameter(help="Why you request the agent. The agent's brief includes it.")
    ] = 'requested from the CLI',
    scout: Annotated[
        bool, cyclopts.Parameter(help='Research only: run a scout sweep that looks for outside techniques.')
    ] = False,
) -> None:
    """Ask the daemon for one more agent of a role.

    The daemon launches it on its next tick, within the role's `max_concurrent`.
    """
    if scout and role != WorkerKind.RESEARCH:
        raise SystemExit('--scout is a mode of the research role: `piloop launch <project> research --scout`')
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
        s.flush()
        if scout:
            emit(s, SCOUT_EVENT, TargetKind.WORKER, worker.id, actor=common.settings.actor)
        s.commit()
        common.console.print(
            f'worker#{worker.id} {role}{" (scout)" if scout else ""} requested for {project.name}: {reason}'
        )


def prompt(
    worker_id: Annotated[int, cyclopts.Parameter(help='The id of the agent, as `piloop status` lists it.')],
    text: Annotated[str, cyclopts.Parameter(help='The message.')],
    /,
) -> None:
    """Send a message to a running agent of any role (Herdr runtime only)."""
    with common.session() as s:
        worker = s.scalars(select(Worker).where(Worker.id == worker_id, Worker.state == WorkerState.RUNNING)).first()
        if worker is None:
            raise SystemExit(f'worker#{worker_id} is not running')
        handle = handle_for(worker)
        runtime = runtime_for(s, worker, common.settings.home)
    try:
        runtime.prompt(handle, text)
    except NotSteerable as e:
        raise SystemExit(str(e)) from e
    common.console.print(f'prompted worker#{worker_id}')


def attach(worker_id: Annotated[int, cyclopts.Parameter(help='The id of the agent.')], /) -> None:
    """Focus the tab of a running agent in Herdr, and attach this terminal to the piloop Herdr session."""
    with common.session() as s:
        worker = s.scalars(select(Worker).where(Worker.id == worker_id, Worker.state == WorkerState.RUNNING)).first()
        if worker is None:
            raise SystemExit(f'worker#{worker_id} is not running')
        if worker.runtime != HerdrRuntime.kind:
            common.console.print(
                f'worker#{worker.id} is headless ({worker.runtime} runtime): {worker.log_dir}/{STDOUT} while it runs,'
                f' transcript.txt once it ends'
            )
            return
        tab_id = str(handle_for(worker).data['tab_id'])
    try:
        focus_tab(tab_id)
        _attach(tab_id)
    except HerdrError as e:
        raise SystemExit(str(e)) from e


def _attach(tab_id: str) -> None:
    """Replace this process with the Herdr client of the piloop session, unless this terminal is in it already."""
    if not sys.stdout.isatty():
        common.console.print(
            f'not a terminal; would attach to Herdr session {common.settings.herdr_session} with tab {tab_id} focused'
        )
        return
    if common.settings.herdr_socket_path == session_socket():
        common.console.print(f'focused tab {tab_id}')
        return
    os.execvp(HERDR, [HERDR, '--session', common.settings.herdr_session])
