"""The commands that act on agents: ``launch``, ``prompt``, ``attach``, ``agent cancel`` and ``run cancel``."""

from typing import Annotated

import cyclopts
from sqlalchemy import select

from piloop.agents.launcher import TRANSCRIPT
from piloop.campaign.stop import CANCELLABLE, request_cancel
from piloop.cli import common
from piloop.cli.common import HARNESS_HELP, CampaignRef
from piloop.config import config_of
from piloop.db.models import OPEN_RUNS, Run, Worker, WorkerState
from piloop.runtime import Handle, NotAttachable, NotSteerable, Runtime, RuntimeFailure
from piloop.runtime.factory import handle_for, runtime_for

agent_app = cyclopts.App(name='agent', help='Act on one agent.')
run_app = cyclopts.App(name='run', help='Act on one run.')


def launch(
    ref: CampaignRef,
    role: Annotated[
        str,
        cyclopts.Parameter(
            help='The role of the agent: a sweeping role of the campaign (`[roles.<name>]`). The daemon launches the'
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
        campaign = common.campaign(s, ref)
        roles = config_of(campaign).roles
        sweeping = roles.sweeping()
        if role not in sweeping:
            workers = (
                ' The daemon launches workers by itself, one for each queued run.' if role in roles.doing_runs() else ''
            )
            raise SystemExit(f'{campaign.name} has no sweeping role {role!r}; it has {", ".join(sweeping)}.{workers}')
        worker = Worker(kind=role, campaign_id=campaign.id, harness=harness, model=model, reason=reason)
        s.add(worker)
        s.commit()
        common.console.print(f'worker#{worker.id} {role} requested for {campaign.name}: {reason}')


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


WrapUp = Annotated[
    bool,
    cyclopts.Parameter(
        name='--wrap-up', help='Tell the agent to commit and stop first, and end it 10 minutes later.', negative=''
    ),
]


@agent_app.command(name='cancel')
def agent_cancel(
    worker_id: Annotated[int, cyclopts.Parameter(help='The id of the agent.')], /, *, wrap_up: WrapUp = False
) -> None:
    """Cancel an agent of any role. The daemon acts on its next tick.

    A requested agent never starts. A running one is ended with every process that it started, at once or, with
    `--wrap-up`, after it was told to commit and stop. For a worker, this cancels its run (`piloop run cancel`).
    """
    with common.session() as s:
        worker = s.get(Worker, worker_id)
        if worker is None or worker.state not in CANCELLABLE:
            raise SystemExit(f'worker#{worker_id} is not requested, starting or running')
        request_cancel(s, worker, common.settings.actor, wrap_up=wrap_up)
        s.commit()
    common.console.print(f'cancel requested for worker#{worker_id}: the daemon acts on its next tick')


@run_app.command(name='cancel')
def run_cancel(
    run_id: Annotated[int, cyclopts.Parameter(help='The id of the run.')], /, *, wrap_up: WrapUp = False
) -> None:
    """Cancel a queued or running run. The daemon acts on its next tick.

    A queued run never starts, and a run whose worker fails to start is not launched again. A running worker is ended
    with every process that it started, at once or, with `--wrap-up`, after it was told to commit and stop. The run
    closes as `cancelled`, unless the worker left its finding. Its commits are scored, and its idea is parked: set it
    `ready` to run it again.
    """
    with common.session() as s:
        run = s.get(Run, run_id)
        if run is None or run.status not in OPEN_RUNS:
            raise SystemExit(f'run#{run_id} is not queued or running')
        request_cancel(s, run, common.settings.actor, wrap_up=wrap_up)
        s.commit()
    common.console.print(f'cancel requested for run#{run_id}: the daemon acts on its next tick')
