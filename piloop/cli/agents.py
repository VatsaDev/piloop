"""The commands that act on agents, clients of the API: ``launch``, ``prompt``, ``agent cancel`` and ``run cancel``."""

from typing import Annotated, assert_never

import cyclopts

from piloop.api.contract import CancelBody, LaunchBody, PromptBody
from piloop.api.views import Delivered, Pending, Queued, Undelivered
from piloop.cli import common
from piloop.cli.client import api, campaign_name
from piloop.cli.common import HARNESS_HELP, CampaignRef
from piloop.ids import AgentSessionId, Ending, RoleName, RunId

agent_app = cyclopts.App(name='agent', help='Act on one agent.')
run_app = cyclopts.App(name='run', help='Act on one run.')
agent_app.command('piloop.cli.agent_io:messages', name='messages')


def launch(
    ref: CampaignRef,
    role: Annotated[
        RoleName,
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

    The daemon launches it at once, within the role's `max_concurrent`.
    """
    with api() as client:
        name = campaign_name(client, ref)
        agent = client.launch(name, LaunchBody(role=role, harness=harness, model=model, reason=reason))
    common.console.print(f'agent_session#{agent.agent} {agent.role} requested for {name}: {reason}')


DELIVERY_WAIT = 20.0
"""How long ``piloop prompt`` waits for the outcome: two heartbeats of the ``workers`` executor."""


def prompt(
    agent_session_id: Annotated[
        AgentSessionId, cyclopts.Parameter(help='The id of the agent, as `piloop status` lists it.')
    ],
    text: Annotated[str, cyclopts.Parameter(help='The message.')],
    /,
) -> None:
    """Queue a message for the next turn of a running agent of any role, and say whether it is queued.

    The message is a note on the agent. The daemon queues it through the agent's executor and its runtime. When it
    cannot, the command says why. After 20 seconds without an outcome, the daemon still queues it once the agent runs.
    """
    with api() as client:
        prompted = client.prompt(agent_session_id, PromptBody(text=text, wait=DELIVERY_WAIT))
    comment = prompted.comment
    match prompted.delivery:
        case Pending():
            line = f'comment#{comment}: the daemon queues it for agent_session#{agent_session_id} once it runs'
        case Queued():
            line = f'queued comment#{comment} for agent_session#{agent_session_id}: it reads it at its next turn'
        case Delivered():
            line = f'agent_session#{agent_session_id} read comment#{comment}'
        case Undelivered(error=error):
            raise SystemExit(f'agent_session#{agent_session_id} did not get comment#{comment}: {error}')
        case _:
            assert_never(prompted.delivery)
    common.console.print(line, markup=False)


WrapUp = Annotated[
    bool,
    cyclopts.Parameter(
        name='--wrap-up',
        help='Tell the agent to save its work and stop first, and end it 10 minutes later.',
        negative='',
    ),
]


@agent_app.command(name='cancel')
def agent_cancel(
    agent_session_id: Annotated[AgentSessionId, cyclopts.Parameter(help='The id of the agent.')],
    /,
    *,
    wrap_up: WrapUp = False,
) -> None:
    """Cancel an agent of any role. The daemon acts at once.

    A requested agent never starts. A running one is ended with every process that it started, at once or, with
    `--wrap-up`, after it was told to save its work and stop. For an implementer, this cancels its run (`piloop run cancel`).
    """
    with api() as client:
        client.cancel_agent(agent_session_id, _cancel(wrap_up))
    common.console.print(f'cancel requested for agent_session#{agent_session_id}: the daemon acts at once')


@run_app.command(name='cancel')
def run_cancel(
    run_id: Annotated[RunId, cyclopts.Parameter(help='The id of the run.')], /, *, wrap_up: WrapUp = False
) -> None:
    """Cancel a queued or running run. The daemon acts at once.

    A queued run never starts, and a run whose implementer fails to start is not launched again. A running implementer is ended
    with every process that it started, at once or, with `--wrap-up`, after it was told to save its work and stop. The run
    closes as `cancelled`, unless the implementer left its finding before the daemon ended it: a later finding is
    refused. Its commits are scored, and its idea is parked: set it `ready` to run it again.
    """
    with api() as client:
        client.cancel_run(run_id, _cancel(wrap_up))
    common.console.print(f'cancel requested for run#{run_id}: the daemon acts at once')


def _cancel(wrap_up: bool) -> CancelBody:
    return CancelBody(ending=Ending.WRAP_UP if wrap_up else Ending.NOW)
