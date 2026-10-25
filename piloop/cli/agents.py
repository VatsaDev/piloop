"""The commands that act on agents: ``launch``, ``prompt``, ``attach``, ``agent messages``, ``agent cancel`` and ``run cancel``."""

from typing import Annotated

import cyclopts
from sqlalchemy.orm import Session

from piloop.agents import results
from piloop.campaign.stop import CANCELLABLE, request_cancel
from piloop.cli import common, local
from piloop.cli.common import HARNESS_HELP, CampaignRef
from piloop.db.events import has, latest
from piloop.db.models import LIVE, OPEN_RUNS, AgentSession, Event, Run, TargetKind
from piloop.db.session import Database
from piloop.harnesses import MessageFormat, render_messages
from piloop.ids import AgentSessionId, CommentId, Ending, RoleName, RunId, TargetRef
from piloop.plugins.runtime.contract import MESSAGES_ENV, take
from piloop.reports.overview import agent_output

agent_app = cyclopts.App(name='agent')  # their help is where piloop.cli.main registers them
run_app = cyclopts.App(name='run')


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
    with local.session() as s:
        campaign = local.campaign(s, ref)
        roles = campaign.config.roles
        sweeping = roles.sweeping()
        if role not in sweeping:
            agent_sessions = (
                ' The daemon launches implementers by itself, one for each queued run.'
                if role in roles.doing_runs()
                else ''
            )
            raise SystemExit(
                f'{campaign.name} has no sweeping role {role!r}; it has {", ".join(sweeping)}.{agent_sessions}'
            )
        agent_session = AgentSession(kind=role, campaign_id=campaign.id, harness=harness, model=model, reason=reason)
        s.add(agent_session)
        s.commit()
        common.console.print(f'agent_session#{agent_session.id} {role} requested for {campaign.name}: {reason}')


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
    with local.session() as s:
        comment_id = results.queue_prompt(s, agent_session_id, text, common.settings.actor.signature)
        if comment_id is None:
            raise SystemExit(f'agent_session#{agent_session_id} is not running')
        s.commit()
    outcome = _outcome(agent_session_id, comment_id)
    match outcome:
        case None:
            line = f'comment#{comment_id}: the daemon queues it for agent_session#{agent_session_id} once it runs'
        case results.PromptOutcome(kind='comment.queued'):
            line = f'queued comment#{comment_id} for agent_session#{agent_session_id}: it reads it at its next turn'
        case results.PromptOutcome(kind='comment.delivered'):
            line = f'agent_session#{agent_session_id} read comment#{comment_id}'
        case _:
            raise SystemExit(f'agent_session#{agent_session_id} did not get comment#{comment_id}: {outcome.error}')
    common.console.print(line, markup=False)


def _outcome(agent_session_id: AgentSessionId, comment_id: CommentId) -> results.PromptOutcome | None:
    """Wait up to ``DELIVERY_WAIT`` for the comment's outcome on the agent."""
    found: list[results.PromptOutcome] = []
    on_agent = Event.target == TargetRef(TargetKind.AGENT_SESSION, agent_session_id)

    def settled(s: Session) -> bool:
        event = latest(s, results.PromptOutcome, on_agent, has(results.PromptOutcome, comment=comment_id))
        if event is not None:
            found.append(event[1])
        return event is not None

    db = Database(common.settings.database_url, actor=common.settings.actor)
    try:
        db.wait_until(settled, timeout=DELIVERY_WAIT)
    finally:
        db.close()
    return found[0] if found else None


ATTACH_LINES = 40
"""How many lines of the agent's output ``piloop attach`` prints."""


def attach(agent_session_id: Annotated[AgentSessionId, cyclopts.Parameter(help='The id of the agent.')], /) -> None:
    """Print the last lines that an agent wrote, running or ended.

    An agent is a background process, with no terminal to type into: `piloop prompt` sends it a message.
    An agent on a worker elsewhere writes there until it ends: the command names the worker.
    """
    with local.session() as s:
        agent_session = s.get(AgentSession, agent_session_id)
        if agent_session is None:
            raise SystemExit(f'no agent_session#{agent_session_id}')
        if (machine := agent_session.machine) is not None and agent_session.state in LIVE:
            raise SystemExit(
                f'agent_session#{agent_session_id} runs on the worker {machine}: what it writes is there until it ends'
            )
        output, state = agent_output(s, agent_session, ATTACH_LINES), agent_session.state
    if not output:
        raise SystemExit(f'agent_session#{agent_session_id} is {state} and has written nothing yet')
    common.console.print('\n'.join(output), markup=False, highlight=False, soft_wrap=True)


WrapUp = Annotated[
    bool,
    cyclopts.Parameter(
        name='--wrap-up',
        help='Tell the agent to save its work and stop first, and end it 10 minutes later.',
        negative='',
    ),
]


@agent_app.command(name='messages')
def messages(
    form: Annotated[
        MessageFormat,
        cyclopts.Parameter(name='--format', help='How to print them: the format that the hook of the harness reads.'),
    ] = MessageFormat.LINES,
) -> None:
    """Take the messages that wait for this agent's turn, and print them.

    A hook of the harness runs it during a turn, so a message reaches the turn in progress. It prints nothing when no
    message waits.
    """
    if common.settings.messages is None:
        raise SystemExit(f'{MESSAGES_ENV} is not set: the runtime of this agent keeps no message queue')
    if taken := take(common.settings.messages):
        common.console.out(render_messages(taken, form), highlight=False)


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
    with local.session() as s:
        agent_session = s.get(AgentSession, agent_session_id)
        if agent_session is None or agent_session.state not in CANCELLABLE:
            raise SystemExit(f'agent_session#{agent_session_id} is not requested, starting or running')
        request_cancel(s, agent_session, common.settings.actor, ending=Ending.WRAP_UP if wrap_up else Ending.NOW)
        s.commit()
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
    with local.session() as s:
        run = s.get(Run, run_id)
        if run is None or run.status not in OPEN_RUNS:
            raise SystemExit(f'run#{run_id} is not queued or running')
        request_cancel(s, run, common.settings.actor, ending=Ending.WRAP_UP if wrap_up else Ending.NOW)
        s.commit()
    common.console.print(f'cancel requested for run#{run_id}: the daemon acts at once')
