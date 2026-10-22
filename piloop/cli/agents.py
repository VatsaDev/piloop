"""The commands that act on agents: ``launch``, ``prompt``, ``attach``, ``agent cancel`` and ``run cancel``."""

import time
from typing import Annotated

import cyclopts
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.agents import results
from piloop.campaign.stop import CANCELLABLE, request_cancel
from piloop.cli import common
from piloop.cli.common import HARNESS_HELP, CampaignRef
from piloop.config import TRANSCRIPT, config_of
from piloop.db.models import OPEN_RUNS, AgentSession, AgentSessionState, Event, Run, TargetKind
from piloop.db.session import Database
from piloop.ids import AgentSessionId, CommentId, RoleName, RunId
from piloop.plugins.runtime.contract import Handle, NotAttachable, Runtime, RuntimeFailure
from piloop.runtime.factory import handle_for, runtime_for

agent_app = cyclopts.App(name='agent', help='Act on one agent.')
run_app = cyclopts.App(name='run', help='Act on one run.')


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

    The daemon launches it on its next tick, within the role's `max_concurrent`.
    """
    with common.session() as s:
        campaign = common.campaign(s, ref)
        roles = config_of(campaign).roles
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
    """Queue a message for the next turn of a running agent of any role, and say whether it arrived.

    The message is a note on the agent. The daemon delivers it through the agent's executor and its runtime. When it
    cannot, the command says why. After 20 seconds without an outcome, the note stays queued.
    """
    with common.session() as s:
        comment_id = results.queue_prompt(s, agent_session_id, text, common.settings.actor.signature)
        if comment_id is None:
            raise SystemExit(f'agent_session#{agent_session_id} is not running')
        s.commit()
    outcome = _outcome(agent_session_id, comment_id)
    match outcome:
        case None:
            common.console.print(
                f'comment#{comment_id}: queued for agent_session#{agent_session_id}; the daemon delivers it once it runs',
                markup=False,
            )
        case results.DELIVERED, _:
            common.console.print(f'prompted agent_session#{agent_session_id} (comment#{comment_id})', markup=False)
        case _:
            raise SystemExit(f'agent_session#{agent_session_id} did not get comment#{comment_id}: {outcome[1]}')


def _outcome(agent_session_id: AgentSessionId, comment_id: CommentId) -> tuple[str, object] | None:
    """Wait up to ``DELIVERY_WAIT`` for the comment's outcome on the agent: its event kind and its error."""
    found: list[tuple[str, object]] = []

    def settled(s: Session) -> bool:
        event = s.scalars(
            select(Event).where(
                Event.target_kind == TargetKind.AGENT_SESSION,
                Event.target_id == agent_session_id,
                Event.kind.in_(results.SETTLED),
                Event.payload['comment'].as_integer() == comment_id,
            )
        ).first()
        if event is not None:
            found.append((event.kind, event.payload.get('error')))
        return event is not None

    db = Database(common.settings.database_url, actor=common.settings.actor)
    deadline = time.monotonic() + DELIVERY_WAIT
    try:
        db.wait_until(settled, give_up=lambda: time.monotonic() > deadline)
    finally:
        db.close()
    return found[0] if found else None


def attach(agent_session_id: Annotated[AgentSessionId, cyclopts.Parameter(help='The id of the agent.')], /) -> None:
    """Show a running agent in this terminal.

    The runtime of the agent defines how. When it cannot, the command says so and where to look instead.
    """
    try:
        runtime, handle, log_dir = _running(agent_session_id)
        common.console.print(runtime.attach(handle))
    except NotAttachable as e:
        raise SystemExit(
            f'agent_session#{agent_session_id}: {e}; once it ends, its transcript is {log_dir}/{TRANSCRIPT}'
        ) from e
    except RuntimeFailure as e:
        raise SystemExit(str(e)) from e


def _running(agent_session_id: AgentSessionId) -> tuple[Runtime, Handle, str | None]:
    """Return the runtime, the handle and the log directory of a running agent."""
    with common.session() as s:
        agent_session = s.scalars(
            select(AgentSession).where(
                AgentSession.id == agent_session_id, AgentSession.state == AgentSessionState.RUNNING
            )
        ).first()
        if agent_session is None:
            raise SystemExit(f'agent_session#{agent_session_id} is not running')
        return runtime_for(s, agent_session, common.settings.home), handle_for(agent_session), agent_session.log_dir


WrapUp = Annotated[
    bool,
    cyclopts.Parameter(
        name='--wrap-up', help='Tell the agent to commit and stop first, and end it 10 minutes later.', negative=''
    ),
]


@agent_app.command(name='cancel')
def agent_cancel(
    agent_session_id: Annotated[AgentSessionId, cyclopts.Parameter(help='The id of the agent.')],
    /,
    *,
    wrap_up: WrapUp = False,
) -> None:
    """Cancel an agent of any role. The daemon acts on its next tick.

    A requested agent never starts. A running one is ended with every process that it started, at once or, with
    `--wrap-up`, after it was told to commit and stop. For an implementer, this cancels its run (`piloop run cancel`).
    """
    with common.session() as s:
        agent_session = s.get(AgentSession, agent_session_id)
        if agent_session is None or agent_session.state not in CANCELLABLE:
            raise SystemExit(f'agent_session#{agent_session_id} is not requested, starting or running')
        request_cancel(s, agent_session, common.settings.actor, wrap_up=wrap_up)
        s.commit()
    common.console.print(f'cancel requested for agent_session#{agent_session_id}: the daemon acts on its next tick')


@run_app.command(name='cancel')
def run_cancel(
    run_id: Annotated[RunId, cyclopts.Parameter(help='The id of the run.')], /, *, wrap_up: WrapUp = False
) -> None:
    """Cancel a queued or running run. The daemon acts on its next tick.

    A queued run never starts, and a run whose implementer fails to start is not launched again. A running implementer is ended
    with every process that it started, at once or, with `--wrap-up`, after it was told to commit and stop. The run
    closes as `cancelled`, unless the implementer left its finding before the daemon ended it: a later finding is
    refused. Its commits are scored, and its idea is parked: set it `ready` to run it again.
    """
    with common.session() as s:
        run = s.get(Run, run_id)
        if run is None or run.status not in OPEN_RUNS:
            raise SystemExit(f'run#{run_id} is not queued or running')
        request_cancel(s, run, common.settings.actor, wrap_up=wrap_up)
        s.commit()
    common.console.print(f'cancel requested for run#{run_id}: the daemon acts on its next tick')
