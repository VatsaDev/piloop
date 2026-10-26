"""The commands that read an agent's own files and rows on this machine: ``attach`` and ``agent messages``."""

from typing import Annotated

import cyclopts

from piloop import deprecated
from piloop.cli import common, local
from piloop.db.models import AgentSession
from piloop.ids import AgentSessionId
from piloop.plugins.harness.contract import HARNESSES, Harness
from piloop.plugins.runtime.contract import MESSAGES_ENV, take
from piloop.reports.overview import agent_output

ATTACH_LINES = 40
"""How many lines of the agent's output ``piloop attach`` prints."""


def attach(agent_session_id: Annotated[AgentSessionId, cyclopts.Parameter(help='The id of the agent.')], /) -> None:
    """Print the last lines that an agent wrote, running or ended.

    An agent is a background process, with no terminal to type into: `piloop prompt` sends it a message.
    A running agent's lines come from the latest snapshot of its session, on any machine: a few seconds old, 30 at most.
    """
    with local.session() as s:
        agent_session = s.get(AgentSession, agent_session_id)
        if agent_session is None:
            raise SystemExit(f'no agent_session#{agent_session_id}')
        output, state = agent_output(s, agent_session, ATTACH_LINES, local.cas()), agent_session.state
    if not output:
        raise SystemExit(f'agent_session#{agent_session_id} is {state} and has written nothing yet')
    common.console.print('\n'.join(output), markup=False, highlight=False, soft_wrap=True)


def messages(
    harness: Annotated[
        str | None,
        cyclopts.Parameter(name='--harness', help='The harness whose hook reads them: one message a line without it.'),
    ] = None,
    form: Annotated[deprecated.MessageFormat | None, cyclopts.Parameter(name='--format', show=False)] = None,
) -> None:
    """Take the messages that wait for this agent's turn, and print them.

    A hook of the harness runs it during a turn, so a message reaches the turn in progress. It prints nothing when no
    message waits.
    """
    if common.settings.messages is None:
        raise SystemExit(f'{MESSAGES_ENV} is not set: the runtime of this agent keeps no message queue')
    name = harness or (deprecated.message_format(form) if form is not None else None)
    model = HARNESSES.model_of(name) if name is not None else Harness  # before the take: a wrong name loses nothing
    if taken := take(common.settings.messages):
        common.console.out(model.messages(taken), highlight=False)
