"""The agent's end of the relay (``relay``): the ``piloop`` that sends a call to its worker, on the worker's socket.

An agent on a worker elsewhere runs this module's ``main`` for every ``piloop`` command, so it imports only what the
call needs: neither the database nor the executor's contract.
"""

import os
import sys

import httpx2 as httpx
from pydantic import BaseModel

from piloop.actor import Agent
from piloop.ids import AgentSessionId
from piloop.proc import PILOOP_ARGV
from piloop.settings import settings

RELAY_SECONDS = 120.0
"""The longest that a relayed call runs on the server before it is stopped."""


class Call(BaseModel):
    """A relayed call: the agent's argv after ``piloop``, and its stdin."""

    argv: list[str]
    stdin: str | None = None


class AgentCall(Call):
    """What an agent's ``piloop`` sends its worker: a call, and the agent that makes it (its ``PILOOP_ACTOR``)."""

    agent: AgentSessionId


class Relayed(BaseModel):
    """What a relayed call printed, and how it exited."""

    stdout: str = ''
    stderr: str = ''
    exit_code: int


LOCAL: frozenset[tuple[str, ...]] = frozenset({('agent', 'messages')})
"""The commands that run on the worker itself: they read what is there (the agent's queue of messages)."""


def main() -> None:
    """The ``piloop`` of an agent on a worker elsewhere: relay the call, print what it printed, exit with its code.

    A command of ``LOCAL`` runs here, with this worker's piloop.
    """
    argv = sys.argv[1:]
    if any(tuple(argv[: len(command)]) == command for command in LOCAL):
        os.execv(PILOOP_ARGV[0], [*PILOOP_ARGV, *argv])
    match settings.actor:
        case Agent(session=session):
            agent = session
        case actor:
            raise SystemExit(f'piloop: this piloop relays the calls of an agent, and {actor} is none')
    socket = settings.relay_file
    if not socket.exists():
        raise SystemExit(f'piloop: no worker relays on {socket}, and the call did not run')
    stdin = sys.stdin.read() if '-' in argv and not sys.stdin.isatty() else None
    transport = httpx.HTTPTransport(uds=str(socket))
    try:
        with httpx.Client(transport=transport, timeout=RELAY_SECONDS + 30) as client:
            response = client.post(
                'http://worker/v1/cli',
                content=AgentCall(agent=agent, argv=argv, stdin=stdin).model_dump_json(),
                headers={'content-type': 'application/json'},
            )
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        raise SystemExit(f'piloop: the call did not run: {e.response.text}') from e
    except httpx.HTTPError as e:
        raise SystemExit(f'piloop: the relay of this worker did not answer, and the call did not run: {e}') from e
    done = Relayed.model_validate_json(response.content)
    sys.stdout.write(done.stdout)
    sys.stderr.write(done.stderr)
    raise SystemExit(done.exit_code)


if __name__ == '__main__':
    main()
