"""The ``piloop`` calls of an agent on a worker elsewhere: its worker relays each one to the server, on its lease.

The agent's ``piloop`` (``$PILOOP_BIN``, first on its PATH) is ``door.main``. It sends its argv, and its stdin
for ``piloop sql -``, to its worker's ``Relay``, on a Unix socket, which sends them on with the worker's own credential
(``Remote.cli``, ``POST /v1/executor/cli``). The server runs the call as the agent, with its own piloop and database
(``Runner``), and sends back what it printed. The fence of the agent's lease scopes the call (``Leases.relayed``): a
call after the lease ended is refused.

An agent runs as the worker's user, as an agent of ``piloop serve`` runs as the daemon's. The socket admits only that
user, in a directory of mode 0700. Each call carries the agent's own token (``PILOOP_AGENT_TOKEN``), and the server runs
it only when the token is the lease's agent's: one agent of a worker cannot call as another.

Only the commands of the agent's role run (``RoleConfig.RELAYED``), the ones that its prompt names: a test keeps the
two in step. The call runs
in the campaign's checkout on the server, so a command that finds its campaign from the cwd finds it there.

Retirement: deleted when agents call typed operations over HTTP, if they ever do.
"""

import subprocess
import sys
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from socketserver import ThreadingUnixStreamServer

import httpx2 as httpx
from pydantic import ValidationError

from piloop.actor import Agent
from piloop.db.models import AgentSession, Campaign
from piloop.db.session import Database
from piloop.ids import AgentSessionId
from piloop.net import tokens
from piloop.net.tokens import Caller
from piloop.plugins.executor.contract import AgentJob, RepoId
from piloop.plugins.executor.kit import door as agent_end
from piloop.plugins.executor.kit.client import Dismissed
from piloop.plugins.executor.kit.door import RELAY_SECONDS, AgentCall, Call, Relayed
from piloop.plugins.executor.kit.leases import StaleLease
from piloop.plugins.role.contract import RELAYED, Verb
from piloop.proc import ACTOR_ENV, AGENT_TOKEN_ENV, PILOOP_ARGV, child_env
from piloop.settings import socket_dir

RELAY_ARGV = (sys.executable, '-P', '-m', agent_end.__name__)
"""What an agent's ``piloop`` runs on a worker elsewhere (``jobs.Site.piloop``): ``door.main``."""


class LeaseCall(Call):
    """The body of ``POST /v1/executor/cli``: a relayed call, on a lease of the caller."""

    lease: int


def verb(argv: Sequence[str], relayed: frozenset[Verb]) -> Verb | None:
    """The command of ``relayed`` that ``argv`` runs, or None."""
    return next((v for v in relayed if tuple(argv[: len(v)]) == v), None)


def refused(argv: Sequence[str], relayed: frozenset[Verb]) -> Relayed:
    """The answer to a call of a command that an agent of its role does not run on a worker elsewhere."""
    named = ', '.join(sorted(' '.join(v) for v in relayed))
    command = ' '.join(['piloop', *argv[:2]])
    return Relayed(stderr=f'{command}: an agent on another machine runs only {named}\n', exit_code=2)


@dataclass(frozen=True)
class Runner:
    """The server's end: runs a relayed call as the lease's agent, with this home's piloop, in the campaign's checkout."""

    db: Database
    home: Path
    repos: Callable[[RepoId], Path]

    def __call__(self, job: AgentJob, call: Call) -> Relayed:
        with self.db.session() as s:
            w = s.get_one(AgentSession, job.agent_session)
            role = s.get_one(Campaign, w.campaign_id).config.roles.all().get(w.kind)
        relayed = role.RELAYED if role is not None else RELAYED
        if verb(call.argv, relayed) is None:
            return refused(call.argv, relayed)
        token = call.token.get_secret_value()
        with self.db.session() as s:
            caller = tokens.verify(s, token)
        match caller:
            case Caller(identity=Agent(session=session) as agent) if session == job.agent_session:
                pass
            case _:
                return Relayed(
                    stderr=f'piloop: the token is not the one of agent_session#{job.agent_session}\n', exit_code=1
                )
        env = child_env() | {'PILOOP_HOME': str(self.home), ACTOR_ENV: agent.signature, AGENT_TOKEN_ENV: token}
        try:
            done = subprocess.run(
                [*PILOOP_ARGV, *call.argv],
                cwd=self.repos(job.artifact.repo),
                env=env,
                input=call.stdin,
                stdin=None if call.stdin is not None else subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=RELAY_SECONDS,
            )
        except subprocess.TimeoutExpired:
            return Relayed(
                stderr=f'piloop: the call ran for {RELAY_SECONDS:g} s on the server and was stopped\n', exit_code=124
            )
        return Relayed(stdout=done.stdout, stderr=done.stderr, exit_code=done.returncode)


type Run = Callable[[AgentJob, Call], Relayed]
"""Run an agent's relayed call on the server (``Runner``)."""
type Send = Callable[[int, Call], Relayed]
"""Send a relayed call to the server on a lease (``Remote.cli``). Raises ``StaleLease`` once the lease ended."""


class Relay:
    """A worker's end: a Unix socket that takes its agents' calls, and the lease of each agent whose job runs.

    The standard library's server, not the server of ``piloop serve``: it serves one route to this user's processes,
    and the worker imports no web framework for it.
    """

    def __init__(self, path: Path, send: Send) -> None:
        self.path = path
        """The socket: ``Settings.relay_file``."""
        self._send = send
        self._lock = threading.Lock()
        self._leases: dict[AgentSessionId, int] = {}
        """The lease of each agent whose job runs on this worker."""
        self._server: ThreadingUnixStreamServer | None = None

    def start(self) -> None:
        """Serve on the socket, in place of one that an earlier process of this worker left."""
        socket_dir(self.path)
        self.path.unlink(missing_ok=True)
        self._server = ThreadingUnixStreamServer(str(self.path), _handler(self))
        threading.Thread(target=self._server.serve_forever, name='relay', daemon=True).start()

    def open(self, leases: Mapping[AgentSessionId, int]) -> None:
        """Relay the calls of each agent, on its lease: its job runs on this worker."""
        with self._lock:
            self._leases |= leases

    def close(self, lease: int) -> None:
        """Relay nothing more on ``lease``: its job ended."""
        with self._lock:
            self._leases = {agent: held for agent, held in self._leases.items() if held != lease}

    def stop(self) -> None:
        """Stop serving."""
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self.path.unlink(missing_ok=True)

    def answer(self, body: bytes) -> tuple[HTTPStatus, Relayed | str]:
        """What a call gets: what the server's run printed, or why there is none."""
        try:
            call = AgentCall.model_validate_json(body)
        except ValidationError as e:
            return HTTPStatus.UNPROCESSABLE_ENTITY, str(e)
        with self._lock:
            lease = self._leases.get(call.agent)
        if lease is None:
            return HTTPStatus.NOT_FOUND, f'agent_session#{call.agent} has no job on this worker: it ended'
        try:
            return HTTPStatus.OK, self._send(lease, Call(argv=call.argv, stdin=call.stdin, token=call.token))
        except StaleLease as e:
            return HTTPStatus.OK, Relayed(stderr=f'piloop: the lease of this agent ended: {e}\n', exit_code=1)
        except (httpx.ConnectError, Dismissed) as e:  # it never reached the command: running it again is safe
            return HTTPStatus.OK, Relayed(stderr=f'piloop: the call did not run on the server: {e}\n', exit_code=1)
        except (
            Exception
        ) as e:  # the answer was lost, so the command may have run: the agent must look before it retries
            stderr = f'piloop: the answer of the server was lost, and the call may have run there: {e}\n'
            return HTTPStatus.OK, Relayed(stderr=stderr, exit_code=1)


def _handler(relay: Relay) -> type[BaseHTTPRequestHandler]:
    """The request handler of ``relay``'s server: ``POST /v1/cli``."""

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            body = self.rfile.read(int(self.headers.get('content-length', 0)))
            status, answer = relay.answer(body) if self.path == '/v1/cli' else (HTTPStatus.NOT_FOUND, 'no route')
            payload = (answer.model_dump_json() if isinstance(answer, Relayed) else answer).encode()
            self.send_response(status)
            self.send_header('content-type', 'application/json' if isinstance(answer, Relayed) else 'text/plain')
            self.send_header('content-length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:
            """Log nothing: the worker's spans say what failed."""

    return Handler
