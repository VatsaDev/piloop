"""The client of the API: one call for every endpoint of the contract, over the home's Unix socket.

It imports neither the database nor the daemon, so a command that only calls the API starts fast.
"""

import socket as stdlib_socket
from pathlib import Path
from types import TracebackType
from typing import Self

import httpx2 as httpx
from pydantic import BaseModel, TypeAdapter, ValidationError

from piloop.api.contract import Endpoint, Params, Polled

TIMEOUT_SECONDS = 30
"""How long a call waits for its answer, beyond a long-poll's own wait."""


def answers(socket: Path) -> bool:
    """Whether a daemon listens on ``socket``: one that a killed daemon left refuses the connection."""
    with stdlib_socket.socket(stdlib_socket.AF_UNIX, stdlib_socket.SOCK_STREAM) as probe:
        try:
            probe.connect(str(socket))
        except FileNotFoundError, ConnectionRefusedError:
            return False
    return True


class NoDaemon(ConnectionError):
    """No daemon serves the home: the message says how to start one."""


class Refused(Exception):
    """The daemon refused a call: ``error`` says why, as the endpoint's ``Problem``, and the message how to fix it."""

    def __init__(self, error: str | None, detail: str) -> None:
        super().__init__(detail)
        self.error = error


class Client:
    """A client of the daemon that serves on ``socket``. Close it, or use it as a context manager."""

    def __init__(self, socket: Path) -> None:
        self.socket = socket
        self._http = httpx.Client(
            transport=httpx.HTTPTransport(uds=str(socket)), base_url='http://piloop', timeout=TIMEOUT_SECONDS
        )

    def call[P: Params, A](self, endpoint: Endpoint[P, A], params: P) -> A:
        """Call ``endpoint`` with ``params``, and return its answer.

        Raises:
            NoDaemon: Nothing listens on the socket.
            Refused: The daemon refused the call.
        """
        wait = params.wait if isinstance(params, Polled) else 0
        # httpcore leaks the socket of a connect that fails: try one first
        if not answers(self.socket):
            raise NoDaemon(
                f'no daemon serves {self.socket.parent}: start it with `piloop serve --detach`, or set PILOOP_HOME;'
                ' a daemon of an older piloop serves no socket: `piloop restart`'
            )
        response = self._http.request(
            endpoint.method,
            endpoint.path,
            params=params.model_dump(mode='json', exclude_defaults=True),
            timeout=TIMEOUT_SECONDS + wait,
        )
        if response.is_success:
            return TypeAdapter(endpoint.answer).validate_json(response.content)
        raise _refused(endpoint, response)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self, kind: type[BaseException] | None, error: BaseException | None, traceback: TracebackType | None
    ) -> None:
        self.close()


class _Invalid(BaseModel):
    """One parameter that FastAPI refused: where, and why."""

    loc: list[str | int]
    msg: str


class _Body(BaseModel):
    """The body of a refusal."""

    error: str | None = None
    detail: str | list[_Invalid] = ''


def _refused(endpoint: Endpoint[Params, object], response: httpx.Response) -> Refused:
    """What a refusal says: the route's ``{error, detail}``, FastAPI's ``{detail}``, or a route the daemon lacks."""
    try:
        body = _Body.model_validate_json(response.content)
    except ValidationError:  # not a refusal of the API's, such as a crash's 500
        body = _Body(detail=f'the daemon answered {response.status_code}: {response.text}')
    if response.status_code == 404 and body.error is None:
        return Refused(None, f'the daemon has no {endpoint.path}: it runs an older piloop; `piloop restart` updates it')
    match body.detail:
        case str(detail):
            return Refused(body.error, detail)
        case invalid:
            return Refused(body.error, '; '.join(f'{i.loc[-1]}: {i.msg}' for i in invalid))
