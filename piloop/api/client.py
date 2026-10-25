"""The API's client: one method per route, which sends the route's query model and reads back its answer model.

The models are the ones that the routes use (``piloop.api.contract``, ``piloop.api.views``), so the client needs no
second set. ``tests/net/test_api.py`` checks each method's method, path and models against the server's routes.
"""

import errno
import socket as sockets
from collections.abc import Generator
from contextlib import contextmanager
from http import HTTPStatus
from pathlib import Path
from typing import Self

import httpx2 as httpx
from pydantic import BaseModel, ValidationError

from piloop.api.contract import MAX_POLL_SECONDS, EventsQuery, InboxQuery, Params, Polled, Selection
from piloop.api.views import Batch, Inbox, Overview, ProblemDetail, Status

TIMEOUT_SECONDS = 30
"""How long a call waits for its answer, beyond a long-poll's own wait."""


class NoDaemon(ConnectionError):
    """No daemon serves the home: the message says how to start one."""


class Refused(Exception):
    """The daemon refused a call: ``error`` says why, as a ``Problem``, and the message what to do."""

    def __init__(self, error: str | None, detail: str) -> None:
        super().__init__(detail)
        self.error = error


class Client:
    """A client of one daemon, over ``http``."""

    def __init__(self, http: httpx.Client) -> None:
        self._http = http

    @classmethod
    @contextmanager
    def on(cls, socket: Path) -> Generator[Self]:
        """A client of the daemon that serves on the Unix socket ``socket``, until the block ends.

        Raises:
            NoDaemon: Nothing listens on the socket.
        """
        if not answers(socket):
            raise NoDaemon(
                f'no daemon listens on {socket}: start it with `piloop serve --detach`, or set PILOOP_HOME;'
                ' a daemon of an older piloop serves no socket: `piloop restart`'
            )
        with httpx.Client(transport=httpx.HTTPTransport(uds=str(socket)), base_url='http://piloop') as http:
            yield cls(http)

    def status(self, query: Selection) -> Status:
        return self._get('/v1/status', query, Status)

    def overview(self, query: Selection) -> Overview:
        return self._get('/v1/overview', query, Overview)

    def inbox(self, query: InboxQuery) -> Inbox:
        return self._get('/v1/inbox', query, Inbox)

    def events(self, query: EventsQuery) -> Batch:
        return self._get('/v1/events', query, Batch)

    def _get[A: BaseModel](self, path: str, query: Params, answer: type[A]) -> A:
        """GET ``path`` with ``query`` as its query string, and read the answer as ``answer``.

        Raises:
            Refused: The daemon refused the call.
        """
        wait = query.wait if isinstance(query, Polled) else 0
        params = query.model_dump(mode='json', exclude_defaults=True)
        response = self._http.get(path, params=params, timeout=TIMEOUT_SECONDS + min(wait, MAX_POLL_SECONDS))
        if response.is_success:
            return answer.model_validate_json(response.content)
        raise _refused(response)


class _Invalid(BaseModel):
    """One parameter that FastAPI refused: where, and why."""

    loc: list[str | int]
    msg: str


class _Invalids(BaseModel):
    """FastAPI's answer to parameters that its models refuse (422)."""

    detail: list[_Invalid]


def _refused(response: httpx.Response) -> Refused:
    """What a refusal says: a route's ``ProblemDetail``, FastAPI's 422, a route that the daemon lacks, or a crash."""
    if response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY:
        invalid = _Invalids.model_validate_json(response.content)
        return Refused(None, '; '.join(f'{i.loc[-1]}: {i.msg}' for i in invalid.detail))
    try:
        problem = ProblemDetail.model_validate_json(response.content)
    except ValidationError:  # not a body of the API's, such as a crash's 500
        return Refused(None, f'the daemon answered {response.status_code}: {response.text}')
    if problem.error is None:  # FastAPI's own 404: no such route
        return Refused(
            None, f'the daemon has no such route ({problem.detail}): it runs an older piloop; `piloop restart`'
        )
    return Refused(problem.error, problem.detail)


def answers(socket: Path) -> bool:
    """Whether a daemon listens on ``socket``: one that a killed daemon left refuses the connection.

    httpcore leaks the socket of a Unix connect that fails (``connect_unix_socket`` of
    https://github.com/encode/httpcore/blob/master/httpcore/_backends/sync.py closes none), so the client tries one
    first.
    """
    with sockets.socket(sockets.AF_UNIX, sockets.SOCK_STREAM) as probe:
        try:
            probe.connect(str(socket))
        except FileNotFoundError, ConnectionRefusedError:
            return False
        except OSError as e:
            if e.errno != errno.ENOTSOCK:  # macOS, for a file that is no socket; Linux refuses the connection
                raise
            return False
    return True
