"""The server of ``piloop serve --listen``: the routes that other machines call, and who may call each one.

The network capability (``piloop.plugins.network.contract``) says how machines reach the server and who a caller is.
What a caller may do is core's: each route has a ``TokenScope``, and a caller of another scope gets 403. The server is
FastAPI on uvicorn (``piloop.net.server``), and it serves the routes of the executor (``Executor.routes``). Data never
goes through it: workers read and write the artifact store's remote directly, and the routes pass only hashes.
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from functools import partial

from pydantic import BaseModel, Field, ValidationError

from piloop.db.models import TokenScope
from piloop.net.tokens import Caller

MAX_POLL_SECONDS = 25
"""The longest that the server holds a long-poll (``Polled.wait``), as the GitHub Actions runner's."""

type Changed = Callable[[int, float], Awaitable[int]]
"""``Database.changed``: the commit count once it passes the one seen, or at the timeout."""


class Polled(BaseModel):
    """The body of a long-poll: the server holds it up to ``wait`` seconds for an answer."""

    wait: float = Field(ge=0, le=MAX_POLL_SECONDS)


@dataclass(frozen=True)
class Call:
    """A request whose body is valid: its handler, bound to the caller and the body, and how long it may wait."""

    handle: Callable[[], BaseModel | None]
    wait: float


class Route:
    """``POST /v1/executor/<path>`` of an executor: a JSON ``body`` in, ``handle``'s answer out, or 204 for None.

    ``handle`` runs in a thread of the server's pool, so it must not wait: a long-poll is a ``long_poll``.
    """

    def __init__[I: BaseModel](
        self, path: str, scope: TokenScope, body: type[I], handle: Callable[[Caller, I], BaseModel | None]
    ) -> None:
        self.path = path
        self.scope = scope

        def bind(caller: Caller, raw: bytes) -> Call:
            return Call(partial(handle, caller, _parsed(body, raw)), 0)

        self.bind: Callable[[Caller, bytes], Call] = bind
        """Validate a request's body, and bind the handler to it. Raises ``InvalidBody``."""
        self.changed: Changed | None = None
        """What the server awaits between the calls of a long-poll's handler. None for any other route."""

    @classmethod
    def long_poll[I: Polled](
        cls,
        path: str,
        scope: TokenScope,
        body: type[I],
        handle: Callable[[Caller, I], BaseModel | None],
        changed: Changed,
    ) -> Route:
        """A long-poll, as the GitHub Actions runner's ``GET message``: ``handle`` answers None for "nothing yet".

        The server then awaits ``changed`` and calls it again, until the body's ``wait`` ends. No thread waits.
        """
        route = cls(path, scope, body, handle)

        def bind(caller: Caller, raw: bytes) -> Call:
            parsed = _parsed(body, raw)
            return Call(partial(handle, caller, parsed), parsed.wait)

        route.bind, route.changed = bind, changed
        return route


def _parsed[I: BaseModel](body: type[I], raw: bytes) -> I:
    try:
        return body.model_validate_json(raw)
    except ValidationError as e:
        raise InvalidBody(e) from e


class InvalidBody(ValueError):
    """A request's body is not the route's model: the server answers 422 with ``error``'s details."""

    def __init__(self, error: ValidationError) -> None:
        super().__init__(str(error))
        self.error = error
