"""The server of ``piloop serve``: the routes that clients and workers call, and who may call each one.

The network capability (``piloop.plugins.network.contract``) says how other machines reach the server and who a caller
is. What a caller may do is core's: the access of its credential (``piloop.api.contract.Access``) must grant the
route's. The server is FastAPI on uvicorn (``piloop.net.server``). It serves the API (``Served`` endpoints of
``piloop.api.contract``) and the routes of the executor (``Executor.routes``). Data never goes through it: workers read
and write the artifact store's remote directly, and the routes pass only hashes.
"""

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from functools import partial
from http import HTTPStatus

from pydantic import BaseModel, TypeAdapter, ValidationError

from piloop.api.contract import Endpoint, Params, Polled
from piloop.net.tokens import Caller

type Changed = Callable[[int, float], Awaitable[int]]
"""``Database.changed``: the count of wakes once it passes the one seen, or at the timeout."""


@dataclass(frozen=True)
class Pending[P: Params, A]:
    """A long-poll's answer while there is nothing new: the server awaits a change, then calls again with ``again``.

    At the end of the wait it sends ``answer``, so a cursor still moves over what the filters left out.
    """

    answer: A
    again: P


@dataclass(frozen=True)
class Step:
    """One call of an endpoint's handler: the answer, as JSON, and the next call of a long-poll that has nothing new."""

    answer: bytes
    again: Callable[[], Step] | None


class Served:
    """An endpoint of the API, and the function that answers it.

    ``handle`` runs in a thread of the server's pool, so it must not wait. A long-poll answers ``Pending`` until there
    is something new, and ``changed`` is what the server awaits in between, outside the pool.
    """

    def __init__[P: Params, A](
        self, endpoint: Endpoint[P, A], handle: Callable[[Caller, P], A | Pending[P, A]], changed: Changed | None = None
    ) -> None:
        self.endpoint: Endpoint[Params, object] = endpoint
        self.changed = changed
        answers = TypeAdapter(endpoint.answer)

        def step(caller: Caller, params: P) -> Step:
            match handle(caller, params):
                case Pending(answer=answer, again=again):
                    return Step(answers.dump_json(answer), partial(step, caller, again))
                case answer:
                    return Step(answers.dump_json(answer), None)

        def bind(caller: Caller, query: Mapping[str, object]) -> Call[Step]:
            params = endpoint.params.model_validate(query)
            return Call(partial(step, caller, params), params.wait if isinstance(params, Polled) else 0)

        self.bind: Callable[[Caller, Mapping[str, object]], Call[Step]] = bind
        """Bind the handler to a request's parameters, which the server has validated."""


@dataclass(frozen=True)
class Call[T]:
    """A request whose body is valid: its handler, bound to the caller and the body, and how long it may wait."""

    handle: Callable[[], T]
    wait: float


class Route:
    """``POST /v1/executor/<path>`` of an executor: a JSON ``body`` in, ``handle``'s answer out, or 204 for None.

    ``handle`` runs in a thread of the server's pool, so it must not wait: a long-poll is a ``long_poll``.
    """

    def __init__[I: BaseModel](self, path: str, body: type[I], handle: Callable[[Caller, I], BaseModel | None]) -> None:
        self.path = path

        def bind(caller: Caller, raw: bytes) -> Call[BaseModel | None]:
            return Call(partial(handle, caller, _parsed(body, raw)), 0)

        self.bind: Callable[[Caller, bytes], Call[BaseModel | None]] = bind
        """Validate a request's body, and bind the handler to it. Raises ``InvalidBody``."""
        self.changed: Changed | None = None
        """What the server awaits between the calls of a long-poll's handler. None for any other route."""

    @classmethod
    def long_poll[I: Polled](
        cls,
        path: str,
        body: type[I],
        handle: Callable[[Caller, I], BaseModel | None],
        changed: Changed,
    ) -> Route:
        """A long-poll, as the GitHub Actions runner's ``GET message``: ``handle`` answers None for "nothing yet".

        The server then awaits ``changed`` and calls it again, until the body's ``wait`` ends. No thread waits.
        """
        route = cls(path, body, handle)

        def bind(caller: Caller, raw: bytes) -> Call[BaseModel | None]:
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


class Refusal(Exception):
    """A route refuses a request: the server answers ``status`` with ``{"error": error, "detail": detail}``."""

    def __init__(self, status: HTTPStatus, error: StrEnum, detail: str) -> None:
        super().__init__(detail)
        self.status = status
        self.error = error
