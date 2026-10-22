"""The server of ``piloop serve --listen``: the routes that other machines call, and who may call each one.

The network capability (``piloop.plugins.network.contract``) says how machines reach the server and who a caller is.
What a caller may do is core's: each route has a ``TokenScope``, and a caller of another scope gets 403. The server is
FastAPI on uvicorn (``piloop.net.server``), and it serves the routes of the executor (``Executor.routes``). Data never
goes through it: workers read and write the artifact store's remote directly, and the routes pass only hashes.
"""

from collections.abc import Callable

from pydantic import BaseModel, ValidationError

from piloop.db.models import TokenScope
from piloop.net.tokens import Caller


class Route:
    """``POST /v1/executor/<path>`` of an executor: a JSON ``body`` in, ``handle``'s answer out, or 204 for None.

    ``handle`` runs in a thread of the server, so it may block, as a long-poll does.
    """

    def __init__[I: BaseModel](
        self, path: str, scope: TokenScope, body: type[I], handle: Callable[[Caller, I], BaseModel | None]
    ) -> None:
        self.path = path
        self.scope = scope

        def call(caller: Caller, raw: bytes) -> BaseModel | None:
            try:
                parsed = body.model_validate_json(raw)
            except ValidationError as e:
                raise InvalidBody(e) from e
            return handle(caller, parsed)

        self.call: Callable[[Caller, bytes], BaseModel | None] = call
        """Validate a request's body, then handle it. Raises ``InvalidBody``."""


class InvalidBody(ValueError):
    """A request's body is not the route's model: the server answers 422 with ``error``'s details."""

    def __init__(self, error: ValidationError) -> None:
        super().__init__(str(error))
        self.error = error
