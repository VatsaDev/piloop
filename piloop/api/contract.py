"""The table of the API's endpoints, which the server mounts and the client calls, and who may call each one.

An ``Endpoint`` names its method, its path, the model of its parameters, the type of its answer and the access that
it needs. The server (``piloop.net.server``) builds its routes and ``/v1/openapi.json`` from ``ENDPOINTS``, and the
client (``piloop.api.client``) its requests: the two cannot drift apart, as with ts-rest's shared contract.
"""

from collections.abc import Mapping
from enum import StrEnum
from typing import NamedTuple

from pydantic import BaseModel, ConfigDict, Field

from piloop.api.views import Batch, Inbox, Overview, Status
from piloop.ids import CommentKind

MAX_POLL_SECONDS = 25
"""The longest that the server holds a long-poll (``Polled.wait``), as the GitHub Actions runner's."""
LIMIT = 100
"""How many events a read returns at most, by default."""
MAX_LIMIT = 1000


class Access(StrEnum):
    """What a caller of the server may do. A token has one (``piloop token create --access``)."""

    READ = 'read'
    """Every read: the views and the feed."""
    ACT = 'act'
    """``read``, and every operation on a campaign."""
    ADMIN = 'admin'
    """``act``, and the home's own settings. The owner on the Unix socket has it."""
    WORKER = 'worker'
    """A worker's calls: the executor's routes and the CAS."""


GRANTS: Mapping[Access, frozenset[Access]] = {
    Access.READ: frozenset({Access.READ}),
    Access.ACT: frozenset({Access.READ, Access.ACT}),
    Access.ADMIN: frozenset({Access.READ, Access.ACT, Access.ADMIN}),
    Access.WORKER: frozenset({Access.WORKER}),
}
"""The access of the routes that a caller of each access may call."""


class Method(StrEnum):
    """An endpoint's HTTP method."""

    GET = 'GET'


class Params(BaseModel):
    """The parameters of an endpoint."""

    model_config = ConfigDict(frozen=True, use_attribute_docstrings=True)


class Polled(Params):
    """The parameters of a long-poll: the server holds it up to ``wait`` seconds for an answer."""

    wait: float = Field(0, ge=0, le=MAX_POLL_SECONDS)
    """Seconds to hold the request while there is nothing to answer yet."""


class Located(Params):
    """A campaign by its name or a path, from where the client runs."""

    campaign: str | None = None
    """The campaign's name, or a path inside its repo."""
    cwd: str | None = None
    """Where the client runs, as an absolute path: a relative path in ``campaign`` is from there."""


class Selection(Located):
    """Which campaigns: the one that ``campaign`` names, else the one ``cwd`` is in, else the only active one, else all."""


class InboxQuery(Located):
    """Which inbox items: those of ``campaign``, or of every campaign."""

    kind: tuple[CommentKind, ...] = ()
    """Only the items of these kinds."""
    acked: bool = False
    """The acknowledged items too."""


class EventsQuery(Polled, Located):
    """Which events: those after a cursor, oldest first, about rows of ``campaign`` or of every campaign."""

    after: int = Field(0, ge=0)
    """The cursor to read after: the ``next`` of the last read, or the ``cursor`` of a snapshot."""
    kind: tuple[str, ...] = ()
    """Only the events of these kinds."""
    limit: int = Field(LIMIT, ge=1, le=MAX_LIMIT)
    """The most events to answer."""


class Endpoint[P: Params, A](NamedTuple):
    """One route of the API: its ``params`` are its query parameters.

    A tuple, so that an endpoint of narrower types is one of ``Endpoint[Params, object]``: ``ENDPOINTS`` holds every one.
    """

    name: str
    """The OpenAPI ``operationId``."""
    method: Method
    path: str
    params: type[P]
    answer: type[A]
    """A model or a ``TypedDict``."""
    access: Access
    description: str
    """What it answers, in one sentence."""


STATUS = Endpoint(
    'status', Method.GET, '/v1/status', Selection, Status, Access.READ, 'The main facts of each campaign, at a cursor.'
)
OVERVIEW = Endpoint(
    'overview',
    Method.GET,
    '/v1/overview',
    Selection,
    Overview,
    Access.READ,
    'The overview of each campaign, as text, and the live keys that config.toml or a limit sets.',
)
INBOX = Endpoint(
    'inbox', Method.GET, '/v1/inbox', InboxQuery, Inbox, Access.READ, 'The inbox items and the seeds, at a cursor.'
)
EVENTS = Endpoint(
    'events',
    Method.GET,
    '/v1/events',
    EventsQuery,
    Batch,
    Access.READ,
    'The events after a cursor. With wait, it answers once an event matches, or at the end of the wait with none.',
)
ENDPOINTS: tuple[Endpoint[Params, object], ...] = (STATUS, OVERVIEW, INBOX, EVENTS)


class Problem(StrEnum):
    """Why the API refuses a request: the ``error`` of its body."""

    UNKNOWN_CAMPAIGN = 'unknown_campaign'
    CURSOR_AHEAD = 'cursor_ahead'
