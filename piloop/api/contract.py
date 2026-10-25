"""What the API's routes and its client share: who may call, the query models, the bounds, and why a route refuses.

The routes are FastAPI's (``piloop.daemon.api``). The client (``piloop.api.client``) sends these query models and
reads the answers of ``piloop.api.views``: one set of models on both sides.
"""

from collections.abc import Mapping
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

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


class Problem(StrEnum):
    """Why the API refuses a request: the ``error`` of its body."""

    UNKNOWN_CAMPAIGN = 'unknown_campaign'
    CURSOR_AHEAD = 'cursor_ahead'
