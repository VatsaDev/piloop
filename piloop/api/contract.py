"""What the API's routes and its client share: who may call, the models of a call, the bounds, and why a route refuses.

The routes are FastAPI's (``piloop.daemon.api``). The client (``piloop.api.client``) sends these query models and
reads the answers of ``piloop.api.views``: one set of models on both sides.
"""

from collections.abc import Mapping, Sequence
from enum import StrEnum
from typing import Annotated, Protocol

from pydantic import BaseModel, ConfigDict, Field

from piloop.ids import CampaignStatus, CandidateId, CommentId, CommentKind, Ending, IdeaId, TrackName

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
    """Which campaigns: the one that ``campaign`` names, else the one that ``cwd`` is in, else every campaign."""


class Stated(Protocol):
    """A campaign with its status: a row, or a ``CampaignState``."""

    @property
    def status(self) -> CampaignStatus:
        """Whether the daemon still requests work for the campaign."""
        ...


def defaulted[C: Stated](campaigns: Sequence[C]) -> list[C]:
    """What a command shows of a selection that names no campaign: the only active campaign, else every campaign."""
    active = [c for c in campaigns if c.status is CampaignStatus.ACTIVE]
    return active if len(active) == 1 else list(campaigns)


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


class NewsQuery(Polled, Located):
    """What needs the orchestrator: the news after a cursor, of ``campaign`` or of every campaign."""

    after: int | None = Field(None, ge=0)
    """The cursor to read after: the ``next`` of the last wait. Default: now."""


class Problem(StrEnum):
    """Why the API refuses a request: the ``error`` of its body."""

    UNKNOWN_CAMPAIGN = 'unknown_campaign'
    UNKNOWN_COMMENT = 'unknown_comment'
    CURSOR_AHEAD = 'cursor_ahead'
    REFUSED = 'refused'
    """An operation refused its input and changed nothing. The detail says why."""


type Priority = Annotated[int, Field(ge=0, le=4)]
"""An inbox item's or an idea's priority: from 0 (urgent) to 4 (someday)."""


class PauseBody(Params):
    """How to pause a campaign."""

    reason: str
    """Why, with ids: what the pause waits for."""
    evals: bool = False
    """Hold the measurements that the daemon starts too: after a new evaluator, or asked."""


class StopBody(Params):
    """How to stop a campaign."""

    reason: str = ''
    """Why, for ``stopped_reason`` and the closing review."""
    ending: Ending = Ending.WRAP_UP
    """How the running implementers end. ``now`` also ends those of a stopped campaign that still wind down."""


class IdeaBody(Params):
    """A defined idea, which the daemon schedules in priority order once it is ready."""

    title: str
    hypothesis: str = ''
    """Why this should help, and by how much. Default: the title."""
    plan: str | None = None
    """The steps for the implementer."""
    provenance: str | None = None
    """What prompted the idea, with ids or a URL."""
    family: str | None = None
    """The mechanism family. Ideas that try the same mechanism share it."""
    priority: Priority = 2
    harness: str | None = None
    """The harness to use instead of the rotation."""
    base: CandidateId | None = None
    """The candidate to start from instead of the head. This makes a branch."""
    proposed: bool = False
    """Wait for a review before the daemon schedules the idea."""
    track: TrackName | None = None
    """The track it works on. Required when the campaign has more than one track."""
    after: tuple[IdeaId, ...] = ()
    """The ideas to wait for. The idea runs from the head when all are done."""
    supersedes: tuple[IdeaId, ...] = ()
    """The ideas that this idea refines."""


class AckBody(Params):
    """The inbox items to acknowledge."""

    ids: tuple[CommentId, ...]


class ReplyBody(Params):
    """An answer to an inbox item: a ``decision`` on the root of its thread."""

    body: str
    priority: Priority = 2
