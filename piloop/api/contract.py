"""What the API's routes and its client share: who may call, the models of a call, the bounds, and why a route refuses.

The routes are FastAPI's (``piloop.daemon.api``). The client (``piloop.api.client``) sends these query models and
reads the answers of ``piloop.api.views``: one set of models on both sides.
"""

from collections.abc import Mapping, Sequence
from enum import StrEnum
from pathlib import PurePath
from typing import Annotated, Protocol

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    JsonValue,
    PlainSerializer,
    SecretStr,
    Tag,
)
from pydantic_core import PydanticCustomError

from piloop.ids import (
    CampaignStatus,
    CandidateId,
    CommentId,
    CommentKind,
    Ending,
    IdeaId,
    IdeaStatus,
    RoleName,
    TrackName,
)
from piloop.mcp import EnvName

MAX_POLL_SECONDS = 25
"""The longest that the server holds a long-poll (``Polled.wait``), as the GitHub Actions runner's."""
LIMIT = 100
"""How many events a read returns at most, by default."""
MAX_LIMIT = 1000
JOURNAL_LINE_BYTES = 280
"""The longest journal line, in UTF-8 bytes: one decision, plan or result, with its ids."""


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


type Revealed = Annotated[SecretStr, PlainSerializer(SecretStr.get_secret_value, when_used='json')]
"""A secret in a body or an answer: its JSON carries the value, which pydantic writes as stars otherwise. Its repr
hides it."""


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
    UNKNOWN_IDEA = 'unknown_idea'
    UNKNOWN_CANDIDATE = 'unknown_candidate'
    UNKNOWN_AGENT = 'unknown_agent'
    UNKNOWN_RUN = 'unknown_run'
    UNKNOWN_LIMIT = 'unknown_limit'
    UNKNOWN_TOKEN = 'unknown_token'
    UNKNOWN_SECRET = 'unknown_secret'
    CURSOR_AHEAD = 'cursor_ahead'
    REFUSED = 'refused'
    """An operation refused its input and changed nothing. The detail says why."""
    FORBIDDEN = 'forbidden'
    """The caller's actor may not make this change, which another actor may. The detail says why."""


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


class SeedBody(Params):
    """A seed: a hunch with pointers, for research to develop into a defined idea or to reject."""

    hunch: str
    provenance: str
    """Pointers to what prompted the hunch: ids, paths or numbers."""
    base: CandidateId | None = None
    """The candidate to start from, when the hunch is about that tree."""
    track: TrackName | None = None
    """The track it works on. Required when the campaign has more than one track."""


class DevelopBody(Params):
    """What makes a seed a defined idea: each field that the implementer needs."""

    hypothesis: str
    """The mechanism, and why it should move the metric."""
    evidence: str
    """What supports it, with ids and paths."""
    check: str
    """The cheapest measurement that can prove it wrong, and the result that stops the run."""
    metric: str
    """The metric that decides success."""
    effect: str
    """The expected size of the effect in the metric's units, and why."""
    plan: str | None = None
    title: str | None = None
    """A clearer title. Default: the seed's."""
    family: str | None = None
    base: CandidateId | None = None
    priority: Priority | None = None
    """Default: the seed's priority."""
    proposed: bool = False
    after: tuple[IdeaId, ...] = ()
    supersedes: tuple[IdeaId, ...] = ()


class Reasoned(Params):
    """A change to an idea, with why: it goes into the event or a note on the idea."""

    reason: str


class StatusBody(Reasoned):
    """An idea's new status."""

    status: IdeaStatus


class BaseBody(Reasoned):
    """The candidate that an idea's runs start from. Null: no pin, the head when the run starts."""

    base: CandidateId | None


class TrackBody(Reasoned):
    """The track to move an idea to."""

    track: TrackName


class AckBody(Params):
    """The inbox items to acknowledge."""

    ids: tuple[CommentId, ...]


class ReplyBody(Params):
    """An answer to an inbox item: a ``decision`` on the root of its thread."""

    body: str
    priority: Priority = 2


class MessageBody(Params):
    """A word to a campaign's orchestrator: a ``message`` on the campaign, which it answers with a reply."""

    body: str
    priority: Priority = 2


class LimitBody(Reasoned):
    """A value for a live key, with why: it shows next to the value."""

    value: JsonValue


class LaunchBody(Reasoned):
    """One more agent of a sweeping role (``[roles.<name>]``). The agent's brief includes the reason."""

    role: RoleName
    harness: str | None = None
    """The harness to use instead of the role's."""
    model: str | None = None
    """The model id, as the harness names it."""


class PromptBody(Polled):
    """A message for the next turn of a running agent. With wait, the answer waits for what came of it."""

    text: str


class CancelBody(Params):
    """How to cancel a run or an agent."""

    ending: Ending = Ending.NOW
    """How its running agents end: at once, or after they were told to save their work and stop."""


class TokenBody(Params):
    """A bearer token to make for a caller."""

    name: str
    """The name of the caller that uses it, such as a worker."""
    access: Access


def _journal_line(line: str) -> str:
    """``line`` with each run of whitespace as one space. Refuses one of no byte or of more than ``JOURNAL_LINE_BYTES``."""
    collapsed = ' '.join(line.split())
    if not 0 < (size := len(collapsed.encode())) <= JOURNAL_LINE_BYTES:
        raise PydanticCustomError(
            'journal_line',
            'a journal line is 1 to {most} bytes, not {size}: say less',
            {'most': JOURNAL_LINE_BYTES, 'size': size},
        )
    return collapsed


class JournalBody(Params):
    """A line of the orchestrator's journal: a decision, a plan or a result, with its ids."""

    line: Annotated[str, AfterValidator(_journal_line)]


class SecretBody(Params):
    """The value of a secret, in place of any."""

    value: Revealed


def _absolute(path: str) -> str:
    if not PurePath(path).is_absolute():
        raise PydanticCustomError('absolute_path', 'an absolute path, not {path}', {'path': path})
    return path


class Checkout(Params):
    """A checkout on the daemon's machine, registered where it is."""

    model_config = ConfigDict(extra='forbid')

    path: Annotated[str, AfterValidator(_absolute)]


class Remote(Params):
    """A repository to clone, and the home's secret that its host asks for."""

    model_config = ConfigDict(extra='forbid')

    url: str
    secret: EnvName | None = None
    """The name of the home's secret that holds the credential, such as a token. None: the host asks for none."""


class Clone(Params):
    """A repository that the daemon clones under its home, then registers."""

    model_config = ConfigDict(extra='forbid')

    clone: Remote


def _source(value: object) -> str | None:
    """Which of a ``Registration``'s models a body is, by its key."""
    match value:
        case Checkout() | {'path': _}:
            return 'path'
        case Clone() | {'clone': _}:
            return 'clone'
        case _:
            return None


type Registration = Annotated[Annotated[Checkout, Tag('path')] | Annotated[Clone, Tag('clone')], Discriminator(_source)]
"""What to register as a campaign: exactly one of ``{"path": ...}`` and ``{"clone": {...}}``."""
