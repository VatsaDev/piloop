"""The documents that the API answers.

``piloop status --json``, ``piloop inbox --json`` and ``piloop events --json`` print them as they are, with
``protocol`` first.

A snapshot (``Status``, ``Inbox``) carries the cursor that it was read at: a client reads the events after it
(``Batch``) and so joins the two without a gap. The fields are on the "Event feed" page of the docs.
"""

from typing import NotRequired, TypedDict

from piloop.ids import (
    AgentSessionId,
    CampaignId,
    CandidateId,
    CommentId,
    EvaluatorId,
    IdeaId,
    RoleName,
    RunId,
    TrackName,
)
from piloop.text import cell

PROTOCOL = 1
"""The version of the documents and of the event envelope. A field may be added within a version, and readers ignore
fields they do not know. Removing or changing a field is a new version."""


type Json = None | bool | int | float | str | list[Json] | dict[str, Json]


class Subject(TypedDict):
    """The row that an event is about."""

    kind: str
    """A ``TargetKind``: ``campaign``, ``idea``, ``run``, ``candidate``, ``measurement``, ``agent_session``, ``comment``, or
    ``home`` for an event about the whole home."""
    id: int
    campaign: CampaignId | None
    """The campaign of the row, resolved when the event is read. Null when the row is gone or has no campaign (a reply
    to a reply). A campaign filter keeps these events too, because they may be of that campaign."""


class Envelope(TypedDict):
    """One event, as every reader sees it."""

    id: int
    """The cursor: events are read in id order."""
    kind: str
    subject: Subject
    actor: str
    time: str | None
    """When the event was written, ISO 8601 in UTC. Null when the stored value is not a time."""
    payload: Json
    """The payload as stored: any JSON value. Null when the stored value is not JSON."""
    invalid: NotRequired[dict[str, str]]
    """Only when a stored value did not read: the stored text of each such field, ``time`` or ``payload``."""


class Batch(TypedDict):
    """The result of a read."""

    events: list[Envelope]
    next: int
    """The cursor for the next read. It passes every event that the read examined, those that the filters left out
    too, so a read that returns nothing still moves on."""
    more: bool
    """Events newer than ``next`` exist already: read again without waiting. They may not match the filters."""


class TrackScore(TypedDict):
    """The head on one track."""

    track: TrackName
    metric: str
    """The primary metric of the track."""
    value: float | None
    """Null when the current evaluator has not measured the head."""
    delta: float | None
    """Against the baseline. Null when the head is the baseline, or either is not measured."""


class Head(TypedDict):
    """The head of the trunk: the latest keep."""

    candidate: CandidateId
    tracks: list[TrackScore]


class OpenRun(TypedDict):
    """A run that is queued or running."""

    id: RunId
    idea: IdeaId
    title: str
    harness: str
    model: str
    status: str


class LiveAgent(TypedDict):
    """An agent that is starting or running."""

    id: AgentSessionId
    kind: RoleName
    state: str
    harness: str | None
    model: str | None
    run: RunId | None


class Named(TypedDict):
    """A campaign, by its id and its name."""

    id: CampaignId
    name: str


class CampaignState(TypedDict):
    """What ``piloop status`` shows of a campaign, as data."""

    id: CampaignId
    name: str
    path: str
    parent: Named | None
    """The campaign that it is under. Null for a root."""
    status: str
    stopped_reason: str | None
    paused: str | None
    """What ``piloop pause`` holds: ``launches`` or ``all``. Null while the campaign is not paused."""
    paused_reason: str | None
    evaluator: EvaluatorId | None
    head: Head | None
    """Null when the campaign has no trunk keep."""
    kept: int
    candidates: int
    """The baseline and the commits of runs, not the imported references."""
    ideas: dict[str, int]
    """The number of ideas in each status."""
    runnable: int
    """The ``ready`` ideas that no prerequisite blocks."""
    runs: list[OpenRun]
    agents: list[LiveAgent]
    """The agents that are starting or running."""
    inbox: int
    """The unacknowledged inbox items."""
    tokens: int


class Target(TypedDict):
    """The row that a comment is about."""

    kind: str
    id: int


class Item(TypedDict):
    """An inbox item: a comment with a priority."""

    id: CommentId
    priority: int | None
    kind: str
    campaign: CampaignId | None
    campaign_name: str | None
    target: Target
    author: str
    created: str
    acked_at: str | None
    acked_by: str | None
    body: str


class Seed(TypedDict):
    """A seed that waits for research."""

    idea: IdeaId
    campaign: CampaignId
    campaign_name: str
    created_by: str
    title: str
    provenance: str | None


def seed_line(seed: Seed) -> str:
    """One seed as the inbox prints it: who filed it, its title and its provenance."""
    return (
        f'seed idea#{seed["idea"]} ({seed["campaign_name"]}) by {seed["created_by"]}: {cell(seed["title"])}'
        f' — {cell(seed["provenance"] or "")}'
    )


class Inbox(TypedDict):
    """What ``piloop inbox`` lists, read at ``cursor``."""

    cursor: int

    items: list[Item]
    """Most urgent first, then oldest first."""
    seeds: list[Seed]


class DaemonState(TypedDict):
    """The daemon that answers: its process, and where it listens."""

    pid: int
    started: str
    """When it started, ISO 8601 in UTC."""
    listen: str | None
    """The URL that it serves other machines on. Null when it serves only this machine."""


class Status(TypedDict):
    """What ``piloop status --json`` prints: the daemon, and each campaign read at ``cursor``."""

    cursor: int
    daemon: DaemonState
    campaigns: list[CampaignState]


class LiveKey(TypedDict):
    """A live key of a provider of the home (``piloop limits``), and where its value comes from."""

    key: str
    value: Json
    origin: str
    """``default``, ``config.toml`` or ``limit``."""
    actor: str | None
    """Who set the limit. Null unless ``origin`` is ``limit``."""
    reason: str | None


class CampaignOverview(TypedDict):
    """A campaign's overview: the text that a sweep reads in its brief."""

    id: CampaignId
    name: str
    path: str
    parent: Named | None
    text: str


class Overview(TypedDict):
    """What ``piloop status`` prints: the daemon, the live keys that ``config.toml`` or a limit sets, and each overview."""

    daemon: DaemonState
    limits: list[LiveKey]
    campaigns: list[CampaignOverview]
