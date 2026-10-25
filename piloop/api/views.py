"""The documents that the API answers.

``piloop status --json``, ``piloop inbox --json`` and ``piloop events --json`` print them as they are, with
``protocol`` first.

A snapshot (``Status``, ``Inbox``) carries the cursor that it was read at: a client reads the events after it
(``Batch``) and so joins the two without a gap. The fields are on the "Event feed" page of the docs.
"""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue

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
from piloop.text import CELL_CHARS, NONE, cell, metric

PROTOCOL = 1
"""The version of the documents and of the event envelope. A field may be added within a version, and readers ignore
fields they do not know. Removing or changing a field is a new version."""


class View(BaseModel):
    """A document of the API: frozen, its fields documented, and read back by the client with the same model."""

    model_config = ConfigDict(frozen=True, use_attribute_docstrings=True)


def _absent(value: object) -> bool:
    return value is None


class Subject(View):
    """The row that an event is about."""

    kind: str
    """A ``TargetKind``: ``campaign``, ``idea``, ``run``, ``candidate``, ``measurement``, ``agent_session``, ``comment``, or
    ``home`` for an event about the whole home."""
    id: int
    campaign: CampaignId | None
    """The campaign of the row, resolved when the event is read. Null when the row is gone or has no campaign (a reply
    to a reply). A campaign filter keeps these events too, because they may be of that campaign."""


class Envelope(View):
    """One event, as every reader sees it."""

    id: int
    """The cursor: events are read in id order."""
    kind: str
    subject: Subject
    actor: str
    time: str | None
    """When the event was written, ISO 8601 in UTC. Null when the stored value is not a time."""
    payload: JsonValue
    """The payload as stored: any JSON value. Null when the stored value is not JSON."""
    invalid: dict[str, str] | None = Field(default=None, exclude_if=_absent)
    """Only when a stored value did not read: the stored text of each such field, ``time`` or ``payload``."""


class Batch(View):
    """The result of a read."""

    events: list[Envelope]
    next: int
    """The cursor for the next read. It passes every event that the read examined, those that the filters left out
    too, so a read that returns nothing still moves on."""
    more: bool
    """Events newer than ``next`` exist already: read again without waiting. They may not match the filters."""


class TrackScore(View):
    """The head on one track."""

    track: TrackName
    metric: str
    """The primary metric of the track."""
    value: float | None
    """Null when the current evaluator has not measured the head."""
    delta: float | None
    """Against the baseline. Null when the head is the baseline, or either is not measured."""


class Head(View):
    """The head of the trunk: the latest keep."""

    candidate: CandidateId
    tracks: list[TrackScore]


class OpenRun(View):
    """A run that is queued or running."""

    id: RunId
    idea: IdeaId
    title: str
    harness: str
    model: str
    status: str


class LiveAgent(View):
    """An agent that is starting or running."""

    id: AgentSessionId
    kind: RoleName
    state: str
    harness: str | None
    model: str | None
    run: RunId | None


class Named(View):
    """A campaign, by its id and its name."""

    id: CampaignId
    name: str


class CampaignState(View):
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


class Target(View):
    """The row that a comment is about."""

    kind: str
    id: int


class Item(View):
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


class Seed(View):
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
        f'seed idea#{seed.idea} ({seed.campaign_name}) by {seed.created_by}: {cell(seed.title)}'
        f' — {cell(seed.provenance or "")}'
    )


class Inbox(View):
    """What ``piloop inbox`` lists, read at ``cursor``."""

    cursor: int

    items: list[Item]
    """Most urgent first, then oldest first."""
    seeds: list[Seed]


class DaemonState(View):
    """The daemon that answers: its process, and where it listens."""

    pid: int
    started: str
    """When it started, ISO 8601 in UTC."""
    listen: str | None
    """The URL that it serves other machines on. Null when it serves only this machine."""


class Status(View):
    """What ``piloop status --json`` prints: the daemon, and each campaign read at ``cursor``."""

    cursor: int
    daemon: DaemonState
    campaigns: list[CampaignState]


class LiveKey(View):
    """A live key of a provider of the home (``piloop limits``), and where its value comes from."""

    key: str
    value: JsonValue
    origin: str
    """``default``, ``config.toml`` or ``limit``."""
    actor: str | None
    """Who set the limit. Null unless ``origin`` is ``limit``."""
    reason: str | None


class Text(View):
    """Markdown text: a paragraph, or a document with headings and tables of its own."""

    kind: Literal['text'] = 'text'
    text: str

    def markdown(self) -> str:
        return self.text


class Lines(View):
    """A list: one item per line."""

    kind: Literal['lines'] = 'lines'
    lines: tuple[str, ...]
    """Markdown list items, ``- …``, with ``  - …`` for an item under the one before."""

    def markdown(self) -> str:
        return '\n'.join(self.lines)


type Value = int | float | str | None
"""A value of a figure or a table cell: a number for a reader to format, a text, or None when it is unknown."""


def shown(value: Value) -> str:
    """A value for people: a number as ``piloop.text.metric`` writes it, None as ``—``."""
    if value is None:
        return NONE
    return value if isinstance(value, str) else metric(value)


class Figure(View):
    """One named value."""

    label: str
    value: Value
    unit: str | None = None
    """What the value counts, after it: ``h``, ``tokens``, ``USD``."""

    def markdown(self) -> str:
        unit = '' if self.unit is None else f' {self.unit}'
        return f'{self.label}: {shown(self.value)}{unit}'


class Figures(View):
    """Named values, in order."""

    kind: Literal['figures'] = 'figures'
    figures: tuple[Figure, ...]

    def markdown(self) -> str:
        """The figures on one line: ``head: candidate#4 · score: 3 · kept: 3``."""
        return ' · '.join(f.markdown() for f in self.figures)


class Table(View):
    """Rows under named columns."""

    kind: Literal['table'] = 'table'
    columns: tuple[str, ...]
    rows: tuple[tuple[Value, ...], ...]
    """Each row has one value per column."""

    def markdown(self) -> str:
        def row(values: tuple[str, ...]) -> str:
            return '| ' + ' | '.join(cell(v, CELL_CHARS) if v else '' for v in values) + ' |'

        return '\n'.join(
            [
                row(self.columns),
                '|' + '|'.join('---' for _ in self.columns) + '|',
                *(row(tuple(shown(v) for v in r)) for r in self.rows),
            ]
        )


type Block = Annotated[Text | Lines | Figures | Table, Field(discriminator='kind')]
"""One block of a section. The kinds are fixed: a section composes them, and every reader renders each kind its own
way."""


class Rendered(View):
    """One section of what a reader sees: a title and its blocks (``piloop.plugins.section``).

    A section provider returns it, the API answers it, and ``piloop status``, a brief and a UI render it.
    """

    title: str
    blocks: tuple[Block, ...]

    def markdown(self) -> str:
        """The section as markdown: ``## <title>``, then each block, a blank line between two."""
        return '\n\n'.join([f'## {self.title}', *(b.markdown() for b in self.blocks)])


class CampaignOverview(View):
    """A campaign's overview: the sections of its ``[status] sections``, in order."""

    id: CampaignId
    name: str
    path: str
    parent: Named | None
    sections: list[Rendered]


class Overview(View):
    """What ``piloop status`` prints: the daemon, the live keys that ``config.toml`` or a limit sets, and each overview."""

    daemon: DaemonState
    limits: list[LiveKey]
    campaigns: list[CampaignOverview]


class ProblemDetail(View):
    """Why a route refuses a request, with its status: a ``Problem``, and what to do."""

    error: str | None = None
    """A ``Problem``. Missing from a 404 of a route that the daemon does not have: it runs an older piloop."""
    detail: str
