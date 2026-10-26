"""The documents that the API answers.

``piloop status --json``, ``piloop inbox --json`` and ``piloop events --json`` print them as they are, with
``protocol`` first.

A snapshot (``Status``, ``Inbox``) carries the cursor that it was read at: a client reads the events after it
(``Batch``) and so joins the two without a gap. The fields are on the "Event feed" page of the docs.
"""

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal, assert_never

from pydantic import BaseModel, ConfigDict, Field, JsonValue, PlainSerializer, SecretStr

from piloop.api.contract import Access
from piloop.ids import (
    AgentSessionId,
    CampaignId,
    CampaignStatus,
    CandidateId,
    Cause,
    CommentId,
    EvaluatorId,
    IdeaId,
    IdeaStatus,
    JournalId,
    KeyOrigin,
    MeasurementId,
    RoleName,
    RunId,
    TrackName,
)
from piloop.text import CELL_CHARS, NONE, age, block, cell, metric

PROTOCOL = 1
"""The version of the documents and of the event envelope. A field may be added within a version, and readers ignore
fields they do not know. Removing or changing a field is a new version."""


type Revealed = Annotated[SecretStr, PlainSerializer(SecretStr.get_secret_value, when_used='json')]
"""A secret in an answer: its JSON carries the value, which pydantic writes as stars otherwise. Its repr hides it."""


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
    status: CampaignStatus
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
    origin: KeyOrigin
    actor: str | None
    """Who set the limit. Null unless ``origin`` is ``limit``."""
    reason: str | None


class LiveKeys(View):
    """Each live key of the home's providers, read at ``cursor``."""

    cursor: int
    keys: list[LiveKey]


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


class CommentView(View):
    """A comment, as ``piloop inbox show`` shows it."""

    id: CommentId
    kind: str
    target: str
    """What it is about, as ``idea#12``."""
    author: str
    created: str
    priority: int | None
    """Null for a comment that is not in the inbox."""
    acked_at: str | None
    acked_by: str | None
    body: str


class Thread(View):
    """An inbox item, and the comments that it cites, that cite it, or that reply to it."""

    comment: CommentView
    linked: list[CommentView]


class CommentNews(View):
    """A new inbox item that someone else wrote."""

    kind: Literal['comment'] = 'comment'
    comment: CommentView
    campaign_name: str | None

    def text(self) -> str:
        """The item as ``piloop inbox wait`` prints it."""
        c = self.comment
        return f'comment#{c.id} {c.kind} p{c.priority} on {c.target} ({self.campaign_name or "—"}) by {c.author}: {cell(c.body)}'


class StoppedNews(View):
    """A campaign that the daemon stopped."""

    kind: Literal['campaign_stopped'] = 'campaign_stopped'
    campaign_name: str
    reason: str | None

    def text(self) -> str:
        """The item as ``piloop inbox wait`` prints it."""
        return f'campaign {self.campaign_name} stopped: {self.reason}'


class AnomalyNews(View):
    """An anomaly that the daemon gave to a watchdog."""

    kind: Literal['anomaly_requested'] = 'anomaly_requested'
    role: str
    agent_session: AgentSessionId
    line: str

    def text(self) -> str:
        """The item as ``piloop inbox wait`` prints it."""
        return f'anomaly, {self.role}#{self.agent_session} requested: {self.line}'


class SeedNews(View):
    """A seed that someone else filed."""

    kind: Literal['seed_filed'] = 'seed_filed'
    seed: Seed

    def text(self) -> str:
        """The item as ``piloop inbox wait`` prints it."""
        return seed_line(self.seed)


class VerdictNews(View):
    """The daemon's verdict on a measured candidate."""

    kind: Literal['verdict'] = 'verdict'
    candidate: CandidateId
    verdict: str
    reason: str

    def text(self) -> str:
        """The item as ``piloop inbox wait`` prints it."""
        return f'candidate#{self.candidate} {self.verdict}: {cell(self.reason)}'


type NewsItem = Annotated[CommentNews | StoppedNews | AnomalyNews | SeedNews | VerdictNews, Field(discriminator='kind')]


class News(View):
    """What needs the orchestrator after a cursor: what ends ``piloop inbox wait``."""

    next: int
    """The cursor for the next wait. It passes every event that the read examined."""
    items: list[NewsItem]


class CampaignAnswer(View):
    """A campaign after a change, read at ``cursor``: the events after it are the effects of the change."""

    cursor: int
    campaign: CampaignState


class IdeaView(View):
    """An idea, as the change that wrote it left it."""

    id: IdeaId
    campaign: CampaignId
    title: str
    status: IdeaStatus
    priority: int
    track: TrackName
    created_by: str


class IdeaAnswer(View):
    """An idea that a change wrote, read at ``cursor``."""

    cursor: int
    idea: IdeaView


class Defined(IdeaAnswer):
    """An idea that ``add`` or ``develop`` defined."""

    review: str | None
    """Why the idea waits for a review, as ``proposed`` although nobody asked: its author's role is held. Else null."""


class StatusChanged(IdeaAnswer):
    """An idea whose status changed."""

    was: IdeaStatus


class TrackChanged(IdeaAnswer):
    """An idea that moved to another track."""

    was: TrackName


class CandidateView(View):
    """A candidate, as the change that wrote it left it."""

    id: CandidateId
    summary: str | None
    label: str
    """The verdict as a person reads it: the verdict, ``pending``, or the origin of a candidate that is never judged."""
    cause: Cause | None
    verdict_reason: str | None


class CandidateAnswer(View):
    """A candidate that a change wrote, read at ``cursor``."""

    cursor: int
    candidate: CandidateView


class Remeasured(CandidateAnswer):
    """A candidate that the daemon measures again."""

    bar: CandidateId | None
    """The bar that it is measured next to. Null: alone."""
    held: str | None
    """What the daemon waits for before it measures, such as a resume. Null: it measures soon."""
    rejudged: bool
    """Whether the daemon judges the candidate again after the measurements. Else its verdict stays."""


class Noted(CandidateAnswer):
    """A regressed candidate kept in view as a lead."""

    seeds: list[IdeaView]
    """The follow-ups: seeds pinned to the candidate."""


class Merge(View):
    """A keep off the head, merged onto it: a candidate that the daemon measures and judges against that head."""

    candidate: CandidateId
    head: CandidateId


class VerdictChanged(CandidateAnswer):
    """A candidate whose verdict was set by hand."""

    was: str
    """The verdict that it had, as ``label`` reads it."""
    merge: Merge | None


class MeasurementAdded(View):
    """A measurement recorded by hand, read at ``cursor``."""

    cursor: int
    measurement: MeasurementId
    rejudged: bool
    """Whether the daemon judges the candidate again (``RejudgeRequested``)."""
    effect: str
    """What happens to the verdict, in one line: when the daemon judges it again, or why the verdict stays."""


class RunningSince(View):
    """An evaluation that has a slot of the executor."""

    kind: Literal['running'] = 'running'
    since: datetime


class WaitingSince(View):
    """An evaluation that waits for a slot, or a remeasure that someone asked for and that is not queued yet."""

    kind: Literal['waiting'] = 'waiting'
    since: datetime
    """When it was asked for."""


class CancelledBy(View):
    """A running evaluation that someone cancelled: the daemon stops its evaluator."""

    kind: Literal['cancelled'] = 'cancelled'
    by: str
    """Who cancelled it, as the actor signs."""


type EvaluationState = Annotated[RunningSince | WaitingSince | CancelledBy, Field(discriminator='kind')]


class QueuedEvaluation(View):
    """One evaluation of the queue."""

    candidate: CandidateId
    campaign: str
    """The name of the candidate's campaign."""
    summary: str | None
    state: EvaluationState


class EvaluationQueue(View):
    """The running evaluations, oldest start first, then the waiting ones in the order in which they get a slot."""

    evaluations: list[QueuedEvaluation]


class TokenView(View):
    """A bearer token of the server, never its secret."""

    name: str
    """The name of its caller."""
    access: Access
    created_at: datetime


class TokenList(View):
    """The tokens, by name."""

    tokens: list[TokenView]


class TokenMade(TokenView):
    """A new token, with its secret: the only answer that carries it, since the home keeps its hash only."""

    secret: Revealed


class AgentAnswer(View):
    """An agent that a change requested or asked to cancel, read at ``cursor``."""

    cursor: int
    agent: AgentSessionId
    role: RoleName


class RunAnswer(View):
    """A run that a change asked to cancel, read at ``cursor``."""

    cursor: int
    run: RunId


class Pending(View):
    """Nothing came of the note yet: the daemon queues it once the agent runs."""

    kind: Literal['pending'] = 'pending'


class Queued(View):
    """The agent's runtime holds the note: the agent reads it at its next turn."""

    kind: Literal['queued'] = 'queued'


class Delivered(View):
    """The agent read the note."""

    kind: Literal['delivered'] = 'delivered'


class Undelivered(View):
    """The note never reached the agent: its runtime refused it, or the agent ended before it read it."""

    kind: Literal['undelivered'] = 'undelivered'
    error: str | None


type Delivery = Annotated[Pending | Queued | Delivered | Undelivered, Field(discriminator='kind')]


class Prompted(View):
    """A note on an agent, which the daemon delivers, and what came of it, read at ``cursor``."""

    cursor: int
    comment: CommentId
    delivery: Delivery


class Journaled(View):
    """A line appended to the orchestrator's journal, read at ``cursor``."""

    cursor: int
    journal: JournalId


class AckOutcome(StrEnum):
    """What an acknowledgement did to one id."""

    ACKED = 'acked'
    ALREADY = 'already'
    """Someone acknowledged it before: ``acked_by`` says who."""
    NOT_IN_INBOX = 'not_in_inbox'
    """No such comment, or one without a priority: nothing to acknowledge."""


class AckResult(View):
    """What an acknowledgement did to one id."""

    id: CommentId
    outcome: AckOutcome
    acked_by: str | None
    """Who acknowledged it: the caller, or whoever did before. Null for an id that is not in the inbox."""

    def line(self) -> str:
        """The result as ``piloop inbox ack`` prints it."""
        match self.outcome:
            case AckOutcome.ACKED:
                return f'comment#{self.id} acknowledged'
            case AckOutcome.ALREADY:
                return f'comment#{self.id} was acknowledged already, by {self.acked_by}'
            case AckOutcome.NOT_IN_INBOX:
                return f'comment#{self.id} is not in the inbox'
            case _:
                assert_never(self.outcome)


class Acked(View):
    """What an acknowledgement did to each id, read at ``cursor``."""

    cursor: int
    results: list[AckResult]


class Replied(View):
    """The answer to an inbox item, read at ``cursor``."""

    cursor: int
    comment: CommentView


def thread_lines(thread: Thread, now: datetime) -> tuple[str, list[str]]:
    """``thread`` as ``piloop inbox show`` prints it: a title line, the body in full, then each linked comment, cut."""
    c = thread.comment
    if c.priority is None:
        state = 'not in the inbox'
    elif c.acked_at is None:
        state = f'p{c.priority}, unacknowledged'
    else:
        state = f'p{c.priority}, acknowledged by {c.acked_by} {age(datetime.fromisoformat(c.acked_at), now)} ago'
    title = f'comment#{c.id} {c.kind} on {c.target} by {c.author}, {age(datetime.fromisoformat(c.created), now)} ago · {state}'
    lines = c.body.strip().splitlines()
    if thread.linked:
        lines += ['', 'linked: the comments it cites, those citing it, its replies']
    for other in thread.linked:
        head = f'- comment#{other.id} {other.kind} by {other.author}, {age(datetime.fromisoformat(other.created), now)} ago: '
        lines += (head + block(other.body).replace('\n', '\n  ')).splitlines()
    return title, lines
