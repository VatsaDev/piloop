"""The public read side: the event feed after a cursor, and snapshots that carry the cursor they were read at.

A client reads a snapshot (``status_view``, ``inbox_view``) inside ``piloop.db.feed.snapshot``, which also returns the
id of the newest event in the same read transaction. It then reads the events after that cursor (``read``, or
``follow`` to wait for them), so it joins the two without a gap. Delivery is at least once: a client checkpoints
``next`` after it has handled a batch, resumes from its checkpoint after a disconnect, and ignores an event id it has
already handled. Each client keeps its own cursor; piloop keeps no subscriber state.

Every stored event is shown in one envelope (``Envelope``, version ``PROTOCOL``), whatever its kind, without
rewriting the row. A kind that this piloop does not know (a plugin's, or an agent's own insert) passes through with its
payload as opaque JSON. A field that does not read as its type, in a row that an older piloop let through, is null,
and ``invalid`` holds its stored text: one bad row never stops a reader.

These functions read the database and nothing else, so the CLI and a future network adapter call them unchanged and
only format the result. The documents and their fields are on the "Event feed" page of the docs.
"""

import json
import math
from collections.abc import Collection, Sequence
from datetime import UTC, datetime
from typing import NotRequired, TypedDict

from sqlalchemy import String, case, cast, func, select
from sqlalchemy.orm import Session

from piloop.campaign import inbox
from piloop.campaign.inbox import campaign_of
from piloop.db import feed as db_feed
from piloop.db.models import OPEN_RUNS, Campaign, CommentKind, Event, Idea, IdeaStatus, Run, campaign_runs, count
from piloop.db.session import Database
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
from piloop.reports.overview import head_scores, live_agent_sessions, produced, queued_ideas
from piloop.scoring.ledger import keeps
from piloop.usage.totals import campaign_tokens

PROTOCOL = 1
"""The version of the documents and of the event envelope. A field may be added within a version, and readers ignore
fields they do not know. Removing or changing a field is a new version."""
LIMIT = 100
"""How many events a read returns at most, by default."""
MAX_LIMIT = 1000

type Json = None | bool | int | float | str | list[Json] | dict[str, Json]


class Subject(TypedDict):
    """The row that an event is about."""

    kind: str
    """A ``TargetKind``: ``campaign``, ``idea``, ``run``, ``candidate``, ``measurement``, ``agent_session`` or ``comment``."""
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


def read(
    s: Session, after: int, *, campaign_id: CampaignId | None = None, kinds: Collection[str] = (), limit: int = LIMIT
) -> Batch:
    """Read at most ``limit`` events after the cursor ``after``, oldest first.

    ``campaign_id`` keeps the events whose subject belongs to that campaign, or whose campaign is unknown (``Subject``),
    and ``kinds`` the events of those kinds.

    Raises:
        ValueError: ``after`` is negative, or ``limit`` is not from 1 to ``MAX_LIMIT``.
        CursorAhead: ``after`` is past the newest event.
    """
    if not 1 <= limit <= MAX_LIMIT:
        raise ValueError(f'limit must be from 1 to {MAX_LIMIT}, not {limit}')
    owner = campaign_of(Event.target_kind, Event.target_id)
    of_kinds = [Event.kind.in_(kinds)] if kinds else []
    of_campaign = [] if campaign_id is None else [(owner == campaign_id) | owner.is_(None)]
    page = db_feed.after(s, after, limit=limit, where=[*of_kinds, *of_campaign])
    rows = s.execute(
        select(
            Event.id,
            cast(Event.kind, String).label('kind'),
            cast(Event.actor, String).label('actor'),
            Event.target_kind,
            Event.target_id,
            owner.label('campaign'),
            # the stored values as text, parsed here, so that a row that an older piloop let through cannot fail
            cast(Event.at, String).label('at'),
            case(
                (func.typeof(Event.payload) == 'blob', func.quote(Event.payload)), else_=cast(Event.payload, String)
            ).label('payload'),
        )
        .where(Event.id.in_(page.ids))
        .order_by(Event.id)
    ).all()
    events: list[Envelope] = []
    for row in rows:
        invalid: dict[str, str] = {}
        time_ = _time(row.at)
        if time_ is None:
            invalid['time'] = row.at
        try:
            payload = _json(row.payload)
        except ValueError:
            invalid['payload'], payload = row.payload, None
        event = Envelope(
            id=row.id,
            kind=row.kind,
            subject=Subject(kind=str(row.target_kind), id=row.target_id, campaign=row.campaign),
            actor=row.actor,
            time=time_,
            payload=payload,
        )
        if invalid:
            event['invalid'] = invalid
        events.append(event)
    return Batch(events=events, next=page.next, more=page.more)


def follow(
    db: Database,
    after: int,
    *,
    campaign_id: CampaignId | None = None,
    kinds: Collection[str] = (),
    limit: int = LIMIT,
    timeout: float,
) -> Batch:
    """``read``, and while it returns no event, read again on each commit (``Database.wait_until``) until ``timeout``.

    It returns the first batch with an event, or at the timeout the last empty batch, whose ``next`` has moved over
    what the filters left out. The wait blocks the caller: a streaming transport (server-sent events) loops over
    ``read`` with its own wait instead.
    """
    batch = Batch(events=[], next=after, more=False)

    def found(s: Session) -> bool:
        nonlocal batch
        batch = read(s, batch['next'], campaign_id=campaign_id, kinds=kinds, limit=limit)
        return bool(batch['events'])

    db.wait_until(found, timeout=timeout)
    return batch


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


class CampaignState(TypedDict):
    """What ``piloop status`` shows of a campaign, as data."""

    id: CampaignId
    name: str
    path: str
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


def status_view(s: Session, campaigns: Sequence[Campaign]) -> list[CampaignState]:
    """The state of each campaign, as ``piloop status`` shows it. Call it inside a snapshot for one consistent view."""
    states = []
    for campaign in campaigns:
        at = head_scores(s, campaign, campaign.config)
        by_status = select(Idea.status, func.count()).where(Idea.campaign_id == campaign.id).group_by(Idea.status)
        counts: dict[IdeaStatus, int] = {status: n for status, n in s.execute(by_status)}
        open_runs = s.scalars(campaign_runs(campaign.id).where(Run.status.in_(OPEN_RUNS)).order_by(Run.id)).all()
        states.append(
            CampaignState(
                id=campaign.id,
                name=campaign.name,
                path=campaign.path,
                status=str(campaign.status),
                stopped_reason=campaign.stopped_reason,
                paused=None if campaign.paused is None else str(campaign.paused),
                paused_reason=campaign.paused_reason,
                evaluator=campaign.evaluator_id,
                head=None
                if at is None
                else Head(
                    candidate=at[0].id,
                    tracks=[
                        TrackScore(track=track.name, metric=track.primary.name, value=value, delta=delta)
                        for track, value, delta in at[1]
                    ],
                ),
                kept=count(s, keeps(campaign)),
                candidates=count(s, produced(campaign)),
                ideas={str(status): counts.get(status, 0) for status in IdeaStatus},
                runnable=len(queued_ideas(s, campaign)[1]),
                runs=[
                    OpenRun(
                        id=r.id,
                        idea=r.idea_id,
                        title=r.idea.title,
                        harness=r.harness,
                        model=r.model,
                        status=str(r.status),
                    )
                    for r in open_runs
                ],
                agents=[
                    LiveAgent(id=w.id, kind=w.kind, state=str(w.state), harness=w.harness, model=w.model, run=w.run_id)
                    for w in live_agent_sessions(s, campaign)
                ],
                inbox=len(inbox.items(s, campaign_id=campaign.id)),
                tokens=campaign_tokens(s, campaign.id),
            )
        )
    return states


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
    created_by: str
    title: str
    provenance: str | None


class Inbox(TypedDict):
    """What ``piloop inbox`` lists."""

    items: list[Item]
    """Most urgent first, then oldest first."""
    seeds: list[Seed]


def inbox_view(
    s: Session, *, campaign_id: CampaignId | None = None, kinds: Collection[CommentKind] = (), acked: bool = False
) -> Inbox:
    """What ``piloop inbox`` lists, as data, with the same filters (``piloop.campaign.inbox.items``)."""
    return Inbox(
        items=[
            Item(
                id=c.id,
                priority=c.priority,
                kind=str(c.kind),
                campaign=p.id if p else None,
                target=Target(kind=str(c.target_kind), id=c.target_id),
                author=c.author,
                created=_iso(c.created_at),
                acked_at=_iso(c.acked_at) if c.acked_at else None,
                acked_by=c.acked_by,
                body=c.body,
            )
            for c, p in inbox.items(s, campaign_id=campaign_id, kinds=kinds, acked=acked)
        ],
        seeds=[
            Seed(idea=i.id, campaign=p.id, created_by=i.created_by, title=i.title, provenance=i.provenance)
            for i, p in inbox.seeds(s, campaign_id=campaign_id)
        ],
    )


def _iso(at: datetime) -> str:
    return at.astimezone(UTC).isoformat(timespec='microseconds').replace('+00:00', 'Z')


def _time(stored: str) -> str | None:
    """A stored time (SQLite's text, naive in UTC, or with an offset) in ISO 8601 UTC, None when it is not a time."""
    try:
        at = datetime.fromisoformat(stored)
    except ValueError:
        return None
    return _iso(at if at.tzinfo else at.replace(tzinfo=UTC))


def _json(stored: str) -> Json:
    """The JSON value of a stored payload.

    Raises:
        ValueError: It is not strict JSON, which has no ``NaN``, ``Infinity`` or ``1e400``.
    """

    def finite(number: str) -> float:
        value = float(number)
        if not math.isfinite(value):
            raise ValueError(f'{number} is not a finite number')
        return value

    def constant(name: str) -> float:
        raise ValueError(f'{name} is not JSON')

    return json.loads(stored, parse_float=finite, parse_constant=constant)
