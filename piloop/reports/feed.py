"""The public read side: the event feed after a cursor, and snapshots that carry the cursor they were read at.

A client reads a snapshot (``status_view``, ``inbox_view``) inside ``snapshot``, which also returns the id of the
newest event in the same read transaction. It then reads the events after that cursor (``read``, or ``follow`` to wait
for them), so it joins the two without a gap. Delivery is at least once: a client checkpoints ``next`` after it has
handled a batch, resumes from its checkpoint after a disconnect, and ignores an event id it has already handled.
Each client keeps its own cursor; piloop keeps no subscriber state.

The event id is the cursor. SQLite has one writer at a time and allocates an id inside the writing transaction, so
the events at or below the newest id that a reader sees are all committed: a later commit never takes a smaller id.
Every piloop connection, ``piloop sql`` included, keeps it so: events are append-only, and a new event takes the next
id (``piloop.db.triggers.immutable``). Otherwise a reader whose cursor had passed a deleted, moved or explicit id would
miss the event that takes it.

Every stored event is shown in one envelope (``Envelope``, version ``PROTOCOL``), whatever its kind, without
rewriting the row. A kind that this piloop does not know (a plugin's, or an agent's own insert) passes through with its
payload as opaque JSON. A field that does not read as its type, in a row that an older piloop let through, is null,
and ``invalid`` holds its stored text: one bad row never stops a reader.

These functions read the database and nothing else, so the CLI and a future network adapter call them unchanged and
only format the result. The documents and their fields are on the "Event feed" page of the docs.
"""

import json
import math
import time
from collections.abc import Collection, Generator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import NotRequired, TypedDict

from sqlalchemy import String, case, cast, func, select
from sqlalchemy.orm import Session

from piloop.campaign import inbox
from piloop.campaign.inbox import campaign_of
from piloop.config import config_of
from piloop.db.models import (
    OPEN_RUNS,
    Campaign,
    CommentKind,
    Event,
    Idea,
    IdeaStatus,
    Run,
    campaign_runs,
    count,
)
from piloop.db.session import read_transaction
from piloop.reports.overview import head_scores, live_workers, produced, queued_ideas
from piloop.scoring.ledger import keeps
from piloop.usage import campaign_tokens

PROTOCOL = 1
"""The version of the documents and of the event envelope. A field may be added within a version, and readers ignore
fields they do not know. Removing or changing a field is a new version."""
LIMIT = 100
"""How many events a read returns at most, by default."""
MAX_LIMIT = 1000
POLL_SECONDS = 1.0
"""How often ``follow`` reads the feed while it waits. A read is one indexed query, as for ``piloop inbox wait``."""

type Json = None | bool | int | float | str | list[Json] | dict[str, Json]


class Subject(TypedDict):
    """The row that an event is about."""

    kind: str
    """A ``TargetKind``: ``campaign``, ``idea``, ``run``, ``candidate``, ``measurement``, ``worker`` or ``comment``."""
    id: int
    campaign: int | None
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


class CursorAhead(Exception):
    """The cursor is past the newest event: it is not a cursor of this database. Read a new snapshot."""

    code = 'cursor_ahead'

    def __init__(self, after: int, newest: int) -> None:
        super().__init__(
            f'cursor {after} is ahead of the feed, which ends at {newest}: read a new snapshot and use its cursor'
        )


def newest(s: Session) -> int:
    """The id of the newest event, 0 when there is none."""
    return s.scalar(select(func.max(Event.id))) or 0


@contextmanager
def snapshot(s: Session) -> Generator[int]:
    """Hold one read transaction on ``s`` and yield its cursor: the id of the newest event that it sees."""
    with read_transaction(s):
        yield newest(s)


def read(
    s: Session, after: int, *, campaign_id: int | None = None, kinds: Collection[str] = (), limit: int = LIMIT
) -> Batch:
    """Read at most ``limit`` events after the cursor ``after``, oldest first.

    ``campaign_id`` keeps the events whose subject belongs to that campaign, or whose campaign is unknown (``Subject``),
    and ``kinds`` the events of those kinds.

    Raises:
        ValueError: ``after`` is negative, or ``limit`` is not from 1 to ``MAX_LIMIT``.
        CursorAhead: ``after`` is past the newest event.
    """
    if after < 0:
        raise ValueError(f'cursor must be 0 or more, not {after}')
    if not 1 <= limit <= MAX_LIMIT:
        raise ValueError(f'limit must be from 1 to {MAX_LIMIT}, not {limit}')
    owner = campaign_of(Event.target_kind, Event.target_id)
    query = (
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
        .where(Event.id > after)
        .order_by(Event.id)
        .limit(limit)
    )
    if campaign_id is not None:
        query = query.where((owner == campaign_id) | owner.is_(None))
    if kinds:
        query = query.where(Event.kind.in_(kinds))
    with snapshot(s) as end:
        if after > end:
            raise CursorAhead(after, end)
        rows = s.execute(query.where(Event.id <= end)).all()
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
    last = rows[-1].id if len(rows) == limit else end
    return Batch(events=events, next=last, more=last < end)


def follow(
    s: Session,
    after: int,
    *,
    campaign_id: int | None = None,
    kinds: Collection[str] = (),
    limit: int = LIMIT,
    timeout: float,
    poll: float = POLL_SECONDS,
) -> Batch:
    """``read``, and while it returns no event, read again every ``poll`` seconds until ``timeout`` seconds pass.

    It returns the first batch with an event, or at the timeout the last empty batch, whose ``next`` has moved over
    what the filters left out. The sleep blocks the caller: a streaming transport (server-sent events) loops over
    ``read`` with its own wait instead.
    """
    deadline = time.monotonic() + timeout
    while True:
        batch = read(s, after, campaign_id=campaign_id, kinds=kinds, limit=limit)
        if batch['events'] or time.monotonic() >= deadline:
            return batch
        after = batch['next']
        time.sleep(poll)


class TrackScore(TypedDict):
    """The head on one track."""

    track: str
    metric: str
    """The primary metric of the track."""
    value: float | None
    """Null when the current evaluator has not measured the head."""
    delta: float | None
    """Against the baseline. Null when the head is the baseline, or either is not measured."""


class Head(TypedDict):
    """The head of the trunk: the latest keep."""

    candidate: int
    tracks: list[TrackScore]


class OpenRun(TypedDict):
    """A run that is queued or running."""

    id: int
    idea: int
    title: str
    harness: str
    model: str
    status: str


class LiveAgent(TypedDict):
    """An agent that is starting or running."""

    id: int
    kind: str
    state: str
    harness: str | None
    model: str | None
    run: int | None


class CampaignState(TypedDict):
    """What ``piloop status`` shows of a campaign, as data."""

    id: int
    name: str
    path: str
    status: str
    stopped_reason: str | None
    paused: str | None
    """What ``piloop pause`` holds: ``launches`` or ``all``. Null while the campaign is not paused."""
    paused_reason: str | None
    evaluator: int | None
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
    """The state of each campaign, as ``piloop status`` shows it. Call it inside ``snapshot`` for one consistent view."""
    states = []
    for campaign in campaigns:
        at = head_scores(s, campaign, config_of(campaign))
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
                    for w in live_workers(s, campaign)
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

    id: int
    priority: int | None
    kind: str
    campaign: int | None
    target: Target
    author: str
    created: str
    acked_at: str | None
    acked_by: str | None
    body: str


class Seed(TypedDict):
    """A seed that waits for research."""

    idea: int
    campaign: int
    created_by: str
    title: str
    provenance: str | None


class Inbox(TypedDict):
    """What ``piloop inbox`` lists."""

    items: list[Item]
    """Most urgent first, then oldest first."""
    seeds: list[Seed]


def inbox_view(
    s: Session, *, campaign_id: int | None = None, kinds: Collection[CommentKind] = (), acked: bool = False
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
    return at.astimezone(UTC).isoformat().replace('+00:00', 'Z')


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
