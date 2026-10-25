"""The public read side: the event feed after a cursor, and snapshots that carry the cursor they were read at.

A client reads a snapshot (``status_view``, ``inbox_view``) inside ``piloop.db.feed.snapshot``, which also returns the
id of the newest event in the same read transaction. It then reads the events after that cursor (``read``), so it
joins the two without a gap. Delivery is at least once: a client checkpoints
``next`` after it has handled a batch, resumes from its checkpoint after a disconnect, and ignores an event id it has
already handled. Each client keeps its own cursor; piloop keeps no subscriber state.

Every stored event is shown in one envelope (``Envelope``, version ``PROTOCOL``), whatever its kind, without
rewriting the row. A kind that this piloop does not know (a plugin's, or an agent's own insert) passes through with its
payload as opaque JSON. A field that does not read as its type, in a row that an older piloop let through, is null,
and ``invalid`` holds its stored text: one bad row never stops a reader.

These functions read the database and nothing else. The API serves what they return (``piloop.api.views``).
"""

import json
import math
from collections.abc import Collection, Sequence
from datetime import UTC, datetime

from pydantic import JsonValue
from sqlalchemy import String, case, cast, func, select
from sqlalchemy.orm import Session

from piloop.api.contract import LIMIT, MAX_LIMIT
from piloop.api.views import (
    Batch,
    CampaignState,
    Envelope,
    Head,
    Inbox,
    Item,
    LiveAgent,
    Named,
    OpenRun,
    Subject,
    Target,
    TrackScore,
)
from piloop.campaign import inbox
from piloop.campaign.inbox import campaign_of
from piloop.db import feed as db_feed
from piloop.db.models import OPEN_RUNS, Campaign, Event, Idea, Run, campaign_runs, count
from piloop.ids import CampaignId, CommentKind, IdeaStatus
from piloop.reports.overview import head_scores, live_agent_sessions, produced, queued_ideas
from piloop.scoring.ledger import keeps
from piloop.text import iso
from piloop.usage.totals import campaign_tokens


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
        events.append(
            Envelope(
                id=row.id,
                kind=row.kind,
                subject=Subject(kind=str(row.target_kind), id=row.target_id, campaign=row.campaign),
                actor=row.actor,
                time=time_,
                payload=payload,
                invalid=invalid or None,
            )
        )
    return Batch(events=events, next=page.next, more=page.more)


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
                parent=parent_of(s, campaign),
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


def parent_of(s: Session, campaign: Campaign) -> Named | None:
    """The campaign that ``campaign`` is under, or None for a root."""
    if campaign.parent_id is None:
        return None
    parent = s.get_one(Campaign, campaign.parent_id)
    return Named(id=parent.id, name=parent.name)


def inbox_view(
    s: Session,
    cursor: int,
    *,
    campaign_id: CampaignId | None = None,
    kinds: Collection[CommentKind] = (),
    acked: bool = False,
) -> Inbox:
    """What ``piloop inbox`` lists, as data, with the same filters (``piloop.campaign.inbox.items``).

    Call it inside the snapshot whose cursor is ``cursor``.
    """
    return Inbox(
        cursor=cursor,
        items=[
            Item(
                id=c.id,
                priority=c.priority,
                kind=str(c.kind),
                campaign=p.id if p else None,
                campaign_name=p.name if p else None,
                target=Target(kind=str(c.target_kind), id=c.target_id),
                author=c.author,
                created=iso(c.created_at),
                acked_at=iso(c.acked_at) if c.acked_at else None,
                acked_by=c.acked_by,
                body=c.body,
            )
            for c, p in inbox.items(s, campaign_id=campaign_id, kinds=kinds, acked=acked)
        ],
        seeds=[inbox.seed_of(i, p) for i, p in inbox.seeds(s, campaign_id=campaign_id)],
    )


def _time(stored: str) -> str | None:
    """A stored time (SQLite's text, naive in UTC, or with an offset) in ISO 8601 UTC, None when it is not a time."""
    try:
        at = datetime.fromisoformat(stored)
    except ValueError:
        return None
    return iso(at if at.tzinfo else at.replace(tzinfo=UTC))


def _json(stored: str) -> JsonValue:
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
