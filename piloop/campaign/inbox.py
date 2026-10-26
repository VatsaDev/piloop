"""The human's inbox: every comment with a ``priority`` that nobody has acknowledged yet.

A comment addressed to the human has a priority from 0 (urgent) to 4 (someday). Examples are decisions that the
orchestrator took for the human, escalations and findings. Other comments have no priority and form the log.
Nothing waits for the human. An item is a record to review, and ``piloop inbox ack`` only marks it as seen.

The orchestrator is any harness that runs the ``piloop-operate`` skill. It learns of new items from the events
table: ``piloop inbox wait`` blocks until ``news`` returns something, so the orchestrator loops on one command.

The inbox also lists seeds (``seeds``): hunches filed for research that research has not yet developed or rejected.
They need no action from the human. They are listed so that the human can see what goes to research.
"""

import re
from collections.abc import Collection, Sequence
from datetime import datetime
from typing import Final

from sqlalchemy import ColumnElement, ScalarSelect, case, select
from sqlalchemy.orm import Mapped, Session, aliased

from piloop.actor import Actor
from piloop.api.views import (
    AckOutcome,
    AckResult,
    AnomalyNews,
    CommentNews,
    CommentView,
    Linked,
    News,
    NewsItem,
    Relation,
    Seed,
    SeedNews,
    StoppedNews,
    Thread,
    VerdictNews,
)
from piloop.campaign.anomaly import AnomalyReported
from piloop.db import feed as db_feed
from piloop.db.models import (
    NOT_RETIRED,
    AgentSession,
    Campaign,
    Candidate,
    Comment,
    Event,
    Idea,
    Measurement,
    Run,
    utcnow,
)
from piloop.db.triggers.changelog import CampaignStatusChanged, Commented, IdeaCreated
from piloop.ids import CampaignId, CampaignStatus, CommentId, CommentKind, IdeaStatus, TargetKind, TargetRef
from piloop.scoring.ledger import CandidateJudged
from piloop.text import iso

CITATION = re.compile(r'comment#(\d+)')
"""How a comment refers to another comment in its body. ``thread`` follows these references in both directions."""


def items(
    s: Session, *, campaign_id: CampaignId | None = None, kinds: Collection[CommentKind] = (), acked: bool = False
) -> list[tuple[Comment, Campaign | None]]:
    """Return the inbox items with their campaigns, most urgent first, then oldest first.

    Only unacknowledged items are returned, unless ``acked`` is True, and none of a retired campaign. ``campaign_id``
    and ``kinds`` filter the items.
    An item whose target has no campaign (``campaign_of``) appears only when ``campaign_id`` is None.
    """
    owner = campaign_of(Comment.target_kind, Comment.target_id)
    query = (
        select(Comment, Campaign)
        .outerjoin(Campaign, Campaign.id == owner)
        .where(Comment.priority.is_not(None), Campaign.id.is_(None) | NOT_RETIRED)
        .order_by(Comment.priority, Comment.id)
    )
    if not acked:
        query = query.where(Comment.acked_at.is_(None))
    if campaign_id is not None:
        query = query.where(owner == campaign_id)
    if kinds:
        query = query.where(Comment.kind.in_(kinds))
    return [(c, p) for c, p in s.execute(query)]


def waiting_for(
    s: Session,
    me: Actor,
    *,
    campaign_id: CampaignId | None = None,
    kinds: Collection[CommentKind] = (),
    acked: bool = False,
) -> list[tuple[Comment, Campaign | None]]:
    """Return the inbox items of ``items`` that ``me`` did not write: what ``me`` has to look at.

    A comment is for its recipients. Its author sees it in its thread (``thread_of``), with the answers, and never as
    an item to acknowledge (``ack``).
    """
    return [(c, p) for c, p in items(s, campaign_id=campaign_id, kinds=kinds, acked=acked) if c.author != me.signature]


def seeds(s: Session, *, campaign_id: CampaignId | None = None) -> list[tuple[Idea, Campaign]]:
    """Return the seeds that wait for research, oldest first, with their campaigns. ``campaign_id`` filters them."""
    query = select(Idea, Campaign).join(Campaign).where(Idea.status == IdeaStatus.SEED).order_by(Idea.id)
    if campaign_id is not None:
        query = query.where(Idea.campaign_id == campaign_id)
    return [(i, p) for i, p in s.execute(query)]


def seed_of(idea: Idea, campaign: Campaign) -> Seed:
    """A seed of ``campaign``, as the inbox lists it."""
    return Seed(
        idea=idea.id,
        campaign=campaign.id,
        campaign_name=campaign.name,
        created_by=idea.created_by,
        title=idea.title,
        provenance=idea.provenance,
    )


WAKES: Final = tuple(
    db_feed.Topic(m) for m in (Commented, CampaignStatusChanged, AnomalyReported, IdeaCreated, CandidateJudged)
)
"""The events that ``news`` reports: what ends ``piloop inbox wait``."""
PAGE = 500


def news(s: Session, after: int, *, campaign_id: CampaignId | None = None, me: Actor) -> News:
    """Return the events after event#``after`` that the orchestrator must look at, and the next cursor.

    The news reports:

    - new unacknowledged inbox items that wait for ``me`` (``waiting_for``). The orchestrator's own decisions are for
      the human.
    - seeds that someone else filed.
    - campaigns that the daemon stopped.
    - anomalies that the daemon gave to a watchdog.
    - the daemon's verdicts.

    It reads the ``WAKES`` events from the feed (``piloop.db.feed``), a page at a time until a page gives a line.
    ``campaign_id`` filters all of them. The returned cursor passes every event that it read, so no event is missed.
    """
    cursor, found, more = after, list[NewsItem](), True
    while more and not found:
        page = db_feed.after(s, cursor, WAKES, limit=PAGE)
        cursor, more = page.next, page.more
        events = s.scalars(select(Event).where(Event.id.in_(page.ids)).order_by(Event.id)).all()
        found = _news(s, events, campaign_id, me)
    return News(next=cursor, items=found)


def _news(s: Session, events: Sequence[Event], campaign_id: CampaignId | None, me: Actor) -> list[NewsItem]:
    fresh = {e.target_id for e in events if isinstance(e.payload, Commented)}
    found: list[NewsItem] = [
        CommentNews(comment=comment_view(c), campaign_name=campaign.name if campaign else None)
        for c, campaign in waiting_for(s, me, campaign_id=campaign_id)
        if c.id in fresh
    ]
    for e in events:
        match e.payload:
            case CampaignStatusChanged(new=CampaignStatus.STOPPED, stopped_reason=why):
                campaign = s.get(Campaign, e.target_id)
                if campaign is not None and campaign_id in (None, campaign.id):
                    found.append(StoppedNews(campaign_name=campaign.name, reason=why))
            case AnomalyReported(line=line):
                agent_session = s.get(AgentSession, e.target_id)
                if agent_session is not None and campaign_id in (None, agent_session.campaign_id):
                    found.append(AnomalyNews(role=agent_session.kind, agent_session=agent_session.id, line=line))
            case IdeaCreated() if e.actor != me.signature:
                idea = s.get(Idea, e.target_id)
                if idea is not None and idea.status is IdeaStatus.SEED and campaign_id in (None, idea.campaign_id):
                    found.append(SeedNews(seed=seed_of(idea, idea.campaign)))
            case CandidateJudged(verdict=verdict, reason=reason):
                candidate = s.get(Candidate, e.target_id)
                if candidate is not None and campaign_id in (None, candidate.campaign_id):
                    found.append(VerdictNews(candidate=candidate.id, verdict=str(verdict), reason=str(reason)))
    return found


def ack(s: Session, ids: Sequence[CommentId], actor: Actor) -> list[AckResult]:
    """Acknowledge the inbox items ``ids`` as ``actor`` and commit. Return what happened to each id.

    An id that is not an inbox item, that ``actor`` wrote, or that is already acknowledged, is reported and not changed.
    """
    now = utcnow()
    results = []
    for comment_id in ids:
        match comment := s.get(Comment, comment_id):
            case None | Comment(priority=None):
                results.append(AckResult(id=comment_id, outcome=AckOutcome.NOT_IN_INBOX, acked_by=None))
            case Comment(author=author) if author == actor.signature:
                results.append(AckResult(id=comment_id, outcome=AckOutcome.OWN, acked_by=comment.acked_by))
            case Comment(acked_at=datetime()):
                results.append(AckResult(id=comment_id, outcome=AckOutcome.ALREADY, acked_by=comment.acked_by))
            case _:
                comment.acked_at, comment.acked_by = now, actor.signature
                results.append(AckResult(id=comment_id, outcome=AckOutcome.ACKED, acked_by=actor.signature))
    s.commit()
    return results


def reply(s: Session, comment_id: CommentId, body: str, *, author: Actor, priority: int) -> Comment:
    """Answer inbox item ``comment_id`` as ``author`` with a ``decision`` on the root of its thread, and commit.

    As in email, the answer is ``in_reply_to`` the item, and its target is the root (``root_of``): threads stay flat,
    so ``campaign_of`` finds the campaign of every reply. The priority makes the answer an inbox item, so ``news``
    reports it to every orchestrator except its author until it is acknowledged.
    """
    answer = Comment(
        target=TargetRef(TargetKind.COMMENT, root_of(s, s.get_one(Comment, comment_id)).id),
        in_reply_to=comment_id,
        author=author.signature,
        kind=CommentKind.DECISION,
        priority=priority,
        body=body.strip(),
    )
    s.add(answer)
    s.commit()
    return answer


def message(s: Session, campaign_id: CampaignId, body: str, *, author: Actor, priority: int) -> Comment:
    """Write ``body`` to the orchestrator of campaign ``campaign_id`` as ``author``: a ``message`` on it, and commit.

    The priority makes it an inbox item, so ``news`` reports it to every orchestrator except its author. The
    orchestrator answers with ``reply``, which the author then sees in the inbox.
    """
    sent = Comment(
        target=TargetRef(TargetKind.CAMPAIGN, campaign_id),
        author=author.signature,
        kind=CommentKind.MESSAGE,
        priority=priority,
        body=body.strip(),
    )
    s.add(sent)
    s.commit()
    return sent


def root_of(s: Session, comment: Comment) -> Comment:
    """The root of ``comment``'s thread: the first comment up the chain of targets that is not on a comment."""
    while comment.target_kind is TargetKind.COMMENT and (parent := s.get(Comment, comment.target_id)) is not None:
        comment = parent
    return comment


def comment_view(c: Comment) -> CommentView:
    """``c`` as the API shows it."""
    return CommentView(
        id=c.id,
        kind=str(c.kind),
        target=str(c.target),
        in_reply_to=c.in_reply_to,
        author=c.author,
        created=iso(c.created_at),
        priority=c.priority,
        acked_at=iso(c.acked_at) if c.acked_at else None,
        acked_by=c.acked_by,
        body=c.body,
    )


def thread_of(s: Session, comment: Comment) -> Thread:
    """The thread that ``comment`` is in: its root (``root_of``) and the comments linked to the root (``thread``)."""
    root = root_of(s, comment)
    return Thread(
        comment=comment_view(root),
        linked=[Linked(**comment_view(c).model_dump(), relation=relation) for c, relation in thread(s, root)],
    )


def thread(s: Session, root: Comment) -> list[tuple[Comment, Relation]]:
    """Return the comments linked to the root of a thread, with how: its replies, then those it cites or that cite it.

    Each part is oldest first. A citation is ``comment#<id>`` in the body, for example ``Re: comment#12`` in a
    decision. Other comments on the same target are not part of the thread. A campaign collects every research
    finding, and the answer would be lost among them.
    """
    ref = f'comment#{root.id}'
    reply = Comment.target == TargetRef(TargetKind.COMMENT, root.id)
    cites = Comment.body.op('GLOB', is_comparison=True)(f'*{ref}') | Comment.body.op('GLOB', is_comparison=True)(
        f'*{ref}[^0-9]*'
    )
    cited = Comment.id.in_(int(n) for n in CITATION.findall(root.body))
    relation = case((reply, Relation.REPLY.value), (cites, Relation.CITES.value), else_=Relation.CITED_BY.value)
    rows = s.execute(
        select(Comment, relation)
        .where(reply | cites | cited, Comment.id != root.id)
        .order_by(case((reply, 0), else_=1), Comment.id)
    )
    return [(c, Relation(r)) for c, r in rows]


def campaign_of(target_kind: Mapped[TargetKind], target_id: Mapped[int]) -> ColumnElement[int | None]:
    """Return the campaign that the target of a comment or event belongs to.

    A ``comment`` target is resolved through that comment's target, and through one more comment. That covers every
    reply, because ``reply`` keeps threads flat: a reply is on its root, and the event of a reply is on the reply.
    Deeper comments have no campaign.
    """
    return case(*_owner_branches(target_kind, target_id, comments=2))


def _owner_of_comment(comment_id: Mapped[int], comments: int) -> ScalarSelect[int | None]:
    parent = aliased(Comment)
    return (
        select(case(*_owner_branches(parent.target_kind, parent.target_id, comments)))
        .where(parent.id == comment_id)
        .scalar_subquery()
    )


def _owner_branches(
    target_kind: Mapped[TargetKind], target_id: Mapped[int], comments: int
) -> list[tuple[ColumnElement[bool], Mapped[int] | ScalarSelect[int] | ScalarSelect[int | None]]]:
    through = [(target_kind == TargetKind.COMMENT, _owner_of_comment(target_id, comments - 1))] if comments else []
    return [
        *through,
        (target_kind == TargetKind.CAMPAIGN, target_id),
        (target_kind == TargetKind.IDEA, select(Idea.campaign_id).where(Idea.id == target_id).scalar_subquery()),
        (
            target_kind == TargetKind.RUN,
            select(Idea.campaign_id).join(Run.idea).where(Run.id == target_id).scalar_subquery(),
        ),
        (
            target_kind == TargetKind.CANDIDATE,
            select(Candidate.campaign_id).where(Candidate.id == target_id).scalar_subquery(),
        ),
        (
            target_kind == TargetKind.MEASUREMENT,
            select(Candidate.campaign_id)
            .join(Measurement.candidate)
            .where(Measurement.id == target_id)
            .scalar_subquery(),
        ),
        (
            target_kind == TargetKind.AGENT_SESSION,
            select(AgentSession.campaign_id).where(AgentSession.id == target_id).scalar_subquery(),
        ),
    ]
