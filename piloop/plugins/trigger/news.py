"""``news``: what ``piloop inbox wait`` would return to the role since its last agent took the news.

The orchestrator role's trigger. Each requested row takes the news up to a cursor (``NewsTaken``), and the next row
takes what came after it. A row that failed did not take its news: the next one takes it again.
"""

from datetime import datetime, timedelta
from enum import StrEnum
from typing import ClassVar, Literal

from pydantic import Field
from sqlalchemy import select

from piloop.api.views import CommentNews, News, NewsItem, StoppedNews
from piloop.campaign import inbox
from piloop.db import feed as db_feed
from piloop.db.events import latest
from piloop.db.models import AgentSession, AgentSessionState, Event, EventModel
from piloop.ids import CommentKind, TargetKind
from piloop.plugins.trigger.contract import Fire, Pass, Quiet, Trigger

URGENT: frozenset[str] = frozenset({CommentKind.MESSAGE, CommentKind.ESCALATION})
"""The kinds of inbox item that wake the role after ``quiet_seconds`` alone, also in a quiet phase."""
TOOK = (AgentSessionState.REQUESTED, AgentSessionState.STARTING, AgentSessionState.RUNNING, AgentSessionState.ENDING)
"""The states of a row that took its news, besides one that exited cleanly."""


class NewsTaken(EventModel):
    """A requested row of the role takes the news of the events after ``after``, up to ``upto``."""

    kind: Literal['agent_session.news_taken'] = 'agent_session.news_taken'
    after: int
    upto: int


class Urgency(StrEnum):
    """How soon a news item wakes the role."""

    NOW = 'now'
    """After ``quiet_seconds``, also while the campaign is quiet: a message, an escalation, the stop."""
    ROUTINE = 'routine'
    """At most once in ``routine_every``, and only while the campaign runs: a verdict, a seed, an anomaly."""


def urgency(item: NewsItem) -> Urgency:
    """How soon ``item`` wakes the role."""
    match item:
        case CommentNews(comment=comment) if comment.kind in URGENT:
            return Urgency.NOW
        case StoppedNews():
            return Urgency.NOW
        case _:
            return Urgency.ROUTINE


def taken(p: Pass) -> tuple[int, datetime | None]:
    """The cursor up to which the role's agents took the news, and when the last one took it. ``(0, None)``: never."""
    mine = select(AgentSession.id).where(AgentSession.campaign_id == p.campaign.id, AgentSession.kind == p.role)
    took = mine.where(
        AgentSession.state.in_(TOOK) | ((AgentSession.state == AgentSessionState.EXITED) & AgentSession.error.is_(None))
    )
    on = (Event.target_kind == TargetKind.AGENT_SESSION, Event.target_id.in_(took))
    last = latest(p.s, NewsTaken, *on)
    return (0, None) if last is None else (last[1].upto, last[0].at)


class NewsTrigger(Trigger):
    """New inbox items for the role's party, seeds, anomalies, verdicts and the stop: ``piloop inbox wait``'s news.

    Messages, escalations and the stop wake the role ``quiet_seconds`` after the first news, also while the campaign is
    paused, held or stopped. The rest wakes it at most once in ``routine_every``, and only while the campaign runs.
    """

    WHILE: ClassVar[frozenset[Quiet]] = frozenset({Quiet.PAUSED, Quiet.HELD, Quiet.STOPPED})
    WAKES_ON: ClassVar[frozenset[type[EventModel]]] = frozenset(topic.model for topic in inbox.WAKES)

    quiet_seconds: float = Field(default=30, ge=0)
    """How long the role waits after the first news, so that one agent takes a burst of it."""
    routine_every: float = Field(default=900, ge=0)
    """The fewest seconds between two agents that only routine news wakes: verdicts, seeds and anomalies."""

    def fire(self, p: Pass) -> Fire | None:
        if p.live():
            return None
        cursor, last = taken(p)
        found = inbox.news(p.s, cursor, campaign_id=p.campaign.id, party=p.role)
        due = self._due(p, found, cursor, last)
        if due is None or due > p.now:
            return None
        kinds = sorted({item.kind for item in found.items})
        return Fire(
            f'news: {len(found.items)} items ({", ".join(kinds)})', events=(NewsTaken(after=cursor, upto=found.next),)
        )

    def due_in(self, p: Pass) -> timedelta | None:
        if p.live():
            return None
        cursor, last = taken(p)
        due = self._due(p, inbox.news(p.s, cursor, campaign_id=p.campaign.id, party=p.role), cursor, last)
        return None if due is None else max(due - p.now, timedelta())

    def _due(self, p: Pass, found: News, cursor: int, last: datetime | None) -> datetime | None:
        """When the role's next agent may take ``found``, the news after ``cursor``, or None when none waits for it.

        ``last`` is when the role's last agent took its news.
        """
        if not found.items:
            return None
        page = db_feed.after(p.s, cursor, inbox.WAKES, limit=1)
        first = p.s.scalar(select(Event.at).where(Event.id.in_(page.ids)))
        quiet = (first or p.now) + timedelta(seconds=self.quiet_seconds)
        if any(urgency(item) is Urgency.NOW for item in found.items):
            return quiet
        if p.quiet is not None:  # routine news waits for the campaign to run again
            return None
        return quiet if last is None else max(quiet, last + timedelta(seconds=self.routine_every))
