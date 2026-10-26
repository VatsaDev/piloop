"""``news``: what ``piloop inbox wait`` would return to the role since its last agent took the news.

The orchestrator role's trigger. The news goes to the role's agent in notes (``Steer``): in its brief when the note
is written before it starts, else as it works. Each note takes the news up to a cursor (``NewsTaken``), and the next one
takes what came after it. A row that failed did not take its news, and an agent that ended did not take a note that it
did not get: the next note takes them again.
"""

from collections.abc import Sequence
from datetime import datetime, timedelta
from enum import StrEnum
from typing import ClassVar, Literal

from pydantic import Field
from sqlalchemy import func, select

from piloop.agents.results import PromptOutcome
from piloop.api.views import CommentNews, News, NewsItem, StoppedNews
from piloop.campaign import inbox
from piloop.db import feed as db_feed
from piloop.db.events import has, latest
from piloop.db.models import OPEN, AgentSession, AgentSessionState, Comment, Event, EventModel, ReadBy
from piloop.ids import CommentKind, TargetKind
from piloop.plugins.trigger.contract import Pass, Quiet, Steer, Trigger

URGENT: frozenset[str] = frozenset({CommentKind.MESSAGE, CommentKind.ESCALATION})
"""The kinds of inbox item that wake the role after ``quiet_seconds`` alone, also in a quiet phase."""


class NewsTaken(EventModel):
    """A note to the role's agent takes the news of the events after ``after``, up to ``upto``."""

    READ_BY: ClassVar[ReadBy] = ReadBy.READERS
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
    """The cursor up to which the role's agents took the news, and when the last one took it. ``(0, None)``: never.

    A note's ``NewsTaken`` counts while its row is open, and after that only if the row exited cleanly and the agent got
    the note (``comment.delivered``): in its brief, or as it worked. An agent gets its notes in order, so the latest one
    that counts covers those before it.
    """
    mine = select(AgentSession.id).where(AgentSession.campaign_id == p.campaign.id, AgentSession.kind == p.role)
    clean = mine.where((AgentSession.state == AgentSessionState.EXITED) & AgentSession.error.is_(None))
    got = (
        select(func.json_extract(Event.payload, '$.comment'))
        .where(
            has(PromptOutcome, kind='comment.delivered'),
            Event.target_kind == TargetKind.AGENT_SESSION,
            Event.target_id.in_(clean),
        )
        .correlate(None)  # its own events, not the NewsTaken that the outer query reads
    )
    on_open = select(Comment.id).where(
        Comment.target_kind == TargetKind.AGENT_SESSION, Comment.target_id.in_(mine.where(AgentSession.state.in_(OPEN)))
    )
    last = latest(p.s, NewsTaken, Event.target_kind == TargetKind.COMMENT, Event.target_id.in_(on_open.union(got)))
    return (0, None) if last is None else (last[1].upto, last[0].at)


def note(items: Sequence[NewsItem]) -> str:
    """The note that hands ``items`` to the role's agent."""
    return '\n'.join(['News for you:', *(f'- {i.text()}' for i in items)])


def is_open(p: Pass) -> bool:
    """Whether the role has an agent that a note reaches without a launch: requested, starting or running."""
    return bool(p.live()) or (p.last is not None and p.last.state is AgentSessionState.REQUESTED)


class NewsTrigger(Trigger):
    """New inbox items for the role's party, seeds, anomalies, verdicts and the stop: ``piloop inbox wait``'s news.

    Messages, escalations and the stop wake the role ``quiet_seconds`` after the first news, also while the campaign is
    paused, held or stopped. The rest wakes it at most once in ``routine_every``, and only while the campaign runs.
    An agent of the role that is open already gets the news at once, in a note.
    """

    WHILE: ClassVar[frozenset[Quiet]] = frozenset({Quiet.PAUSED, Quiet.HELD, Quiet.STOPPED})
    WAKES_ON: ClassVar[frozenset[type[EventModel]]] = frozenset(topic.model for topic in inbox.WAKES)

    quiet_seconds: float = Field(default=30, ge=0)
    """How long the role waits after the first news to launch an agent, so that one agent takes a burst of it."""
    routine_every: float = Field(default=900, ge=0)
    """The fewest seconds between two agents that only routine news wakes: verdicts, seeds and anomalies."""

    def fire(self, p: Pass) -> Steer | None:
        cursor, last = taken(p)
        found = inbox.news(p.s, cursor, campaign_id=p.campaign.id, party=p.role)
        due = self._due(p, found, cursor, last)
        if due is None or due > p.now:
            return None
        kinds = sorted({item.kind for item in found.items})
        reason = f'news: {len(found.items)} items ({", ".join(kinds)})'
        return Steer(note(found.items), reason, events=(NewsTaken(after=cursor, upto=found.next),))

    def due_in(self, p: Pass) -> timedelta | None:
        cursor, last = taken(p)
        due = self._due(p, inbox.news(p.s, cursor, campaign_id=p.campaign.id, party=p.role), cursor, last)
        return None if due is None else max(due - p.now, timedelta())

    def _due(self, p: Pass, found: News, cursor: int, last: datetime | None) -> datetime | None:
        """When the role's agents may take ``found``, the news after ``cursor``, or None when none waits for it.

        ``last`` is when the role's agents last took news. An open agent takes urgent news at once: the quiet window
        only saves launches.
        """
        if not found.items:
            return None
        page = db_feed.after(p.s, cursor, inbox.WAKES, limit=1)
        first = p.s.scalar(select(Event.at).where(Event.id.in_(page.ids))) or p.now
        quiet = first if is_open(p) else first + timedelta(seconds=self.quiet_seconds)
        if any(urgency(item) is Urgency.NOW for item in found.items):
            return quiet
        if p.quiet is not None:  # routine news waits for the campaign to run again
            return None
        return quiet if last is None else max(quiet, last + timedelta(seconds=self.routine_every))
