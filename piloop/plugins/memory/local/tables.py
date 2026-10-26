"""The tables of ``local``: the lessons, the documents and the orchestrator's journal, in the home's database."""

from datetime import datetime
from enum import StrEnum
from pathlib import Path

from sqlalchemy import CheckConstraint, ForeignKey, Index, Integer, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from piloop.db.models import NOW, Base, Tracked, by_value, text_checks, utcnow
from piloop.db.schema import Schema
from piloop.db.triggers import on
from piloop.ids import CampaignId, JournalId
from piloop.plugins.memory.local.wire import JOURNAL_LINE_BYTES


class LessonStatus(StrEnum):
    """What a lesson concludes about its mechanism family."""

    DEAD = 'dead'
    PROMISING = 'promising'
    UNTESTED = 'untested'


class Lesson(Base):
    """The standing conclusion about one mechanism family (``ideas.family``) of a campaign, for the next campaigns.

    The closing watchdog writes one for each family that a campaign tried, and the orchestrator may write more. The
    newest lesson for a (campaign, family) is the current one: a new conclusion is a new row, and rows are never
    updated. Briefs open with the current lessons of the repo (``piloop.reports.wake``).
    """

    __tablename__ = 'lessons'
    __table_args__ = (
        Index('ix_lessons_campaign_family', 'campaign_id', 'family'),
        CheckConstraint('(effect IS NULL) = (metric IS NULL)', name='lessons.effect and lessons.metric go together'),
        CheckConstraint("typeof(effect) IN ('integer', 'real', 'null')", name='lessons.effect must be a number'),
        *text_checks('lessons', 'family', 'body', 'created_by', nullable=('metric',)),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    campaign_id: Mapped[CampaignId] = mapped_column(ForeignKey('campaigns.id', ondelete='CASCADE'))
    family: Mapped[str]
    """The mechanism family, as ``ideas.family`` names it. Case and spacing do not matter when lessons are matched."""
    status: Mapped[LessonStatus] = mapped_column(by_value(LessonStatus))
    effect: Mapped[float | None]
    """The best effect that the campaign measured in the family, in the units of ``metric``, with the sign it had: a
    gain can be a lower time. NULL when none was measured."""
    metric: Mapped[str | None]
    """The metric of ``effect``, as the campaign names it. NULL when no effect was measured."""
    body: Mapped[str]
    """Why: what was tried, the evaluator that measured the effect, why it stopped. It cites idea and candidate ids."""
    created_by: Mapped[str]
    """Who wrote it: the closing watchdog (``watchdog#<agent session id>``), the orchestrator or a human."""
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)


class Document(Tracked, Base):
    """A text document of a campaign, for agents to search: a campaign report, an orchestrator log, a design note.

    ``piloop init`` and the stop of a campaign copy in the repo files that ``[plugins.memory] documents`` names. Agents
    may insert more with raw SQL: ``hash`` is ``blake3(body)``, a function that every piloop connection has. Each
    version is a row. One path may have several versions, and the newest has the highest ``id``. The same text at the same
    path is one row, however often it is synced. ``documents_fts`` indexes the title and the body. Never a document
    derived from validation or test data: nothing that agents can search may show the scored data.
    """

    __tablename__ = 'documents'
    __table_args__ = (
        UniqueConstraint('campaign_id', 'path', 'hash'),
        CheckConstraint(
            "length(hash) = 64 AND hash NOT GLOB '*[^0-9a-f]*'", name='documents.hash is the blake3 hex digest of body'
        ),
        *text_checks('documents', 'path', 'title', 'body', 'created_by'),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    campaign_id: Mapped[CampaignId] = mapped_column(ForeignKey('campaigns.id', ondelete='CASCADE'))
    path: Mapped[str]
    """Where it is: repo-relative for a synced file, or else wherever its writer keeps it."""
    hash: Mapped[str]
    """BLAKE3 of the UTF-8 bytes of the body, in lowercase hex. It makes a sync of unchanged text do nothing."""
    title: Mapped[str]
    """Its first heading, or else the file name."""
    body: Mapped[str]
    created_by: Mapped[str]
    """Who inserted it: the actor of ``piloop init``, or the daemon, for a synced file. Else the agent's signature."""
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)


class Journal(Base):
    """The orchestrator's journal: one line for each decision, plan or result, so a new session can pick up the work.

    A coordinating session can lose its context (a crash, a full window). ``piloop wake`` prints the latest lines
    first, next to the lessons, the inbox and the heads. ``piloop journal "<line>"`` appends a line, signed with the
    actor. Rows are never updated or deleted.
    """

    __tablename__ = 'journal'
    __table_args__ = (
        CheckConstraint(
            f"typeof(line) IN ('text') AND length(CAST(line AS BLOB)) BETWEEN 1 AND {JOURNAL_LINE_BYTES}",
            name=f'journal.line is text of 1 to {JOURNAL_LINE_BYTES} bytes',
        ),
        *text_checks('journal', 'author'),
    )

    id: Mapped[JournalId] = mapped_column(Integer, primary_key=True)
    author: Mapped[str]
    """Who wrote it, for example ``orchestrator``."""
    line: Mapped[str]
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)


TABLES = (Lesson, Document, Journal)
SCHEMA = Schema(tables=TABLES, versions=Path(__file__).parent / 'migrations', triggers=on(TABLES))
