"""Wake overviews: the most important memory, in a fixed reading budget, at the start of a session.

A session reads a short overview first and queries for more only when it needs it (the idea of OptMem's
``memo wake``). Two readers use it. A brief shows the current lessons of the repo (the ``lessons`` section). The
orchestrator runs ``piloop wake`` (``orchestrator_overview``): the latest lines of its campaign's journal, the open
inbox, the head of the campaign and the current lessons. A budget is in tokens, counted as
``CHARS_PER_TOKEN`` characters each. It covers every line printed: the headings, and the last line of a list that
counts what did not fit and says how to read it.
"""

import re
from collections.abc import Sequence
from datetime import datetime
from enum import Enum, IntEnum, auto
from typing import NamedTuple

from sqlalchemy import ColumnElement, func, select
from sqlalchemy.orm import Session

from piloop.campaign import inbox
from piloop.config import CampaignConfig
from piloop.db.models import Campaign
from piloop.ids import CampaignId, CampaignStatus
from piloop.plugins.memory.local.tables import Journal, Lesson, LessonStatus
from piloop.reports.overview import summary
from piloop.text import metric as number

CHARS_PER_TOKEN = 4
JOURNAL_SHARE = 0.4
"""The part of ``piloop wake``'s budget for the journal. The inbox, the campaigns and the lessons get 0.2 each."""
OTHER_SHARE = 0.2
LESSONS_MORE = 'the lessons query under "Remember past campaigns" in the procedures'
WORD = re.compile(r'[a-z0-9]+')
COMMON_WORDS = frozenset({'val', 'dev', 'test', 'train', 'mean', 'avg', 'total', 'all'})
"""Words of metric names that say which split or aggregate, not what is measured: sharing one makes no relation."""


def cost(lines: Sequence[str]) -> int:
    """The tokens that ``lines`` take: ``len // CHARS_PER_TOKEN + 1`` for each line as printed, which counts the newline.

    A value that an agent wrote can hold newlines, so one entry can print as several lines: each one counts.
    """
    return sum(len(part) // CHARS_PER_TOKEN + 1 for line in lines for part in line.split('\n'))


def fit(lines: Sequence[str], budget: int) -> tuple[list[str], int]:
    """The first ``lines`` that fit in ``budget`` tokens, and how many did not."""
    used = 0
    for i, line in enumerate(lines):
        used += cost([line])
        if used > budget:
            return list(lines[:i]), len(lines) - i
    return list(lines), 0


def fitted(lines: Sequence[str], budget: int, more: str) -> list[str]:
    """``fit``, then ``… N more: <more>`` when some lines did not fit, all within ``budget``.

    The notice takes the place of as many kept lines as it needs. Empty when not even the notice fits.
    """
    kept, rest = fit(lines, budget)
    while rest:
        notice = f'… {rest} more: {more}'
        if cost([*kept, notice]) <= budget:
            return [*kept, notice]
        if not kept:
            return []
        kept.pop()
        rest += 1
    return kept


class Tier(IntEnum):
    """How close a lesson's metric is to the metrics of the campaigns being briefed. A lower tier ranks first."""

    PRIMARY = 0
    """A primary metric of theirs."""
    MEASURED = 1
    """Another metric they measure: a guardrail, a watched one."""
    NEAR_PRIMARY = 2
    """Shares a word with a primary (``rep_correct_at_b`` with ``val_correct_at_b``)."""
    NEAR_MEASURED = 3
    """Shares a word with another of their metrics."""
    NO_METRIC = 4
    """The lesson measured no effect: it may be close, nothing says."""
    UNRELATED = 5
    """A metric that is none of these."""


class Relevance:
    """The ``Tier`` of a lesson's metric against the metrics of the campaigns being briefed.

    Names compare in lower case. Words are runs of letters and digits; split and aggregate words (``COMMON_WORDS``)
    and one- or two-letter words do not count.
    """

    def __init__(self, configs: Sequence[CampaignConfig]) -> None:
        self._primary = {t.primary.name.lower() for cfg in configs for t in cfg.all_tracks}
        self._other = {m.name.lower() for cfg in configs for m in cfg.metrics} - self._primary
        self._primary_words = set().union(*map(_words, self._primary))
        self._other_words = set().union(*map(_words, self._other))

    def tier(self, metric: str | None) -> Tier:
        if metric is None:
            return Tier.NO_METRIC
        metric = metric.lower()
        if metric in self._primary:
            return Tier.PRIMARY
        if metric in self._other:
            return Tier.MEASURED
        words = _words(metric)
        if words & self._primary_words:
            return Tier.NEAR_PRIMARY
        return Tier.NEAR_MEASURED if words & self._other_words else Tier.UNRELATED


def _words(metric: str) -> set[str]:
    return {w for w in WORD.findall(metric.lower()) if len(w) > 2 and w not in COMMON_WORDS}


class Current(NamedTuple):
    """The current lesson of a family, with the name of its campaign and the age of its campaign (0 is the newest)."""

    lesson: Lesson
    campaign: str
    age: int


def _family(lesson: Lesson) -> str:
    """The family in lower case, with runs of spaces as one: the same family across campaigns."""
    return ' '.join(lesson.family.lower().split())


def current_lessons(s: Session, repos: Sequence[str | None], relevance: Relevance) -> list[tuple[Lesson, str]]:
    """The current lesson of each family on each of ``repos``, most relevant first, with its campaign's name.

    A family's current lesson is the newest one of the newest campaign that has one, whichever campaign on the repo
    that is. Then they rank: by ``relevance`` of their metric first, so that a lesson in seconds does not outrank one
    in the briefed campaign's own metric. Within a tier, dead or promising before untested, then a measured effect
    before none, then the effect's rank among the lessons in the same metric (the largest, whatever its sign, first):
    effects in different units are never compared by size, the best of each metric comes before the second of any.
    Last, a newer campaign first.
    """
    rows = s.execute(
        select(Lesson, Campaign.name, Campaign.repo)
        .join(Campaign, Campaign.id == Lesson.campaign_id)
        .where(Campaign.repo.in_([r for r in repos if r is not None]))
        .order_by(Campaign.created_at.desc(), Campaign.id.desc(), Lesson.id.desc())
    ).all()
    current: dict[tuple[str | None, str], Current] = {}
    for age, (lesson, name, repo) in enumerate(rows):
        current.setdefault((repo, _family(lesson)), Current(lesson, name, age))
    by_metric: dict[str, list[float]] = {}
    for lesson, _, _ in current.values():
        if lesson.metric is not None and lesson.effect is not None:
            by_metric.setdefault(lesson.metric.lower(), []).append(abs(lesson.effect))
    for effects in by_metric.values():
        effects.sort(reverse=True)

    def key(row: Current) -> tuple[Tier, bool, bool, int, int]:
        lesson = row.lesson
        if lesson.metric is None or lesson.effect is None:
            place, measured = 0, False
        else:
            place, measured = by_metric[lesson.metric.lower()].index(abs(lesson.effect)), True
        untested = lesson.status is LessonStatus.UNTESTED
        return relevance.tier(lesson.metric), untested, not measured, place, row.age

    return [(row.lesson, row.campaign) for row in sorted(current.values(), key=key)]


def lesson_line(lesson: Lesson, campaign: str) -> str:
    """One line of an overview: the campaign, the family, the status, the best effect and the id to read it by."""
    best = 'none measured' if lesson.effect is None else f'{number(lesson.effect, signed=True)} {lesson.metric}'
    return f'- [{campaign}] {lesson.family} | {lesson.status} | {best} (lesson#{lesson.id})'


def orchestrator_overview(s: Session, campaign: Campaign | None, budget: int, now: datetime) -> str:
    """What ``piloop wake`` prints: the journal, the open inbox, the active campaigns and their lessons, in ``budget``.

    Of ``campaign``, or of the whole home when it is None: every campaign's journal, and the lines that no campaign
    has. A campaign's wake shows those lines too, under a heading of their own, after its own lines. The title comes
    off the budget first. Of the rest, the journal gets ``JOURNAL_SHARE``, its own lines first, and each other section
    ``OTHER_SHARE``, its heading included. A section that does not fit is left out. Empty when the title does not fit.
    """
    scope = _scope(campaign)
    title = f'# piloop wake: {scope.name}, {now:%Y-%m-%d %H:%M} UTC'
    rest = budget - cost([title])
    if rest < 0:
        return ''
    journal_share, other_share = int(rest * JOURNAL_SHARE), int(rest * OTHER_SHARE)

    journal: list[str] = []
    for part in scope.journals:
        journal += _journal_part(s, part, journal_share - cost(journal))

    items = inbox.items(s, campaign_id=scope.campaign_id)
    inbox_title = f'## Inbox ({len(items)} open)'
    inbox_lines = fitted(
        [
            f'- p{c.priority} comment#{c.id} {c.kind} on {c.target} by {c.author}'
            f'{f" ({p.name})" if p else ""}: {_first_line(c.body)[:160]}'
            for c, p in items
        ],
        _room(inbox_title, other_share),
        f'piloop inbox{scope.ref}',
    )

    active = s.scalars(
        select(Campaign).where(Campaign.status == CampaignStatus.ACTIVE, *scope.campaigns).order_by(Campaign.id)
    ).all()
    campaigns_title = f'## Active campaigns ({len(active)})'
    campaign_lines = fitted(
        [f'- {summary(s, p, p.config)}' for p in active],
        _room(campaigns_title, other_share),
        f'piloop status{scope.ref}',
    )

    repos = sorted({p.repo for p in active if p.repo is not None})
    lessons = current_lessons(s, repos, Relevance([p.config for p in active]))
    lessons_title = f'## Current lessons on their repos ({len(lessons)})'
    lesson_lines = fitted(
        [lesson_line(lesson, name) for lesson, name in lessons], _room(lessons_title, other_share), LESSONS_MORE
    )

    sections = (
        journal,
        _section(inbox_title, inbox_lines, len(items), other_share),
        _section(campaigns_title, campaign_lines, len(active), other_share),
        _section(lessons_title, lesson_lines, len(lessons), other_share),
    )
    return '\n'.join([title, *(line for section in sections for line in section)])


class IfEmpty(Enum):
    """What a part of the journal shows when it has no line."""

    NONE = auto()
    """Its heading and ``- none``."""
    LEFT_OUT = auto()
    """Nothing."""


class JournalPart(NamedTuple):
    """Some lines of the journal, under a heading of their own."""

    heading: str
    conditions: tuple[ColumnElement[bool], ...]
    """The conditions on ``journal`` rows."""
    where: str
    """The same in SQL, for the reader: a ``WHERE`` clause after a space, or empty."""
    if_empty: IfEmpty


HOME_JOURNAL = JournalPart(
    'Home journal from before lines had a campaign, oldest to newest',
    (Journal.campaign_id.is_(None),),
    ' WHERE campaign_id IS NULL',
    IfEmpty.LEFT_OUT,
)
"""The lines that no campaign has, which a campaign's wake shows after its own: nothing tells their campaign."""


class Scope(NamedTuple):
    """What ``piloop wake`` reads: one campaign, or the whole home."""

    name: str
    campaign_id: CampaignId | None
    ref: str
    """The argument of a command that reads more of the same, after a space; empty for the whole home."""
    journals: tuple[JournalPart, ...]
    """The parts of the journal, in the order that they take the journal's share and print."""
    campaigns: tuple[ColumnElement[bool], ...]
    """The conditions on ``campaigns`` rows."""


def _scope(campaign: Campaign | None) -> Scope:
    if campaign is None:
        return Scope('the home', None, '', (JournalPart('Journal, oldest to newest', (), '', IfEmpty.NONE),), ())
    own = JournalPart(
        'Journal, oldest to newest',
        (Journal.campaign_id == campaign.id,),
        f' WHERE campaign_id = {campaign.id}',
        IfEmpty.NONE,
    )
    return Scope(campaign.name, campaign.id, f' {campaign.name}', (own, HOME_JOURNAL), (Campaign.id == campaign.id,))


def _journal_part(s: Session, part: JournalPart, budget: int) -> list[str]:
    """The section of ``part``: its newest lines that fit in ``budget``, its heading included."""
    lines = s.scalar(select(func.count()).select_from(Journal).where(*part.conditions)) or 0
    if not lines and part.if_empty is IfEmpty.LEFT_OUT:
        return []
    title = f'## {part.heading} ({lines} lines in all)'
    # a line costs at least one token, so no more lines than tokens fit: the read never grows with the journal
    newest = s.scalars(select(Journal).where(*part.conditions).order_by(Journal.id.desc()).limit(budget)).all()
    kept = _journal(
        [f'- {j.created_at:%Y-%m-%d %H:%M} {j.author}: {j.line}' for j in newest],
        lines,
        _room(title, budget),
        part.where,
    )
    return _section(title, kept, lines, budget)


def _room(title: str, budget: int) -> int:
    """The tokens of ``budget`` that a section's lines have, after its blank line and ``title``."""
    return budget - cost(['', title])


def _section(title: str, lines: Sequence[str], total: int, budget: int) -> list[str]:
    """A blank line, ``title`` and ``lines``, or ``- none`` when there are none of ``total``. Empty when it does not fit.

    ``lines`` already fit in ``_room(title, budget)``. When ``total`` has rows but none of them fit, the section is left
    out rather than claim that there are none.
    """
    if total and not lines:
        return []
    block = ['', title, *(lines or ['- none'])]
    return block if cost(block) <= budget else []


def _journal(newest_first: Sequence[str], total: int, budget: int, where: str) -> list[str]:
    """The newest journal lines that fit in ``budget``, oldest first, after a line that says how to read the older.

    ``where`` is the clause of that query that selects the lines of the journal, or empty for the whole home.
    """
    kept, _ = fit(newest_first, budget)
    while len(kept) < total:
        notice = f'… {total - len(kept)} older: SELECT created_at, author, line FROM journal{where} ORDER BY id DESC'
        if cost([notice, *kept]) <= budget:
            return [notice, *reversed(kept)]
        if not kept:
            return []
        kept.pop()
    return list(reversed(kept))


def _first_line(body: str) -> str:
    return body.strip().split('\n', 1)[0]
