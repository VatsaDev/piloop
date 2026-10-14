"""Wake overviews: the most important memory, in a fixed reading budget, at the start of a session.

A session reads a short overview first and queries for more only when it needs it (the idea of OptMem's
``memo wake``). Two overviews use it. A worker's or research sweep's brief opens with the current lessons of the
repo (``lessons_overview``). The orchestrator runs ``piloop wake`` (``orchestrator_overview``): its latest journal
lines, the open inbox, the head of each active project and the current lessons. A budget is in tokens, counted as
``CHARS_PER_TOKEN`` characters each. It covers every line printed: the headings, and the last line of a list that
counts what did not fit and says how to read it.
"""

import re
from collections.abc import Sequence
from datetime import datetime
from enum import IntEnum
from typing import NamedTuple

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from piloop.campaign import inbox
from piloop.config import ProjectConfig, config_of
from piloop.db.models import Journal, Lesson, LessonStatus, Project, ProjectStatus
from piloop.reports.overview import summary
from piloop.text import metric as number

CHARS_PER_TOKEN = 4
JOURNAL_SHARE = 0.4
"""The part of ``piloop wake``'s budget for the journal. The inbox, the projects and the lessons get 0.2 each."""
OTHER_SHARE = 0.2
LESSONS_MORE = 'the lessons query under "Remember past campaigns" in the procedures'
WORD = re.compile(r'[a-z0-9]+')
COMMON_WORDS = frozenset({'val', 'dev', 'test', 'train', 'mean', 'avg', 'total', 'all'})
"""Words of metric names that say which split or aggregate, not what is measured: sharing one makes no relation."""


def cost(lines: Sequence[str]) -> int:
    """The tokens that ``lines`` take: ``len // CHARS_PER_TOKEN + 1`` each, which counts the newline."""
    return sum(len(line) // CHARS_PER_TOKEN + 1 for line in lines)


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
    """How close a lesson's metric is to the metrics of the projects being briefed. A lower tier ranks first."""

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
    """The ``Tier`` of a lesson's metric against the metrics of the projects being briefed.

    Names compare in lower case. Words are runs of letters and digits; split and aggregate words (``COMMON_WORDS``)
    and one- or two-letter words do not count.
    """

    def __init__(self, configs: Sequence[ProjectConfig]) -> None:
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
    """The current lesson of a family, with the name of its project and the age of its campaign (0 is the newest)."""

    lesson: Lesson
    project: str
    age: int


def _family(lesson: Lesson) -> str:
    """The family in lower case, with runs of spaces as one: the same family across projects."""
    return ' '.join(lesson.family.lower().split())


def current_lessons(s: Session, repos: Sequence[str | None], relevance: Relevance) -> list[tuple[Lesson, str]]:
    """The current lesson of each family on each of ``repos``, most relevant first, with its project's name.

    A family's current lesson is the newest one of the newest campaign that has one, whichever project on the repo
    that is. Then they rank: by ``relevance`` of their metric first, so that a lesson in seconds does not outrank one
    in the briefed project's own metric. Within a tier, dead or promising before untested, then a measured effect
    before none, then the effect's rank among the lessons in the same metric (the largest, whatever its sign, first):
    effects in different units are never compared by size, the best of each metric comes before the second of any.
    Last, a newer campaign first.
    """
    rows = s.execute(
        select(Lesson, Project.name, Project.repo)
        .join(Project, Project.id == Lesson.project_id)
        .where(Project.repo.in_([r for r in repos if r is not None]))
        .order_by(Project.created_at.desc(), Project.id.desc(), Lesson.id.desc())
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

    return [(row.lesson, row.project) for row in sorted(current.values(), key=key)]


def lesson_line(lesson: Lesson, project: str) -> str:
    """One line of an overview: the project, the family, the status, the best effect and the id to read it by."""
    best = 'none measured' if lesson.effect is None else f'{number(lesson.effect, signed=True)} {lesson.metric}'
    return f'- [{project}] {lesson.family} | {lesson.status} | {best} (lesson#{lesson.id})'


def lessons_overview(s: Session, project: Project, budget: int) -> str:
    """The block that opens a brief: the current lessons on the project's repo, in ``budget``, heading included.

    Empty when the budget is 0, when there are no lessons, or when not one line fits after the heading.
    """
    lessons = current_lessons(s, [project.repo], Relevance([config_of(project)])) if budget else []
    head = [
        '# What past campaigns on this repo concluded',
        '',
        "The current lesson of each mechanism family, from the newest campaign that tried it; those in this project's"
        ' metrics first, then related metrics; within them, dead or promising first, then the largest effects of each'
        ' metric. Before you propose or build a mechanism, read more with the queries under "Remember past campaigns"'
        ' in the procedures.',
        '',
    ]
    lines = fitted([lesson_line(lesson, name) for lesson, name in lessons], budget - cost(head), LESSONS_MORE)
    return '\n'.join([*head, *lines]) if lines else ''


def orchestrator_overview(s: Session, budget: int, now: datetime) -> str:
    """What ``piloop wake`` prints: the journal, the open inbox, the active projects and their lessons, in ``budget``.

    The title comes off the budget first. Of the rest, the journal gets ``JOURNAL_SHARE`` and each other section
    ``OTHER_SHARE``, its heading included. A section that does not fit is left out. Empty when the title does not fit.
    """
    title = f'# piloop wake, {now:%Y-%m-%d %H:%M} UTC'
    rest = budget - cost([title])
    if rest < 0:
        return ''
    journal_share, other_share = int(rest * JOURNAL_SHARE), int(rest * OTHER_SHARE)

    lines = s.scalar(select(func.count()).select_from(Journal)) or 0
    journal_title = f'## Journal, oldest to newest ({lines} lines in all)'
    # a line costs at least one token, so no more lines than tokens fit: the read never grows with the journal
    newest = s.scalars(select(Journal).order_by(Journal.id.desc()).limit(journal_share)).all()
    journal = _journal(
        [f'- {j.created_at:%Y-%m-%d %H:%M} {j.author}: {j.line}' for j in newest],
        lines,
        _room(journal_title, journal_share),
    )

    items = inbox.items(s)
    inbox_title = f'## Inbox ({len(items)} open)'
    inbox_lines = fitted(
        [
            f'- p{c.priority} comment#{c.id} {c.kind} on {c.target_kind}#{c.target_id} by {c.author}'
            f'{f" ({p.name})" if p else ""}: {_first_line(c.body)[:160]}'
            for c, p in items
        ],
        _room(inbox_title, other_share),
        'piloop inbox',
    )

    active = s.scalars(select(Project).where(Project.status == ProjectStatus.ACTIVE).order_by(Project.id)).all()
    projects_title = f'## Active projects ({len(active)})'
    project_lines = fitted(
        [f'- {summary(s, p, config_of(p))}' for p in active], _room(projects_title, other_share), 'piloop status'
    )

    repos = sorted({p.repo for p in active if p.repo is not None})
    lessons = current_lessons(s, repos, Relevance([config_of(p) for p in active]))
    lessons_title = f'## Current lessons on their repos ({len(lessons)})'
    lesson_lines = fitted(
        [lesson_line(lesson, name) for lesson, name in lessons], _room(lessons_title, other_share), LESSONS_MORE
    )

    sections = (
        _section(journal_title, journal, lines, journal_share),
        _section(inbox_title, inbox_lines, len(items), other_share),
        _section(projects_title, project_lines, len(active), other_share),
        _section(lessons_title, lesson_lines, len(lessons), other_share),
    )
    return '\n'.join([title, *(line for section in sections for line in section)])


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


def _journal(newest_first: Sequence[str], total: int, budget: int) -> list[str]:
    """The newest journal lines that fit in ``budget``, oldest first, after a line that says how to read the older."""
    kept, _ = fit(newest_first, budget)
    while len(kept) < total:
        notice = f'… {total - len(kept)} older: SELECT created_at, author, line FROM journal ORDER BY id DESC'
        if cost([notice, *kept]) <= budget:
            return [notice, *reversed(kept)]
        if not kept:
            return []
        kept.pop()
    return list(reversed(kept))


def _first_line(body: str) -> str:
    return body.strip().split('\n', 1)[0]
