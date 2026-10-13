"""Wake overviews: the most important memory, in a fixed reading budget, at the start of a session.

A session reads a short overview first and queries for more only when it needs it (the idea of OptMem's
``memo wake``). Two overviews use it. A worker's or research sweep's brief opens with the current lessons of the
repo (``lessons_overview``). The orchestrator runs ``piloop wake`` (``orchestrator_overview``): its latest journal
lines, the open inbox, the head of each active project and the current lessons. A budget is in tokens, counted as
``CHARS_PER_TOKEN`` characters each. Lines that do not fit are counted in a last line that says how to read them.
"""

from collections.abc import Sequence
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.campaign import inbox
from piloop.config import config_of
from piloop.db.models import Comment, CommentKind, Journal, Project, ProjectStatus
from piloop.reports.overview import summary

CHARS_PER_TOKEN = 4
JOURNAL_SHARE = 0.4
"""The part of ``piloop wake``'s budget for the journal. The inbox, the projects and the lessons get 0.2 each."""
OTHER_SHARE = 0.2
LESSONS_MORE = 'the lessons query under "Remember past campaigns" in the procedures'


def fit(lines: Sequence[str], budget: int) -> tuple[list[str], int]:
    """The first ``lines`` that fit in ``budget`` tokens, and how many did not."""
    used = 0
    for i, line in enumerate(lines):
        used += len(line) // CHARS_PER_TOKEN + 1
        if used > budget:
            return list(lines[:i]), len(lines) - i
    return list(lines), 0


def fitted(lines: Sequence[str], budget: int, more: str) -> list[str]:
    """``fit``, then ``… N more: <more>`` when some lines did not fit."""
    kept, rest = fit(lines, budget)
    return [*kept, *([f'… {rest} more: {more}'] if rest else [])]


def _first_line(body: str) -> str:
    return body.strip().split('\n', 1)[0]


def current_lessons(s: Session, repos: Sequence[str | None], first: int | None = None) -> list[tuple[Comment, str]]:
    """The current lesson of each (project, family) of the projects on ``repos``, with the project's name.

    The newest lesson of a family is the current one. They come newest first, the lessons of project ``first`` before
    the others.
    """
    rows = s.execute(
        select(Comment, Project)
        .join(Project, Project.id == Comment.target_id)
        .where(Comment.kind == CommentKind.LESSON, Project.repo.in_([r for r in repos if r is not None]))
        .order_by(Comment.id.desc())
    ).all()
    seen: set[tuple[int, str]] = set()
    current: list[tuple[Comment, str]] = []
    for comment, project in rows:
        family = _first_line(comment.body).split(' | ', 1)[0]
        if (project.id, family) not in seen:
            seen.add((project.id, family))
            current.append((comment, project.name))
    return sorted(current, key=lambda row: row[0].target_id != first)


def _lesson_line(comment: Comment, project: str) -> str:
    return f'- [{project}] {_first_line(comment.body)} (comment#{comment.id})'


def lessons_overview(s: Session, project: Project, budget: int) -> str:
    """The block that opens a brief: the current lessons on the project's repo. Empty when off or when there are none."""
    lessons = current_lessons(s, [project.repo], first=project.id) if budget else []
    if not lessons:
        return ''
    head = [
        '# What past campaigns on this repo concluded',
        '',
        'The current lesson of each mechanism family, this project first, newest first. Before you propose or build a'
        ' mechanism, read more with the queries under "Remember past campaigns" in the procedures.',
        '',
    ]
    return '\n'.join(fitted([*head, *(_lesson_line(c, name) for c, name in lessons)], budget, LESSONS_MORE))


def orchestrator_overview(s: Session, budget: int, now: datetime) -> str:
    """What ``piloop wake`` prints: the journal, the open inbox, the active projects and their lessons, in ``budget``."""
    if budget <= 0:
        return ''
    journal = s.scalars(select(Journal).order_by(Journal.id.desc())).all()
    newest, older = fit(
        [f'- {j.created_at:%Y-%m-%d %H:%M} {j.author}: {j.line}' for j in journal], int(budget * JOURNAL_SHARE)
    )
    journal_lines = [
        *([f'… {older} older: SELECT created_at, author, line FROM journal ORDER BY id DESC'] if older else []),
        *reversed(newest),
    ]
    items = inbox.items(s)
    inbox_lines = fitted(
        [
            f'- p{c.priority} comment#{c.id} {c.kind} on {c.target_kind}#{c.target_id} by {c.author}'
            f'{f" ({p.name})" if p else ""}: {_first_line(c.body)[:160]}'
            for c, p in items
        ],
        int(budget * OTHER_SHARE),
        'piloop inbox',
    )
    active = s.scalars(select(Project).where(Project.status == ProjectStatus.ACTIVE).order_by(Project.id)).all()
    project_lines = fitted(
        [f'- {summary(s, p, config_of(p))}' for p in active], int(budget * OTHER_SHARE), 'piloop status'
    )
    lessons = current_lessons(s, sorted({p.repo for p in active if p.repo is not None}))
    lesson_lines = fitted([_lesson_line(c, name) for c, name in lessons], int(budget * OTHER_SHARE), LESSONS_MORE)
    sections = (
        (f'## Journal, oldest to newest ({len(journal)} lines in all)', journal_lines),
        (f'## Inbox ({len(items)} open)', inbox_lines),
        (f'## Active projects ({len(active)})', project_lines),
        (f'## Current lessons on their repos ({len(lessons)})', lesson_lines),
    )
    return '\n'.join(
        [
            f'# piloop wake, {now:%Y-%m-%d %H:%M} UTC',
            *(line for title, lines in sections for line in ('', title, *(lines or ['- none']))),
        ]
    )
