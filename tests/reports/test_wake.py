"""Wake overviews: the current lessons that open a brief, and what ``piloop wake`` prints, each in its budget."""

from datetime import UTC, datetime

from sqlalchemy.orm import Session

from piloop.db.models import Comment, CommentKind, Journal, Project, ProjectStatus, TargetKind
from piloop.db.session import Database
from piloop.reports.wake import fit, lessons_overview, orchestrator_overview

NOW = datetime(2026, 10, 12, 21, 0, tzinfo=UTC)


def lesson(s: Session, project: int, family: str, status: str) -> int:
    comment = Comment(
        target_kind=TargetKind.PROJECT,
        target_id=project,
        author='watchdog#1',
        kind=CommentKind.LESSON,
        body=f'family: {family} | status: {status} | best: +1 score (evaluator#1)\nwhy, with ids',
    )
    s.add(comment)
    s.flush()
    return comment.id


def clones(s: Session, project: Project) -> tuple[Project, Project]:
    """Mark ``project`` as a clone of github.com/o/r and add another clone of it and a project on other code."""
    s.get_one(Project, project.id).repo = 'github.com/o/r'
    sibling = Project(name='sibling', path='/sibling', repo='github.com/o/r', config=project.config)
    other = Project(name='other', path='/other', repo='github.com/o/elsewhere', config=project.config)
    s.add_all([sibling, other])
    s.flush()
    return sibling, other


def test_a_brief_opens_with_the_current_lesson_of_each_family_on_the_repo_this_project_first(
    db: Database, project: Project
) -> None:
    with db.session() as s:
        assert lessons_overview(s, s.get_one(Project, project.id), 400) == '', 'no lessons, no block'
        sibling, other = clones(s, project)
        lesson(s, sibling.id, 'voting', 'dead')
        lesson(s, project.id, 'voting', 'promising')
        newer = lesson(s, sibling.id, 'voting', 'promising')
        lesson(s, other.id, 'voting', 'dead')
        mine = lesson(s, project.id, 'calibration', 'dead')
        s.commit()
        text = lessons_overview(s, s.get_one(Project, project.id), 400)
        assert text.splitlines()[4:] == [
            f'- [demo] family: calibration | status: dead | best: +1 score (evaluator#1) (comment#{mine})',
            '- [demo] family: voting | status: promising | best: +1 score (evaluator#1) (comment#2)',
            f'- [sibling] family: voting | status: promising | best: +1 score (evaluator#1) (comment#{newer})',
        ], 'the newest per (project, family); never another repo; this project first, then newest'
        assert text.startswith('# What past campaigns on this repo concluded')
        assert lessons_overview(s, s.get_one(Project, project.id), 0) == '', '0 turns it off'
        short = lessons_overview(s, s.get_one(Project, project.id), 90).splitlines()
        assert short[-1] == '… 2 more: the lessons query under "Remember past campaigns" in the procedures'


def test_fit_keeps_the_lines_that_fit_in_the_budget() -> None:
    assert fit(['a' * 7, 'b' * 7, 'c'], 4) == (['a' * 7, 'b' * 7], 1), 'each line costs len // 4 + 1 tokens'
    assert fit([], 0) == ([], 0)


def test_wake_prints_the_journal_newest_lines_in_order_the_inbox_the_active_heads_and_the_lessons(
    db: Database, project: Project
) -> None:
    with db.session() as s:
        assert orchestrator_overview(s, 0, NOW) == ''
        assert orchestrator_overview(s, 2000, NOW).splitlines()[2:] == [
            '## Journal, oldest to newest (0 lines in all)',
            '- none',
            '',
            '## Inbox (0 open)',
            '- none',
            '',
            '## Active projects (1)',
            '- demo · head score 1 (candidate#1) · 1 kept / 1 candidates',
            '',
            '## Current lessons on their repos (0)',
            '- none',
        ]
        sibling, _ = clones(s, project)
        s.get_one(Project, sibling.id).status = ProjectStatus.STOPPED
        lesson(s, sibling.id, 'voting', 'dead')
        for i in range(40):
            s.add(Journal(author='orchestrator', line=f'step {i:02d}: ' + 'x' * 60, created_at=NOW))
        s.add(
            Comment(
                target_kind=TargetKind.PROJECT,
                target_id=project.id,
                author='w',
                body='Raise the floor?',
                kind=CommentKind.ESCALATION,
                priority=1,
            )
        )
        s.commit()
        lines = orchestrator_overview(s, 1000, NOW).splitlines()
    assert lines[0] == '# piloop wake, 2026-10-12 21:00 UTC'
    journal = lines[
        lines.index('## Journal, oldest to newest (40 lines in all)') + 1 : lines.index('## Inbox (1 open)') - 1
    ]
    assert journal[0] == '… 25 older: SELECT created_at, author, line FROM journal ORDER BY id DESC'
    assert [line[:31] for line in journal[1:3]] == [
        '- 2026-10-12 21:00 orchestrator',
        '- 2026-10-12 21:00 orchestrator',
    ]
    assert journal[1].split(': ', 1)[1].startswith('step 25') and journal[-1].split(': ', 1)[1].startswith('step 39')
    assert '- p1 comment#2 escalation on project#1 by w (demo): Raise the floor?' in lines
    active = lines[lines.index('## Active projects (2)') + 1 : lines.index('## Current lessons on their repos (1)') - 1]
    assert [line.split(' · ')[0] for line in active] == ['- demo', '- other'], 'the stopped sibling is not active'
    assert lines[-1] == '- [sibling] family: voting | status: dead | best: +1 score (evaluator#1) (comment#1)', (
        "a stopped project's lessons count when it shares a repo with an active one"
    )
