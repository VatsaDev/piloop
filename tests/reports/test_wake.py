"""Wake overviews: the current lessons that open a brief, and what ``piloop wake`` prints, each in its budget."""

from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy.orm import Session

from piloop.config import load_project_config
from piloop.db.models import Comment, CommentKind, Journal, Project, ProjectStatus, TargetKind
from piloop.db.session import Database
from piloop.reports.wake import Lesson, Relevance, Tier, fit, lessons_overview, orchestrator_overview, parse

NOW = datetime(2026, 10, 12, 21, 0, tzinfo=UTC)


def lesson(s: Session, project: int, family: str, status: str, best: str = '+1 score') -> int:
    comment = Comment(
        target_kind=TargetKind.PROJECT,
        target_id=project,
        author='watchdog#1',
        kind=CommentKind.LESSON,
        body=f'family: {family} | status: {status} | best: {best} (evaluator#1)\nwhy, with ids',
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


def test_a_brief_opens_with_one_line_per_family_on_the_repo_from_the_newest_campaign_that_tried_it(
    db: Database, project: Project
) -> None:
    with db.session() as s:
        assert lessons_overview(s, s.get_one(Project, project.id), 400) == '', 'no lessons, no block'
        sibling, other = clones(s, project)
        lesson(s, sibling.id, 'voting', 'dead')
        newer = lesson(s, sibling.id, 'Voting', 'promising')
        lesson(s, project.id, 'voting', 'promising', '+9 score')
        lesson(s, other.id, 'calibration', 'promising', '+50 score')
        mine = lesson(s, project.id, 'calibration', 'dead')
        s.commit()
        text = lessons_overview(s, s.get_one(Project, project.id), 400)
        assert text.splitlines()[4:] == [
            f'- [sibling] family: Voting | status: promising | best: +1 score (evaluator#1) (comment#{newer})',
            f'- [demo] family: calibration | status: dead | best: +1 score (evaluator#1) (comment#{mine})',
        ], 'the newest lesson of the newest campaign per family, whatever the case; never another repo'
        assert text.startswith('# What past campaigns on this repo concluded')
        assert lessons_overview(s, s.get_one(Project, project.id), 0) == '', '0 turns it off'
        short = lessons_overview(s, s.get_one(Project, project.id), 130).splitlines()
        assert short[-1] == '… 1 more: the lessons query under "Remember past campaigns" in the procedures'


def test_lessons_rank_dead_or_promising_first_then_the_larger_effect_then_the_newer_campaign(
    db: Database, project: Project
) -> None:
    with db.session() as s:
        sibling, _ = clones(s, project)
        lesson(s, sibling.id, 'big but untested', 'untested', '+900 score')
        lesson(s, project.id, 'unmeasured', 'dead', 'none measured')
        lesson(s, project.id, 'small', 'promising', '+2 score')
        lesson(s, project.id, 'faster', 'promising', '-40.5 seconds')
        lesson(s, project.id, 'tied, older campaign', 'dead', '+2 score')
        lesson(s, sibling.id, 'tied, newer campaign', 'dead', '+2 score')
        s.commit()
        lines = lessons_overview(s, s.get_one(Project, project.id), 1000).splitlines()[4:]
    assert [line.split(' | ')[0] for line in lines] == [
        '- [sibling] family: tied, newer campaign',
        '- [demo] family: tied, older campaign',
        '- [demo] family: small',
        '- [sibling] family: big but untested',
        '- [demo] family: unmeasured',
        '- [demo] family: faster',
    ], "the project's metric first; in one campaign, the newer lesson first; seconds are not its metric"


def test_lessons_in_the_projects_metrics_outrank_larger_effects_in_other_units(db: Database, project: Project) -> None:
    """comment#10930: a decoder-accuracy brief opened with eval-time lessons, because seconds outnumber words."""
    metrics = [
        {'name': 'val_correct_at_b', 'direction': 'higher', 'role': 'primary'},
        {'name': 'rep_correct_at_b', 'direction': 'higher', 'role': 'guardrail', 'margin': 40},
        {'name': 'val_ece', 'direction': 'lower', 'role': 'guardrail', 'margin': 0.003},
    ]
    with db.session() as s:
        p = s.get_one(Project, project.id)
        p.config = load_project_config(Path(project.path), metrics=metrics).model_dump(mode='json')
        sibling, _ = clones(s, project)
        lesson(s, sibling.id, 'cross-tree-replay', 'promising', '-1182.8 postproc_seconds')
        lesson(s, sibling.id, 'stack-composition', 'promising', '-1071.1 full_eval_seconds')
        lesson(s, sibling.id, 'exact-compute-cuts', 'promising', '-1054.6 full_eval_seconds')
        lesson(s, sibling.id, 'disagreement withholding', 'promising', '+723 unseen_score')
        lesson(s, sibling.id, 'calibration', 'promising', '+716 rep_correct_at_b')
        lesson(s, sibling.id, 'causal acquisition', 'promising', '+658 unseen_correct_at_500')
        lesson(s, sibling.id, 'plausibility', 'dead', '+12 val_correct_at_b')
        lesson(s, sibling.id, 'ece trim', 'dead', '-0.004 val_ece')
        lesson(s, sibling.id, 'never scored', 'dead', 'none measured')
        lesson(s, sibling.id, 'binning', 'dead', '-0.01 test_ece_bins')
        s.commit()
        lines = lessons_overview(s, s.get_one(Project, project.id), 1000).splitlines()[4:]
    assert [line.split(' | ')[0].removeprefix('- [sibling] family: ') for line in lines] == [
        'plausibility',
        'ece trim',
        'calibration',
        'causal acquisition',
        'binning',
        'never scored',
        'disagreement withholding',
        'stack-composition',
        'cross-tree-replay',
        'exact-compute-cuts',
    ], (
        'primary, then guardrails, then a metric sharing a word with the primary, then no metric, then the rest:'
        ' there, the best of each metric before the second of any'
    )


def test_relevance_tiers_a_metric_by_its_name_against_the_projects_metrics(project: Project) -> None:
    metrics = [
        {'name': 'val_correct_at_b', 'direction': 'higher', 'role': 'primary'},
        {'name': 'val_ece', 'direction': 'lower', 'role': 'guardrail', 'margin': 0.003},
    ]
    relevance = Relevance([load_project_config(Path(project.path), metrics=metrics)])
    assert [
        relevance.tier(m)
        for m in ('val_correct_at_b', 'val_ece', 'rep_correct_at_b', 'test_ece_bins', None, 'val_seconds')
    ] == [Tier.PRIMARY, Tier.MEASURED, Tier.NEAR_PRIMARY, Tier.NEAR_MEASURED, Tier.NO_METRIC, Tier.UNRELATED], (
        '`val` is a split word: it relates nothing'
    )


def test_parse_reads_the_family_the_status_and_the_size_of_the_first_number_after_best() -> None:
    assert parse('family: A  b | status: dead | best: -53% to -63% ms') == Lesson('a b', False, 53.0, 'ms')
    assert parse('family: a | status: dead | best: +716 Rep_Correct_at_B (evaluator#15)').metric == 'rep_correct_at_b'
    assert parse('family: a | status: dead | best: +3') == Lesson('a', False, 3.0, None)
    assert parse('family: a | status: untested | best: none measured (evaluator#2)') == Lesson('a', True, None)
    assert parse('not a lesson') == Lesson('not a lesson', True, None), 'another shape ranks last'


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
