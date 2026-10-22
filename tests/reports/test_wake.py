"""Wake overviews: the current lessons that open a brief, and what ``piloop wake`` prints, each in its budget."""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import event
from sqlalchemy.orm import Session

from piloop.config import load_campaign_config
from piloop.db.models import Campaign, CampaignStatus, Comment, CommentKind, Journal, Lesson, LessonStatus
from piloop.db.session import Database
from piloop.ids import TargetKind, TargetRef
from piloop.reports.wake import (
    LESSONS_MORE,
    Relevance,
    Tier,
    cost,
    fit,
    fitted,
    lessons_overview,
    orchestrator_overview,
)

NOW = datetime(2026, 10, 12, 21, 0, tzinfo=UTC)


def lesson(
    s: Session, campaign: int, family: str, status: str, effect: float | None = 1, metric: str | None = 'score'
) -> int:
    row = Lesson(
        campaign_id=campaign,
        family=family,
        status=LessonStatus(status),
        effect=effect,
        metric=metric if effect is not None else None,
        body='why, with ids (evaluator#1)',
        created_by='watchdog#1',
    )
    s.add(row)
    s.flush()
    return row.id


def clones(s: Session, campaign: Campaign) -> tuple[Campaign, Campaign]:
    """Mark ``campaign`` as a clone of github.com/o/r and add another clone of it and a campaign on other code."""
    s.get_one(Campaign, campaign.id).repo = 'github.com/o/r'
    sibling = Campaign(name='sibling', path='/sibling', repo='github.com/o/r', config=campaign.config)
    other = Campaign(name='other', path='/other', repo='github.com/o/elsewhere', config=campaign.config)
    s.add_all([sibling, other])
    s.flush()
    return sibling, other


def test_a_brief_opens_with_one_line_per_family_on_the_repo_from_the_newest_campaign_that_tried_it(
    db: Database, campaign: Campaign
) -> None:
    with db.session() as s:
        assert lessons_overview(s, s.get_one(Campaign, campaign.id), 400) == '', 'no lessons, no block'
        sibling, other = clones(s, campaign)
        lesson(s, sibling.id, 'voting', 'dead')
        newer = lesson(s, sibling.id, 'Voting', 'promising')
        lesson(s, campaign.id, 'voting', 'promising', 9)
        lesson(s, other.id, 'calibration', 'promising', 50)
        mine = lesson(s, campaign.id, 'calibration', 'dead')
        s.commit()
        text = lessons_overview(s, s.get_one(Campaign, campaign.id), 400)
        assert text.splitlines()[4:] == [
            f'- [sibling] Voting | promising | +1 score (lesson#{newer})',
            f'- [demo] calibration | dead | +1 score (lesson#{mine})',
        ], 'the newest lesson of the newest campaign per family, whatever the case; never another repo'
        assert text.startswith('# What past campaigns on this repo concluded')
        assert lessons_overview(s, s.get_one(Campaign, campaign.id), 0) == '', '0 turns it off'
        whole = lessons_overview(s, s.get_one(Campaign, campaign.id), 400)
        short = lessons_overview(s, s.get_one(Campaign, campaign.id), cost(whole.splitlines()) - 1).splitlines()
        assert short[-1] == f'… 2 more: {LESSONS_MORE}', 'the notice takes the place of the line it cannot fit beside'


def test_lessons_rank_dead_or_promising_first_then_the_larger_effect_then_the_newer_campaign(
    db: Database, campaign: Campaign
) -> None:
    with db.session() as s:
        sibling, _ = clones(s, campaign)
        lesson(s, sibling.id, 'big but untested', 'untested', 900)
        lesson(s, campaign.id, 'unmeasured', 'dead', None)
        lesson(s, campaign.id, 'small', 'promising', 2)
        lesson(s, campaign.id, 'faster', 'promising', -40.5, 'seconds')
        lesson(s, campaign.id, 'tied, older campaign', 'dead', 2)
        lesson(s, sibling.id, 'tied, newer campaign', 'dead', 2)
        s.commit()
        lines = lessons_overview(s, s.get_one(Campaign, campaign.id), 1000).splitlines()[4:]
    assert [line.split(' | ')[0] for line in lines] == [
        '- [sibling] tied, newer campaign',
        '- [demo] tied, older campaign',
        '- [demo] small',
        '- [sibling] big but untested',
        '- [demo] unmeasured',
        '- [demo] faster',
    ], "the campaign's metric first; in one campaign, the newer lesson first; seconds are not its metric"


def test_lessons_in_the_campaigns_metrics_outrank_larger_effects_in_other_units(
    db: Database, campaign: Campaign
) -> None:
    """comment#10930: a decoder-accuracy brief opened with eval-time lessons, because seconds outnumber words."""
    metrics = [
        {'name': 'val_correct_at_b', 'direction': 'higher', 'role': 'primary'},
        {'name': 'rep_correct_at_b', 'direction': 'higher', 'role': 'guardrail', 'margin': 40},
        {'name': 'val_ece', 'direction': 'lower', 'role': 'guardrail', 'margin': 0.003},
    ]
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        p.config = load_campaign_config(Path(campaign.path), metrics=metrics).model_dump(mode='json')
        sibling, _ = clones(s, campaign)
        lesson(s, sibling.id, 'cross-tree-replay', 'promising', -1182.8, 'postproc_seconds')
        lesson(s, sibling.id, 'stack-composition', 'promising', -1071.1, 'full_eval_seconds')
        lesson(s, sibling.id, 'exact-compute-cuts', 'promising', -1054.6, 'full_eval_seconds')
        lesson(s, sibling.id, 'disagreement withholding', 'promising', 723, 'unseen_score')
        lesson(s, sibling.id, 'calibration', 'promising', 716, 'Rep_Correct_at_B')
        lesson(s, sibling.id, 'causal acquisition', 'promising', 658, 'unseen_correct_at_500')
        lesson(s, sibling.id, 'plausibility', 'dead', 12, 'val_correct_at_b')
        lesson(s, sibling.id, 'ece trim', 'dead', -0.004, 'val_ece')
        lesson(s, sibling.id, 'never scored', 'dead', None)
        lesson(s, sibling.id, 'binning', 'dead', -0.01, 'test_ece_bins')
        s.commit()
        lines = lessons_overview(s, s.get_one(Campaign, campaign.id), 1000).splitlines()[4:]
    assert [line.split(' | ')[0].removeprefix('- [sibling] ') for line in lines] == [
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
        ' there, the best of each metric before the second of any; metric names compare in any case'
    )


def test_relevance_tiers_a_metric_by_its_name_against_the_campaigns_metrics(campaign: Campaign) -> None:
    metrics = [
        {'name': 'val_correct_at_b', 'direction': 'higher', 'role': 'primary'},
        {'name': 'val_ece', 'direction': 'lower', 'role': 'guardrail', 'margin': 0.003},
    ]
    relevance = Relevance([load_campaign_config(Path(campaign.path), metrics=metrics)])
    assert [
        relevance.tier(m)
        for m in ('val_correct_at_b', 'val_ece', 'rep_correct_at_b', 'test_ece_bins', None, 'val_seconds')
    ] == [Tier.PRIMARY, Tier.MEASURED, Tier.NEAR_PRIMARY, Tier.NEAR_MEASURED, Tier.NO_METRIC, Tier.UNRELATED], (
        '`val` is a split word: it relates nothing'
    )


def test_fit_keeps_the_lines_that_fit_in_the_budget() -> None:
    assert fit(['a' * 7, 'b' * 7, 'c'], 4) == (['a' * 7, 'b' * 7], 1), 'each line costs len // 4 + 1 tokens'
    assert fit([], 0) == ([], 0)


def test_fitted_makes_room_for_its_notice_inside_the_budget() -> None:
    lines = ['a' * 7] * 5
    assert fitted(lines, 10, 'm') == lines, 'all fit: no notice'
    assert fitted(lines, 9, 'm') == ['a' * 7] * 3 + ['… 2 more: m'], 'a line costs 2 and the notice 3: it takes two'
    assert fitted(lines, 2, 'm') == [], 'not even the notice fits'


def test_wake_prints_the_journal_newest_lines_in_order_the_inbox_the_active_heads_and_the_lessons(
    db: Database, campaign: Campaign
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
            '## Active campaigns (1)',
            '- demo · head score 1 (candidate#1) · 1 kept / 1 candidates',
            '',
            '## Current lessons on their repos (0)',
            '- none',
        ]
        sibling, _ = clones(s, campaign)
        s.get_one(Campaign, sibling.id).status = CampaignStatus.STOPPED
        voting = lesson(s, sibling.id, 'voting', 'dead')
        for i in range(40):
            s.add(Journal(author='orchestrator', line=f'step {i:02d}: ' + 'x' * 60, created_at=NOW))
        s.add(
            Comment(
                target=TargetRef(TargetKind.CAMPAIGN, campaign.id),
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
    assert journal[0].endswith(' older: SELECT created_at, author, line FROM journal ORDER BY id DESC')
    older = int(journal[0].split()[1])
    assert [line[:31] for line in journal[1:3]] == [
        '- 2026-10-12 21:00 orchestrator',
        '- 2026-10-12 21:00 orchestrator',
    ]
    assert journal[1].split(': ', 1)[1].startswith(f'step {older:02d}'), 'the newest lines, oldest first'
    assert journal[-1].split(': ', 1)[1].startswith('step 39')
    assert '- p1 comment#1 escalation on campaign#1 by w (demo): Raise the floor?' in lines
    active = lines[
        lines.index('## Active campaigns (2)') + 1 : lines.index('## Current lessons on their repos (1)') - 1
    ]
    assert [line.split(' · ')[0] for line in active] == ['- demo', '- other'], 'the stopped sibling is not active'
    assert lines[-1] == f'- [sibling] voting | dead | +1 score (lesson#{voting})', (
        "a stopped campaign's lessons count when it shares a repo with an active one"
    )


@pytest.mark.parametrize('budget', [*range(0, 120, 7), 200, 500, 1000])
def test_wake_and_a_brief_print_no_more_than_their_budget_headings_and_notices_included(
    db: Database, campaign: Campaign, budget: int
) -> None:
    """Greptile on hiloopai/piloop#7: `piloop wake --budget 1` printed 54 to 97 tokens."""
    with db.session() as s:
        s.get_one(Campaign, campaign.id).repo = 'github.com/o/r'
        for i in range(30):
            lesson(s, campaign.id, f'family {i}', 'dead', i)
            s.add(Journal(author='orchestrator', line=f'step {i}: ' + 'x' * 60, created_at=NOW))
            s.add(
                Comment(
                    target=TargetRef(TargetKind.CAMPAIGN, campaign.id),
                    author='w',
                    body='Raise the floor?',
                    kind=CommentKind.ESCALATION,
                    priority=1,
                )
            )
        s.commit()
        wake = orchestrator_overview(s, budget, NOW).splitlines()
        brief = lessons_overview(s, s.get_one(Campaign, campaign.id), budget).splitlines()
    assert cost(wake) <= budget and cost(brief) <= budget
    if budget >= 200:
        assert len(wake) > 10 and len(brief) > 5, 'a usual budget prints every section'
    if budget == 0:
        assert wake == brief == []


def test_wake_reads_only_the_journal_lines_that_can_fit(db: Database, campaign: Campaign) -> None:
    """Greptile on hiloopai/piloop#7: wake loaded and formatted every journal row to keep a few."""
    with db.session() as s:
        s.add_all(Journal(author='orchestrator', line=f'step {i}', created_at=NOW) for i in range(5000))
        s.commit()
        loaded: list[object] = []
        event.listen(s, 'loaded_as_persistent', lambda _s, obj: loaded.append(obj))
        lines = orchestrator_overview(s, 100, NOW).splitlines()
    journal = [o for o in loaded if isinstance(o, Journal)]
    assert 0 < len(journal) <= 100 * 0.4, 'no more rows than tokens in the journal share'
    assert lines[2] == '## Journal, oldest to newest (5000 lines in all)', 'the total is a count, not a load'


def test_a_value_with_newlines_counts_every_line_it_prints(db: Database, campaign: Campaign) -> None:
    """A family of 600 newlines printed 712 tokens in a 400-token brief."""
    with db.session() as s:
        s.get_one(Campaign, campaign.id).repo = 'github.com/o/r'
        lesson(s, campaign.id, 'multi\n' * 600, 'dead', 1, 'score\n' * 50)
        lesson(s, campaign.id, 'plain', 'dead', 2)
        s.add(Journal(author='orchestrator', line='a\n' * 140, created_at=NOW))
        s.commit()
        brief = lessons_overview(s, s.get_one(Campaign, campaign.id), 400)
        wake = orchestrator_overview(s, 1000, NOW)
    assert cost(brief.split('\n')) <= 400 and cost(wake.split('\n')) <= 1000
    assert '- [demo] plain | dead | +2 score' in brief, 'what fits is still printed'
