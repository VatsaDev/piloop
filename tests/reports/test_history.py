"""Tests for the history that an agent reads in its brief and that ``piloop history`` prints."""

from inline_snapshot import snapshot
from sqlalchemy.orm import Session

from piloop.config import CampaignConfig
from piloop.db.models import (
    AgentSession,
    Campaign,
    Candidate,
    Comment,
    CommentKind,
    Idea,
    IdeaLink,
    IdeaStatus,
    LinkKind,
    Measurement,
    Run,
    RunStatus,
    Verdict,
)
from piloop.db.session import Database
from piloop.ids import IMPLEMENTER, TargetKind, TargetRef
from piloop.reports.history import (
    FINDING_CHARS,
    FINDINGS_IN_HISTORY,
    IDEAS_IN_HISTORY,
    block,
    history,
)
from piloop.text import CELL_CHARS, cell


def idea(
    s: Session,
    campaign: Campaign,
    title: str,
    status: IdeaStatus = IdeaStatus.DONE,
    hypothesis: str | None = None,
    *,
    after: tuple[int, ...] = (),
    supersedes: tuple[int, ...] = (),
    **fields: object,
) -> Idea:
    row = Idea(
        track='score',
        campaign_id=campaign.id,
        title=title,
        hypothesis=hypothesis or f'{title} helps',
        status=status,
        links=[
            *(IdeaLink(target_id=t, kind=LinkKind.AFTER) for t in after),
            *(IdeaLink(target_id=t, kind=LinkKind.SUPERSEDES) for t in supersedes),
        ],
        **{'created_by': 't', **fields},
    )
    s.add(row)
    s.flush()
    return row


def scored(
    s: Session,
    row: Idea,
    base: int,
    *outcomes: tuple[Verdict, dict[str, float], str | None],
    checkpoint: str | None = None,
    tokens: int | None = None,
) -> Candidate:
    """Add one run of ``row`` from candidate ``base`` with one measured candidate per outcome. Return the last."""
    run = Run(idea=row, attempt=len(row.runs) + 1, base_candidate_id=base, harness='h', model='m')
    run.status = RunStatus.SUCCEEDED
    s.add(run)
    s.flush()
    usage = {'total': tokens, 'turns': 3, 'source': '/x.jsonl'} if tokens is not None else None
    s.add(
        AgentSession(
            kind=IMPLEMENTER, campaign_id=row.campaign_id, run_id=run.id, checkpoint_id=checkpoint, usage=usage
        )
    )
    evaluator_id = row.campaign.evaluator_id
    assert evaluator_id is not None
    parent = base
    for n, (verdict, metrics, error) in enumerate(outcomes):
        c = Candidate(
            campaign_id=row.campaign_id,
            run_id=run.id,
            parent_id=parent,
            tree_sha=f'{row.id}-{run.id}-{n}',
            commit_sha=f'{row.id}{run.id}{n}0000deadbeef',
            summary=f'{row.title} commit' if error is None else None,
            verdict=verdict,
        )
        s.add(c)
        s.flush()
        s.add(Measurement(candidate_id=c.id, evaluator_id=evaluator_id, metrics=metrics, error=error))
        parent = c.id
    return c


def comment(s: Session, kind: TargetKind, target: int, body: str, author: str = 'implementer#1') -> None:
    s.add(Comment(target=TargetRef(kind, target), author=author, kind=CommentKind.FINDING, body=body))


def render(db: Database, campaign: Campaign) -> str:
    """Render the history with the Rich column padding collapsed, so that assertions match the rows."""
    return '\n'.join(compact(line) for line in raw(db, campaign).splitlines()) + '\n'


def raw(db: Database, campaign: Campaign) -> str:
    with db.session() as s:
        return history(s, s.get_one(Campaign, campaign.id), CampaignConfig.model_validate(campaign.config))


def compact(line: str) -> str:
    if not line.startswith('|'):
        return line
    if set(line) <= {'|', '-', ' '}:
        return '|' + '---|' * (line.count('|') - 1)
    return '| ' + ' | '.join(c.strip() for c in line.strip('|').split('|')) + ' |'


def test_history_lists_every_idea_with_its_outcome_the_lineage_and_the_findings(
    db: Database, campaign: Campaign
) -> None:
    with db.session() as s:
        s.get_one(Candidate, 1).commit_sha = 'ba5e11ae0000'
        kept = idea(s, campaign, 'more lines', priority=1, family='appending', created_by='research#9')
        e2 = scored(
            s,
            kept,
            1,
            (Verdict.KEEP, {'score': 3, 'size': 12}, None),
            checkpoint='01M29FSXJHMJ9X718SBKJ1K0E2',
            tokens=234_444,
        )
        comment(s, TargetKind.IDEA, kept.id, 'appending works | keep appending\nsecond line is not shown')
        worse = idea(s, campaign, 'fewer lines', family='appending', created_by='implementer#4')
        scored(s, worse, e2.id, (Verdict.DISCARD, {'score': 2, 'size': 8}, None))
        crashed = idea(s, campaign, 'delete solve.txt', IdeaStatus.REJECTED, priority=3)
        scored(s, crashed, e2.id, (Verdict.CRASH, {}, 'no such file'))
        comment(s, TargetKind.IDEA, crashed.id, 'first attempt: evaluate needs the file')
        comment(s, TargetKind.IDEA, crashed.id, 'second attempt agrees; rejected', author='watchdog#2')
        mixed = idea(s, campaign, 'two commits', after=(kept.id,))
        scored(s, mixed, e2.id, (Verdict.CRASH, {}, 'syntax'), (Verdict.KEEP, {'score': 4, 'size': 16}, None))
        idea(s, campaign, 'a' * 200, IdeaStatus.READY, 'h' * 200, priority=0, after=(crashed.id, kept.id))
        idea(s, campaign, 'human should look', IdeaStatus.PROPOSED, 'the metric | is wrong\nsecond line')
        idea(s, campaign, 'fewer but longer lines', IdeaStatus.READY, supersedes=(worse.id,), after=(mixed.id, kept.id))
        idea(s, campaign, 'no lines at all', IdeaStatus.READY, supersedes=(worse.id, crashed.id))
        comment(
            s, TargetKind.CAMPAIGN, campaign.id, 'batch 1: extend appending\nrejected: deleting', author='research#1'
        )
        comment(s, TargetKind.CAMPAIGN, campaign.id, 'idle: best is #2; try longer lines', author='watchdog#2')
        comment(s, TargetKind.CAMPAIGN, campaign.id, 'batch 2: nothing new worth proposing', author='research#3')
        comment(s, TargetKind.CAMPAIGN, campaign.id, 'the cache keep holds on fresh data', author='reviewer#7')
        comment(s, TargetKind.CAMPAIGN, campaign.id, 'digest: on track', author='orchestrator')
        s.commit()
    assert render(db, campaign) == snapshot("""\
## History: ideas (8 of 8)

| id | status | by | family | links | prio | title | best candidate (score) |
|---|---|---|---|---|---|---|---|
| 1 | done | research#9 | appending | — | 1 | more lines | keep 3 (+2 vs candidate#1) (candidate#2) |
| 2 | done | implementer#4 | appending | superseded by idea#7, idea#8 | 2 | fewer lines | discard 2 (-1 vs candidate#2) (candidate#3) |
| 3 | rejected | t | — | superseded by idea#8 | 3 | delete solve.txt | crash (candidate#4) |
| 4 | done | t | — | after idea#1 | 2 | two commits | keep 4 (+1 vs candidate#2) (candidate#6) |
| 5 | ready | t | — | blocked by idea#3 (rejected); after idea#1 | 0 | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa… | — |
| 6 | proposed | t | — | — | 2 | human should look | — |
| 7 | ready | t | — | after idea#1, idea#4; supersedes idea#2 | 2 | fewer but longer lines | — |
| 8 | ready | t | — | supersedes idea#2, idea#3 | 2 | no lines at all | — |


## History: kept lineage

| candidate | parent | score | vs parent | idea (by) | commit | checkpoint | tokens | summary |
|---|---|---|---|---|---|---|---|---|
| #1 | — | 1 | — | — | ba5e11a | — | — | baseline |
| #2 | #1 | 3 | +2 | idea#1 (research#9) | 1100000 | 01M29FSXJHMJ9X718SBKJ1K0E2 | 234k | more lines commit |
| #6 | #5 | 4 | — | idea#4 (t) | 4410000 | — | — | two commits commit |


## History: findings on the campaign (last 10)
- research#1: batch 1: extend appending
  rejected: deleting
- watchdog#2: idle: best is #2; try longer lines
- research#3: batch 2: nothing new worth proposing
- reviewer#7: the cache keep holds on fresh data
""")


def test_history_is_bounded_and_counts_what_it_omits(db: Database, campaign: Campaign) -> None:
    with db.session() as s:
        for n in range(IDEAS_IN_HISTORY + 3):
            idea(s, campaign, f'idea {n}', IdeaStatus.READY)
        for n in range(FINDINGS_IN_HISTORY + 2):
            comment(s, TargetKind.CAMPAIGN, campaign.id, f'finding {n}', author='research#1')
        s.commit()
    rendered = render(db, campaign)
    rows = [line for line in rendered.splitlines() if line.startswith('| ') and line.split(' | ')[0][2:].isdigit()]
    assert rendered.startswith(f'## History: ideas ({IDEAS_IN_HISTORY} of {IDEAS_IN_HISTORY + 3}; 3 older omitted)')
    assert [r.split(' | ')[0] for r in rows[:2]] == ['| 4', '| 5'], 'the newest ideas are kept, the oldest dropped'
    assert len(rows) == IDEAS_IN_HISTORY
    findings = [line for line in rendered.splitlines() if line.startswith('- research#1: ')]
    assert findings[0] == '- research#1: finding 2' and len(findings) == FINDINGS_IN_HISTORY


def test_history_worst_case_stays_under_the_documented_size(db: Database, campaign: Campaign) -> None:
    with db.session() as s:
        previous: Idea | None = None
        for n in range(IDEAS_IN_HISTORY):
            links = (previous.id,) if previous is not None else ()
            row = idea(s, campaign, f'{n} ' + 't' * 200, IdeaStatus.REJECTED, 'h' * 200, after=links, supersedes=links)
            scored(s, row, 1, (Verdict.DISCARD, {'score': 0.123456, 'size': 1}, None))
            comment(s, TargetKind.IDEA, row.id, 'f' * 200)
            previous = row
        for _ in range(FINDINGS_IN_HISTORY):
            comment(s, TargetKind.CAMPAIGN, campaign.id, '\n'.join(['r' * 79] * 30), author='research#1')
        s.commit()
    rendered = render(db, campaign)
    assert 'blocked by idea#58 (rejected); supersedes idea#58; superseded by idea#60' in rendered, (
        'every link, both ways'
    )
    assert len(raw(db, campaign)) < 64_000
    assert len(rendered) < 27_000


def test_history_when_a_human_kept_an_unscored_commit_and_an_idea_builds_on_it(
    db: Database, campaign: Campaign
) -> None:
    with db.session() as s:
        unscored = Candidate(
            campaign_id=campaign.id, tree_sha='y', commit_sha='y', verdict=Verdict.KEEP, verdict_by='human:t'
        )
        s.add(unscored)
        s.flush()
        row = idea(s, campaign, 'from an unscored base')
        scored(s, row, unscored.id, (Verdict.KEEP, {'score': 5, 'size': 1}, None))
        s.commit()
    rendered = render(db, campaign)
    assert '| 1 | done | t | — | — | 2 | from an unscored base | keep 5 (candidate#3) |' in rendered, (
        'no delta without a base score'
    )
    assert '| #2 | — | — | — | — | y | — | — | — |' in rendered, 'the unscored base is in the lineage without a value'
    assert '| #3 | #2 | 5 | — | idea#1 (t) |' in rendered, 'no gain over a parent without a score'


def test_cell_is_one_bounded_line() -> None:
    assert cell('  \n') == '—'
    assert cell('a | b\nc') == 'a / b'
    assert len(cell('x' * 500)) == CELL_CHARS
    assert len(cell('x' * 500, 40)) == 40


def test_block_keeps_whole_lines_and_counts_the_rest() -> None:
    assert block('  \n') == '—'
    assert block('a\nb\n\nc') == 'a\nb\n\nc'
    assert block('a\n' + 'b' * 10 + '\nc\nd', 8) == 'a\n… (3 more lines)'
    assert block('x' * 500 + '\ny', 8) == 'xxxxxxx…\n… (1 more lines)'
    assert block('\n'.join(['r' * 79] * 30)) == '\n'.join(['r' * 79] * 10) + '\n… (20 more lines)'
    assert len(block('x' * 2000)) == FINDING_CHARS
