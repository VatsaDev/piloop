"""Tests for the history that an agent reads in its brief and that ``piloop history`` prints."""

from inline_snapshot import snapshot
from sqlalchemy.orm import Session

from piloop.config import ProjectConfig
from piloop.db.models import (
    Candidate,
    Comment,
    CommentKind,
    Idea,
    IdeaLink,
    IdeaStatus,
    LinkKind,
    Measurement,
    Project,
    Run,
    RunStatus,
    TargetKind,
    Verdict,
    Worker,
    WorkerKind,
)
from piloop.db.session import Database
from piloop.reports.history import (
    FINDING_CHARS,
    FINDINGS_IN_HISTORY,
    HYPOTHESIS_CHARS,
    IDEAS_IN_HISTORY,
    block,
    history,
)
from piloop.text import CELL_CHARS, cell


def idea(
    s: Session,
    project: Project,
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
        project_id=project.id,
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
        Worker(kind=WorkerKind.WORKER, project_id=row.project_id, run_id=run.id, checkpoint_id=checkpoint, usage=usage)
    )
    evaluator_id = row.project.evaluator_id
    assert evaluator_id is not None
    parent = base
    for n, (verdict, metrics, error) in enumerate(outcomes):
        c = Candidate(
            project_id=row.project_id,
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


def comment(s: Session, kind: TargetKind, target: int, body: str, author: str = 'worker#1') -> None:
    s.add(Comment(target_kind=kind, target_id=target, author=author, kind=CommentKind.FINDING, body=body))


def render(db: Database, project: Project) -> str:
    """Render the history with the Rich column padding collapsed, so that assertions match the rows."""
    return '\n'.join(compact(line) for line in raw(db, project).splitlines()) + '\n'


def raw(db: Database, project: Project) -> str:
    with db.session() as s:
        return history(s, s.get_one(Project, project.id), ProjectConfig.model_validate(project.config))


def compact(line: str) -> str:
    if not line.startswith('|'):
        return line
    if set(line) <= {'|', '-', ' '}:
        return '|' + '---|' * (line.count('|') - 1)
    return '| ' + ' | '.join(c.strip() for c in line.strip('|').split('|')) + ' |'


def test_history_lists_every_idea_with_its_outcome_the_lineage_and_the_findings(db: Database, project: Project) -> None:
    with db.session() as s:
        s.get_one(Candidate, 1).commit_sha = 'ba5e11ae0000'
        kept = idea(s, project, 'more lines', priority=1, family='appending', created_by='research#9')
        e2 = scored(
            s,
            kept,
            1,
            (Verdict.KEEP, {'score': 3, 'size': 12}, None),
            checkpoint='01M29FSXJHMJ9X718SBKJ1K0E2',
            tokens=234_444,
        )
        comment(s, TargetKind.IDEA, kept.id, 'appending works | keep appending\nsecond line is not shown')
        worse = idea(s, project, 'fewer lines', family='appending', created_by='worker#4')
        scored(s, worse, e2.id, (Verdict.DISCARD, {'score': 2, 'size': 8}, None))
        crashed = idea(s, project, 'delete solve.txt', IdeaStatus.REJECTED, priority=3)
        scored(s, crashed, e2.id, (Verdict.CRASH, {}, 'no such file'))
        comment(s, TargetKind.IDEA, crashed.id, 'first attempt: evaluate needs the file')
        comment(s, TargetKind.IDEA, crashed.id, 'second attempt agrees; rejected', author='watchdog#2')
        mixed = idea(s, project, 'two commits', after=(kept.id,))
        scored(s, mixed, e2.id, (Verdict.CRASH, {}, 'syntax'), (Verdict.KEEP, {'score': 4, 'size': 16}, None))
        idea(s, project, 'a' * 200, IdeaStatus.READY, 'h' * 200, priority=0, after=(crashed.id, kept.id))
        idea(s, project, 'human should look', IdeaStatus.PROPOSED, 'the metric | is wrong\nsecond line')
        idea(s, project, 'fewer but longer lines', IdeaStatus.READY, supersedes=(worse.id,), after=(mixed.id, kept.id))
        idea(s, project, 'no lines at all', IdeaStatus.READY, supersedes=(worse.id, crashed.id))
        comment(s, TargetKind.PROJECT, project.id, 'batch 1: extend appending\nrejected: deleting', author='research#1')
        comment(s, TargetKind.PROJECT, project.id, 'idle: best is #2; try longer lines', author='watchdog#2')
        comment(s, TargetKind.PROJECT, project.id, 'batch 2: nothing new worth proposing', author='research#3')
        s.commit()
    assert render(db, project) == snapshot("""\
## History: ideas (8 of 8)

| id | status | by | family | links | prio | title | hypothesis | best candidate (score) | worker finding |
|---|---|---|---|---|---|---|---|---|---|
| 1 | done | research#9 | appending | — | 1 | more lines | more lines helps | keep 3 (+2 vs candidate#1) (candidate#2) | appending works / keep appending |
| 2 | done | worker#4 | appending | superseded by idea#7, idea#8 | 2 | fewer lines | fewer lines helps | discard 2 (-1 vs candidate#2) (candidate#3) | — |
| 3 | rejected | t | — | superseded by idea#8 | 3 | delete solve.txt | delete solve.txt helps | crash (candidate#4) | second attempt agrees; rejected |
| 4 | done | t | — | after idea#1 | 2 | two commits | two commits helps | keep 4 (+1 vs candidate#2) (candidate#6) | — |
| 5 | ready | t | — | blocked by idea#3 (rejected); after idea#1 | 0 | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa… | hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh… | — | — |
| 6 | proposed | t | — | — | 2 | human should look | the metric / is wrong | — | — |
| 7 | ready | t | — | after idea#1, idea#4; supersedes idea#2 | 2 | fewer but longer lines | fewer but longer lines helps | — | — |
| 8 | ready | t | — | supersedes idea#2, idea#3 | 2 | no lines at all | no lines at all helps | — | — |


## History: kept lineage

| candidate | parent | score | vs parent | idea (by) | commit | checkpoint | tokens | summary |
|---|---|---|---|---|---|---|---|---|
| #1 | — | 1 | — | — | ba5e11a | — | — | baseline |
| #2 | #1 | 3 | +2 | idea#1 (research#9) | 1100000 | 01M29FSXJHMJ9X718SBKJ1K0E2 | 234k | more lines commit |
| #6 | #5 | 4 | — | idea#4 (t) | 4410000 | — | — | two commits commit |


## History: research findings (last 10)
- research#1: batch 1: extend appending
  rejected: deleting
- research#3: batch 2: nothing new worth proposing
""")


def test_history_is_bounded_and_counts_what_it_omits(db: Database, project: Project) -> None:
    with db.session() as s:
        for n in range(IDEAS_IN_HISTORY + 3):
            idea(s, project, f'idea {n}', IdeaStatus.READY)
        for n in range(FINDINGS_IN_HISTORY + 2):
            comment(s, TargetKind.PROJECT, project.id, f'finding {n}', author='research#1')
        s.commit()
    rendered = render(db, project)
    rows = [line for line in rendered.splitlines() if line.startswith('| ') and line.split(' | ')[0][2:].isdigit()]
    assert rendered.startswith(f'## History: ideas ({IDEAS_IN_HISTORY} of {IDEAS_IN_HISTORY + 3}; 3 older omitted)')
    assert [r.split(' | ')[0] for r in rows[:2]] == ['| 4', '| 5'], 'the newest ideas are kept, the oldest dropped'
    assert len(rows) == IDEAS_IN_HISTORY
    findings = [line for line in rendered.splitlines() if line.startswith('- research#1: ')]
    assert findings[0] == '- research#1: finding 2' and len(findings) == FINDINGS_IN_HISTORY


def test_history_worst_case_stays_under_the_documented_size(db: Database, project: Project) -> None:
    with db.session() as s:
        previous: Idea | None = None
        for n in range(IDEAS_IN_HISTORY):
            links = (previous.id,) if previous is not None else ()
            row = idea(s, project, f'{n} ' + 't' * 200, IdeaStatus.REJECTED, 'h' * 200, after=links, supersedes=links)
            scored(s, row, 1, (Verdict.DISCARD, {'score': 0.123456, 'size': 1}, None))
            comment(s, TargetKind.IDEA, row.id, 'f' * 200)
            previous = row
        for _ in range(FINDINGS_IN_HISTORY):
            comment(s, TargetKind.PROJECT, project.id, '\n'.join(['r' * 79] * 30), author='research#1')
        s.commit()
    rendered = render(db, project)
    assert 'blocked by idea#58 (rejected); supersedes idea#58; superseded by idea#60' in rendered, (
        'every link, both ways'
    )
    assert len(raw(db, project)) < 64_000
    assert len(rendered) < 40_000


def test_history_when_a_human_kept_an_unscored_commit_and_an_idea_builds_on_it(db: Database, project: Project) -> None:
    with db.session() as s:
        unscored = Candidate(
            project_id=project.id, tree_sha='y', commit_sha='y', verdict=Verdict.KEEP, verdict_by='human:t'
        )
        s.add(unscored)
        s.flush()
        row = idea(s, project, 'from an unscored base')
        scored(s, row, unscored.id, (Verdict.KEEP, {'score': 5, 'size': 1}, None))
        s.commit()
    rendered = render(db, project)
    assert (
        '| 1 | done | t | — | — | 2 | from an unscored base | from an unscored base helps | keep 5 (candidate#3) | — |'
        in rendered
    ), 'no delta without a base score'
    assert '| #2 | — | — | — | — | y | — | — | — |' in rendered, 'the unscored base is in the lineage without a value'
    assert '| #3 | #2 | 5 | — | idea#1 (t) |' in rendered, 'no gain over a parent without a score'


def test_cell_is_one_bounded_line() -> None:
    assert cell('  \n') == '—'
    assert cell('a | b\nc') == 'a / b'
    assert len(cell('x' * 500)) == CELL_CHARS
    assert len(cell('x' * 500, HYPOTHESIS_CHARS)) == HYPOTHESIS_CHARS


def test_block_keeps_whole_lines_and_counts_the_rest() -> None:
    assert block('  \n') == '—'
    assert block('a\nb\n\nc') == 'a\nb\n\nc'
    assert block('a\n' + 'b' * 10 + '\nc\nd', 8) == 'a\n… (3 more lines)'
    assert block('x' * 500 + '\ny', 8) == 'xxxxxxx…\n… (1 more lines)'
    assert block('\n'.join(['r' * 79] * 30)) == '\n'.join(['r' * 79] * 10) + '\n… (20 more lines)'
    assert len(block('x' * 2000)) == FINDING_CHARS
