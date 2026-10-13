"""Tests for the research pack: the context that a research sweep or a progress review reads in its brief."""

import os
from pathlib import Path

from inline_snapshot import snapshot
from sqlalchemy.orm import Session

from piloop.agents.roles import files_of
from piloop.campaign.ideas import Yield, source, yields
from piloop.config import Files, ProjectConfig, load_project_config
from piloop.db.models import Comment, CommentKind, IdeaStatus, Project, TargetKind, Verdict
from piloop.db.session import Database
from piloop.reports.pack import (
    ARTIFACTS_SHOWN,
    DIGESTS_SHOWN,
    ESCALATIONS_SHOWN,
    FINDINGS_SHOWN,
    OUTLINE_CHARS,
    SEEDS_SHOWN,
    WORKSPACE_SHOWN,
    pack,
)
from tests.reports.test_history import comment, compact, idea, scored

JAN_1 = 1_767_225_600
"""A fixed modification time for artifacts (2026-01-01 00:00 UTC), so their order and dates are stable."""


def configure(db: Database, project: Project, **research: object) -> None:
    with db.session() as s:
        row = s.get_one(Project, project.id)
        row.config = load_project_config(Path(project.path), roles={'research': research}).model_dump(mode='json')
        s.commit()


def write(path: Path, text: str, minute: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    os.utime(path, (JAN_1 + 60 * minute, JAN_1 + 60 * minute))


def render(db: Database, project: Project, workspace: Path | None) -> str:
    """Render the pack with the Rich padding collapsed and the repo path shortened to ``<repo>``."""
    with db.session() as s:
        row = s.get_one(Project, project.id)
        text = pack(s, row, ProjectConfig.model_validate(row.config), workspace=workspace)
    return '\n'.join(compact(line) for line in text.replace(project.path, '<repo>').splitlines()) + '\n'


def note(s: Session, project: Project, author: str, kind: CommentKind, body: str, **fields: object) -> Comment:
    row = Comment(target_kind=TargetKind.PROJECT, target_id=project.id, author=author, kind=kind, body=body, **fields)
    s.add(row)
    s.flush()
    return row


def campaign(db: Database, project: Project, tmp_path: Path) -> Path:
    """Add ideas from four sources, a keep, a discard, proposals, findings, digests, escalations and artifacts."""
    repo = Path(project.path)
    write(
        repo / 'docs' / 'losses.md', '# Where we lose\n## By slice\n```\n# not a heading\n```\n#12 is no heading\n', 2
    )
    write(repo / 'docs' / 'old-losses.md', 'no headings here\n', 1)
    write(repo / 'docs' / 'eval.json', '{"a": 1}', 3)
    configure(db, project, context=['docs/*losses.md', 'docs/eval.json', 'reports/*.md'])
    workspace = tmp_path / 'workspace'
    write(workspace / 'research' / 'losses-c2.md', '# Losses of candidate#2\n', 5)
    with db.session() as s:
        kept = idea(s, project, 'more lines', family='appending', created_by='orchestrator', priority=1)
        e2 = scored(s, kept, 1, (Verdict.KEEP, {'score': 3, 'size': 12}, None))
        worse = idea(s, project, 'fewer lines', family='appending', created_by='research#4')
        scored(s, worse, e2.id, (Verdict.DISCARD, {'score': 2, 'size': 8}, None))
        odd = idea(s, project, 'odd lines', created_by='research#4')
        scored(s, odd, e2.id, (Verdict.INTERESTING, {'score': 3, 'size': 9}, None))
        idea(s, project, 'pad lines', IdeaStatus.SEED, family='padding', created_by='watchdog#7', provenance='run#1')
        idea(s, project, 'ask the human', IdeaStatus.PROPOSED, created_by='human')
        idea(s, project, 'parked lines', IdeaStatus.PARKED, family='padding', created_by='watchdog#3')
        comment(s, TargetKind.IDEA, worse.id, 'NEGATIVE: fewer lines lose score\nbecause score counts lines')
        note(s, project, 'orchestrator', CommentKind.FINDING, 'digest: best #2 +2 over the start\nby slice: …')
        note(s, project, 'human', CommentKind.DECISION, 'no external data')
        note(s, project, 'research#4', CommentKind.FINDING, 'batch 1: rejected deleting')
        note(s, project, 'daemon', CommentKind.FINDING, 'not a digest')
        note(s, project, 'watchdog#3', CommentKind.ESCALATION, 'is the metric right?', priority=1)
        acked = note(s, project, 'watchdog#3', CommentKind.ESCALATION, 'answered already', priority=2)
        acked.acked_at, acked.acked_by = acked.created_at, 'orchestrator'
        s.commit()
    return workspace


def test_the_pack_of_a_campaign(db: Database, project: Project, tmp_path: Path) -> None:
    workspace = campaign(db, project, tmp_path)
    text = render(db, project, workspace)
    head, rest = text.split('## History: ideas', 1)
    _, tail = rest.split('## History: research findings (last 10)\n', 1)
    assert head == snapshot("""\
## Research pack: metrics (confidence 0.95; start candidate#1, head candidate#2)

| metric | role | better | margin | floor | start | head | gain |
|---|---|---|---|---|---|---|---|
| score | primary | higher | — | — | 1 | 3 | +2 |
| size | watch | lower | — | — | 5 | 12 | -7 |


## Research pack: ideas by source (keeps, not ideas, measure a source)

| source | ideas | ran | keep | interesting |
|---|---|---|---|---|
| orchestrator | 1 | 1 | 1 | 0 |
| research | 2 | 2 | 0 | 1 |
| watchdog | 2 | 0 | 0 | 0 |
| human | 1 | 0 | 0 | 0 |


## Research pack: analysis artifacts (read before proposing; newest first)
- <repo>/docs/eval.json (8 bytes, modified 2026-01-01 00:03 UTC)
- <repo>/docs/losses.md (6 lines, modified 2026-01-01 00:02 UTC)
  # Where we lose · ## By slice
- <repo>/docs/old-losses.md (1 lines, modified 2026-01-01 00:01 UTC)
  (no headings)
- reports/*.md: matches no file in <repo>

## Research pack: shared workspace (what agents left in ./workspace/; newest first)
- ./workspace/research/losses-c2.md (1 lines, modified 2026-01-01 00:05 UTC)
  # Losses of candidate#2

## Research pack: what was tried, by mechanism family (ideas.family)

| family | ideas | keep | interesting | discard/crash | not run | ideas and outcomes |
|---|---|---|---|---|---|---|
| appending | 2 | 1 | 0 | 1 | 0 | #2 discard, #1 keep |
| padding | 2 | 0 | 0 | 0 | 2 | #6 parked, #4 seed |
| — | 2 | 0 | 1 | 0 | 1 | #5 proposed, #3 interesting |


## Research pack: seeds to develop (oldest first)
- idea#4 by watchdog#7: pad lines — pointers: run#1

""")
    assert tail == snapshot("""\
- research#4: batch 1: rejected deleting

## Research pack: latest worker findings (newest first)
- comment#1 worker#1 on idea#2: NEGATIVE: fewer lines lose score
  because score counts lines

## Research pack: orchestrator and human digests and decisions (newest first)
- comment#3 decision by human on project#1: no external data
- comment#2 finding by orchestrator on project#1: digest: best #2 +2 over the start
  by slice: …

## Research pack: open escalations (unacknowledged, newest first)
- comment#6 p1 by watchdog#3 on project#1: is the metric right?
""")
    assert '| #2 | #1 | 3 | +2 | idea#1 (orchestrator) |' in rest, (
        'the lineage shows the gain of each keep and who proposed its idea'
    )


def test_an_idea_whose_commits_were_all_skipped_is_unmeasured_not_a_dead_end(
    db: Database, project: Project, tmp_path: Path
) -> None:
    with db.session() as s:
        scaffold = idea(s, project, 'scaffold only', family='scaffolding', created_by='research#4')
        scored(s, scaffold, 1, (Verdict.UNMEASURED, {}, None))
        s.commit()
    assert f'| scaffolding | 1 | 0 | 0 | 0 | 0 | #{scaffold.id} unmeasured |' in render(db, project, None)


def test_an_empty_pack_says_what_is_missing(db: Database, project: Project, tmp_path: Path) -> None:
    text = render(db, project, tmp_path / 'no-such-dir')
    assert '- None configured: `[roles.research] context` in piloop.toml lists them.' in text
    assert 'shared workspace' not in text, 'no section without a workspace directory'
    assert '## Research pack: seeds to develop (oldest first)\n- None.' in text
    for title, empty in [
        ('latest worker findings', '- None yet.'),
        ('digests and decisions', '- None yet.'),
        ('open escalations', '- None.'),
    ]:
        assert f'{title} (newest first)\n{empty}' in text or f'{title} (unacknowledged, newest first)\n{empty}' in text
    (tmp_path / 'workspace').mkdir()
    assert '(what agents left in ./workspace/; newest first)\n- Empty.' in render(db, project, tmp_path / 'workspace')


def test_the_pack_is_bounded(db: Database, project: Project, tmp_path: Path) -> None:
    repo = Path(project.path)
    for n in range(ARTIFACTS_SHOWN + 2):
        write(repo / 'docs' / f'a{n:02}.md', '\n'.join(f'## heading {k} ' + 'x' * 60 for k in range(40)), n)
    workspace = tmp_path / 'workspace'
    for n in range(WORKSPACE_SHOWN + 3):
        write(workspace / f'w{n:02}.txt', 'x', n)
    configure(db, project, context=['docs/*.md'])
    with db.session() as s:
        for n in range(max(SEEDS_SHOWN, FINDINGS_SHOWN) + 2):
            row = idea(s, project, f'idea {n} ' + 't' * 200, IdeaStatus.SEED, provenance='p' * 500, family='f' * 80)
            comment(s, TargetKind.IDEA, row.id, '\n'.join(['w' * 79] * 30))
        for _ in range(DIGESTS_SHOWN + 2):
            note(s, project, 'orchestrator', CommentKind.FINDING, '\n'.join(['d' * 79] * 30))
        for _ in range(ESCALATIONS_SHOWN + 2):
            note(s, project, 'watchdog#1', CommentKind.ESCALATION, '\n'.join(['e' * 79] * 30), priority=2)
        s.commit()
    text = render(db, project, workspace)
    assert '- … and 2 older files.' in text and '- … and 3 older files.' in text
    assert f'<repo>/docs/a{ARTIFACTS_SHOWN + 1:02}.md' in text and '<repo>/docs/a00.md' not in text, 'newest first'
    outlines = [line for line in text.splitlines() if line.startswith('  ## heading 0')]
    assert len(outlines) == ARTIFACTS_SHOWN and all(len(line) == OUTLINE_CHARS + 2 for line in outlines)
    assert text.count('- idea#') == SEEDS_SHOWN
    assert '- idea#1 by t: idea 0 ' in text, 'oldest first: research develops the seeds in the order they arrived'
    assert text.count(' worker#1 on idea#') == FINDINGS_SHOWN
    assert text.count(' finding by orchestrator on project#') == DIGESTS_SHOWN
    assert text.count(' p2 by watchdog#1 on project#') == ESCALATIONS_SHOWN
    pack_only = text.split('## History: ideas', 1)[0] + text.split('## History: research findings (last 10)', 1)[1]
    assert len(pack_only) < 45_000, 'the module docstring promises about 45 KB on top of the history'


def test_yields_count_ideas_runs_keeps_and_interesting_per_source(db: Database, project: Project) -> None:
    with db.session() as s:
        a = idea(s, project, 'a', created_by='research#1')
        c = scored(s, a, 1, (Verdict.KEEP, {'score': 2, 'size': 1}, None), (Verdict.INTERESTING, {'score': 2}, None))
        scored(s, a, c.id, (Verdict.KEEP, {'score': 3, 'size': 1}, None))
        idea(s, project, 'b', created_by='research#2')
        d = idea(s, project, 'd', created_by='orchestrator')
        scored(s, d, 1, (Verdict.INTERESTING, {'score': 1}, None))
        s.commit()
        assert yields(s, project.id) == [
            Yield('research', ideas=2, ran=1, keeps=2, interesting=1),
            Yield('orchestrator', ideas=1, ran=1, keeps=0, interesting=1),
        ]
    assert Yield('worker', 3, 2, 1, 0).line() == 'worker 3 ideas, 2 ran → 1 keep, 0 interesting'


def test_where_an_idea_comes_from_says_whether_it_may_be_more_than_a_seed(project_path: Path) -> None:
    assert [source(a) for a in ('research#12', 'orchestrator', 'human', 'worker#3')] == [
        'research',
        'orchestrator',
        'human',
        'worker',
    ]
    actors = ('worker#3', 'watchdog#4', 'scout#6', 'research#5', 'orchestrator', 'human')
    cfg = load_project_config(project_path)
    assert [files_of(cfg, a) for a in actors] == [Files.NOTHING, Files.SEEDS, Files.SEEDS, Files.IDEAS, None, None]
