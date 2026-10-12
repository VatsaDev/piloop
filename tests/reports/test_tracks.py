"""Tests for how status, watch, the history and the research pack show a project with two tracks."""

from inline_snapshot import snapshot

from piloop.config import ProjectConfig
from piloop.db.models import Candidate, Project, Run, Verdict
from piloop.db.session import Database
from piloop.reports.history import history
from piloop.reports.pack import pack
from piloop.watch.panels import PANELS, Context, header, projects
from tests.reports.test_overview import NOW, campaign, candidate, render

TRACKS = {
    'metrics': [
        {'name': 'score', 'direction': 'higher', 'role': 'primary', 'margin': 1, 'floor': 0.5},
        {'name': 'size', 'direction': 'lower', 'role': 'primary', 'margin': 2, 'floor': 20},
    ],
    'tracks': {'score': {'share': 0.75}, 'size': {'share': 0.25}},
}
"""Two tracks named after the metrics of the test project, so the ideas of ``campaign`` (on ``score``) still fit."""


def two_tracks(db: Database, project: Project) -> ProjectConfig:
    """Add ``campaign``, then a trunk keep on ``size``, and put the project on two tracks."""
    campaign(db, project)
    with db.session() as s:
        p = s.get_one(Project, project.id)
        cfg = ProjectConfig.model_validate({**p.config, **TRACKS})
        p.config = cfg.model_dump(mode='json')
        lighter = Run(idea_id=3, attempt=2, base_candidate_id=2, harness='script', model='m')
        s.add(lighter)
        s.flush()
        s.get_one(Candidate, 1).measurements[0].metrics = {'score': 1.0, 'size': 12.0}
        kept = candidate(s, p, 2, Verdict.KEEP, 3, 60, run_id=lighter.id)
        kept.measurements[0].metrics = {'score': 3.0, 'size': 8.0}
        lighter.idea.track = 'size'
        s.commit()
    return cfg


def test_status_names_every_track_its_head_its_floor_and_what_works_on_it(db: Database, project: Project) -> None:
    two_tracks(db, project)
    lay = render(db, project).split('### Now')[0]
    assert lay == snapshot("""\
## Lay of the land (computed at 2026-10-01 14:00 UTC)
demo · head candidate#4: score 3.0000 (+2.0000), size 8.0000 (-4.0000) · 3 kept / 4 candidates · 4k tok · evaluator #1, 0s old
### Goal
- Tracks, each a primary its keeps improve while holding the others: score (score, higher, margin 1, floor 0.5, share 0.75); size (size, lower, margin 2, floor 20, share 0.25)
- Campaign: active. Progress: run 3 · 1.9h (no [stop] condition: open-ended).
- The goal in words is the project's brief.md: weigh the metric against it.
### Tracks
- score: score 3 at the head (+2 on the start); floor 0.5, 2.5 to spare; 1 trunk keeps, the latest candidate#2 1h ago; share 0.75 of 1 slots, 0 running; 0 runnable ideas.
- size: size 8 at the head (-4 on the start); floor 20, 12 to spare; 1 trunk keeps, the latest candidate#4 1h ago; share 0.25 of 1 slots, 2 running; 0 runnable ideas.
### Trajectory
- Start candidate#1, head candidate#4: score 1 → 3 (+2, +200.0%); size 12 → 8 (-4, -33.3%); 2 trunk keeps.
- Last trunk keep 1h ago; 0 runs finished since.
- Trunk keeps (latest 8; when first measured, the gain on its track over the head before it):
  - 10-01 12:00 candidate#1 1 (start): baseline
  - 10-01 12:18 candidate#2 [score] score 3 (+2) idea#1: score 3
  - 10-01 13:00 candidate#4 [size] size 8 (-2) idea#3: score 3
""")
    recent = render(db, project).split('### Recent candidates')[1].split('### Anomalies')[0]
    assert recent == snapshot("""\
 (4 of 4; each on its track's primary, vs parent)
- candidate#1 keep [score] 1.0000 (—), no run, 0s ago: baseline
- candidate#2 keep [score] 3.0000 (+2.0000), run#1, 0s ago: score 3
- candidate#3 discard [score] 3.0000 (+0.0000), run#1, 0s ago: score 3
- candidate#4 keep [size] 8.0000 (-2.0000), run#4, 0s ago: score 3
""")


def test_history_and_pack_show_each_track(db: Database, project: Project) -> None:
    cfg = two_tracks(db, project)
    with db.session() as s:
        p = s.get_one(Project, project.id)
        text = history(s, p, cfg)
        packed = pack(s, p, cfg, workspace=None)
    assert text.splitlines()[2] == snapshot(
        "| id | status    | track | by         | family | links                        | prio | title                 | hypothesis            | best candidate (its track's primary)     | worker finding |"
    )
    assert [line for line in text.splitlines() if line.startswith(('| candidate', '| #4'))] == snapshot(
        [
            "| candidate | parent | track | score | size | vs parent (its track's primary) | idea (by)           | commit  | checkpoint | tokens | summary  |",
            '| #4        | #2     | size  | 3     | 8    | -2                              | idea#3 (research#4) | 0000060 | —          | —      | score 3  |',
        ]
    )
    assert [line for line in packed.splitlines() if line.startswith('| metric')] == snapshot(
        ['| metric | role    | track | better | margin | floor | start | head | gain |']
    )
    assert [line for line in packed.splitlines() if line.startswith(('| score', '| size'))] == snapshot(
        [
            '| score  | primary | score | higher | 1      | 0.5   | 1     | 3    | +2   |',
            '| size   | primary | size  | lower  | 2      | 20    | 12    | 8    | +4   |',
        ]
    )


def test_watch_shows_the_head_per_track(db: Database, project: Project) -> None:
    cfg = two_tracks(db, project)
    with db.session() as s:
        p = s.get_one(Project, project.id)
        assert header(s, p, cfg, NOW).plain == snapshot("""\
demo · head candidate#4: score 3.0000 (+2.0000), size 8.0000 (-4.0000) · 3 kept / 4 candidates · 4k tok
run 3 · 1.9h
- score: score 3 at the head (+2 on the start); floor 0.5, 2.5 to spare; 1 trunk keeps, the latest candidate#2 1h ago; share 0.75 of 1 slots, 0 running; 0 runnable ideas.
- size: size 8 at the head (-4 on the start); floor 20, 12 to spare; 1 trunk keeps, the latest candidate#4 1h ago; share 0.25 of 1 slots, 2 running; 0 runnable ideas.\
""")
        [row] = projects(s, NOW, 'orchestrator')
        assert row.cells[2:4] == snapshot(('3.0000 score, 8.0000 size', '+2.0000, -4.0000'))
        ctx = Context(p, cfg, NOW, 'orchestrator')
        panel = PANELS['candidates']
        items = panel.items(s, ctx)
        assert panel.title(s, ctx, items) == snapshot('Candidates score ▁██ 1 → 3 · size █▅▁ 12 → 8')


def test_lineages_show_every_track_primary_of_their_latest_keep(db: Database, project: Project) -> None:
    two_tracks(db, project)
    with db.session() as s:
        branch = Run(idea_id=1, attempt=2, base_candidate_id=1, harness='script', model='m', branch=True)
        s.add(branch)
        s.flush()
        candidate(s, s.get_one(Project, project.id), 1, Verdict.KEEP, 2, 70, run_id=branch.id)
        s.commit()
    lines = [line for line in render(db, project).splitlines() if line.startswith('  - ') and 'from candidate#' in line]
    assert lines == snapshot(
        [
            '  - trunk from candidate#1 (keep): latest candidate#4 score 3, size 8, 3 kept',
            '  - branch from candidate#1 (keep): latest candidate#5 score 2, size 10, 1 kept',
        ]
    )
