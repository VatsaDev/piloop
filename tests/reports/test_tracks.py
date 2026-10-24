"""Tests for how status, watch, the history and the research pack show a campaign with two tracks."""

from inline_snapshot import snapshot

from piloop.actor import System
from piloop.config import CampaignConfig
from piloop.db.models import Campaign, Candidate, Run
from piloop.db.session import Database
from piloop.ids import TrackName
from piloop.plugins.verdict_rule.contract import Outcome
from piloop.reports.history import history
from piloop.reports.pack import pack
from piloop.watch.panels import PANELS, Context, campaigns, header
from tests.conftest import reconfigured
from tests.reports.test_overview import NOW, candidate, live_campaign, render

TRACKS = {
    'metrics': [
        {'name': 'score', 'direction': 'higher', 'role': 'primary', 'margin': 1, 'floor': 0.5},
        {'name': 'size', 'direction': 'lower', 'role': 'primary', 'margin': 2, 'floor': 20},
    ],
    'tracks': {'score': {'share': 0.75}, 'size': {'share': 0.25}},
}
"""Two tracks named after the metrics of the test campaign, so the ideas of ``campaign`` (on ``score``) still fit."""


def two_tracks(db: Database, campaign: Campaign) -> CampaignConfig:
    """Add ``campaign``, then a trunk keep on ``size``, and put the campaign on two tracks."""
    live_campaign(db, campaign)
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        cfg = reconfigured(p.config, **TRACKS)
        p.config = cfg
        lighter = Run(idea_id=3, attempt=2, base_candidate_id=2, harness='script', model='m')
        s.add(lighter)
        s.flush()
        s.get_one(Candidate, 1).measurements[0].metrics = {'score': 1.0, 'size': 12.0}
        kept = candidate(s, p, 2, Outcome.KEEP, 3, 60, run_id=lighter.id)
        kept.measurements[0].metrics = {'score': 3.0, 'size': 8.0}
        lighter.idea.track = TrackName('size')
        s.commit()
    return cfg


def test_status_names_every_track_its_head_its_floor_and_what_works_on_it(db: Database, campaign: Campaign) -> None:
    two_tracks(db, campaign)
    lay = render(db, campaign).split('### Now')[0]
    assert lay == snapshot("""\
## Campaign overview (computed at 2026-10-01 14:00 UTC)
demo · head candidate#4: score 3 (+2), size 8 (-4) · 3 kept / 4 candidates · 4k tok · evaluator #1, 0s old
### Goal
- Tracks, each a primary its keeps improve while holding the others: score (score, higher, margin 1, floor 0.5, share 0.75); size (size, lower, margin 2, floor 20, share 0.25)
- Campaign: active. Progress: run 3 · 1.9h (no limit stops it: open-ended).
- The goal in words is the campaign's brief.md: weigh the metric against it.
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
    recent = render(db, campaign).split('### Recent candidates')[1].split('### Anomalies')[0]
    assert recent == snapshot("""\
 (4 of 4; each on its track's primary, vs parent)
- candidate#1 keep [score] 1 (—), no run, 0s ago: baseline
- candidate#2 keep [score] 3 (+2), run#1, 0s ago: score 3
- candidate#3 discard [score] 3 (+0), run#1, 0s ago: score 3
- candidate#4 keep [size] 8 (-2), run#4, 0s ago: score 3
""")


def test_history_and_pack_show_each_track(db: Database, campaign: Campaign) -> None:
    cfg = two_tracks(db, campaign)
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        text = history(s, p, cfg)
        packed = pack(s, p, cfg, workspace=None)
    assert text.splitlines()[2] == snapshot(
        "| id | status    | track | by         | family | links                        | prio | title                 | best candidate (its track's primary)     |"
    )
    assert [line for line in text.splitlines() if line.startswith(('| candidate', '| #4'))] == snapshot(
        [
            "| candidate | parent | track | score | size | vs parent (its track's primary) | idea (by)           | commit  | tokens | summary  |",
            '| #4        | #2     | size  | 3     | 8    | -2                              | idea#3 (research#4) | 0000060 | —      | score 3  |',
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


def test_watch_shows_the_head_per_track(db: Database, campaign: Campaign) -> None:
    cfg = two_tracks(db, campaign)
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        assert header(s, p, cfg, NOW).plain == snapshot("""\
demo · head candidate#4: score 3 (+2), size 8 (-4) · 3 kept / 4 candidates · 4k tok
run 3 · 1.9h
- score: score 3 at the head (+2 on the start); floor 0.5, 2.5 to spare; 1 trunk keeps, the latest candidate#2 1h ago; share 0.75 of 1 slots, 0 running; 0 runnable ideas.
- size: size 8 at the head (-4 on the start); floor 20, 12 to spare; 1 trunk keeps, the latest candidate#4 1h ago; share 0.25 of 1 slots, 2 running; 0 runnable ideas.\
""")
        [row] = campaigns(s, NOW, System.ORCHESTRATOR)
        assert row.cells[2:4] == snapshot(('3 score, 8 size', '+2, -4'))
        ctx = Context(p, cfg, NOW, System.ORCHESTRATOR)
        panel = PANELS['candidates']
        items = panel.items(s, ctx)
        assert panel.title(s, ctx, items) == snapshot('Candidates score ▁██ 1 → 3 · size █▅▁ 12 → 8')


def test_lineages_show_every_track_primary_of_their_latest_keep(db: Database, campaign: Campaign) -> None:
    two_tracks(db, campaign)
    with db.session() as s:
        branch = Run(idea_id=1, attempt=2, base_candidate_id=1, harness='script', model='m', branch=True)
        s.add(branch)
        s.flush()
        candidate(s, s.get_one(Campaign, campaign.id), 1, Outcome.KEEP, 2, 70, run_id=branch.id)
        s.commit()
    lines = [
        line for line in render(db, campaign).splitlines() if line.startswith('  - ') and 'from candidate#' in line
    ]
    assert lines == snapshot(
        [
            '  - trunk from candidate#1 (keep): latest candidate#4 score 3, size 8, 3 kept',
            '  - branch from candidate#1 (keep): latest candidate#5 score 2, size 10, 1 kept',
        ]
    )
