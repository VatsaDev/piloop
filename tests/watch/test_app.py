"""``piloop watch``: the Textual app driven key by key against a fixture database, and its screens as text."""

import asyncio
import io
import os
import pty
import subprocess
import sys
import threading
import time
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from inline_snapshot import snapshot
from rich.console import Console, RenderableType
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from textual.pilot import Pilot
from textual.widgets import HelpPanel

from piloop.actor import Actor, System
from piloop.campaign import inbox, ops
from piloop.config import CampaignConfig
from piloop.db.models import (
    AgentSession,
    AgentSessionState,
    Campaign,
    CampaignStatus,
    Candidate,
    Comment,
    CommentKind,
    Event,
    Idea,
    IdeaStatus,
    Measurement,
    Origin,
    Run,
    RunEnd,
    RunStatus,
    utcnow,
)
from piloop.db.session import Database
from piloop.ids import IMPLEMENTER, CommentId, IdeaId, TargetKind, TargetRef
from piloop.plugins.verdict_rule.contract import Outcome
from piloop.watch.app import CampaignScreen, CampaignsScreen, CandidatesTable, Watch
from piloop.watch.panels import PANELS, Context, kept_scores
from tests.conftest import RESEARCH, WATCHDOG, reconfigured, spent, wait_until
from tests.reports.test_history import idea


def ago(**delta: float) -> datetime:
    return utcnow() - timedelta(**delta)


STOP = {'max_runs': 50, 'max_hours': 12, 'max_tokens': 5_000_000, 'plateau': 10, 'target': 10}
SIZE = (160, 44)


def screen_text(app: Watch) -> str:
    """The screen as plain text, as a terminal would show it."""
    width, height = app.size
    console = Console(
        width=width, height=height, file=io.StringIO(), record=True, color_system=None, legacy_windows=False
    )
    console.print(app.screen._compositor.render_update(full=True, screen_stack=app._background_screens))
    return '\n'.join(line.rstrip() for line in console.export_text().splitlines()) + '\n'


def drive(
    db: Database,
    ref: str | None,
    scenario: Callable[[Pilot[None], Watch], Awaitable[None]],
    actor: Actor = System.HUMAN,
) -> Watch:
    """Run the app headless on the fixture database and play ``scenario`` against it."""
    mine = Database(str(db.engine.url), actor=actor)
    app = Watch(mine, Path(str(db.engine.url.database)), ref, actor)

    async def run() -> None:
        async with app.run_test(size=SIZE) as pilot:
            await pilot.pause()
            await scenario(pilot, app)

    try:
        asyncio.run(run())
    finally:
        mine.close()
    return app


def campaign_screen(app: Watch) -> CampaignScreen:
    screen = app.screen
    assert isinstance(screen, CampaignScreen)
    return screen


@pytest.fixture
def live_campaign(db: Database, campaign: Campaign, tmp_path: Path) -> Campaign:
    """A running campaign: kept candidates, two live agents, a queue, an inbox, a stopped twin."""
    log_dir = tmp_path / 'research-log'
    log_dir.mkdir()
    (log_dir / 'transcript.txt').write_text('reading the history\nproposing idea B\n')
    implementer_log = tmp_path / 'implementer-log'
    implementer_log.mkdir()
    (implementer_log / 'agent_session.stdout').write_text('hello from the agent\n')
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        p.config = reconfigured(p.config, plugins={'stop_rule': STOP})
        s.get_one(Candidate, 1).created_at = ago(hours=3)
        done = idea(s, p, 'more lines', IdeaStatus.DONE, created_at=ago(hours=3))
        run = Run(
            idea=done,
            attempt=1,
            base_candidate_id=1,
            harness='pi',
            model='m',
            status=RunStatus.SUCCEEDED,
            end=RunEnd.FINISHED,
            started_at=ago(hours=2),
            finished_at=ago(hours=1),
        )
        s.add(run)
        s.flush()
        for n, (verdict, value, minutes) in enumerate(
            [(Outcome.KEEP, 3, 90), (Outcome.DISCARD, 2, 70), (Outcome.KEEP, 4, 60)], start=2
        ):
            s.add(
                Candidate(
                    id=n,
                    campaign_id=p.id,
                    run_id=run.id,
                    parent_id=n - 1,
                    tree_sha=f't{n}',
                    revision='x',
                    summary=f'[wip] step {n}',
                    verdict=verdict,
                    created_at=ago(minutes=minutes),
                )
            )
            s.flush()
            s.add(Measurement(candidate_id=n, evaluator_id=1, metrics={'score': value, 'size': 1}, seconds=2))
        live = idea(s, p, 'longer lines', IdeaStatus.SCHEDULED, created_at=ago(minutes=20))
        live_run = Run(idea=live, attempt=1, base_candidate_id=4, harness='pi', model='m', status=RunStatus.RUNNING)
        s.add(live_run)
        s.flush()
        idea(s, p, 'shorter lines', IdeaStatus.READY, priority=1, created_at=ago(minutes=5))
        idea(s, p, 'soft voting', IdeaStatus.PROPOSED, priority=2, created_at=ago(minutes=4))
        s.add_all(
            [
                AgentSession(
                    kind=IMPLEMENTER,
                    campaign_id=p.id,
                    run_id=live_run.id,
                    harness='pi',
                    model='m',
                    runtime='subprocess',
                    handle={'pid': 1, 'start_time': 0, 'log_dir': str(implementer_log)},
                    state=AgentSessionState.RUNNING,
                    started_at=ago(minutes=3),
                ),
                AgentSession(
                    kind=RESEARCH,
                    campaign_id=p.id,
                    harness='claude',
                    model='opus',
                    state=AgentSessionState.STARTING,
                    reason='low_water: 1 ready < 10',
                    log_dir=str(log_dir),
                    started_at=ago(minutes=10, seconds=30),  # "10m" for 30 s: "40s" became "41s" on a loaded host
                ),
                AgentSession(
                    kind=WATCHDOG,
                    campaign_id=p.id,
                    state=AgentSessionState.EXITED,
                    usage=spent(1_234_567, 9),
                ),
            ]
        )
        comment = dict(author='watchdog#3', created_at=ago(minutes=30))
        s.add_all(
            [
                Comment(
                    target=TargetRef(TargetKind.RUN, run.id),
                    kind=CommentKind.ESCALATION,
                    priority=1,
                    acked_at=ago(minutes=20),
                    acked_by='pi',
                    body='keep #2?',
                    **comment,
                ),
                Comment(
                    target=TargetRef(TargetKind.RUN, run.id),
                    author='pi',
                    kind=CommentKind.DECISION,
                    priority=2,
                    body='Keep #2: the floor holds.\nDetails follow.',
                    created_at=ago(minutes=20),
                ),
                Comment(
                    target=TargetRef(TargetKind.CAMPAIGN, p.id),
                    kind=CommentKind.ESCALATION,
                    priority=1,
                    body='Codex cannot start: trust dialog. Environment or harness?\nSecond line.',
                    **comment,
                ),
                Comment(target=TargetRef(TargetKind.CAMPAIGN, p.id), kind=CommentKind.NOTE, body='a note', **comment),
                Comment(
                    target=TargetRef(TargetKind.RUN, run.id),
                    author='human',
                    priority=2,
                    body='Why keep it?\nRe: comment#2',
                    created_at=ago(minutes=10),
                ),
            ]
        )
        other = Campaign(
            name='other',
            path=str(tmp_path / 'other'),
            config=p.config,
            status=CampaignStatus.STOPPED,
            stopped_reason='max_runs: 50 runs, and more words than the column holds',
        )
        s.add(other)
        s.flush()
        s.add(Candidate(campaign_id=other.id, tree_sha='o1', revision='o'))
        s.commit()
        for e in s.scalars(select(Event)):  # trigger-written events are stamped to the second
            e.at = ago(minutes=1)
        s.commit()
    return campaign


def comment(s: object, comment_id: CommentId) -> Comment:
    assert isinstance(s, Session)
    return s.get_one(Comment, comment_id)


def test_the_campaigns_screen_opens_a_campaign_and_esc_comes_back(db: Database, live_campaign: Campaign) -> None:
    shots: list[str] = []

    async def scenario(pilot: Pilot[None], app: Watch) -> None:
        shots.append(screen_text(app))
        await pilot.press('enter')
        await pilot.pause()
        shots.append(screen_text(app))
        await pilot.press('escape', 'down', 'enter')
        await pilot.pause()
        shots.append(screen_text(app))

    drive(db, None, scenario)
    assert shots[0].splitlines()[:5] == snapshot(
        [
            ' piloop: 2 campaigns',
            '',
            ' campaign  status                                    head     vs baseline  agents    ready    inbox    tokens    last event',
            ' demo      active                                    4 score  +3           2 agents  1 ready  2 inbox  1.2M tok  comment.note 1m ago',
            ' other     stopped: max_runs: 50 runs, and more wo…  —        —            0 agents  0 ready  0 inbox  —         —',
        ]
    )
    assert shots[1] == snapshot("""\
 demo · head score 4 (candidate#4, +3 vs baseline) · 3 kept / 4 candidates · 1.2M tok
 run 2/50 · 2.0h/12h · 1.2M/5.0M tok · 0/10 runs without a keep · target ≥ 10 (head 4)
╭─ [1] Inbox 2 ────────────────────────────────────────────────────────────────────────╮╭─ [0] inbox ──────────────────────────────────────────────────────────╮
│ p1  escalation  watchdog#3  30m  Codex cannot start: trust dialog. Environment or har││ comment#3 escalation on campaign#1 by watchdog#3, 30m ago · p1,      │
│ p2  decision    pi          20m  Keep #2: the floor holds.                           ││ unacknowledged                                                       │
╰──────────────────────────────────────────────────────────────────────────────────────╯│                                                                      │
╭─ [2] Agents 2 ───────────────────────────────────────────────────────────────────────╮│ Codex cannot start: trust dialog. Environment or harness?            │
│ #1  implementer  running   pi/m         3m   idea#2 longer lines                     ││ Second line.                                                         │
│ #2  research     starting  claude/opus  10m  low_water: 1 ready < 10                 ││                                                                      │
╰──────────────────────────────────────────────────────────────────────────────────────╯│                                                                      │
╭─ [3] Ideas 1 proposed · 1 ready · 1 scheduled ───────────────────────────────────────╮│                                                                      │
│ P2  proposed   #4  soft voting    —                                                  ││                                                                      │
│ P1  ready      #3  shorter lines  —                                                  ││                                                                      │
│ P2  scheduled  #2  longer lines   —                                                  ││                                                                      │
╰──────────────────────────────────────────────────────────────────────────────────────╯│                                                                      │
╭─ [4] Candidates ▁▆█ 1 → 4 ───────────────────────────────────────────────────────────╮│                                                                      │
│ #4  keep     4  +2  run#1  1h  [wip] step 4                                          ││                                                                      │
│ #3  discard  2  -1  run#1  1h  [wip] step 3                                          ││                                                                      │
│ #2  keep     3  +2  run#1  1h  [wip] step 2                                          ││                                                                      │
│ #1  keep     1  —   —      3h  baseline                                              ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
╰──────────────────────────────────────────────────────────────────────────────────────╯╰──────────────────────────────────────────────────────────────────────╯
 a ack  A ack all shown  r reply  / filter  esc campaigns  q quit  ? keys
""")
    assert shots[2].splitlines()[:2] == snapshot(
        [
            ' other · no trunk keep · 0 kept / 1 candidates · stopped: max_runs: 50 runs, and more words than the column holds',
            ' run 0/50 · 0.0h/12h · 0/5.0M tok · 0/10 runs without a keep · target ≥ 10 (head —)',
        ]
    )


def test_a_campaign_not_registered_yet_is_waited_for(db: Database) -> None:
    shots: list[str] = []

    async def scenario(pilot: Pilot[None], app: Watch) -> None:
        shots.append(screen_text(app).splitlines()[0])

    drive(db, '/nowhere/yet', scenario)
    assert shots == [" no campaign '/nowhere/yet' yet: `piloop init` registers it, and this view follows"]


def test_inbox_keys_acknowledge_one_item_or_every_item_shown(db: Database, live_campaign: Campaign) -> None:
    notes: list[str] = []

    async def scenario(pilot: Pilot[None], app: Watch) -> None:
        await pilot.press('down')
        await pilot.pause()
        assert 'Keep #2: the floor holds.' in screen_text(app).split('[0] inbox')[1], 'the detail follows the cursor'
        await pilot.press('a')
        await pilot.pause()
        notes.append(campaign_screen(app).note)
        assert '[1] Inbox 1' in screen_text(app)
        await pilot.press('A')
        await pilot.pause()
        notes.append(campaign_screen(app).note)
        await pilot.press('a', 'A')

    drive(db, 'demo', scenario)
    assert notes == ['comment#2 acknowledged', 'comment#3 acknowledged']
    with db.session() as s:
        assert [(c.id, c.acked_by) for c in s.scalars(select(Comment).where(Comment.acked_at.is_not(None)))] == [
            (1, 'pi'),
            (2, 'human'),
            (3, 'human'),
        ]


def test_r_replies_to_the_thread_root_with_a_priority_that_wakes_the_orchestrator(
    db: Database, live_campaign: Campaign
) -> None:
    notes: list[str] = []

    async def scenario(pilot: Pilot[None], app: Watch) -> None:
        await pilot.press('down', 'r')
        await pilot.pause()
        await pilot.press(*'Revert it', 'tab', 'enter', 'down', 'enter', 'shift+tab', 'enter')
        await pilot.pause()
        notes.append(campaign_screen(app).note)
        await pilot.press('r', 'escape')
        await pilot.pause()
        notes.append(campaign_screen(app).note)
        await pilot.press('r', 'enter')
        await pilot.pause()
        notes.append(campaign_screen(app).note)

    with db.session() as s:
        cursor = s.scalar(select(func.max(Event.id))) or 0
    drive(db, 'demo', scenario)
    assert notes == ['sent comment#6 (p3) in reply to comment#2', 'reply dropped', 'reply dropped']
    with db.session() as s:
        sent = comment(s, CommentId(6))
        assert (sent.target_kind, sent.target_id, sent.author, sent.kind, sent.priority, sent.body) == (
            TargetKind.COMMENT,
            2,
            'human',
            CommentKind.DECISION,
            3,
            'Revert it',
        )
        assert inbox.news(s, cursor, campaign_id=live_campaign.id, me=System.ORCHESTRATOR)[1] == [
            'comment#6 decision p3 on comment#2 (demo) by human: Revert it'
        ]


def test_slash_filters_the_focused_panel_and_esc_clears_it(db: Database, live_campaign: Campaign) -> None:
    shots: list[str] = []

    async def scenario(pilot: Pilot[None], app: Watch) -> None:
        await pilot.press('slash', *'keep', 'enter')
        await pilot.pause()
        shots.append(screen_text(app))
        await pilot.press('slash', 'escape')
        await pilot.pause()
        shots.append(screen_text(app))

    drive(db, 'demo', scenario)
    assert '[1] Inbox 1 /keep' in shots[0] and 'Codex cannot start' not in shots[0].split('[2] Agents')[0]
    assert '[1] Inbox 2 ─' in shots[1]


def test_the_agents_panel_shows_the_selected_agents_output_and_its_state(db: Database, live_campaign: Campaign) -> None:
    shots: list[str] = []

    async def scenario(pilot: Pilot[None], app: Watch) -> None:
        await pilot.press('2')
        await pilot.pause()
        shots.append(screen_text(app))
        await pilot.press('down')
        await pilot.pause()
        shots.append(screen_text(app).split('[0] agents')[1])

    drive(db, 'demo', scenario)
    assert shots[0] == snapshot("""\
 demo · head score 4 (candidate#4, +3 vs baseline) · 3 kept / 4 candidates · 1.2M tok
 run 2/50 · 2.0h/12h · 1.2M/5.0M tok · 0/10 runs without a keep · target ≥ 10 (head 4)
╭─ [1] Inbox 2 ────────────────────────────────────────────────────────────────────────╮╭─ [0] agents ─────────────────────────────────────────────────────────╮
│ p1  escalation  watchdog#3  30m  Codex cannot start: trust dialog. Environment or har││ implementer-1 · pi/m · running 3m · subprocess                       │
│ p2  decision    pi          20m  Keep #2: the floor holds.                           ││ on: idea#2 longer lines                                              │
╰──────────────────────────────────────────────────────────────────────────────────────╯│                                                                      │
╭─ [2] Agents 2 ───────────────────────────────────────────────────────────────────────╮│ the last 1 lines it wrote                                            │
│ #1  implementer  running   pi/m         3m   idea#2 longer lines                     ││ hello from the agent                                                 │
│ #2  research     starting  claude/opus  10m  low_water: 1 ready < 10                 ││                                                                      │
╰──────────────────────────────────────────────────────────────────────────────────────╯│                                                                      │
╭─ [3] Ideas 1 proposed · 1 ready · 1 scheduled ───────────────────────────────────────╮│                                                                      │
│ P2  proposed   #4  soft voting    —                                                  ││                                                                      │
│ P1  ready      #3  shorter lines  —                                                  ││                                                                      │
│ P2  scheduled  #2  longer lines   —                                                  ││                                                                      │
╰──────────────────────────────────────────────────────────────────────────────────────╯│                                                                      │
╭─ [4] Candidates ▁▆█ 1 → 4 ───────────────────────────────────────────────────────────╮│                                                                      │
│ #4  keep     4  +2  run#1  1h  [wip] step 4                                          ││                                                                      │
│ #3  discard  2  -1  run#1  1h  [wip] step 3                                          ││                                                                      │
│ #2  keep     3  +2  run#1  1h  [wip] step 2                                          ││                                                                      │
│ #1  keep     1  —   —      3h  baseline                                              ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
│                                                                                      ││                                                                      │
╰──────────────────────────────────────────────────────────────────────────────────────╯╰──────────────────────────────────────────────────────────────────────╯
 s steer  / filter  esc campaigns  q quit  ? keys
""")
    assert 'proposing idea B' in shots[1], "a headless agent's saved transcript"


def test_s_steers_a_running_agent(db: Database, live_campaign: Campaign) -> None:
    notes: list[str] = []

    async def scenario(pilot: Pilot[None], app: Watch) -> None:
        async def note(*keys: str) -> None:
            await pilot.press(*keys)
            await pilot.pause()
            notes.append(campaign_screen(app).note)

        await note('2', 's', *'try the cheaper model', 'enter')
        await note('s', 'escape', 'down', 's', *'go', 'enter')

    drive(db, 'demo', scenario)
    with db.session() as s:
        steered = s.scalars(select(Comment).where(Comment.target_kind == TargetKind.AGENT_SESSION)).one()
        assert (steered.target_id, steered.kind, steered.body) == (1, CommentKind.NOTE, 'try the cheaper model')
    assert notes == [
        f'comment#{steered.id}: queued for agent_session#1',
        'agent_session#2 is not running: nothing to steer',
    ], 'the daemon delivers the note'


def test_ideas_keys_approve_park_reject_with_a_reason_and_move_the_priority(
    db: Database, live_campaign: Campaign
) -> None:
    notes: list[str] = []

    async def scenario(pilot: Pilot[None], app: Watch) -> None:
        async def note(*keys: str) -> None:
            await pilot.press(*keys)
            await pilot.pause()
            notes.append(campaign_screen(app).note)

        await note('3', 'y', *'worth a run', 'enter')
        await note('y')
        await note('p', 'escape')
        await note('p', *'later', 'enter')
        await note('plus')
        await note('minus', 'minus', 'minus', 'minus')
        await note('x', *'never', 'enter')

    drive(db, 'demo', scenario)
    assert notes == [
        'idea#4 proposed → ready',
        'idea#4 is ready already',
        'idea#4 left ready: no reason given',
        'idea#4 ready → parked',
        'idea#4 priority 1',
        'idea#4 priority 4',
        'idea#4 parked → rejected',
    ]
    with db.session() as s:
        assert s.get_one(Idea, 4).status is IdeaStatus.REJECTED
        assert [c.body for c in s.scalars(select(Comment).where(Comment.target_kind == TargetKind.IDEA))] == [
            'proposed → ready: worth a run',
            'ready → parked: later',
            'parked → rejected: never',
        ]
        assert {e.actor for e in s.scalars(select(Event).where(Event.kind == 'idea.status'))} == {'human'}


def test_a_review_that_another_client_made_meanwhile_is_refused_with_why(db: Database, live_campaign: Campaign) -> None:
    notes: list[str] = []

    async def scenario(pilot: Pilot[None], app: Watch) -> None:
        await pilot.press('3', 'y')
        await pilot.pause()
        with db.session() as s:
            ops.set_idea_status(
                s, IdeaId(4), IdeaStatus.READY, reason='approved from the CLI', actor=System.ORCHESTRATOR
            )
            s.commit()
        await pilot.press(*'worth a run', 'enter')
        await pilot.pause()
        notes.append(campaign_screen(app).note)

    drive(db, 'demo', scenario)
    assert notes == ['idea#4 is ready already']


def test_a_paused_campaign_says_so_in_its_row_and_its_header(db: Database, live_campaign: Campaign) -> None:
    with db.session() as s:
        ops.pause(s, live_campaign.id, evals=False, reason='evaluator v2')
        s.commit()
    shots: list[str] = []

    async def scenario(pilot: Pilot[None], app: Watch) -> None:
        shots.append(screen_text(app))
        await pilot.press('enter')
        await pilot.pause()
        shots.append(screen_text(app))

    drive(db, None, scenario)
    assert shots[0].splitlines()[3].startswith(' demo      paused (launches): evaluator v2  ')
    assert shots[1].splitlines()[0].endswith(' · paused (launches): evaluator v2')


def test_a_seed_heads_the_ideas_panel_and_is_developed_by_research_not_approved_by_a_key(
    db: Database, live_campaign: Campaign
) -> None:
    with db.session() as s:
        seed = Idea(
            track='score',
            campaign_id=live_campaign.id,
            title='a hunch',
            hypothesis='a hunch',
            status=IdeaStatus.SEED,
            created_by='watchdog#9',
            provenance='run#1',
        )
        s.add(seed)
        s.commit()
        seed_id = seed.id
    notes: list[str] = []

    async def scenario(pilot: Pilot[None], app: Watch) -> None:
        async def note(*keys: str) -> None:
            await pilot.press(*keys)
            await pilot.pause()
            notes.append(campaign_screen(app).note)

        await note('3', 'y')
        await note('x', *'a duplicate of idea#4', 'enter')

    drive(db, 'demo', scenario)
    assert notes == [
        f'idea#{seed_id} is a seed: research defines it first (piloop idea develop {seed_id} …), or park or reject it',
        f'idea#{seed_id} seed → rejected',
    ]


def test_the_ideas_and_candidates_details(db: Database, live_campaign: Campaign) -> None:
    shots: list[str] = []

    async def scenario(pilot: Pilot[None], app: Watch) -> None:
        await pilot.press('3', 'down', 'down')
        await pilot.pause()
        shots.append(screen_text(app).split('[0] ideas')[1])
        await pilot.press('4')
        await pilot.pause()
        shots.append(screen_text(app).split('[0] candidates')[1])

    drive(db, 'demo', scenario)
    assert [line.split('│')[-2].rstrip() for line in shots[0].splitlines()[1:12]] == snapshot(
        [
            ' idea#2 longer lines',
            ' status scheduled · priority 2 · by t 20m ago · links: —',
            '',
            ' hypothesis:',
            ' longer lines helps',
            '',
            ' runs (1):',
            ' - run#2 running pi/m',
            '',
            ' comments (0):',
            '',
        ]
    )
    assert [line.split('│')[-2].rstrip() for line in shots[1].splitlines()[1:12]] == snapshot(
        [
            ' candidate#4',
            ' verdict keep by —: —',
            ' parent candidate#3 · run #1 · revision x · 1h ago',
            '',
            ' metrics under evaluator#1 (vs parent):',
            '   score 4 (+2)',
            '   size 1 (+0)',
            '',
            ' summary:',
            ' [wip] step 4',
            '',
        ]
    )


def test_imported_references_are_listed_apart_as_starting_points(db: Database, live_campaign: Campaign) -> None:
    with db.session() as s:
        s.add(
            Candidate(
                campaign_id=live_campaign.id,
                tree_sha='r1',
                revision='r',
                summary='paper repro',
                origin=Origin.REFERENCE,
                created_at=utcnow() - timedelta(hours=2),  # a fixed age: "0s" became "1s" on a slow machine
            )
        )
        s.commit()
    shots: list[str] = []

    async def scenario(pilot: Pilot[None], app: Watch) -> None:
        shots.append(screen_text(app))

    drive(db, 'demo', scenario)
    candidates = shots[0].split('[4] ')[1].splitlines()
    assert candidates[0].startswith('Candidates ▁▆█ 1 → 4 · 1 starting point ─')
    assert [line.split('│')[1].rstrip() for line in candidates[1:6]] == snapshot(
        [
            ' #4  keep       4  +2  run#1  1h  [wip] step 4',
            ' #3  discard    2  -1  run#1  1h  [wip] step 3',
            ' #2  keep       3  +2  run#1  1h  [wip] step 2',
            ' #1  keep       1  —   —      3h  baseline',
            ' #6  reference  —  —   —      2h  paper repro',
        ]
    )


def test_question_mark_shows_the_panels_keys_and_0_focuses_the_detail(db: Database, live_campaign: Campaign) -> None:
    async def scenario(pilot: Pilot[None], app: Watch) -> None:
        await pilot.press('question_mark')
        await pilot.pause()
        assert app.screen.query(HelpPanel)
        await pilot.press('question_mark')
        await pilot.pause()
        assert not app.screen.query(HelpPanel)
        await pilot.press('3', '0')
        await pilot.pause()
        assert app.focused is not None and app.focused.id == 'main'
        assert '[0] ideas' in screen_text(app), 'the detail keeps the last panel focused'
        await pilot.press('q')

    drive(db, 'demo', scenario)


def test_an_empty_campaign_takes_every_key_without_acting(db: Database, live_campaign: Campaign) -> None:
    async def scenario(pilot: Pilot[None], app: Watch) -> None:
        assert isinstance(app.focused, CandidatesTable), 'an empty inbox does not take the focus'
        await pilot.press('1', 'a', 'A', 'r', '2', 'enter', 's', '3', 'y', 'plus')
        await pilot.pause()
        assert campaign_screen(app).note == ''
        assert ' nothing here' in screen_text(app)

    drive(db, 'other', scenario)


def test_the_screen_follows_the_database_and_a_dialog_holds_it_still(
    db: Database, live_campaign: Campaign, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_file = Path(str(db.engine.url.database))

    async def scenario(pilot: Pilot[None], app: Watch) -> None:
        with db.session() as s:
            s.add(Comment(target=TargetRef(TargetKind.CAMPAIGN, live_campaign.id), author='w', priority=0, body='new'))
            s.commit()
        app.poll()
        await pilot.pause()
        assert '[1] Inbox 3' in screen_text(app), 'a write is drawn on the next poll'
        await pilot.press('r')
        await pilot.pause()
        os.utime(db_file, ns=(0, 1))
        app.poll()
        await pilot.press('escape', 'escape')
        await pilot.pause()
        monkeypatch.setattr('piloop.watch.app.REFRESH_SECONDS', 0)
        app.poll()
        await pilot.pause()
        assert 'piloop: 2 campaigns' in screen_text(app), 'the campaigns screen re-reads too'
        app.poll()

    drive(db, 'demo', scenario)


def test_details_of_rows_gone_or_bare(db: Database, live_campaign: Campaign) -> None:
    with db.session() as s:
        other = s.get_one(Campaign, 2)
        ctx = Context(other, CampaignConfig.model_validate(other.config), utcnow(), System.HUMAN)
        s.add(
            Idea(
                track='score',
                campaign_id=2,
                title='with a plan',
                hypothesis='h',
                plan='step one\nstep two',
                created_by='t',
            )
        )
        s.commit()
        assert text(PANELS['inbox'].detail(s, ctx, 99)) == 'comment#99 is gone\n'
        assert 'plan:\nstep one\nstep two\n' in text(PANELS['ideas'].detail(s, ctx, 5))
        assert 'metrics under — (vs parent):\n  —\n' in text(PANELS['candidates'].detail(s, ctx, 5))


def text(renderable: RenderableType) -> str:
    console = Console(width=100, file=io.StringIO(), record=True, color_system=None)
    console.print(renderable)
    return console.export_text()


def test_the_campaigns_screen_without_a_campaign_opens_nothing(db: Database) -> None:
    async def scenario(pilot: Pilot[None], app: Watch) -> None:
        await pilot.press('enter')
        await pilot.pause()
        assert isinstance(app.screen, CampaignsScreen) and 'piloop: 0 campaigns' in screen_text(app)

    drive(db, None, scenario)


def test_a_reply_typed_in_the_watch_wakes_the_orchestrators_inbox_wait(
    db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    """End to end in an isolated PILOOP_HOME: `piloop watch` in a terminal, `piloop inbox wait` as the orchestrator."""
    with db.session() as s:
        s.add(
            Comment(
                target=TargetRef(TargetKind.CAMPAIGN, campaign.id),
                author='watchdog#3',
                kind=CommentKind.ESCALATION,
                priority=1,
                body='Codex cannot start: trust dialog.',
            )
        )
        s.commit()
        cursor = s.scalar(select(func.max(Event.id)))
    env: dict[str, str] = {**os.environ, 'PILOOP_HOME': str(tmp_path), 'HOME': str(tmp_path), 'TERM': 'xterm-256color'}
    env.pop('PILOOP_ACTOR', None)
    piloop = [sys.executable, '-m', 'piloop']
    waiting = subprocess.Popen(
        [*piloop, 'inbox', 'wait', 'demo', '--after', str(cursor), '--timeout', '60'],
        env={**env, 'PILOOP_ACTOR': 'orchestrator'},
        stdout=subprocess.PIPE,
        text=True,
    )
    controller, terminal = pty.openpty()
    shown = bytearray()

    def drain() -> None:
        while chunk := _read(controller):
            shown.extend(chunk)

    watching = subprocess.Popen([*piloop, 'watch', 'demo'], env=env, stdin=terminal, stdout=terminal, stderr=terminal)
    # The child has the terminal now. On macOS a close of the terminal side blocks while its output waits to be read,
    # so the parent closes its copy at once, while the drain thread still reads the controller.
    os.close(terminal)
    threading.Thread(target=drain, daemon=True).start()

    def replied() -> bool:
        with db.session() as s:
            return s.get(Comment, 2) is not None

    try:
        wait_until(lambda: b'Inbox' in shown, timeout=30)
        for keys in ('r', 'Use the cheaper model', '\r'):
            time.sleep(1)
            os.write(controller, keys.encode())
        wait_until(replied, timeout=30)
        os.write(controller, b'q')
        assert watching.wait(timeout=30) == 0
        out, _ = waiting.communicate(timeout=30)
    finally:
        for proc in (watching, waiting):
            proc.kill()
        os.close(controller)
    assert out.splitlines()[0] == 'comment#2 decision p2 on comment#1 (demo) by human: Use the cheaper model'


def _read(fd: int) -> bytes:
    try:
        return os.read(fd, 65536)
    except OSError:  # the terminal closed under the reader: the test is over
        return b''


def test_the_sparkline_is_the_head_after_each_trunk_keep_so_a_branch_keep_leaves_it_level(
    db: Database, live_campaign: Campaign
) -> None:
    with db.session() as s:
        branch = Run(idea_id=1, attempt=2, base_candidate_id=1, harness='pi', model='m', branch=True)
        s.add(branch)
        s.flush()
        below = Candidate(
            campaign_id=live_campaign.id,
            run_id=branch.id,
            tree_sha='b',
            revision='b',
            summary='b',
            verdict=Outcome.KEEP,
        )
        s.add(below)
        s.flush()
        s.add(Measurement(candidate_id=below.id, evaluator_id=1, metrics={'score': 9.0, 'size': 1}))
        s.commit()
        p = s.get_one(Campaign, live_campaign.id)
        assert kept_scores(s, p, CampaignConfig.model_validate(p.config)) == {'score': [1.0, 3.0, 4.0]}


def test_a_screen_that_is_not_mounted_yet_is_not_read_again(db: Database, live_campaign: Campaign) -> None:
    """The poll timer can fire between a push and the mount of the new screen, whose widgets do not exist yet."""

    async def scenario(pilot: Pilot[None], app: Watch) -> None:
        app.push_screen(CampaignScreen('demo'))
        app._stamp = None
        app.poll()
        await pilot.pause()
        assert isinstance(app.screen, CampaignScreen) and app.screen.is_mounted

    drive(db, None, scenario)


def test_a_screen_that_the_exit_is_tearing_down_is_not_read_again(db: Database, live_campaign: Campaign) -> None:
    """The poll timer can also fire while the exit prunes the widgets of the screen, which is still mounted."""

    async def scenario(pilot: Pilot[None], app: Watch) -> None:
        app.exit()
        await campaign_screen(app).query_one('#header').remove()
        app._stamp = None
        app.poll()

    drive(db, 'demo', scenario)


def test_a_poll_after_the_app_stopped_running_reads_nothing(db: Database, live_campaign: Campaign) -> None:
    """The shutdown prunes the screens after the app stops running, and the poll timer can fire meanwhile: the screen is
    still mounted and attached (no ``exit`` ran), but its widgets are going."""

    async def scenario(pilot: Pilot[None], app: Watch) -> None:
        await pilot.pause()

    app = drive(db, 'demo', scenario)
    assert not app.is_running
    app._stamp = None
    app.poll()
