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

from piloop.campaign import inbox
from piloop.config import ProjectConfig
from piloop.db.models import (
    Candidate,
    Comment,
    CommentKind,
    Event,
    Idea,
    IdeaStatus,
    Measurement,
    Project,
    ProjectStatus,
    Run,
    RunStatus,
    TargetKind,
    Verdict,
    Worker,
    WorkerKind,
    WorkerState,
    utcnow,
)
from piloop.db.session import Database
from piloop.watch.app import CampaignScreen, CandidatesTable, ProjectsScreen, Watch
from piloop.watch.panels import PANELS, Context, kept_scores
from tests.daemon.test_serve import wait_until
from tests.reports.test_history import idea
from tests.runtime.test_herdr import Response, calls, scripted


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
    db: Database, ref: str | None, scenario: Callable[[Pilot[None], Watch], Awaitable[None]], actor: str = 'human'
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
def campaign(db: Database, project: Project, tmp_path: Path) -> Project:
    """A running campaign: kept candidates, two live agents (one in a Herdr tab), a queue, an inbox, a stopped twin."""
    log_dir = tmp_path / 'research-log'
    log_dir.mkdir()
    (log_dir / 'transcript.txt').write_text('reading the history\nproposing idea B\n')
    with db.session() as s:
        p = s.get_one(Project, project.id)
        p.config = {**p.config, 'stop': STOP}
        s.get_one(Candidate, 1).created_at = ago(hours=3)
        done = idea(s, p, 'more lines', IdeaStatus.DONE, created_at=ago(hours=3))
        run = Run(
            idea=done,
            attempt=1,
            base_candidate_id=1,
            harness='pi',
            model='m',
            status=RunStatus.SUCCEEDED,
            started_at=ago(hours=2),
            finished_at=ago(hours=1),
        )
        s.add(run)
        s.flush()
        for n, (verdict, value, minutes) in enumerate(
            [(Verdict.KEEP, 3, 90), (Verdict.DISCARD, 2, 70), (Verdict.KEEP, 4, 60)], start=2
        ):
            s.add(
                Candidate(
                    id=n,
                    project_id=p.id,
                    run_id=run.id,
                    parent_id=n - 1,
                    tree_sha=f't{n}',
                    commit_sha='x',
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
                Worker(
                    kind=WorkerKind.WORKER,
                    project_id=p.id,
                    run_id=live_run.id,
                    harness='pi',
                    model='m',
                    runtime='herdr',
                    handle={'workspace_id': 'w7', 'tab_id': 'w7:t2', 'pane_id': 'w7:p2', 'agent': 'worker-1'},
                    state=WorkerState.RUNNING,
                    started_at=ago(minutes=3),
                ),
                Worker(
                    kind=WorkerKind.RESEARCH,
                    project_id=p.id,
                    harness='claude',
                    model='opus',
                    state=WorkerState.STARTING,
                    reason='low_water: 1 ready < 10',
                    log_dir=str(log_dir),
                    started_at=ago(seconds=40),
                ),
                Worker(
                    kind=WorkerKind.WATCHDOG,
                    project_id=p.id,
                    state=WorkerState.EXITED,
                    usage={'total': 1_234_567, 'turns': 9, 'source': '/x.jsonl'},
                ),
            ]
        )
        comment = dict(author='watchdog#3', created_at=ago(minutes=30))
        s.add_all(
            [
                Comment(
                    target_kind=TargetKind.RUN,
                    target_id=run.id,
                    kind=CommentKind.ESCALATION,
                    priority=1,
                    acked_at=ago(minutes=20),
                    acked_by='pi',
                    body='keep #2?',
                    **comment,
                ),
                Comment(
                    target_kind=TargetKind.RUN,
                    target_id=run.id,
                    author='pi',
                    kind=CommentKind.DECISION,
                    priority=2,
                    body='Keep #2: the floor holds.\nDetails follow.',
                    created_at=ago(minutes=20),
                ),
                Comment(
                    target_kind=TargetKind.PROJECT,
                    target_id=p.id,
                    kind=CommentKind.ESCALATION,
                    priority=1,
                    body='Codex cannot start: trust dialog. Environment or harness?\nSecond line.',
                    **comment,
                ),
                Comment(
                    target_kind=TargetKind.PROJECT, target_id=p.id, kind=CommentKind.NOTE, body='a note', **comment
                ),
                Comment(
                    target_kind=TargetKind.RUN,
                    target_id=run.id,
                    author='human',
                    priority=2,
                    body='Why keep it?\nRe: comment#2',
                    created_at=ago(minutes=10),
                ),
            ]
        )
        other = Project(
            name='other',
            path=str(tmp_path / 'other'),
            config=p.config,
            status=ProjectStatus.STOPPED,
            stopped_reason='max_runs: 50 runs, and more words than the column holds',
        )
        s.add(other)
        s.flush()
        s.add(Candidate(project_id=other.id, tree_sha='o1', commit_sha='o', verdict=Verdict.PENDING))
        s.commit()
        for e in s.scalars(select(Event)):  # trigger-written events are stamped to the second
            e.at = ago(minutes=1)
        s.commit()
    return project


BLOCKED_AGENT: Response = {'result': {'type': 'agent_info', 'agent': {'name': 'worker-1', 'agent_status': 'blocked'}}}


def comment(s: object, comment_id: int) -> Comment:
    assert isinstance(s, Session)
    return s.get_one(Comment, comment_id)


def test_the_projects_screen_opens_a_project_and_esc_comes_back(
    db: Database, campaign: Project, herdr_log: Path
) -> None:
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
            ' piloop: 2 projects',
            '',
            ' project  status                                    head     vs baseline  agents    ready    inbox    tokens    last event',
            ' demo     active                                    4 score  +3           2 agents  1 ready  2 inbox  1.2M tok  comment.note 1m ago',
            ' other    stopped: max_runs: 50 runs, and more wo…  —        —            0 agents  0 ready  0 inbox  —         —',
        ]
    )
    assert shots[1] == snapshot("""\
 demo · head score 4 (candidate#4, +3 vs baseline) · 3 kept / 4 candidates · 1.2M tok
 run 2/50 · 2.0h/12h · 1.2M/5.0M tok · 0/10 runs without a keep · target ≥ 10 (head 4)
╭─ [1] Inbox 2 ────────────────────────────────────────────────────────────────────────╮╭─ [0] inbox ──────────────────────────────────────────────────────────╮
│ p1  escalation  watchdog#3  30m  Codex cannot start: trust dialog. Environment or har││ comment#3 escalation on project#1 by watchdog#3, 30m ago · p1,       │
│ p2  decision    pi          20m  Keep #2: the floor holds.                           ││ unacknowledged                                                       │
╰──────────────────────────────────────────────────────────────────────────────────────╯│                                                                      │
╭─ [2] Agents 2 ───────────────────────────────────────────────────────────────────────╮│ Codex cannot start: trust dialog. Environment or harness?            │
│ ● #1  worker    working  pi/m         3m   idea#2 longer lines                       ││ Second line.                                                         │
│ · #2  research  —        claude/opus  40s  low_water: 1 ready < 10                   ││                                                                      │
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
 a ack  A ack all shown  r reply  / filter  esc projects  q quit  ? keys
""")
    assert shots[2].splitlines()[:2] == snapshot(
        [
            ' other · no trunk keep · 0 kept / 1 candidates · stopped: max_runs: 50 runs, and more words than the column holds',
            ' run 0/50 · 0.0h/12h · 0/5.0M tok · 0/10 runs without a keep · target ≥ 10 (head —)',
        ]
    )


def test_a_project_not_registered_yet_is_waited_for(db: Database) -> None:
    shots: list[str] = []

    async def scenario(pilot: Pilot[None], app: Watch) -> None:
        shots.append(screen_text(app).splitlines()[0])

    drive(db, '/nowhere/yet', scenario)
    assert shots == [" no project '/nowhere/yet' yet: `piloop init` registers it, and this view follows"]


def test_inbox_keys_acknowledge_one_item_or_every_item_shown(db: Database, campaign: Project, herdr_log: Path) -> None:
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
    db: Database, campaign: Project, herdr_log: Path
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
        sent = comment(s, 6)
        assert (sent.target_kind, sent.target_id, sent.author, sent.kind, sent.priority, sent.body) == (
            TargetKind.COMMENT,
            2,
            'human',
            CommentKind.DECISION,
            3,
            'Revert it',
        )
        assert inbox.news(s, cursor, project_id=campaign.id, me='orchestrator')[1] == [
            'comment#6 decision p3 on comment#2 (demo) by human: Revert it'
        ]


def test_slash_filters_the_focused_panel_and_esc_clears_it(db: Database, campaign: Project, herdr_log: Path) -> None:
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


def test_the_agents_panel_shows_the_selected_agents_output_and_its_state(
    db: Database, campaign: Project, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    scripted(monkeypatch, agent_get=[BLOCKED_AGENT])
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
│ p1  escalation  watchdog#3  30m  Codex cannot start: trust dialog. Environment or har││ worker-1 · pi/m · running 3m · herdr tab w7:t2                       │
│ p2  decision    pi          20m  Keep #2: the floor holds.                           ││ on: idea#2 longer lines                                              │
╰──────────────────────────────────────────────────────────────────────────────────────╯│ blocked on a dialog: steering is refused until it is answered; enter │
╭─ [2] Agents 2 · 1 blocked ───────────────────────────────────────────────────────────╮│ brings it to the front                                               │
│ ◐ #1  worker    blocked  pi/m         3m   idea#2 longer lines                       ││                                                                      │
│ · #2  research  —        claude/opus  40s  low_water: 1 ready < 10                   ││ the last 1 lines it wrote                                            │
╰──────────────────────────────────────────────────────────────────────────────────────╯│ hello from the agent                                                 │
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
 ⏎ focus  s steer  / filter  esc projects  q quit  ? keys
""")
    assert 'proposing idea B' in shots[1], "a headless agent's saved transcript"


def test_agents_keys_focus_the_tab_and_steer(
    db: Database, campaign: Project, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    notes: list[str] = []

    async def scenario(pilot: Pilot[None], app: Watch) -> None:
        async def note(*keys: str) -> None:
            await pilot.press(*keys)
            await pilot.pause()
            notes.append(campaign_screen(app).note)

        await note('2', 'enter')
        await note('s', *'try the cheaper model', 'enter')
        await note('s', 'escape', 'down', 'enter')
        await note('s', *'go', 'enter')
        scripted(
            monkeypatch,
            tab_focus=[{'error': {'code': 'tab_not_found', 'message': 'w7:t2 is gone'}}],
            agent_prompt=[
                {'result': {'type': 'agent_prompted', 'agent': {'name': 'worker-1'}}},
                {'error': {'code': 'agent_blocked', 'message': 'blocked'}},
                {'error': {'code': 'agent_not_found', 'message': 'gone'}},
            ],
        )
        await note('up', 'enter')
        await note('s', *'go', 'enter')
        await note('s', *'go', 'enter')
        with db.session() as s:
            headless = s.get_one(Worker, 2)
            headless.state, headless.runtime = WorkerState.RUNNING, 'subprocess'
            headless.handle = {'pid': 1, 'start_time': 0, 'log_dir': '/x'}
            s.commit()
        await note('down', 'enter')

    drive(db, 'demo', scenario)
    assert notes == [
        'worker#1: focused tab w7:t2',
        'steered worker#1',
        'worker#2 is starting, not running: nothing to focus',
        'worker#2 is starting, not running: nothing to steer',
        'could not focus worker#1: tab_not_found: w7:t2 is gone',
        'not steered: worker-1 is blocked on an approval or question; read it and answer with herdr agent send-keys',
        'could not steer worker#1: agent_not_found: gone',
        'worker#2: a background process has no terminal to show: read /x/worker.stdout',
    ]
    assert [c for c in calls(herdr_log) if c[:2] in (['tab', 'focus'], ['agent', 'prompt'])][:2] == [
        ['tab', 'focus', 'w7:t2'],
        ['agent', 'prompt', 'worker-1', 'try the cheaper model'],
    ]


def test_ideas_keys_approve_park_reject_with_a_reason_and_move_the_priority(
    db: Database, campaign: Project, herdr_log: Path
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


def test_a_seed_heads_the_ideas_panel_and_is_developed_by_research_not_approved_by_a_key(
    db: Database, campaign: Project, herdr_log: Path
) -> None:
    with db.session() as s:
        seed = Idea(
            track='score',
            project_id=campaign.id,
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
        f'idea#{seed_id} is a seed: research defines it first (piloop idea develop {seed_id} …)',
        f'idea#{seed_id} seed → rejected',
    ]


def test_the_ideas_and_candidates_details(db: Database, campaign: Project, herdr_log: Path) -> None:
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
            ' parent candidate#3 · run #1 · commit x · 1h ago',
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


def test_imported_references_are_listed_apart_as_starting_points(
    db: Database, campaign: Project, herdr_log: Path
) -> None:
    with db.session() as s:
        s.add(
            Candidate(
                project_id=campaign.id, tree_sha='r1', commit_sha='r', summary='paper repro', verdict=Verdict.REFERENCE
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
            ' #6  reference  —  —   —      0s  paper repro',
        ]
    )


def test_question_mark_shows_the_panels_keys_and_0_focuses_the_detail(
    db: Database, campaign: Project, herdr_log: Path
) -> None:
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


def test_an_empty_project_takes_every_key_without_acting(db: Database, campaign: Project, herdr_log: Path) -> None:
    async def scenario(pilot: Pilot[None], app: Watch) -> None:
        assert isinstance(app.focused, CandidatesTable), 'an empty inbox does not take the focus'
        await pilot.press('1', 'a', 'A', 'r', '2', 'enter', 's', '3', 'y', 'plus')
        await pilot.pause()
        assert campaign_screen(app).note == ''
        assert ' nothing here' in screen_text(app)

    drive(db, 'other', scenario)


def test_the_screen_follows_the_database_and_a_dialog_holds_it_still(
    db: Database, campaign: Project, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_file = Path(str(db.engine.url.database))

    async def scenario(pilot: Pilot[None], app: Watch) -> None:
        with db.session() as s:
            s.add(Comment(target_kind=TargetKind.PROJECT, target_id=campaign.id, author='w', priority=0, body='new'))
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
        assert 'piloop: 2 projects' in screen_text(app), 'the projects screen re-reads too'
        app.poll()

    drive(db, 'demo', scenario)


def test_details_of_rows_gone_or_bare(db: Database, campaign: Project) -> None:
    with db.session() as s:
        other = s.get_one(Project, 2)
        ctx = Context(other, ProjectConfig.model_validate(other.config), utcnow(), 'human')
        s.add(
            Idea(
                track='score',
                project_id=2,
                title='with a plan',
                hypothesis='h',
                plan='step one\nstep two',
                created_by='t',
            )
        )
        s.commit()
        assert text(PANELS['inbox'].detail(s, ctx, 99)) == 'comment#99 is gone\n'
        assert 'plan:\nstep one\nstep two\n' in text(PANELS['ideas'].detail(s, ctx, 5))
        assert 'metrics under evaluator#0 (vs parent):\n  —\n' in text(PANELS['candidates'].detail(s, ctx, 5))


def text(renderable: RenderableType) -> str:
    console = Console(width=100, file=io.StringIO(), record=True, color_system=None)
    console.print(renderable)
    return console.export_text()


def test_the_projects_screen_without_a_project_opens_nothing(db: Database) -> None:
    async def scenario(pilot: Pilot[None], app: Watch) -> None:
        await pilot.press('enter')
        await pilot.pause()
        assert isinstance(app.screen, ProjectsScreen) and 'piloop: 0 projects' in screen_text(app)

    drive(db, None, scenario)


def test_a_reply_typed_in_the_watch_wakes_the_orchestrators_inbox_wait(
    db: Database, project: Project, tmp_path: Path
) -> None:
    """End to end in an isolated PILOOP_HOME: `piloop watch` in a terminal, `piloop inbox wait` as the orchestrator."""
    with db.session() as s:
        s.add(
            Comment(
                target_kind=TargetKind.PROJECT,
                target_id=project.id,
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
    db: Database, campaign: Project
) -> None:
    with db.session() as s:
        branch = Run(idea_id=1, attempt=2, base_candidate_id=1, harness='pi', model='m', branch=True)
        s.add(branch)
        s.flush()
        below = Candidate(
            project_id=campaign.id, run_id=branch.id, tree_sha='b', commit_sha='b', summary='b', verdict=Verdict.KEEP
        )
        s.add(below)
        s.flush()
        s.add(Measurement(candidate_id=below.id, evaluator_id=1, metrics={'score': 9.0, 'size': 1}))
        s.commit()
        p = s.get_one(Project, campaign.id)
        assert kept_scores(s, p, ProjectConfig.model_validate(p.config)) == {'score': [1.0, 3.0, 4.0]}
