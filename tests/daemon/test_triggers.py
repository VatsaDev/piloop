"""Each trigger in the roles docs fires exactly when the docs say, and requests launch within ``max_concurrent``."""

import sqlite3
import time
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import select

from piloop.actor import System
from piloop.campaign.anomaly import AnomalyKind, AnomalyReported
from piloop.campaign.stop import request_cancel
from piloop.config import load_campaign_config
from piloop.daemon.main import ACTOR
from piloop.daemon.run_controller import RunController
from piloop.daemon.scheduler import NO_RUN_ERROR, Scheduler
from piloop.db import sql
from piloop.db.events import in_order
from piloop.db.models import (
    AgentSession,
    AgentSessionState,
    Campaign,
    Candidate,
    Evaluator,
    Idea,
    IdeaLink,
    Measurement,
    Run,
    RunEnd,
    RunStatus,
    utcnow,
)
from piloop.db.session import Database
from piloop.ids import IMPLEMENTER, AgentSessionId, Ending, IdeaId, IdeaStatus, LinkKind, RunId, TargetKind, TargetRef
from piloop.otel import Tracer
from piloop.plugins.executor.workers import Workers
from piloop.plugins.verdict_rule.contract import Outcome
from piloop.reports.overview import anomalies
from tests.conftest import (
    HARNESS_NOOP,
    HARNESS_PROMPT_TO_FILE,
    RESEARCH,
    WATCHDOG,
    ScriptRuntime,
    end_of,
    factory,
    serving,
    wait_until,
    write_toml,
)

BLOCKING = '#!/bin/sh\nwhile [ ! -f {release} ]; do sleep 0.05; done\n'


def scheduler(db: Database, runtime: ScriptRuntime, tmp_path: Path) -> Scheduler:
    controller = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers()))
    return Scheduler(db, controller, Tracer(None), tmp_path, factory(runtime))


def low_water(buffer: int) -> list[dict[str, object]]:
    """Research's ``low_water`` trigger, with ``buffer``, and its ``seed`` trigger."""
    return [{'provider': 'low_water', 'buffer': buffer}, {'provider': 'seed'}]


def configure(db: Database, campaign: Campaign, harness_body: str = HARNESS_NOOP, **roles: dict[str, object]) -> None:
    """Rewrite the campaign's harness script and role settings (merged over the test toml)."""
    write_toml(Path(campaign.path), harness_body)
    with db.session() as s:
        cfg = load_campaign_config(Path(campaign.path), roles=roles)
        s.get_one(Campaign, campaign.id).config = cfg
        s.commit()


def agent_sessions(db: Database, kind: str) -> list[tuple[int, AgentSessionState, str | None]]:
    with db.session() as s:
        rows = s.scalars(select(AgentSession).where(AgentSession.kind == kind).order_by(AgentSession.id)).all()
        return [(w.id, w.state, w.reason) for w in rows]


def fail_a_run(db: Database, campaign: Campaign, title: str, status: RunStatus = RunStatus.FAILED) -> RunId:
    with db.session() as s:
        idea = Idea(
            track='score',
            campaign_id=campaign.id,
            title=title,
            hypothesis=title,
            created_by='t',
            status=IdeaStatus.DONE,
        )
        s.add(idea)
        s.flush()
        run = Run(
            idea_id=idea.id,
            attempt=1,
            base_candidate_id=1,
            harness='h',
            model='m',
            status=status,
            end=end_of(status),
            finished_at=utcnow(),
        )
        s.add(run)
        s.commit()
        return run.id


def finish_a_run(db: Database, campaign: Campaign, title: str, verdict: Outcome = Outcome.DISCARD) -> RunId:
    """Add a run that succeeded just now with a measured candidate, a discard by default.

    The research trigger sees a new result. The watchdog trigger sees no failure.
    """
    time.sleep(0.01)
    run_id = fail_a_run(db, campaign, title, RunStatus.SUCCEEDED)
    with db.session() as s:
        candidate = Candidate(
            campaign_id=campaign.id,
            run_id=run_id,
            parent_id=1,
            tree_sha=f'tree{run_id}',
            revision=f'commit{run_id}',
            verdict=verdict,
        )
        s.add(candidate)
        s.flush()
        evaluator_id = s.get_one(Campaign, campaign.id).evaluator_id or 0
        s.add(Measurement(candidate_id=candidate.id, evaluator_id=evaluator_id, metrics={'score': 0.0}))
        s.commit()
    return run_id


def wait_for(db: Database, agent_session_id: AgentSessionId, state: AgentSessionState) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        with db.session() as s:
            if s.get_one(AgentSession, agent_session_id).state is state:
                return
        time.sleep(0.02)
    raise AssertionError(f'implementer#{agent_session_id} never reached {state}')


def backdate(db: Database, agent_session_id: AgentSessionId, seconds: float) -> None:
    with db.session() as s:
        w = s.get_one(AgentSession, agent_session_id)
        w.started_at = w.started_at - timedelta(seconds=seconds)
        if w.ended_at is not None:
            w.ended_at = w.ended_at - timedelta(seconds=seconds)
        s.commit()


def reported_events(db: Database) -> list[tuple[int, AnomalyReported]]:
    with db.session() as s:
        return [(e.target_id, p) for e, p in in_order(s, AnomalyReported)]


def test_an_anomaly_requests_an_incident_sweep_once_and_collapses_while_one_is_pending(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    release = tmp_path / 'release'
    configure(db, campaign, BLOCKING.format(release=release))
    sched = scheduler(db, runtime, tmp_path)
    assert sched.run_all() == [], 'nothing went wrong yet'

    fail_a_run(db, campaign, 'first')
    assert sched.run_all() == [1], 'requested and launched on the same tick'
    assert sched.run_all() == [], 'the same failure does not fire twice'
    [(agent_session, payload)] = reported_events(db)
    assert (agent_session, payload.subject, payload.anomaly) == (
        1,
        TargetRef(TargetKind.RUN, 1),
        AnomalyKind.RUN_FAILED,
    )
    line = payload.line
    assert line.startswith('- [incident] run#1 failed, ') and line.endswith(
        ' ago: failed on h (idea#1): no error recorded'
    )

    fail_a_run(db, campaign, 'second', RunStatus.TIMED_OUT)
    assert sched.run_all() == [], 'max_concurrent=1: the sweep for the second failure waits'
    fail_a_run(db, campaign, 'third')
    sched.run_all()
    wait_for(db, AgentSessionId(1), AgentSessionState.RUNNING)
    assert agent_sessions(db, WATCHDOG) == [
        (1, AgentSessionState.RUNNING, 'anomaly: run#1 failed'),
        (2, AgentSessionState.REQUESTED, 'anomaly: run#2 failed'),
    ], 'one pending request absorbs later anomalies'

    release.touch()
    sched.wait()
    assert sched.run_all() == [2], 'the slot frees; the pending request launches'
    sched.wait()
    assert sched.run_all() == [3], 'the anomaly that came while a request was pending gets its own sweep'
    assert [(w, str(p.subject), p.anomaly) for w, p in reported_events(db)] == [
        (1, 'run#1', AnomalyKind.RUN_FAILED),
        (2, 'run#2', AnomalyKind.RUN_FAILED),
        (3, 'run#3', AnomalyKind.RUN_FAILED),
    ]
    sched.wait()
    sched.stop(timeout=5)
    again = scheduler(db, runtime, tmp_path)
    assert again.run_all() == [], 'the events are the record: a restarted daemon replays nothing'
    again.stop(timeout=5)
    assert [w[1] for w in agent_sessions(db, WATCHDOG)] == [AgentSessionState.EXITED] * 3


def test_an_anomaly_takes_the_tick_and_does_not_reset_the_timer(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """When a timer is due and a new failure arrives in the same tick, the incident sweep starts first."""
    configure(db, campaign, HARNESS_PROMPT_TO_FILE, watchdog={'interval': 30})
    with db.session() as s:
        s.get_one(Campaign, campaign.id).created_at = utcnow() - timedelta(seconds=31)
        s.commit()
    fail_a_run(db, campaign, 'broken')
    sched = scheduler(db, runtime, tmp_path)
    assert sched.run_all() == [1]
    sched.wait()
    assert sched.run_all() == [2], 'the incident sweep is not a progress review: the timer is still due'
    sched.wait()
    assert agent_sessions(db, WATCHDOG) == [
        (1, AgentSessionState.EXITED, 'anomaly: run#1 failed'),
        (2, AgentSessionState.EXITED, 'timer: 30s'),
    ]
    assert sched.run_all() == [], 'the timer counts from the progress review'

    incident = (tmp_path / 'agent_sessions' / '1' / 'brief.md').read_text()
    assert incident.index('# Watchdog') < incident.index('## This sweep: an incident') < incident.index('# Demo')
    assert '## Incidents to investigate\n\n- [incident] run#1 failed' in incident
    assert '## Goal' in incident and '## Research pack' not in incident
    progress = (tmp_path / 'agent_sessions' / '2' / 'brief.md').read_text()
    assert '## This sweep: a progress review' in progress and '## This sweep: an incident' not in progress
    assert '## Incidents to investigate' not in progress
    assert progress.index('## Goal') < progress.index('### History: ideas')
    sched.stop(timeout=5)


def test_only_incident_anomalies_request_a_sweep(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """An idle queue goes into the progress review, not into an incident sweep."""
    sched = scheduler(db, runtime, tmp_path)
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        [idle] = anomalies(s, p, load_campaign_config(Path(campaign.path)))
    assert idle.name == 'campaign#1 idle' and not idle.incident
    assert sched.run_all() == []
    assert agent_sessions(db, WATCHDOG) == []
    sched.stop(timeout=1)


def test_cron_fires_once_per_interval(db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path) -> None:
    configure(db, campaign, watchdog={'interval': 30})
    sched = scheduler(db, runtime, tmp_path)
    assert sched.run_all() == [], 'the campaign was registered a moment ago'
    with db.session() as s:
        s.get_one(Campaign, campaign.id).created_at = utcnow() - timedelta(seconds=31)
        s.commit()
    assert sched.run_all() == [1]
    assert agent_sessions(db, WATCHDOG)[0][2] == 'timer: 30s'
    sched.wait()
    assert sched.run_all() == [], 'not due again'
    backdate(db, AgentSessionId(1), 31)
    assert sched.run_all() == [2]
    sched.wait()

    configure(db, campaign, watchdog={'interval': 0})
    backdate(db, AgentSessionId(2), 3600)
    assert sched.run_all() == [], 'interval 0 turns the timer off'
    sched.stop(timeout=5)


def occupy(db: Database, campaign: Campaign, title: str = 'busy') -> RunId:
    """Add a run in flight that holds an implementer slot without the queue starting anything."""
    with db.session() as s:
        idea = Idea(
            track='score',
            campaign_id=campaign.id,
            title=title,
            hypothesis='h',
            created_by='human',
            status=IdeaStatus.DONE,
        )
        s.add(idea)
        s.flush()
        run = Run(idea_id=idea.id, attempt=1, base_candidate_id=1, harness='h', model='m', status=RunStatus.RUNNING)
        s.add(run)
        s.commit()
        return run.id


def add_ideas(
    db: Database, campaign: Campaign, count: int, by: str = 'human', status: IdeaStatus = IdeaStatus.READY
) -> list[IdeaId]:
    with db.session() as s:
        ideas = [
            Idea(
                track='score', campaign_id=campaign.id, title=f'{by} {i}', hypothesis='h', created_by=by, status=status
            )
            for i in range(count)
        ]
        s.add_all(ideas)
        s.commit()
        return [idea.id for idea in ideas]


def consume(db: Database, idea_id: IdeaId) -> None:
    with db.session() as s:
        s.get_one(Idea, idea_id).status = IdeaStatus.DONE
        s.commit()


def last_reason(db: Database) -> str | None:
    return agent_sessions(db, RESEARCH)[-1][2]


def take(db: Database, agent_session_id: AgentSessionId, seconds: float, ago: float = 0) -> None:
    """Make an ended sweep have taken ``seconds`` and have ended ``ago`` seconds ago."""
    with db.session() as s:
        w = s.get_one(AgentSession, agent_session_id)
        assert w.ended_at is not None
        ended = utcnow() - timedelta(seconds=ago)
        w.ended_at, w.started_at = ended, ended - timedelta(seconds=seconds)
        s.commit()


def test_low_water_keeps_a_slot_s_worth_plus_the_buffer_and_tops_it_up_on_new_results(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """Low water keeps one idea per implementer slot plus ``buffer`` ready, even when every slot is busy.

    Below that target a sweep starts. Each sweep that defined no idea doubles the number of finished runs the next
    sweep waits for. A sweep that defined an idea resets the wait.
    """
    configure(db, campaign, research={'max_ideas': 1, 'triggers': low_water(2)})
    sched = scheduler(db, runtime, tmp_path)
    occupy(db, campaign)
    first, *_ = add_ideas(db, campaign, 3)
    assert sched.run_all() == [], 'one implementer slot plus a buffer of two: three runnable ideas is the target'

    consume(db, first)
    assert sched.run_all() == [1], 'below the target with no slot free: the first sweep needs nothing'
    assert last_reason(db) == '2 of 3 runnable ideas'
    sched.wait()
    take(db, AgentSessionId(1), seconds=600)
    assert sched.run_all() == [], 'nothing finished since the sweep started, which defined nothing: research rests'

    finish_a_run(db, campaign, 'one')
    assert sched.run_all() == [], 'the sweep defined nothing: the next one waits for two finished runs'
    finish_a_run(db, campaign, 'two')
    assert sched.run_all() == [2]
    assert last_reason(db) == (
        '2 of 3 runnable ideas; 2 runs finished since agent_session#1 started (after 1 sweeps that defined no idea)'
    )
    sched.wait()

    add_ideas(db, campaign, 1, by='research#2')
    finish_a_run(db, campaign, 'three')
    assert sched.run_all() == [], 'back at the target: new results alone ask for nothing'
    consume(db, IdeaId(first + 1))
    assert sched.run_all() == [3], 'implementer#2 inserted an idea: the back-off is reset, one finished run is enough'
    assert last_reason(db) == '2 of 3 runnable ideas; 1 runs finished since agent_session#2 started'
    sched.wait()
    sched.stop(timeout=5)


def test_low_water_keeps_sweeping_a_short_backlog_though_every_slot_is_busy_resting_only_after_empty_sweeps(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A short backlog starts a sweep even when every implementer slot is busy and no new result exists.

    A sweep that defined an idea is followed at once. A sweep that defined no idea is followed by a rest as long as
    the sweep took.
    """
    configure(db, campaign, research={'max_ideas': 1, 'triggers': low_water(2)})
    sched = scheduler(db, runtime, tmp_path)
    occupy(db, campaign)
    assert sched.run_all() == [1]
    sched.wait()
    add_ideas(db, campaign, 1, by='research#1')
    assert sched.run_all() == [2], 'implementer#1 defined an idea, the backlog is still short: no result needed'
    assert last_reason(db) == '1 of 3 runnable ideas'
    sched.wait()
    take(db, AgentSessionId(2), seconds=60)
    assert sched.run_all() == [], 'implementer#2 defined nothing: research rests as long as it took'
    take(db, AgentSessionId(2), seconds=60, ago=61)
    assert sched.run_all() == [3]
    assert last_reason(db) == '1 of 3 runnable ideas; rested 60s after 1 sweeps that defined no idea'
    sched.wait()
    sched.stop(timeout=5)


def test_a_seed_fires_one_research_sweep_however_full_the_backlog_once(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A new seed starts one research sweep, even when the backlog is at its target or ``low_water`` is off.

    A seed is new until a sweep starts after it was filed. A sweep that read the seed and left it does not start
    another sweep.
    """
    configure(db, campaign, research={'max_ideas': 0, 'max_concurrent': 1})
    sched = scheduler(db, runtime, tmp_path)
    occupy(db, campaign)
    assert sched.run_all() == [], 'max_ideas 0 turns low_water off'
    add_ideas(db, campaign, 1, by='watchdog#9', status=IdeaStatus.SEED)
    assert sched.run_all() == [1]
    assert last_reason(db) == '1 new seeds to develop'
    sched.wait()
    assert sched.run_all() == [], (
        'implementer#1 read the seed and left it: the next sweep, whatever fires it, lists it again'
    )
    time.sleep(0.01)
    add_ideas(db, campaign, 1, by='watchdog#3', status=IdeaStatus.SEED)
    assert sched.run_all() == [2]
    assert last_reason(db) == '1 new seeds to develop'
    sched.wait()
    sched.stop(timeout=5)


def test_stalled_sweeps_on_new_results_however_full_the_backlog(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A stall needs a pivot, so ``stalled`` sweeps while the backlog is at its target or above it.

    The next sweep waits for the next stall step, and for the new results that the back-off of empty sweeps wants.
    """
    triggers = [*low_water(0), {'provider': 'stalled'}]
    configure(db, campaign, implementer={'max_concurrent': 1}, research={'max_ideas': 1, 'triggers': triggers})
    sched = scheduler(db, runtime, tmp_path)
    occupy(db, campaign)
    add_ideas(db, campaign, 2)
    finish_a_run(db, campaign, 'tried once')
    assert sched.run_all() == [], 'one run without a keep, of the two that mark a stall: the full backlog wants nothing'

    finish_a_run(db, campaign, 'tried twice')
    assert sched.run_all() == [1], 'stalled: the backlog holds no pivot'
    assert last_reason(db) == 'stalled, step 1'
    sched.wait()
    assert sched.run_all() == [], 'research read every result'

    finish_a_run(db, campaign, 'tried again')
    assert sched.run_all() == [], 'still the first stall step'
    finish_a_run(db, campaign, 'tried once more')
    assert sched.run_all() == [2]
    assert last_reason(db) == (
        'stalled, step 2; 2 runs finished since agent_session#1 started (after 1 sweeps that defined no idea)'
    )
    sched.wait()
    sched.stop(timeout=5)


def test_a_long_stall_gets_one_stalled_sweep_per_stall_step_and_a_keep_ends_it(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A stall of N runs gets N / mark sweeps of ``stalled``, not N: one at each mark of runs without a keep."""
    triggers = [{'provider': 'stalled'}]
    configure(db, campaign, implementer={'max_concurrent': 2}, research={'max_ideas': 1, 'triggers': triggers})
    sched = scheduler(db, runtime, tmp_path)  # The stall mark is two rounds of 2 run slots: 4 runs.
    for n in range(12):
        finish_a_run(db, campaign, f'tried {n}')
        sched.run_all()
        sched.wait()
    reasons = [reason or '' for _, _, reason in agent_sessions(db, RESEARCH)]
    assert [r.split(';')[0] for r in reasons] == ['stalled, step 1', 'stalled, step 2', 'stalled, step 3']

    finish_a_run(db, campaign, 'kept', Outcome.KEEP)
    for n in range(4):
        finish_a_run(db, campaign, f'after the keep {n}')
        sched.run_all()
        sched.wait()
    assert len(agent_sessions(db, RESEARCH)) == 4, 'the keep ended the stall: four runs mark a new one'
    assert (last_reason(db) or '').startswith('stalled, step 1; 5 runs finished since agent_session#3 started')
    sched.stop(timeout=5)


def test_a_killed_sweep_s_seeds_are_retried_once_and_a_new_seed_waits_for_a_free_research_slot(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    release = tmp_path / 'release'
    configure(
        db,
        campaign,
        BLOCKING.format(release=release),
        research={'max_ideas': 0, 'max_concurrent': 1, 'timeout': 0.5},
    )
    sched = scheduler(db, runtime, tmp_path)
    occupy(db, campaign)
    add_ideas(db, campaign, 1, by='watchdog#9', status=IdeaStatus.SEED)
    assert sched.run_all() == [1]
    sched.wait()
    assert sched.run_all() == [2]
    assert last_reason(db) == '1 seeds to develop; retrying after agent_session#1 was killed: agent timeout after 0.5s'
    sched.wait()
    assert sched.run_all() == [], 'a second killed batch is a systemic fault: no more retries'

    configure(db, campaign, BLOCKING.format(release=release), research={'max_ideas': 0, 'max_concurrent': 1})
    add_ideas(db, campaign, 1, by='watchdog#10', status=IdeaStatus.SEED)
    assert sched.run_all() == [3], 'a seed filed after the latest sweep started is new'
    wait_until(lambda: agent_sessions(db, RESEARCH)[-1][1] is AgentSessionState.RUNNING)
    add_ideas(db, campaign, 1, by='watchdog#11', status=IdeaStatus.SEED)
    assert sched.run_all() == [], 'a new seed, but the only research slot is busy'
    release.touch()
    sched.wait()
    sched.stop(timeout=5)


def finish(db: Database, run_id: RunId) -> None:
    with db.session() as s:
        run = s.get_one(Run, run_id)
        run.status, run.end, run.finished_at = RunStatus.SUCCEEDED, RunEnd.FINISHED, utcnow()
        s.commit()


def test_after_a_barren_sweep_research_never_waits_for_more_results_than_the_runs_in_flight_can_give(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """After sweeps that defined no idea, research waits for no more finished runs than the runs in flight can give.

    Idle implementer slots end the rest on time. When the last run in flight finishes, no more results can come, so that
    one result is enough.
    """
    configure(db, campaign, implementer={'max_concurrent': 4}, research={'max_ideas': 2, 'max_concurrent': 1})
    sched = scheduler(db, runtime, tmp_path)
    in_flight = occupy(db, campaign)
    assert sched.run_all() == [1]
    sched.wait()
    take(db, AgentSessionId(1), seconds=600, ago=60)
    assert sched.run_all() == [], 'barren: two finished runs wanted, none yet, and the idle rest has 540 s to go'
    take(db, AgentSessionId(1), seconds=600, ago=601)
    assert sched.run_all() == [2], 'three slots idle, no result: the rest is over'
    assert last_reason(db) == (
        '3 free implementer slots without a runnable idea; 0 of 6 runnable ideas; rested 600s after 1 sweeps that defined'
        ' no idea'
    )
    sched.wait()

    take(db, AgentSessionId(2), seconds=600, ago=60)
    finish(db, in_flight)
    assert sched.run_all() == [3], 'two barren sweeps want four finished runs, but nothing else is in flight'
    assert last_reason(db) == (
        '4 free implementer slots without a runnable idea; 0 of 6 runnable ideas; 1 runs finished since agent_session#2 started'
        ' (after 2 sweeps that defined no idea)'
    )
    sched.wait()
    sched.stop(timeout=5)


def test_low_water_sizes_the_batch_to_the_shortfall_net_of_the_sweeps_in_flight(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """Low water starts only as many sweeps as the shortfall needs.

    Five implementer slots plus a buffer of one is a target of six ideas, so three sweeps of two ideas start, though four
    may run. Each starts on its own harness. A sweep in flight already counts for its ``max_ideas``.
    """
    release = tmp_path / 'release'
    write_toml(Path(campaign.path), BLOCKING.format(release=release))
    script = {'executable': str(tmp_path / 'harness.sh'), 'headless': ['{prompt}']}
    cfg = load_campaign_config(
        Path(campaign.path),
        harness={'b': script, 'c': script},
        roles={
            'implementer': {'max_concurrent': 5},
            'research': {
                'harnesses': ['script', 'b', 'c'],
                'model': {'b': 'mb', 'c': 'mc'},
                'max_concurrent': 4,
                'max_ideas': 2,
                'triggers': low_water(1),
            },
        },
    )
    with db.session() as s:
        s.get_one(Campaign, campaign.id).config = cfg
        s.commit()
    sched = scheduler(db, runtime, tmp_path)
    assert sched.run_all() == [1, 2, 3]
    assert [w[2] for w in agent_sessions(db, RESEARCH)] == [
        '5 free implementer slots without a runnable idea; 0 of 6 runnable ideas'
    ] * 3
    assert sched.run_all() == [], 'the three in flight may insert the six: a fourth would overshoot'
    release.touch()
    sched.wait()
    with db.session() as s:
        picks = [(w.harness, w.model) for w in s.scalars(select(AgentSession).order_by(AgentSession.id))]
    assert picks == [('script', 'test-model'), ('b', 'mb'), ('c', 'mc')]

    with db.session() as s:  # a sweep in flight, started by hand
        s.add(AgentSession(kind=RESEARCH, campaign_id=campaign.id, state=AgentSessionState.RUNNING))
        s.commit()
    add_ideas(db, campaign, 1, by='research#3', status=IdeaStatus.PROPOSED)
    assert sched.run_all() == [5, 6], 'six wanted, two coming: two sweeps of two'
    assert last_reason(db) == (
        '3 free implementer slots without a runnable idea; 0 of 6 runnable ideas, 2 more from 1 sweeps in flight'
    )
    sched.wait()
    sched.stop(timeout=5)


def test_low_water_counts_ideas_waiting_only_on_a_prerequisite_in_flight_as_backlog(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """Low water counts an idea that waits only on prerequisites in flight as backlog.

    Such an idea runs when its prerequisites finish, with no sweep in between. An idea that waits on a prerequisite
    that is not running is not backlog.
    """
    configure(
        db,
        campaign,
        implementer={'max_concurrent': 2},
        research={'max_ideas': 1, 'max_concurrent': 1, 'triggers': low_water(1)},
    )
    sched = scheduler(db, runtime, tmp_path)
    with db.session() as s:
        prerequisite = s.get_one(Run, occupy(db, campaign)).idea
        prerequisite.status = IdeaStatus.SCHEDULED
        s.commit()
        prerequisite_id = prerequisite.id
    occupy(db, campaign, 'busy too')
    with db.session() as s:
        s.add_all(
            Idea(
                track='score',
                campaign_id=campaign.id,
                title=f'after {i}',
                hypothesis='h',
                created_by='research#1' if i else 'human',
                links=[IdeaLink(target_id=prerequisite_id, kind=LinkKind.AFTER)],
            )
            for i in range(2)
        )
        s.commit()
    assert sched.run_all() == [1]
    assert last_reason(db) == '0 of 3 runnable ideas, 2 more waiting on runs in flight'
    sched.wait()
    add_ideas(db, campaign, 1, by='research#1')
    assert sched.run_all() == [], 'both slots busy, one idea runnable and two waiting on the run in flight: three'
    with db.session() as s:
        s.get_one(Idea, prerequisite_id).status = IdeaStatus.PARKED
        s.commit()
    assert sched.run_all() == [2], 'a parked prerequisite is no run in flight: the two behind it are blocked'
    assert last_reason(db) == '1 of 3 runnable ideas'
    sched.wait()
    sched.stop(timeout=5)


def test_low_water_requests_nothing_while_every_research_slot_is_busy(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    release = tmp_path / 'release'
    configure(db, campaign, BLOCKING.format(release=release), research={'max_ideas': 1, 'max_concurrent': 1})
    sched = scheduler(db, runtime, tmp_path)
    assert sched.run_all() == [1]
    finish_a_run(db, campaign, 'one')
    assert sched.run_all() == [], 'one sweep of one cannot fill a target of three, but the only research slot is busy'
    release.touch()
    sched.wait()
    sched.stop(timeout=5)


def test_low_water_wants_no_more_ideas_than_max_runs_leaves_to_run(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    release = tmp_path / 'release'
    write_toml(Path(campaign.path), BLOCKING.format(release=release))
    cfg = load_campaign_config(
        Path(campaign.path), roles={'research': {'max_ideas': 1}}, plugins={'stop_rule': {'max_runs': 3}}
    )
    with db.session() as s:
        s.get_one(Campaign, campaign.id).config = cfg
        s.commit()
    finish_a_run(db, campaign, 'one')
    add_ideas(db, campaign, 2)
    sched = scheduler(db, runtime, tmp_path)
    assert sched.run_all() == [1], 'a run of the first idea; one run left, one idea for it: no research'
    assert agent_sessions(db, RESEARCH) == []
    release.touch()
    sched.wait()
    sched.stop(timeout=5)


def test_ideas_updated_at_is_bumped_by_raw_sql_and_orm_updates_alike(
    db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    with db.session() as s:
        idea = Idea(track='score', campaign_id=campaign.id, title='t', hypothesis='h', created_by='human')
        s.add(idea)
        s.commit()
        created = idea.updated_at
        assert idea.updated_by is None, 'no update yet'

    def stamp() -> tuple[datetime, str | None]:
        with db.session() as s:
            row = s.get_one(Idea, idea.id)
            return row.updated_at, row.updated_by

    time.sleep(0.01)
    with closing(sql.connect(tmp_path / 'piloop.db', actor=System.HUMAN)) as con:
        sql.run(con, f"UPDATE ideas SET status = 'rejected' WHERE id = {idea.id}")
    raw, by = stamp()
    assert raw > created and by == 'human'

    time.sleep(0.01)
    with db.session() as s:
        same = s.get_one(Idea, idea.id)
        same.priority = 0
        s.commit()
        orm, by = stamp()
        assert orm > raw and by == ACTOR.signature
        assert (same.updated_at, same.updated_by) == (orm, ACTOR.signature), (
            'the instance re-reads what the triggers wrote'
        )

    pinned = created - timedelta(days=1)
    with closing(sql.connect(tmp_path / 'piloop.db', actor=System.HUMAN)) as con:
        sql.run(
            con,
            f"UPDATE ideas SET status = 'parked', updated_at = '{pinned:%Y-%m-%d %H:%M:%S.%f}' WHERE id = {idea.id}",
        )
    assert stamp() == (pinned, 'human'), 'a statement that sets updated_at itself is left alone'


def test_low_water_retries_a_killed_batch_once(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A batch of sweeps that the daemon killed is retried once as a whole.

    A killed batch says nothing about the backlog. A second killed batch in a row is a systemic fault, so research
    rests until a run finishes.
    """
    configure(
        db,
        campaign,
        BLOCKING.format(release=tmp_path / 'never'),
        implementer={'max_concurrent': 3},
        research={'max_ideas': 1, 'timeout': 0.2, 'max_concurrent': 3},
    )
    sched = scheduler(db, runtime, tmp_path)
    assert sched.run_all() == [1, 2, 3]
    sched.wait()
    assert {state for _, state, _ in agent_sessions(db, RESEARCH)} == {AgentSessionState.KILLED}
    assert sched.run_all() == [4, 5, 6], 'the killed siblings are one batch, not three prior attempts'
    assert agent_sessions(db, RESEARCH)[3][2] == (
        '3 free implementer slots without a runnable idea; 0 of 5 runnable ideas; retrying after agent_session#3 was killed:'
        ' agent timeout after 0.2s'
    )
    sched.wait()
    assert sched.run_all() == [], 'two killed batches in a row: six empty sweeps, a rest of 63 times the last one'
    finish_a_run(db, campaign, 'one')
    assert sched.run_all() == [7, 8, 9], 'a run finished since: research is back, killed sweeps are not barren ones'
    sched.stop(timeout=5)


def test_a_cancelled_sweep_is_not_retried_and_its_row_says_who_cancelled_it(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A cancel is a decision, not a crash: neither low_water nor seed requests a sweep to replace it."""
    configure(
        db,
        campaign,
        BLOCKING.format(release=tmp_path / 'never'),
        research={'max_ideas': 1, 'max_concurrent': 1, 'triggers': low_water(0)},
    )
    sched = scheduler(db, runtime, tmp_path)
    assert sched.run_all() == [1]
    wait_until(lambda: agent_sessions(db, RESEARCH)[0][1] is AgentSessionState.RUNNING)
    with db.session() as s:
        old = utcnow() - timedelta(days=1)  # read by the cancelled sweep: no news to the next one
        seed = Idea(track='score', campaign_id=campaign.id, title='s', hypothesis='h', created_by='t', created_at=old)
        seed.status = IdeaStatus.SEED
        s.add(seed)
        request_cancel(s, s.get_one(AgentSession, 1), System.HUMAN, ending=Ending.NOW)
        s.commit()
    assert sched.run_all() == []
    sched.wait()
    assert sched.run_all() == [], 'a running sweep that was cancelled is not retried'
    with db.session() as s:
        requested = AgentSession(kind=RESEARCH, campaign_id=campaign.id, reason='by hand')
        s.add(requested)
        s.flush()
        request_cancel(s, requested, System.HUMAN, ending=Ending.NOW)
        s.commit()
    assert sched.run_all() == []
    assert sched.run_all() == [], 'nor is a cancelled request, and the rest after empty sweeps does not start one'
    with db.session() as s:
        rows = s.scalars(select(AgentSession).order_by(AgentSession.id)).all()
        assert [(w.state, w.error) for w in rows] == [(AgentSessionState.KILLED, 'cancelled by human')] * 2
    finish_a_run(db, campaign, 'one')
    assert sched.run_all() == [3], 'new results do'
    sched.stop(timeout=5)


def test_manual_requests_are_launched_under_max_concurrent(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    release = tmp_path / 'release'
    configure(db, campaign, BLOCKING.format(release=release))
    with closing(sql.connect(tmp_path / 'piloop.db', actor=System.HUMAN)) as con:
        for _ in range(2):
            sql.run(
                con,
                f"INSERT INTO agent_sessions (kind, campaign_id, state) VALUES ('research', {campaign.id}, 'requested')",
            )
    sched = scheduler(db, runtime, tmp_path)
    assert sched.run_all() == [1]
    assert sched.run_all() == []
    wait_for(db, AgentSessionId(1), AgentSessionState.RUNNING)
    assert agent_sessions(db, RESEARCH) == [
        (1, AgentSessionState.RUNNING, None),
        (2, AgentSessionState.REQUESTED, None),
    ]
    release.touch()
    sched.wait()
    assert sched.run_all() == [2]
    sched.wait()
    sched.stop(timeout=5)
    with db.session() as s:
        second = s.get_one(AgentSession, 2)
    assert (second.state, second.harness, second.model) == (AgentSessionState.EXITED, 'script', 'test-model')
    assert (
        'Why this agent started (context, not a scope): requested'
        in (tmp_path / 'agent_sessions' / '2' / 'brief.md').read_text()
    )


def test_an_implementer_request_without_a_run_is_refused(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    with db.session() as s:
        s.add(AgentSession(kind=IMPLEMENTER, campaign_id=campaign.id, reason='by hand'))
        s.commit()
    sched = scheduler(db, runtime, tmp_path)
    assert sched.run_all() == []
    with db.session() as s:
        w = s.get_one(AgentSession, 1)
    assert (w.state, w.error) == (AgentSessionState.KILLED, NO_RUN_ERROR) and w.ended_at is not None
    sched.stop(timeout=1)


def test_requests_go_through_the_agent_sql_path(db: Database, campaign: Campaign, tmp_path: Path) -> None:
    """The SQL statement that the roles docs give agents passes the authorizer and inserts a request."""
    with closing(sql.connect(tmp_path / 'piloop.db', actor=System.HUMAN)) as con:
        result = sql.run(
            con,
            f"INSERT INTO agent_sessions (kind, campaign_id, state) VALUES ('research', {campaign.id}, 'requested')",
        )
        assert result.last_insert_rowid == 1
        row = sql.run(con, 'SELECT kind, state, harness, reason FROM agent_sessions').rows
    assert row == [('research', 'requested', None, None)]
    with (
        closing(sql.connect(tmp_path / 'piloop.db', actor=System.HUMAN)) as con,
        pytest.raises(sqlite3.IntegrityError),
    ):
        sql.run(con, "INSERT INTO agent_sessions (kind, campaign_id, state) VALUES ('janitor', 1, 'requested')")


def test_no_research_sweep_while_the_queue_holds_its_runnable_ideas_for_the_head_to_be_measured(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """Sweeps that ran while every runnable idea waited for a remeasure spent tokens on ideas that could not run.

    Nothing a sweep adds could run sooner. ``low_water`` waits until the head has a score from the current evaluator.
    """
    configure(db, campaign, research={'max_ideas': 1, 'triggers': low_water(2)})
    sched = scheduler(db, runtime, tmp_path)
    occupy(db, campaign)
    add_ideas(db, campaign, 1)
    with db.session() as s:
        evaluator = Evaluator(campaign_id=campaign.id, key='new', definition={})
        s.add(evaluator)
        s.flush()
        s.get_one(Campaign, campaign.id).evaluator_id = evaluator.id
        s.commit()
        new = evaluator.id
    assert sched.run_all() == [], 'the head has no score from the new evaluator: the runnable idea is held'
    sched.wait()
    with db.session() as s:
        if not s.scalars(select(Measurement).where(Measurement.evaluator_id == new)).first():
            s.add(Measurement(candidate_id=1, evaluator_id=new, metrics={'score': 1.0}))
            s.commit()
    assert sched.run_all() == [1], 'the head is measured: below the target, research sweeps again'
    sched.wait()
    sched.stop(timeout=5)
