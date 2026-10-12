"""Each trigger in the roles docs fires exactly when the docs say, and requests launch within ``max_concurrent``."""

import sqlite3
import time
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import select

from piloop.agents.roles import Trigger
from piloop.config import ProjectConfig, WorkerRole, load_project_config
from piloop.daemon.main import ACTOR
from piloop.daemon.runner import RunExecutor
from piloop.daemon.scheduler import NO_RUN_ERROR, Scheduler
from piloop.db import sql
from piloop.db.models import (
    Candidate,
    Event,
    Idea,
    IdeaLink,
    IdeaStatus,
    LinkKind,
    Measurement,
    Project,
    Run,
    RunStatus,
    Verdict,
    Worker,
    WorkerKind,
    WorkerState,
    utcnow,
)
from piloop.db.session import Database
from piloop.otel import Tracer
from piloop.reports.overview import ANOMALY_EVENT, anomalies
from tests.conftest import HARNESS_NOOP, HARNESS_PROMPT_TO_FILE, ScriptRuntime, factory, write_toml

BLOCKING = '#!/bin/sh\nwhile [ ! -f {release} ]; do sleep 0.05; done\n'


def scheduler(db: Database, runtime: ScriptRuntime, tmp_path: Path) -> Scheduler:
    executor = RunExecutor(db, Tracer(None), tmp_path, factory(runtime))
    return Scheduler(db, executor, Tracer(None), tmp_path, factory(runtime))


def configure(db: Database, project: Project, harness_body: str = HARNESS_NOOP, **roles: dict[str, object]) -> None:
    """Rewrite the project's harness script and role settings (merged over the test toml)."""
    write_toml(Path(project.path), harness_body)
    with db.session() as s:
        cfg = load_project_config(Path(project.path), roles=roles)
        s.get_one(Project, project.id).config = cfg.model_dump(mode='json')
        s.commit()


def workers(db: Database, kind: WorkerKind) -> list[tuple[int, WorkerState, str | None]]:
    with db.session() as s:
        rows = s.scalars(select(Worker).where(Worker.kind == kind).order_by(Worker.id)).all()
        return [(w.id, w.state, w.reason) for w in rows]


def fail_a_run(db: Database, project: Project, title: str, status: RunStatus = RunStatus.FAILED) -> int:
    with db.session() as s:
        idea = Idea(
            track='score', project_id=project.id, title=title, hypothesis=title, created_by='t', status=IdeaStatus.DONE
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
            finished_at=utcnow(),
        )
        s.add(run)
        s.commit()
        return run.id


def finish_a_run(db: Database, project: Project, title: str) -> int:
    """Add a run that succeeded just now with a measured discard.

    The research trigger sees a new result. The watchdog trigger sees no failure.
    """
    time.sleep(0.01)
    run_id = fail_a_run(db, project, title, RunStatus.SUCCEEDED)
    with db.session() as s:
        candidate = Candidate(
            project_id=project.id,
            run_id=run_id,
            parent_id=1,
            tree_sha=f'tree{run_id}',
            commit_sha=f'commit{run_id}',
            verdict=Verdict.DISCARD,
        )
        s.add(candidate)
        s.flush()
        evaluator_id = s.get_one(Project, project.id).evaluator_id or 0
        s.add(Measurement(candidate_id=candidate.id, evaluator_id=evaluator_id, metrics={'score': 0.0}))
        s.commit()
    return run_id


def wait_for(db: Database, worker_id: int, state: WorkerState) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        with db.session() as s:
            if s.get_one(Worker, worker_id).state is state:
                return
        time.sleep(0.02)
    raise AssertionError(f'worker#{worker_id} never reached {state}')


def backdate(db: Database, worker_id: int, seconds: float) -> None:
    with db.session() as s:
        w = s.get_one(Worker, worker_id)
        w.started_at = w.started_at - timedelta(seconds=seconds)
        if w.ended_at is not None:
            w.ended_at = w.ended_at - timedelta(seconds=seconds)
        s.commit()


def reported_events(db: Database) -> list[tuple[int, dict[str, object]]]:
    with db.session() as s:
        rows = s.scalars(select(Event).where(Event.kind == ANOMALY_EVENT).order_by(Event.id))
        return [(e.target_id, e.payload) for e in rows]


def test_an_anomaly_requests_an_incident_sweep_once_and_collapses_while_one_is_pending(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    release = tmp_path / 'release'
    configure(db, project, BLOCKING.format(release=release))
    sched = scheduler(db, runtime, tmp_path)
    assert sched.tick() == [], 'nothing went wrong yet'

    fail_a_run(db, project, 'first')
    assert sched.tick() == [1], 'requested and launched on the same tick'
    assert sched.tick() == [], 'the same failure does not fire twice'
    [(worker, payload)] = reported_events(db)
    assert (worker, payload['key']) == (1, 'run#1 failed')
    line = str(payload['anomaly'])
    assert line.startswith('- [incident] run#1 failed, ') and line.endswith(
        ' ago: failed on h (idea#1): no error recorded'
    )

    fail_a_run(db, project, 'second', RunStatus.TIMED_OUT)
    assert sched.tick() == [], 'max_concurrent=1: the sweep for the second failure waits'
    fail_a_run(db, project, 'third')
    sched.tick()
    wait_for(db, 1, WorkerState.RUNNING)
    assert workers(db, WorkerKind.WATCHDOG) == [
        (1, WorkerState.RUNNING, 'anomaly: run#1 failed'),
        (2, WorkerState.REQUESTED, 'anomaly: run#2 failed'),
    ], 'one pending request absorbs later anomalies'

    release.touch()
    sched.wait(10)
    assert sched.tick() == [2], 'the slot frees; the pending request launches'
    sched.wait(10)
    assert sched.tick() == [3], 'the anomaly that came while a request was pending gets its own sweep'
    assert [(w, p['key']) for w, p in reported_events(db)] == [
        (1, 'run#1 failed'),
        (2, 'run#2 failed'),
        (3, 'run#3 failed'),
    ]
    sched.wait(10)
    sched.stop(timeout=5)
    again = scheduler(db, runtime, tmp_path)
    assert again.tick() == [], 'the events are the record: a restarted daemon replays nothing'
    again.stop(timeout=5)
    assert [w[1] for w in workers(db, WorkerKind.WATCHDOG)] == [WorkerState.EXITED] * 3


def test_an_anomaly_takes_the_tick_and_does_not_reset_the_timer(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """When a timer is due and a new failure arrives in the same tick, the incident sweep starts first."""
    configure(db, project, HARNESS_PROMPT_TO_FILE, watchdog={'interval': 30})
    with db.session() as s:
        s.get_one(Project, project.id).created_at = utcnow() - timedelta(seconds=31)
        s.commit()
    fail_a_run(db, project, 'broken')
    sched = scheduler(db, runtime, tmp_path)
    assert sched.tick() == [1]
    sched.wait(10)
    assert sched.tick() == [2], 'the incident sweep is not a progress review: the timer is still due'
    sched.wait(10)
    assert workers(db, WorkerKind.WATCHDOG) == [
        (1, WorkerState.EXITED, 'anomaly: run#1 failed'),
        (2, WorkerState.EXITED, 'timer: 30s'),
    ]
    assert sched.tick() == [], 'the timer counts from the progress review'

    incident = (tmp_path / 'workers' / '1' / 'brief.md').read_text()
    assert incident.index('# Watchdog') < incident.index('## This sweep: an incident') < incident.index('# Demo')
    assert '## Incidents to investigate\n- [incident] run#1 failed' in incident
    assert '## Lay of the land' in incident and '## History' not in incident
    progress = (tmp_path / 'workers' / '2' / 'brief.md').read_text()
    assert '## This sweep: a progress review' in progress and '## This sweep: an incident' not in progress
    assert '## Incidents to investigate' not in progress
    assert progress.index('## Lay of the land') < progress.index('## History: ideas')
    sched.stop(timeout=5)


def test_only_incident_anomalies_request_a_sweep(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """An idle queue goes into the progress review, not into an incident sweep."""
    sched = scheduler(db, runtime, tmp_path)
    with db.session() as s:
        p = s.get_one(Project, project.id)
        [idle] = anomalies(s, p, load_project_config(Path(project.path)), utcnow(), lambda w: [])
    assert idle.key == 'project#1 idle' and not idle.incident
    assert sched.tick() == []
    assert workers(db, WorkerKind.WATCHDOG) == []
    sched.stop(timeout=1)


def test_cron_fires_once_per_interval(db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path) -> None:
    configure(db, project, watchdog={'interval': 30})
    sched = scheduler(db, runtime, tmp_path)
    assert sched.tick() == [], 'the project was registered a moment ago'
    with db.session() as s:
        s.get_one(Project, project.id).created_at = utcnow() - timedelta(seconds=31)
        s.commit()
    assert sched.tick() == [1]
    assert workers(db, WorkerKind.WATCHDOG)[0][2] == 'timer: 30s'
    sched.wait(10)
    assert sched.tick() == [], 'not due again'
    backdate(db, 1, 31)
    assert sched.tick() == [2]
    sched.wait(10)

    configure(db, project, watchdog={'interval': 0})
    backdate(db, 2, 3600)
    assert sched.tick() == [], 'interval 0 turns the timer off'
    sched.stop(timeout=5)


def occupy(db: Database, project: Project, title: str = 'busy') -> int:
    """Add a run in flight that holds a worker slot without the queue starting anything."""
    with db.session() as s:
        idea = Idea(
            track='score',
            project_id=project.id,
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
    db: Database, project: Project, count: int, by: str = 'human', status: IdeaStatus = IdeaStatus.READY
) -> list[int]:
    with db.session() as s:
        ideas = [
            Idea(track='score', project_id=project.id, title=f'{by} {i}', hypothesis='h', created_by=by, status=status)
            for i in range(count)
        ]
        s.add_all(ideas)
        s.commit()
        return [idea.id for idea in ideas]


def consume(db: Database, idea_id: int) -> None:
    with db.session() as s:
        s.get_one(Idea, idea_id).status = IdeaStatus.DONE
        s.commit()


def last_reason(db: Database) -> str | None:
    return workers(db, WorkerKind.RESEARCH)[-1][2]


def take(db: Database, worker_id: int, seconds: float, ago: float = 0) -> None:
    """Make an ended sweep have taken ``seconds`` and have ended ``ago`` seconds ago."""
    with db.session() as s:
        w = s.get_one(Worker, worker_id)
        assert w.ended_at is not None
        ended = utcnow() - timedelta(seconds=ago)
        w.ended_at, w.started_at = ended, ended - timedelta(seconds=seconds)
        s.commit()


def test_low_water_keeps_a_slot_s_worth_plus_the_buffer_and_tops_it_up_on_new_results(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """Low water keeps one idea per worker slot plus ``buffer`` ready, even when every slot is busy.

    Below that target a sweep starts. Each sweep that defined no idea doubles the number of finished runs the next
    sweep waits for. A sweep that defined an idea resets the wait.
    """
    configure(db, project, research={'max_ideas': 1, 'buffer': 2})
    sched = scheduler(db, runtime, tmp_path)
    occupy(db, project)
    first, *_ = add_ideas(db, project, 3)
    assert sched.tick() == [], 'one worker slot plus a buffer of two: three runnable ideas is the target'

    consume(db, first)
    assert sched.tick() == [1], 'below the target with no slot free: the first sweep needs nothing'
    assert last_reason(db) == '2 of 3 runnable ideas'
    sched.wait(10)
    take(db, 1, seconds=600)
    assert sched.tick() == [], 'nothing finished since the sweep started, which defined nothing: research rests'

    finish_a_run(db, project, 'one')
    assert sched.tick() == [], 'the sweep defined nothing: the next one waits for two finished runs'
    finish_a_run(db, project, 'two')
    assert sched.tick() == [2]
    assert last_reason(db) == (
        '2 of 3 runnable ideas; 2 runs finished since worker#1 started (after 1 sweeps that defined no idea)'
    )
    sched.wait(10)

    add_ideas(db, project, 1, by='research#2')
    finish_a_run(db, project, 'three')
    assert sched.tick() == [], 'back at the target: new results alone ask for nothing'
    consume(db, first + 1)
    assert sched.tick() == [3], 'worker#2 inserted an idea: the back-off is reset, one finished run is enough'
    assert last_reason(db) == '2 of 3 runnable ideas; 1 runs finished since worker#2 started'
    sched.wait(10)
    sched.stop(timeout=5)


def test_low_water_keeps_sweeping_a_short_backlog_though_every_slot_is_busy_resting_only_after_empty_sweeps(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A short backlog starts a sweep even when every worker slot is busy and no new result exists.

    A sweep that defined an idea is followed at once. A sweep that defined no idea is followed by a rest as long as
    the sweep took.
    """
    configure(db, project, research={'max_ideas': 1, 'buffer': 2})
    sched = scheduler(db, runtime, tmp_path)
    occupy(db, project)
    assert sched.tick() == [1]
    sched.wait(10)
    add_ideas(db, project, 1, by='research#1')
    assert sched.tick() == [2], 'worker#1 defined an idea, the backlog is still short: no result needed'
    assert last_reason(db) == '1 of 3 runnable ideas'
    sched.wait(10)
    take(db, 2, seconds=60)
    assert sched.tick() == [], 'worker#2 defined nothing: research rests as long as it took'
    take(db, 2, seconds=60, ago=61)
    assert sched.tick() == [3]
    assert last_reason(db) == '1 of 3 runnable ideas; rested 60s after 1 sweeps that defined no idea'
    sched.wait(10)
    sched.stop(timeout=5)


def test_a_seed_fires_one_research_sweep_however_full_the_backlog_once(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A new seed starts one research sweep, even when the backlog is at its target or ``low_water`` is off.

    A seed is new until a sweep starts after it was filed. A sweep that read the seed and left it does not start
    another sweep.
    """
    configure(db, project, research={'max_ideas': 0, 'max_concurrent': 1})
    sched = scheduler(db, runtime, tmp_path)
    occupy(db, project)
    assert sched.tick() == [], 'max_ideas 0 turns low_water off'
    add_ideas(db, project, 1, by='watchdog#9', status=IdeaStatus.SEED)
    assert sched.tick() == [1]
    assert last_reason(db) == '1 new seeds to develop'
    sched.wait(10)
    assert sched.tick() == [], 'worker#1 read the seed and left it: the next sweep, whatever fires it, lists it again'
    time.sleep(0.01)
    add_ideas(db, project, 1, by='watchdog#3', status=IdeaStatus.SEED)
    assert sched.tick() == [2]
    assert last_reason(db) == '1 new seeds to develop'
    sched.wait(10)
    sched.stop(timeout=5)


def test_a_killed_sweep_s_seeds_are_retried_once_and_a_new_seed_waits_for_a_free_research_slot(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    release = tmp_path / 'release'
    configure(
        db,
        project,
        BLOCKING.format(release=release),
        research={'max_ideas': 0, 'max_concurrent': 1, 'timeout': 0.5},
    )
    sched = scheduler(db, runtime, tmp_path)
    occupy(db, project)
    add_ideas(db, project, 1, by='watchdog#9', status=IdeaStatus.SEED)
    assert sched.tick() == [1]
    sched.wait(10)
    assert sched.tick() == [2]
    assert last_reason(db) == '1 seeds to develop; retrying after worker#1 was killed: worker timeout after 0.5s'
    sched.wait(10)
    assert sched.tick() == [], 'a second killed batch is a systemic fault: no more retries'

    configure(db, project, BLOCKING.format(release=release), research={'max_ideas': 0, 'max_concurrent': 1})
    add_ideas(db, project, 1, by='watchdog#10', status=IdeaStatus.SEED)
    assert sched.tick() == [3], 'a seed filed after the latest sweep started is new'
    deadline = time.monotonic() + 10
    while workers(db, WorkerKind.RESEARCH)[-1][1] is not WorkerState.RUNNING and time.monotonic() < deadline:
        time.sleep(0.02)
    add_ideas(db, project, 1, by='watchdog#11', status=IdeaStatus.SEED)
    assert sched.tick() == [], 'a new seed, but the only research slot is busy'
    release.touch()
    sched.wait(10)
    sched.stop(timeout=5)


def finish(db: Database, run_id: int) -> None:
    with db.session() as s:
        run = s.get_one(Run, run_id)
        run.status, run.finished_at = RunStatus.SUCCEEDED, utcnow()
        s.commit()


def test_after_a_barren_sweep_research_never_waits_for_more_results_than_the_runs_in_flight_can_give(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """After sweeps that defined no idea, research waits for no more finished runs than the runs in flight can give.

    Idle worker slots end the rest on time. When the last run in flight finishes, no more results can come, so that
    one result is enough.
    """
    configure(db, project, worker={'max_concurrent': 4}, research={'max_ideas': 2, 'max_concurrent': 1})
    sched = scheduler(db, runtime, tmp_path)
    in_flight = occupy(db, project)
    assert sched.tick() == [1]
    sched.wait(10)
    take(db, 1, seconds=600, ago=60)
    assert sched.tick() == [], 'barren: two finished runs wanted, none yet, and the idle rest has 540 s to go'
    take(db, 1, seconds=600, ago=601)
    assert sched.tick() == [2], 'three slots idle, no result: the rest is over'
    assert last_reason(db) == (
        '3 free worker slots without a runnable idea; 0 of 6 runnable ideas; rested 600s after 1 sweeps that defined'
        ' no idea'
    )
    sched.wait(10)

    take(db, 2, seconds=600, ago=60)
    finish(db, in_flight)
    assert sched.tick() == [3], 'two barren sweeps want four finished runs, but nothing else is in flight'
    assert last_reason(db) == (
        '4 free worker slots without a runnable idea; 0 of 6 runnable ideas; 1 runs finished since worker#2 started'
        ' (after 2 sweeps that defined no idea)'
    )
    sched.wait(10)
    sched.stop(timeout=5)


def test_low_water_sizes_the_batch_to_the_shortfall_net_of_the_sweeps_in_flight(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """Low water starts only as many sweeps as the shortfall needs.

    Five worker slots plus a buffer of one is a target of six ideas, so three sweeps of two ideas start, though four
    may run. Each starts on its own harness. A sweep in flight already counts for its ``max_ideas``.
    """
    release = tmp_path / 'release'
    write_toml(Path(project.path), BLOCKING.format(release=release))
    script = {'executable': str(tmp_path / 'harness.sh'), 'headless': ['{prompt}'], 'interactive': []}
    cfg = load_project_config(
        Path(project.path),
        harness={'b': script, 'c': script},
        roles={
            'worker': {'max_concurrent': 5},
            'research': {
                'harnesses': ['script', 'b', 'c'],
                'model': {'b': 'mb', 'c': 'mc'},
                'max_concurrent': 4,
                'max_ideas': 2,
                'buffer': 1,
            },
        },
    )
    with db.session() as s:
        s.get_one(Project, project.id).config = cfg.model_dump(mode='json')
        s.commit()
    sched = scheduler(db, runtime, tmp_path)
    assert sched.tick() == [1, 2, 3]
    assert [w[2] for w in workers(db, WorkerKind.RESEARCH)] == [
        '5 free worker slots without a runnable idea; 0 of 6 runnable ideas'
    ] * 3
    assert sched.tick() == [], 'the three in flight may insert the six: a fourth would overshoot'
    release.touch()
    sched.wait(10)
    with db.session() as s:
        picks = [(w.harness, w.model) for w in s.scalars(select(Worker).order_by(Worker.id))]
    assert picks == [('script', 'test-model'), ('b', 'mb'), ('c', 'mc')]

    with db.session() as s:  # a sweep in flight, started by hand
        s.add(Worker(kind=WorkerKind.RESEARCH, project_id=project.id, state=WorkerState.RUNNING))
        s.commit()
    add_ideas(db, project, 1, by='research#3', status=IdeaStatus.PROPOSED)
    assert sched.tick() == [5, 6], 'six wanted, two coming: two sweeps of two'
    assert last_reason(db) == (
        '3 free worker slots without a runnable idea; 0 of 6 runnable ideas, 2 more from 1 sweeps in flight'
    )
    sched.wait(10)
    sched.stop(timeout=5)


def test_low_water_counts_ideas_waiting_only_on_a_prerequisite_in_flight_as_backlog(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """Low water counts an idea that waits only on prerequisites in flight as backlog.

    Such an idea runs when its prerequisites finish, with no sweep in between. An idea that waits on a prerequisite
    that is not running is not backlog.
    """
    configure(db, project, worker={'max_concurrent': 2}, research={'max_ideas': 1, 'max_concurrent': 1, 'buffer': 1})
    sched = scheduler(db, runtime, tmp_path)
    with db.session() as s:
        prerequisite = s.get_one(Run, occupy(db, project)).idea
        prerequisite.status = IdeaStatus.SCHEDULED
        s.commit()
        prerequisite_id = prerequisite.id
    occupy(db, project, 'busy too')
    with db.session() as s:
        s.add_all(
            Idea(
                track='score',
                project_id=project.id,
                title=f'after {i}',
                hypothesis='h',
                created_by='research#1' if i else 'human',
                links=[IdeaLink(target_id=prerequisite_id, kind=LinkKind.AFTER)],
            )
            for i in range(2)
        )
        s.commit()
    assert sched.tick() == [1]
    assert last_reason(db) == '0 of 3 runnable ideas, 2 more waiting on runs in flight'
    sched.wait(10)
    add_ideas(db, project, 1, by='research#1')
    assert sched.tick() == [], 'both slots busy, one idea runnable and two waiting on the run in flight: three'
    with db.session() as s:
        s.get_one(Idea, prerequisite_id).status = IdeaStatus.PARKED
        s.commit()
    assert sched.tick() == [2], 'a parked prerequisite is no run in flight: the two behind it are blocked'
    assert last_reason(db) == '1 of 3 runnable ideas'
    sched.wait(10)
    sched.stop(timeout=5)


def test_low_water_requests_nothing_while_every_research_slot_is_busy(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    release = tmp_path / 'release'
    configure(db, project, BLOCKING.format(release=release), research={'max_ideas': 1, 'max_concurrent': 1})
    sched = scheduler(db, runtime, tmp_path)
    assert sched.tick() == [1]
    finish_a_run(db, project, 'one')
    assert sched.tick() == [], 'one sweep of one cannot fill a target of three, but the only research slot is busy'
    release.touch()
    sched.wait(10)
    sched.stop(timeout=5)


def test_low_water_wants_no_more_ideas_than_max_runs_leaves_to_run(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    release = tmp_path / 'release'
    write_toml(Path(project.path), BLOCKING.format(release=release))
    cfg = load_project_config(Path(project.path), roles={'research': {'max_ideas': 1}}, stop={'max_runs': 3})
    with db.session() as s:
        s.get_one(Project, project.id).config = cfg.model_dump(mode='json')
        s.commit()
    finish_a_run(db, project, 'one')
    add_ideas(db, project, 2)
    sched = scheduler(db, runtime, tmp_path)
    assert sched.tick() == [1], 'a run of the first idea; one run left, one idea for it: no research'
    assert workers(db, WorkerKind.RESEARCH) == []
    release.touch()
    sched.wait(10)
    sched.stop(timeout=5)


def test_ideas_updated_at_is_bumped_by_raw_sql_and_orm_updates_alike(
    db: Database, project: Project, tmp_path: Path
) -> None:
    with db.session() as s:
        idea = Idea(track='score', project_id=project.id, title='t', hypothesis='h', created_by='human')
        s.add(idea)
        s.commit()
        created = idea.updated_at
        assert idea.updated_by is None, 'no update yet'

    def stamp() -> tuple[datetime, str | None]:
        with db.session() as s:
            row = s.get_one(Idea, idea.id)
            return row.updated_at, row.updated_by

    time.sleep(0.01)
    with closing(sql.connect(tmp_path / 'piloop.db', actor='human')) as con:
        sql.run(con, f"UPDATE ideas SET status = 'rejected' WHERE id = {idea.id}")
    raw, by = stamp()
    assert raw > created and by == 'human'

    time.sleep(0.01)
    with db.session() as s:
        same = s.get_one(Idea, idea.id)
        same.priority = 0
        s.commit()
        orm, by = stamp()
        assert orm > raw and by == ACTOR
        assert (same.updated_at, same.updated_by) == (orm, ACTOR), 'the instance re-reads what the triggers wrote'

    pinned = created - timedelta(days=1)
    with closing(sql.connect(tmp_path / 'piloop.db', actor='human')) as con:
        sql.run(
            con,
            f"UPDATE ideas SET status = 'parked', updated_at = '{pinned:%Y-%m-%d %H:%M:%S.%f}' WHERE id = {idea.id}",
        )
    assert stamp() == (pinned, 'human'), 'a statement that sets updated_at itself is left alone'


def test_low_water_retries_a_killed_batch_once(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A batch of sweeps that the daemon killed is retried once as a whole.

    A killed batch says nothing about the backlog. A second killed batch in a row is a systemic fault, so research
    rests until a run finishes.
    """
    configure(
        db,
        project,
        BLOCKING.format(release=tmp_path / 'never'),
        worker={'max_concurrent': 3},
        research={'max_ideas': 1, 'timeout': 0.2, 'max_concurrent': 3},
    )
    sched = scheduler(db, runtime, tmp_path)
    assert sched.tick() == [1, 2, 3]
    sched.wait(10)
    assert {state for _, state, _ in workers(db, WorkerKind.RESEARCH)} == {WorkerState.KILLED}
    assert sched.tick() == [4, 5, 6], 'the killed siblings are one batch, not three prior attempts'
    assert workers(db, WorkerKind.RESEARCH)[3][2] == (
        '3 free worker slots without a runnable idea; 0 of 5 runnable ideas; retrying after worker#3 was killed:'
        ' worker timeout after 0.2s'
    )
    sched.wait(10)
    assert sched.tick() == [], 'two killed batches in a row: six empty sweeps, a rest of 63 times the last one'
    finish_a_run(db, project, 'one')
    assert sched.tick() == [7, 8, 9], 'a run finished since: research is back, killed sweeps are not barren ones'
    sched.stop(timeout=5)


def test_a_trigger_needs_its_settings_on_the_role_config(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    sched = scheduler(db, runtime, tmp_path)
    with db.session() as s, pytest.raises(TypeError, match='WorkerRole has no settings for the low_water trigger'):
        project_row = s.get_one(Project, project.id)
        cfg = ProjectConfig.model_validate(project_row.config)
        sched._fires(s, project_row, cfg, WorkerRole(), Trigger.LOW_WATER, None)


def test_manual_requests_are_launched_under_max_concurrent(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    release = tmp_path / 'release'
    configure(db, project, BLOCKING.format(release=release))
    with closing(sql.connect(tmp_path / 'piloop.db', actor='human')) as con:
        for _ in range(2):
            sql.run(
                con, f"INSERT INTO workers (kind, project_id, state) VALUES ('research', {project.id}, 'requested')"
            )
    sched = scheduler(db, runtime, tmp_path)
    assert sched.tick() == [1]
    assert sched.tick() == []
    wait_for(db, 1, WorkerState.RUNNING)
    assert workers(db, WorkerKind.RESEARCH) == [(1, WorkerState.RUNNING, None), (2, WorkerState.REQUESTED, None)]
    release.touch()
    sched.wait(10)
    assert sched.tick() == [2]
    sched.wait(10)
    sched.stop(timeout=5)
    with db.session() as s:
        second = s.get_one(Worker, 2)
    assert (second.state, second.harness, second.model) == (WorkerState.EXITED, 'script', 'test-model')
    assert 'Trigger: requested' in (tmp_path / 'workers' / '2' / 'brief.md').read_text()


def test_a_worker_request_without_a_run_is_refused(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    with db.session() as s:
        s.add(Worker(kind=WorkerKind.WORKER, project_id=project.id, reason='by hand'))
        s.commit()
    sched = scheduler(db, runtime, tmp_path)
    assert sched.tick() == []
    with db.session() as s:
        w = s.get_one(Worker, 1)
    assert (w.state, w.error) == (WorkerState.KILLED, NO_RUN_ERROR) and w.ended_at is not None
    sched.stop(timeout=1)


def test_requests_go_through_the_agent_sql_path(db: Database, project: Project, tmp_path: Path) -> None:
    """The SQL statement that the roles docs give agents passes the authorizer and inserts a request."""
    with closing(sql.connect(tmp_path / 'piloop.db', actor='human')) as con:
        result = sql.run(
            con, f"INSERT INTO workers (kind, project_id, state) VALUES ('research', {project.id}, 'requested')"
        )
        assert result.last_insert_rowid == 1
        row = sql.run(con, 'SELECT kind, state, harness, reason FROM workers').rows
    assert row == [('research', 'requested', None, None)]
    with closing(sql.connect(tmp_path / 'piloop.db', actor='human')) as con, pytest.raises(sqlite3.IntegrityError):
        sql.run(con, "INSERT INTO workers (kind, project_id, state) VALUES ('janitor', 1, 'requested')")
