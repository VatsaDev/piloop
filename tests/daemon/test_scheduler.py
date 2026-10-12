import json
import re
import threading
import time
from contextlib import closing
from datetime import timedelta
from pathlib import Path

import pytest
from dirty_equals import IsStr
from git import Repo
from inline_snapshot import snapshot
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode
from pytest_mock import MockerFixture
from sqlalchemy import func, select

from piloop import git, proc
from piloop.agents.launcher import STARTUP_ANOMALY
from piloop.agents.roles import compose_brief
from piloop.campaign.project import sync
from piloop.campaign.stop import STALE_ERROR
from piloop.config import ProjectConfig, load_project_config
from piloop.daemon.runner import RunExecutor
from piloop.daemon.scheduler import STARTUP_BREAKER, STARTUP_WINDOW, STOPPED_ERROR, Scheduler
from piloop.db import sql
from piloop.db.models import (
    Candidate,
    Comment,
    CommentKind,
    Event,
    Idea,
    IdeaLink,
    IdeaStatus,
    LinkKind,
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
from piloop.db.triggers import DAEMON_ACTOR
from piloop.otel import Tracer
from piloop.reports.overview import SILENT_SECONDS
from piloop.runtime import Handle
from piloop.scoring.ledger import current_evaluator, head, import_reference
from tests.conftest import (
    HARNESS_APPEND,
    HARNESS_SLEEPS,
    HARNESS_TWO_COMMITS,
    Gate,
    ScriptRuntime,
    events,
    factory,
    span_named,
    wait_for_worker_handle,
    write_toml,
)
from tests.usage.test_usage import PI_ID, store


def make_scheduler(db: Database, runtime: ScriptRuntime, tmp_path: Path, tracer: Tracer | None = None) -> Scheduler:
    tracer = tracer or Tracer(None)
    executor = RunExecutor(db, tracer, tmp_path, factory(runtime))
    return Scheduler(db, executor, tracer, tmp_path, factory(runtime))


def request_watchdog(db: Database, project: Project) -> int:
    with db.session() as s:
        worker = Worker(kind=WorkerKind.WATCHDOG, project_id=project.id, reason='by hand')
        s.add(worker)
        s.commit()
        return worker.id


def add_ideas(
    db: Database,
    project: Project,
    *titles: str,
    priority: int = 2,
    harness: str | None = None,
    status: IdeaStatus = IdeaStatus.READY,
    after: tuple[int, ...] = (),
    supersedes: tuple[int, ...] = (),
    track: str = 'score',
) -> list[int]:
    with db.session() as s:
        ideas = [
            Idea(
                track=track,
                project_id=project.id,
                title=t,
                hypothesis=t,
                priority=priority,
                harness=harness,
                status=status,
                created_by='test',
                links=[
                    *(IdeaLink(target_id=a, kind=LinkKind.AFTER) for a in after),
                    *(IdeaLink(target_id=o, kind=LinkKind.SUPERSEDES) for o in supersedes),
                ],
            )
            for t in titles
        ]
        s.add_all(ideas)
        s.commit()
        return [i.id for i in ideas]


def statuses(db: Database) -> list[IdeaStatus]:
    with db.session() as s:
        return [i.status for i in s.scalars(select(Idea).order_by(Idea.id))]


def set_config(db: Database, project: Project, **overrides: object) -> None:
    with db.session() as s:
        s.get_one(Project, project.id).config = load_project_config(Path(project.path), **overrides).model_dump(
            mode='json'
        )
        s.commit()


def test_ready_ideas_become_runs_in_priority_order(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    scheduler = make_scheduler(db, runtime, tmp_path)
    (low,) = add_ideas(db, project, 'low', priority=3)
    (high,) = add_ideas(db, project, 'high', priority=0)

    assert scheduler.tick() == [1]
    with db.session() as s:
        run = s.get_one(Run, 1)
        assert (run.idea_id, run.attempt, run.harness, run.model) == (high, 1, 'script', 'test-model')
        assert s.get_one(Idea, high).status is IdeaStatus.SCHEDULED
        worker = s.get_one(Worker, 1)
        assert (worker.kind, worker.run_id, worker.harness, worker.model, worker.reason) == (
            WorkerKind.WORKER,
            1,
            'script',
            'test-model',
            f'idea#{high} is ready',
        )
    assert scheduler.tick() == [], 'max_concurrent=1: the second idea waits'
    scheduler.wait(timeout=30)

    assert scheduler.tick() == [2]
    scheduler.wait(timeout=30)
    scheduler.stop(timeout=1)
    with db.session() as s:
        assert [(i.id, i.status) for i in s.scalars(select(Idea).order_by(Idea.id))] == [
            (low, IdeaStatus.DONE),
            (high, IdeaStatus.DONE),
        ]
        assert [m.metrics['score'] for m in s.scalars(select(Measurement).order_by(Measurement.id))] == [1.0, 2.0, 3.0]
        second = s.get_one(Run, 2)
        assert second.base_candidate_id == 2, 'the second run builds on the first kept result'
    assert scheduler.active() == []


def test_a_sweep_checks_out_the_head(db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path) -> None:
    scheduler = make_scheduler(db, runtime, tmp_path)
    add_ideas(db, project, 'more')
    assert scheduler.tick() == [1]
    scheduler.wait(timeout=30)
    with db.session() as s:
        best = s.get_one(Candidate, 2)
        assert best.verdict is Verdict.KEEP and best.commit_sha != s.get_one(Candidate, 1).commit_sha
        best_sha = best.commit_sha
    write_toml(Path(project.path), '#!/bin/sh\ngit rev-parse HEAD > workspace/head.txt\n')
    set_config(db, project)
    worker_id = request_watchdog(db, project)
    assert scheduler.tick() == [worker_id]
    scheduler.wait(timeout=30)
    head = tmp_path / 'projects' / str(project.id) / 'workspace' / 'head.txt'
    assert head.read_text().strip() == best_sha
    assert (
        'detached checkout of candidate#2, the head (score=2.0)'
        in (tmp_path / 'workers' / str(worker_id) / 'brief.md').read_text()
    )
    assert git.worktrees_under(Repo(project.path), tmp_path / 'workers') == [], 'removed with the sweep'
    with db.session() as s:
        cfg = load_project_config(Path(project.path), evaluate=['sh', '-c', 'echo METRIC score=9; echo METRIC size=9'])
        s.get_one(Project, project.id).config = cfg.model_dump(mode='json')
        current_evaluator(s, s.get_one(Project, project.id), cfg)
        s.commit()
    again = request_watchdog(db, project)
    assert scheduler.tick() == [again]
    scheduler.wait(timeout=30)
    scheduler.stop(timeout=1)
    assert head.read_text().strip() == best_sha, (
        'a new evaluator: nothing is measured under it yet, and the head stays the latest trunk keep'
    )
    assert (
        'detached checkout of candidate#2, the head (score=unmeasured)'
        in (tmp_path / 'workers' / str(again) / 'brief.md').read_text()
    )


def test_harness_rotation_and_pinning(db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path) -> None:
    write_toml(Path(project.path), '#!/bin/sh\ntrue\n')
    cfg = load_project_config(
        Path(project.path),
        harnesses=['script', 'pi'],
        model={'script': 'm', 'pi': 'p'},
        roles={'worker': {'max_concurrent': 3}},
    )
    with db.session() as s:
        s.get_one(Project, project.id).config = cfg.model_dump(mode='json')
        s.commit()
    add_ideas(db, project, 'a', 'b')
    add_ideas(db, project, 'c', harness='script')
    scheduler = make_scheduler(db, runtime, tmp_path)
    assert scheduler.tick() == [1, 2, 3]
    scheduler.wait(timeout=30)
    scheduler.stop(timeout=1)
    with db.session() as s:
        assert [(r.harness, r.model) for r in s.scalars(select(Run).order_by(Run.id))] == snapshot(
            [('script', 'm'), ('pi', 'p'), ('script', 'm')]
        )


def test_a_harness_at_its_cap_passes_its_turn_and_an_idea_pinned_to_it_waits(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A harness at its ``max_concurrent`` cap passes its turn, and an idea pinned to it waits.

    ``max_concurrent = {claude = N}`` limits a project to N Claude agents while other harnesses fill the free slots.
    """
    set_config(
        db,
        project,
        harnesses=['script', 'pi'],
        model={'script': 'm', 'pi': 'p'},
        max_concurrent={'script': 1, 'pi': 2},
        roles={'worker': {'max_concurrent': 5}},
    )
    a, b, c, d = add_ideas(db, project, 'a', 'b', 'c', 'd')
    (pinned,) = add_ideas(db, project, 'pinned', harness='script', priority=0)
    with db.session() as s:
        s.add(Worker(kind=WorkerKind.WORKER, project_id=project.id, harness='script', state=WorkerState.RUNNING))
        s.commit()
    scheduler = make_scheduler(db, runtime, tmp_path)
    with db.session() as s:
        p = s.get_one(Project, project.id)
        scheduler._queue(s, p, ProjectConfig.model_validate(p.config))
        s.commit()
    with db.session() as s:
        assert [(r.idea_id, r.harness) for r in s.scalars(select(Run).order_by(Run.id))] == [(a, 'pi'), (b, 'pi')], (
            "script's turn went to pi; once pi was full too nothing more was queued"
        )
        assert [s.get_one(Idea, i).status for i in (c, d, pinned)] == [IdeaStatus.READY] * 3, 'the pin waits'


def test_a_sweep_passes_over_a_harness_at_its_cap_and_a_pinned_one_stays_requested(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    set_config(
        db,
        project,
        harnesses=['script', 'pi'],
        model={'script': 'm', 'pi': 'p'},
        max_concurrent={'script': 1},
        roles={'watchdog': {'harnesses': ['script', 'pi'], 'max_concurrent': 4}},
    )
    with db.session() as s:
        rows = [Worker(kind=WorkerKind.WATCHDOG, project_id=project.id, reason='by hand') for _ in range(3)]
        rows.append(Worker(kind=WorkerKind.WATCHDOG, project_id=project.id, harness='script', reason='pinned'))
        s.add_all(rows)
        s.commit()
    scheduler = make_scheduler(db, runtime, tmp_path)
    with db.session() as s:
        p = s.get_one(Project, project.id)
        taken = scheduler._take(s, p, ProjectConfig.model_validate(p.config))
        s.commit()
    assert [w for w, _ in taken] == [1, 2, 3]
    with db.session() as s:
        assert [(w.harness, w.state) for w in s.scalars(select(Worker).order_by(Worker.id))] == [
            ('script', WorkerState.STARTING),
            ('pi', WorkerState.STARTING),
            ('pi', WorkerState.STARTING),
            ('script', WorkerState.REQUESTED),
        ], "the third sweep's turn was script's, at its cap"


def test_a_queued_run_whose_harness_left_the_rotation_launches_on_the_new_one_unless_its_idea_pins_it(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A queued run whose harness left the rotation starts on a harness that is still in it, unless its idea pins it."""
    set_config(
        db, project, harnesses=['script'], model={'script': 'm', 'pi': 'p'}, roles={'worker': {'max_concurrent': 3}}
    )
    a, b = add_ideas(db, project, 'a', 'b')
    (pinned,) = add_ideas(db, project, 'pinned', harness='script')
    scheduler = make_scheduler(db, runtime, tmp_path)
    with db.session() as s:
        p = s.get_one(Project, project.id)
        scheduler._queue(s, p, ProjectConfig.model_validate(p.config))
        s.commit()
    set_config(
        db,
        project,
        harnesses=['pi'],
        model={'script': 'm', 'pi': 'p'},
        max_concurrent={'pi': 1},
        roles={'worker': {'max_concurrent': 3}},
    )
    with db.session() as s:
        p = s.get_one(Project, project.id)
        scheduler._take(s, p, ProjectConfig.model_validate(p.config))
        s.commit()
    with db.session() as s:
        runs = s.scalars(select(Run).order_by(Run.id)).all()
        assert [(r.idea_id, r.harness, r.model) for r in runs] == [
            (a, 'pi', 'p'),
            (b, 'script', 'm'),
            (pinned, 'script', 'm'),
        ]
        assert [(w.harness, w.model, w.state) for w in s.scalars(select(Worker).order_by(Worker.id))] == [
            ('pi', 'p', WorkerState.STARTING),
            ('script', 'm', WorkerState.REQUESTED),
            ('script', 'm', WorkerState.STARTING),
        ], 'b waits for a pi slot; the pin is launched where it was pinned'
        [rerouted] = s.scalars(select(Event).where(Event.kind == 'run.rerouted')).all()
        assert (rerouted.target_id, rerouted.payload) == (runs[0].id, {'from': 'script', 'to': 'pi'})


def test_idea_without_a_base_is_parked(db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path) -> None:
    with db.session() as s:
        for c in s.scalars(select(Candidate)):
            c.verdict = Verdict.DISCARD
        s.commit()
    (idea,) = add_ideas(db, project, 'orphan')
    scheduler = make_scheduler(db, runtime, tmp_path)
    assert scheduler.tick() == []
    with db.session() as s:
        assert s.get_one(Idea, idea).status is IdeaStatus.PARKED


def test_a_run_starts_from_the_best_as_of_its_start_not_as_of_the_idea(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A run starts from the head at the time the run starts, not at the time its idea was written."""
    (idea,) = add_ideas(db, project, 'written while the baseline was best')
    with db.session() as s:
        s.add(Candidate(project_id=project.id, tree_sha='b', commit_sha='b', verdict=Verdict.KEEP, verdict_by='human'))
        s.flush()
        s.add(Measurement(candidate_id=2, evaluator_id=1, metrics={'score': 9, 'size': 9}))
        s.commit()
    scheduler = make_scheduler(db, runtime, tmp_path)
    with db.session() as s:
        p = s.get_one(Project, project.id)
        scheduler._queue(s, p, ProjectConfig.model_validate(p.config))
        s.commit()
    with db.session() as s:
        assert (s.get_one(Run, 1).idea_id, s.get_one(Run, 1).base_candidate_id) == (idea, 2)


def test_a_pin_is_honored_over_the_after_link_and_the_run_records_both_bases(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A pin to an older candidate wins over the ``after`` link, and ``run.queued`` records both bases."""
    set_config(db, project, roles={'worker': {'max_concurrent': 2}})
    (done,) = add_ideas(db, project, 'prerequisite', status=IdeaStatus.DONE)
    (branch,) = add_ideas(db, project, 'branch from the baseline', after=(done,))
    (missing,) = add_ideas(db, project, 'pinned to nothing', priority=3)
    with db.session() as s:
        s.add(Candidate(project_id=project.id, tree_sha='b', commit_sha='b', verdict=Verdict.KEEP, verdict_by='human'))
        s.flush()
        s.add(Measurement(candidate_id=2, evaluator_id=1, metrics={'score': 9, 'size': 9}))
        s.get_one(Idea, branch).base_candidate_id = 1
        s.get_one(Idea, missing).base_candidate_id = 99
        s.commit()
    scheduler = make_scheduler(db, runtime, tmp_path)
    with db.session() as s:
        p = s.get_one(Project, project.id)
        scheduler._queue(s, p, ProjectConfig.model_validate(p.config))
        s.commit()
    with db.session() as s:
        run = s.scalars(select(Run).where(Run.idea_id == branch)).one()
        assert run.base_candidate_id == 1, 'the pin, not the head candidate#2 that the run would otherwise start from'
        queued = s.scalars(select(Event).where(Event.kind == 'run.queued', Event.target_id == run.id)).one()
        assert (queued.payload['pinned_base'], queued.payload['computed_base']) == (1, 2)
        assert run.branch, 'a pin other than the computed base: a branch run, judged against its own base'
        assert s.get_one(Idea, missing).status is IdeaStatus.PARKED, 'a pin to a candidate that does not exist'
        assert s.scalars(select(Run).where(Run.idea_id == missing)).all() == []


def test_a_run_pinned_to_a_non_keep_is_a_branch_run_only_when_its_nearest_keep_is_not_the_head(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A run pinned to a candidate that is not a keep is a branch run only when its nearest keep is not the head.

    A candidate that is not a keep is not a bar, so the run is judged against its nearest kept ancestor
    (``ledger.bar_of``). When the pin is a noted candidate on top of the head, that ancestor is the head. The run is
    then a trunk run, and its keep moves the head.
    """
    set_config(db, project, roles={'worker': {'max_concurrent': 2}})
    on_head, on_older = add_ideas(db, project, 'on a note of the head', 'on a discard of the baseline')
    with db.session() as s:
        s.add_all(
            [
                Candidate(project_id=project.id, parent_id=1, tree_sha='k', commit_sha='k', verdict=Verdict.KEEP),
                Candidate(
                    project_id=project.id, parent_id=2, tree_sha='n', commit_sha='n', verdict=Verdict.INTERESTING
                ),
                Candidate(project_id=project.id, parent_id=1, tree_sha='d', commit_sha='d', verdict=Verdict.DISCARD),
            ]
        )
        s.flush()
        s.add(Measurement(candidate_id=2, evaluator_id=1, metrics={'score': 9, 'size': 9}))
        s.get_one(Idea, on_head).base_candidate_id = 3
        s.get_one(Idea, on_older).base_candidate_id = 4
        s.commit()
    scheduler = make_scheduler(db, runtime, tmp_path)
    with db.session() as s:
        p = s.get_one(Project, project.id)
        scheduler._queue(s, p, ProjectConfig.model_validate(p.config))
        s.commit()
    with db.session() as s:
        runs = {r.idea_id: (r.base_candidate_id, r.branch) for r in s.scalars(select(Run))}
    assert runs == {on_head: (3, False), on_older: (4, True)}, 'the base is the pin; the bar decides the branch'


def test_a_blocked_idea_waits_for_every_prerequisite_then_starts_from_the_head(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    (first,) = add_ideas(db, project, 'first', priority=3)
    (second,) = add_ideas(db, project, 'second', priority=4)
    (third,) = add_ideas(db, project, 'third', priority=0, after=(first, second))
    scheduler = make_scheduler(db, runtime, tmp_path)

    assert scheduler.tick() == [1]
    with db.session() as s:
        assert s.get_one(Run, 1).idea_id == first, 'the urgent idea is blocked, so the lower priorities run first'
        assert s.get_one(Idea, third).status is IdeaStatus.READY, 'skipped, not touched'
    scheduler.wait(timeout=30)
    assert scheduler.tick() == [2]
    with db.session() as s:
        assert s.get_one(Run, 2).idea_id == second, 'one prerequisite done is not enough'
        assert s.get_one(Idea, third).status is IdeaStatus.READY
    scheduler.wait(timeout=30)
    with db.session() as s:
        assert [s.get_one(Candidate, c).verdict for c in (2, 3)] == [Verdict.KEEP] * 2, 'scores 2 and 3'
        # a human keeps a commit outside any run: the head is now candidate#4, which no prerequisite made
        sha = s.get_one(Candidate, 3).commit_sha
        s.add(Candidate(project_id=project.id, tree_sha='h', commit_sha=sha, verdict=Verdict.KEEP, verdict_by='human'))
        s.flush()
        s.add(Measurement(candidate_id=4, evaluator_id=1, metrics={'score': 9, 'size': 9}))
        s.commit()

    assert scheduler.tick() == [3]
    scheduler.wait(timeout=30)
    scheduler.stop(timeout=1)
    with db.session() as s:
        run = s.get_one(Run, 3)
        assert (run.idea_id, run.base_candidate_id) == (third, 4), 'the head, which holds what the prerequisites kept'
        assert s.get_one(Idea, third).status is IdeaStatus.DONE


def test_a_prerequisite_that_is_not_done_keeps_blocking(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    (rejected,) = add_ideas(db, project, 'rejected', status=IdeaStatus.REJECTED)
    (parked,) = add_ideas(db, project, 'parked', status=IdeaStatus.PARKED)
    (proposed,) = add_ideas(db, project, 'proposed', status=IdeaStatus.PROPOSED)
    (after_rejected,) = add_ideas(db, project, 'after rejected', after=(rejected,))
    (after_parked,) = add_ideas(db, project, 'after parked', after=(parked,))
    (after_proposed,) = add_ideas(db, project, 'after proposed', after=(proposed,))
    (chained,) = add_ideas(db, project, 'after a blocked one', after=(after_rejected,))
    scheduler = make_scheduler(db, runtime, tmp_path)

    assert scheduler.tick() == [], 'rejected, parked and proposed prerequisites all block; so does a blocked one'
    assert statuses(db)[3:] == [IdeaStatus.READY] * 4, 'blocked ideas stay ready for the watchdog or a human'

    with db.session() as s:
        s.get_one(Idea, rejected).status = IdeaStatus.DONE  # a human reconsidered; it never ran, so it kept nothing
        s.commit()
    assert scheduler.tick() == [1]
    scheduler.wait(timeout=30)
    scheduler.stop(timeout=1)
    with db.session() as s:
        run = s.get_one(Run, 1)
        assert (run.idea_id, run.base_candidate_id) == (after_rejected, 1), 'the head: the baseline'
        assert s.get_one(Idea, after_rejected).status is IdeaStatus.DONE
        assert [s.get_one(Idea, i).status for i in (after_parked, after_proposed, chained)] == [IdeaStatus.READY] * 3


def test_low_water_does_not_count_blocked_ideas_as_backlog(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    set_config(db, project, roles={'research': {'max_ideas': 1, 'max_concurrent': 1, 'timeout': 20}})
    (gate,) = add_ideas(db, project, 'gate', status=IdeaStatus.PROPOSED)
    add_ideas(db, project, 'behind the gate', after=(gate,))
    scheduler = make_scheduler(db, runtime, tmp_path)
    assert scheduler.tick() == [1]
    scheduler.wait(timeout=30)
    scheduler.stop(timeout=1)
    with db.session() as s:
        worker = s.get_one(Worker, 1)
        assert (worker.kind, worker.reason) == (
            WorkerKind.RESEARCH,
            '1 free worker slots without a runnable idea; 0 of 3 runnable ideas',
        ), 'one ready idea exists, but it cannot run: the backlog is empty for the research role'
        assert s.scalars(select(Run)).all() == []


def test_a_superseded_idea_that_never_ran_is_rejected_on_tick_and_one_that_ran_is_left_alone(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, tracer: Tracer, spans: InMemorySpanExporter
) -> None:
    (ready,) = add_ideas(db, project, 'ready')
    (proposed,) = add_ideas(db, project, 'proposed', status=IdeaStatus.PROPOSED)
    (done,) = add_ideas(db, project, 'done', status=IdeaStatus.DONE)
    (parked,) = add_ideas(db, project, 'parked', status=IdeaStatus.PARKED)
    (retrying,) = add_ideas(db, project, 'failed once, one attempt left')
    with db.session() as s:
        s.add(Run(idea_id=retrying, attempt=1, base_candidate_id=1, harness='h', model='m', status=RunStatus.FAILED))
        p = s.get_one(Project, project.id)
        p.status, p.stopped_reason = ProjectStatus.STOPPED, 'by the test'  # bookkeeping happens, nothing launches
        s.commit()
    add_ideas(db, project, 'sharper than both', supersedes=(ready, proposed))
    add_ideas(db, project, 'sharper done', supersedes=(done,))
    add_ideas(db, project, 'sharper parked', supersedes=(parked,))
    add_ideas(db, project, 'sharper still', supersedes=(ready,))
    add_ideas(db, project, 'sharper retry', supersedes=(retrying,))
    scheduler = make_scheduler(db, runtime, tmp_path, tracer)

    assert scheduler.tick() == []
    assert statuses(db)[:5] == [
        IdeaStatus.REJECTED,
        IdeaStatus.REJECTED,
        IdeaStatus.DONE,
        IdeaStatus.PARKED,
        IdeaStatus.READY,
    ], 'never ran → rejected; ran, parked by a human, or ready again after a failed attempt → untouched'
    assert statuses(db)[5:] == [IdeaStatus.READY] * 5
    with db.session() as s:
        logged = s.scalars(select(Event).where(Event.kind == 'idea.status').order_by(Event.id)).all()
        assert [(e.target_id, e.actor, e.payload) for e in logged] == [
            (ready, 'daemon', {'old': 'ready', 'new': 'rejected'}),
            (proposed, 'daemon', {'old': 'proposed', 'new': 'rejected'}),
        ], 'the change-log trigger is the only writer of idea.status; the daemon emits nothing by hand'
        notes = s.scalars(select(Comment).where(Comment.author == 'daemon').order_by(Comment.id)).all()
        superseded = 'rejected: superseded by idea#{0}. It does not run, even if idea#{0} has not run yet.'
        assert [(c.target_id, str(c.kind), c.body) for c in notes] == [
            (ready, 'note', superseded.format(9)),
            (proposed, 'note', superseded.format(6)),
        ], 'the newest superseding idea is named where the event cannot say it'
    traced = [dict(sp.attributes or {}) for sp in spans.get_finished_spans() if sp.name == 'idea.superseded']
    assert traced == [
        {'project': 'demo', 'idea': ready, 'by': 9},
        {'project': 'demo', 'idea': proposed, 'by': 6},
    ], 'no open span: own spans'
    assert scheduler.tick() == []
    with db.session() as s:
        assert s.scalar(select(func.count()).select_from(Event).where(Event.kind == 'idea.status')) == 2, 'once'


def test_reap(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv('HOME', str(tmp_path / 'home'))
    store(tmp_path / 'home', 'pi', PI_ID, cwd=tmp_path / 'workers' / '2' / 'worktree')
    (idea,) = add_ideas(db, project, 'stale')
    repo = Repo(project.path)
    with db.session() as s:
        s.add(Run(idea_id=idea, attempt=1, base_candidate_id=1, harness='script', model='m', status=RunStatus.RUNNING))
        s.add(
            Worker(
                kind='worker',
                project_id=project.id,
                run_id=1,
                harness='script',
                model='m',
                runtime='subprocess',
                handle={},
                state=WorkerState.STARTING,  # mid-start: nothing to re-attach to
                log_dir='x',
                session_id='no-spec-to-read-it-with',
            )
        )
        s.get_one(Idea, idea).status = IdeaStatus.SCHEDULED
        s.get_one(Idea, idea).max_runs = 1
        s.commit()
        s.add(
            Worker(
                kind='research',
                project_id=project.id,
                harness='pi',
                model='m',
                runtime='subprocess',
                handle={},
                state=WorkerState.STARTING,
                log_dir='y',
                session_id=PI_ID,
            )
        )
        s.add(
            Worker(
                kind='watchdog',
                project_id=project.id,
                harness='pi',
                model='m',
                runtime='subprocess',
                handle={},
                state=WorkerState.RUNNING,
                log_dir='z',  # running, but its worktree is gone: nothing to re-attach to either
                session_id='a-session-whose-file-is-gone',
            )
        )
        s.commit()
    wt = tmp_path / 'runs' / '1' / 'worktree'
    sweep_wt = tmp_path / 'workers' / '2' / 'worktree'
    for path in (wt, sweep_wt):
        path.parent.mkdir(parents=True)
        repo.git.worktree('add', '--detach', str(path), 'HEAD')

    make_scheduler(db, runtime, tmp_path).reap()

    with db.session() as s:
        run = s.get_one(Run, 1)
        assert (run.status, run.error) == (RunStatus.FAILED, STALE_ERROR)
        assert s.get_one(Idea, idea).status is IdeaStatus.READY, 'a crash is not an attempt: its only one remains'
        assert s.get_one(Worker, 1).state is WorkerState.KILLED
        sweep = s.get_one(Worker, 2)
        assert sweep.state is WorkerState.KILLED
        assert sweep.usage is not None and sweep.usage['total'] == 16478, (
            'a reaped agent spent tokens too: read from its session file as the row closes, so max_tokens sees them'
        )
        gone = s.get_one(Worker, 3)
        assert (gone.state, gone.usage) == (WorkerState.KILLED, None), 'a session file not found leaves usage NULL'
    assert not wt.exists() and not sweep_wt.exists(), "the run's and the sweep's worktrees are both removed"
    assert len(runtime.killed) == 3, 'the stale worker processes were asked to stop'


def test_reap_tolerates_a_runtime_that_cannot_kill(
    db: Database,
    runtime: ScriptRuntime,
    tmp_path: Path,
    mocker: MockerFixture,
    tracer: Tracer,
    spans: InMemorySpanExporter,
) -> None:
    with db.session() as s:
        s.add(
            Worker(
                kind='watchdog',
                harness='pi',
                model='m',
                runtime='subprocess',
                handle={},
                state=WorkerState.RUNNING,
                log_dir='x',
            )
        )
        s.commit()
    mocker.patch.object(runtime, 'kill', side_effect=RuntimeError('gone'))
    make_scheduler(db, runtime, tmp_path, tracer).reap()
    assert events(spans, 'worker.kill_failed') == [{'worker': 1, 'error': 'gone'}]
    assert span_named(spans, 'piloop.reap').events[0].name == 'worker.kill_failed', 'attached to the reap span'
    with db.session() as s:
        assert s.get_one(Worker, 1).state is WorkerState.KILLED


def test_reap_survives_a_deleted_project(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, tracer: Tracer, spans: InMemorySpanExporter
) -> None:
    (idea,) = add_ideas(db, project, 'stale')
    with db.session() as s:
        s.get_one(Project, project.id).path = str(tmp_path / 'gone')
        s.add(Run(idea_id=idea, attempt=1, base_candidate_id=1, harness='script', model='m', status=RunStatus.RUNNING))
        s.commit()
    make_scheduler(db, runtime, tmp_path, tracer).reap()
    with db.session() as s:
        assert s.get_one(Run, 1).status is RunStatus.FAILED
    assert events(spans, 'worktrees.orphaned') == [
        {'agent_dir': str(tmp_path / 'runs' / '1'), 'project_path': str(tmp_path / 'gone')}
    ]


def test_serve_forever_stops(db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path) -> None:
    scheduler = make_scheduler(db, runtime, tmp_path)
    t = threading.Thread(target=scheduler.serve_forever, args=(0.05,))
    t.start()
    scheduler.stop(timeout=5)
    t.join(timeout=5)
    assert not t.is_alive()


def test_a_dead_thread_is_traced_and_its_row_closed(
    db: Database,
    project: Project,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
    mocker: MockerFixture,
) -> None:
    scheduler = make_scheduler(db, runtime, tmp_path, tracer)
    mocker.patch.object(scheduler._executor, 'execute', side_effect=RuntimeError('thread broke'))
    add_ideas(db, project, 'x')
    scheduler.tick()
    scheduler.wait(30)
    assert scheduler.active() == []
    died = span_named(spans, 'worker.thread_died')
    assert dict(died.attributes or {}) == {'worker': 1, 'error': 'thread broke'}, 'no open span: it gets its own'
    with db.session() as s:
        worker = s.get_one(Worker, 1)
    assert (worker.state, worker.error) == (WorkerState.KILLED, 'RuntimeError: thread broke')


def test_invalid_project_config_is_skipped_not_fatal(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, tracer: Tracer, spans: InMemorySpanExporter
) -> None:
    with db.session() as s:
        s.get_one(Project, project.id).config = {}
        s.commit()
    assert make_scheduler(db, runtime, tmp_path, tracer).tick() == []
    assert dict(span_named(spans, 'project.invalid_config').attributes or {})['project'] == 'demo'


def test_reap_leaves_a_human_decision_alone(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    (idea,) = add_ideas(db, project, 'parked meanwhile')
    with db.session() as s:
        s.add(Run(idea_id=idea, attempt=1, base_candidate_id=1, harness='script', model='m', status=RunStatus.RUNNING))
        s.get_one(Idea, idea).status = IdeaStatus.PARKED
        s.commit()
    make_scheduler(db, runtime, tmp_path).reap()
    with db.session() as s:
        assert s.get_one(Idea, idea).status is IdeaStatus.PARKED


def test_stop_kills_in_flight_workers_and_fails_their_runs(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, tracer: Tracer, spans: InMemorySpanExporter
) -> None:
    write_toml(Path(project.path), HARNESS_SLEEPS, worker_timeout=60)
    set_config(db, project)
    (idea,) = add_ideas(db, project, 'long')
    scheduler = make_scheduler(db, runtime, tmp_path, tracer)
    assert scheduler.tick() == [1]
    handle = wait_for_worker_handle(db)
    child_pid = int((tmp_path / 'runs' / '1' / 'child.pid').read_text())

    started = time.monotonic()
    scheduler.stop(timeout=10)
    assert time.monotonic() - started < 3, 'stop() does not wait for worker_timeout'
    assert proc.started(int(str(handle['pid']))) is None and proc.started(child_pid) is None
    assert scheduler.active() == []
    with db.session() as s:
        run = s.get_one(Run, 1)
        assert (run.status, run.error) == (RunStatus.FAILED, STOPPED_ERROR)
        assert s.get_one(Idea, idea).status is IdeaStatus.READY
        assert (s.get_one(Worker, 1).state, s.get_one(Worker, 1).error) == (WorkerState.KILLED, STOPPED_ERROR)
        assert s.scalars(select(Candidate).where(Candidate.run_id == 1)).all() == [], 'no scoring after stop'
    assert not (tmp_path / 'runs' / '1' / 'worktree').exists()
    assert dict(span_named(spans, 'run.aborted').attributes or {}) == {'run': 1, 'error': STOPPED_ERROR}
    assert dict(span_named(spans, 'piloop.run').attributes or {})['status'] == 'cancelled'


def test_stop_keeps_the_work_of_the_runs_it_cuts_pinned_and_noted_for_the_next_attempt(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A stop pins the commits and the uncommitted work of the runs it ends, and notes them for the next attempt.

    The note reaches the brief of the next run on the same idea.
    """
    harness = (
        '#!/bin/sh\necho step >> solve.txt\ngit commit -qam "step"\necho draft >> solve.txt\n'
        'sleep 30 &\necho $! > ../child.pid\nwait\n'
    )
    write_toml(Path(project.path), harness, worker_timeout=60)
    set_config(db, project)
    add_ideas(db, project, 'long')
    scheduler = make_scheduler(db, runtime, tmp_path)
    assert scheduler.tick() == [1]
    pid_file = tmp_path / 'runs' / '1' / 'child.pid'
    deadline = time.monotonic() + 10
    while not pid_file.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    scheduler.stop(timeout=10)

    repo = Repo(project.path)
    (head,) = git.pinned(repo, 'refs/piloop/demo/r1/')
    commit = repo.commit(head)
    assert commit.summary == 'step' and commit.tree['solve.txt'].data_stream.read().endswith(b'step\ndraft\n'), (
        'the uncommitted line is amended into the last commit, as for any run'
    )
    with db.session() as s:
        (note,) = s.scalars(select(Comment).where(Comment.target_kind == TargetKind.RUN, Comment.target_id == 1)).all()
        assert (note.author, note.kind) == (DAEMON_ACTOR, CommentKind.NOTE)
        assert f'its 1 commits are pinned, unscored, at refs/piloop/demo/r1/<n> (head {head})' in note.body
        assert s.scalars(select(Candidate).where(Candidate.run_id == 1)).all() == [], 'pinned, not scored'


def test_stop_closes_herdr_tabs(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, herdr_log: Path
) -> None:
    with db.session() as s:
        s.add(
            Worker(
                kind='worker',
                project_id=project.id,
                harness='pi',
                model='m',
                runtime='herdr',
                handle={'workspace_id': 'w7', 'tab_id': 'w7:t2', 'pane_id': 'w7:p2', 'agent': 'worker-1'},
                state=WorkerState.RUNNING,
                log_dir='x',
            )
        )
        s.commit()
    executor = RunExecutor(db, Tracer(None), tmp_path, factory(runtime))
    Scheduler(db, executor, Tracer(None), tmp_path).stop(timeout=5)
    assert [json.loads(line) for line in herdr_log.read_text().splitlines()] == [
        ['--session', 'piloop', 'tab', 'close', 'w7:t2']
    ]
    with db.session() as s:
        assert s.get_one(Worker, 1).state is WorkerState.KILLED


def test_a_hand_over_leaves_a_sweep_running_and_the_next_scheduler_re_attaches_to_it(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """An in-place restart during a sweep keeps the sweep's agent and worktree, and the next daemon closes its row."""
    hold = tmp_path / 'hold'
    hold.touch()
    write_toml(Path(project.path), f'#!/bin/sh\necho $$ > ../agent.pid\nwhile [ -f {hold} ]; do sleep 0.05; done\n')
    set_config(db, project)
    worker_id = request_watchdog(db, project)
    first = make_scheduler(db, runtime, tmp_path)
    assert worker_id in first.tick()
    sweep_dir = tmp_path / 'workers' / str(worker_id)
    deadline = time.monotonic() + 10
    while not (sweep_dir / 'agent.pid').exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    first.hand_over(timeout=5)
    with db.session() as s:
        assert s.get_one(Worker, worker_id).state is WorkerState.RUNNING
    assert (sweep_dir / 'worktree').is_dir()

    second = make_scheduler(db, ScriptRuntime(), tmp_path)
    second.reap()
    hold.unlink()
    second.wait(30)
    first.wait(10)
    with db.session() as s:
        sweep = s.get_one(Worker, worker_id)
        assert (sweep.state, sweep.error) == (WorkerState.EXITED, None)
    assert not (sweep_dir / 'worktree').exists()
    second.stop(timeout=5)


def test_stop_before_the_worker_starts_never_starts_it(
    db: Database,
    project: Project,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = Gate()
    monkeypatch.setattr('piloop.daemon.runner.git.add_worktree', gate.wrap(git.add_worktree))
    scheduler = make_scheduler(db, runtime, tmp_path, tracer)
    (idea,) = add_ideas(db, project, 'x')
    scheduler.tick()
    assert gate.entered.wait(10)
    started = time.monotonic()
    scheduler.stop(timeout=0.5)  # the run thread is past its run-row write, before its start window
    assert time.monotonic() - started < 3
    gate.release.set()
    scheduler.wait(10)
    with db.session() as s:
        run = s.get_one(Run, 1)
        assert (run.status, run.error) == (RunStatus.FAILED, STOPPED_ERROR)
        assert s.get_one(Idea, idea).status is IdeaStatus.READY
        worker = s.get_one(Worker, 1)
        assert (worker.state, worker.error, worker.runtime, worker.handle) == (
            WorkerState.KILLED,
            STOPPED_ERROR,
            None,
            {},
        ), 'closed by stop(); nothing was started'
    assert not (tmp_path / 'runs' / '1' / 'worktree').exists()
    assert events(spans, 'worker.kill_failed') == [], 'a row without a process is not asked to die'
    assert dict(span_named(spans, 'piloop.run').attributes or {})['status'] == 'cancelled'
    assert dict(span_named(spans, 'piloop.agent').attributes or {}) == {
        'worker': 1,
        'state': 'killed',
        'error': STOPPED_ERROR,
    }


def test_stop_waits_for_a_start_in_flight_then_kills_the_worker(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_toml(Path(project.path), HARNESS_SLEEPS, worker_timeout=60)
    set_config(db, project)
    gate = Gate()
    monkeypatch.setattr(runtime, 'start', gate.wrap(runtime.start))
    scheduler = make_scheduler(db, runtime, tmp_path)
    add_ideas(db, project, 'long')
    scheduler.tick()
    assert gate.entered.wait(10)
    stopper = threading.Thread(target=scheduler.stop, args=(10,))
    stopper.start()
    stopper.join(0.3)
    assert stopper.is_alive(), 'stop() waits for the handle to be published'
    with db.session() as s:
        assert (s.get_one(Worker, 1).state, s.get_one(Worker, 1).handle) == (WorkerState.STARTING, {})
    gate.release.set()
    stopper.join(10)
    assert not stopper.is_alive()
    with db.session() as s:
        worker = s.get_one(Worker, 1)
        assert (worker.state, worker.error) == (WorkerState.KILLED, STOPPED_ERROR)
        assert proc.started(int(str(worker.handle['pid']))) is None
        run = s.get_one(Run, 1)
        assert (run.status, run.error) == (RunStatus.FAILED, STOPPED_ERROR)
    assert scheduler.active() == []


def test_stop_kills_the_evaluator_and_scores_nothing_more(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, tracer: Tracer, spans: InMemorySpanExporter
) -> None:
    pid_file = tmp_path / 'evaluator.pid'
    write_toml(Path(project.path), HARNESS_TWO_COMMITS)
    set_config(db, project, evaluate=['sh', '-c', f'echo $$ > {pid_file}; exec sleep 30'])
    scheduler = make_scheduler(db, runtime, tmp_path, tracer)
    add_ideas(db, project, 'two commits')
    scheduler.tick()
    deadline = time.monotonic() + 10
    while not pid_file.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    pid = int(pid_file.read_text())

    started = time.monotonic()
    scheduler.stop(timeout=10)
    assert time.monotonic() - started < 3, 'stop() does not wait for evaluate_timeout'
    assert proc.started(pid) is None
    assert scheduler.active() == []
    with db.session() as s:
        run = s.get_one(Run, 1)
        assert (run.status, run.error) == (RunStatus.SUCCEEDED, None), 'closed when its worker ended'
        assert s.get_one(Idea, 1).status is IdeaStatus.SCHEDULED, (
            'its run finished and only its scoring was cut: the next daemon scores it, the idea is not run again'
        )
        assert s.get_one(Worker, 1).state is WorkerState.EXITED, 'the worker had finished; only scoring was cut'
        [first] = s.scalars(select(Candidate).where(Candidate.run_id == 1)).all()
        assert (first.verdict, first.measurements) == (Verdict.PENDING, []), (
            'the tree exists as a candidate; its measurement was killed and nothing judged it'
        )
    measured = [sp for sp in spans.get_finished_spans() if sp.name == 'piloop.measure']
    assert len(measured) == 1, 'the second commit is never measured'
    assert dict(measured[0].attributes or {})['status'] == 'cancelled'
    assert dict(span_named(spans, 'piloop.run').attributes or {})['status'] == 'cancelled'
    assert not (tmp_path / 'runs' / '1' / 'worktree').exists()
    assert not (tmp_path / 'runs' / '1' / 'eval-0' / 'worktree').exists()


def test_a_restart_during_an_evaluation_scores_the_run_on_start_and_never_runs_its_idea_again(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """After a restart during an evaluation, the next daemon scores the run's commits and does not run its idea again.

    The next daemon scores and judges every commit of the run, and settles the idea once.
    """
    hold = tmp_path / 'hold'
    hold.touch()
    write_toml(Path(project.path), HARNESS_TWO_COMMITS)
    wait_then_score = f'while [ -f {hold} ]; do sleep 0.05; done; echo METRIC score=$(wc -l < solve.txt)'
    set_config(db, project, evaluate=['sh', '-c', f'{wait_then_score}; echo METRIC size=1'])
    first = make_scheduler(db, runtime, tmp_path)
    add_ideas(db, project, 'two commits')
    first.tick()
    deadline = time.monotonic() + 10
    while not (tmp_path / 'runs' / '1' / 'eval-0' / 'eval.stdout').exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    first.stop(timeout=10)
    with db.session() as s:
        assert s.get_one(Idea, 1).status is IdeaStatus.SCHEDULED
        assert [c.verdict for c in s.scalars(select(Candidate).where(Candidate.run_id == 1))] == [Verdict.PENDING]

    hold.unlink()
    second = make_scheduler(db, runtime, tmp_path)
    second.reap()
    second.wait(30)
    with db.session() as s:
        made = s.scalars(select(Candidate).where(Candidate.run_id == 1).order_by(Candidate.id)).all()
        assert [(c.summary, c.verdict, len(c.measurements)) for c in made] == [
            ('first', Verdict.KEEP, 1),
            ('second', Verdict.KEEP, 1),
        ], 'the cut commit and the one never reached are both measured and judged'
        assert s.get_one(Idea, 1).status is IdeaStatus.DONE, 'settled by its one finished run'
    second.tick()
    second.wait(30)
    with db.session() as s:
        assert s.scalar(select(func.count()).select_from(Run)) == 1, 'the idea was never queued again'
    second.stop(timeout=5)


def test_a_run_whose_scoring_cannot_resume_returns_its_idea_to_the_queue(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, tracer: Tracer, spans: InMemorySpanExporter
) -> None:
    (idea,) = add_ideas(db, project, 'orphan')
    with db.session() as s:
        s.add(
            Run(idea_id=idea, attempt=1, base_candidate_id=1, harness='script', model='m', status=RunStatus.SUCCEEDED)
        )
        s.get_one(Idea, idea).status = IdeaStatus.SCHEDULED
        s.get_one(Project, project.id).path = str(tmp_path / 'gone')
        s.commit()
    sched = make_scheduler(db, runtime, tmp_path, tracer)
    sched.reap()
    sched.wait(10)
    with db.session() as s:
        assert s.get_one(Idea, idea).status is IdeaStatus.READY, 'attempts remain'
    assert span_named(spans, 'piloop.resume').status.status_code is StatusCode.ERROR


def measured_by(db: Database, candidate_id: int) -> list[tuple[int, float | None, str | None]]:
    with db.session() as s:
        rows = s.get_one(Candidate, candidate_id).measurements
        return [(m.evaluator_id, m.metrics.get('score'), m.error) for m in rows]


def test_a_new_evaluator_measures_every_keep_again_newest_first_and_runs_wait_for_every_score(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A new evaluator measures every keep again, the baseline too, newest first, and runs wait for every score.

    Without these scores, the head is empty and new runs cannot be compared with the kept lineage.
    """
    write_toml(Path(project.path), HARNESS_APPEND)
    set_config(db, project)
    sched = make_scheduler(db, runtime, tmp_path)
    add_ideas(db, project, 'one more line')
    sched.tick()
    sched.wait(30)
    with db.session() as s:
        old = s.get_one(Project, project.id).evaluator_id
        kept = s.scalars(select(Candidate).where(Candidate.run_id == 1)).one()
        assert kept.verdict is Verdict.KEEP
    doubled = ['sh', '-c', 'echo METRIC score=$(( 2 * $(wc -l < solve.txt) )); echo METRIC size=1']
    write_toml(Path(project.path), HARNESS_APPEND, evaluate=doubled)
    with db.session() as s:
        proj = s.get_one(Project, project.id)
        sync(s, proj)
        s.commit()
        new = proj.evaluator_id
    assert new != old
    add_ideas(db, project, 'another line')

    assert sched.tick() == [], 'no keep has a score under the new evaluator: the run waits instead of being parked'
    assert statuses(db) == [IdeaStatus.DONE, IdeaStatus.READY]
    sched.wait(30)
    assert measured_by(db, kept.id) == [(old, 2.0, None), (new, 4.0, None)], 'the newest keep first'
    assert measured_by(db, 1) == [(old, 1.0, None)], 'one at a time'

    assert sched.tick() == [], 'the baseline is not measured yet: the run still waits'
    sched.wait(30)
    assert measured_by(db, 1) == [(old, 1.0, None), (new, 2.0, None)]
    assert sched.tick() == [2], 'every keep has a score: the run starts from the best'
    sched.wait(30)
    with db.session() as s:
        assert s.get_one(Run, 2).base_candidate_id == kept.id
        assert s.get_one(Candidate, kept.id).verdict is Verdict.KEEP, 'measuring again judges nothing again'
    assert measured_by(db, 1) == [(old, 1.0, None), (new, 2.0, None)], 'every keep measured once: nothing more'


def test_after_a_new_evaluator_no_run_starts_from_the_best_until_every_keep_is_measured(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """After a new evaluator, no run starts until every keep is measured again.

    A keep can land while the new evaluator measures an older one. If the queue opened when the older keep had a
    score, runs would start from it, because the newer keep has no score yet.
    """
    write_toml(Path(project.path), HARNESS_APPEND, max_workers=2)
    set_config(db, project)
    sched = make_scheduler(db, runtime, tmp_path)
    add_ideas(db, project, 'first line')
    sched.tick()
    sched.wait(30)
    (second,) = add_ideas(db, project, 'second line')
    sched.tick()
    sched.wait(30)
    with db.session() as s:
        older, newer = (s.scalars(select(Candidate).where(Candidate.run_id == r)).one().id for r in (1, 2))
        assert [s.get_one(Candidate, c).verdict for c in (older, newer)] == [Verdict.KEEP, Verdict.KEEP]
    doubled = ['sh', '-c', 'echo METRIC score=$(( 2 * $(wc -l < solve.txt) )); echo METRIC size=1']
    write_toml(Path(project.path), HARNESS_APPEND, max_workers=2, evaluate=doubled)
    with db.session() as s:
        proj = s.get_one(Project, project.id)
        sync(s, proj)
        new = proj.evaluator_id
        assert new is not None
        for candidate, value in ((1, 2.0), (older, 4.0)):  # measured before the newer keep landed
            s.add(Measurement(candidate_id=candidate, evaluator_id=new, metrics={'score': value, 'size': 1.0}))
        s.commit()
    (chained,) = add_ideas(db, project, 'chained', after=(second,))
    (free,) = add_ideas(db, project, 'free')

    assert sched.tick() == [], 'the newer keep has no score: no base is known to be the best'
    sched.wait(30)
    assert measured_by(db, newer)[-1] == (new, 6.0, None)
    assert sorted(sched.tick()) == [3, 4]
    with db.session() as s:
        assert {s.get_one(Run, r).idea_id: s.get_one(Run, r).base_candidate_id for r in (3, 4)} == {
            chained: newer,
            free: newer,
        }
    sched.stop(timeout=5)
    sched.stop(timeout=5)


def test_an_imported_reference_is_measured_by_the_daemon_and_stays_a_reference(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    repo = Repo(project.path)
    repo.git.checkout('-qb', 'elsewhere')
    (Path(project.path) / 'solve.txt').write_text('a\nb\nc\n')
    repo.git.commit('-qam', 'three lines')
    repo.git.checkout('-q', 'main')
    with db.session() as s:
        proj = s.get_one(Project, project.id)
        cfg = load_project_config(Path(project.path))
        reference = import_reference(s, proj, cfg, repo, 'elsewhere', summary=None, actor='orchestrator').id
        evaluator = proj.evaluator_id
        s.commit()
    sched = make_scheduler(db, runtime, tmp_path)
    sched.tick()
    sched.wait(30)
    assert measured_by(db, reference) == [(evaluator, 3.0, None)]
    with db.session() as s:
        assert s.get_one(Candidate, reference).verdict is Verdict.REFERENCE, 'measured, never judged'
        trunk = head(s, s.get_one(Project, project.id))
        assert trunk is not None and trunk.id == 1, "it scores 3 to the baseline's 1 and is still not the head"
    sched.stop(timeout=5)


def test_a_keep_that_cannot_be_checked_out_is_measured_as_a_crash_once(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    with db.session() as s:
        s.add(
            Candidate(
                project_id=project.id, tree_sha='0' * 40, commit_sha='0' * 40, verdict=Verdict.KEEP, verdict_by='human'
            )
        )
        s.commit()
    sched = make_scheduler(db, runtime, tmp_path)
    sched.tick()
    sched.wait(10)
    [(_, score, error)] = measured_by(db, 2)
    assert score is None and error is not None and error.startswith('GitCommandError'), error
    sched.tick()
    sched.wait(10)
    assert len(measured_by(db, 2)) == 1, 'a crash is an answer: not measured again'
    sched.stop(timeout=5)


def test_stop_before_a_sweep_starts_never_starts_it(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate = Gate()
    monkeypatch.setattr('piloop.daemon.scheduler.compose_brief', gate.wrap(compose_brief))
    scheduler = make_scheduler(db, runtime, tmp_path)
    worker_id = request_watchdog(db, project)
    assert scheduler.tick() == [worker_id]
    assert gate.entered.wait(10)
    started = time.monotonic()
    scheduler.stop(timeout=0.5)  # the sweep thread is composing its brief, before its start window
    assert time.monotonic() - started < 3
    gate.release.set()
    scheduler.wait(10)
    with db.session() as s:
        worker = s.get_one(Worker, worker_id)
        assert (worker.state, worker.error, worker.harness, worker.handle) == (
            WorkerState.KILLED,
            STOPPED_ERROR,
            'script',
            {},
        )
        assert s.scalars(select(Event).where(Event.kind == 'watchdog.started')).all() == []
    assert not (tmp_path / 'workers' / str(worker_id)).exists(), 'nothing was started'


def test_stop_waits_for_a_sweep_start_in_flight_then_kills_it(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_toml(Path(project.path), HARNESS_SLEEPS)
    set_config(db, project)
    gate = Gate()
    monkeypatch.setattr(runtime, 'start', gate.wrap(runtime.start))
    scheduler = make_scheduler(db, runtime, tmp_path)
    worker_id = request_watchdog(db, project)
    assert scheduler.tick() == [worker_id]
    assert gate.entered.wait(10)
    stopper = threading.Thread(target=scheduler.stop, args=(10,))
    stopper.start()
    stopper.join(0.3)
    assert stopper.is_alive(), 'stop() waits for the handle to be published'
    with db.session() as s:
        worker = s.get_one(Worker, worker_id)
        assert (worker.state, worker.harness, worker.handle) == (WorkerState.STARTING, 'script', {})
    gate.release.set()
    stopper.join(10)
    assert not stopper.is_alive()
    with db.session() as s:
        worker = s.get_one(Worker, worker_id)
        assert (worker.state, worker.error) == (WorkerState.KILLED, STOPPED_ERROR)
        assert proc.started(int(str(worker.handle['pid']))) is None
    assert scheduler.active() == []


class PromptingRuntime(ScriptRuntime):
    """A subprocess runtime with a prompt channel that records what the daemon sends, in place of a Herdr pane."""

    def __init__(self) -> None:
        super().__init__()
        self.prompts: list[tuple[str, str]] = []

    def prompt(self, handle: Handle, text: str) -> None:
        self.prompts.append((str(handle.data['agent']), text))


def running_worker(db: Database, project: Project, title: str, *, state: WorkerState = WorkerState.RUNNING) -> int:
    """Add an idea with one run that started now and its worker row in ``state``.

    The idea, the run and the worker have the same id.
    """
    (idea,) = add_ideas(db, project, title)
    with db.session() as s:
        s.get_one(Idea, idea).status = IdeaStatus.SCHEDULED
        run = Run(
            idea_id=idea,
            attempt=1,
            base_candidate_id=1,
            harness='script',
            model='m',
            status=RunStatus.RUNNING,
            started_at=utcnow(),
        )
        s.add(run)
        s.flush()
        s.add(
            Worker(
                kind=WorkerKind.WORKER,
                project_id=project.id,
                run_id=run.id,
                harness='script',
                model='m',
                runtime='subprocess',
                handle={'agent': f'worker-{run.id}', 'pid': 4242},
                state=state,
                log_dir='x',
            )
        )
        s.commit()
    return idea


def comment(
    db: Database, kind: str, target_kind: str, target_id: int, body: str, *, author: str = 'human', priority: int = 2
) -> int:
    """Add a comment through the raw SQL path that agents use. ``created_at`` is CURRENT_TIMESTAMP, to the second.

    Each comment is addressed to the human at ``priority``. An escalation must have this, and other kinds may. The
    push to the worker ignores it.
    """
    with closing(sql.connect(Path(db.engine.url.database or ''), actor=author)) as con:
        return sql.run(
            con,
            f"INSERT INTO comments (target_kind, target_id, author, kind, body, priority) VALUES ('{target_kind}',"
            f" {target_id}, '{author}', '{kind}', '{body}', {priority})",
        ).last_insert_rowid


def settled(db: Database, kind: str) -> list[tuple[int, dict[str, object]]]:
    with db.session() as s:
        return [(e.target_id, e.payload) for e in s.scalars(select(Event).where(Event.kind == kind))]


def test_comments_on_a_running_run_or_its_idea_reach_the_worker_once_as_one_line(
    db: Database, project: Project, tmp_path: Path, tracer: Tracer, spans: InMemorySpanExporter
) -> None:
    runtime = PromptingRuntime()
    idea = running_worker(db, project, 'live')
    coming_up = running_worker(db, project, 'coming up', state=WorkerState.STARTING)
    with db.session() as s:
        before = utcnow() - timedelta(minutes=5)
        s.add(
            Comment(target_kind=TargetKind.IDEA, target_id=idea, author='human', body='in the brief', created_at=before)
        )
        s.commit()
    scheduler = make_scheduler(db, runtime, tmp_path, tracer)
    assert scheduler.tick() == [] and runtime.prompts == [], 'a comment from before the run started is in its brief'

    decision = comment(db, 'decision', 'idea', idea, 'keep solve.py under 120 lines\nthe rest is detail')
    note = comment(db, 'note', 'run', 1, 'x' * 250, author='pi')
    comment(db, 'finding', 'project', project.id, 'not for any worker')
    waiting = comment(db, 'note', 'idea', coming_up, 'its worker is starting: waits for its pane')
    comment(db, 'escalation', 'idea', idea, 'for the human, not the pane')
    scheduler.tick()
    assert runtime.prompts == [
        (
            'worker-1',
            '[piloop] decision on idea#1 by human: keep solve.py under 120 lines — full text: SELECT body FROM comments'
            f' WHERE id=2\n[piloop] note on run#1 by pi: {"x" * 199}… — full text: SELECT body FROM comments WHERE id=3',
        )
    ], 'one prompt with one line per comment: two prompts close together run into one another in the queue'
    assert settled(db, 'comment.delivered') == [(1, {'comment': decision}), (1, {'comment': note})]
    assert events(spans, 'comment.delivered') == [{'comment': decision, 'worker': 1}, {'comment': note, 'worker': 1}]
    scheduler.tick()
    assert len(runtime.prompts) == 1, 'settled by the events: nothing is delivered twice'

    with db.session() as s:
        s.get_one(Worker, 2).state = WorkerState.RUNNING
        s.commit()
    scheduler.tick()
    assert runtime.prompts[-1] == (
        'worker-2',
        '[piloop] note on idea#2 by human: its worker is starting: waits for its pane — full text: SELECT body FROM'
        ' comments WHERE id=5',
    ), 'a comment that landed while the pane was coming up is delivered once the worker runs'
    assert settled(db, 'comment.delivered')[-1] == (2, {'comment': waiting})

    restarted = make_scheduler(db, runtime, tmp_path)
    restarted.tick()
    assert len(runtime.prompts) == 2, 'a new daemon replays nothing: the record is in the database'
    later = comment(db, 'decision', 'run', 1, 'and stop after the next commit')
    restarted.tick()
    assert runtime.prompts[-1][0] == 'worker-1' and settled(db, 'comment.delivered')[-1] == (1, {'comment': later})


def test_a_workers_own_comments_are_not_echoed_back(db: Database, project: Project, tmp_path: Path) -> None:
    runtime = PromptingRuntime()
    idea = running_worker(db, project, 'live')
    scheduler = make_scheduler(db, runtime, tmp_path)
    comment(db, 'finding', 'idea', idea, 'what I tried', author='worker#1')
    comment(db, 'note', 'run', 1, 'signed the other documented way', author='run#1')
    scheduler.tick()
    assert runtime.prompts == [] and settled(db, 'comment.delivered') == []
    comment(db, 'note', 'idea', idea, 'a sibling worker is not the author', author='worker#2')
    scheduler.tick()
    assert [text.split(' — ')[0] for _, text in runtime.prompts] == [
        '[piloop] note on idea#1 by worker#2: a sibling worker is not the author'
    ]


def test_a_comment_that_cannot_be_delivered_is_settled_as_undelivered_and_never_fails_the_tick(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, tracer: Tracer, spans: InMemorySpanExporter
) -> None:
    """A comment that cannot reach the worker is settled as undelivered, and the tick does not fail.

    The subprocess runtime raises NotSteerable. The tick records this on the worker and does not try again.
    """
    idea = running_worker(db, project, 'headless')
    scheduler = make_scheduler(db, runtime, tmp_path, tracer)
    stuck = comment(db, 'decision', 'idea', idea, 'a headless worker has no input channel')
    assert scheduler.tick() == []
    error = IsStr(regex='subprocess worker pid 4242 cannot be steered.*')
    assert settled(db, 'comment.delivered') == []
    assert settled(db, 'comment.undelivered') == [(1, {'comment': stuck, 'error': error})]
    assert events(spans, 'comment.undelivered') == [{'comment': stuck, 'worker': 1, 'error': error}]
    scheduler.tick()
    assert len(settled(db, 'comment.undelivered')) == 1 and len(events(spans, 'comment.undelivered')) == 1, (
        'settled: the next brief carries the comment, retrying would fail the same way'
    )


def test_a_run_frees_its_slot_when_its_worker_ends_and_its_idea_settles_once_scored(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A run frees its worker slot when its worker ends, and its idea settles only when the commits are scored."""
    go = tmp_path / 'go'
    gated = f'while [ ! -f {go} ]; do sleep 0.05; done; echo METRIC score=$(wc -l < solve.txt); echo METRIC size=1'
    set_config(db, project, evaluate=['sh', '-c', gated])
    first, second = add_ideas(db, project, 'first', 'second')
    with db.session() as s:  # the new evaluator never measured the baseline, so only a pin makes it a base
        s.get_one(Idea, second).base_candidate_id = 1
        s.commit()
    scheduler = make_scheduler(db, runtime, tmp_path)
    assert scheduler.tick() == [1]
    deadline = time.monotonic() + 10
    # the run closes, then its commit becomes a candidate: wait for both, the evaluation is gated
    while not candidate_rows(db, 1) and time.monotonic() < deadline:
        time.sleep(0.05)
    with db.session() as s:
        run = s.get_one(Run, 1)
        assert (run.status, run.finished_at is not None) == (RunStatus.SUCCEEDED, True), 'closed: its worker ended'
        assert s.get_one(Idea, first).status is IdeaStatus.SCHEDULED, 'its commit is not judged yet'
        assert s.scalars(select(Candidate).where(Candidate.run_id == 1)).one().verdict is Verdict.PENDING
    assert scheduler.tick() == [2], 'the one slot is free while run#1 is scored'
    go.touch()
    scheduler.wait(20)
    assert statuses(db) == [IdeaStatus.DONE, IdeaStatus.DONE]
    with db.session() as s:
        assert s.scalars(select(Candidate).where(Candidate.run_id == 1)).one().verdict is Verdict.KEEP
        assert s.get_one(Idea, second).status is IdeaStatus.DONE


def candidate_rows(db: Database, run_id: int) -> int:
    with db.session() as s:
        return len(s.scalars(select(Candidate).where(Candidate.run_id == run_id)).all())


def stop_project(db: Database, project: Project, reason: str) -> None:
    with db.session() as s:
        p = s.get_one(Project, project.id)
        p.status, p.stopped_reason = ProjectStatus.STOPPED, reason
        s.commit()


def test_workers_of_a_stopped_project_are_told_to_wrap_up_then_ended_after_the_grace(
    db: Database, project: Project, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When a project stops, its workers are told to wrap up, and they are ended after the grace period."""
    runtime = PromptingRuntime()
    running_worker(db, project, 'in flight')
    with db.session() as s:
        s.add(
            Worker(
                kind=WorkerKind.RESEARCH,
                project_id=project.id,
                runtime='subprocess',
                handle={'agent': 'research-2', 'pid': 4243},
                state=WorkerState.RUNNING,
            )
        )
        s.commit()
    scheduler = make_scheduler(db, runtime, tmp_path)
    scheduler.tick()
    assert runtime.prompts == [], 'an active project is left alone'

    stop_project(db, project, 'max_runs: 2 runs')
    scheduler.tick()
    assert runtime.prompts == snapshot(
        [
            (
                'worker-1',
                '[piloop] The campaign stopped (max_runs: 2 runs). Commit what you have now, with a message that says where it stands. Leave your finding and stop. This session ends in 10 minutes.',
            )
        ]
    ), 'a sweep is bounded by its timeout and the closing watchdog is one: only workers are wound down'
    assert settled(db, 'worker.wrap_up') == [(1, {'reason': 'max_runs: 2 runs'})]
    scheduler.tick()
    assert (len(runtime.prompts), runtime.killed) == (1, []), 'told once; ended only when the grace is over'

    monkeypatch.setattr('piloop.daemon.scheduler.STOP_GRACE_SECONDS', 0)
    scheduler.tick()
    assert [h.data['agent'] for h in runtime.killed] == ['worker-1']


def test_a_headless_worker_of_a_stopped_project_cannot_be_told_and_is_just_ended(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    running_worker(db, project, 'headless')
    stop_project(db, project, 'plateau: 5 runs without a keep')
    monkeypatch.setattr('piloop.daemon.scheduler.STOP_GRACE_SECONDS', 0)
    scheduler = make_scheduler(db, runtime, tmp_path)
    scheduler.tick()
    assert settled(db, 'worker.wrap_up') == [
        (1, {'reason': 'plateau: 5 runs without a keep', 'error': IsStr(regex='subprocess worker pid 4242 cannot.*')})
    ]
    scheduler.tick()
    assert [h.data['agent'] for h in runtime.killed] == ['worker-1']


class BusyRuntime(ScriptRuntime):
    """A subprocess runtime whose worker has a job in flight, or whose jobs it cannot report (``error``)."""

    def __init__(self, error: str | None = None) -> None:
        super().__init__()
        self.error = error

    def jobs(self, handle: Handle) -> list[str]:
        if self.error is not None:
            raise RuntimeError(self.error)
        return ['4242 python train.py']


def quiet_for_over_an_hour(db: Database, project: Project) -> None:
    running_worker(db, project, 'train')
    with db.session() as s:
        s.get_one(Run, 1).started_at = utcnow() - timedelta(seconds=SILENT_SECONDS + 60)
        s.commit()


def test_a_worker_silent_while_its_jobs_run_requests_an_incident_sweep(
    db: Database, project: Project, tmp_path: Path
) -> None:
    quiet_for_over_an_hour(db, project)
    scheduler = make_scheduler(db, BusyRuntime(), tmp_path)
    scheduler.tick()
    with db.session() as s:
        [watchdog] = s.scalars(select(Worker).where(Worker.kind == WorkerKind.WATCHDOG)).all()
        assert watchdog.reason == 'anomaly: worker#1 silent while its jobs run'
    scheduler.stop(timeout=5)


def test_jobs_the_runtime_cannot_report_are_traced_and_page_nobody(
    db: Database, project: Project, tmp_path: Path, tracer: Tracer, spans: InMemorySpanExporter
) -> None:
    quiet_for_over_an_hour(db, project)
    scheduler = make_scheduler(db, BusyRuntime('server_not_running: herdr is down'), tmp_path, tracer)
    assert scheduler.tick() == []
    failed = span_named(spans, 'worker.jobs_failed').attributes
    assert dict(failed or {}) == {'worker': 1, 'error': 'server_not_running: herdr is down'}
    with db.session() as s:
        assert s.scalars(select(Worker).where(Worker.kind == WorkerKind.WATCHDOG)).all() == []


def died_at_startup(db: Database, project: Project, kind: WorkerKind, minutes_ago: float = 0) -> int:
    """Add a killed agent row and the launcher's ``anomaly.startup`` event, ``minutes_ago`` minutes ago."""
    with db.session() as s:
        worker = Worker(kind=kind, project_id=project.id, harness='script', state=WorkerState.KILLED)
        s.add(worker)
        s.flush()
        payload: dict[str, object] = {
            'kind': kind,
            'harness': 'script',
            'run': None,
            'seconds': 35.0,
            'error': f'HerdrError: prompt_lost: {kind}-{worker.id} did not start working\nIts screen ended with:\n> brief',
        }
        s.add(
            Event(
                kind=STARTUP_ANOMALY,
                actor='daemon',
                target_kind=TargetKind.WORKER,
                target_id=worker.id,
                payload=payload,
                at=utcnow() - timedelta(minutes=minutes_ago),
            )
        )
        s.commit()
        return worker.id


def pause_escalations(db: Database) -> list[Comment]:
    with db.session() as s:
        return list(s.scalars(select(Comment).where(Comment.kind == 'escalation').order_by(Comment.id)))


def test_startup_deaths_pause_launches_and_escalate_until_they_age_out(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """Three startup deaths within the window pause launches, with one escalation that says why.

    Without the pause, each death launches another agent on a host that is already overloaded. Launches resume when
    the deaths age out of the window.
    """
    died_at_startup(db, project, WorkerKind.WORKER, minutes_ago=11)
    for kind in (WorkerKind.WATCHDOG, WorkerKind.RESEARCH):
        died_at_startup(db, project, kind)
    wanted = request_watchdog(db, project)
    scheduler = make_scheduler(db, runtime, tmp_path)
    assert wanted in scheduler.tick(), 'two recent deaths: launches go on'
    scheduler.wait(10)

    died = [died_at_startup(db, project, WorkerKind.WATCHDOG) for _ in range(2)]
    request_watchdog(db, project)
    assert scheduler.tick() == []
    [escalation] = pause_escalations(db)
    assert (escalation.target_kind, escalation.author, escalation.priority) == (TargetKind.PROJECT, 'daemon', 1)
    assert escalation.body == IsStr(
        regex=r'Launches paused: 4 agents died at startup in the last 10 minutes\. Load average [\d.]+ / [\d.]+ / [\d.]+'
        rf' on \d+ CPUs when launches paused\..*\n- worker#\d+ \(research on script\): .*\n- worker#{died[0]} \(watchdog on script\):'
        rf' HerdrError: prompt_lost: watchdog-{died[0]} did not start working\nIts screen ended with:\n> brief\n'
        rf'- worker#{died[1]} .*',
        regex_flags=re.DOTALL,
    )
    assert scheduler.tick() == [] and len(pause_escalations(db)) == 1, 'one escalation while the pause holds'

    with db.session() as s:
        s.get_one(Comment, escalation.id).acked_at = utcnow()
        s.commit()
    assert scheduler.tick() == [] and len(pause_escalations(db)) == 1, 'acknowledged means seen: not posted again'

    latest = died_at_startup(db, project, WorkerKind.RESEARCH)
    assert scheduler.tick() == []
    [extended] = pause_escalations(db)
    assert extended.id == escalation.id and extended.acked_at is not None, 'the same pause: updated in place'
    assert extended.body.startswith('Launches paused: 5 agents') and f'- worker#{latest} (research' in extended.body

    with db.session() as s:
        for event in s.scalars(select(Event).where(Event.kind == STARTUP_ANOMALY)):
            event.at -= STARTUP_WINDOW
        s.commit()
    assert scheduler.tick() != [], 'the deaths aged out of the window: launches resume'
    scheduler.wait(10)

    for _ in range(STARTUP_BREAKER):
        died_at_startup(db, project, WorkerKind.WATCHDOG)
    request_watchdog(db, project)
    assert scheduler.tick() == [] and len(pause_escalations(db)) == 2, 'a new pause: a new escalation'
    with db.session() as s:
        kinds = s.scalars(select(Event.kind).where(Event.kind.like('launch.%')).order_by(Event.id)).all()
    assert kinds == ['launch.paused', 'launch.resumed', 'launch.paused']


def explore_setup(db: Database, project: Project, explore_every: int) -> tuple[int, int, int]:
    """Set up two finished trunk runs, a better keep (candidate 2) and three ready ideas.

    The ideas are an urgent one, one pinned to the head (a trunk run), and a someday one pinned to the baseline (a
    branch run).
    """
    set_config(db, project, roles={'worker': {'max_concurrent': 1, 'explore_every': explore_every}})
    (done,) = add_ideas(db, project, 'history', status=IdeaStatus.DONE)
    urgent, level, explore = (
        add_ideas(db, project, t, priority=p)[0] for t, p in (('urgent', 0), ('level', 1), ('explore', 4))
    )
    with db.session() as s:
        s.add(Candidate(project_id=project.id, tree_sha='b', commit_sha='b', verdict=Verdict.KEEP, verdict_by='human'))
        s.flush()
        s.add(Measurement(candidate_id=2, evaluator_id=1, metrics={'score': 9, 'size': 9}))
        s.get_one(Idea, level).base_candidate_id = 2
        s.get_one(Idea, explore).base_candidate_id = 1
        for attempt in (1, 2):
            s.add(
                Run(
                    idea_id=done,
                    attempt=attempt,
                    base_candidate_id=1,
                    harness='h',
                    model='m',
                    status=RunStatus.SUCCEEDED,
                )
            )
        s.commit()
    return urgent, level, explore


def queue_one(db: Database, scheduler: Scheduler) -> tuple[int, bool]:
    """Queue the next run and finish it at once to free the slot.

    Returns:
        The run's idea id and whether the run is a branch run.
    """
    with db.session() as s:
        p = s.scalars(select(Project)).one()
        scheduler._queue(s, p, ProjectConfig.model_validate(p.config))
        run = s.scalars(select(Run).order_by(Run.id.desc())).first()
        assert run is not None
        run.status = RunStatus.SUCCEEDED
        s.commit()
        return run.idea_id, run.branch


def test_every_nth_run_starts_from_an_alternative_base_whatever_its_priority(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """Every nth run starts from an alternative base, whatever its priority."""
    urgent, level, explore = explore_setup(db, project, explore_every=3)
    scheduler = make_scheduler(db, runtime, tmp_path)
    assert queue_one(db, scheduler) == (explore, True), 'the last two runs were no branch: this one explores'
    assert queue_one(db, scheduler) == (urgent, False), 'a branch run among the last two: priority order'
    assert queue_one(db, scheduler) == (level, False), 'a pin to the computed base is no alternative'
    (later,) = add_ideas(db, project, 'later')
    assert queue_one(db, scheduler) == (later, False), 'due, but no idea pinned to an alternative base is ready'


def test_explore_every_0_keeps_the_priority_order(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    urgent, _, _ = explore_setup(db, project, explore_every=0)
    assert queue_one(db, make_scheduler(db, runtime, tmp_path)) == (urgent, False)


TWO_TRACKS: dict[str, object] = {
    'metrics': [
        {'name': 'score', 'direction': 'higher', 'role': 'primary', 'track': 'accuracy', 'margin': 1},
        {'name': 'size', 'direction': 'lower', 'role': 'primary', 'track': 'cost', 'margin': 5},
    ],
    'tracks': {'accuracy': {'share': 0.75}, 'cost': {'share': 0.25}},
}
"""The test project's two metrics as two tracks: accuracy (``score``) and cost (``size``), three slots to one."""


def queue(db: Database, scheduler: Scheduler, project: Project) -> list[tuple[int, dict[str, object]]]:
    """Run one ``_queue`` pass.

    Returns:
        The idea and the ``run.queued`` payload of every queued run, in order.
    """
    with db.session() as s:
        p = s.get_one(Project, project.id)
        scheduler._queue(s, p, ProjectConfig.model_validate(p.config))
        s.commit()
    with db.session() as s:
        rows = s.scalars(select(Event).where(Event.kind == 'run.queued').order_by(Event.id)).all()
        return [(int(str(e.payload['idea'])), e.payload) for e in rows]


def test_a_free_slot_goes_to_the_track_furthest_below_its_share_whatever_the_priorities(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    set_config(db, project, roles={'worker': {'max_concurrent': 4}}, **TWO_TRACKS)
    accuracy = add_ideas(db, project, 'a1', 'a2', 'a3', 'a4', priority=0, track='accuracy')
    (cost,) = add_ideas(db, project, 'c1', priority=3, track='cost')
    queued = queue(db, make_scheduler(db, runtime, tmp_path), project)
    assert [idea for idea, _ in queued] == [*accuracy[:3], cost], (
        'three of four slots for the 0.75 share, the fourth for cost although its idea is the least urgent'
    )
    assert [(p['track'], p.get('priority_pick')) for _, p in queued] == [
        ('accuracy', None),
        ('accuracy', None),
        ('accuracy', None),
        ('cost', accuracy[3]),
    ], 'the share rule bound once: plain priority order would have taken a4'


def test_a_track_with_nothing_runnable_gives_its_slot_away(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    set_config(db, project, roles={'worker': {'max_concurrent': 4}}, **TWO_TRACKS)
    accuracy = add_ideas(db, project, 'a1', 'a2', 'a3', 'a4', track='accuracy')
    queued = queue(db, make_scheduler(db, runtime, tmp_path), project)
    assert [idea for idea, _ in queued] == accuracy, 'no cost idea: its slot goes to accuracy'
    assert all('priority_pick' not in p for _, p in queued)


def test_an_idea_on_a_track_the_config_does_not_name_is_parked_and_a_track_at_share_zero_waits(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    wound_down = {**TWO_TRACKS, 'tracks': {'accuracy': {'share': 1}, 'cost': {'share': 0}}}
    set_config(db, project, **wound_down)
    (stray,) = add_ideas(db, project, 'on a renamed track', track='speed')
    (waiting,) = add_ideas(db, project, 'cost work', track='cost')
    assert queue(db, make_scheduler(db, runtime, tmp_path), project) == []
    with db.session() as s:
        assert (s.get_one(Idea, stray).status, s.get_one(Idea, waiting).status) == (IdeaStatus.PARKED, IdeaStatus.READY)
        [note] = s.scalars(select(Comment).where(Comment.target_id == stray)).all()
        assert note.body == (
            "parked: its track 'speed' is not in piloop.toml (tracks: accuracy, cost); set ideas.track to one of"
            ' them, then set it ready'
        )


def test_low_water_counts_each_track_short_of_its_share_whatever_the_others_hold(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    research = {'buffer': 0, 'max_ideas': 1}
    set_config(db, project, roles={'worker': {'max_concurrent': 4}, 'research': research}, **TWO_TRACKS)
    add_ideas(db, project, *(f'a{i}' for i in range(6)), track='accuracy')
    scheduler = make_scheduler(db, runtime, tmp_path)
    with db.session() as s:
        p = s.get_one(Project, project.id)
        cfg = ProjectConfig.model_validate(p.config)
        backlog = scheduler._backlog(s, p, cfg)
        assert (backlog.runnable, backlog.target, backlog.short) == (6, 4, 1), 'cost is one short; accuracy has 3 spare'
        assert str(backlog) == '6 of 4 runnable ideas (accuracy 6 of 3, cost 0 of 1)'
        assert scheduler._low_water(s, p, cfg, cfg.roles.research, None) == str(backlog)
    add_ideas(db, project, 'c0', track='cost')
    with db.session() as s:
        p = s.get_one(Project, project.id)
        assert scheduler._backlog(s, p, cfg).short == -3, 'no track short: the surplus, as with one track'
