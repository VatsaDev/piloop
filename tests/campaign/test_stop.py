"""Each ``[stop]`` condition ends the campaign exactly as the configuration reference says. A stopped project requests nothing."""

from datetime import timedelta
from pathlib import Path

from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from sqlalchemy import func, select

from piloop.agents.launcher import STOPPED_ERROR
from piloop.campaign.stop import CAMPAIGN_STOPPED_ERROR, STALE_ERROR, runs, runs_without_keep, stall, stop_reason
from piloop.config import ProjectConfig, load_project_config
from piloop.db.models import (
    Candidate,
    Event,
    Idea,
    IdeaStatus,
    Measurement,
    Project,
    ProjectStatus,
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
from piloop.scoring.ledger import SKIPPED
from tests.conftest import HARNESS_NOOP, HARNESS_WORSE, ScriptRuntime, span_named, write_toml
from tests.daemon.test_scheduler import add_ideas, make_scheduler

LOWER_PRIMARY = [{'name': 'score', 'direction': 'higher'}, {'name': 'size', 'direction': 'lower', 'role': 'primary'}]


def configure(
    db: Database, project: Project, max_workers: int = 1, harness: str = HARNESS_NOOP, **stop: float
) -> ProjectConfig:
    write_toml(Path(project.path), harness, max_workers=max_workers)
    with db.session() as s:
        cfg = load_project_config(Path(project.path), stop=stop)
        s.get_one(Project, project.id).config = cfg.model_dump(mode='json')
        s.commit()
    return cfg


def add_run(
    db: Database,
    project: Project,
    *,
    status: RunStatus = RunStatus.SUCCEEDED,
    started_at: timedelta = timedelta(0),
    finished_at: timedelta = timedelta(0),
    keep_score: float | None = None,
    error: str | None = None,
    measured: bool = True,
) -> int:
    """Add a run, finished by default, with a measured candidate: a discard, or a keep that scores ``keep_score``.

    With ``measured=False`` the run has no candidate. Add one with ``add_candidate``. A run in flight has none.
    ``started_at`` and ``finished_at`` are how long ago.
    """
    with db.session() as s:
        n = s.scalar(select(func.count(Idea.id))) or 0
        idea = Idea(
            track='score',
            project_id=project.id,
            title=f'idea {n}',
            hypothesis='t',
            created_by='t',
            status=IdeaStatus.DONE,
        )
        s.add(idea)
        s.flush()
        in_flight = status in (RunStatus.QUEUED, RunStatus.RUNNING)
        run = Run(
            idea_id=idea.id,
            attempt=1,
            base_candidate_id=1,
            harness='h',
            model='m',
            status=status,
            error=error,
            started_at=utcnow() - started_at,
            finished_at=None if in_flight else utcnow() - finished_at,
        )
        s.add(run)
        s.commit()
    if keep_score is not None or (measured and not in_flight):
        verdict = Verdict.DISCARD if keep_score is None else Verdict.KEEP
        candidate_id = add_candidate(db, project, run.id, verdict)
        with db.session() as s:
            evaluator_id = s.get_one(Project, project.id).evaluator_id or 0
            s.add(Measurement(candidate_id=candidate_id, evaluator_id=evaluator_id, metrics={'score': keep_score or 0}))
            s.commit()
    return run.id


def add_candidate(db: Database, project: Project, run_id: int, verdict: Verdict, why: str | None = None) -> int:
    """Add an unmeasured candidate of run ``run_id``, a child of the baseline."""
    with db.session() as s:
        candidate = Candidate(
            project_id=project.id,
            run_id=run_id,
            parent_id=1,
            tree_sha=f'tree{run_id}',
            commit_sha=f'commit{run_id}',
            verdict=verdict,
            verdict_reason=why,
        )
        s.add(candidate)
        s.commit()
        return candidate.id


def reason(db: Database, project: Project) -> str | None:
    with db.session() as s:
        row = s.get_one(Project, project.id)
        return stop_reason(s, row, ProjectConfig.model_validate(row.config))


def project_state(db: Database, project: Project) -> tuple[ProjectStatus, str | None]:
    with db.session() as s:
        row = s.get_one(Project, project.id)
        return row.status, row.stopped_reason


def test_no_stop_table_means_open_ended(db: Database, project: Project) -> None:
    configure(db, project)
    add_run(db, project, keep_score=100.0, started_at=timedelta(days=30))
    for _ in range(3):
        add_run(db, project, status=RunStatus.FAILED)
    assert reason(db, project) is None


def test_target_on_the_best_kept_candidate_in_the_metric_direction(db: Database, project: Project) -> None:
    configure(db, project, target=3.0)
    with db.session() as s:
        s.get_one(Candidate, 1).verdict = Verdict.DISCARD
        s.commit()
    assert reason(db, project) is None, 'nothing kept'
    with db.session() as s:
        s.get_one(Candidate, 1).verdict = Verdict.KEEP
        s.commit()
    assert reason(db, project) is None, 'the baseline scores 1'
    add_run(db, project, keep_score=2.9)
    assert reason(db, project) is None
    with db.session() as s:
        s.get_one(Candidate, 2).verdict = Verdict.DISCARD
        s.commit()
    add_run(db, project, keep_score=3.0)
    assert reason(db, project) == 'target: score 3 ≥ 3'
    with db.session() as s:
        s.get_one(Candidate, 3).verdict = Verdict.INTERESTING
        s.commit()
    assert reason(db, project) is None, 'only a keep counts'
    with db.session() as s:
        s.get_one(Candidate, 3).verdict = Verdict.REFERENCE
        s.commit()
    assert reason(db, project) is None, 'a reference is never the best'
    with db.session() as s:
        s.get_one(Candidate, 3).verdict = Verdict.KEEP
        for m in s.scalars(select(Measurement).where(Measurement.candidate_id == 3)):
            m.metrics = {'score': 3.4}
        s.commit()
    assert reason(db, project) == 'target: score 3.4 ≥ 3'


def test_target_lower_is_better(db: Database, project: Project) -> None:
    with db.session() as s:
        cfg = load_project_config(Path(project.path), metrics=LOWER_PRIMARY, stop={'target': 5})
        s.get_one(Project, project.id).config = cfg.model_dump(mode='json')
        s.commit()
    assert reason(db, project) == 'target: size 5 ≤ 5', "the baseline's solve.txt is five bytes"


def test_max_runs_counts_every_run_created(db: Database, project: Project) -> None:
    configure(db, project, max_runs=2)
    add_run(db, project)
    assert reason(db, project) is None
    add_run(db, project, status=RunStatus.RUNNING)
    assert reason(db, project) == 'max_runs: 2 runs', 'in flight or finished, a run is a run'


def test_max_runs_caps_the_runs_queued_in_one_tick(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    configure(db, project, max_workers=4, max_runs=1)
    add_ideas(db, project, 'a', 'b', 'c', 'd')
    sched = make_scheduler(db, runtime, tmp_path)
    assert sched.tick() == [1], 'four free slots, but max_runs leaves room for one run'
    sched.wait(10)
    with db.session() as s:
        assert s.scalars(select(Run.id)).all() == [1]
    sched.stop(timeout=5)


def test_max_runs_leaves_room_for_the_remaining_runs_only(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    configure(db, project, max_workers=4, max_runs=3)
    add_run(db, project)
    add_run(db, project)
    add_ideas(db, project, 'a', 'b')
    sched = make_scheduler(db, runtime, tmp_path)
    assert sched.tick() == [1], 'two runs already: max_runs 3 leaves room for one more'
    sched.wait(10)
    with db.session() as s:
        assert s.scalar(select(func.count(Run.id))) == 3
    sched.stop(timeout=5)


def test_max_hours_since_the_first_run_started(db: Database, project: Project) -> None:
    configure(db, project, max_hours=12)
    assert reason(db, project) is None, 'the clock starts with the first run'
    add_run(db, project, started_at=timedelta(hours=11, minutes=59))
    assert reason(db, project) is None
    add_run(db, project, started_at=timedelta(hours=12, minutes=30))
    assert reason(db, project) == 'max_hours: 12.5h since the first run'


def test_max_tokens_sums_the_usage_of_every_ended_agent_of_the_project(db: Database, project: Project) -> None:
    configure(db, project, max_tokens=100_000)
    assert reason(db, project) is None, 'nothing spent yet'
    with db.session() as s:
        other = Project(name='other', path='/other', config=project.config)
        s.add(other)
        s.flush()
        ended = dict(state=WorkerState.EXITED, ended_at=utcnow())
        s.add_all(
            [
                Worker(kind=WorkerKind.WORKER, project_id=project.id, usage={'total': 60_000, 'turns': 9}, **ended),
                Worker(kind=WorkerKind.RESEARCH, project_id=project.id, usage={'total': 30_000, 'turns': 2}, **ended),
                Worker(kind=WorkerKind.WATCHDOG, project_id=project.id, usage=None, **ended),
                Worker(kind=WorkerKind.WORKER, project_id=other.id, usage={'total': 50_000, 'turns': 1}, **ended),
            ]
        )
        s.commit()
    assert reason(db, project) is None, "90k: every role counts, another project's agents do not, unknown usage is 0"
    with db.session() as s:
        s.add(Worker(kind=WorkerKind.WATCHDOG, project_id=project.id, usage={'total': 10_000, 'turns': 1}, **ended))
        s.commit()
    assert reason(db, project) == 'max_tokens: 100000 tokens spent'


def test_plateau_counts_finished_runs_since_the_last_keep(db: Database, project: Project) -> None:
    configure(db, project, plateau=2)
    add_run(db, project, status=RunStatus.FAILED)
    add_run(db, project, status=RunStatus.RUNNING)
    assert reason(db, project) is None, 'a run in flight has not failed to keep yet'
    add_run(db, project, status=RunStatus.TIMED_OUT)
    assert reason(db, project) == 'plateau: 2 runs without a keep'
    add_run(db, project, keep_score=5.0)
    assert reason(db, project) is None, 'a keep resets the count'
    add_run(db, project, status=RunStatus.SUCCEEDED)
    assert reason(db, project) is None
    add_run(db, project, status=RunStatus.SUCCEEDED)
    assert reason(db, project) == 'plateau: 2 runs without a keep'
    with db.session() as s:
        s.add(Candidate(project_id=project.id, tree_sha='imported', commit_sha='c', verdict=Verdict.REFERENCE))
        s.commit()
    assert reason(db, project) == 'plateau: 2 runs without a keep', 'an imported reference resets nothing'


def test_only_runs_that_produced_a_measured_candidate_count_toward_max_runs_and_plateau(
    db: Database, project: Project
) -> None:
    """A run with no commit, or with only ``Piloop-Eval: skip`` commits, does not count.

    A run in flight counts, so the queue never starts more runs than ``max_runs`` allows. A run whose candidate waits
    for its measurement also counts.
    """
    configure(db, project, max_runs=3, plateau=2)
    add_run(db, project, status=RunStatus.FAILED, error='worker produced no commits', measured=False)
    add_candidate(db, project, add_run(db, project, measured=False), Verdict.UNMEASURED, SKIPPED)
    with db.session() as s:
        row = s.get_one(Project, project.id)
        assert (runs(s, row), runs_without_keep(s, row)) == (0, 0)
    add_run(db, project)
    assert reason(db, project) is None, 'one measured run'
    add_candidate(db, project, add_run(db, project, measured=False), Verdict.PENDING)
    assert reason(db, project) == 'plateau: 2 runs without a keep'
    add_run(db, project, status=RunStatus.RUNNING)
    assert reason(db, project) == 'max_runs: 3 runs'


def test_the_stall_mark_is_half_the_plateau_else_two_rounds_of_the_worker_slots() -> None:
    bare = {
        'name': 'bare',
        'metrics': [{'name': 'a', 'direction': 'higher', 'role': 'primary'}],
        'evaluate': ['true'],
        'model': {'pi': 'm'},
    }
    assert stall(ProjectConfig.model_validate({**bare, 'stop': {'plateau': 15}})) == (8, 'half of [stop] plateau = 15')
    workers = ProjectConfig.model_validate({**bare, 'roles': {'worker': {'max_concurrent': 3}}})
    assert stall(workers) == (6, 'two rounds of [roles.worker] max_concurrent = 3')


def test_runs_that_the_daemon_stopped_count_toward_no_budget(db: Database, project: Project) -> None:
    configure(db, project, plateau=1, max_runs=2)
    for error in (STOPPED_ERROR, STALE_ERROR, CAMPAIGN_STOPPED_ERROR):
        add_run(db, project, status=RunStatus.FAILED, error=error)
    assert reason(db, project) is None, 'a stop, a crash or a campaign stop is not an attempt'
    add_run(db, project, status=RunStatus.FAILED, error='evaluate timed out')
    assert reason(db, project) == 'plateau: 1 runs without a keep'
    add_run(db, project, status=RunStatus.RUNNING)
    assert reason(db, project) == 'max_runs: 2 runs'


def test_plateau_counts_in_completion_order(db: Database, project: Project) -> None:
    configure(db, project, plateau=1)
    add_run(db, project)
    add_run(db, project, keep_score=5.0, finished_at=timedelta(hours=1))
    assert reason(db, project) == 'plateau: 1 runs without a keep', (
        'the earlier-created run finished after the keep, so it counts'
    )


def test_plateau_ignores_runs_that_finished_before_a_later_keep(db: Database, project: Project) -> None:
    configure(db, project, plateau=1)
    add_run(db, project, keep_score=5.0)
    add_run(db, project, finished_at=timedelta(hours=1))
    assert reason(db, project) is None, 'the later-created run finished before the keep, so it does not count'


def test_stopping_drains_the_requests_and_queued_runs_pending_at_that_moment(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    configure(db, project, max_runs=1)
    add_run(db, project)
    (idea,) = add_ideas(db, project, 'queued before the stop')
    with db.session() as s:
        s.get_one(Idea, idea).status = IdeaStatus.SCHEDULED
        run = Run(idea_id=idea, attempt=1, base_candidate_id=1, harness='script', model='m')
        s.add(run)
        s.flush()
        s.add(Worker(kind=WorkerKind.WORKER, project_id=project.id, run_id=run.id, harness='script', model='m'))
        s.add(Worker(kind=WorkerKind.RESEARCH, project_id=project.id, reason='low_water before the stop'))
        s.commit()
    sched = make_scheduler(db, runtime, tmp_path)

    assert sched.tick() == [3], 'only the closing watchdog launches'
    sched.wait(10)
    with db.session() as s:
        assert [(w.kind, w.state, w.error, w.ended_at is not None) for w in s.scalars(select(Worker))] == [
            (WorkerKind.WORKER, WorkerState.KILLED, CAMPAIGN_STOPPED_ERROR, True),
            (WorkerKind.RESEARCH, WorkerState.KILLED, CAMPAIGN_STOPPED_ERROR, True),
            (WorkerKind.WATCHDOG, WorkerState.EXITED, None, True),
        ]
        run = s.get_one(Run, 2)
        assert (run.status, run.error, run.finished_at is not None) == (RunStatus.FAILED, CAMPAIGN_STOPPED_ERROR, True)
        assert s.get_one(Idea, idea).status is IdeaStatus.READY
        [event] = s.scalars(select(Event).where(Event.kind == 'run.finished')).all()
        assert (event.target_id, event.payload) == (2, {'status': 'failed', 'error': CAMPAIGN_STOPPED_ERROR})
        s.add(Worker(kind=WorkerKind.RESEARCH, project_id=project.id, reason='by hand, after the stop'))
        s.commit()
    assert sched.tick() == [4], 'a request made after the stop is explicit and launches'
    sched.wait(10)
    sched.stop(timeout=5)
    with db.session() as s:
        assert s.get_one(Worker, 4).state is WorkerState.EXITED


def test_a_stopped_project_gets_one_closing_watchdog_and_no_trigger_requests(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, tracer: Tracer, spans: InMemorySpanExporter
) -> None:
    configure(db, project, harness=HARNESS_WORSE, max_runs=1)
    add_ideas(db, project, 'a', 'b')
    sched = make_scheduler(db, runtime, tmp_path, tracer)
    assert sched.tick() == [1], 'below max_runs: the queue trigger creates run 1, which commits a measured discard'
    sched.wait(10)
    assert project_state(db, project) == (ProjectStatus.ACTIVE, None)

    assert sched.tick() == [2], 'the closing watchdog launches on the tick that stops the project'
    assert project_state(db, project) == (ProjectStatus.STOPPED, 'max_runs: 1 runs')
    sched.wait(10)
    assert sched.tick() == []
    sched.tick()
    with db.session() as s:
        assert [(w.kind, w.reason) for w in s.scalars(select(Worker).where(Worker.id > 1).order_by(Worker.id))] == [
            (WorkerKind.WATCHDOG, 'campaign stopped: max_runs: 1 runs')
        ], 'requested exactly once, and no worker or research request follows'
        assert s.scalars(select(Run)).all() and s.get_one(Idea, 2).status is IdeaStatus.READY, 'idea b never ran'
        [event] = s.scalars(select(Event).where(Event.kind == 'project.status')).all()
        assert (event.actor, event.target_id, event.payload) == (
            'daemon',
            project.id,
            {'old': 'active', 'new': 'stopped', 'stopped_reason': 'max_runs: 1 runs'},
        ), 'the change log trigger records the stop, reason included; the daemon emits nothing by hand'
    assert dict(span_named(spans, 'project.stopped').attributes or {}) == {
        'project': 'demo',
        'reason': 'max_runs: 1 runs',
    }
    assert 'Trigger: campaign stopped: max_runs: 1 runs' in (tmp_path / 'workers' / '2' / 'brief.md').read_text()

    with db.session() as s:
        s.add(Worker(kind=WorkerKind.RESEARCH, project_id=project.id, reason='by hand'))
        s.commit()
    assert sched.tick() == [3], 'a hand-inserted request is explicit and launches'
    sched.wait(10)
    sched.stop(timeout=5)
    with db.session() as s:
        assert s.get_one(Worker, 3).state is WorkerState.EXITED
