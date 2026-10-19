import json
import os
import subprocess
import threading
from pathlib import Path
from unittest.mock import DEFAULT

import pytest
from dirty_equals import IsPositive, IsStr
from git import Repo
from inline_snapshot import snapshot
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode
from pytest_mock import MockerFixture
from sqlalchemy import func, insert, select

from piloop.agents.capture import ENTIRE_DIR
from piloop.agents.launcher import STARTUP_ANOMALY
from piloop.agents.roles import SQL_HINT
from piloop.campaign import stop
from piloop.campaign.project import sync
from piloop.config import ProjectConfig, load_project_config
from piloop.daemon.runner import (
    INTERRUPTED,
    NO_CHANGES,
    RECOVERED_REASON,
    UNCOMMITTED_SUMMARY,
    RunExecutor,
)
from piloop.db.models import (
    Candidate,
    Comment,
    Evaluator,
    Event,
    Idea,
    IdeaStatus,
    Lesson,
    LessonStatus,
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
from piloop.git import pin as git_pin
from piloop.otel import Span, Tracer
from piloop.runtime import Handle, Outcome, StartFailed
from piloop.scoring.ledger import (
    DAEMON,
    REBASED,
    SKIPPED,
    branches,
    current_evaluator,
    due,
    head,
    import_reference,
    request_remeasure,
    requested,
    resolve,
    save_units,
    units,
)
from tests.conftest import (
    HARNESS_APPEND,
    HARNESS_COMMITS_AND_LEAVES_DIRT,
    HARNESS_FAILS,
    HARNESS_FORGETS_COMMIT,
    HARNESS_NOOP,
    HARNESS_PROMPT_TO_FILE,
    HARNESS_SLEEPS,
    HARNESS_TRY_AND_REVERT,
    HARNESS_TWO_COMMITS,
    HARNESS_WORSE,
    Gate,
    ScriptRuntime,
    baseline,
    events,
    factory,
    span_named,
    write_script,
    write_toml,
)
from tests.fixtures.fake_entire import CHECKPOINT
from tests.usage.test_usage import PI_SPEC

HARNESS_READS_ENTIRE = '#!/bin/sh\ncat .entire/settings.json > seen.json\n'
HARNESS_COMMITS_ENTIRE = (
    '#!/bin/sh\nrm .entire\nmkdir .entire\necho "{}" > .entire/settings.json\ngit add .entire\n'
    'git commit -qm "entire settings"\necho more >> solve.txt\n'
)


def make_run(
    db: Database,
    project: Project,
    *,
    title: str = 'more lines',
    plan: str | None = None,
    max_runs: int = 2,
    provenance: str | None = None,
    track: str = 'score',
) -> int:
    with db.session() as s:
        idea = Idea(
            track=track,
            project_id=project.id,
            title=title,
            hypothesis='lines are good',
            plan=plan,
            provenance=provenance,
            created_by='test',
            max_runs=max_runs,
            status=IdeaStatus.SCHEDULED,
        )
        s.add(idea)
        s.flush()
        run = Run(
            idea_id=idea.id,
            attempt=1,
            base_candidate_id=baseline(db, project.id).id,
            harness='script',
            model='test-model',
        )
        s.add(run)
        s.flush()
        s.add(
            Worker(
                kind=WorkerKind.WORKER,
                project_id=project.id,
                run_id=run.id,
                harness='script',
                model='test-model',
                reason=f'idea#{idea.id} is ready',
            )
        )
        s.commit()
        return run.id


def execute(ex: RunExecutor, db: Database, run_id: int) -> None:
    """Run ``run_id`` through its requested worker row, the way the scheduler does."""
    with db.session() as s:
        requested = select(Worker.id).where(Worker.run_id == run_id, Worker.state == WorkerState.REQUESTED)
        worker_id = s.scalars(requested).one()
    ex.execute(run_id, worker_id)


def executor(db: Database, runtime: ScriptRuntime, tmp_path: Path, tracer: Tracer | None = None) -> RunExecutor:
    return RunExecutor(db, tracer or Tracer(None), tmp_path, factory(runtime))


def reload_project(db: Database, project: Project) -> None:

    with db.session() as s:
        s.get_one(Project, project.id).config = load_project_config(Path(project.path)).model_dump(mode='json')
        s.commit()


class Scored:
    """A candidate with its (single) measurement flattened on, for terse assertions."""

    def __init__(self, c: Candidate) -> None:
        self.id, self.parent_id, self.commit_sha, self.summary = c.id, c.parent_id, c.commit_sha, c.summary
        self.verdict, self.verdict_by = c.verdict, c.verdict_by
        [m] = c.measurements
        self.metrics, self.error = m.metrics, m.error


def candidates(db: Database, run_id: int) -> list[Scored]:
    """The candidates a run produced (its commits' trees), with their one measurement each."""
    with db.session() as s:
        rows = s.scalars(select(Candidate).where(Candidate.run_id == run_id).order_by(Candidate.id)).all()
        return [Scored(c) for c in rows]


def candidates_measured(db: Database, run_id: int) -> list[Scored]:
    """``candidates``, without the ones nothing measured (a skip commit, recovered work)."""
    with db.session() as s:
        rows = s.scalars(select(Candidate).where(Candidate.run_id == run_id).order_by(Candidate.id)).all()
        return [Scored(c) for c in rows if c.measurements]


def run_row(db: Database, run_id: int) -> Run:
    with db.session() as s:
        return s.get_one(Run, run_id)


def test_successful_run_scores_the_commit(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, tracer: Tracer, spans: InMemorySpanExporter
) -> None:
    run_id = make_run(db, project, plan='append one line', provenance='finding comment#3: short files lose')
    execute(executor(db, runtime, tmp_path, tracer), db, run_id)
    agent, measure, candidate, run_span = spans.get_finished_spans()
    assert dict(agent.attributes or {}) == snapshot(
        {
            'worker': 1,
            'kind': 'worker',
            'harness': 'script',
            'model': 'test-model',
            'reason': 'idea#1 is ready',
            'state': 'exited',
        }
    )
    assert dict(run_span.attributes or {}) == snapshot(
        {'run': 1, 'idea': 1, 'worker': 1, 'harness': 'script', 'model': 'test-model', 'status': 'succeeded'}
    )
    assert candidate.parent is not None and candidate.parent.span_id == run_span.context.span_id
    assert measure.parent is not None and measure.parent.span_id == candidate.context.span_id
    assert dict(measure.attributes or {}) == snapshot(
        {
            'run': 1,
            'commit': IsStr(),
            'candidate': 2,
            'measurement': 2,
            'metric.score': 2.0,
            'metric.size': IsPositive(),
        }
    )
    assert dict(candidate.attributes or {}) == snapshot(
        {'run': 1, 'commit': IsStr(), 'candidate': 2, 'verdict': 'keep', 'summary': 'add a line', 'score': 2.0}
    )

    run = run_row(db, run_id)
    assert (run.status, run.error) == (RunStatus.SUCCEEDED, None)
    assert run.brief is not None and run.brief.startswith('# Worker') and '### Plan\nappend one line' in run.brief
    assert 'lines are good\n\nProposed by test, prompted by: finding comment#3: short files lose\n' in run.brief
    [e] = candidates(db, run_id)
    assert (e.verdict, e.summary, e.metrics) == (Verdict.KEEP, 'add a line', {'score': 2.0, 'size': IsPositive()})
    assert e.parent_id == baseline(db, project.id).id
    with db.session() as s:
        assert s.get_one(Idea, run.idea_id).status is IdeaStatus.DONE
        [worker] = s.scalars(select(Worker)).all()
        assert (worker.state, worker.run_id) == (WorkerState.EXITED, run_id)
        kinds = [ev.kind for ev in s.scalars(select(Event)).all()]
    assert kinds == snapshot(
        [
            'run.started',
            'worker.started',
            'worker.finished',
            'run.finished',
            'measurement.recorded',
            'candidate.judged',
            'candidate.verdict',
            'idea.status',
        ]
    ), 'the run closes when its worker ends; the idea settles once the commit is judged'

    repo = Repo(project.path)
    assert [r.path for r in repo.refs if r.path.startswith('refs/piloop/')] == [f'refs/piloop/demo/r{run_id}/0']
    assert repo.head.commit.summary == 'baseline', 'the project checkout never moves'
    log_dir = tmp_path / 'runs' / str(run_id)
    assert (
        (log_dir / 'brief.md').is_file()
        and (log_dir / 'transcript.txt').is_file()
        and not (log_dir / 'worktree').exists()
    )
    assert (tmp_path / 'projects' / str(project.id) / 'workspace').is_dir()
    assert runtime.killed, 'the runtime is told to release the worker'


def test_every_commit_is_scored_and_lineage_follows_keeps(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    write_toml(Path(project.path), HARNESS_TWO_COMMITS)
    reload_project(db, project)
    run_id = make_run(db, project)
    execute(executor(db, runtime, tmp_path), db, run_id)
    first, second = candidates(db, run_id)
    assert (first.summary, first.verdict, first.metrics['score']) == ('first', Verdict.KEEP, 2.0)
    assert (second.summary, second.verdict, second.metrics['score'], second.parent_id) == (
        'second',
        Verdict.KEEP,
        3.0,
        first.id,
    )


@pytest.mark.parametrize('moved_to', [None, IdeaStatus.PARKED, IdeaStatus.REJECTED])
def test_a_run_counts_toward_max_runs_from_its_close_to_its_verdict(
    db: Database,
    project: Project,
    runtime: ScriptRuntime,
    tmp_path: Path,
    mocker: MockerFixture,
    moved_to: IdeaStatus | None,
) -> None:
    """A run keeps its place under ``max_runs`` from its close until its commits are scored, before its candidates
    exist, and also when a human parks or rejects its idea meanwhile (``piloop watch`` allows it)."""
    ex = executor(db, runtime, tmp_path)
    close = ex._close
    counted: list[tuple[int, bool]] = []

    def close_and_count(span: Span, run_id: int, status: RunStatus, error: str | None) -> None:
        close(span, run_id, status, error)
        with db.session() as s:
            if moved_to is not None:
                s.get_one(Run, run_id).idea.status = moved_to
                s.commit()
            project_row = s.get_one(Project, project.id)
            counted.append((stop.runs(s, project_row), bool(s.scalar(select(Run.id).where(stop.unsettled())))))

    mocker.patch.object(ex, '_close', side_effect=close_and_count)
    run_id = make_run(db, project)
    execute(ex, db, run_id)
    assert counted == [(1, True)], 'counted and unsettled while it is scored'
    with db.session() as s:
        assert s.scalar(select(Run.id).where(stop.unsettled())) is None, f'run#{run_id} settled'


def test_worse_commit_is_discarded(db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path) -> None:
    write_toml(Path(project.path), HARNESS_WORSE)
    reload_project(db, project)
    run_id = make_run(db, project)
    execute(executor(db, runtime, tmp_path), db, run_id)
    [e] = candidates(db, run_id)
    assert (e.verdict, e.verdict_by) == (Verdict.DISCARD, DAEMON)
    assert run_row(db, run_id).status is RunStatus.SUCCEEDED


def test_work_a_stopped_worker_left_uncommitted_is_recovered_and_a_new_run_finishes_it(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """comment#10936: a failed worker's uncommitted tree was scored, and could become the head, never declared ready.

    It is now ``recovered``: committed, pinned, never measured, never a keep. The idea goes back to the queue with the
    recovered candidate as its base, attempts left or not, and the run that finishes it is judged as any other.
    """
    write_toml(Path(project.path), HARNESS_FORGETS_COMMIT)
    reload_project(db, project)
    run_id = make_run(db, project, max_runs=1)
    execute(executor(db, runtime, tmp_path), db, run_id)
    assert (run_row(db, run_id).status, run_row(db, run_id).error) == (RunStatus.FAILED, INTERRUPTED)
    [(summary, verdict, reason, measurements, _)] = scored(db, run_id)
    assert (summary, verdict, reason, measurements) == (
        UNCOMMITTED_SUMMARY,
        Verdict.RECOVERED,
        RECOVERED_REASON.format(run=run_id),
        0,
    )
    with db.session() as s:
        recovered = s.scalars(select(Candidate).where(Candidate.run_id == run_id)).one()
        idea = s.get_one(Idea, run_row(db, run_id).idea_id)
        assert (idea.status, idea.base_candidate_id) == (IdeaStatus.READY, recovered.id), 'max_runs=1 is spent'
        [note] = s.scalars(select(Comment).where(Comment.target_kind == TargetKind.IDEA)).all()
        assert note.body.startswith(f'requeued from candidate#{recovered.id}: run#{run_id} ended failed')
        trunk = head(s, s.get_one(Project, project.id))
        assert trunk is not None and trunk.id == baseline(db, project.id).id, 'the head did not move'
        recovered_id, recovered_sha, idea_id = recovered.id, recovered.commit_sha, idea.id
    repo = Repo(project.path)
    assert 'data' not in repo.git.show('--stat', '--format=', recovered_sha), 'linked paths are never committed'
    assert repo.git.log('-1', '--format=%(trailers:key=Piloop-Recovered,valueonly)', recovered_sha).strip() == (
        f'run#{run_id}'
    )

    write_toml(Path(project.path), HARNESS_APPEND)
    reload_project(db, project)
    with db.session() as s:
        s.get_one(Idea, idea_id).status = IdeaStatus.SCHEDULED
        run = Run(idea_id=idea_id, attempt=2, base_candidate_id=recovered_id, harness='script', model='test-model')
        s.add(run)
        s.flush()
        s.add(
            Worker(kind=WorkerKind.WORKER, project_id=project.id, run_id=run.id, harness='script', model='test-model')
        )
        s.commit()
        finishing = run.id
    ex = executor(db, runtime, tmp_path)
    execute(ex, db, finishing)
    with db.session() as s:
        assert '- candidate#' in (s.get_one(Run, finishing).brief or '') and 'left uncommitted when its worker' in (
            s.get_one(Run, finishing).brief or ''
        )
    [done] = candidates(db, finishing)
    assert (done.summary, done.verdict, done.metrics['score'], done.parent_id) == (
        'add a line',
        Verdict.KEEP,
        3.0,
        recovered_id,
    ), 'judged against the nearest keep below the recovered work, the baseline'
    with db.session() as s:
        assert s.get_one(Idea, idea_id).status is IdeaStatus.DONE


def test_dirt_after_a_commit_is_recovered_on_its_own_and_the_commit_is_scored_as_made(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    write_toml(Path(project.path), HARNESS_COMMITS_AND_LEAVES_DIRT)
    reload_project(db, project)
    run_id = make_run(db, project)
    execute(executor(db, runtime, tmp_path), db, run_id)
    assert [(c[0], c[1], c[3]) for c in scored(db, run_id)] == [
        ('the idea', Verdict.KEEP, 1),
        (UNCOMMITTED_SUMMARY, Verdict.RECOVERED, 0),
    ], 'never amended: the worker declared its commit, not the dirt after it'
    [e] = [c for c in candidates_measured(db, run_id)]
    assert e.metrics['score'] == 2.0
    with db.session() as s:
        idea = s.get_one(Idea, run_row(db, run_id).idea_id)
        assert (idea.status, idea.base_candidate_id) == (IdeaStatus.DONE, None), 'a run that succeeded is not requeued'


def test_an_idea_whose_workers_keep_dying_is_requeued_from_its_recovered_work_only_so_often(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """``[roles.worker] recovery_requeues`` caps the requeues, counted in events: workers that leave the same two trees
    in turn reuse two candidates, and counting candidates would requeue them forever."""
    toggles = '#!/bin/sh\nif grep -q A solve.txt; then echo B > solve.txt; else echo A > solve.txt; fi\n'
    write_toml(Path(project.path), toggles)
    reload_project(db, project)
    run_id = make_run(db, project, max_runs=1)
    ex = executor(db, runtime, tmp_path)
    execute(ex, db, run_id)
    for attempt in range(2, 10):
        with db.session() as s:
            idea = s.get_one(Idea, run_row(db, run_id).idea_id)
            if idea.status is not IdeaStatus.READY:
                break
            idea.status = IdeaStatus.SCHEDULED
            run = Run(
                idea_id=idea.id,
                attempt=attempt,
                base_candidate_id=idea.base_candidate_id or 0,
                harness='script',
                model='test-model',
            )
            s.add(run)
            s.flush()
            s.add(
                Worker(
                    kind=WorkerKind.WORKER, project_id=project.id, run_id=run.id, harness='script', model='test-model'
                )
            )
            s.commit()
            run_id = run.id
        execute(ex, db, run_id)
    with db.session() as s:
        idea = s.get_one(Idea, run_row(db, run_id).idea_id)
        recovered = s.scalars(select(Candidate).where(Candidate.verdict == Verdict.RECOVERED)).all()
        assert (idea.status, len(idea.runs), len(recovered)) == (IdeaStatus.DONE, 3, 2), (
            'two requeues (the default), three runs, two recovered trees'
        )


def test_recovery_requeues_0_leaves_recovered_work_to_the_orchestrator(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    write_toml(Path(project.path), HARNESS_FORGETS_COMMIT)
    with db.session() as s:
        s.get_one(Project, project.id).config = load_project_config(
            Path(project.path), roles={'worker': {'recovery_requeues': 0}}
        ).model_dump(mode='json')
        s.commit()
    run_id = make_run(db, project, max_runs=1)
    execute(executor(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        idea = s.get_one(Idea, run_row(db, run_id).idea_id)
        assert (idea.status, idea.base_candidate_id) == (IdeaStatus.DONE, None)


def test_the_role_of_the_run_caps_its_recovery_requeues(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A role that does runs has its own ``recovery_requeues``: the worker's value does not apply to its runs."""
    write_toml(Path(project.path), HARNESS_FORGETS_COMMIT)
    roles = {'worker': {'recovery_requeues': 0}, 'quick': {'runs': True, 'recovery_requeues': 1}}
    with db.session() as s:
        s.get_one(Project, project.id).config = load_project_config(Path(project.path), roles=roles).model_dump(
            mode='json'
        )
        s.commit()
    run_id = make_run(db, project, max_runs=1)
    with db.session() as s:
        s.get_one(Run, run_id).role = 'quick'
        s.commit()
    execute(executor(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        idea = s.get_one(Idea, run_row(db, run_id).idea_id)
        assert idea.status is IdeaStatus.READY and idea.base_candidate_id is not None, (
            'requeued from its recovered work'
        )


def test_no_changes_fails_the_run_and_retries_the_idea(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    write_toml(Path(project.path), HARNESS_NOOP)
    reload_project(db, project)
    run_id = make_run(db, project, max_runs=2)
    execute(executor(db, runtime, tmp_path), db, run_id)
    run = run_row(db, run_id)
    assert (run.status, run.error) == (RunStatus.FAILED, NO_CHANGES)
    with db.session() as s:
        assert s.get_one(Idea, run.idea_id).status is IdeaStatus.READY, 'one attempt left'


def test_worker_failure_after_max_runs_finishes_the_idea(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    write_toml(Path(project.path), HARNESS_FAILS)
    reload_project(db, project)
    run_id = make_run(db, project, max_runs=1)
    execute(executor(db, runtime, tmp_path), db, run_id)
    run = run_row(db, run_id)
    assert (run.status, run.error) == (RunStatus.FAILED, 'worker exit 3')
    with db.session() as s:
        assert s.get_one(Idea, run.idea_id).status is IdeaStatus.DONE


def test_timeout(db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path) -> None:
    write_toml(Path(project.path), HARNESS_SLEEPS, worker_timeout=0.5)
    reload_project(db, project)
    run_id = make_run(db, project, max_runs=1)
    execute(executor(db, runtime, tmp_path), db, run_id)
    run = run_row(db, run_id)
    assert (run.status, run.error) == (RunStatus.TIMED_OUT, 'worker timeout after 0.5s')
    with db.session() as s:
        [worker] = s.scalars(select(Worker)).all()
        assert worker.state is WorkerState.KILLED


def test_evaluate_failure_is_a_crash_verdict(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:

    cfg = load_project_config(
        Path(project.path),
        evaluate=['sh', '-c', 'test $(wc -l < solve.txt) -eq 1 && echo METRIC score=1 && echo METRIC size=1 || exit 9'],
    )
    with db.session() as s:
        s.get_one(Project, project.id).config = cfg.model_dump(mode='json')
        s.commit()
    run_id = make_run(db, project)
    execute(executor(db, runtime, tmp_path), db, run_id)
    [e] = candidates(db, run_id)
    assert (e.verdict, e.metrics) == (Verdict.CRASH, {})
    assert e.error is not None and e.error.startswith('evaluate exit 9')
    assert run_row(db, run_id).status is RunStatus.SUCCEEDED, (
        'the worker did its job; the candidate carries the verdict'
    )


def test_a_crash_from_a_fault_of_the_host_is_measured_again_on_request_and_judged(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A crash that the host caused (a full disk) says nothing about the code, and its tree cannot be committed again.

    ``piloop candidate remeasure`` asks the daemon to measure the candidate again with the current evaluator. A crash
    that the daemon judged is then judged as its run would have judged it: here it keeps against its own bar after the
    head moved on, so it is rebased onto the head, and the merge is judged there.
    """
    fault = tmp_path / 'disk-full'
    measure = (
        f'test -e {fault} && exit 28; echo METRIC score=$(cat solve.txt other.txt 2>/dev/null | wc -l);'
        ' echo METRIC size=$(wc -c < solve.txt)'
    )
    write_toml(Path(project.path), HARNESS_APPEND, evaluate=['sh', '-c', measure])
    reload_project(db, project)
    ex = executor(db, runtime, tmp_path)
    ex.remeasure(baseline(db, project.id).id)  # the new evaluator measures the head first, as the daemon does
    run_id = make_run(db, project)
    fault.touch()
    execute(ex, db, run_id)
    [crashed] = candidates(db, run_id)
    assert crashed.verdict is Verdict.CRASH
    with db.session() as s:
        request_remeasure(s, s.get_one(Candidate, crashed.id), 'orchestrator')
        s.commit()
    ex.remeasure(crashed.id)
    with db.session() as s:
        c = s.get_one(Candidate, crashed.id)
        assert (c.verdict, len(c.measurements)) == (Verdict.CRASH, 2), 'the fault is still there: a crash again'
    fault.unlink()
    write_script(
        Path(project.path).parent / 'harness.sh',
        '#!/bin/sh\necho x > other.txt\ngit add other.txt\ngit commit -qm other\n',
    )
    execute(ex, db, make_run(db, project, title='elsewhere'))
    with db.session() as s:
        p = s.get_one(Project, project.id)
        moved = head(s, p)
        assert moved is not None and moved.summary == 'other', 'the head moved on meanwhile'
        assert requested(s, p) == [], 'nobody asked'
        request_remeasure(s, s.get_one(Candidate, crashed.id), 'orchestrator')
        s.commit()
        assert [c.id for c in requested(s, p)] == [crashed.id]

    ex.remeasure(crashed.id)
    with db.session() as s:
        c = s.get_one(Candidate, crashed.id)
        assert (c.verdict, c.verdict_by) == (Verdict.INTERESTING, DAEMON)
        assert c.verdict_reason is not None and c.verdict_reason.startswith(
            f'keeps against its own bar, but the head moved on to candidate#{moved.id}: against candidate#1: score 2 vs 1'
        )
        assert [m.error is None for m in c.measurements] == [False, False, True]
        p = s.get_one(Project, project.id)
        assert requested(s, p) == [], 'measured since the request'
        merged = head(s, p)
        assert merged is not None and (merged.parent_id, merged.run_id, merged.verdict) == (
            moved.id,
            run_id,
            Verdict.KEEP,
        )

        request_remeasure(s, merged, 'orchestrator')
        s.commit()
        merged_id = merged.id
    ex.remeasure(merged_id)
    with db.session() as s:
        c = s.get_one(Candidate, merged_id)
        assert (len(c.measurements), c.verdict) == (2, Verdict.KEEP), 'a repeat of a judged candidate keeps its verdict'


def test_a_crash_on_the_evaluate_timeout_is_measured_again_on_request_once_the_timeout_is_raised(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A load that slows the evaluator past ``evaluate_timeout`` crashes good code, and its tree cannot be committed
    again. The timeout is no part of the evaluator, so once it is raised, ``piloop candidate remeasure`` measures the
    candidate again under the same evaluator, with the new timeout, and judges it again."""
    measure = 'test $(wc -l < solve.txt) -gt 1 && sleep 1; echo METRIC score=$(wc -l < solve.txt); echo METRIC size=1'
    write_toml(Path(project.path), HARNESS_APPEND, evaluate=['sh', '-c', measure])
    toml = Path(project.path) / 'piloop.toml'
    toml.write_text(toml.read_text().replace('evaluate_timeout = 30', 'evaluate_timeout = 0.2'))
    reload_project(db, project)
    ex = executor(db, runtime, tmp_path)
    ex.remeasure(baseline(db, project.id).id)
    run_id = make_run(db, project)
    execute(ex, db, run_id)
    [crashed] = candidates(db, run_id)
    assert (crashed.verdict, crashed.error) == (Verdict.CRASH, 'evaluate timeout after 0.2s')
    with db.session() as s:
        evaluator_id = s.get_one(Project, project.id).evaluator_id
    toml.write_text(toml.read_text().replace('evaluate_timeout = 0.2', 'evaluate_timeout = 30'))
    reload_project(db, project)
    with db.session() as s:
        request_remeasure(s, s.get_one(Candidate, crashed.id), 'orchestrator')
        s.commit()
    ex.remeasure(crashed.id)
    with db.session() as s:
        c = s.get_one(Candidate, crashed.id)
        assert [m.evaluator_id == evaluator_id for m in c.measurements] == [True, True], 'the same evaluator'
        assert (c.verdict, c.verdict_by, c.verdict_reason) == (
            Verdict.KEEP,
            DAEMON,
            'against candidate#1: score 2 vs 1 (+1; aggregate: no per-unit results)',
        )
        assert head(s, s.get_one(Project, project.id)) == c


def test_a_remeasure_that_a_stop_cancels_records_and_judges_nothing(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    ex = executor(db, runtime, tmp_path)
    ex.launcher.cancel()
    ex.remeasure(baseline(db, project.id).id)
    with db.session() as s:
        c = s.get_one(Candidate, baseline(db, project.id).id)
        assert (len(c.measurements), c.verdict) == (1, Verdict.KEEP), 'the stop owns the rows'


def test_brief_carries_history_and_comments(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    write_toml(Path(project.path), HARNESS_PROMPT_TO_FILE)
    reload_project(db, project)
    first = make_run(db, project, title='first idea')
    execute(executor(db, runtime, tmp_path), db, first)  # produces an uncommitted prompt.txt → scored as a commit
    second = make_run(db, project, title='second idea')
    with db.session() as s:
        idea_id = s.get_one(Run, second).idea_id
        s.add(Comment(target_kind=TargetKind.IDEA, target_id=idea_id, author='human:t', body='try harder'))
        s.add(Comment(target_kind=TargetKind.RUN, target_id=second, author='pi', kind='decision', body='and faster'))
        s.add(Comment(target_kind=TargetKind.RUN, target_id=first, author='pi', body='about another idea'))
        s.commit()
    execute(executor(db, runtime, tmp_path), db, second)
    brief = run_row(db, second).brief or ''
    order = [
        '# Worker',
        '# Demo',
        '# Working with the piloop database',
        '# Context',
        '## Your idea',
        '## Comments on this idea',
        '## History: ideas',
        '## History: kept lineage',
    ]
    assert [brief.index(s) for s in order] == sorted(brief.index(s) for s in order), 'the fixed brief order'
    assert '## Your idea (idea#2): second idea' in brief and 'candidate#2' in brief
    assert (
        '## Comments on this idea and its runs\n- human:t (note): try harder\n- pi (decision on run#2): and faster\n'
        in (brief)
    ), "the idea's comments and those on any of its runs, in order, each run comment saying which run"
    assert 'about another idea' not in brief
    assert '| 1 ' in brief and 'first idea' in brief, 'the same history block the research agent reads'
    assert 'Tried so far' not in brief
    assert '-m piloop sql' in brief and "'worker#2', 'finding'" in brief
    assert "VALUES ('idea', 2, 'worker#2', 'escalation', 1, '...')" in brief, 'a request is an escalation, never a note'
    assert 'Never wait with `pgrep -f "<pattern>"`' in brief, 'a waiter that finds itself keeps the run open'
    assert '- A hunch worth a run of its own goes in your finding' in brief and 'idea seed demo' not in brief


@pytest.mark.parametrize('budget', [400, 0])
def test_a_worker_brief_opens_with_the_current_lessons_of_the_repo_within_its_budget(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, budget: int
) -> None:
    with db.session() as s:
        p = s.get_one(Project, project.id)
        p.repo = 'github.com/o/r'
        p.config = load_project_config(Path(project.path), roles={'worker': {'wake_budget': budget}}).model_dump(
            mode='json'
        )
        s.add(
            Lesson(
                project_id=project.id,
                family='voting',
                status=LessonStatus.DEAD,
                effect=0,
                metric='score',
                body='idea#1 on evaluator#1',
                created_by='watchdog#9',
            )
        )
        s.commit()
    run_id = make_run(db, project)
    execute(executor(db, runtime, tmp_path), db, run_id)
    brief = run_row(db, run_id).brief or ''
    line = '- [demo] voting | dead | +0 score (lesson#1)'
    if budget:
        assert brief.index('# Worker') < brief.index(line) < brief.index('# Demo'), 'after the role, before the project'
    else:
        assert line not in brief and 'What past campaigns' not in brief, '[roles.worker] wake_budget = 0 turns it off'


def test_pre_written_brief_is_used_verbatim(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    run_id = make_run(db, project)
    with db.session() as s:
        s.get_one(Run, run_id).brief = 'custom brief'
        s.commit()
    execute(executor(db, runtime, tmp_path), db, run_id)
    assert (tmp_path / 'runs' / str(run_id) / 'brief.md').read_text() == 'custom brief'


def test_exception_in_run_is_recorded(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, tracer: Tracer, spans: InMemorySpanExporter
) -> None:
    run_id = make_run(db, project, max_runs=1)
    with db.session() as s:
        s.get_one(Run, run_id).base_candidate_id = 999
        s.commit()
    execute(executor(db, runtime, tmp_path, tracer), db, run_id)
    run = run_row(db, run_id)
    assert run.status is RunStatus.FAILED and run.error is not None and run.error.startswith('NoResultFound')
    with db.session() as s:
        assert s.get_one(Idea, run.idea_id).status is IdeaStatus.DONE
    [span] = spans.get_finished_spans()
    assert span.status.status_code is StatusCode.ERROR and [e.name for e in span.events] == ['exception']


def test_the_head_is_the_latest_trunk_keep_whatever_it_scores(db: Database, project: Project) -> None:
    with db.session() as s:
        proj = s.get_one(Project, project.id)
        evaluator_id = proj.evaluator_id
        assert evaluator_id is not None
        idea = Idea(track='score', project_id=project.id, title='t', hypothesis='h', created_by='t')
        s.add(idea)
        s.flush()
        branch = Run(idea_id=idea.id, attempt=1, base_candidate_id=1, harness='h', model='m', branch=True)
        branch.status = RunStatus.SUCCEEDED
        s.add(branch)
        s.flush()
        rows = {}
        for tree, verdict, run_id, score in (
            ('a', Verdict.KEEP, None, 9.0),
            ('b', Verdict.KEEP, None, 1.0),
            ('c', Verdict.INTERESTING, None, 99.0),
            ('d', Verdict.KEEP, branch.id, 50.0),
            ('r', Verdict.REFERENCE, None, 5.0),
        ):
            rows[tree] = c = Candidate(
                project_id=project.id, tree_sha=tree, commit_sha=tree, verdict=verdict, run_id=run_id
            )
            s.add(c)
            s.flush()
            s.add(Measurement(candidate_id=c.id, evaluator_id=evaluator_id, metrics={'score': score}))
        s.commit()
        trunk = head(s, proj)
        assert trunk is not None and trunk.tree_sha == 'b', (
            'b is the latest trunk keep: a lower score than a changes nothing, an interesting is no keep, a branch'
            ' run keep (d) never moves the head and a reference is never one'
        )
        assert head(s, proj, excluding=rows['b']) == rows['a'], 'the candidate being judged is never its own bar'
        unscored = Candidate(project_id=project.id, tree_sha='u', commit_sha='u', verdict=Verdict.REFERENCE)
        s.add(unscored)
        s.flush()
        assert [c.tree_sha for c in due(s, proj)] == ['u'], 'a reference that no evaluator measured'
        proj.evaluator_id = s.scalar(
            insert(Evaluator).values(project_id=project.id, key='next', definition={}).returning(Evaluator.id)
        )
        assert [c.tree_sha for c in due(s, proj)] == ['b', 'u'], (
            'under a new evaluator the head, not the older keeps, the branch keep or the measured reference'
        )


def test_a_new_evaluator_measures_what_a_verdict_needs_then_what_someone_asked_for(
    db: Database, project: Project
) -> None:
    """After an evaluator change, the daemon measures the head, the bars of open runs, the asks and the bars of pinned
    ideas, in that order. A requested crash that the daemon judges again comes after the bar it is judged against.

    Every other keep keeps its old scores until someone asks: measuring the whole history again held every evaluation
    for hours after each change of the evaluator.
    """
    with db.session() as s:
        proj = s.get_one(Project, project.id)
        old = proj.evaluator_id
        assert old is not None
        idea = Idea(track='score', project_id=project.id, title='t', hypothesis='h', created_by='t')
        s.add(idea)
        s.flush()
        rows: dict[str, Candidate] = {}

        def add(tree: str, verdict: Verdict, parent: str | None = None, run: Run | None = None) -> Candidate:
            rows[tree] = c = Candidate(
                project_id=project.id,
                tree_sha=tree,
                commit_sha=tree,
                verdict=verdict,
                verdict_by=DAEMON,
                parent_id=rows[parent].id if parent else None,
                run_id=run.id if run else None,
            )
            s.add(c)
            s.flush()
            s.add(Measurement(candidate_id=c.id, evaluator_id=old, metrics={'score': 1.0}))
            return c

        for tree in ('k1', 'k2', 'k3', 'k4', 'k5'):
            add(tree, Verdict.KEEP, parent='k1' if tree != 'k1' else None)
        add('lead', Verdict.INTERESTING, parent='k1')
        add('ref', Verdict.REFERENCE)
        done = Run(idea_id=idea.id, attempt=1, base_candidate_id=rows['k2'].id, harness='h', model='m')
        done.status = RunStatus.SUCCEEDED
        s.add_all([done, Run(idea_id=idea.id, attempt=2, base_candidate_id=rows['lead'].id, harness='h', model='m')])
        s.flush()
        add('crash', Verdict.CRASH, parent='k2', run=done)
        add('repeat', Verdict.DISCARD, parent='k3', run=done)
        add('head', Verdict.KEEP, parent='k5')
        fresh = Candidate(project_id=project.id, tree_sha='fresh', commit_sha='fresh', verdict=Verdict.REFERENCE)
        s.add(fresh)
        s.add(
            Idea(
                track='score',
                project_id=project.id,
                title='pinned',
                hypothesis='h',
                created_by='t',
                base_candidate_id=rows['k4'].id,
            )
        )
        for asked in ('crash', 'repeat'):
            request_remeasure(s, rows[asked], 'orchestrator')
        proj.evaluator_id = new = s.scalar(
            insert(Evaluator).values(project_id=project.id, key='next', definition={}).returning(Evaluator.id)
        )
        s.flush()
        assert [c.tree_sha for c in due(s, proj)] == ['head', 'k1', 'k2', 'crash', 'repeat', 'k4', 'fresh'], (
            'the head; the bar of the open run (its base is a lead of k1); the bar of the asked crash, then the crash;'
            ' the asked repeat alone; the bar of the pinned idea; the new reference. Not k3, k5, the baseline or ref'
        )
        for tree in ('head', 'k1', 'k2'):
            s.add(Measurement(candidate_id=rows[tree].id, evaluator_id=new, metrics={'score': 1.0}))
        s.flush()
        s.expire_all()
        assert [c.tree_sha for c in due(s, proj)] == ['crash', 'repeat', 'k4', 'fresh'], 'measured: no longer due'


def test_transcript_failure_is_only_an_event(
    db: Database,
    project: Project,
    runtime: ScriptRuntime,
    tmp_path: Path,
    mocker: MockerFixture,
    tracer: Tracer,
    spans: InMemorySpanExporter,
) -> None:
    mocker.patch.object(runtime, 'read', side_effect=OSError('pane gone'))
    run_id = make_run(db, project)
    execute(executor(db, runtime, tmp_path, tracer), db, run_id)
    assert run_row(db, run_id).status is RunStatus.SUCCEEDED
    assert events(spans, 'worker.transcript_lost') == [{'error': 'pane gone'}]


def test_a_rejection_during_the_run_is_not_undone(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    run_id = make_run(db, project)
    with db.session() as s:
        s.get_one(Idea, s.get_one(Run, run_id).idea_id).status = IdeaStatus.REJECTED
        s.commit()
    execute(executor(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        assert s.get_one(Idea, 1).status is IdeaStatus.REJECTED


def test_worker_env_points_at_the_daemon_home(
    db: Database, project: Project, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The worker sees the daemon's home and PATH, and not the child marker of a Claude Code session.

    A daemon started from a Claude Code session inherits that marker, and a Claude agent that has it saves no
    transcript. The marker is cleared for every agent, whatever its harness definition: ``[harness.claude]`` replaces
    the built-in one, and a custom harness can run the same CLI. This one is the ``script`` harness of the fixture.
    """
    monkeypatch.setenv('CLAUDE_CODE_CHILD_SESSION', '1')
    write_toml(
        Path(project.path),
        '#!/bin/sh\necho "$PILOOP_HOME" > home.txt\necho "$PATH" >> home.txt\n'
        'echo "child=$CLAUDE_CODE_CHILD_SESSION" >> home.txt\ngit add home.txt\ngit commit -qm home\n',
    )
    reload_project(db, project)
    run_id = make_run(db, project)
    execute(executor(db, ScriptRuntime(), tmp_path), db, run_id)
    [e] = candidates(db, run_id)
    assert Repo(project.path).git.show(f'{e.commit_sha}:home.txt').splitlines() == [
        str(tmp_path),
        os.environ['PATH'],
        'child=',
    ]


def test_lineage_follows_the_actual_predecessor(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    write_toml(
        Path(project.path),
        '#!/bin/sh\n: > solve.txt\ngit commit -qam worse\necho a >> solve.txt\necho b >> solve.txt\ngit commit -qam better\n',
    )
    reload_project(db, project)
    run_id = make_run(db, project)
    execute(executor(db, runtime, tmp_path), db, run_id)
    worse, better = candidates(db, run_id)
    assert (worse.verdict, better.verdict) == (Verdict.DISCARD, Verdict.KEEP)
    assert better.parent_id == worse.id, 'built on the discarded commit, judged against the last keep'


def test_a_worker_that_never_got_going_is_relaunched_on_its_run_then_the_run_fails(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """A worker that dies at startup is not an attempt: the same run is queued again for a new worker.

    The dead worker's transcript is kept aside. Only a run whose every launch died fails, and only then does it count
    toward ``max_runs``.
    """
    mocker.patch.object(runtime, 'start', side_effect=StartFailed('HerdrError: prompt_lost: no pickup', 'Welcome\n'))
    run_id = make_run(db, project, max_runs=1)
    ex = executor(db, runtime, tmp_path)
    for launches in (1, 2):
        execute(ex, db, run_id)
        with db.session() as s:
            run = s.get_one(Run, run_id)
            assert (run.status, run.finished_at, run.idea.status) == (RunStatus.QUEUED, None, IdeaStatus.SCHEDULED)
            rows = s.scalars(select(Worker).where(Worker.run_id == run_id).order_by(Worker.id)).all()
            assert [w.state for w in rows] == [*[WorkerState.KILLED] * launches, WorkerState.REQUESTED]
            assert (rows[-1].harness, rows[-1].model) == ('script', 'test-model')
            assert rows[-1].reason == f'run#{run_id} relaunched: worker#{rows[-2].id} failed to start', 'its role'
            assert stop.runs(s, run.idea.project) == 1
        assert (tmp_path / 'runs' / str(run_id) / f'transcript-worker{rows[-2].id}.txt').read_text() == 'Welcome\n'
    execute(ex, db, run_id)
    with db.session() as s:
        relaunched = s.scalars(select(Event).where(Event.kind == 'run.relaunched')).all()
        assert [e.payload['launches'] for e in relaunched] == [1, 2]
        idea = s.get_one(Idea, run_row(db, run_id).idea_id)
        assert idea.status is IdeaStatus.PARKED, 'its last attempt failed to start'
        [note] = s.scalars(select(Comment).where(Comment.target_id == idea.id)).all()
        assert (note.target_kind, note.author, note.kind, note.body) == (
            TargetKind.IDEA,
            'daemon',
            'note',
            f'parked: its last attempt (run#{run_id}) failed to start; fix what stopped the agent (the run error, the'
            ' anomaly.startup event), then set it ready',
        )
    run = run_row(db, run_id)
    assert run.status is RunStatus.FAILED and (run.error or '').startswith('HerdrError: prompt_lost')

    mocker.patch.object(runtime, 'start', side_effect=RuntimeError('no server'))
    retried = make_run(db, project, title='retried', max_runs=2)
    for _ in range(3):
        execute(ex, db, retried)
    with db.session() as s:
        assert s.get_one(Idea, run_row(db, retried).idea_id).status is IdeaStatus.READY, 'one attempt left'


def test_a_relaunched_worker_signs_as_itself_and_its_finding_without_a_commit_is_a_result(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """A relaunched worker gets a brief that names it, not the dead worker, and its finding without a commit counts."""
    run_id = make_run(db, project, max_runs=1)
    idea_id = run_row(db, run_id).idea_id
    finding = f"INSERT INTO comments (target_kind, target_id, author, kind, body) VALUES ('idea', {idea_id}, '$me', 'finding', 'no effect')"
    me = 'me=$(grep -o "Sign what you write as .worker#[0-9]*" ../brief.md | grep -o "worker#[0-9]*")'
    write_toml(Path(project.path), f'#!/bin/sh\n{me}\n{SQL_HINT} "{finding}"\n')
    reload_project(db, project)
    start = mocker.patch.object(runtime, 'start', wraps=runtime.start)
    start.side_effect = [StartFailed('HerdrError: prompt_lost: no pickup', ''), DEFAULT]
    ex = executor(db, runtime, tmp_path)
    execute(ex, db, run_id)
    execute(ex, db, run_id)
    run = run_row(db, run_id)
    assert (run.status, run.error) == (RunStatus.SUCCEEDED, None)
    with db.session() as s:
        dead, replacement = s.scalars(select(Worker.id).where(Worker.run_id == run_id).order_by(Worker.id)).all()
        [author] = s.scalars(select(Comment.author).where(Comment.kind == 'finding')).all()
        assert author == f'worker#{replacement}'
        assert f'worker#{dead}' not in (run.brief or '') and f'You are worker#{replacement},' in (run.brief or '')
        assert s.get_one(Idea, idea_id).status is IdeaStatus.DONE


@pytest.mark.parametrize('work', ['commit', 'finding'])
def test_a_fast_worker_whose_tokens_are_unknown_is_no_startup_death_when_it_committed_or_left_a_finding(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, work: str
) -> None:
    """A worker whose tokens are unknown is not a startup death when it committed or left a finding.

    The runtime may report no session for a worker, so its tokens cannot be read. Work done within the startup window
    proves that it started.
    """
    monkeypatch.setenv('HOME', str(tmp_path / 'home'))  # no session file: the tokens are unknown
    run_id = make_run(db, project, max_runs=1)
    idea_id = run_row(db, run_id).idea_id
    finding = f"INSERT INTO comments (target_kind, target_id, author, kind, body) VALUES ('idea', {idea_id}, '$me', 'finding', 'no effect')"
    me = 'me=$(grep -o "Sign what you write as .worker#[0-9]*" ../brief.md | grep -o "worker#[0-9]*")'
    body = HARNESS_APPEND if work == 'commit' else f'#!/bin/sh\n{me}\n{SQL_HINT} "{finding}"\n'
    write_toml(Path(project.path), body)
    script = load_project_config(Path(project.path)).harness['script'].model_dump(mode='json')
    counted = {'headless': ['--session-id', '{session_id}', '{prompt}'], 'usage': PI_SPEC}
    set_config(db, project, harness={'script': {**script, **counted}})
    execute(executor(db, runtime, tmp_path), db, run_id)
    run = run_row(db, run_id)
    assert (run.status, run.error) == (RunStatus.SUCCEEDED, None)
    with db.session() as s:
        assert s.scalar(select(func.count()).select_from(Worker).where(Worker.run_id == run_id)) == 1, 'no relaunch'
        assert not s.scalars(select(Event).where(Event.kind.in_(['run.relaunched', STARTUP_ANOMALY]))).all()


def test_a_startup_death_during_a_stop_leaves_the_run_to_the_stop(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """A startup death during a stop does not relaunch the run, because the stop fails the open runs itself."""
    ex = executor(db, runtime, tmp_path)

    def stopped_meanwhile(spec: object) -> Handle:
        ex.launcher.cancelled.set()  # what ``cancel`` sets; it takes the lock this start holds
        raise StartFailed('HerdrError: prompt_lost: no pickup', '')

    mocker.patch.object(runtime, 'start', side_effect=stopped_meanwhile)
    run_id = make_run(db, project)
    execute(ex, db, run_id)
    with db.session() as s:
        assert s.get_one(Run, run_id).status is RunStatus.RUNNING, 'left for the stop to fail'
        assert s.scalar(select(func.count()).select_from(Worker).where(Worker.run_id == run_id)) == 1


def test_a_finding_without_a_commit_is_a_result(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A run that posts a finding and commits nothing succeeds, and its idea is not retried.

    Some plans, such as an audit, produce a finding and no code.
    """
    run_id = make_run(db, project, max_runs=2)
    with db.session() as s:
        idea_id, worker_id = run_row(db, run_id).idea_id, s.scalars(select(Worker.id)).one()
    finding = f"INSERT INTO comments (target_kind, target_id, author, kind, body) VALUES ('idea', {idea_id}, 'worker#{worker_id}', 'finding', 'arm B: no effect')"
    write_toml(Path(project.path), f'#!/bin/sh\n{SQL_HINT} "{finding}"\n')
    reload_project(db, project)
    execute(executor(db, runtime, tmp_path), db, run_id)
    run = run_row(db, run_id)
    assert (run.status, run.error) == (RunStatus.SUCCEEDED, None)
    with db.session() as s:
        assert s.get_one(Idea, idea_id).status is IdeaStatus.DONE, 'not retried'


def test_a_worker_that_stops_on_its_open_escalation_is_blocked_and_its_idea_waits_for_the_answer(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A worker that asks for a decision and stops before it comes did not succeed, and its idea is not done.

    The run fails as blocked, which no incident sweep investigates, and the idea is parked with a note that names the
    escalation. A retry would stop on the same question, and ``done`` would release the ideas that wait for it.
    """
    run_id = make_run(db, project, max_runs=2)
    with db.session() as s:
        idea_id, worker_id = run_row(db, run_id).idea_id, s.scalars(select(Worker.id)).one()
    ask = (
        'INSERT INTO comments (target_kind, target_id, author, kind, priority, body) VALUES'
        f" ('idea', {idea_id}, 'worker#{worker_id}', 'escalation', 1, 'run the audit that I cannot run')"
    )
    report = (
        'INSERT INTO comments (target_kind, target_id, author, kind, body) VALUES'
        f" ('idea', {idea_id}, 'worker#{worker_id}', 'finding', 'blocked on the audit')"
    )
    write_toml(
        Path(project.path),
        (f'#!/bin/sh\n{SQL_HINT} "{ask}"\n{SQL_HINT} "{report}"\necho line >> solve.txt\ngit commit -qam "a line"\n'),
    )
    reload_project(db, project)
    execute(executor(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        escalation = s.scalars(select(Comment.id).where(Comment.kind == 'escalation')).one()
        run = s.get_one(Run, run_id)
        assert (run.status, run.error) == (RunStatus.FAILED, f'{stop.BLOCKED} comment#{escalation}')
        assert s.get_one(Idea, idea_id).status is IdeaStatus.PARKED
        note = s.scalars(select(Comment).where(Comment.author == 'daemon', Comment.target_id == idea_id)).one()
        assert f'escalation comment#{escalation}' in note.body and 'set the idea ready' in note.body
        assert [c.verdict for c in s.scalars(select(Candidate).where(Candidate.run_id == run_id))] == [Verdict.KEEP], (
            'its commits are scored as any run'
        )


def test_an_escalation_answered_before_the_worker_stops_does_not_block_its_run(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    run_id = make_run(db, project, max_runs=2)
    with db.session() as s:
        idea_id, worker_id = run_row(db, run_id).idea_id, s.scalars(select(Worker.id)).one()
        s.add(
            Comment(
                target_kind=TargetKind.RUN,
                target_id=run_id,
                author=f'worker#{worker_id}',
                kind='escalation',
                priority=1,
                body='may I use the cache?',
                acked_at=utcnow(),
                acked_by='orchestrator',
            )
        )
        s.add(
            Comment(
                target_kind=TargetKind.IDEA,
                target_id=idea_id,
                author='watchdog#9',
                kind='escalation',
                priority=1,
                body='not the worker',
            )
        )
        s.commit()
    execute(executor(db, runtime, tmp_path), db, run_id)
    assert (run_row(db, run_id).status, run_row(db, run_id).error) == (RunStatus.SUCCEEDED, None)
    with db.session() as s:
        assert s.get_one(Idea, idea_id).status is IdeaStatus.DONE


def test_uncommitted_changes_without_a_finding_are_scored_but_not_an_attempt_that_finished(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A worker that leaves uncommitted changes and no finding did not finish its idea.

    The daemon commits and scores the changes, but the run fails and the idea is ready again. A finding on the run
    makes the same run a success.
    """
    write_toml(Path(project.path), HARNESS_FORGETS_COMMIT)
    reload_project(db, project)
    run_id = make_run(db, project, max_runs=2)
    execute(executor(db, runtime, tmp_path), db, run_id)
    run = run_row(db, run_id)
    assert (run.status, run.error) == (RunStatus.FAILED, INTERRUPTED)
    [(summary, verdict, *_)] = scored(db, run_id)
    assert (summary, verdict) == (UNCOMMITTED_SUMMARY, Verdict.RECOVERED), 'what it left is kept, not judged'
    with db.session() as s:
        assert s.get_one(Idea, run.idea_id).status is IdeaStatus.READY

    run_id = make_run(db, project, title='reported', max_runs=2)
    with db.session() as s:
        idea_id, worker_id = (
            run_row(db, run_id).idea_id,
            s.scalars(select(Worker.id).where(Worker.run_id == run_id)).one(),
        )
        s.add(
            Comment(
                target_kind=TargetKind.RUN, target_id=run_id, author=f'worker#{worker_id}', kind='finding', body='x'
            )
        )
        s.add(Comment(target_kind=TargetKind.IDEA, target_id=idea_id, author='watchdog#9', kind='finding', body='y'))
        s.commit()
    write_toml(Path(project.path), '#!/bin/sh\necho other >> solve.txt\n')
    reload_project(db, project)
    execute(executor(db, runtime, tmp_path), db, run_id)
    assert run_row(db, run_id).status is RunStatus.SUCCEEDED, 'it reported where it stands'


def test_a_cancelled_executor_leaves_the_bookkeeping_to_the_scheduler(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, tracer: Tracer, spans: InMemorySpanExporter
) -> None:
    ex = executor(db, runtime, tmp_path, tracer)
    queued = make_run(db, project)
    ex.launcher.cancel()
    execute(ex, db, queued)
    assert run_row(db, queued).status is RunStatus.QUEUED, 'not even started'
    assert dict(span_named(spans, 'piloop.run').attributes or {}) == {'run': queued, 'status': 'cancelled'}


def test_a_worker_dying_after_cancel_is_not_a_run_failure(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    ex = executor(db, runtime, tmp_path)

    def tab_closed(handle: Handle, timeout: float) -> Outcome:
        ex.launcher.cancel()
        raise RuntimeError('agent_not_found')

    mocker.patch.object(runtime, 'wait', side_effect=tab_closed)
    run_id = make_run(db, project)
    execute(ex, db, run_id)
    run = run_row(db, run_id)
    assert (run.status, run.error) == (RunStatus.RUNNING, None), 'stop() writes the row, not the dying thread'
    assert not (tmp_path / 'runs' / str(run_id) / 'worktree').exists()


def test_a_stop_between_the_workers_end_and_the_runs_close_leaves_the_row_to_the_scheduler(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    ex = executor(db, runtime, tmp_path)

    def stopped_meanwhile(*args: object) -> list[str]:
        ex.launcher.cancel()
        raise RuntimeError('worktree pulled away')

    mocker.patch.object(RunExecutor, '_commits', side_effect=stopped_meanwhile)
    run_id = make_run(db, project)
    execute(ex, db, run_id)
    run = run_row(db, run_id)
    assert (run.status, run.error) == (RunStatus.RUNNING, None), 'stop() writes the row, not the dying thread'
    with db.session() as s:
        assert s.get_one(Idea, run.idea_id).status is IdeaStatus.SCHEDULED, 'and returns the idea'


def test_an_evaluator_spawned_after_cancel_is_killed_at_once(
    db: Database, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    ex = executor(db, runtime, tmp_path)
    ex.cancel()
    proc = subprocess.Popen(['sleep', '30'], start_new_session=True)
    ex._track_evaluation(proc)  # what run_evaluate calls right after Popen: cancel() may have won meanwhile
    assert proc.wait(timeout=2) == -9


def test_a_commit_that_reverts_to_a_known_tree_is_that_candidate_and_is_not_measured_again(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, tracer: Tracer, spans: InMemorySpanExporter
) -> None:
    write_toml(Path(project.path), HARNESS_TRY_AND_REVERT)
    reload_project(db, project)
    run_id = make_run(db, project)
    execute(executor(db, runtime, tmp_path, tracer), db, run_id)
    [tried] = candidates(db, run_id)
    assert (tried.summary, tried.verdict, tried.metrics['score']) == ('try', Verdict.KEEP, 2.0)
    base = baseline(db, project.id)
    with db.session() as s:
        assert s.scalar(select(func.count()).select_from(Candidate)) == 2, 'the revert is the baseline, not a third row'
        assert len(s.get_one(Candidate, base.id).measurements) == 1, 'and the baseline was not measured again'
        kinds = [ev.kind for ev in s.scalars(select(Event)).all()]
    assert kinds.count('measurement.recorded') == 1 and 'candidate.judged' in kinds
    known = [sp for sp in spans.get_finished_spans() if sp.name == 'piloop.candidate' and sp.attributes]
    assert [sp.attributes.get('status') for sp in known if sp.attributes] == [None, 'known']
    repo = Repo(project.path)
    assert len([r for r in repo.refs if r.path.startswith('refs/piloop/')]) == 2, 'both commits are still pinned'


def test_a_changed_evaluator_measures_again_and_old_numbers_do_not_count(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    run_id = make_run(db, project)
    execute(executor(db, runtime, tmp_path), db, run_id)
    [added] = candidates(db, run_id)
    assert added.metrics['score'] == 2.0
    doubled = ['sh', '-c', 'echo METRIC score=$(( 2 * $(wc -l < solve.txt) )); echo METRIC size=1']
    write_toml(Path(project.path), HARNESS_TRY_AND_REVERT, evaluate=doubled)
    with db.session() as s:
        proj = s.get_one(Project, project.id)
        before = proj.evaluator_id
        sync(s, proj)
        s.commit()
        assert proj.evaluator_id != before, 'a new evaluate command is a new evaluator'
        trunk = head(s, proj)
        assert trunk is not None and trunk.id == added.id, (
            'nothing is measured under the new evaluator yet, and the head stays the latest trunk keep'
        )
    second = make_run(db, project, title='again')
    with db.session() as s:
        s.get_one(Run, second).base_candidate_id = added.id
        s.commit()
    execute(executor(db, runtime, tmp_path), db, second)
    with db.session() as s:
        proj = s.get_one(Project, project.id)
        tried = s.scalars(select(Candidate).where(Candidate.run_id == second)).one()
        assert [m.metrics['score'] for m in tried.measurements] == [6.0], 'measured under the doubled evaluator'
        base = s.get_one(Candidate, added.id)
        assert [m.metrics['score'] for m in base.measurements] == [2.0, 4.0], (
            'the revert resolved to the base, which the new evaluator had not seen: measured once more, under it'
        )
        assert base.verdict is Verdict.KEEP, 'a known candidate keeps its verdict; the revert says nothing new about it'
        assert (tried.verdict, tried.verdict_by) == (Verdict.KEEP, DAEMON), 'an unmeasured bar is beaten by any score'
        trunk = head(s, proj)
        assert trunk is not None and trunk.id == tried.id


def test_a_config_change_during_a_run_scores_it_under_the_new_evaluator(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``piloop init`` during a long run: the run finishing must not point the project back at the old evaluator."""
    gate = Gate()
    monkeypatch.setattr(runtime, 'start', gate.wrap(runtime.start))
    run_id = make_run(db, project)
    thread = threading.Thread(target=execute, args=(executor(db, runtime, tmp_path), db, run_id))
    thread.start()
    assert gate.entered.wait(10), 'the worker is about to start; the run holds the old config'
    doubled = ['sh', '-c', 'echo METRIC score=$(( 2 * $(wc -l < solve.txt) )); echo METRIC size=1']
    write_toml(Path(project.path), HARNESS_APPEND, evaluate=doubled)
    with db.session() as s:
        proj = s.get_one(Project, project.id)
        sync(s, proj)
        s.commit()
        new_evaluator = proj.evaluator_id
    gate.release.set()
    thread.join(30)
    with db.session() as s:
        assert s.get_one(Project, project.id).evaluator_id == new_evaluator
        [added] = candidates(db, run_id)
        assert [m.evaluator_id for m in s.get_one(Candidate, added.id).measurements] == [new_evaluator]
        assert added.metrics['score'] == 4.0, 'measured under the doubled evaluator'


def test_entire_settings_reach_a_worktree_whose_tree_lacks_them_without_being_committed(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    repo = Repo(project.path)
    (Path(project.path) / ENTIRE_DIR).mkdir()
    (Path(project.path) / ENTIRE_DIR / 'settings.json').write_text('{"enabled":true}')
    write_toml(Path(project.path), HARNESS_READS_ENTIRE)
    reload_project(db, project)
    run_id = make_run(db, project)
    execute(executor(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        sha = s.scalars(select(Candidate.commit_sha).where(Candidate.run_id == run_id)).one()
    assert repo.git.show(f'{sha}:seen.json') == '{"enabled":true}', 'the worktree saw the project settings'
    assert ENTIRE_DIR not in repo.git.ls_tree('--name-only', sha), 'a link is never committed'
    write_toml(Path(project.path), HARNESS_COMMITS_ENTIRE)
    reload_project(db, project)
    second = make_run(db, project, title='commit the settings')
    execute(executor(db, runtime, tmp_path), db, second)
    [e] = candidates_measured(db, second)
    assert e.summary == 'entire settings' and e.metrics['score'] == 1.0, 'the dirt after it is recovered on its own'
    assert repo.git.show(f'{e.commit_sha}:.entire/settings.json') == '{}', 'a tracked .entire stays in the commit'


def test_the_worker_session_is_attached_inside_the_worktree_before_it_is_removed(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, tracer: Tracer, entire_log
) -> None:
    write_toml(Path(project.path), HARNESS_APPEND)
    cfg = load_project_config(Path(project.path))
    script = {
        **cfg.harness['script'].model_dump(mode='json'),
        'entire_agent': 'claude-code',
        'headless': ['{session_id}', '{prompt}'],
        'interactive': ['{session_id}'],
    }
    with db.session() as s:
        s.get_one(Project, project.id).config = load_project_config(
            Path(project.path), harness={'script': script}
        ).model_dump(mode='json')
        s.commit()
    run_id = make_run(db, project)
    execute(executor(db, runtime, tmp_path, tracer), db, run_id)
    with db.session() as s:
        w = s.scalars(select(Worker).where(Worker.run_id == run_id)).one()
        assert (w.checkpoint_id, run_row(db, run_id).status) == (CHECKPOINT, RunStatus.SUCCEEDED)
        assert w.session_id is not None
        assert entire_log() == [
            {
                'argv': ['session', 'attach', w.session_id, '--agent', 'claude-code'],
                'cwd': str(tmp_path / 'runs' / str(run_id) / 'worktree'),
            },
            {
                'argv': ['session', 'info', w.session_id, '--json'],
                'cwd': str(tmp_path / 'runs' / str(run_id) / 'worktree'),
            },
        ], 'entire ran in the worktree, which only exists until the run is over'
    assert not (tmp_path / 'runs' / str(run_id) / 'worktree').exists()


SPLIT_UNITS = [
    'sh',
    '-c',
    'L=$(wc -l < solve.txt); echo METRIC score=$L; echo METRIC size=$(wc -c < solve.txt);'
    ' [ -z "$PILOOP_UNITS_FILE" ] || printf \'{"score": {"a": %d, "b": %d}}\' $((2*L-1)) $((1-L)) > "$PILOOP_UNITS_FILE"',
]
"""score = lines, split over two units so one more line is +2 on ``a`` and -1 on ``b``: up, but not significantly."""


def set_config(db: Database, project: Project, **overrides: object) -> None:
    with db.session() as s:
        s.get_one(Project, project.id).config = load_project_config(Path(project.path), **overrides).model_dump(
            mode='json'
        )
        s.commit()


def test_per_unit_results_decide_the_verdict_and_live_outside_the_database(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    units_dir = tmp_path / 'units'
    with db.session() as s:
        proj = s.get_one(Project, project.id)
        cfg = load_project_config(Path(project.path), evaluate=SPLIT_UNITS)
        proj.config = cfg.model_dump(mode='json')
        bar = Measurement(
            candidate_id=baseline(db, project.id).id,
            evaluator_id=current_evaluator(s, proj, cfg).id,
            metrics={'score': 1.0, 'size': 5.0},
        )
        s.add(bar)
        s.commit()
        base_measurement = bar.id
    save_units(units_dir, base_measurement, {'score': {'a': 1.0, 'b': 0.0}})
    run_id = make_run(db, project)
    execute(executor(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        c = s.scalars(select(Candidate).where(Candidate.run_id == run_id)).one()
        [m] = c.measurements
        assert (c.verdict, c.verdict_by) == (Verdict.INTERESTING, DAEMON)
        assert c.verdict_reason == snapshot(
            'against candidate#1: score 2 vs 1 (+1; 95% worst case -2 over 2 units): the gain is not significant'
        )
        judged = s.scalars(select(Event).where(Event.kind == 'candidate.judged')).one()
        assert judged.payload['reason'] == c.verdict_reason
        assert m.metrics.keys() == {'score', 'size'}, 'per-unit results are not a column any agent can SELECT'
    assert json.loads((units_dir / f'{m.id}.json').read_text()) == {'score': {'a': 3, 'b': -1}}
    assert sorted(p.name for p in units_dir.iterdir()) == [f'{base_measurement}.json', f'{m.id}.json'], (
        'no pending file is left behind'
    )


def test_without_per_unit_results_the_aggregate_decides_and_the_reason_says_so(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    run_id = make_run(db, project)
    execute(executor(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        c = s.scalars(select(Candidate).where(Candidate.run_id == run_id)).one()
        assert (c.verdict, c.verdict_reason) == snapshot(
            (Verdict.KEEP, 'against candidate#1: score 2 vs 1 (+1; aggregate: no per-unit results)')
        )
    assert list((tmp_path / 'units').iterdir()) == []


def test_guardrails_are_enforced_by_the_daemon_even_after_the_campaign_stopped(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """The daemon enforces a guardrail floor, also on a run still in flight after the project stopped."""
    metrics = [
        {'name': 'score', 'direction': 'higher', 'role': 'primary'},
        {'name': 'size', 'direction': 'lower', 'role': 'guardrail', 'margin': 100, 'floor': 8},
    ]
    set_config(db, project, metrics=metrics)
    run_id = make_run(db, project)
    with db.session() as s:
        proj = s.get_one(Project, project.id)
        proj.status, proj.stopped_reason = ProjectStatus.STOPPED, 'max_runs: 1'
        s.commit()
    execute(executor(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        c = s.scalars(select(Candidate).where(Candidate.run_id == run_id)).one()
        assert (c.verdict, c.verdict_reason) == snapshot((Verdict.DISCARD, 'floor: size 10 past its floor 8'))


def test_units_of_a_candidate_are_the_mean_per_unit_over_its_measurements(
    db: Database, project: Project, tmp_path: Path
) -> None:
    with db.session() as s:
        evaluator_id = s.get_one(Project, project.id).evaluator_id
        assert evaluator_id is not None
        c = Candidate(project_id=project.id, tree_sha='t', commit_sha='t')
        s.add(c)
        s.flush()
        rows = [Measurement(candidate_id=c.id, evaluator_id=evaluator_id, metrics={'score': 1.0}) for _ in range(4)]
        rows[3].error = 'crashed'
        s.add_all(rows)
        s.flush()
        save_units(tmp_path, rows[0].id, {'score': {'a': 1.0, 'b': 0.0}})
        save_units(tmp_path, rows[1].id, {'score': {'a': 3.0, 'b': 0.0}})
        save_units(tmp_path, rows[3].id, {'score': {'a': 100.0, 'b': 0.0}})
        (tmp_path / f'{rows[2].id}.json').write_text('{"score": {"a": 50}}')
        save_units(tmp_path, rows[2].id, {})
        assert units(c, evaluator_id, tmp_path) == {'score': {'a': 2.0, 'b': 0.0}}, (
            'a crashed measurement does not count, and one without units leaves no stale file behind'
        )


HARNESS_SCAFFOLD_THEN_WORK = (
    '#!/bin/sh\necho a >> solve.txt\ngit commit -qam scaffold -m "piloop-eval: SKIP"\n'
    'echo b >> solve.txt\ngit commit -qam work\n'
)


def scored(db: Database, run_id: int) -> list[tuple[str | None, Verdict, str | None, int, int | None]]:
    """``(summary, verdict, reason, measurements, parent)`` of each candidate the run produced, in order."""
    with db.session() as s:
        rows = s.scalars(select(Candidate).where(Candidate.run_id == run_id).order_by(Candidate.id)).all()
        return [(c.summary, c.verdict, c.verdict_reason, len(c.measurements), c.parent_id) for c in rows]


def test_a_commit_marked_skip_is_a_candidate_without_an_evaluation_and_moves_no_bar(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    write_toml(Path(project.path), HARNESS_SCAFFOLD_THEN_WORK)
    reload_project(db, project)
    run_id = make_run(db, project)
    execute(executor(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        first = s.scalars(select(Candidate).where(Candidate.run_id == run_id).order_by(Candidate.id)).first()
        assert first is not None
        skip_id = first.id
    assert scored(db, run_id) == [
        ('scaffold', Verdict.UNMEASURED, SKIPPED, 0, 1),
        ('work', Verdict.KEEP, IsStr(regex=r'against candidate#1: score 3 vs 1 .*'), 1, skip_id),
    ], 'the next commit descends from the scaffold and is judged against the bar, the baseline'
    with db.session() as s:
        judged = s.scalars(select(Event).where(Event.kind == 'candidate.judged').order_by(Event.id)).first()
        assert judged is not None and judged.payload == {'verdict': 'unmeasured', 'score': None, 'reason': SKIPPED}


def test_a_skipped_tree_reached_again_without_the_trailer_is_measured_and_judged(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A revert to a skipped tree is measured and judged against the keep before it.

    The run makes a scaffold commit marked skip, then a work commit that keeps, then reverts to the scaffold's tree.
    """
    write_toml(Path(project.path), HARNESS_SCAFFOLD_THEN_WORK + 'git revert --no-edit HEAD >/dev/null\n')
    reload_project(db, project)
    run_id = make_run(db, project)
    execute(executor(db, runtime, tmp_path), db, run_id)
    [scaffold, work] = scored(db, run_id)
    assert scaffold[1:4] == (Verdict.DISCARD, IsStr(regex=r'against candidate#\d+: score 2 vs 3 .*'), 1)
    assert work[1] is Verdict.KEEP


def test_a_skip_commit_on_a_known_tree_keeps_its_verdict_unmeasured(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    harness = HARNESS_TRY_AND_REVERT + 'git commit -q --amend -m "back" -m "Piloop-Eval: skip"\n'
    write_toml(Path(project.path), harness)
    reload_project(db, project)
    run_id = make_run(db, project)
    execute(executor(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        base = s.get_one(Candidate, baseline(db, project.id).id)
        assert (base.verdict, base.verdict_reason, len(base.measurements)) == (Verdict.KEEP, None, 1)


def test_dirt_after_a_skip_commit_is_committed_on_its_own_and_evaluated(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    harness = '#!/bin/sh\necho a >> solve.txt\ngit commit -qam scaffold -m "Piloop-Eval: skip"\necho b >> solve.txt\n'
    write_toml(Path(project.path), harness)
    reload_project(db, project)
    run_id = make_run(db, project)
    execute(executor(db, runtime, tmp_path), db, run_id)
    assert [(c[0], c[1], c[3]) for c in scored(db, run_id)] == [
        ('scaffold', Verdict.UNMEASURED, 0),
        (UNCOMMITTED_SUMMARY, Verdict.RECOVERED, 0),
    ], 'the work after a skip commit is recovered on its own: never amended into the commit'


def test_a_commit_that_moves_no_deciding_metric_says_no_op(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    write_toml(Path(project.path), '#!/bin/sh\necho x > notes.txt\ngit add notes.txt\ngit commit -qm notes\n')
    reload_project(db, project)
    run_id = make_run(db, project)
    execute(executor(db, runtime, tmp_path), db, run_id)
    [(_, verdict, reason, _, _)] = scored(db, run_id)
    assert (verdict, reason) == (
        Verdict.DISCARD,
        snapshot(
            'no-op: score identical to parent candidate#1; against candidate#1: score 1 vs 1 (+0; aggregate: no per-unit results)'
        ),
    )


def import_empty_solution(db: Database, project: Project) -> int:
    """Import an empty solution from another branch as a reference that scores 0, below the baseline's 1."""
    repo = Repo(project.path)
    repo.git.checkout('-qb', 'elsewhere')
    (Path(project.path) / 'solve.txt').write_text('')
    repo.git.commit('-qam', 'an empty solution')
    repo.git.checkout('-q', 'main')
    with db.session() as s:
        proj = s.get_one(Project, project.id)
        cfg = load_project_config(Path(project.path))
        reference = import_reference(s, proj, cfg, repo, 'elsewhere', summary=None, actor='orchestrator').id
        s.commit()
    return reference


def test_a_run_whose_history_holds_the_head_is_judged_against_the_head_itself(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A run whose base descends from the head has nothing to merge, so a keep must beat the head."""
    reference = import_empty_solution(db, project)
    write_toml(Path(project.path), HARNESS_TWO_COMMITS)
    reload_project(db, project)
    ex = executor(db, runtime, tmp_path)
    ex.remeasure(reference)
    run_id = make_run(db, project)
    with db.session() as s:
        s.get_one(Run, run_id).base_candidate_id = reference
        s.commit()
    execute(ex, db, run_id)
    [(_, first, first_reason, _, parent), (_, second, second_reason, _, _)] = scored(db, run_id)
    assert parent == reference
    assert (first, first_reason) == (
        Verdict.DISCARD,
        snapshot(
            'the head candidate#1 is in its history: against candidate#1: score 1 vs 1 (+0; aggregate: no per-unit results)'
        ),
    ), 'level with the head: a gain over its own base only'
    assert (second, second_reason) == (
        Verdict.KEEP,
        snapshot(
            'the head candidate#1 is in its history: against candidate#1: score 2 vs 1 (+1; aggregate: no per-unit results)'
        ),
    ), 'above the head: kept, the new head'
    with db.session() as s:
        assert s.get_one(Candidate, reference).verdict is Verdict.REFERENCE, 'the starting point is never judged'
        trunk = head(s, s.get_one(Project, project.id))
        assert trunk is not None and trunk.summary == 'second'
        assert not s.scalars(select(Event).where(Event.kind == REBASED)).all(), 'nothing was merged'


def test_a_branch_run_is_judged_against_its_own_base_and_its_keep_never_moves_the_head(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A branch run is judged against its own base, not the head, and its keeps never move the head."""
    reference = import_empty_solution(db, project)
    write_toml(Path(project.path), HARNESS_TWO_COMMITS)
    reload_project(db, project)
    ex = executor(db, runtime, tmp_path)
    ex.remeasure(reference)
    run_id = make_run(db, project)
    with db.session() as s:
        run = s.get_one(Run, run_id)
        run.base_candidate_id, run.branch = reference, True
        s.commit()
    execute(ex, db, run_id)
    [(_, first, first_reason, _, _), (_, second, second_reason, _, _)] = scored(db, run_id)
    assert (first, first_reason) == (
        Verdict.KEEP,
        snapshot(
            'against candidate#2: score 1 vs 0 (+1; aggregate: no per-unit results); a branch keep, not above the head candidate#1 (score 1)'
        ),
    ), 'a gain over its own base keeps on the branch'
    assert (second, second_reason) == (
        Verdict.KEEP,
        snapshot(
            'against candidate#3: score 2 vs 1 (+1; aggregate: no per-unit results); a branch keep, above the head candidate#1 (score 1)'
        ),
    ), 'the bar moved to the branch keep; above the head'
    with db.session() as s:
        proj = s.get_one(Project, project.id)
        trunk = head(s, proj)
        assert trunk is not None and trunk.id == 1, 'a branch keep never moves the head, however it scores'
        assert [(b.root.id, b.branched, b.latest.summary, b.keeps) for b in branches(s, proj)] == [
            (1, False, 'baseline', 1),
            (reference, True, 'second', 2),
        ], "two lineages: the trunk first, then the reference's branch"


def keep_elsewhere(
    db: Database,
    project: Project,
    summary: str,
    files: dict[str, str],
    metrics: dict[str, float],
    *,
    on: int = 1,
    verdict: Verdict = Verdict.KEEP,
) -> int:
    """Add a trunk keep made outside any run.

    The keep writes ``files`` on a commit off candidate ``on`` and is measured as ``metrics``. To a run that started
    before it, it looks like another run's keep: the head moved on. With another ``verdict``, it is a candidate that
    the orchestrator judged.
    """
    repo = Repo(project.path)
    with db.session() as s:
        parent = s.get_one(Candidate, on)
        repo.git.checkout('-q', '--detach', parent.commit_sha)
        for name, text in files.items():
            (Path(project.path) / name).write_text(text)
        repo.git.add('-A')
        repo.git.commit('-qm', summary)
        sha = repo.head.commit.hexsha
        repo.git.checkout('-q', 'main')
        git_pin(repo, f'refs/piloop/demo/elsewhere/{summary.replace(" ", "-")}', sha)
        candidate, _ = resolve(s, project.id, repo, sha, parent=parent, run_id=None)
        candidate.verdict, candidate.verdict_by = verdict, 'orchestrator'
        evaluator_id = s.get_one(Project, project.id).evaluator_id
        assert evaluator_id is not None
        s.add(Measurement(candidate_id=candidate.id, evaluator_id=evaluator_id, metrics=metrics))
        s.commit()
        return candidate.id


def test_a_run_pinned_to_a_non_keep_is_judged_against_its_nearest_keep_on_every_metric(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A run pinned to a candidate that is not a keep is judged against that candidate's nearest keep.

    A candidate that is not a keep is not a bar. The run starts from its tree and is judged as a trunk run against
    the head it grew from, so its keep moves the head.
    """
    lead = keep_elsewhere(
        db, project, 'lead', {'solve.txt': 'base\nx\n'}, {'score': 9.0, 'size': 1.0}, verdict=Verdict.INTERESTING
    )
    run_id = make_run(db, project)
    with db.session() as s:
        s.get_one(Run, run_id).base_candidate_id = lead
        s.commit()
    ex = executor(db, runtime, tmp_path)
    execute(ex, db, run_id)
    [(_, verdict, reason, _, parent)] = scored(db, run_id)
    assert (verdict, reason, parent) == (
        Verdict.KEEP,
        'against candidate#1: score 3 vs 1 (+2; aggregate: no per-unit results)',
        lead,
    ), "the lead's 9 is no bar: the head's 1 is"
    with db.session() as s:
        trunk = head(s, s.get_one(Project, project.id))
        assert trunk is not None and trunk.parent_id == lead, 'a trunk run: its keep is the new head'
        brief = s.get_one(Run, run_id).brief or ''
    assert f'- candidate#{lead} is no keep: your commits are judged against candidate#1, its nearest keep' in brief


def test_a_keep_that_lost_the_race_to_the_head_is_merged_onto_it_and_judged_there_again_and_again(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A keep whose head moved on is merged onto the new head and judged again, as often as the head moves.

    Run X starts from head H1, and H2 is kept meanwhile. X keeps against H1, so it is merged onto H2. While that merge
    is measured, H3 is kept. The merge keeps against H2, so it is merged onto H3 in turn.
    """
    run_id = make_run(db, project)
    h2 = keep_elsewhere(db, project, 'notes', {'notes.txt': 'n\n'}, {'score': 1.0, 'size': 5.0})
    ex = executor(db, runtime, tmp_path)
    ended: list[None] = []
    track = ex._track_evaluation

    def h3_kept_while_the_merge_is_measured(proc: subprocess.Popen[bytes] | None) -> None:
        track(proc)
        if proc is None:
            ended.append(None)
            if len(ended) == 2:
                keep_elsewhere(db, project, 'docs', {'docs.txt': 'd\n'}, {'score': 1.0, 'size': 5.0}, on=h2)

    monkeypatch.setattr(ex, '_track_evaluation', h3_kept_while_the_merge_is_measured)
    execute(ex, db, run_id)
    repo = Repo(project.path)
    with db.session() as s:
        rows = s.scalars(select(Candidate).order_by(Candidate.id)).all()
        assert [(c.id, c.summary, c.verdict, c.parent_id, c.run_id) for c in rows] == snapshot(
            [
                (1, 'baseline', Verdict.KEEP, None, None),
                (2, 'notes', Verdict.KEEP, 1, None),
                (3, 'add a line', Verdict.INTERESTING, 1, 1),
                (4, 'add a line (rebased onto candidate#2)', Verdict.INTERESTING, 2, 1),
                (5, 'docs', Verdict.KEEP, 2, None),
                (6, 'add a line (rebased onto candidate#5)', Verdict.KEEP, 5, 1),
            ]
        )
        x, merged, h3, final = rows[2], rows[3], rows[4], rows[5]
        assert [x.verdict_reason, merged.verdict_reason, final.verdict_reason] == snapshot(
            [
                'keeps against its own bar, but the head moved on to candidate#2: against candidate#1: score 2 vs 1 (+1; aggregate: no per-unit results); rebased onto the head as candidate#4',
                'keeps against its own bar, but the head moved on to candidate#5: against candidate#2: score 2 vs 1 (+1; aggregate: no per-unit results); rebased onto the head as candidate#6',
                'against candidate#5: score 2 vs 1 (+1; aggregate: no per-unit results)',
            ]
        )
        trunk = head(s, s.get_one(Project, project.id))
        assert trunk is not None and trunk.id == final.id, 'the last merge is the head'
        rebased = s.scalars(select(Event).where(Event.kind == REBASED).order_by(Event.id)).all()
        assert [(e.target_id, e.payload) for e in rebased] == [
            (x.id, {'head': h2, 'rebased': merged.id}),
            (merged.id, {'head': h3.id, 'rebased': final.id}),
        ]
        assert [p.hexsha for p in repo.commit(final.commit_sha).parents] == [h3.commit_sha, merged.commit_sha]
        assert sorted((Path(project.path) / '.git' / 'refs' / 'piloop' / 'demo' / 'rebase').iterdir()) == [
            Path(project.path) / '.git' / 'refs' / 'piloop' / 'demo' / 'rebase' / str(c)
            for c in sorted((x.id, merged.id))
        ], 'every merge commit is pinned'
        tree = set(repo.git.ls_tree('--name-only', final.commit_sha).splitlines())
        assert {'notes.txt', 'docs.txt', 'solve.txt'} <= tree, "the head's changes and the run's, together"
        assert s.get_one(Idea, run_row(db, run_id).idea_id).status is IdeaStatus.DONE


def test_a_keep_whose_merge_onto_the_head_conflicts_stays_interesting_and_files_a_seed_to_compose_it(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    run_id = make_run(db, project)
    h2 = keep_elsewhere(db, project, 'other line', {'solve.txt': 'base\nother\n'}, {'score': 2.0, 'size': 11.0})
    execute(executor(db, runtime, tmp_path), db, run_id)
    [(_, verdict, reason, _, _)] = scored(db, run_id)
    assert (verdict, reason) == (
        Verdict.INTERESTING,
        snapshot(
            'keeps against its own bar, but the head moved on to candidate#2: against candidate#1: score 2 vs 1 (+1; aggregate: no per-unit results); its merge onto the head conflicts in solve.txt: seed idea#2 composes it by hand'
        ),
    )
    with db.session() as s:
        [x] = s.scalars(select(Candidate).where(Candidate.run_id == run_id)).all()
        [seed] = s.scalars(select(Idea).where(Idea.status == IdeaStatus.SEED)).all()
        assert (seed.title, seed.track, seed.base_candidate_id, seed.created_by, seed.provenance) == (
            f'Compose candidate#{x.id} onto the head',
            'score',
            None,
            DAEMON,
            f'candidate#{x.id}',
        ), 'unpinned, so it runs from the head, on the run track'
        assert seed.hypothesis == (
            f'Follow-up of candidate#{x.id} (add a line): it kept against its own bar but its merge onto the head'
            f' candidate#{h2} conflicts in solve.txt: from the head, git merge {x.commit_sha} and resolve the conflicts'
        )
        [event] = s.scalars(select(Event).where(Event.kind == REBASED)).all()
        assert (event.target_id, event.payload) == (x.id, {'head': h2, 'conflicts': ['solve.txt']})
        trunk = head(s, s.get_one(Project, project.id))
        assert trunk is not None and trunk.id == h2


def test_a_keep_whose_merge_onto_the_head_is_the_head_already_changes_nothing(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    run_id = make_run(db, project)
    files = {'solve.txt': 'base\nline\n', 'notes.txt': 'n\n'}
    h2 = keep_elsewhere(db, project, 'line and notes', files, {'score': 2.0, 'size': 10.0})
    execute(executor(db, runtime, tmp_path), db, run_id)
    [(_, verdict, reason, _, _)] = scored(db, run_id)
    assert (verdict, reason) == (
        Verdict.INTERESTING,
        snapshot(
            'keeps against its own bar, but the head moved on to candidate#2: against candidate#1: score 2 vs 1 (+1; aggregate: no per-unit results); its merge onto the head is candidate#2 already (keep)'
        ),
    )
    with db.session() as s:
        [event] = s.scalars(select(Event).where(Event.kind == REBASED)).all()
        assert event.payload == {'head': h2, 'known': h2}


def test_a_merge_a_stop_left_unjudged_is_measured_and_judged_when_the_run_resumes(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_id = make_run(db, project)
    keep_elsewhere(db, project, 'notes', {'notes.txt': 'n\n'}, {'score': 1.0, 'size': 5.0})
    ex = executor(db, runtime, tmp_path)
    measure = ex._measure

    merged_id = 4

    def stopped_at_the_merge(
        run: int | None,
        project_id: int,
        cfg: ProjectConfig,
        repo: Repo,
        sha: str,
        candidate_id: int,
        evaluator_id: int,
        eval_dir: Path,
    ) -> bool:
        if candidate_id == merged_id:
            return False  # what a stop does to an evaluation in flight
        return measure(run, project_id, cfg, repo, sha, candidate_id, evaluator_id, eval_dir)

    monkeypatch.setattr(ex, '_measure', stopped_at_the_merge)
    execute(ex, db, run_id)
    with db.session() as s:
        merged = s.get_one(Candidate, merged_id)
        assert (merged.verdict, merged.measurements, merged.parent_id) == (Verdict.PENDING, [], 2)
    ex.resume(run_id)  # stopped again on the merge
    with db.session() as s:
        assert s.get_one(Candidate, merged_id).verdict is Verdict.PENDING
    monkeypatch.setattr(ex, '_measure', measure)
    ex.resume(run_id)
    with db.session() as s:
        merged = s.get_one(Candidate, merged_id)
        assert (merged.verdict, len(merged.measurements)) == (Verdict.KEEP, 1)


def test_a_tree_two_runs_reach_is_never_judged_against_itself(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A tree that two runs reach is judged against the best candidate other than itself.

    The second run reaches the tree while the first run's evaluation is pending. The first keep then makes the tree the
    head, but the second run must not judge it level with itself.
    """
    ex = executor(db, runtime, tmp_path)
    first = make_run(db, project)
    execute(ex, db, first)
    [kept] = candidates(db, first)
    assert kept.verdict is Verdict.KEEP
    with db.session() as s:  # rewind to before the first run's measurement landed
        candidate = s.get_one(Candidate, kept.id)
        [measurement] = candidate.measurements
        evaluator_id = measurement.evaluator_id
        s.delete(measurement)
        candidate.verdict = Verdict.PENDING
        s.commit()
    track = ex._track_evaluation

    def kept_meanwhile(proc: subprocess.Popen[bytes] | None) -> None:
        """Land the first run's measurement and keep while the second run evaluates the same tree."""
        track(proc)
        if proc is None:
            with db.session() as s:
                s.add(Measurement(candidate_id=kept.id, evaluator_id=evaluator_id, metrics=kept.metrics))
                s.get_one(Candidate, kept.id).verdict = Verdict.KEEP
                s.commit()

    monkeypatch.setattr(ex, '_track_evaluation', kept_meanwhile)
    execute(ex, db, make_run(db, project, title='same lines'))
    with db.session() as s:
        again = s.get_one(Candidate, kept.id)
        assert (len(again.measurements), again.verdict, again.verdict_reason) == (
            2,
            Verdict.KEEP,
            snapshot('against candidate#1: score 2 vs 1 (+1; aggregate: no per-unit results)'),
        ), 'judged against the bar it beat, the best but for itself'


HARNESS_NOTED = (
    '#!/bin/sh\n: > solve.txt\n'
    'git commit -qam wipe -m "Piloop-Note: faster, though it scores less\nPiloop-Follow-Up: wipe, then refill"\n'
    'echo a >> solve.txt\necho b >> solve.txt\n'
    'git commit -qam refill -m "Piloop-Note: redundant"\n'
)


def test_a_piloop_note_keeps_a_regressed_commit_in_view_files_no_follow_up_and_moves_no_bar(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    write_toml(Path(project.path), HARNESS_NOTED)
    reload_project(db, project)
    run_id = make_run(db, project)
    execute(executor(db, runtime, tmp_path), db, run_id)
    [(_, wipe, wipe_reason, _, _), (_, refill, refill_reason, _, _)] = scored(db, run_id)
    assert (wipe, wipe_reason) == (
        Verdict.INTERESTING,
        snapshot(
            'note: faster, though it scores less; against candidate#1: score 0 vs 1 (-1; aggregate: no per-unit results)'
        ),
    )
    assert (refill, refill_reason) == (
        Verdict.KEEP,
        snapshot('against candidate#1: score 2 vs 1 (+1; aggregate: no per-unit results)'),
    ), 'judged against the baseline: the note moved no bar, and a keep keeps its verdict'
    with db.session() as s:
        noted = s.scalars(select(Candidate).where(Candidate.run_id == run_id).order_by(Candidate.id)).first()
        assert noted is not None and noted.verdict_by == f'run#{run_id}', 'the daemon never judges a noted commit again'
        follow_ups = s.scalars(select(Idea).where(Idea.base_candidate_id.is_not(None))).all()
        assert follow_ups == [], 'a Piloop-Follow-Up trailer files no seed: a worker files none'


def test_a_keep_on_one_track_must_hold_the_other_track_primaries_and_its_brief_carries_the_track(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A keep on one track must stay within the floor of the other track's primary metric.

    An accuracy candidate that adds one line stays within the cost track's floor on size.
    """
    path = Path(project.path)
    (path / 'briefs').mkdir()
    (path / 'briefs' / 'accuracy.md').write_text('# Accuracy track\nOnly add lines that are right.\n')
    tracks = {
        'metrics': [
            {'name': 'score', 'direction': 'higher', 'role': 'primary', 'track': 'accuracy', 'margin': 1},
            {'name': 'size', 'direction': 'lower', 'role': 'primary', 'track': 'cost', 'margin': 2, 'floor': 8},
        ],
        'tracks': {'accuracy': {'share': 0.5, 'addendum': 'briefs/accuracy.md'}, 'cost': {'share': 0.5}},
    }
    with db.session() as s:
        s.get_one(Project, project.id).config = load_project_config(path, **tracks).model_dump(mode='json')
        s.commit()
    run_id = make_run(db, project, track='accuracy')
    execute(executor(db, runtime, tmp_path), db, run_id)
    [(_, verdict, reason, _, _)] = scored(db, run_id)
    assert (verdict, reason) == (Verdict.DISCARD, 'floor: size 10 past its floor 8'), 'score 2 > 1, but size 10 > 8'
    brief = run_row(db, run_id).brief or ''
    assert '# Accuracy track\nOnly add lines that are right.' in brief, "the track's addendum"
    assert (
        '- Your track is accuracy: a keep must improve score (higher is better) with confidence and hold size'
        ' (margin 2, floor 8).'
    ) in brief
    assert '- Starting from candidate#1: score=1.0, size=5.0 — baseline' in brief
    assert '- Tracks: accuracy (primary score, higher is better; share 0.5); cost (primary size' in brief
    assert 'Your run is on track accuracy.' in brief


def test_recovered_work_whose_tree_is_already_a_candidate_still_requeues_its_idea(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """The recovery commit is that candidate, which keeps its verdict; the idea still starts again from it."""
    write_toml(Path(project.path), HARNESS_APPEND)
    reload_project(db, project)
    first = make_run(db, project, title='commits the line')
    execute(executor(db, runtime, tmp_path), db, first)
    [known] = candidates(db, first)
    write_toml(Path(project.path), HARNESS_FORGETS_COMMIT)
    reload_project(db, project)
    run_id = make_run(db, project, title='leaves the same line', max_runs=1)
    execute(executor(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        idea = s.get_one(Idea, run_row(db, run_id).idea_id)
        assert (idea.status, idea.base_candidate_id) == (IdeaStatus.READY, known.id)
        assert s.get_one(Candidate, known.id).verdict is Verdict.KEEP, 'a known tree keeps its verdict'
