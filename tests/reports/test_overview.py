"""Tests for the overview that ``piloop status`` prints and that each sweep reads in its brief."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from git import Repo
from inline_snapshot import snapshot
from sqlalchemy.orm import Session

from piloop.agents.launcher import STARTUP_ANOMALY, STOPPED_ERROR
from piloop.config import ProjectConfig, load_project_config
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
)
from piloop.db.session import Database
from piloop.reports import overview as overview_module
from piloop.reports.overview import AnomalyKind, anomalies, overview
from tests.runtime.test_herdr import Response, calls, scripted

T = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
NOW = T + timedelta(hours=2)


def no_jobs(worker: Worker) -> list[str]:
    return []


def at(minutes: float) -> datetime:
    return T + timedelta(minutes=minutes)


def idea(
    s: Session, project: Project, title: str, status: IdeaStatus, *, after: tuple[int, ...] = (), **kw: object
) -> Idea:
    row = Idea(
        track='score',
        project_id=project.id,
        title=title,
        hypothesis=title,
        status=status,
        created_by='research#4',
        links=[IdeaLink(target_id=t, kind=LinkKind.AFTER) for t in after],
        **kw,
    )
    s.add(row)
    s.flush()
    return row


def run(s: Session, i: Idea, status: RunStatus, base: int, start: float, end: float | None, **kw: object) -> Run:
    row = Run(
        idea_id=i.id,
        attempt=1,
        base_candidate_id=base,
        harness='script',
        model='m',
        status=status,
        created_at=at(start),
        started_at=at(start),
        finished_at=at(end) if end is not None else None,
        **kw,
    )
    s.add(row)
    s.flush()
    return row


def agent(
    s: Session,
    project: Project,
    kind: WorkerKind,
    start: float,
    lived: float,
    *,
    run_id: int | None = None,
    **kw: object,
) -> Worker:
    row = Worker(
        kind=kind,
        project_id=project.id,
        run_id=run_id,
        harness='script',
        runtime='subprocess',
        started_at=at(start),
        ended_at=at(start) + timedelta(seconds=lived),
        **{'state': WorkerState.EXITED, **kw},
    )
    s.add(row)
    s.flush()
    return row


def candidate(
    s: Session, project: Project, parent: int, verdict: Verdict, value: float, when: float, **kw: object
) -> Candidate:
    row = Candidate(
        project_id=project.id,
        parent_id=parent,
        tree_sha=f'tree{when}',
        commit_sha=f'{when:07.0f}abcdef',
        summary=f'score {value:g}',
        verdict=verdict,
        **kw,
    )
    s.add(row)
    s.flush()
    s.add(
        Measurement(
            candidate_id=row.id,
            evaluator_id=project.evaluator_id,
            metrics={'score': value, 'size': 10.0},
            seconds=60 * value,
            created_at=at(when),
        )
    )
    return row


def campaign(db: Database, project: Project) -> None:
    """Add two hours of a small campaign that has one example of each anomaly."""
    with db.session() as s:
        p = s.get_one(Project, project.id)
        p.created_at = T
        baseline = s.get_one(Candidate, 1)
        baseline.measurements[0].created_at = T
        good = idea(s, p, 'longer lines', IdeaStatus.DONE)
        first = run(s, good, RunStatus.SUCCEEDED, 1, 5, 20)
        agent(s, p, WorkerKind.WORKER, 5, 600, run_id=first.id, usage={'total': 1000})
        kept = candidate(s, p, 1, Verdict.KEEP, 3, 18, run_id=first.id)
        candidate(s, p, kept.id, Verdict.DISCARD, 3, 19, run_id=first.id)
        bad = idea(s, p, 'crash it', IdeaStatus.REJECTED)
        failed = run(s, bad, RunStatus.FAILED, 1, 30, 40, error='boom\nand the rest')
        dead = agent(s, p, WorkerKind.WORKER, 30, 5, run_id=failed.id, state=WorkerState.KILLED, error='dialog')
        startup = {'kind': 'worker', 'harness': 'script', 'run': failed.id, 'seconds': 5.0, 'error': 'dialog'}
        s.add(
            Event(
                kind=STARTUP_ANOMALY,
                actor='daemon',
                target_kind=TargetKind.WORKER,
                target_id=dead.id,
                payload=startup,
                at=at(30) + timedelta(seconds=5),
            )
        )
        pinned = idea(s, p, 'build on longer lines', IdeaStatus.SCHEDULED, after=(good.id,), base_candidate_id=1)
        stuck = run(s, pinned, RunStatus.RUNNING, 1, 50, None)
        agent(s, p, WorkerKind.WORKER, 50, 300, run_id=stuck.id, usage={'total': 2000})
        idea(s, p, 'after the crash', IdeaStatus.READY, after=(bad.id,))
        agent(s, p, WorkerKind.RESEARCH, 60, 120, usage={'total': 500})
        s.add_all(
            [
                Comment(
                    target_kind=TargetKind.RUN,
                    target_id=failed.id,
                    author='watchdog#9',
                    kind=CommentKind.ESCALATION,
                    priority=1,
                    body='run#2 died in a dialog\nevidence below',
                    created_at=at(45),
                ),
                Comment(
                    target_kind=TargetKind.PROJECT,
                    target_id=p.id,
                    author='pi',
                    kind=CommentKind.DECISION,
                    body='keep going',
                    created_at=at(46),
                ),
                Comment(
                    target_kind=TargetKind.IDEA, target_id=good.id, author='worker#1', body='a note', created_at=at(47)
                ),
            ]
        )
        s.commit()


def render(db: Database, project: Project, cfg: ProjectConfig | None = None, now: datetime = NOW) -> str:
    with db.session() as s:
        p = s.get_one(Project, project.id)
        return overview(s, p, cfg or ProjectConfig.model_validate(p.config), now, no_jobs)


def test_the_overview_of_a_campaign(db: Database, project: Project) -> None:
    campaign(db, project)
    text = render(db, project)
    lay, tools = text.split('### Tools\n')
    assert lay == snapshot("""\
## Lay of the land (computed at 2026-10-01 14:00 UTC)
demo · head score 3.0000 (candidate#2, +2.0000 vs baseline) · 2 kept / 3 candidates · 4k tok · evaluator #1, 0s old
### Goal
- Primary metric: score, higher is better; also measured: size (lower, watch)
- Campaign: active. Progress: run 2 · 1.9h (no [stop] condition: open-ended).
- The goal in words is the project's brief.md: weigh the metric against it.
### Trajectory
- Start candidate#1, head candidate#2: score 1 → 3 (+2, +200.0%); 1 trunk keeps.
- Last trunk keep 1h ago; 2 runs finished since.
- Trunk keeps (latest 8; when first measured, the gain on its track over the head before it):
  - 10-01 12:00 candidate#1 1 (start): baseline
  - 10-01 12:18 candidate#2 3 (+2) idea#1: score 3
### Now
- Worker slots: 1 of 1 busy.
  - run#3 idea#3 "build on longer lines" (script): running 1h, worker#3 ended 1h ago
- Ideas: 0 runnable, 1 blocked (idea#4 on idea#2 (rejected)); 0 seed, 0 proposed, 1 scheduled, 1 done, 0 parked, 1 rejected.
- Agents live: none
- Agents so far: 3 worker (1 killed), 0 watchdog, 1 research; 0 of 1 ended research sweeps defined no idea.
- Ideas by source: research 4 ideas, 3 ran → 1 keep, 0 interesting.
- Tokens: 4k (worker 3k, watchdog 0, research 500); 500 by agents that ended in the last hour.
- Evaluations: 3 (0 crashed); median 3.0 min, first 5 3.0 min, latest 5 3.0 min; 6.0 min in all; 0 candidates waiting for a verdict.
### Recent candidates (3 of 3; score, vs parent)
- candidate#1 keep 1.0000 (—), no run, 0s ago: baseline
- candidate#2 keep 3.0000 (+2.0000), run#1, 0s ago: score 3
- candidate#3 discard 3.0000 (+0.0000), run#1, 0s ago: score 3
### Anomalies the daemon detected
- [incident] run#2 failed, 1h ago: failed on script (idea#2): boom
- [incident] worker#2 dead at startup, 1h ago: worker on script failed to start (5.0s): dialog
- candidate#3 scored exactly its parent, 1h ago: scored what candidate#2 did on score and on 1 of 1 other metrics; 3.0 min of evaluation
### Inbox (1 unacknowledged), decisions, escalations and watchdog findings
- comment#2 decision on project#1 by pi, 1h ago: keep going
- comment#1 escalation (p1, unacknowledged) on run#2 by watchdog#9, 1h ago: run#2 died in a dialog
### Recent events
- comment.note comment#3 0s ago · comment.decision comment#2 0s ago · comment.escalation comment#1 0s ago · anomaly.startup worker#2 1h ago
""")
    assert tools.startswith('- `piloop sql "<one statement>"`')
    assert '  - workers(id, kind, project_id, run_id,' in tools, (
        'the schema is listed, so an agent does not need PRAGMA table_info'
    )


def test_stop_conditions_say_how_far_each_one_is(db: Database, project: Project) -> None:
    campaign(db, project)
    stop = {'target': 5, 'max_runs': 10, 'max_hours': 4, 'max_tokens': 1_000_000, 'plateau': 3}
    cfg = load_project_config(Path(project.path), stop=stop)
    [line] = [x for x in render(db, project, cfg).splitlines() if x.startswith('- Campaign:')]
    assert line == snapshot(
        '- Campaign: active. Progress: run 2/10 · 1.9h/4h · 4k/1.0M tok · 0/3 runs without a keep · target ≥ 5 (head 3).'
    )
    with db.session() as s:
        s.get_one(Project, project.id).status = ProjectStatus.STOPPED
        s.get_one(Project, project.id).stopped_reason = 'plateau: 3 runs without a keep'
        s.commit()
    assert 'Campaign: stopped — plateau: 3 runs without a keep.' in render(db, project, cfg)


def test_a_fresh_project_has_little_to_say(db: Database, project: Project) -> None:
    """A project with no run, no comment and no trunk keep shows no trajectory and reports an idle queue."""
    stop = {'max_hours': 4, 'plateau': 2}
    cfg = load_project_config(Path(project.path), stop=stop)
    with db.session() as s:
        s.get_one(Project, project.id).created_at = T
        s.get_one(Candidate, 1).verdict = Verdict.PENDING
        s.commit()
    text = render(db, project, cfg)
    assert '- Campaign: active. Progress: run 0 · 0.0h/4h · 0/2 runs without a keep.' in text
    assert "- No trunk keep: the baseline's verdict was overridden and nothing was kept since." in text
    assert '- Evaluations: 1 (0 crashed); median' not in text, 'the baseline has no recorded seconds'
    assert '- Evaluations: 1 (0 crashed); 1 candidates waiting for a verdict.' in text
    assert '- project#1 idle, 2h ago: 1 free worker slots and no runnable idea; no research sweep pending' in text, (
        'not tagged: an idle queue is a cost for the progress review, not an incident'
    )
    assert '### Inbox (0 unacknowledged), decisions, escalations and watchdog findings\n- None.\n' in text


def test_no_anomaly_reads_none(db: Database, project: Project) -> None:
    with db.session() as s:
        s.add(Idea(track='score', project_id=project.id, title='t', hypothesis='h', created_by='human'))
        s.commit()
    assert '### Anomalies the daemon detected\n- None.\n' in render(db, project)


def test_the_idle_queue_counts_from_the_last_run_and_names_pending_research(db: Database, project: Project) -> None:
    campaign(db, project)
    with db.session() as s:
        s.get_one(Run, 3).status = RunStatus.SUCCEEDED
        s.get_one(Run, 3).finished_at = at(70)
        s.add(Worker(kind=WorkerKind.RESEARCH, project_id=project.id, started_at=at(100)))
        s.commit()
        [idle] = [a for a in found(s, project) if a.kind is AnomalyKind.IDLE_QUEUE]
    assert (idle.at, idle.detail) == (at(70), '1 free worker slots and no runnable idea; research#5 requested')


def test_max_runs_reached_is_not_idle(db: Database, project: Project) -> None:
    """Two runs count toward ``max_runs``. The run that failed with no candidate does not count."""
    campaign(db, project)
    cfg = load_project_config(Path(project.path), stop={'max_runs': 2}, roles={'worker': {'max_concurrent': 4}})
    with db.session() as s:
        p = s.get_one(Project, project.id)
        assert AnomalyKind.IDLE_QUEUE not in {a.kind for a in anomalies(s, p, cfg, NOW, no_jobs)}
        p.status = ProjectStatus.STOPPED
        uncapped = load_project_config(Path(project.path), roles={'worker': {'max_concurrent': 4}})
        assert AnomalyKind.IDLE_QUEUE not in {a.kind for a in anomalies(s, p, uncapped, NOW, no_jobs)}, (
            'a stopped project'
        )


def test_a_run_that_the_daemon_stopped_is_not_a_failed_run(db: Database, project: Project) -> None:
    campaign(db, project)
    with db.session() as s:
        run = s.get_one(Run, 3)
        run.status, run.error, run.finished_at = RunStatus.FAILED, STOPPED_ERROR, at(70)
        s.commit()
        assert f'run#{run.id}' not in {a.ref for a in found(s, project) if a.kind is AnomalyKind.RUN_FAILED}


def found(s: Session, project: Project, now: datetime = NOW) -> list[overview_module.Anomaly]:
    p = s.get_one(Project, project.id)
    return anomalies(s, p, ProjectConfig.model_validate(p.config), now, no_jobs)


def test_a_short_life_without_the_launcher_event_is_not_a_startup_death(db: Database, project: Project) -> None:
    """The launcher decides that an agent died at startup (``STARTUP_ANOMALY``). The overview only reports it."""
    with db.session() as s:
        agent(s, s.get_one(Project, project.id), WorkerKind.WATCHDOG, 0, 5)
        s.commit()
        assert [a.key for a in found(s, project) if a.kind is AnomalyKind.DEAD_AT_STARTUP] == []


def test_a_watchdog_dead_at_startup_requests_no_watchdog(db: Database, project: Project) -> None:
    """A watchdog that died at startup is reported, but it is not an incident and does not request a watchdog.

    Otherwise each watchdog that dies at startup requests a new watchdog, which can die the same way.
    """
    with db.session() as s:
        dead = agent(s, s.get_one(Project, project.id), WorkerKind.WATCHDOG, 0, 35, state=WorkerState.KILLED)
        payload = {'kind': 'watchdog', 'harness': 'script', 'run': None, 'seconds': 35.0, 'error': 'prompt_lost'}
        s.add(
            Event(
                kind=STARTUP_ANOMALY, actor='daemon', target_kind=TargetKind.WORKER, target_id=dead.id, payload=payload
            )
        )
        s.commit()
        [anomaly] = [a for a in found(s, project) if a.ref == f'worker#{dead.id}']
        assert (anomaly.kind, anomaly.incident) == (AnomalyKind.WATCHDOG_DEAD_AT_STARTUP, False)
        assert anomaly.key == f'worker#{dead.id} dead at startup, a watchdog'


def test_a_crashed_measurement_or_an_unmeasured_parent_is_not_same_as_parent(db: Database, project: Project) -> None:
    with db.session() as s:
        p = s.get_one(Project, project.id)
        orphan = Candidate(project_id=p.id, parent_id=1, tree_sha='x', commit_sha='x', verdict=Verdict.CRASH)
        s.add(orphan)
        s.flush()
        s.add(Measurement(candidate_id=orphan.id, evaluator_id=p.evaluator_id, metrics={}, error='died'))
        s.commit()
        assert found(s, project) == [a for a in found(s, project) if a.kind is not AnomalyKind.SAME_AS_PARENT]


def test_the_anomaly_list_shows_the_latest_and_counts_the_rest(db: Database, project: Project) -> None:
    with db.session() as s:
        p = s.get_one(Project, project.id)
        for n in range(overview_module.ANOMALIES_SHOWN + 2):
            run(s, idea(s, p, f'idea {n}', IdeaStatus.DONE), RunStatus.TIMED_OUT, 1, n, n + 1)
        idea(s, p, 'next', IdeaStatus.READY)
        s.commit()
    text = render(db, project)
    assert 'run#14 failed' in text and 'run#3 failed' in text and 'run#2 failed' not in text
    assert '- … and 2 older incidents.\n' in text
    assert 'timed_out on script (idea#14): no error recorded' in text


def test_costs_of_one_kind_share_a_line(db: Database, project: Project) -> None:
    campaign(db, project)
    with db.session() as s:
        p = s.get_one(Project, project.id)
        for n in range(overview_module.REFS_SHOWN):
            candidate(s, p, 2, Verdict.DISCARD, 3, 30 + n, run_id=1)
        s.commit()
    [line] = [x for x in render(db, project).splitlines() if 'scored exactly its parent' in x]
    assert line == snapshot(
        '- 9 × scored exactly its parent (candidate#11, candidate#10, candidate#9, candidate#8, candidate#7, candidate#6, candidate#5, candidate#4, …); the latest, 1h ago: scored what candidate#2 did on score and on 1 of 1 other metrics; 3.0 min of evaluation'
    )


def test_blocked_ideas_beyond_the_shown_few_are_counted(db: Database, project: Project) -> None:
    with db.session() as s:
        p = s.get_one(Project, project.id)
        gate = idea(s, p, 'gate', IdeaStatus.PARKED)
        for n in range(overview_module.BLOCKED_SHOWN + 1):
            idea(s, p, f'waits {n}', IdeaStatus.READY, after=(gate.id,))
        s.commit()
    assert ', 1 more); 0 seed, 0 proposed' in render(db, project)


def test_open_runs_say_how_far_they_got(db: Database, project: Project) -> None:
    with db.session() as s:
        p = s.get_one(Project, project.id)
        waiting = run(s, idea(s, p, 'waiting', IdeaStatus.SCHEDULED), RunStatus.QUEUED, 1, 100, None)
        waiting.started_at = None
        going = run(s, idea(s, p, 'going', IdeaStatus.SCHEDULED), RunStatus.RUNNING, 1, 110, None)
        s.add(Worker(kind=WorkerKind.WORKER, project_id=p.id, run_id=going.id, state=WorkerState.RUNNING))
        s.commit()
    cfg = load_project_config(Path(project.path), roles={'worker': {'max_concurrent': 2}})
    lines = render(db, project, cfg).splitlines()
    start = lines.index('- Worker slots: 2 of 2 busy.')
    assert lines[start + 1 : start + 3] == [
        '  - run#1 idea#1 "waiting" (script): queued',
        '  - run#2 idea#2 "going" (script): running 10m',
    ]


def busy(s: Session, project: Project, log_dir: Path | None, *, said: float | None = None) -> Worker:
    """A worker running since minute 0 on run#1, which comments at minute ``said``."""
    p = s.get_one(Project, project.id)
    r = run(s, idea(s, p, 'train', IdeaStatus.SCHEDULED), RunStatus.RUNNING, 1, 0, None)
    r.log_dir = str(log_dir) if log_dir is not None else None
    w = Worker(kind=WorkerKind.WORKER, project_id=p.id, run_id=r.id, state=WorkerState.RUNNING, started_at=at(0))
    s.add(w)
    s.flush()
    if said is not None:
        s.add(
            Comment(
                target_kind=TargetKind.RUN, target_id=r.id, author=f'worker#{w.id}', body='ETA', created_at=at(said)
            )
        )
    s.commit()
    return w


class Jobs:
    """A ``JobsProbe`` that answers ``running`` and remembers whom it was asked about."""

    def __init__(self, *running: str) -> None:
        self.running = list(running)
        self.asked: list[int] = []

    def __call__(self, worker: Worker) -> list[str]:
        self.asked.append(worker.id)
        return self.running


def silent(s: Session, project: Project, jobs: Jobs, now: datetime = NOW) -> list[overview_module.Anomaly]:
    p = s.get_one(Project, project.id)
    found = anomalies(s, p, ProjectConfig.model_validate(p.config), now, jobs)
    return [a for a in found if a.kind is AnomalyKind.SILENT]


def test_a_worker_quiet_for_an_hour_with_jobs_in_flight_is_an_incident(
    db: Database, project: Project, tmp_path: Path
) -> None:
    jobs = Jobs('4242 python train.py --epochs 90', '4243 tee train.log')
    with db.session() as s:
        busy(s, project, tmp_path / 'run', said=30)
        [quiet] = silent(s, project, jobs)
    assert (quiet.key, quiet.at, quiet.incident) == ('worker#1 silent while its jobs run', at(90), True)
    assert quiet.detail == snapshot(
        'run#1 idea#1: no comment or commit for 1h (its last comment); 2 jobs in flight, e.g. 4242 python train.py --epochs 90'
    )
    with db.session() as s:
        text = overview(s, s.get_one(Project, project.id), ProjectConfig.model_validate(project.config), NOW, jobs)
    assert '- [incident] worker#1 silent while its jobs run, 30m ago: run#1 idea#1' in text


def test_a_worker_that_spoke_or_committed_within_the_hour_is_not_asked_about_its_jobs(
    db: Database, project: Project, tmp_path: Path
) -> None:
    jobs = Jobs('4242 python train.py')
    with db.session() as s:
        busy(s, project, None, said=61)
        assert silent(s, project, jobs) == [] and jobs.asked == [], 'it commented 59 minutes ago'
        assert silent(s, project, jobs, at(60)) == [], 'its run started an hour ago, to the second'
    worktree = tmp_path / 'run' / 'worktree'
    repo = Repo.init(worktree)
    repo.index.commit(
        'epoch 40 checkpoint',
        commit_date=at(100).strftime('%Y-%m-%dT%H:%M:%S%z'),
        author_date=at(100).strftime('%Y-%m-%dT%H:%M:%S%z'),
    )
    with db.session() as s:
        s.get_one(Run, 1).log_dir = str(tmp_path / 'run')
        s.commit()
        assert silent(s, project, jobs, at(150)) == [] and jobs.asked == [], 'it committed 50 minutes ago'
        assert [a.detail.split('; ')[0] for a in silent(s, project, jobs, at(170))] == [
            'run#1 idea#1: no comment or commit for 1h (its last commit)'
        ]


def test_a_quiet_worker_with_nothing_running_is_not_silent(db: Database, project: Project) -> None:
    """A silent worker with no job in flight is not an anomaly. The anomaly is about jobs that nobody watches."""
    jobs = Jobs()
    with db.session() as s:
        worker = busy(s, project, None)
        assert silent(s, project, jobs) == [] and jobs.asked == [worker.id]


def test_branches_and_notes_say_where_else_a_run_may_start(db: Database, project: Project) -> None:
    """A branch keep starts its own lineage, and a note shows a candidate that regressed.

    A keep that the current evaluator has not measured yet is in no lineage.
    """
    campaign(db, project)
    with db.session() as s:
        p = s.get_one(Project, project.id)
        explore = idea(s, p, 'from the baseline', IdeaStatus.DONE, base_candidate_id=1)
        branch = run(s, explore, RunStatus.SUCCEEDED, 1, 60, 70, branch=True)
        candidate(s, p, 1, Verdict.KEEP, 2, 65, run_id=branch.id)
        noted = candidate(s, p, 1, Verdict.INTERESTING, 0.5, 66, run_id=branch.id, verdict_by=f'run#{branch.id}')
        noted.verdict_reason = 'note: halves the eval time; against candidate#1: score 0.5 vs 1'
        s.add(Candidate(project_id=p.id, tree_sha='unmeasured', commit_sha='u', verdict=Verdict.KEEP, run_id=branch.id))
        s.commit()
    trajectory = render(db, project).split('### Trajectory\n')[1].split('### Now')[0]
    assert trajectory == snapshot("""\
- Start candidate#1, head candidate#2: score 1 → 3 (+2, +200.0%); 1 trunk keeps.
- Last trunk keep 1h ago; 3 runs finished since.
- Trunk keeps (latest 8; when first measured, the gain on its track over the head before it):
  - 10-01 12:00 candidate#1 1 (start): baseline
  - 10-01 12:18 candidate#2 3 (+2) idea#1: score 3
- Lineages (latest keep of each; a branch keep never moves the head):
  - trunk from candidate#1 (keep): latest candidate#2 3, 2 kept
  - branch from candidate#1 (keep): latest candidate#6 —, 2 kept
- Notes (regressed, kept in view as leads; pin one with base_candidate_id):
  - candidate#5 0.5 by run#4: halves the eval time; against candidate#1: score 0.5 vs 1
""")


def test_the_queue_the_live_agents_the_inbox_and_the_events(db: Database, project: Project, tmp_path: Path) -> None:
    """The Now, Inbox and Recent events sections show every row with its age."""
    campaign(db, project)
    log_dir = tmp_path / 'research-6'
    log_dir.mkdir()
    (log_dir / 'transcript.txt').write_text('thinking\n\nran evaluate.py: METRIC score=3\n\n')
    with db.session() as s:
        p = s.get_one(Project, project.id)
        s.get_one(Idea, 2).status = IdeaStatus.DONE  # unblocks idea#4
        idea(s, p, 'a' * 120, IdeaStatus.READY, priority=0, created_at=at(100))
        newer = idea(s, p, 'even longer lines', IdeaStatus.READY, priority=1, created_at=at(110))
        s.add(IdeaLink(idea_id=newer.id, target_id=4, kind=LinkKind.SUPERSEDES))
        s.add_all(
            [
                Worker(
                    kind=WorkerKind.RESEARCH,
                    project_id=p.id,
                    harness='script',
                    model='m',
                    runtime='subprocess',
                    state=WorkerState.RUNNING,
                    started_at=at(115),
                    reason='low_water: 2 ready < 10',
                    log_dir=str(log_dir),
                ),
                Worker(kind=WorkerKind.WATCHDOG, project_id=p.id, state=WorkerState.REQUESTED, reason='cron'),
            ]
        )
        s.add(Event(kind='idea.created', actor='cli', target_kind=TargetKind.IDEA, target_id=newer.id, at=at(110)))
        s.commit()
    text = render(db, project)
    now = text[text.index('### Now') : text.index('### Anomalies')]
    assert now == snapshot("""\
### Now
- Worker slots: 1 of 1 busy.
  - run#3 idea#3 "build on longer lines" (script): running 1h, worker#3 ended 1h ago
- Ideas: 3 runnable, 0 blocked; 0 seed, 0 proposed, 1 scheduled, 2 done, 0 parked, 0 rejected.
  - idea#5 p0 "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa…" by research#4, 20m ago
  - idea#6 p1 "even longer lines" by research#4, 10m ago; supersedes idea#4
  - idea#4 p2 "after the crash" by research#4, 0s ago; after idea#2; superseded by idea#6
- Agents live: 1; requested: watchdog#6
  - research#5 script/m 5m, on low_water: 2 ready < 10: ran evaluate.py: METRIC score=3
- Agents so far: 3 worker (1 killed), 1 watchdog, 2 research; 0 of 1 ended research sweeps defined no idea.
- Ideas by source: research 6 ideas, 3 ran → 1 keep, 0 interesting.
- Tokens: 4k (worker 3k, watchdog 0, research 500); 500 by agents that ended in the last hour.
- Evaluations: 3 (0 crashed); median 3.0 min, first 5 3.0 min, latest 5 3.0 min; 6.0 min in all; 0 candidates waiting for a verdict.
### Recent candidates (3 of 3; score, vs parent)
- candidate#1 keep 1.0000 (—), no run, 0s ago: baseline
- candidate#2 keep 3.0000 (+2.0000), run#1, 0s ago: score 3
- candidate#3 discard 3.0000 (+0.0000), run#1, 0s ago: score 3
""")
    assert text[text.index('### Inbox') :].split('### Tools')[0] == snapshot("""\
### Inbox (1 unacknowledged), decisions, escalations and watchdog findings
- comment#2 decision on project#1 by pi, 1h ago: keep going
- comment#1 escalation (p1, unacknowledged) on run#2 by watchdog#9, 1h ago: run#2 died in a dialog
### Recent events
- idea.created idea#6 10m ago · idea.status idea#2 0s ago · comment.note comment#3 0s ago · comment.decision comment#2 0s ago · comment.escalation comment#1 0s ago
""")


def test_age_floors_to_the_largest_unit() -> None:
    assert overview_module.age(NOW, NOW) == '0s'
    assert overview_module.age(NOW - timedelta(seconds=59), NOW) == '59s'
    assert overview_module.age(NOW - timedelta(minutes=3, seconds=59), NOW) == '3m'
    assert overview_module.age(NOW - timedelta(hours=23, minutes=59), NOW) == '23h'
    assert overview_module.age(NOW - timedelta(days=40), NOW) == '40d'
    assert overview_module.age(NOW + timedelta(hours=1), NOW) == '0s', 'a clock skew is not a negative age'


CLAUDE_SCREEN = """\
⏺ Baseline finished. Starting the two training runs.

✻ Training… (12s · esc to interrupt)
──────────────────────────────────────────
❯
──────────────────────────────────────────
  Opus 5.5 · ~/.piloop/runs/112/worktree ⎇ ca4aefc · ▰▰▱▱▱▱▱▱▱▱ 20%
  ⏵⏵ bypass permissions on (shift+tab to cycle) · ← for agents
"""
"""The end of a Claude Code pane as ``herdr agent read`` returns it: the output above the input box and status bar."""
CODEX_SCREEN = """\
• Ran the evaluation; measurement pending.
  Worked for 7m 23s • 4:21 PM
› Ask Codex to do anything
  GPT-6.1-Sol high · ~/.piloop/runs/95/worktree · Test worker idea
  ? for shortcuts                                         ⚠ 3 warnings · f2 to view
"""
PI_SCREEN = """\
 The ensemble wins because the HGBs make partially uncorrelated errors.
──────────────────────────────────────────
──────────────────────────────────────────
~/.piloop/runs/7/worktree (detached)
↑24 ↓21k R382k W31k CH98.3% $0.541 3.1%/1.0M (auto)                      (anthropic) claude-sonnet
"""
SESSION_ID = 'd96a2121-355c-4608-b2f7-8bfc85bba1b5'


def test_live_agents_say_their_last_words_from_the_session_file_else_the_screen_without_chrome(
    db: Database, project: Project, tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv('HOME', str(tmp_path))
    session = tmp_path / '.claude' / 'projects' / '-somewhere' / f'{SESSION_ID}.jsonl'
    session.parent.mkdir(parents=True)
    session.write_text((Path(__file__).parent.parent / 'fixtures' / 'said' / 'claude.jsonl').read_text())
    saved = tmp_path / 'saved'
    saved.mkdir()
    (saved / 'transcript.txt').write_text(CODEX_SCREEN)
    gone: Response = {'error': {'code': 'agent_not_found', 'message': 'no agent'}}
    screens = [CLAUDE_SCREEN, gone, gone, PI_SCREEN, 'just a shell $\n']
    scripted(monkeypatch, agent_read=[{'text': s} if isinstance(s, str) else s for s in screens])
    with db.session() as s:
        for name, harness, session_id, log_dir in (
            ('w0', 'claude', SESSION_ID, None),
            ('w1', 'claude', 'no-file-yet', None),
            ('w2', 'codex', None, saved),
            ('w3', 'gone', None, tmp_path / 'nowhere'),
            ('w4', 'script', None, None),
            ('w5', None, None, None),
        ):
            s.add(
                Worker(
                    kind=WorkerKind.RESEARCH,
                    project_id=project.id,
                    state=WorkerState.RUNNING,
                    harness=harness,
                    model='m' if harness else None,
                    session_id=session_id,
                    runtime='herdr',
                    handle={'workspace_id': 'w7', 'tab_id': 'w7:t2', 'pane_id': 'w7:p2', 'agent': name},
                    log_dir=str(log_dir) if log_dir else None,
                    reason='cron',
                    started_at=at(100),
                )
            )
        s.commit()
    assert [line for line in render(db, project).splitlines() if line.startswith('  - research#')] == snapshot(
        [
            '  - research#1 claude/m 20m, tab w7:t2, on cron: Starting both training runs (16 processes each).',
            '  - research#2 claude/m 20m, tab w7:t2, on cron: ⏺ Baseline finished. Starting the two training runs.',
            '  - research#3 codex/m 20m, tab w7:t2, on cron: • Ran the evaluation; measurement pending.',
            '  - research#4 gone/m 20m, tab w7:t2, on cron: —',
            '  - research#5 script/m 20m, tab w7:t2, on cron: The ensemble wins because the HGBs make partially uncorrelated errors.',
            '  - research#6 —/— 20m, tab w7:t2, on cron: just a shell $',
        ]
    )
    assert [c[2] for c in calls(herdr_log) if c[:2] == ['agent', 'read']] == ['w1', 'w2', 'w3', 'w4', 'w5'], (
        'an agent with text in its session file needs no runtime read'
    )


def test_a_row_holding_a_blob_renders_as_a_placeholder_with_one_warning_naming_it(
    db: Database, project: Project, capsys: pytest.CaptureFixture[str]
) -> None:
    """A comment body stored as bytes renders as a placeholder and prints one warning. It does not crash."""
    with db.session() as s:
        s.add(Comment(target_kind=TargetKind.PROJECT, target_id=project.id, author='w', body='x', priority=1))
        s.commit()
    with db.engine.begin() as con:  # The CHECK constraints reject a blob, so turn them off to insert one.
        con.exec_driver_sql('PRAGMA ignore_check_constraints = ON')
        con.exec_driver_sql("UPDATE comments SET body = x'00ff' WHERE id = 1")
        con.exec_driver_sql('PRAGMA ignore_check_constraints = OFF')
    assert '- comment#1 note (p1, unacknowledged) on project#1 by w, 0s ago: <not text: a 2-byte blob>' in render(
        db, project
    )
    render(db, project)
    assert capsys.readouterr().err == (
        'piloop: warning: comments#1 body is a 2-byte blob, not text; shown as a placeholder. Repair it:'
        ' UPDATE comments SET body = CAST(body AS TEXT) WHERE id = 1\n'
    ), 'the warning prints once per process'
    with db.session() as s:
        s.get_one(Comment, 1).priority = 2
        s.commit()
    with db.engine.connect() as con:
        assert con.exec_driver_sql('SELECT typeof(body) FROM comments').scalar() == 'blob', 'never written back'


def test_progress_lists_only_the_configured_conditions(db: Database, project: Project) -> None:
    def config(stop: dict[str, object], direction: str = 'higher') -> ProjectConfig:
        metrics = [{'name': 'score', 'direction': direction, 'role': 'primary'}, {'name': 'size', 'direction': 'lower'}]
        return ProjectConfig.model_validate({**project.config, 'stop': stop, 'metrics': metrics})

    with db.session() as s:
        p = s.get_one(Project, project.id)
        assert (
            overview_module.progress(s, p, config({'target': 0.5}, 'lower'), NOW)
            == 'run 0 · 0.0h · target ≤ 0.5 (head 1)'
        )
        assert overview_module.progress(s, p, config({}), NOW) == 'run 0 · 0.0h'
        s.get_one(Candidate, 1).verdict = Verdict.PENDING
        assert overview_module.progress(s, p, config({'target': 0.5}), NOW) == 'run 0 · 0.0h · target ≥ 0.5 (head —)'
