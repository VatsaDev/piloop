"""The daemon loop: check each role's triggers for each project, launch requested workers and reap stale state.

A tick has two phases. First, the triggers (https://piloop.autoresearch.inc/concepts/roles) insert ``workers`` rows in state ``requested``. Then the
requested rows launch in id order, as far as the role's ``max_concurrent`` allows. Anyone can insert a requested row
by hand, for example the CLI or an agent. Such a row launches in the same second phase.

Before both phases, the tick sends new comments to the running workers. For each running worker, it collects the
``note``, ``finding`` and ``decision`` comments on its run or its idea that were written since the run started. It
sends them through the worker's runtime in one prompt, one ``[piloop]`` line each (``steer_line``). An event on the
worker, ``comment.delivered`` or ``comment.undelivered``, records each delivery. A daemon restart does not send a
comment again. A worker that is still ``starting`` gets the comments on the next tick. A failed delivery is not
retried.
"""

import itertools
import math
import os
import threading
from collections import Counter
from collections.abc import Callable, Collection, Sequence
from concurrent.futures import Future, ThreadPoolExecutor, wait
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import timedelta
from functools import partial
from pathlib import Path

from git import Repo
from git.exc import InvalidGitRepositoryError, NoSuchPathError
from pydantic import ValidationError
from sqlalchemy import Select, exists, func, or_, select
from sqlalchemy.orm import Session

from piloop import git
from piloop.agents import scout
from piloop.agents.launcher import STARTUP_ANOMALY, STOPPED_ERROR, rotate
from piloop.agents.refill import usage_of
from piloop.agents.roles import ROLES, SCOUT, Role, Trigger, WatchdogMode, compose_brief, role_of
from piloop.agents.scout import SCOUT_EVENT, scouting
from piloop.campaign import spend
from piloop.campaign.ideas import defined_by, runnable, supersede, waiting
from piloop.campaign.memory import sync_documents
from piloop.campaign.project import WORKSPACE, worktree_links
from piloop.campaign.spend import SpendError
from piloop.campaign.stop import (
    CAMPAIGN_STOPPED_ERROR,
    STALE_ERROR,
    attempts,
    runs,
    runs_without_keep,
    stall,
    stop_reason,
)
from piloop.config import SCOUT_SEEDS, ProjectConfig, ResearchRole, RoleConfig, Spend, WatchdogRole, config_of
from piloop.daemon.runner import RunExecutor
from piloop.db.events import emit
from piloop.db.models import (
    LIVE,
    OPEN_RUNS,
    Candidate,
    Comment,
    CommentKind,
    Event,
    Idea,
    IdeaStatus,
    Project,
    ProjectStatus,
    Run,
    RunStatus,
    TargetKind,
    Verdict,
    Worker,
    WorkerKind,
    WorkerState,
    count,
    project_runs,
    utcnow,
)
from piloop.db.session import Database
from piloop.db.triggers import DAEMON_ACTOR
from piloop.otel import Tracer
from piloop.reports.overview import ANOMALY_EVENT, Anomaly, anomalies, assigned, overview, reported
from piloop.reports.pack import pack
from piloop.reports.wake import lessons_overview
from piloop.runtime import Outcome
from piloop.runtime.factory import RuntimeFactory, handle_for, make_runtime, runtime_for
from piloop.scoring.ledger import bar_of, checkout_candidate, head, score, unmeasured
from piloop.usage import Usage

NO_RUN_ERROR = 'a worker needs a run; the scheduler requests them from ready ideas'
PUSHED_KINDS = tuple(kind for kind in CommentKind if kind is not CommentKind.ESCALATION)
"""Comment kinds that are sent to a running worker. Escalations are not sent, because they are for the human."""
SETTLED = ('comment.delivered', 'comment.undelivered')
"""Events on a worker that record a comment as sent or failed. The comment id is in the payload."""
STEER_LINE_CHARS = 200
"""The number of characters of a comment's first line that a ``[piloop]`` line quotes. The worker can query the rest."""
WRAP_UP = 'worker.wrap_up'
"""Event on a worker of a stopped project: the worker was told to commit and stop.

The daemon ends the worker ``STOP_GRACE_SECONDS`` later."""
STOP_GRACE_SECONDS = 600
"""The seconds that a worker of a stopped project has to commit its work before the daemon ends it."""
WRAP_UP_PROMPT = (
    '[piloop] The campaign stopped ({reason}). Commit what you have now, with a message that says where it stands.'
    ' Leave your finding and stop. This session ends in {minutes:g} minutes.'
)
STARTUP_BREAKER = 3
"""The number of a project's agents that can fail at startup within ``STARTUP_WINDOW`` before its launches pause.

A host that is too loaded to start agents makes every new agent fail, and each failure costs a start window."""
STARTUP_WINDOW = timedelta(minutes=10)
LAUNCH_PAUSED = 'launch.paused'
"""Event on a project whose launches the startup breaker paused. ``comment`` in the payload is the escalation."""
LAUNCH_RESUMED = 'launch.resumed'
"""Event on a project whose paused launches resumed. It ends the pause that ``LAUNCH_PAUSED`` started."""


@dataclass(frozen=True)
class TrackBacklog:
    """One track's part of the backlog: its runnable and waiting ideas, and its share of the target."""

    name: str
    runnable: int
    waiting: int
    """Ideas that only prerequisites in flight block (``piloop.campaign.ideas.waiting``)."""
    target: int
    """The track's share of ``Backlog.target``, rounded up. It is 0 for a track with share 0."""

    @property
    def gap(self) -> int:
        """The ideas that the track needs to reach its share. Negative if it has more."""
        return self.target - self.runnable - self.waiting


@dataclass(frozen=True)
class Backlog:
    """The input to ``low_water``: the runnable ideas, the target, and what the sweeps in flight will add."""

    tracks: tuple[TrackBacklog, ...]
    """One entry per track, in config order. A track below its share is short, whatever the other tracks have."""
    target: int
    """One idea per worker slot plus ``[roles.research] buffer``, but never more than ``[stop] max_runs`` allows."""
    free: int
    """The runs that the project can start now."""
    sweeps: int
    """The research sweeps in flight."""
    pending: int
    """The ideas that those sweeps can still insert: ``max_ideas`` each."""

    @property
    def runnable(self) -> int:
        """The ideas that the queue can take now: ``ready``, not blocked, and on a track with a share."""
        return sum(t.runnable for t in self.tracks)

    @property
    def waiting(self) -> int:
        """The ideas that become runnable when the runs in flight that they wait for finish."""
        return sum(t.waiting for t in self.tracks)

    @property
    def short(self) -> int:
        """The ideas missing from the target after the sweeps in flight insert theirs.

        This is the sum of the tracks' shortfalls. A surplus on one track does not cover a shortfall on another. If no
        track is short, it is the negative surplus. An idea that waits for a run in flight is not missing.
        """
        gaps = [t.gap for t in self.tracks]
        wanted = sum(g for g in gaps if g > 0) if any(g > 0 for g in gaps) else sum(gaps)
        return wanted - self.pending

    @property
    def idle(self) -> int:
        """The free worker slots that no runnable idea and no sweep in flight will fill."""
        return self.free - self.runnable - self.pending

    def held(self) -> str:
        """Describe the backlog without the sweeps.

        For example: ``1 of 3 runnable ideas, 2 more waiting on runs in flight``.
        """
        waits = f', {self.waiting} more waiting on runs in flight' if self.waiting else ''
        return f'{self.runnable} of {self.target} runnable ideas{waits}'

    def __str__(self) -> str:
        coming = f', {self.pending} more from {self.sweeps} sweeps in flight' if self.sweeps else ''
        per_track = ''
        if len(self.tracks) > 1:
            per_track = ' (' + ', '.join(f'{t.name} {t.runnable} of {t.target}' for t in self.tracks) + ')'
        backlog = f'{self.held()}{per_track}{coming}'
        return f'{self.idle} free worker slots without a runnable idea; {backlog}' if self.idle > 0 else backlog


class Scheduler:
    """The daemon loop. There is one instance per daemon. ``tick`` is safe to call from a loop or from tests."""

    def __init__(
        self,
        db: Database,
        executor: RunExecutor,
        tracer: Tracer,
        home: Path,
        runtime_factory: RuntimeFactory = make_runtime,
    ) -> None:
        self._db = db
        self._executor = executor
        self._tracer = tracer
        self._home = home
        self._runs_dir = home / 'runs'
        self._workers_dir = home / 'workers'
        self._runtime_factory = runtime_factory
        self._pool = ThreadPoolExecutor(thread_name_prefix='agent')
        self._futures: dict[int, Future[None]] = {}
        self._scoring: dict[tuple[TargetKind, int], Future[None]] = {}
        """Scoring outside a worker: a run's resumed scoring by run id, or a project's re-measure by project id."""
        self._stop = threading.Event()

    def reap(self) -> None:
        """Re-attach the agents that a previous daemon process left. Fail everything else it left in flight.

        The daemon watches a ``running`` agent whose worktree still exists until it ends, even if it ended already. Then
        it finishes the run as if no daemon had stopped (``RunExecutor.reattach``, or ``_reattach_sweep`` for a sweep).
        Both an in-place restart (``hand_over``) and a crash leave agents for this. A closed run whose commits were not
        all scored is scored now (``RunExecutor.resume``). Its idea stayed ``scheduled``, so it does not run again and
        no dependent idea takes it before its keeps are known. The worktrees of failed runs and sweeps are removed.
        """
        with self._tracer.span('piloop.reap'):
            left = self._left_running()
            self._fail_open(STALE_ERROR, remove_worktrees=True, sparing=[worker_id for worker_id, _ in left])
            for worker_id, run_id in left:
                self._tracer.event('worker.reattached', worker=worker_id, run=run_id)
                body = (
                    partial(self._reattach_sweep, worker_id)
                    if run_id is None
                    else partial(self._executor.reattach, run_id, worker_id)
                )
                self._futures[worker_id] = self._pool.submit(body)
            with self._db.session() as s:
                open_run = exists().where(Run.idea_id == Idea.id, Run.status.in_(OPEN_RUNS))
                ideas = s.scalars(select(Idea).where(Idea.status == IdeaStatus.SCHEDULED, ~open_run)).all()
                unscored = [idea.runs[-1].id for idea in ideas]
            for run_id in unscored:
                self._tracer.event('run.unscored', run=run_id)
                self._scoring[TargetKind.RUN, run_id] = self._pool.submit(self._executor.resume, run_id)

    def _left_running(self) -> list[tuple[int, int | None]]:
        """Return ``(worker, run)`` for each ``running`` agent whose worktree exists and whose run, if any, is open."""
        with self._db.session() as s:
            rows = s.scalars(select(Worker).where(Worker.state == WorkerState.RUNNING)).all()
            return [(w.id, w.run_id) for w in rows if self._reattachable(s, w)]

    def _reattachable(self, s: Session, worker: Worker) -> bool:
        if worker.run_id is None:
            return (self._workers_dir / str(worker.id) / 'worktree').is_dir()
        wt = self._runs_dir / str(worker.run_id) / 'worktree'
        return wt.is_dir() and s.get_one(Run, worker.run_id).status is RunStatus.RUNNING

    def _fail_open(self, error: str, *, remove_worktrees: bool, sparing: Collection[int] = ()) -> None:
        """Fail each run in flight and queue its idea again. Kill and close each live worker except ``sparing``.

        Requested workers and their queued runs are not changed, because they are requests, not processes. An idea
        that is ``scheduled`` without an open run stays so. Its run finished and only its scoring stopped, which
        ``reap`` resumes. Only ``reap`` sets ``remove_worktrees``. A live run thread removes its own worktrees in its
        ``finally`` block. Removing a worktree while an evaluator still runs in it is worse than leaving it a moment
        longer.
        """
        with self._db.session() as s:
            spared = Worker.id.in_(sparing)
            launched = select(Worker.run_id).where(Worker.state.in_(LIVE), Worker.run_id.is_not(None), ~spared)
            kept = select(Worker.run_id).where(spared, Worker.run_id.is_not(None))
            in_flight = (Run.status == RunStatus.RUNNING) & Run.id.not_in(kept)
            for run in s.scalars(select(Run).where(in_flight | Run.id.in_(launched))).all():
                self._fail_run(s, run, error)
                if remove_worktrees:
                    self._remove_worktrees(self._runs_dir / str(run.id), Path(run.idea.project.path))
            for worker in s.scalars(select(Worker).where(Worker.state.in_(LIVE), ~spared)):
                # A ``starting`` row that stop() reached before its start window has no process.
                if worker.runtime is not None:
                    self._kill_stale(s, worker)
                if remove_worktrees and worker.run_id is None and worker.project_id is not None:
                    project_path = Path(s.get_one(Project, worker.project_id).path)
                    self._remove_worktrees(self._workers_dir / str(worker.id), project_path)
                self._close(s, worker, error)
            s.commit()

    def _fail_run(self, s: Session, run: Run, error: str) -> None:
        """Fail ``run`` and return its idea to the queue, unless a human parked or rejected it meanwhile.

        A run that the daemon stopped (``stop.NOT_ATTEMPTS``) is not an attempt, so its idea always goes back to
        ``ready``.
        """
        run.status, run.error, run.finished_at = RunStatus.FAILED, error, utcnow()
        emit(s, 'run.finished', TargetKind.RUN, run.id, payload={'status': run.status, 'error': error})
        idea = run.idea
        if idea.status is IdeaStatus.SCHEDULED:
            idea.status = IdeaStatus.READY if attempts(idea) < idea.max_runs else IdeaStatus.DONE
        self._tracer.event('run.aborted', run=run.id, error=error)

    def _close(self, s: Session, worker: Worker, error: str) -> None:
        """Close a worker row that the launcher does not close (after a stop or a reap), and read its usage.

        The usage comes from a scan of the session store. The agent can spend many tokens before it is killed. If the
        budget ignored them, a restarted campaign could go past ``max_tokens``. A requested row never started and has
        no session to read.
        """
        worker.state, worker.ended_at, worker.error = WorkerState.KILLED, utcnow(), error
        if worker.session_id is None:
            return
        found = usage_of(s, worker)
        if isinstance(found, Usage):
            worker.usage = found.as_json()
        elif found is not None:
            self._tracer.event('worker.usage_unknown', worker=worker.id, error=str(found))

    def _remove_worktrees(self, agent_dir: Path, project_path: Path) -> None:
        """Remove the worktrees under a run's or a sweep's directory. If the project is gone, record an event."""
        try:
            repo = git.open_repo(project_path)
        except NoSuchPathError, InvalidGitRepositoryError:
            self._tracer.event('worktrees.orphaned', agent_dir=str(agent_dir), project_path=str(project_path))
            return
        for wt in git.worktrees_under(repo, agent_dir):
            git.remove_worktree(repo, wt)

    def _kill_stale(self, s: Session, worker: Worker) -> None:
        """Ask the runtime to close the worker's Herdr pane, which outlives the daemon. Best effort."""
        try:
            runtime_for(s, worker, self._runs_dir, self._runtime_factory).kill(handle_for(worker))
        except Exception as e:  # The pane can be gone already. The database row is what matters.
            self._tracer.event('worker.kill_failed', worker=worker.id, error=str(e))

    def tick(self) -> list[int]:
        """Send new comments to running workers, run every project's triggers and launch requested workers.

        Returns the ids of the launched workers. Before the triggers, the tick rejects the ideas that a newer idea
        superseded (``piloop.campaign.ideas.supersede``). This also happens for a stopped project, which requests
        nothing. A project that the spend guard holds (``_read_spend``) gets only this step.
        """
        self._collect()
        self._push_comments()
        self._wind_down()
        self._read_spend()
        launched: list[tuple[int, int | None]] = []
        with self._db.session() as s:
            for project in s.scalars(select(Project)).all():
                try:
                    cfg = config_of(project)
                except ValidationError as e:
                    self._tracer.event('project.invalid_config', project=project.name, error=str(e))
                    continue
                for idea_id, by in supersede(s, project.id).items():
                    self._tracer.event('idea.superseded', project=project.name, idea=idea_id, by=by)
                held = spend.held(project, cfg)
                if project.status is ProjectStatus.ACTIVE and held is None:
                    self._remeasure(s, project)
                    self._triggers(s, project, cfg)
                s.flush()
                if held is None:
                    launched += self._take(s, project, cfg)
            s.commit()
        for worker_id, run_id in launched:
            body = partial(self._executor.execute, run_id, worker_id) if run_id else partial(self._sweep, worker_id)
            self._futures[worker_id] = self._pool.submit(body)
        return [worker_id for worker_id, _ in launched]

    def _read_spend(self) -> None:
        """Run the ``[spend] command`` of each active project whose ``interval`` is up, and record the result.

        This runs outside any transaction, because the command can take up to ``spend.TIMEOUT``. A write lock held for
        that long locks all other clients out. While ``spend.held`` returns a reason, the tick does not request,
        re-measure or launch for the project. Work in progress continues.
        """
        with self._db.session() as s:
            todo: list[tuple[int, Path, Spend]] = []
            for project in s.scalars(select(Project).where(Project.status == ProjectStatus.ACTIVE)):
                try:
                    cfg = config_of(project)
                except ValidationError:
                    continue  # The tick records this error for each project.
                if cfg.spend is not None and spend.due(project, cfg.spend):
                    todo.append((project.id, Path(project.path), cfg.spend))
        for project_id, path, budget in todo:
            try:
                reading: float | SpendError = spend.read(budget.command, path)
            except SpendError as e:
                reading = e
            with self._db.session() as s:
                spend.record(s, s.get_one(Project, project_id), budget, reading)
                s.commit()
            self._tracer.event('spend.read', project=project_id, reading=str(reading))

    def _remeasure(self, s: Session, project: Project) -> None:
        """Measure the candidates that the current evaluator has not measured, one at a time per project.

        After an evaluator change, no candidate has a score from the new evaluator. The keeps are measured again first,
        newest first, because the head and every bar are keeps. Then the references that a run can pin are measured
        (``unmeasured``). Runs that are not pinned wait until every keep has a score (``_queue``).
        """
        key = (TargetKind.PROJECT, project.id)
        if key in self._scoring or not (todo := unmeasured(s, project)):
            return
        self._tracer.event('candidate.remeasure', project=project.name, candidate=todo[0].id, left=len(todo))
        self._scoring[key] = self._pool.submit(self._executor.remeasure, todo[0].id)

    def _push_comments(self) -> None:
        """Send each running worker the comments on its run or its idea that it has not received yet.

        The events table records what each worker received: ``SETTLED`` events on the worker, with the comment id in
        the payload. Nothing depends on the daemon's memory. A restart sends nothing again. A worker whose row is still
        ``starting`` (brief composed, pane not yet up) is skipped and gets the comments on the next tick. A failed
        delivery is also recorded and not retried. The comment is in the brief of the idea's next run anyway, and a
        headless worker would fail the same way on every tick. A tick that sends something makes one span.
        """
        with self._db.session() as s:
            pending = [(w, unseen) for w in self._running_workers(s) if (unseen := self._unseen(s, w))]
            if not pending:
                return
            with self._tracer.span('piloop.push', comments=sum(len(c) for _, c in pending)):
                for worker, comments in pending:
                    self._push(s, worker, comments)
                s.commit()

    def _running_workers(self, s: Session) -> Sequence[Worker]:
        return s.scalars(
            select(Worker)
            .where(Worker.kind == WorkerKind.WORKER, Worker.state == WorkerState.RUNNING, Worker.run_id.is_not(None))
            .order_by(Worker.id)
        ).all()

    def _unseen(self, s: Session, worker: Worker) -> list[Comment]:
        """Return the comments on the worker's run or idea that are new since the run started and not yet sent.

        The brief cannot contain comments written after the run started, because the transaction that composes the
        brief sets ``Run.started_at``. The comparison uses whole seconds. A raw INSERT sets ``created_at`` to
        ``CURRENT_TIMESTAMP``, which has second precision, but ``started_at`` is a Python timestamp. So a comment
        written in the same second as the brief can reach the worker twice. That is better than never. The worker's
        own comments are not sent back. A worker signs as ``worker#<id>`` (its ``PILOOP_ACTOR``) or ``run#<id>``.
        """
        run = s.get_one(Run, worker.run_id)
        since = (run.started_at or worker.started_at).strftime('%Y-%m-%d %H:%M:%S')
        settled = {
            e.payload.get('comment')
            for e in s.scalars(
                select(Event).where(
                    Event.target_kind == TargetKind.WORKER, Event.target_id == worker.id, Event.kind.in_(SETTLED)
                )
            )
        }
        on_run = (Comment.target_kind == TargetKind.RUN) & (Comment.target_id == run.id)
        on_idea = (Comment.target_kind == TargetKind.IDEA) & (Comment.target_id == run.idea_id)
        comments = s.scalars(
            select(Comment)
            .where(on_run | on_idea, Comment.kind.in_(PUSHED_KINDS))
            .where(func.strftime('%Y-%m-%d %H:%M:%S', Comment.created_at) >= since)
            .order_by(Comment.id)
        ).all()
        own = (f'worker#{worker.id}', f'run#{run.id}')
        return [c for c in comments if c.id not in settled and c.author not in own]

    def _push(self, s: Session, worker: Worker, comments: list[Comment]) -> None:
        """Send the comments in one ``runtime.prompt``, one line each, and record each as delivered or undelivered.

        One prompt, because two prompts typed into a busy pane join in its input queue with nothing between them.
        """
        try:
            runtime_for(s, worker, self._runs_dir, self._runtime_factory).prompt(
                handle_for(worker), '\n'.join(steer_line(c) for c in comments)
            )
        except Exception as e:  # NotSteerable (subprocess), a blocked agent or a closed pane. The daemon goes on.
            for comment in comments:
                payload: dict[str, object] = {'comment': comment.id, 'error': str(e)}
                emit(s, 'comment.undelivered', TargetKind.WORKER, worker.id, payload=payload)
                self._tracer.event('comment.undelivered', comment=comment.id, worker=worker.id, error=str(e))
            return
        for comment in comments:
            emit(s, 'comment.delivered', TargetKind.WORKER, worker.id, payload={'comment': comment.id})
            self._tracer.event('comment.delivered', comment=comment.id, worker=worker.id)

    def _wind_down(self) -> None:
        """Tell each worker of a stopped project to commit and stop (``WRAP_UP``), and end it after the grace period.

        The grace period is ``STOP_GRACE_SECONDS``. The runs then close as usual and their commits are scored. Sweeps
        are not changed, because their timeouts limit them and the closing watchdog is one of them.
        """
        with self._db.session() as s:
            workers = s.scalars(
                select(Worker)
                .join(Project, Worker.project_id == Project.id)
                .where(
                    Worker.kind == WorkerKind.WORKER,
                    Worker.state == WorkerState.RUNNING,
                    Project.status == ProjectStatus.STOPPED,
                )
                .order_by(Worker.id)
            ).all()
            for worker in workers:
                told = s.scalar(
                    select(Event.at).where(
                        Event.target_kind == TargetKind.WORKER, Event.target_id == worker.id, Event.kind == WRAP_UP
                    )
                )
                if told is None:
                    self._wrap_up(s, worker)
                elif utcnow() - told >= timedelta(seconds=STOP_GRACE_SECONDS):
                    self._tracer.event('worker.ended', worker=worker.id, reason=CAMPAIGN_STOPPED_ERROR)
                    self._kill_stale(s, worker)
            s.commit()

    def _wrap_up(self, s: Session, worker: Worker) -> None:
        """Send the wrap-up prompt. The grace period starts even if the prompt fails, as for a headless worker."""
        reason = s.get_one(Project, worker.project_id).stopped_reason or CAMPAIGN_STOPPED_ERROR
        payload: dict[str, object] = {'reason': reason}
        try:
            runtime_for(s, worker, self._runs_dir, self._runtime_factory).prompt(
                handle_for(worker), WRAP_UP_PROMPT.format(reason=reason, minutes=STOP_GRACE_SECONDS / 60)
            )
        except Exception as e:  # NotSteerable (subprocess), a blocked agent or a closed pane.
            payload['error'] = str(e)
        emit(s, WRAP_UP, TargetKind.WORKER, worker.id, payload=payload)
        self._tracer.event(WRAP_UP, worker=worker.id, **{k: str(v) for k, v in payload.items()})

    def _triggers(self, s: Session, project: Project, cfg: ProjectConfig) -> None:
        """Stop the campaign if a ``[stop]`` condition holds. Otherwise check every role's triggers.

        A stopped project gets one closing watchdog sweep and nothing more from the triggers. Triggers requested
        everything that is pending at that moment, so it is all cancelled. Requested rows are closed and queued runs
        fail with ``CAMPAIGN_STOPPED_ERROR``. Their ideas go back to ``ready``. Rows that a human inserts after the stop
        still launch. Workers in flight are told to stop (``_wind_down``), and their commits are scored. The ``[memory]``
        documents are synced first, so that the closing sweep can search the campaign's reports.
        """
        reason = stop_reason(s, project, cfg)
        if reason is None:
            for role in ROLES.values():
                self._request(s, project, cfg, role)
            return
        project.status, project.stopped_reason = ProjectStatus.STOPPED, reason
        self._tracer.event('project.stopped', project=project.name, reason=reason)
        self._drain(s, project)
        synced = sync_documents(s, project, cfg, DAEMON_ACTOR)
        self._tracer.event('documents.synced', project=project.name, result=synced.line())
        s.add(Worker(kind=WorkerKind.WATCHDOG, project_id=project.id, reason=f'campaign stopped: {reason}'))

    def _drain(self, s: Session, project: Project) -> None:
        mine = Idea.project_id == project.id
        for run in s.scalars(select(Run).join(Idea).where(mine, Run.status == RunStatus.QUEUED)):
            self._fail_run(s, run, CAMPAIGN_STOPPED_ERROR)
        for worker in s.scalars(
            select(Worker).where(Worker.project_id == project.id, Worker.state == WorkerState.REQUESTED)
        ):
            self._close(s, worker, CAMPAIGN_STOPPED_ERROR)

    def _request(self, s: Session, project: Project, cfg: ProjectConfig, role: Role) -> None:
        """Insert requested rows when the role's triggers fire. One batch per call, however many triggers fire.

        ``low_water`` requests enough sweeps to fill the backlog's shortfall at ``max_ideas`` each, within the role's
        free slots. Each sweep uses the next harness of the rotation. The other triggers, ``seed`` included, request
        one sweep. ``anomaly`` fires alone in its tick. Its row gets the new anomalies, one ``anomaly.reported`` event
        each, which makes it an incident sweep. A timer that is due at the same time fires on a later tick.
        """
        rc = cfg.role(role.kind)
        if Trigger.QUEUE in role.triggers:
            self._queue(s, project, cfg)
        last = s.scalars(
            select(Worker)
            .where(Worker.project_id == project.id, Worker.kind == role.kind)
            .order_by(Worker.id.desc())
            .limit(1)
        ).first()
        if last is not None and last.state is WorkerState.REQUESTED:
            return  # One pending request per role is enough.
        if role.kind is WorkerKind.RESEARCH:
            # The other research triggers ignore scout sweeps.
            last = self._latest(s, project, role.kind, scout=False)
        incidents = self._incidents(s, project, cfg) if Trigger.ANOMALY in role.triggers else []
        reasons: dict[Trigger, str]
        if incidents:
            reasons = {Trigger.ANOMALY: 'anomaly: ' + ', '.join(a.key for a in incidents)}
        else:
            reasons = {
                trigger: reason
                for trigger in sorted(role.triggers - {Trigger.QUEUE, Trigger.ANOMALY})
                if (reason := self._fires(s, project, cfg, rc, trigger, last)) is not None
            }
        if len(reasons) > 1:
            reasons.pop(Trigger.SCOUT, None)  # A scout sweep is a separate mode, so it waits for a later tick.
        if not reasons:
            return
        count = 1
        if Trigger.LOW_WATER in reasons:
            backlog = self._backlog(s, project, cfg)
            count = min(-(-backlog.short // cfg.roles.research.max_ideas), rc.max_concurrent - backlog.sweeps)
        reason = '; '.join(reasons.values())
        rows = [Worker(kind=role.kind, project_id=project.id, reason=reason) for _ in range(count)]
        s.add_all(rows)
        s.flush()
        now = utcnow()
        for anomaly in incidents:
            payload: dict[str, object] = {'key': anomaly.key, 'anomaly': anomaly.line(now)}
            emit(s, ANOMALY_EVENT, TargetKind.WORKER, rows[0].id, payload=payload)
        if Trigger.SCOUT in reasons:
            emit(s, SCOUT_EVENT, TargetKind.WORKER, rows[0].id)
        self._tracer.event('worker.requested', project=project.name, kind=role.kind, reason=reason, count=count)

    def _incidents(self, s: Session, project: Project, cfg: ProjectConfig) -> list[Anomaly]:
        """Return the project's incident anomalies that no agent has received yet, oldest first."""
        given = reported(s, project.id)
        found = [
            a for a in anomalies(s, project, cfg, utcnow(), partial(self._jobs, s)) if a.incident and a.key not in given
        ]
        return found[::-1]

    def _jobs(self, s: Session, worker: Worker) -> list[str]:
        """Return the jobs that the worker's runtime reports in flight, or none if the runtime fails (traced)."""
        try:
            return runtime_for(s, worker, self._runs_dir, self._runtime_factory).jobs(handle_for(worker))
        except Exception as e:  # A Herdr error must not stop the tick. The next tick checks the anomaly again.
            self._tracer.event('worker.jobs_failed', worker=worker.id, error=str(e))
            return []

    def _fires(
        self, s: Session, project: Project, cfg: ProjectConfig, rc: RoleConfig, trigger: Trigger, last: Worker | None
    ) -> str | None:
        """Return the reason that a trigger fires now, or None. ``last`` is the role's latest row in this project.

        A trigger's settings are on the role's config class, so ``ROLES[kind].triggers`` and ``Roles`` must agree.
        """
        if trigger is Trigger.CRON and isinstance(rc, WatchdogRole):
            routine = self._routine(s, project, last.kind) if last is not None else None
            since = routine.started_at if routine is not None else project.created_at
            due = rc.interval > 0 and utcnow() - since >= timedelta(seconds=rc.interval)
            return f'timer: {rc.interval:g}s' if due else None
        if trigger is Trigger.LOW_WATER and isinstance(rc, ResearchRole):
            return self._low_water(s, project, cfg, rc, last)
        if trigger is Trigger.SEED and isinstance(rc, ResearchRole):
            return self._seeded(s, project, cfg, rc, last)
        if trigger is Trigger.SCOUT and isinstance(rc, ResearchRole):
            if rc.scout is None:
                return None
            latest = self._latest(s, project, WorkerKind.RESEARCH, scout=True)
            since = latest.started_at if latest is not None and latest.started_at is not None else project.created_at
            due = utcnow() - since >= timedelta(seconds=rc.scout.interval)
            return f'scout: every {rc.scout.interval:g}s' if due else None
        raise TypeError(f'{type(rc).__name__} has no settings for the {trigger} trigger')

    def _latest(self, s: Session, project: Project, kind: WorkerKind, *, scout: bool) -> Worker | None:
        """Return the role's latest row that is a scout sweep (if ``scout``) or that is not one."""
        mode = scouting() if scout else ~scouting()
        query = select(Worker).where(Worker.project_id == project.id, Worker.kind == kind, mode)
        return s.scalars(query.order_by(Worker.id.desc()).limit(1)).first()

    def _routine(self, s: Session, project: Project, kind: WorkerKind) -> Worker | None:
        """Return the role's latest row that did not get an anomaly. The timer counts from it."""
        given = exists().where(
            Event.kind == ANOMALY_EVENT, Event.target_kind == TargetKind.WORKER, Event.target_id == Worker.id
        )
        return s.scalars(
            select(Worker)
            .where(Worker.project_id == project.id, Worker.kind == kind, ~given)
            .order_by(Worker.id.desc())
            .limit(1)
        ).first()

    def _seeded(
        self, s: Session, project: Project, cfg: ProjectConfig, rc: ResearchRole, last: Worker | None
    ) -> str | None:
        """Fire one sweep if seeds wait that no sweep has read and a research slot is free, however full the backlog.

        A seed filed after the role's latest row started is not in any sweep's brief yet. If the latest sweep got the
        seed but was killed, the seed is retried, once per batch, as ``low_water`` retries. A seed that a sweep read
        and did not develop is in the next sweep's brief, whatever trigger fires it. Gotcha: a sweep's brief is read
        a moment before its row's ``started_at`` is set, so a seed filed in that moment waits for the next sweep.
        """
        if self._backlog(s, project, cfg).sweeps >= rc.max_concurrent:
            return None
        seeds = select(Idea).where(Idea.project_id == project.id, Idea.status == IdeaStatus.SEED)
        waiting = count(s, seeds)
        if not waiting:
            return None
        fresh = waiting if last is None else count(s, seeds.where(Idea.created_at > last.started_at))
        if fresh:
            return f'{fresh} new seeds to develop'
        assert last is not None  # With no row yet, every waiting seed is fresh.
        if last.state is WorkerState.KILLED and self._killed_streak(s, project, last.kind) <= rc.max_concurrent:
            return f'{waiting} seeds to develop; retrying after worker#{last.id} was killed: {last.error}'
        return None

    def _low_water(
        self, s: Session, project: Project, cfg: ProjectConfig, rc: ResearchRole, last: Worker | None
    ) -> str | None:
        """Fire if the backlog has fewer ideas than its target (``Backlog``) and there is a reason to sweep now.

        The target is one idea per worker slot plus ``buffer``. So ideas exist before slots free up, and a free slot
        does not wait a whole sweep for an idea. The ideas that sweeps in flight can insert already count as backlog.
        Nothing is requested while every research slot is busy. Runnable ideas count. Ideas blocked only by
        prerequisites in flight (``waiting``) also count, because they run when those finish. An idea blocked by a
        prerequisite that is not running does not count. Below the target, a sweep fires for one of these reasons:

        - The project has no sweep yet, so there is nothing to wait for.
        - The last batch was killed. Lost prompts, timeouts or a daemon stop say nothing about the backlog, so the
          batch is retried once. This applies to a streak of killed rows no longer than one batch
          (``max_concurrent``). A second killed batch is a systemic fault and counts as a normal last sweep.
        - New results: runs finished since the role's latest row started, which that sweep could not read. A sweep
          with nothing new to read reads the same history and proposes nothing. So each consecutive sweep that
          defined no idea doubles the number of finished runs the next sweep waits for (``_empty_sweeps``). A sweep
          that defines an idea resets this. The wait is never longer than what can still come: with no run in flight,
          the finished runs are all there is.
        - The backlog is low. Research keeps sweeping while the backlog is short, not only when results come in or a
          slot is idle. A sweep that defined an idea is followed at once. After empty sweeps, research rests
          2**n − 1 times as long as its latest ended sweep took. n is the number of consecutive ended sweeps, killed
          ones included, that defined no idea. An empty queue never deadlocks, and research that keeps finding nothing
          uses at most 1/2**n of the wall-clock time.
        """
        if rc.max_ideas == 0:
            return None
        backlog = self._backlog(s, project, cfg)
        if backlog.short <= 0 or backlog.sweeps >= rc.max_concurrent:
            return None
        if last is None:
            return str(backlog)
        if last.state is WorkerState.KILLED and self._killed_streak(s, project, last.kind) <= rc.max_concurrent:
            return f'{backlog}; retrying after worker#{last.id} was killed: {last.error}'
        mine = project_runs(project.id)
        finished = count(s, mine.where(Run.finished_at > last.started_at))
        in_flight = count(s, mine.where(Run.status.in_(OPEN_RUNS)))
        barren = len(self._empty_sweeps(s, project, (WorkerState.EXITED,)))
        if finished >= max(min(2**barren, finished + in_flight), 1):
            backoff = f' (after {barren} sweeps that defined no idea)' if barren else ''
            return f'{backlog}; {finished} runs finished since worker#{last.id} started{backoff}'
        empty = self._empty_sweeps(s, project, (WorkerState.EXITED, WorkerState.KILLED))
        if not empty:
            return str(backlog)
        latest = empty[0]
        assert latest.ended_at is not None  # An exited or killed row has ended.
        rest = (2 ** len(empty) - 1) * (latest.ended_at - latest.started_at)
        if utcnow() - latest.ended_at < rest:
            return None
        return f'{backlog}; rested {rest.total_seconds():.0f}s after {len(empty)} sweeps that defined no idea'

    def _backlog(self, s: Session, project: Project, cfg: ProjectConfig) -> Backlog:
        target = cfg.roles.worker.max_concurrent + cfg.roles.research.buffer
        if cfg.stop.max_runs is not None:
            target = min(target, cfg.stop.max_runs - runs(s, project))
        sweeps = count(
            s,
            select(Worker).where(
                Worker.project_id == project.id,
                Worker.kind == WorkerKind.RESEARCH,
                Worker.state.in_(LIVE),
                ~scouting(),
            ),
        )
        ready, blocked = (
            Counter(s.scalars(select(Idea.track).where(Idea.project_id == project.id, which())))
            for which in (runnable, waiting)
        )
        tracks = tuple(
            TrackBacklog(
                t.name,
                runnable=ready[t.name] if t.share else 0,
                waiting=blocked[t.name] if t.share else 0,
                target=math.ceil(t.share * target) if target > 0 else 0,
            )
            for t in cfg.all_tracks
        )
        return Backlog(
            tracks=tracks,
            target=target,
            free=self._free_slots(s, project, cfg),
            sweeps=sweeps,
            pending=sweeps * cfg.roles.research.max_ideas,
        )

    def _free_slots(self, s: Session, project: Project, cfg: ProjectConfig) -> int:
        """Return the runs that the project can start now.

        This is ``[roles.worker] max_concurrent`` minus the open runs, and never more than ``max_runs`` allows.
        """
        open_runs = project_runs(project.id).where(Run.status.in_(OPEN_RUNS))
        free = cfg.roles.worker.max_concurrent - count(s, open_runs)
        if cfg.stop.max_runs is not None:
            free = min(free, cfg.stop.max_runs - runs(s, project))
        return max(free, 0)

    def _empty_sweeps(self, s: Session, project: Project, states: tuple[WorkerState, ...]) -> list[Worker]:
        """Return the latest research sweeps in ``states``, newest first, until one that defined an idea.

        A sweep defined an idea if it inserted one or developed a seed into one (``defined_by``). Rows in other states
        are skipped, not counted. The new-results back-off skips killed sweeps. A killed sweep stopped early, so its
        empty result says nothing about what was left to propose. The idle rest counts killed sweeps. Otherwise, a
        harness that dies every time would be launched again on every tick.
        """
        signed = defined_by(s, project.id)
        ended = s.scalars(
            select(Worker)
            .where(
                Worker.project_id == project.id,
                Worker.kind == WorkerKind.RESEARCH,
                Worker.state.in_(states),
                ~scouting(),
            )
            .order_by(Worker.id.desc())
        )
        return list(itertools.takewhile(lambda worker: f'research#{worker.id}' not in signed, ended))

    def _killed_streak(self, s: Session, project: Project, kind: WorkerKind) -> int:
        """Count the role's latest rows, newest first and without scout sweeps, that were killed in a row."""
        states = s.scalars(
            select(Worker.state)
            .where(Worker.project_id == project.id, Worker.kind == kind, ~scouting())
            .order_by(Worker.id.desc())
        )
        return len(list(itertools.takewhile(lambda state: state is WorkerState.KILLED, states)))

    def _queue(self, s: Session, project: Project, cfg: ProjectConfig) -> None:
        """Queue a run, and request its worker, for each free slot with a runnable idea, within ``[stop] max_runs``.

        A ``ready`` idea with an ``after`` link to an idea that is not ``done`` is skipped and not changed. It stays
        ``ready`` and is taken when every prerequisite has finished. A runnable idea whose track is not in
        ``piloop.toml`` is parked with a note that says why (``_park_strays``). An idea on a track with a share of 0
        waits. While a new evaluator has not measured every keep (``_remeasure``), only a pinned idea has a base. The
        other ideas wait. Otherwise they could start from an old head while a better keep waits for its measurement.
        When an exploration run is due (``_explore``), the slot goes first to an idea pinned to an alternative base.
        Otherwise it goes to the most urgent idea of the track furthest below its share (``_pick``). An idea pinned to
        a harness at its cap (``_full``) waits. When every harness of the rotation is at its cap, nothing more is
        queued in this tick.
        """
        self._park_strays(s, project, cfg)
        live = [t.name for t in cfg.all_tracks if t.share > 0]
        query = select(Idea).where(Idea.project_id == project.id, runnable(), Idea.track.in_(live))
        if any(c.verdict is Verdict.KEEP for c in unmeasured(s, project)):
            query = query.where(Idea.base_candidate_id.is_not(None))
        for _ in range(self._free_slots(s, project, cfg)):
            full = self._full(s, project, cfg)
            fits = query.where(or_(Idea.harness.is_(None), Idea.harness.not_in(full)))
            explored = self._explore(s, project, cfg, fits)
            idea, by_priority = (explored, explored) if explored is not None else self._pick(s, project, cfg, fits)
            if idea is None or not self._enqueue(s, project, cfg, idea, full, by_priority=by_priority):
                break

    def _park_strays(self, s: Session, project: Project, cfg: ProjectConfig) -> None:
        """Park each runnable idea whose track is not in ``piloop.toml``, with a note that says how to fix it.

        Track names are in the config, not in the schema. A track can be renamed or removed, or raw SQL can have a typo.
        """
        names = [t.name for t in cfg.all_tracks]
        strays = s.scalars(select(Idea).where(Idea.project_id == project.id, runnable(), Idea.track.not_in(names)))
        for idea in strays:
            idea.status = IdeaStatus.PARKED
            s.add(
                Comment(
                    target_kind=TargetKind.IDEA,
                    target_id=idea.id,
                    author=DAEMON_ACTOR,
                    kind=CommentKind.NOTE,
                    body=f'parked: its track {idea.track!r} is not in piloop.toml (tracks: {", ".join(names)}); set'
                    ' ideas.track to one of them, then set it ready',
                )
            )
            self._tracer.event('idea.parked', project=project.name, idea=idea.id, reason=f'unknown track {idea.track}')

    def _pick(
        self, s: Session, project: Project, cfg: ProjectConfig, query: Select[Idea]
    ) -> tuple[Idea | None, Idea | None]:
        """Return the most urgent idea of the track furthest below its share, and the pick by priority alone.

        A track's distance below its share is ``share × [roles.worker] max_concurrent`` minus its open runs. Only
        tracks with an idea in ``query`` take part, so a track with nothing runnable gives its slot to another track.
        Ties go to the track listed first. With one track, the two picks are the same.
        """
        first = s.scalars(query.order_by(Idea.priority, Idea.id).limit(1)).first()
        if first is None or not cfg.multitrack:
            return first, first
        open_runs = project_runs(project.id).where(Run.status.in_(OPEN_RUNS)).with_only_columns(Idea.track)
        running = Counter(s.scalars(open_runs))
        ready = set(s.scalars(query.with_only_columns(Idea.track)))
        slots = cfg.roles.worker.max_concurrent
        track = max((t for t in cfg.all_tracks if t.name in ready), key=lambda t: t.share * slots - running[t.name])
        idea = s.scalars(query.where(Idea.track == track.name).order_by(Idea.priority, Idea.id).limit(1)).first()
        return idea, first

    def _explore(self, s: Session, project: Project, cfg: ProjectConfig, query: Select[Idea]) -> Idea | None:
        """Return the most urgent runnable idea pinned to an alternative base if an exploration run is due, else None.

        An exploration run is due if none of the last ``explore_every - 1`` runs was a branch run. If no such idea is
        ready, the queue uses its usual order. Instructions to research alone do not make enough branch runs, so the
        daemon enforces this. It counts branch runs, not runs since the last exploration, so branch runs that research
        or the orchestrator pinned on their own also count.
        """
        every = cfg.roles.worker.explore_every
        if every == 0:
            return None
        latest = (
            select(Run.branch).join(Idea).where(Idea.project_id == project.id).order_by(Run.id.desc()).limit(every - 1)
        )
        recent = s.scalars(latest).all()
        if len(recent) < every - 1 or any(recent):
            return None
        pinned = s.scalars(query.where(Idea.base_candidate_id.is_not(None)).order_by(Idea.priority, Idea.id))
        trunk = head(s, project)
        return next((i for i in pinned if branches(s.get(Candidate, i.base_candidate_id), trunk)), None)

    def _enqueue(
        self,
        s: Session,
        project: Project,
        cfg: ProjectConfig,
        idea: Idea,
        full: Collection[str],
        *,
        by_priority: Idea | None,
    ) -> bool:
        """Queue one run of ``idea`` on its pinned candidate if it has one, else on the current head.

        A pin is a deliberate branch. ``run.queued`` then records ``pinned_base`` and ``computed_base``, so a wrong pin
        is easy to find. If the pin's bar (``ledger.bar_of``: the pin, or the nearest keep of a non-keep) is not the
        head, the run is a branch run (``runs.branch``) and is judged against that bar. ``run.queued`` also records
        ``track``, and ``priority_pick`` if the share rule took a different idea than priority order alone
        (``by_priority``). This shows how often the share rule changes the pick.

        Returns False, and queues nothing, if the idea pins no harness and every harness of the rotation is ``full``.
        """
        total_runs = count(s, project_runs(project.id))
        harnesses = cfg.role_harnesses(WorkerKind.WORKER)
        harness = idea.harness or free_harness(harnesses, harnesses[total_runs % len(harnesses)], full)
        if harness is None:
            return False
        computed = head(s, project)
        pin = idea.base_candidate_id
        pinned = s.get(Candidate, pin) if pin is not None else None
        base = pinned if pin is not None else computed
        if base is None:
            reason = f'pinned candidate#{pin} does not exist' if pin is not None else 'no kept candidate'
            self._tracer.event('idea.parked', project=project.name, idea=idea.id, reason=reason)
            idea.status = IdeaStatus.PARKED
            return True
        attempt = len(idea.runs) + 1
        model = cfg.role_models(WorkerKind.WORKER)[harness]
        run = Run(
            idea=idea,
            attempt=attempt,
            base_candidate_id=base.id,
            branch=branches(pinned, computed),
            harness=harness,
            model=model,
        )
        s.add(run)
        idea.status = IdeaStatus.SCHEDULED
        s.flush()
        payload: dict[str, object] = {'idea': idea.id, 'attempt': attempt, 'harness': harness, 'track': idea.track}
        if by_priority is not None and by_priority is not idea:
            payload['priority_pick'] = by_priority.id
        if pin is not None:
            payload |= {'pinned_base': base.id, 'computed_base': computed.id if computed is not None else None}
        emit(s, 'run.queued', TargetKind.RUN, run.id, payload=payload)
        s.add(
            Worker(
                kind=WorkerKind.WORKER,
                project_id=project.id,
                run_id=run.id,
                harness=harness,
                model=model,
                reason=f'idea#{idea.id} is ready',
            )
        )
        return True

    def _full(self, s: Session, project: Project, cfg: ProjectConfig) -> set[str]:
        """Return the harnesses at their ``max_concurrent`` cap, counting live agents and the workers of queued runs.

        A queued run's harness was chosen when it was queued (``_enqueue``), so its requested worker holds its place.
        A requested sweep gets its harness only when it is taken (``_take``).
        """
        if not cfg.max_concurrent:
            return set()
        queued = (Worker.state == WorkerState.REQUESTED) & (Worker.kind == WorkerKind.WORKER)
        on = select(Worker.harness).where(
            Worker.project_id == project.id, Worker.state.in_(LIVE) | queued, Worker.harness.in_(cfg.max_concurrent)
        )
        agents = Counter(s.scalars(on))
        return {harness for harness, cap in cfg.max_concurrent.items() if agents[harness] >= cap}

    def _take(self, s: Session, project: Project, cfg: ProjectConfig) -> list[tuple[int, int | None]]:
        """Mark requested rows ``starting``, up to each role's ``max_concurrent``. Refuse a worker without a run.

        A sweep's harness is chosen here. A harness at its cap (``_full``) is skipped for the next one in the rotation.
        A sweep pinned to that harness stays requested. A run's harness was chosen when it was queued (``_reroute``).

        Returns ``(worker_id, run_id)`` pairs. The run is what a worker executes. Nothing is taken while the startup
        breaker holds (``_paused``).
        """
        taken: list[tuple[int, int | None]] = []
        if self._paused(s, project):
            return taken
        full = self._full(s, project, cfg)
        for kind in WorkerKind:
            rc = cfg.role(kind)
            mine = (Worker.project_id == project.id, Worker.kind == kind)
            active = count(s, select(Worker).where(*mine, Worker.state.in_(LIVE)))
            requested = s.scalars(
                select(Worker)
                .where(*mine, Worker.state == WorkerState.REQUESTED)
                .order_by(Worker.id)
                .limit(max(rc.max_concurrent - active, 0))
            ).all()
            rotation = cfg.role_harnesses(kind)
            for worker in requested:
                if kind is WorkerKind.WORKER:
                    if worker.run_id is None:
                        self._close(s, worker, NO_RUN_ERROR)
                        continue
                    if not self._reroute(s, cfg, worker, rotation, full):
                        continue
                else:
                    harness = worker.harness or free_harness(rotation, rotate(s, worker, rotation), full)
                    if harness is None or harness in full:
                        continue
                    worker.harness = harness
                worker.state = WorkerState.STARTING
                taken.append((worker.id, worker.run_id))
                full = self._full(s, project, cfg)
        return taken

    def _reroute(self, s: Session, cfg: ProjectConfig, worker: Worker, rotation: list[str], full: set[str]) -> bool:
        """Return True if a run's worker can launch. Move it to the next free harness if its harness left the rotation.

        The harness was in the rotation when the run was queued (``_enqueue``), but ``piloop init`` can change the
        rotation later. A harness that the idea pins does not change. Returns False, and the worker stays requested,
        while every harness of the rotation is at its cap.
        """
        run = s.get_one(Run, worker.run_id)
        if run.idea.harness is not None or run.harness in rotation:
            return True
        harness = free_harness(rotation, rotate(s, worker, rotation), full)
        if harness is None:
            return False
        emit(s, 'run.rerouted', TargetKind.RUN, run.id, payload={'from': run.harness, 'to': harness})
        run.harness = worker.harness = harness
        run.model = worker.model = cfg.role_models(WorkerKind.WORKER)[harness]
        return True

    def _paused(self, s: Session, project: Project) -> bool:
        """Return True if ``STARTUP_BREAKER`` agents of the project failed at startup within ``STARTUP_WINDOW``.

        Requested rows wait, and launch when those failures are older than the window. The window is the back-off.
        Each pause posts one escalation (priority 1) with the load average and the failures' errors, which end with
        the agents' last screens. The escalation is updated in place while the pause holds, so an acknowledgement
        means that the human saw it. A new escalation is posted only after launches resume and pause again.
        """
        workers = select(Worker.id).where(Worker.project_id == project.id)
        deaths = s.scalars(
            select(Event)
            .where(Event.kind == STARTUP_ANOMALY, Event.target_kind == TargetKind.WORKER, Event.target_id.in_(workers))
            .where(Event.at >= utcnow() - STARTUP_WINDOW)
            .order_by(Event.id)
        ).all()
        latest = s.scalars(
            select(Event)
            .where(Event.kind.in_((LAUNCH_PAUSED, LAUNCH_RESUMED)))
            .where(Event.target_kind == TargetKind.PROJECT, Event.target_id == project.id)
            .order_by(Event.id.desc())
            .limit(1)
        ).first()
        pause = latest if latest is not None and latest.kind == LAUNCH_PAUSED else None
        if len(deaths) < STARTUP_BREAKER:
            if pause is not None:
                emit(s, LAUNCH_RESUMED, TargetKind.PROJECT, project.id, payload={'comment': pause.payload['comment']})
            return False
        escalation = s.get(Comment, pause.payload['comment']) if pause is not None else None
        if pause is None or escalation is None:
            self._escalate_pause(s, project, deaths)
        else:
            escalation.body = pause_body(deaths, str(pause.payload['load']))
        return True

    def _escalate_pause(self, s: Session, project: Project, deaths: Sequence[Event]) -> None:
        load = ' / '.join(f'{x:.1f}' for x in os.getloadavg())
        comment = Comment(
            target_kind=TargetKind.PROJECT,
            target_id=project.id,
            author=DAEMON_ACTOR,
            kind=CommentKind.ESCALATION,
            priority=1,
            body=pause_body(deaths, load),
        )
        s.add(comment)
        s.flush()
        payload: dict[str, object] = {'comment': comment.id, 'deaths': len(deaths), 'load': load}
        emit(s, LAUNCH_PAUSED, TargetKind.PROJECT, project.id, payload=payload)
        self._tracer.event(LAUNCH_PAUSED, project=project.name, deaths=len(deaths), load=load)

    def _sweep(self, worker_id: int) -> None:
        """Run a sweep (watchdog or research) in a detached worktree at the head, with a brief made from its row.

        Every sweep gets the project overview (``piloop.reports.overview``) in its context. A watchdog that got
        anomalies is an incident sweep. The anomalies come first and the research pack is left out (``piloop
        history`` prints it). Other watchdogs review progress and read the research pack (``piloop.reports.pack``),
        as research does.

        Research also gets the number of waiting seeds, its share of the backlog's shortfall, the runs since the
        last keep against the stall mark (``piloop.campaign.stop.stall``), and the time the pack was read. With that
        time, it can query for ideas that parallel sweeps inserted later. A scout sweep gets its seed cap, its search
        window, the watch list and the technique library instead (``piloop.agents.scout``). Nothing that a sweep
        commits in its worktree is scored. The commits are removed with the worktree when the agent ends.
        """
        with self._db.session() as s:
            worker = s.get_one(Worker, worker_id)
            project = s.get_one(Project, worker.project_id)
            cfg = config_of(project)
            role = role_of(s, worker)
            base = checkout_candidate(s, project)
            values = ', '.join(
                f'{t.primary.name}={"unmeasured" if (v := score(base, project.evaluator_id or 0, t.primary)) is None else v}'
                for t in cfg.all_tracks
            )
            now = utcnow()
            lines = [
                f'- Your cwd is a detached checkout of candidate#{base.id}, the head ({values}): read and run the code,'
                ' try things. Nothing you commit here is scored or kept; it goes with the checkout when you end.',
                f'- Shared workspace for notes and artifacts to other agents: `./{WORKSPACE}/` (not committed).',
            ]
            if role is SCOUT:
                seeds = cfg.roles.research.scout.seeds if cfg.roles.research.scout is not None else SCOUT_SEEDS
                lines += scout.context(s, project.id, worker_id, seeds, now)
            elif role.kind is WorkerKind.RESEARCH:
                backlog = self._backlog(s, project, cfg)
                most = cfg.roles.research.max_ideas
                own = max(min(backlog.short + most, most), 0)  # This sweep's share of the shortfall.
                seeds = count(s, select(Idea).where(Idea.project_id == project.id, Idea.status == IdeaStatus.SEED))
                without, (mark, why) = runs_without_keep(s, project), stall(cfg)
                stalled = ': stalled, so one of your own ideas pivots (section 3)' if without >= mark else ''
                lines += [
                    f'- Seeds to develop first: {seeds}, listed in the research pack oldest first.',
                    f'- Ideas of your own after the seeds: propose {own} (the backlog holds {backlog.held()};'
                    ' developed seeds do not count against it). Fewer only when what is left duplicates queued work;'
                    ' name that work in your finding.',
                    *(
                        [
                            '- Tracks short of their share of the backlog: '
                            + (
                                ', '.join(
                                    f'{t.name} {t.gap} ({t.runnable} of {t.target} runnable)'
                                    for t in backlog.tracks
                                    if t.gap > 0
                                )
                                or 'none'
                            )
                            + '. Give every idea and every developed seed the track it serves (`--track`); fill the'
                            ' short ones first.'
                        ]
                        if cfg.multitrack
                        else []
                    ),
                    f'- Finished runs since the last keep: {without}, of the {mark} that mark a stall ({why}){stalled}.',
                    f'- Sweep started at: {now.strftime("%Y-%m-%d %H:%M:%S")} (UTC; the research pack below is as of then).',
                ]
            incidents = assigned(s, worker_id) if role.kind is WorkerKind.WATCHDOG else []
            mode = None
            if role.kind is WorkerKind.WATCHDOG:
                mode = WatchdogMode.INCIDENT if incidents else WatchdogMode.PROGRESS
            sections = [*lines, '']
            if incidents:
                sections += ['## Incidents to investigate', *incidents, '']
            sections.append(overview(s, project, cfg, now, partial(self._jobs, s)))
            if mode is not WatchdogMode.INCIDENT:
                sections.append(pack(s, project, cfg, workspace=self._home / 'projects' / str(project.id) / WORKSPACE))
            brief = compose_brief(
                role,
                project,
                worker_id,
                reason=worker.reason or 'requested',
                context='\n'.join(sections),
                mode=mode,
                wake=lessons_overview(s, project, cfg.wake_budget(role.kind)),
            )
            project_path, project_id, base_sha = Path(project.path), project.id, base.commit_sha
        if self._executor.launcher.cancelled.is_set():  # stop() closed the row, so do not create a worktree.
            return
        log_dir = self._workers_dir / str(worker_id)
        repo = git.open_repo(project_path)
        wt_path = log_dir / 'worktree'
        links = worktree_links(cfg, project_path, self._home / 'projects' / str(project_id))

        def start() -> Outcome:
            git.add_worktree(repo, wt_path, base_sha, links)
            return self._executor.launcher.launch(worker_id, brief=brief, cwd=wt_path, log_dir=log_dir)

        _in_worktree(repo, wt_path, start)

    def _reattach_sweep(self, worker_id: int) -> None:
        """Watch the agent of a sweep that a previous daemon process started, in its worktree, until it ends."""
        log_dir = self._workers_dir / str(worker_id)
        with self._db.session() as s:
            repo = git.open_repo(Path(s.get_one(Project, s.get_one(Worker, worker_id).project_id).path))
        reattach = partial(self._executor.launcher.reattach, worker_id, cwd=log_dir / 'worktree', log_dir=log_dir)
        _in_worktree(repo, log_dir / 'worktree', reattach)

    def _collect(self) -> None:
        """Forget finished threads. If a thread died before its launch recorded anything, close its row here."""
        for worker_id, fut in list(self._futures.items()):
            if fut.done():
                del self._futures[worker_id]
                if (exc := fut.exception()) is not None:
                    self._tracer.event('worker.thread_died', worker=worker_id, error=str(exc))
                    self._close_dead(worker_id, f'{type(exc).__name__}: {exc}')
        self._scoring = {key: fut for key, fut in self._scoring.items() if not fut.done()}  # These never raise.

    def _close_dead(self, worker_id: int, error: str) -> None:
        with self._db.session() as s:
            self._close(s, s.get_one(Worker, worker_id), error)
            s.commit()

    def active(self) -> list[int]:
        """Return the ids of the workers that are executing now."""
        self._collect()
        return sorted(self._futures)

    def serve_forever(self, tick_seconds: float) -> None:
        """Tick until ``stop`` or ``interrupt`` is called."""
        while not self._stop.is_set():
            self.tick()
            self._stop.wait(tick_seconds)

    def interrupt(self) -> None:
        """Make ``serve_forever`` return after the current tick. Idempotent, so it is safe in a signal handler."""
        self._stop.set()

    def wait(self, timeout: float) -> None:
        """Wait up to ``timeout`` seconds for the workers in flight to finish."""
        wait([*self._futures.values(), *self._scoring.values()], timeout=timeout)
        self._collect()

    def hand_over(self, timeout: float) -> None:
        """Stop ticking and leave every agent running for the next daemon process. This is an in-place restart.

        As with ``stop``, nothing starts after this and the evaluators are killed. But no agent is killed and no row is
        written. The next daemon's ``reap`` re-attaches to the agents and resumes the scoring that stops here. This
        waits up to ``timeout`` for the threads that do not watch an agent. If an agent ends meanwhile, the next daemon
        also handles it.
        """
        self._stop.set()
        self._executor.hand_over()
        watching = set(self._executor.launcher.watching)
        rest = [future for worker_id, future in self._futures.items() if worker_id not in watching]
        wait([*rest, *self._scoring.values()], timeout=timeout)
        self._pool.shutdown(wait=False, cancel_futures=True)

    def stop(self, timeout: float) -> None:
        """Stop ticking, kill the workers in flight, fail their runs and release the thread pool.

        The executor is cancelled first. After that, every agent is either recorded or never starts, and the evaluator
        child processes are dead. The rows are written here, as ``reap`` does for a crashed daemon. The threads remove
        their own worktrees. ``timeout`` limits the wait for them.
        """
        self._stop.set()
        self._executor.cancel()
        self._fail_open(STOPPED_ERROR, remove_worktrees=False)
        self.wait(timeout)
        self._pool.shutdown(wait=False, cancel_futures=True)


def _in_worktree(repo: Repo, wt_path: Path, agent: Callable[[], Outcome]) -> None:
    """Run a sweep's agent in its worktree, then remove the worktree, unless the agent was handed over."""
    with ExitStack() as cleanup:
        cleanup.callback(git.remove_worktree, repo, wt_path)
        if agent().handed_over:
            cleanup.pop_all()


def branches(pinned: Candidate | None, computed: Candidate | None) -> bool:
    """Return True if a run on ``pinned`` is a branch run: the pinned base's bar is not the current head.

    A pin to a non-keep whose nearest keep is the head is not a branch. Its run is judged against the head
    (``ledger.bar_of``).
    """
    return pinned is not None and (computed is None or bar_of(pinned).id != computed.id)


def steer_line(comment: Comment) -> str:
    """Format the one line that a running worker gets for a comment: who wrote what, and how to read the rest.

    The line quotes the first line of the body, cut at ``STEER_LINE_CHARS``. The worker can read the full text with
    the SQL tool that its brief names, so a long decision does not fill the pane.
    """
    body = comment.body.strip()
    first = body.splitlines()[0] if body else ''
    if len(first) > STEER_LINE_CHARS:
        first = first[: STEER_LINE_CHARS - 1] + '…'
    return (
        f'[piloop] {comment.kind} on {comment.target_kind}#{comment.target_id} by {comment.author}: {first}'
        f' — full text: SELECT body FROM comments WHERE id={comment.id}'
    )


def pause_body(deaths: Sequence[Event], load: str) -> str:
    """Format the pause escalation: the number of failures, the load average and the latest failures' errors."""
    minutes = STARTUP_WINDOW.total_seconds() / 60
    lines = [
        f'Launches paused: {len(deaths)} agents died at startup in the last {minutes:g} minutes. Load average'
        f' {load} on {os.cpu_count()} CPUs when launches paused. Launches resume automatically when fewer than'
        f' {STARTUP_BREAKER} deaths are that recent. If the host is overloaded, stop what overloads it. The latest'
        ' deaths:',
        *(
            f'- worker#{e.target_id} ({e.payload.get("kind")} on {e.payload.get("harness")}): {e.payload.get("error")}'
            for e in deaths[-STARTUP_BREAKER:]
        ),
    ]
    return '\n'.join(lines)


def free_harness(rotation: Sequence[str], turn: str, full: Collection[str]) -> str | None:
    """Return ``turn``, the rotation's pick, unless it is ``full``. Then return the first harness that is not full."""
    return turn if turn not in full else next((h for h in rotation if h not in full), None)
