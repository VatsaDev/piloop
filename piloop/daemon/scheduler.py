"""The daemon loop: check each role's triggers for each campaign, launch requested workers and reap stale state.

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
import sys
import threading
from collections import Counter
from collections.abc import Callable, Collection, Sequence
from concurrent.futures import Future, ThreadPoolExecutor, wait
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path

from git import Repo
from git.exc import GitError, InvalidGitRepositoryError, NoSuchPathError
from pydantic import ValidationError
from sqlalchemy import Boolean, ColumnElement, Select, delete, exists, func, or_, select
from sqlalchemy.orm import Session

from piloop import git
from piloop.agents import scout
from piloop.agents.launcher import STARTUP_ANOMALY, STOP_ENDED, STOPPED_ERROR, WRAP_UP, rotate
from piloop.agents.refill import usage_of
from piloop.agents.roles import compose_brief, role_of
from piloop.campaign import spend
from piloop.campaign.campaigns import WORKSPACE, campaign_dir, workspace_dir, worktree_links
from piloop.campaign.ideas import defined_by, runnable, supersede, waiting
from piloop.campaign.memory import sync_documents
from piloop.campaign.spend import SpendError
from piloop.campaign.stop import (
    BLOCKED,
    CAMPAIGN_STOPPED_ERROR,
    CANCEL_REQUESTED,
    CANCELLED_ERROR,
    ROLE_REMOVED,
    STALE_ERROR,
    agent_cancel,
    attempts,
    park_cancelled,
    requested_stop,
    runs,
    runs_without_keep,
    stall,
    stop_reason,
)
from piloop.config import (
    CampaignConfig,
    ClosingReview,
    Files,
    ResearchRole,
    ScoutRole,
    Spend,
    SweepRole,
    Workdir,
    WorkerRole,
    config_of,
    ideas_taken,
)
from piloop.daemon.archiver import Archiver, Archiving
from piloop.daemon.run_executor import RunExecutor, left_finding, open_escalation
from piloop.db.events import ANOMALY_EVENT, emit
from piloop.db.models import (
    LIVE,
    OPEN_RUNS,
    Campaign,
    CampaignStatus,
    Candidate,
    Comment,
    CommentKind,
    Evaluation,
    Event,
    Idea,
    IdeaStatus,
    Pause,
    Run,
    RunStatus,
    TargetKind,
    Worker,
    WorkerState,
    campaign_runs,
    count,
    utcnow,
)
from piloop.db.session import Database
from piloop.db.triggers import DAEMON_ACTOR
from piloop.otel import Tracer
from piloop.reports.overview import anomalies, assigned, overview, reported
from piloop.reports.pack import pack
from piloop.reports.wake import lessons_overview
from piloop.runtime import Outcome, WorkerStatus
from piloop.runtime.factory import RuntimeFactory, handle_for, make_runtime, runtime_for
from piloop.scoring.ledger import bar_of, checkout_candidate, due, head, head_unmeasured, rejudges, score
from piloop.text import cell
from piloop.triggers import Backlog, Fire, Incident, LowWaterTrigger, TrackBacklog
from piloop.usage import Usage

NO_RUN_ERROR = 'a worker needs a run; the scheduler requests them from ready ideas'
PUSHED_KINDS = tuple(kind for kind in CommentKind if kind is not CommentKind.ESCALATION)
"""Comment kinds that are sent to a running worker. Escalations are not sent, because they are for the human."""
SETTLED = ('comment.delivered', 'comment.undelivered')
"""Events on a worker that record a comment as sent or failed. The comment id is in the payload."""
STEER_LINE_CHARS = 200
"""The number of characters of a comment's first line that a ``[piloop]`` line quotes. The worker can query the rest."""
STOP_GRACE_SECONDS = 600
"""The seconds that a worker that was told to wrap up has to commit its work before the daemon ends it."""
WRAP_UP_PROMPT = (
    '[piloop] {what} ({cause}). Commit what you have now, with a message that says where it stands.'
    ' Leave your finding and stop. This session ends in {minutes:g} minutes.'
)
JOBS_LEFT = 'worker.jobs_left'
"""Event on a worker that left its finding and went idle while jobs that it started still ran.

The daemon told it which jobs keep its run open (``JOBS_LEFT_PROMPT``). The payload has ``jobs``, and ``error`` when the
prompt failed. It is sent once per worker."""
JOBS_LEFT_PROMPT = (
    '[piloop] You left your finding, but your run stays open while these jobs that you started still run: {jobs}.'
    ' Kill the ones that you no longer need: your run ends when you stop with nothing left running.'
)
JOBS_LEFT_SHOWN = 5
"""How many of the jobs the prompt names. Each is ``<pid> <command line>``, cut to ``STEER_LINE_CHARS``."""
STARTUP_BREAKER = 3
"""The number of a campaign's agents that can fail at startup within ``STARTUP_WINDOW`` before its launches pause.

A host that is too loaded to start agents makes every new agent fail, and each failure costs a start window."""
STARTUP_WINDOW = timedelta(minutes=10)
LAUNCH_PAUSED = 'launch.paused'
"""Event on a campaign whose launches the startup breaker paused. ``comment`` in the payload is the escalation."""
LAUNCH_RESUMED = 'launch.resumed'
"""Event on a campaign whose paused launches resumed. It ends the pause that ``LAUNCH_PAUSED`` started."""


class Scheduler:
    """The daemon loop. There is one instance per daemon. ``tick`` is safe to call from a loop or from tests."""

    def __init__(
        self,
        db: Database,
        executor: RunExecutor,
        tracer: Tracer,
        home: Path,
        runtime_factory: RuntimeFactory = make_runtime,
        archiving: Archiving | None = None,
    ) -> None:
        self._db = db
        self._executor = executor
        self._tracer = tracer
        self._home = home
        self._runs_dir = home / 'runs'
        self._workers_dir = home / 'workers'
        self._runtime_factory = runtime_factory
        # No bound: a run's thread waits in the evaluation queue while it scores, and a bound would let those waits
        # hold back agent launches. The roles' max_concurrent and max_evaluations bound the work itself.
        self._pool = ThreadPoolExecutor(max_workers=sys.maxsize, thread_name_prefix='agent')
        self._futures: dict[int, Future[None]] = {}
        self._scoring: dict[tuple[TargetKind, int], Future[None]] = {}
        """Scoring outside a worker: a run's resumed scoring by run id, or a campaign's re-measure by campaign id."""
        self._stop = threading.Event()
        self._archiver = (
            Archiver(db, archiving.store, home, archiving.policy, tracer, self._pool, self._stop)
            if archiving is not None
            else None
        )
        """Archives the workspaces in the background. The daemon always passes ``archiving``; a test that does not
        archives nothing."""

    def reap(self) -> None:
        """Re-attach the agents that a previous daemon process left. Fail everything else it left in flight.

        The daemon watches a ``running`` agent whose worktree still exists until it ends, even if it ended already. Then
        it finishes the run as if no daemon had stopped (``RunExecutor.reattach``, or ``_reattach_sweep`` for a sweep).
        Both an in-place restart (``hand_over``) and a crash leave agents for this. A closed run whose commits were not
        all scored (``runs.scoring``) is scored now (``RunExecutor.resume``), whatever its idea's status. An idea that
        stayed ``scheduled`` does not run again, and no dependent idea takes it before its keeps are known. The
        worktrees of failed runs and sweeps are removed. The evaluation queue starts empty: what still needs a
        measurement asks for it again.
        """
        with self._tracer.span('piloop.reap'):
            with self._db.session() as s:  # their evaluators died with the daemon that ran them
                s.execute(delete(Evaluation))
                s.commit()
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
                unscored = s.scalars(select(Run.id).where(Run.scoring).order_by(Run.id)).all()
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
            checkout = self._in_campaign(s, worker)
            return (checkout or self._workers_dir / str(worker.id) / 'worktree').is_dir()
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
                    self._remove_worktrees(self._runs_dir / str(run.id), Path(run.idea.campaign.path))
            for worker in s.scalars(select(Worker).where(Worker.state.in_(LIVE), ~spared)):
                # A ``starting`` row that stop() reached before its start window has no process.
                if worker.runtime is not None:
                    self._kill_stale(s, worker)
                if remove_worktrees and worker.run_id is None and worker.campaign_id is not None:
                    campaign_path = Path(s.get_one(Campaign, worker.campaign_id).path)
                    self._remove_worktrees(self._workers_dir / str(worker.id), campaign_path)
                self._close(s, worker, error)
            s.commit()

    def _fail_run(self, s: Session, run: Run, error: str) -> None:
        """Fail ``run`` and return its idea to the queue, unless a human parked or rejected it meanwhile.

        A run that the daemon stopped (``stop.NOT_ATTEMPTS``) is not an attempt, so its idea always goes back to
        ``ready``. A running run whose worker left its finding is not failed: it is handed off (``_hand_off``).
        """
        worker = s.scalars(select(Worker).where(Worker.run_id == run.id).order_by(Worker.id.desc()).limit(1)).first()
        if run.status is RunStatus.RUNNING and worker is not None and left_finding(s, run, worker):
            self._hand_off(s, run)
            return
        run.status, run.error, run.finished_at = RunStatus.FAILED, error, utcnow()
        emit(s, 'run.finished', TargetKind.RUN, run.id, payload={'status': run.status, 'error': error})
        idea = run.idea
        if idea.status is IdeaStatus.SCHEDULED and error == CANCELLED_ERROR:
            park_cancelled(s, run)
        elif idea.status is IdeaStatus.SCHEDULED:
            idea.status = IdeaStatus.READY if attempts(idea) < idea.max_runs else IdeaStatus.DONE
        self._tracer.event('run.aborted', run=run.id, error=error)

    def _hand_off(self, s: Session, run: Run) -> None:
        """Close a run whose worker left its finding as finished, and leave its commits to be scored on the next start.

        The worker reported that it is done. A job of its own may still keep the run open, but the run is not cut: its
        idea stays ``scheduled``, so no second run redoes the work. The run thread pins the commits
        (``RunExecutor._keep_cut_work``), and ``reap`` scores them and settles the idea (``runs.scoring``). Its status is
        what the run's end would give: succeeded, or failed as blocked while its worker's escalation waits.
        """
        asked = open_escalation(s, run)
        status, error = (
            (RunStatus.SUCCEEDED, None) if asked is None else (RunStatus.FAILED, f'{BLOCKED} comment#{asked}')
        )
        run.status, run.error, run.finished_at, run.scoring = status, error, utcnow(), True
        emit(s, 'run.finished', TargetKind.RUN, run.id, payload={'status': status, 'error': error})
        self._tracer.event('run.handed_off', run=run.id, status=status)

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

    def _remove_worktrees(self, agent_dir: Path, campaign_path: Path) -> None:
        """Remove the worktrees under a run's or a sweep's directory. If the campaign is gone, record an event."""
        try:
            repo = git.open_repo(campaign_path)
        except NoSuchPathError, InvalidGitRepositoryError:
            self._tracer.event('worktrees.orphaned', agent_dir=str(agent_dir), campaign_path=str(campaign_path))
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
        """Send new comments to running workers, run every campaign's triggers and launch requested workers.

        Returns the ids of the launched workers. Before the triggers, the tick rejects the ideas that a newer idea
        superseded (``piloop.campaign.ideas.supersede``). This also happens for a stopped campaign, which requests
        nothing and is measured again only on request (``ledger.due``). A campaign that the spend guard holds
        (``_read_spend``) gets only this step. A paused campaign (``piloop pause``) gets no triggers and launches
        nothing, and with ``all`` it is not measured again either. Both still stop on a stop request
        (``requested_stop``). A stopped campaign launches its closing review, and what a human requests, whatever
        the pause; while the spend guard holds, as ``[spend] closing_review`` says (``launches``). The
        archiver starts the next archiving of a workspace that is due, or its upkeep, on a background thread
        (``piloop.daemon.archiver``).
        """
        self._collect()
        self._executor.stop_cancelled()
        self._push_comments()
        self._report_jobs()
        self._wind_down()
        self._read_spend()
        if self._archiver is not None:
            self._archiver.tick()
        launched: list[tuple[int, int | None]] = []
        with self._db.session() as s:
            for campaign in s.scalars(select(Campaign)).all():
                try:
                    cfg = config_of(campaign)
                except ValidationError as e:
                    self._tracer.event('campaign.invalid_config', campaign=campaign.name, error=str(e))
                    continue
                for idea_id, by in supersede(s, campaign.id).items():
                    self._tracer.event('idea.superseded', campaign=campaign.name, idea=idea_id, by=by)
                held = spend.held(campaign, cfg)
                if held is None and campaign.paused is not Pause.ALL:
                    self._remeasure(s, campaign)
                # A stop request goes through the spend guard and a pause.
                go = held is None and campaign.paused is None
                if campaign.status is CampaignStatus.ACTIVE and (go or requested_stop(s, campaign) is not None):
                    self._triggers(s, campaign, cfg, held)
                s.flush()
                if launches(campaign, cfg, held):
                    launched += self._take(s, campaign, cfg)
            s.commit()
        for worker_id, run_id in launched:
            body = partial(self._executor.execute, run_id, worker_id) if run_id else partial(self._sweep, worker_id)
            self._futures[worker_id] = self._pool.submit(body)
        return [worker_id for worker_id, _ in launched]

    def _read_spend(self) -> None:
        """Run the ``[spend] command`` of each campaign whose ``interval`` is up, and record the result.

        That is each active campaign, and each stopped one whose closing review waits for the hold to end
        (``spend.review_waits``).

        This runs outside any transaction, because the command can take up to ``spend.TIMEOUT``. A write lock held for
        that long locks all other clients out. While ``spend.held`` returns a reason, the tick does not request,
        re-measure or launch for the campaign. Work in progress continues.
        """
        with self._db.session() as s:
            todo: list[tuple[int, Path, Spend]] = []
            for campaign in s.scalars(select(Campaign)):
                try:
                    cfg = config_of(campaign)
                except ValidationError:
                    continue  # The tick records this error for each campaign.
                if cfg.spend is None or not spend.due(campaign, cfg.spend):
                    continue
                if campaign.status is CampaignStatus.ACTIVE or spend.review_waits(s, campaign, cfg):
                    todo.append((campaign.id, Path(campaign.path), cfg.spend))
        for campaign_id, path, budget in todo:
            try:
                reading: float | SpendError = spend.read(budget.command, path)
            except SpendError as e:
                reading = e
            with self._db.session() as s:
                spend.record(s, s.get_one(Campaign, campaign_id), budget, reading)
                s.commit()
            self._tracer.event('spend.read', campaign=campaign_id, reading=str(reading))

    def _remeasure(self, s: Session, campaign: Campaign) -> None:
        """Measure what the current evaluator must measure (``ledger.due``), one candidate at a time per campaign.

        After an evaluator change, no candidate has a score from the new evaluator. The head is measured first, and runs
        that are not pinned wait until it has a score (``_queue``). Then the bars that runs need, and the candidates
        that someone asked to measure again. Older keeps are measured again only when someone asks. A candidate that
        someone measured by hand is judged again first (``rejudges``), in the same slot: a rebase can measure its merge.
        """
        key = (TargetKind.CAMPAIGN, campaign.id)
        if key in self._scoring:
            return
        if again := rejudges(s, campaign):
            self._tracer.event('candidate.rejudge', campaign=campaign.name, candidate=again[0].id, left=len(again))
            self._scoring[key] = self._pool.submit(self._executor.rejudge, again[0].id)
            return
        if not (todo := due(s, campaign)):
            return
        self._tracer.event('candidate.remeasure', campaign=campaign.name, candidate=todo[0].id, left=len(todo))
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
            select(Worker).where(Worker.state == WorkerState.RUNNING, Worker.run_id.is_not(None)).order_by(Worker.id)
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
        own = (f'{worker.kind}#{worker.id}', f'run#{run.id}')
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

    def _report_jobs(self) -> None:
        """Tell each idle worker that left its finding which of its jobs still keep its run open. Once per worker.

        An idle agent is finished only when nothing that it started still runs (``Runtime.wait``). That keeps a run
        open while its training job works. But a worker that left its finding believes that it is done, and it cannot
        see what the runtime sees: a job that it forgot, or a waiter that never ends, such as a ``pgrep -f`` loop that
        finds its own command line. ``JOBS_LEFT`` on the worker records the prompt, also when it failed. A runtime that
        reports no jobs (the subprocess runtime) never gets this prompt.
        """
        with self._db.session() as s:
            for worker in self._running_workers(s):
                told = select(Event.id).where(
                    Event.target_kind == TargetKind.WORKER, Event.target_id == worker.id, Event.kind == JOBS_LEFT
                )
                if s.scalar(told) is not None or not left_finding(s, s.get_one(Run, worker.run_id), worker):
                    continue
                if not (jobs := self._jobs(s, worker)):
                    continue
                runtime, handle = runtime_for(s, worker, self._runs_dir, self._runtime_factory), handle_for(worker)
                if runtime.status(handle) is not WorkerStatus.IDLE:  # it may still stop them in this turn
                    continue
                payload: dict[str, object] = {'jobs': jobs}
                named = '; '.join(cell(job, STEER_LINE_CHARS) for job in jobs[:JOBS_LEFT_SHOWN])
                try:
                    runtime.prompt(handle, JOBS_LEFT_PROMPT.format(jobs=named))
                except Exception as e:  # NotSteerable, a blocked agent or a closed pane. The daemon goes on.
                    payload['error'] = str(e)
                emit(s, JOBS_LEFT, TargetKind.WORKER, worker.id, payload=payload)
                self._tracer.event(JOBS_LEFT, worker=worker.id, jobs=len(jobs))
            s.commit()

    def _wind_down(self) -> None:
        """Wind down the workers of stopped campaigns and the agents that someone cancelled (``CANCEL_REQUESTED``).

        A worker of a stopped campaign is told to commit and stop (``WRAP_UP``), and ended after the grace period
        (``STOP_GRACE_SECONDS``), or at once when the stop request asked to ``kill``. The runs then close and their
        commits are scored. A worker in the middle of a turn takes the prompt only when the turn ends, which can be
        after the grace period. A worker that the daemon ends (``STOP_ENDED``) without its finding closes its run as
        cut by the stop (``CAMPAIGN_STOPPED_ERROR``), not as a failed attempt. Sweeps of a stopped campaign are not
        changed, because their timeouts limit them and the closing review is one of them.

        A cancelled run or agent of any role (``agent_cancel``) is ended at once, or after the wrap-up prompt and the
        grace period when the cancel asked for it. Its run closes with ``CANCELLED_ERROR``. A cancelled request never
        starts, and its queued run fails. Both park the run's idea. A starting agent is left for a later tick, once it
        runs; if its start fails, the run is not launched again (``RunExecutor._relaunch``).
        """
        with self._db.session() as s:
            requested = select(Event.target_id).where(Event.kind.in_(CANCEL_REQUESTED))
            on_agents = requested.where(Event.target_kind == TargetKind.WORKER)
            on_runs = requested.where(Event.target_kind == TargetKind.RUN)
            runs_of_agents = select(Worker.run_id).where(Worker.id.in_(on_agents), Worker.run_id.is_not(None))
            cancelled = Worker.state.in_((WorkerState.REQUESTED, WorkerState.RUNNING)) & (
                Worker.id.in_(on_agents) | Worker.run_id.in_(on_runs) | Worker.run_id.in_(runs_of_agents)
            )
            stopping = (
                select(Worker.id)
                .join(Campaign, Worker.campaign_id == Campaign.id)
                .where(Worker.run_id.is_not(None), Worker.state == WorkerState.RUNNING)
                .where(Campaign.status == CampaignStatus.STOPPED)
            )
            workers = s.scalars(select(Worker).where(cancelled | Worker.id.in_(stopping)).order_by(Worker.id)).all()
            for worker in workers:
                if (cancel := agent_cancel(s, worker)) is not None:
                    reason, error = f'cancelled by {cancel.actor}', CANCELLED_ERROR
                    what = 'Your run was cancelled' if worker.run_id is not None else 'This sweep was cancelled'
                    cause, wrap_up = f'by {cancel.actor}', bool(cancel.payload.get('wrap_up'))
                else:
                    campaign = s.get_one(Campaign, worker.campaign_id)
                    reason, error = campaign.stopped_reason or CAMPAIGN_STOPPED_ERROR, CAMPAIGN_STOPPED_ERROR
                    what, cause, request = 'The campaign stopped', reason, requested_stop(s, campaign)
                    wrap_up = request is None or not request.payload.get('kill')
                if worker.state is WorkerState.REQUESTED:
                    self._close(s, worker, error)
                    if worker.run_id is not None:
                        self._fail_run(s, s.get_one(Run, worker.run_id), error)
                    continue
                told = s.scalar(
                    select(Event.at).where(
                        Event.target_kind == TargetKind.WORKER, Event.target_id == worker.id, Event.kind == WRAP_UP
                    )
                )
                if not wrap_up:
                    self._end(s, worker, reason, error)
                elif told is None:
                    text = WRAP_UP_PROMPT.format(what=what, cause=cause, minutes=STOP_GRACE_SECONDS / 60)
                    self._wrap_up(s, worker, reason, text)
                elif utcnow() - told >= timedelta(seconds=STOP_GRACE_SECONDS):
                    self._end(s, worker, reason, error)
            s.commit()

    def _wrap_up(self, s: Session, worker: Worker, reason: str, text: str) -> None:
        """Send the wrap-up prompt. The grace period starts even if the prompt fails, as for a headless worker."""
        payload: dict[str, object] = {'reason': reason}
        try:
            runtime_for(s, worker, self._runs_dir, self._runtime_factory).prompt(handle_for(worker), text)
        except Exception as e:  # NotSteerable (subprocess), a blocked agent or a closed pane.
            payload['error'] = str(e)
        emit(s, WRAP_UP, TargetKind.WORKER, worker.id, payload=payload)
        self._tracer.event(WRAP_UP, worker=worker.id, **{k: str(v) for k, v in payload.items()})

    def _end(self, s: Session, worker: Worker, reason: str, error: str) -> None:
        """Kill a wound-down agent, after ``STOP_ENDED``. The kill is tried again on each tick until the agent is gone."""
        ended = select(Event.id).where(
            Event.target_kind == TargetKind.WORKER, Event.target_id == worker.id, Event.kind == STOP_ENDED
        )
        if s.scalar(ended) is None:
            emit(s, STOP_ENDED, TargetKind.WORKER, worker.id, payload={'reason': reason, 'error': error})
            s.commit()  # before the kill: the run that the kill ends reads it when it closes
            self._tracer.event('worker.ended', worker=worker.id, reason=reason)
        self._kill_stale(s, worker)

    def _triggers(self, s: Session, campaign: Campaign, cfg: CampaignConfig, held: str | None) -> None:
        """Stop the campaign if a ``[stop]`` condition holds. Otherwise queue runs and check every role's triggers.

        ``held`` is why the spend guard holds launches: then only a stop request gets here. ``[spend] closing_review
        = "skip"`` then requests no closing review and posts an escalation instead.

        Triggers requested everything that is pending when the campaign stops, so it is all cancelled. Requested rows
        are closed and queued runs fail with ``CAMPAIGN_STOPPED_ERROR``. Their ideas go back to ``ready``. The
        ``[memory]`` documents are synced, so that the closing sweep can search the campaign's reports. Then the
        ``stopped`` triggers fire once (``Trigger.ON_STOP``): by default, the watchdog's closing review. A stopped
        campaign gets nothing more from the triggers. Rows that a human inserts after the stop still launch. Workers in
        flight are told to stop (``_wind_down``), and their commits are scored.
        """
        reason = stop_reason(s, campaign, cfg)
        if reason is None:
            self._queue(s, campaign, cfg)
            for name, role in cfg.roles.sweeping().items():
                self._request(s, campaign, cfg, name, role)
            return
        campaign.status, campaign.stopped_reason = CampaignStatus.STOPPED, reason
        self._tracer.event('campaign.stopped', campaign=campaign.name, reason=reason)
        self._drain(s, campaign)
        try:
            with s.begin_nested():
                synced = sync_documents(s, campaign, cfg, DAEMON_ACTOR)
        except (OSError, GitError) as e:  # the repo could not be read: the stop and the closing review still happen
            self._tracer.event('documents.sync_failed', campaign=campaign.name, error=f'{type(e).__name__}: {e}')
        else:
            self._tracer.event('documents.synced', campaign=campaign.name, result=synced.line())
        if held is not None and cfg.spend is not None and cfg.spend.closing_review is ClosingReview.SKIP:
            spend.review_skipped(s, campaign, held)
            return
        for name, role in cfg.roles.sweeping().items():
            self._request(s, campaign, cfg, name, role, stopping=True)

    def _drain(self, s: Session, campaign: Campaign) -> None:
        mine = Idea.campaign_id == campaign.id
        for run in s.scalars(select(Run).join(Idea).where(mine, Run.status == RunStatus.QUEUED)):
            self._fail_run(s, run, CAMPAIGN_STOPPED_ERROR)
        for worker in s.scalars(
            select(Worker).where(Worker.campaign_id == campaign.id, Worker.state == WorkerState.REQUESTED)
        ):
            self._close(s, worker, CAMPAIGN_STOPPED_ERROR)

    def _request(
        self, s: Session, campaign: Campaign, cfg: CampaignConfig, name: str, role: SweepRole, *, stopping: bool = False
    ) -> None:
        """Insert requested rows when the role's triggers fire. One batch per call, however many triggers fire.

        The triggers fire in the order of the role's list (``piloop.triggers``): those with ``ON_STOP`` when the
        campaign is ``stopping``, else the others. A fire that needs a row of its
        own (``Fire.alone``) is requested alone when it comes first, and otherwise waits for a later tick. The others
        share one batch: the largest count of rows, with every reason. The first row gets the events of the fires. A
        trigger that raises is traced and does not fire.
        """
        latest = s.scalars(
            select(Worker)
            .where(Worker.campaign_id == campaign.id, Worker.kind == name)
            .order_by(Worker.id.desc())
            .limit(1)
        ).first()
        if latest is not None and latest.state is WorkerState.REQUESTED:
            return  # One pending request per role is enough.
        tick = self._tick(s, campaign, cfg, name, role)
        fires: list[Fire] = []
        for trigger in (t for t in role.triggers if t.ON_STOP is stopping):
            try:
                fire = trigger.fire(tick)
            except Exception as e:  # A trigger of a package, or a query that the schema no longer runs.
                self._tracer.event(
                    'trigger.failed', campaign=campaign.name, role=name, trigger=trigger.kind, error=str(e)
                )
                continue
            if fire is not None:
                fires.append(fire)
        if not fires:
            return
        fires = fires[:1] if fires[0].alone else [f for f in fires if not f.alone]
        count = max(f.count for f in fires)
        reason = '; '.join(f.reason for f in fires)
        rows = [Worker(kind=name, campaign_id=campaign.id, reason=reason) for _ in range(count)]
        s.add_all(rows)
        s.flush()
        for fire in fires:
            for kind, payload in fire.events:
                emit(s, kind, TargetKind.WORKER, rows[0].id, payload=payload)
        self._tracer.event('worker.requested', campaign=campaign.name, kind=name, reason=reason, count=count)

    def _tick(self, s: Session, campaign: Campaign, cfg: CampaignConfig, name: str, role: SweepRole) -> RoleTick:
        """Return what the triggers of role ``name`` read in this tick."""
        last = s.scalars(
            select(Worker)
            .where(Worker.campaign_id == campaign.id, Worker.kind == name)
            .order_by(Worker.id.desc())
            .limit(1)
        ).first()
        return RoleTick(
            s=s,
            campaign=campaign,
            role=name,
            last=last,
            max_concurrent=role.max_concurrent,
            interval=role.interval,
            backlog=partial(self._backlog, s, campaign, cfg, name),
            incidents=partial(self._incidents, s, campaign, cfg),
        )

    def _incidents(self, s: Session, campaign: Campaign, cfg: CampaignConfig) -> list[Incident]:
        """Return the campaign's incident anomalies that no agent has received yet, oldest first."""
        given = reported(s, campaign.id)
        now = utcnow()
        found = [
            Incident(a.key, a.line(now))
            for a in anomalies(s, campaign, cfg, now, partial(self._jobs, s))
            if a.incident and a.key not in given
        ]
        return found[::-1]

    def _jobs(self, s: Session, worker: Worker) -> list[str]:
        """Return the jobs that the worker's runtime reports in flight, or none if the runtime fails (traced)."""
        try:
            return runtime_for(s, worker, self._runs_dir, self._runtime_factory).jobs(handle_for(worker))
        except Exception as e:  # A Herdr error must not stop the tick. The next tick checks the anomaly again.
            self._tracer.event('worker.jobs_failed', worker=worker.id, error=str(e))
            return []

    def _backlog(self, s: Session, campaign: Campaign, cfg: CampaignConfig, name: str, buffer: int) -> Backlog:
        """Return the campaign's backlog as role ``name`` fills it: ``buffer`` beyond the run slots, its own sweeps."""
        target = cfg.run_slots + buffer
        if cfg.stop.max_runs is not None:
            target = min(target, cfg.stop.max_runs - runs(s, campaign))
        sweeps = count(
            s,
            select(Worker).where(
                Worker.campaign_id == campaign.id,
                Worker.kind == name,
                Worker.state.in_(LIVE),
            ),
        )
        ready, blocked = (
            Counter(s.scalars(select(Idea.track).where(Idea.campaign_id == campaign.id, which())))
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
            free=self._free_slots(s, campaign, cfg, cfg.roles.doing_runs()),
            sweeps=sweeps,
            per_sweep=cfg.roles.sweeping()[name].max_ideas,
            on_hold=head_unmeasured(s, campaign),
        )

    def _free_slots(self, s: Session, campaign: Campaign, cfg: CampaignConfig, roles: dict[str, WorkerRole]) -> int:
        """Return the runs that ``roles`` can start now.

        For each role, this is its ``max_concurrent`` minus its open runs. The sum is never more than ``max_runs``
        allows.
        """
        open_runs = campaign_runs(campaign.id).where(Run.status.in_(OPEN_RUNS))
        free = sum(
            max(role.max_concurrent - count(s, open_runs.where(done_by(name))), 0) for name, role in roles.items()
        )
        if cfg.stop.max_runs is not None:
            free = min(free, cfg.stop.max_runs - runs(s, campaign))
        return max(free, 0)

    def _queue(self, s: Session, campaign: Campaign, cfg: CampaignConfig) -> None:
        """Queue a run, and request its agent, for each free slot with a runnable idea, within ``[stop] max_runs``.

        Each role that does runs fills its own slots with the ideas that it takes (``takes``), in the order of
        ``Roles.doing_runs``. Its open runs count against its own ``max_concurrent``. Its ``explore_every`` and the
        track shares apply to its own runs. ``max_runs`` and the attempts of an idea count the runs of every role.

        A ``ready`` idea with an ``after`` link to an idea that is not ``done`` is skipped and not changed. It stays
        ``ready`` and is taken when every prerequisite has finished. A runnable idea whose track is not in
        ``piloop.toml`` is parked with a note that says why (``_park_strays``). An idea on a track with a share of 0
        waits. While a new evaluator has not measured the head (``_remeasure`` measures it first), only a pinned idea
        has a base: an unpinned run starts from the head and is judged against it.
        When an exploration run is due (``_explore``), the slot goes first to an idea pinned to an alternative base.
        Otherwise it goes to the most urgent idea of the track furthest below its share (``_pick``). An idea pinned to
        a harness at its cap (``_full``) waits. When every harness of a role's rotation is at its cap, the role queues
        nothing more in this tick.
        """
        self._park_strays(s, campaign, cfg)
        live = [t.name for t in cfg.all_tracks if t.share > 0]
        query = select(Idea).where(Idea.campaign_id == campaign.id, runnable(), Idea.track.in_(live))
        if head_unmeasured(s, campaign):
            query = query.where(Idea.base_candidate_id.is_not(None))
        for name, role in cfg.roles.doing_runs().items():
            takes = query
            if role.takes is not None:
                taken = s.connection().exec_driver_sql(ideas_taken(role.takes)).scalars().all()
                takes = query.where(Idea.id.in_(taken))
            for _ in range(self._free_slots(s, campaign, cfg, {name: role})):
                full = self._full(s, campaign, cfg)
                fits = takes.where(or_(Idea.harness.is_(None), Idea.harness.not_in(full)))
                explored = self._explore(s, campaign, name, role, fits)
                if explored is not None:
                    idea, by_priority = explored, explored
                else:
                    idea, by_priority = self._pick(s, campaign, cfg, name, role, fits)
                if idea is None or not self._enqueue(s, campaign, cfg, name, idea, full, by_priority=by_priority):
                    break

    def _park_strays(self, s: Session, campaign: Campaign, cfg: CampaignConfig) -> None:
        """Park each runnable idea whose track is not in ``piloop.toml``, with a note that says how to fix it.

        Track names are in the config, not in the schema. A track can be renamed or removed, or raw SQL can have a typo.
        """
        names = [t.name for t in cfg.all_tracks]
        strays = s.scalars(select(Idea).where(Idea.campaign_id == campaign.id, runnable(), Idea.track.not_in(names)))
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
            self._tracer.event(
                'idea.parked', campaign=campaign.name, idea=idea.id, reason=f'unknown track {idea.track}'
            )

    def _pick(
        self, s: Session, campaign: Campaign, cfg: CampaignConfig, name: str, role: WorkerRole, query: Select[Idea]
    ) -> tuple[Idea | None, Idea | None]:
        """Return the most urgent idea of the track furthest below its share, and the pick by priority alone.

        A track's distance below its share is ``share × max_concurrent`` of the role minus the role's open runs on the
        track. Only tracks with an idea in ``query`` take part, so a track with nothing runnable gives its slot to
        another track. Ties go to the track listed first. With one track, the two picks are the same.
        """
        first = s.scalars(query.order_by(Idea.priority, Idea.id).limit(1)).first()
        if first is None or not cfg.multitrack:
            return first, first
        open_runs = campaign_runs(campaign.id).where(Run.status.in_(OPEN_RUNS), done_by(name))
        running = Counter(s.scalars(open_runs.with_only_columns(Idea.track)))
        ready = set(s.scalars(query.with_only_columns(Idea.track)))
        slots = role.max_concurrent
        track = max((t for t in cfg.all_tracks if t.name in ready), key=lambda t: t.share * slots - running[t.name])
        idea = s.scalars(query.where(Idea.track == track.name).order_by(Idea.priority, Idea.id).limit(1)).first()
        return idea, first

    def _explore(self, s: Session, campaign: Campaign, name: str, role: WorkerRole, query: Select[Idea]) -> Idea | None:
        """Return the most urgent runnable idea pinned to an alternative base if an exploration run is due, else None.

        An exploration run is due if none of the role's last ``explore_every - 1`` runs was a branch run. If no such
        idea is ready, the queue uses its usual order. Instructions to research alone do not make enough branch runs,
        so the daemon enforces this. It counts branch runs, not runs since the last exploration, so branch runs that
        research or the orchestrator pinned on their own also count.
        """
        every = role.explore_every
        if every == 0:
            return None
        latest = (
            campaign_runs(campaign.id)
            .where(done_by(name))
            .with_only_columns(Run.branch)
            .order_by(Run.id.desc())
            .limit(every - 1)
        )
        recent = s.scalars(latest).all()
        if len(recent) < every - 1 or any(recent):
            return None
        pinned = s.scalars(query.where(Idea.base_candidate_id.is_not(None)).order_by(Idea.priority, Idea.id))
        trunk = head(s, campaign)
        return next((i for i in pinned if branches(s.get(Candidate, i.base_candidate_id), trunk)), None)

    def _enqueue(
        self,
        s: Session,
        campaign: Campaign,
        cfg: CampaignConfig,
        role: str,
        idea: Idea,
        full: Collection[str],
        *,
        by_priority: Idea | None,
    ) -> bool:
        """Queue one run of ``idea`` by ``role`` on its pinned candidate if it has one, else on the current head.

        A pin is a deliberate branch. ``run.queued`` then records ``pinned_base`` and ``computed_base``, so a wrong pin
        is easy to find. If the pin's bar (``ledger.bar_of``: the pin, or the nearest keep of a non-keep) is not the
        head, the run is a branch run (``runs.branch``) and is judged against that bar. ``run.queued`` also records
        ``track``, and ``priority_pick`` if the share rule took a different idea than priority order alone
        (``by_priority``). This shows how often the share rule changes the pick.

        Returns False, and queues nothing, if the idea pins no harness and every harness of the rotation is ``full``.
        """
        total_runs = count(s, campaign_runs(campaign.id).where(done_by(role)))
        harnesses = cfg.role_harnesses(role)
        harness = idea.harness or free_harness(harnesses, harnesses[total_runs % len(harnesses)], full)
        if harness is None:
            return False
        computed = head(s, campaign)
        pin = idea.base_candidate_id
        pinned = s.get(Candidate, pin) if pin is not None else None
        base = pinned if pin is not None else computed
        if base is None:
            reason = f'pinned candidate#{pin} does not exist' if pin is not None else 'no kept candidate'
            self._tracer.event('idea.parked', campaign=campaign.name, idea=idea.id, reason=reason)
            idea.status = IdeaStatus.PARKED
            return True
        attempt = len(idea.runs) + 1
        model = cfg.role_models(role)[harness]
        run = Run(
            idea=idea,
            role=role,
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
                kind=role,
                campaign_id=campaign.id,
                run_id=run.id,
                harness=harness,
                model=model,
                reason=f'idea#{idea.id} is ready',
            )
        )
        return True

    def _full(self, s: Session, campaign: Campaign, cfg: CampaignConfig) -> set[str]:
        """Return the harnesses at their ``max_concurrent`` cap, counting live agents and the workers of queued runs.

        A queued run's harness was chosen when it was queued (``_enqueue``), so its requested worker holds its place.
        A requested sweep gets its harness only when it is taken (``_take``).
        """
        if not cfg.max_concurrent:
            return set()
        queued = (Worker.state == WorkerState.REQUESTED) & Worker.kind.in_(cfg.roles.doing_runs())
        on = select(Worker.harness).where(
            Worker.campaign_id == campaign.id, Worker.state.in_(LIVE) | queued, Worker.harness.in_(cfg.max_concurrent)
        )
        agents = Counter(s.scalars(on))
        return {harness for harness, cap in cfg.max_concurrent.items() if agents[harness] >= cap}

    def _take(self, s: Session, campaign: Campaign, cfg: CampaignConfig) -> list[tuple[int, int | None]]:
        """Mark requested rows ``starting``, up to each role's ``max_concurrent``. Refuse a run role's row without a run.

        A sweep's harness is chosen here. A harness at its cap (``_full``) is skipped for the next one in the rotation.
        A sweep pinned to that harness stays requested. A run's harness was chosen when it was queued (``_reroute``).
        A request for a role that ``piloop.toml`` no longer has is refused, and its queued run fails
        (``ROLE_REMOVED``), so that its idea goes back to the queue.

        Returns ``(worker_id, run_id)`` pairs. The run is what a worker executes. Nothing is taken while the startup
        breaker holds (``_paused``).
        """
        taken: list[tuple[int, int | None]] = []
        if self._paused(s, campaign):
            return taken
        roles = cfg.roles.all()
        unknown = select(Worker).where(
            Worker.campaign_id == campaign.id, Worker.state == WorkerState.REQUESTED, Worker.kind.not_in(roles)
        )
        for worker in s.scalars(unknown):
            self._close(s, worker, f'no [roles.{worker.kind}] in piloop.toml')
            if worker.run_id is not None:
                self._fail_run(s, s.get_one(Run, worker.run_id), ROLE_REMOVED)
        full = self._full(s, campaign, cfg)
        for kind, rc in roles.items():
            mine = (Worker.campaign_id == campaign.id, Worker.kind == kind)
            active = count(s, select(Worker).where(*mine, Worker.state.in_(LIVE)))
            requested = s.scalars(
                select(Worker)
                .where(*mine, Worker.state == WorkerState.REQUESTED)
                .order_by(Worker.id)
                .limit(max(rc.max_concurrent - active, 0))
            ).all()
            rotation = cfg.role_harnesses(kind)
            for worker in requested:
                if isinstance(rc, WorkerRole):
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
                full = self._full(s, campaign, cfg)
        return taken

    def _reroute(self, s: Session, cfg: CampaignConfig, worker: Worker, rotation: list[str], full: set[str]) -> bool:
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
        run.model = worker.model = cfg.role_models(worker.kind)[harness]
        return True

    def _paused(self, s: Session, campaign: Campaign) -> bool:
        """Return True if ``STARTUP_BREAKER`` agents of the campaign failed at startup within ``STARTUP_WINDOW``.

        Requested rows wait, and launch when those failures are older than the window. The window is the back-off.
        Each pause posts one escalation (priority 1) with the load average and the failures' errors, which end with
        the agents' last screens. The escalation is updated in place while the pause holds, so an acknowledgement
        means that the human saw it. A new escalation is posted only after launches resume and pause again.
        """
        workers = select(Worker.id).where(Worker.campaign_id == campaign.id)
        deaths = s.scalars(
            select(Event)
            .where(Event.kind == STARTUP_ANOMALY, Event.target_kind == TargetKind.WORKER, Event.target_id.in_(workers))
            .where(Event.at >= utcnow() - STARTUP_WINDOW)
            .order_by(Event.id)
        ).all()
        latest = s.scalars(
            select(Event)
            .where(Event.kind.in_((LAUNCH_PAUSED, LAUNCH_RESUMED)))
            .where(Event.target_kind == TargetKind.CAMPAIGN, Event.target_id == campaign.id)
            .order_by(Event.id.desc())
            .limit(1)
        ).first()
        pause = latest if latest is not None and latest.kind == LAUNCH_PAUSED else None
        if len(deaths) < STARTUP_BREAKER:
            if pause is not None:
                emit(s, LAUNCH_RESUMED, TargetKind.CAMPAIGN, campaign.id, payload={'comment': pause.payload['comment']})
            return False
        escalation = s.get(Comment, pause.payload['comment']) if pause is not None else None
        if pause is None or escalation is None:
            self._escalate_pause(s, campaign, deaths)
        else:
            escalation.body = pause_body(deaths, str(pause.payload['load']))
        return True

    def _escalate_pause(self, s: Session, campaign: Campaign, deaths: Sequence[Event]) -> None:
        load = ' / '.join(f'{x:.1f}' for x in os.getloadavg())
        comment = Comment(
            target_kind=TargetKind.CAMPAIGN,
            target_id=campaign.id,
            author=DAEMON_ACTOR,
            kind=CommentKind.ESCALATION,
            priority=1,
            body=pause_body(deaths, load),
        )
        s.add(comment)
        s.flush()
        payload: dict[str, object] = {'comment': comment.id, 'deaths': len(deaths), 'load': load}
        emit(s, LAUNCH_PAUSED, TargetKind.CAMPAIGN, campaign.id, payload=payload)
        self._tracer.event(LAUNCH_PAUSED, campaign=campaign.name, deaths=len(deaths), load=load)

    def _sweep(self, worker_id: int) -> None:
        """Run a sweep, an agent without a run, in its role's ``workdir``, with a brief made from its row.

        Every sweep gets the campaign overview (``piloop.reports.overview``) in its context. A sweep that got anomalies
        is an incident sweep: the anomalies come first, the research pack is left out (``piloop history`` prints it),
        and the brief adds the role's incident prompt. Every other sweep reads the research pack
        (``piloop.reports.pack``), and its brief adds the role's routine prompt. The watchdog's are its incident sweep
        and its progress review.

        A built-in role's sweep also gets the context of its role (``_context``). Research gets the number of waiting
        seeds, its share of the backlog's shortfall, the runs since the last keep against the stall mark
        (``piloop.campaign.stop.stall``), and the time the pack was read. With that time, it can query for ideas that
        parallel sweeps inserted later. The scout gets its seed cap, its search window, the watch list and the
        technique library (``piloop.agents.scout``). Nothing that a sweep commits is scored. In a worktree at the
        head, the commits are removed with the worktree when the agent ends.
        """
        with self._db.session() as s:
            worker = s.get_one(Worker, worker_id)
            campaign = s.get_one(Campaign, worker.campaign_id)
            cfg = config_of(campaign)
            role = role_of(s, worker)
            in_campaign = self._in_campaign(s, worker) is not None
            base = checkout_candidate(s, campaign)
            values = ', '.join(
                f'{t.primary.name}={"unmeasured" if (v := score(base, campaign.evaluator_id or 0, t.primary)) is None else v}'
                for t in cfg.all_tracks
            )
            now = utcnow()
            cwd = (
                f'- Your cwd is the campaign checkout, shared with the human; the head is candidate#{base.id} ({values}).'
                ' Leave it as you found it: nothing you change here is scored or kept.'
                if in_campaign
                else f'- Your cwd is a detached checkout of candidate#{base.id}, the head ({values}): read and run the'
                ' code, try things. Nothing you commit here is scored or kept; it goes with the checkout when you end.'
            )
            lines = [
                cwd,
                f'- Shared workspace for notes and artifacts to other agents: `./{WORKSPACE}/` (not committed). The files'
                ' of a done idea are archived, and removed from it after some days or when the disk is short; `piloop'
                f' restore {campaign.name} <idea id>|<workspace path>` writes them back.',
            ]
            lines += self._context(s, campaign, cfg, worker, now)
            incidents = assigned(s, worker_id)
            sections = [*lines, '']
            if incidents:
                sections += ['## Incidents to investigate', *incidents, '']
            sections.append(overview(s, campaign, cfg, now, partial(self._jobs, s)))
            if not incidents:
                sections.append(pack(s, campaign, cfg, workspace=workspace_dir(self._home, campaign.id)))
            brief = compose_brief(
                role,
                campaign,
                worker_id,
                reason=worker.reason or 'requested',
                context='\n'.join(sections),
                incident=bool(incidents),
                wake=lessons_overview(s, campaign, cfg.wake_budget(role.kind)),
            )
            campaign_path, campaign_id, base_sha = Path(campaign.path), campaign.id, base.commit_sha
        if self._executor.launcher.cancelled.is_set():  # stop() closed the row, so do not create a worktree.
            return
        log_dir = self._workers_dir / str(worker_id)
        if in_campaign:
            self._executor.launcher.launch(worker_id, brief=brief, cwd=campaign_path, log_dir=log_dir)
            return
        repo = git.open_repo(campaign_path)
        wt_path = log_dir / 'worktree'
        links = worktree_links(cfg, campaign_path, campaign_dir(self._home, campaign_id))
        make_cwd = partial(git.add_worktree, repo, wt_path, base_sha, links)
        launch = self._executor.launcher.launch
        _in_worktree(
            repo,
            wt_path,
            partial(launch, worker_id, brief=brief, cwd=wt_path, log_dir=log_dir, make_cwd=make_cwd, links=links),
        )

    def _context(self, s: Session, campaign: Campaign, cfg: CampaignConfig, worker: Worker, now: datetime) -> list[str]:
        """Return the context lines of a sweep: a built-in role's own, by the class of its table.

        Another role that defines ideas (``files = "ideas"``) is told its ``max_ideas``, which ``low_water`` counts on.
        Research's context says it with the backlog.
        """
        match cfg.roles.sweeping()[worker.kind]:
            case ResearchRole() as research:
                return self._research_context(s, campaign, cfg, worker.kind, research, now)
            case ScoutRole(seeds=seeds):
                return scout.context(s, campaign.id, worker.kind, worker.id, seeds, now)
            case SweepRole(files=Files.IDEAS, max_ideas=most):
                return [f'- New ideas of your own in this sweep: at most {most} (`max_ideas`).']
            case _:
                return []

    def _research_context(
        self, s: Session, campaign: Campaign, cfg: CampaignConfig, name: str, role: ResearchRole, now: datetime
    ) -> list[str]:
        """Return the context lines of a research sweep: the seeds, its share of the backlog, the stall mark.

        The backlog's target has the ``buffer`` of the role's first ``low_water`` trigger, or none without one.
        """
        buffer = next((t.buffer for t in role.triggers if isinstance(t, LowWaterTrigger)), 0)
        backlog = self._backlog(s, campaign, cfg, name, buffer)
        most = role.max_ideas
        own = max(min(backlog.short + most, most), 0)  # This sweep's share of the shortfall.
        seeds = count(s, select(Idea).where(Idea.campaign_id == campaign.id, Idea.status == IdeaStatus.SEED))
        without, (mark, why) = runs_without_keep(s, campaign), stall(cfg)
        stalled = ': stalled, so one of your own ideas pivots (section 3)' if without >= mark else ''
        short = [
            '- Tracks short of their share of the backlog: '
            + (
                ', '.join(
                    f'{t.name} {t.gap} ({t.runnable} of {t.target} runnable)' for t in backlog.tracks if t.gap > 0
                )
                or 'none'
            )
            + '. Give every idea and every developed seed the track it serves (`--track`); fill the short ones first.'
        ]
        return [
            f'- Seeds to develop first: {seeds}, listed in the research pack oldest first.',
            f'- Ideas of your own after the seeds: propose {own} (the backlog holds {backlog.held()};'
            ' developed seeds do not count against it). Fewer only when what is left duplicates queued work;'
            ' name that work in your finding.',
            *(short if cfg.multitrack else []),
            f'- Measured runs since the last keep: {without}, of the {mark} that mark a stall ({why}){stalled}.',
            f'- Sweep started at: {now.strftime("%Y-%m-%d %H:%M:%S")} (UTC; the research pack below is as of then).',
        ]

    def _in_campaign(self, s: Session, worker: Worker) -> Path | None:
        """Return the campaign checkout if a sweep works in it (``workdir = "repo"``), else None."""
        if worker.campaign_id is None or (campaign := s.get(Campaign, worker.campaign_id)) is None:
            return None
        role = config_of(campaign).roles.sweeping().get(worker.kind)
        return Path(campaign.path) if role is not None and role.workdir is Workdir.REPO else None

    def _reattach_sweep(self, worker_id: int) -> None:
        """Watch the agent of a sweep that a previous daemon process started, in its workdir, until it ends."""
        log_dir = self._workers_dir / str(worker_id)
        with self._db.session() as s:
            worker = s.get_one(Worker, worker_id)
            checkout = self._in_campaign(s, worker)
            repo = git.open_repo(Path(s.get_one(Campaign, worker.campaign_id).path))
        if checkout is not None:
            self._executor.launcher.reattach(worker_id, cwd=checkout, log_dir=log_dir)
            return
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

    def serve_forever(self, tick_seconds: float, *, also: Callable[[], object] = lambda: None) -> None:
        """Tick, and call ``also`` after each tick, until ``stop`` or ``interrupt`` is called."""
        while not self._stop.is_set():
            self.tick()
            also()
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
        wait([*rest, *self._scoring.values(), *self._archiving()], timeout=timeout)
        self._pool.shutdown(wait=False, cancel_futures=True)

    def _archiving(self) -> list[Future[None]]:
        """The archiver's job in flight: it prunes and evicts, so the next daemon must not start under it."""
        return self._archiver.running() if self._archiver is not None else []

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
        wait(self._archiving(), timeout=timeout)
        self._pool.shutdown(wait=False, cancel_futures=True)


@dataclass(frozen=True)
class RoleTick:
    """The ``Tick`` of one sweeping role of one campaign: what its triggers read in this tick."""

    s: Session
    campaign: Campaign
    role: str
    last: Worker | None
    max_concurrent: int
    interval: float
    backlog: Callable[[int], Backlog]
    incidents: Callable[[], list[Incident]]

    def _mine(self) -> Select[Worker]:
        return select(Worker).where(Worker.campaign_id == self.campaign.id, Worker.kind == self.role)

    def live(self) -> int:
        return count(self.s, self._mine().where(Worker.state.in_(LIVE)))

    def killed_streak(self) -> int:
        states = self.s.scalars(self._mine().with_only_columns(Worker.state).order_by(Worker.id.desc()))
        return len(list(itertools.takewhile(lambda state: state is WorkerState.KILLED, states)))

    def empty_sweeps(self, states: tuple[WorkerState, ...]) -> list[Worker]:
        """See ``Tick.empty_sweeps``.

        The new-results back-off of ``low_water`` skips killed sweeps. A killed sweep stopped early, so its empty
        result says nothing about what was left to propose. The idle rest counts killed sweeps. Otherwise, a harness
        that dies every time would be launched again on every tick.
        """
        signed = defined_by(self.s, self.campaign.id)
        ended = self.s.scalars(self._mine().where(Worker.state.in_(states)).order_by(Worker.id.desc()))
        return list(itertools.takewhile(lambda worker: f'{self.role}#{worker.id}' not in signed, ended))

    def routine(self) -> Worker | None:
        given = exists().where(
            Event.kind == ANOMALY_EVENT, Event.target_kind == TargetKind.WORKER, Event.target_id == Worker.id
        )
        return self.s.scalars(self._mine().where(~given).order_by(Worker.id.desc()).limit(1)).first()


def _in_worktree(repo: Repo, wt_path: Path, agent: Callable[[], Outcome]) -> None:
    """Run a sweep's agent in its worktree, then remove the worktree, unless the agent was handed over."""
    with ExitStack() as cleanup:
        cleanup.callback(git.remove_worktree, repo, wt_path)
        if agent().handed_over:
            cleanup.pop_all()


def done_by(role: str) -> ColumnElement[bool]:
    """Select the runs of ``role``.

    ``likely()`` tells SQLite that most runs match, as most runs are the worker's. Without it, SQLite drives a join
    with ``ideas`` from ``runs`` and scans every run of every campaign.
    """
    return func.likely(Run.role == role, type_=Boolean)


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


def launches(campaign: Campaign, cfg: CampaignConfig, held: str | None) -> bool:
    """Whether the tick launches the campaign's requested agents, given why the spend guard holds (``held``).

    An active campaign launches when nothing holds it: no spend hold and no pause. A stopped campaign launches its
    closing review and what a human requests, paused or not. While the spend guard holds, it launches unless
    ``[spend] closing_review`` is ``wait``.
    """
    if campaign.status is CampaignStatus.ACTIVE:
        return held is None and campaign.paused is None
    return held is None or cfg.spend is None or cfg.spend.closing_review is not ClosingReview.WAIT
