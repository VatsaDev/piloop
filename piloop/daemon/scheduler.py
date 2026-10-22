"""The daemon loop: check each role's triggers for each campaign, launch requested agents and reap stale state.

A tick has two phases. First, the triggers (https://piloop.autoresearch.inc/concepts/roles) insert ``agent_sessions`` rows in state ``requested``. Then the
requested rows launch in id order, as far as the role's ``max_concurrent`` allows. Anyone can insert a requested row
by hand, for example the CLI or an agent. Such a row launches in the same second phase.

Before both phases, the tick sends new comments to the running implementers. For each running implementer, it collects the
``note``, ``finding`` and ``decision`` comments on its run or its idea that were written since the run started. It
sends them through the implementer's runtime in one prompt, one ``[piloop]`` line each (``steer_line``). An event on the
implementer, ``comment.delivered`` or ``comment.undelivered``, records each delivery. A daemon restart does not send a
comment again. An implementer that is still ``starting`` gets the comments on the next tick. A failed delivery is not
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
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path

from git.exc import GitError, InvalidGitRepositoryError, NoSuchPathError
from pydantic import ValidationError
from sqlalchemy import Boolean, ColumnElement, Select, delete, exists, func, or_, select
from sqlalchemy.orm import Session

from piloop import deprecated, git
from piloop.actor import Agent
from piloop.agents import scout
from piloop.agents.launcher import rotate
from piloop.agents.refill import usage_of
from piloop.agents.results import STARTUP_ANOMALY, STOP_ENDED, STOPPED_ERROR, WRAP_UP
from piloop.agents.roles import compose_brief, role_of
from piloop.campaign import spend
from piloop.campaign.campaigns import WORKSPACE, campaign_dir, workspace_dir, worktree_links
from piloop.campaign.ideas import defined_by, runnable, supersede, waiting
from piloop.campaign.memory import sync_documents
from piloop.campaign.spend import SpendError
from piloop.campaign.stop import (
    CAMPAIGN_STOPPED_ERROR,
    CANCEL_REQUESTED,
    CANCELLED_ERROR,
    ROLE_REMOVED,
    STALE_ERROR,
    agent_cancel,
    asked,
    attempts,
    check as check_stop,
    park_cancelled,
    requested,
    requested_stop,
    runs,
    runs_without_keep,
    stall,
)
from piloop.config import CampaignConfig, ClosingReview, Spend, config_of
from piloop.daemon.archiver import Archiver, Archiving
from piloop.daemon.run_controller import RunController, RunEnd, commits_of, left_finding, open_escalation, run_outcome
from piloop.db.events import ANOMALY_EVENT, emit
from piloop.db.models import (
    LIVE,
    OPEN_RUNS,
    AgentSession,
    AgentSessionState,
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
    campaign_runs,
    count,
    utcnow,
)
from piloop.db.session import Database
from piloop.db.triggers import DAEMON_ACTOR
from piloop.ids import AgentSessionId, RoleName, RunId, TargetKind, TargetRef
from piloop.otel import Tracer
from piloop.plugins.executor.contract import AgentKey, Artifact, Checkout, GitTree, LeftJob
from piloop.plugins.role.contract import Files, ImplementerRole, SweepRole, Workdir, ideas_taken
from piloop.plugins.role.research import ResearchRole
from piloop.plugins.role.scout import ScoutRole
from piloop.plugins.runtime.contract import AgentStatus
from piloop.plugins.stop_rule.contract import GO, Go, Hold, Stop
from piloop.plugins.trigger.contract import Backlog, Fire, Incident, TrackBacklog
from piloop.plugins.trigger.low_water import LowWaterTrigger
from piloop.reports.overview import anomalies, assigned, overview, reported
from piloop.reports.pack import pack
from piloop.reports.wake import lessons_overview
from piloop.runtime.factory import RuntimeFactory, handle_for, make_runtime, runtime_for
from piloop.scoring.ledger import bar_of, checkout_candidate, due, head, head_unmeasured, rejudges, score
from piloop.text import cell
from piloop.usage import Usage

NO_RUN_ERROR = 'an implementer needs a run; the scheduler requests them from ready ideas'
PUSHED_KINDS = tuple(kind for kind in CommentKind if kind is not CommentKind.ESCALATION)
"""Comment kinds that are sent to a running implementer. Escalations are not sent, because they are for the human."""
SETTLED = ('comment.delivered', 'comment.undelivered')
"""Events on an implementer that record a comment as sent or failed. The comment id is in the payload."""
STEER_LINE_CHARS = 200
"""The number of characters of a comment's first line that a ``[piloop]`` line quotes. The implementer can query the rest."""
STOP_GRACE_SECONDS = 600
"""The seconds that an implementer that was told to wrap up has to commit its work before the daemon ends it."""
WRAP_UP_PROMPT = (
    '[piloop] {what} ({cause}). Commit what you have now, with a message that says where it stands.'
    ' Leave your finding and stop. This session ends in {minutes:g} minutes.'
)
JOBS_LEFT = 'agent_session.jobs_left'
"""Event on an implementer that left its finding and went idle while jobs that it started still ran.

The daemon told it which jobs keep its run open (``JOBS_LEFT_PROMPT``). The payload has ``jobs``, and ``error`` when the
prompt failed. It is sent once per implementer."""
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
        controller: RunController,
        tracer: Tracer,
        home: Path,
        runtime_factory: RuntimeFactory = make_runtime,
        archiving: Archiving | None = None,
    ) -> None:
        self._db = db
        self._controller = controller
        self._tracer = tracer
        self._home = home
        self._runs_dir = home / 'runs'
        self._agent_sessions_dir = home / 'agent_sessions'
        self._runtime_factory = runtime_factory
        # No bound: a run's thread waits in the evaluation queue while it scores, and a bound would let those waits
        # hold back agent launches. The roles' max_concurrent and the executor's slots bound the work itself.
        self._pool = ThreadPoolExecutor(max_workers=sys.maxsize, thread_name_prefix='agent')
        self._futures: dict[AgentSessionId, Future[None]] = {}
        self._scoring: dict[tuple[TargetKind, int], Future[None]] = {}
        """Scoring outside an implementer: a run's resumed scoring by run id, or a campaign's re-measure by campaign id."""
        self._stop = threading.Event()
        self._archiver = (
            Archiver(db, archiving.store, home, archiving.policy, tracer, self._pool, self._stop)
            if archiving is not None
            else None
        )
        """Archives the workspaces in the background. The daemon always passes ``archiving``; a test that does not
        archives nothing."""

    def reap(self) -> None:
        """Hand the jobs that a previous daemon process left to the executor (``Executor.adopt``). Fail the rest.

        The executor takes over what it can find: ``workers`` gives every live lease a full period, so nothing is queued
        again only because of the restart, and its worker re-attaches to each agent that still has its worktree, and
        reports the rest gone, which fails their runs as ``worker lost``, not as attempts. For each agent it adopts,
        the daemon waits for its end as if no daemon had stopped (``RunController.reattach``, or ``_reattach_sweep`` for
        a sweep). Both an in-place restart (``hand_over``) and a crash leave agents for this. An agent that it does not
        adopt is killed, and its row and its run fail. A closed run whose commits were not all scored
        (``runs.scoring``) is scored now (``RunController.resume``), whatever its idea's status. An idea that stayed
        ``scheduled`` does not run again, and no dependent idea takes it before its keeps are known. The worktrees of
        failed runs and sweeps are removed. The evaluation queue starts empty: its jobs were the previous daemon's, and
        what still needs a measurement asks again.
        """
        with self._tracer.span('piloop.reap'):
            with self._db.session() as s:
                s.execute(delete(Evaluation))
                live = s.scalars(select(AgentSession).where(AgentSession.state.in_(LIVE))).all()
                runs = {w.id: w.run_id for w in live}
                left = [self._left(s, w) for w in live]
                s.commit()
            refused = set(self._controller.executor.adopt(left))
            adopted = [
                job.key.agent_session for job in left if isinstance(job.key, AgentKey) and job.key not in refused
            ]
            self._fail_open(STALE_ERROR, reaped=True, sparing=adopted)
            for agent_session_id in adopted:
                run_id = runs[agent_session_id]
                self._tracer.event('agent_session.reattached', agent_session=agent_session_id, run=run_id)
                body = (
                    partial(self._reattach_sweep, agent_session_id)
                    if run_id is None
                    else partial(self._controller.reattach, run_id, agent_session_id)
                )
                self._futures[agent_session_id] = self._pool.submit(body)
            with self._db.session() as s:
                unscored = s.scalars(select(Run.id).where(Run.scoring).order_by(Run.id)).all()
            for run_id in unscored:
                self._tracer.event('run.unscored', run=run_id)
                self._scoring[TargetKind.RUN, run_id] = self._pool.submit(self._controller.resume, run_id)

    def _left(self, s: Session, w: AgentSession) -> LeftJob:
        """The ``LeftJob`` of an agent that a previous daemon left in flight: its key, and the job of a running one."""
        if w.state is AgentSessionState.RUNNING and (job := self._running(s, w)) is not None:
            return job
        return LeftJob(AgentKey(w.id))

    @deprecated.until(
        '0.3.0', 'an agent that a daemon before leases started: delete _running with Launcher.left and LeftJob.job'
    )
    def _running(self, s: Session, w: AgentSession) -> LeftJob | None:
        """The ``LeftJob`` of a running agent with its job, if its worktree exists, for an executor to adopt it.

        Only an agent that a daemon before leases started needs it: so an upgrade does not lose its work, it keeps
        running, and its run is scored when it ends.
        """
        campaign = s.get(Campaign, w.campaign_id) if w.campaign_id is not None else None
        if campaign is None:
            return None
        cfg = config_of(campaign)
        links = worktree_links(cfg, Path(campaign.path), campaign_dir(self._home, campaign.id))
        if w.run_id is not None:
            run = s.get_one(Run, w.run_id)
            cwd = self._runs_dir / str(w.run_id) / 'worktree'
            if run.status is not RunStatus.RUNNING or not cwd.is_dir():
                return None
            artifact: Artifact = GitTree(repo=Path(campaign.path), commit=run.base.commit_sha, links=links)
            return self._controller.launcher.left(s, w, artifact, cwd, commits_of(w.run_id, w.id))
        checkout = Path(campaign.path) if self._in_campaign(campaign, w.kind) else None
        cwd = checkout or self._agent_sessions_dir / str(w.id) / 'worktree'
        if not cwd.is_dir():
            return None
        head = checkout_candidate(s, campaign).commit_sha
        artifact = Checkout(path=cwd) if checkout else GitTree(repo=Path(campaign.path), commit=head, links=links)
        return self._controller.launcher.left(s, w, artifact, cwd, None)

    def _fail_open(self, error: str, *, reaped: bool, sparing: Collection[AgentSessionId] = ()) -> None:
        """Fail each run in flight and queue its idea again. Close each live agent except ``sparing``.

        Requested implementers and their queued runs are not changed, because they are requests, not processes. An idea
        that is ``scheduled`` without an open run stays so. Its run finished and only its scoring stopped, which
        ``reap`` resumes. With ``reaped``, the rows are those that a previous daemon left and its executor did not
        adopt: their agents are killed (``_kill_stale``), and their worktrees removed. Otherwise the executor stopped
        its jobs, and a live run thread removes its own worktrees in its ``finally`` block: removing a worktree while
        an evaluator still runs in it is worse than leaving it a moment longer.
        """
        with self._db.session() as s:
            spared = AgentSession.id.in_(sparing)
            launched = select(AgentSession.run_id).where(
                AgentSession.state.in_(LIVE), AgentSession.run_id.is_not(None), ~spared
            )
            kept = select(AgentSession.run_id).where(spared, AgentSession.run_id.is_not(None))
            in_flight = (Run.status == RunStatus.RUNNING) & Run.id.not_in(kept)
            for run in s.scalars(select(Run).where(in_flight | Run.id.in_(launched))).all():
                self._fail_run(s, run, error)
                if reaped:
                    self._remove_worktrees(self._runs_dir / str(run.id), Path(run.idea.campaign.path))
            for agent_session in s.scalars(select(AgentSession).where(AgentSession.state.in_(LIVE), ~spared)):
                # a ``starting`` row that its daemon left before its start window has no process
                if reaped and agent_session.runtime is not None:
                    self._kill_stale(s, agent_session)
                if reaped and agent_session.run_id is None and agent_session.campaign_id is not None:
                    campaign_path = Path(s.get_one(Campaign, agent_session.campaign_id).path)
                    self._remove_worktrees(self._agent_sessions_dir / str(agent_session.id), campaign_path)
                self._close(s, agent_session, error)
            s.commit()

    def _fail_run(self, s: Session, run: Run, error: str) -> None:
        """Fail ``run`` and return its idea to the queue, unless a human parked or rejected it meanwhile.

        A run that the daemon stopped (``stop.NOT_ATTEMPTS``) is not an attempt, so its idea always goes back to
        ``ready``. A running run whose implementer left its finding is not failed: it is handed off (``_hand_off``).
        """
        agent_session = s.scalars(
            select(AgentSession).where(AgentSession.run_id == run.id).order_by(AgentSession.id.desc()).limit(1)
        ).first()
        if run.status is RunStatus.RUNNING and agent_session is not None and left_finding(s, run, agent_session):
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
        """Close a run whose implementer left its finding as finished, and leave its commits to be scored on the next start.

        The implementer reported that it is done. A job of its own may still keep the run open, but the run is not cut: its
        idea stays ``scheduled``, so no second run redoes the work. The run thread pins the commits
        (``RunController._keep_cut_work``), and ``reap`` scores them and settles the idea (``runs.scoring``). Its status is
        what the run's end would give (``run_outcome``).
        """
        status, error = run_outcome(RunEnd(finding=True, asked=open_escalation(s, run)))
        run.status, run.error, run.finished_at, run.scoring = status, error, utcnow(), True
        emit(s, 'run.finished', TargetKind.RUN, run.id, payload={'status': status, 'error': error})
        self._tracer.event('run.handed_off', run=run.id, status=status)

    def _close(self, s: Session, agent_session: AgentSession, error: str) -> None:
        """Close an agent session row that the launcher does not close (after a stop or a reap), and read its usage.

        The usage comes from a scan of the session store. The agent can spend many tokens before it is killed. If the
        budget ignored them, a restarted campaign could go past ``max_tokens``. A requested row never started and has
        no session to read.
        """
        agent_session.state, agent_session.ended_at, agent_session.error = AgentSessionState.KILLED, utcnow(), error
        if agent_session.session_id is None:
            return
        found = usage_of(s, agent_session)
        if isinstance(found, Usage):
            agent_session.usage = found.as_json()
        elif found is not None:
            self._tracer.event('agent_session.usage_unknown', agent_session=agent_session.id, error=str(found))

    def _remove_worktrees(self, agent_dir: Path, campaign_path: Path) -> None:
        """Remove the worktrees under a run's or a sweep's directory. If the campaign is gone, record an event."""
        try:
            repo = git.open_repo(campaign_path)
        except NoSuchPathError, InvalidGitRepositoryError:
            self._tracer.event('worktrees.orphaned', agent_dir=str(agent_dir), campaign_path=str(campaign_path))
            return
        for wt in git.worktrees_under(repo, agent_dir):
            git.remove_worktree(repo, wt)

    @deprecated.until('0.3.0', 'a kill of an agent that no executor holds: delete _kill_stale with _running')
    def _kill_stale(self, s: Session, agent_session: AgentSession) -> None:
        """Ask the runtime to close the pane of an agent that no executor adopted, which outlives its daemon. Best effort.

        Such an agent was started before leases, or its job ended with no report. Once every agent is an executor's,
        its executor stops it.
        """
        try:
            runtime_for(s, agent_session, self._runs_dir, self._runtime_factory).kill(handle_for(agent_session))
        except Exception as e:  # The pane can be gone already. The database row is what matters.
            self._tracer.event('agent_session.kill_failed', agent_session=agent_session.id, error=str(e))

    def tick(self) -> list[AgentSessionId]:
        """Send new comments to running implementers, run every campaign's triggers and launch requested agents.

        Returns the ids of the launched agent sessions. Before the triggers, the tick rejects the ideas that a newer idea
        superseded (``piloop.campaign.ideas.supersede``). This also happens for a stopped campaign, which requests
        nothing and is measured again only on request (``ledger.due``). Then the tick asks the stop rule
        (``piloop.campaign.stop.check``) of each active campaign, and of each stopped one whose closing review waits
        for a hold (``spend.review_waits``). A campaign that it holds gets only the first step. A paused campaign
        (``piloop pause``) gets no triggers, does not stop at a ``[stop]`` limit, and launches nothing, and with
        ``all`` it is not measured again either. A stop request stops a held or paused campaign too. A stopped
        campaign launches its closing review, and what a human requests, whatever the pause; while the stop rule
        holds, as ``[spend] closing_review`` says (``launches``). The
        archiver starts the next archiving of a workspace that is due, or its upkeep, on a background thread
        (``piloop.daemon.archiver``).
        """
        self._collect()
        self._controller.executor.tick(utcnow())
        self._controller.stop_cancelled()
        self._controller.dispatch()
        self._push_comments()
        self._report_jobs()
        self._wind_down()
        self._read_spend()
        if self._archiver is not None:
            self._archiver.tick()
        launched: list[tuple[AgentSessionId, RunId | None]] = []
        with self._db.session() as s:
            for campaign in s.scalars(select(Campaign)).all():
                try:
                    cfg = config_of(campaign)
                except ValidationError as e:
                    self._tracer.event('campaign.invalid_config', campaign=campaign.name, error=str(e))
                    continue
                for idea_id, by in supersede(s, campaign.id).items():
                    self._tracer.event('idea.superseded', campaign=campaign.name, idea=idea_id, by=by)
                answer = check_stop(s, campaign, cfg) if asked(s, campaign, cfg) else GO
                held = answer.reason if isinstance(answer, Hold) else None
                if held is None and campaign.paused is not Pause.ALL:
                    self._remeasure(s, campaign)
                if campaign.status is CampaignStatus.ACTIVE:
                    # A stop request wins over a hold and a pause.
                    if (stop := requested(s, campaign)) is not None:
                        self._triggers(s, campaign, cfg, stop, held)
                    elif not isinstance(answer, Hold) and campaign.paused is None:
                        self._triggers(s, campaign, cfg, answer, None)
                s.flush()
                if launches(campaign, cfg, held):
                    launched += self._take(s, campaign, cfg)
            s.commit()
        for agent_session_id, run_id in launched:
            body = (
                partial(self._controller.execute, run_id, agent_session_id)
                if run_id
                else partial(self._sweep, agent_session_id)
            )
            self._futures[agent_session_id] = self._pool.submit(body)
        return [agent_session_id for agent_session_id, _ in launched]

    def _read_spend(self) -> None:
        """Run the ``[spend] command`` of each campaign whose ``interval`` is up, and record the result.

        That is each active campaign, and each stopped one whose closing review waits for the hold to end
        (``spend.review_waits``).

        This runs outside any transaction, because the command can take up to ``spend.TIMEOUT``. A write lock held for
        that long locks all other clients out. The reading is a fact for the stop rule (``Fact.SPEND``): ``limits``
        holds the campaign while it is unknown or at the limit.
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
            self._scoring[key] = self._pool.submit(self._controller.rejudge, again[0].id)
            return
        if not (todo := due(s, campaign)):
            return
        self._tracer.event('candidate.remeasure', campaign=campaign.name, candidate=todo[0].id, left=len(todo))
        self._scoring[key] = self._pool.submit(self._controller.remeasure, todo[0].id)

    def _push_comments(self) -> None:
        """Send each running implementer the comments on its run or its idea that it has not received yet.

        The events table records what each implementer received: ``SETTLED`` events on the implementer, with the comment id in
        the payload. Nothing depends on the daemon's memory. A restart sends nothing again. An implementer whose row is still
        ``starting`` (brief composed, pane not yet up) is skipped and gets the comments on the next tick. A failed
        delivery is also recorded and not retried. The comment is in the brief of the idea's next run anyway, and a
        headless implementer would fail the same way on every tick. A tick that sends something makes one span.
        """
        with self._db.session() as s:
            pending = [(w, unseen) for w in self._running_agent_sessions(s) if (unseen := self._unseen(s, w))]
            if not pending:
                return
            with self._tracer.span('piloop.push', comments=sum(len(c) for _, c in pending)):
                for agent_session, comments in pending:
                    self._push(s, agent_session, comments)
                s.commit()

    def _running_agent_sessions(self, s: Session) -> Sequence[AgentSession]:
        return s.scalars(
            select(AgentSession)
            .where(AgentSession.state == AgentSessionState.RUNNING, AgentSession.run_id.is_not(None))
            .order_by(AgentSession.id)
        ).all()

    def _unseen(self, s: Session, agent_session: AgentSession) -> list[Comment]:
        """Return the comments on the implementer's run or idea that are new since the run started and not yet sent.

        The brief cannot contain comments written after the run started, because the transaction that composes the
        brief sets ``Run.started_at``. The comparison uses whole seconds. A raw INSERT sets ``created_at`` to
        ``CURRENT_TIMESTAMP``, which has second precision, but ``started_at`` is a Python timestamp. So a comment
        written in the same second as the brief can reach the implementer twice. That is better than never. The implementer's
        own comments are not sent back. An implementer signs as ``implementer#<id>`` (its ``PILOOP_ACTOR``) or ``run#<id>``.
        """
        run = s.get_one(Run, agent_session.run_id)
        since = (run.started_at or agent_session.started_at).strftime('%Y-%m-%d %H:%M:%S')
        settled = {
            e.payload.get('comment')
            for e in s.scalars(
                select(Event).where(
                    Event.target == TargetRef(TargetKind.AGENT_SESSION, agent_session.id),
                    Event.kind.in_(SETTLED),
                )
            )
        }
        on_run = Comment.target == TargetRef(TargetKind.RUN, run.id)
        on_idea = Comment.target == TargetRef(TargetKind.IDEA, run.idea_id)
        comments = s.scalars(
            select(Comment)
            .where(on_run | on_idea, Comment.kind.in_(PUSHED_KINDS))
            .where(func.strftime('%Y-%m-%d %H:%M:%S', Comment.created_at) >= since)
            .order_by(Comment.id)
        ).all()
        own = (*deprecated.signatures(Agent(role=agent_session.kind, session=agent_session.id)), f'run#{run.id}')
        return [c for c in comments if c.id not in settled and c.author not in own]

    def _push(self, s: Session, agent_session: AgentSession, comments: list[Comment]) -> None:
        """Send the comments in one ``runtime.prompt``, one line each, and record each as delivered or undelivered.

        One prompt, because two prompts typed into a busy pane join in its input queue with nothing between them.
        """
        try:
            runtime_for(s, agent_session, self._runs_dir, self._runtime_factory).prompt(
                handle_for(agent_session), '\n'.join(steer_line(c) for c in comments)
            )
        except Exception as e:  # NotSteerable (subprocess), a blocked agent or a closed pane. The daemon goes on.
            for comment in comments:
                payload: dict[str, object] = {'comment': comment.id, 'error': str(e)}
                emit(s, 'comment.undelivered', TargetKind.AGENT_SESSION, agent_session.id, payload=payload)
                self._tracer.event(
                    'comment.undelivered', comment=comment.id, agent_session=agent_session.id, error=str(e)
                )
            return
        for comment in comments:
            emit(s, 'comment.delivered', TargetKind.AGENT_SESSION, agent_session.id, payload={'comment': comment.id})
            self._tracer.event('comment.delivered', comment=comment.id, agent_session=agent_session.id)

    def _report_jobs(self) -> None:
        """Tell each idle implementer that left its finding which of its jobs still keep its run open. Once per implementer.

        An idle agent is finished only when nothing that it started still runs (``Runtime.wait``). That keeps a run
        open while its training job works. But an implementer that left its finding believes that it is done, and it cannot
        see what the runtime sees: a job that it forgot, or a waiter that never ends, such as a ``pgrep -f`` loop that
        finds its own command line. ``JOBS_LEFT`` on the implementer records the prompt, also when it failed. A runtime that
        reports no jobs (the subprocess runtime) never gets this prompt.
        """
        with self._db.session() as s:
            for agent_session in self._running_agent_sessions(s):
                told = select(Event.id).where(
                    Event.target == TargetRef(TargetKind.AGENT_SESSION, agent_session.id),
                    Event.kind == JOBS_LEFT,
                )
                if s.scalar(told) is not None or not left_finding(
                    s, s.get_one(Run, agent_session.run_id), agent_session
                ):
                    continue
                if not (jobs := self._jobs(s, agent_session)):
                    continue
                runtime, handle = (
                    runtime_for(s, agent_session, self._runs_dir, self._runtime_factory),
                    handle_for(agent_session),
                )
                if runtime.status(handle) is not AgentStatus.IDLE:  # it may still stop them in this turn
                    continue
                payload: dict[str, object] = {'jobs': jobs}
                named = '; '.join(cell(job, STEER_LINE_CHARS) for job in jobs[:JOBS_LEFT_SHOWN])
                try:
                    runtime.prompt(handle, JOBS_LEFT_PROMPT.format(jobs=named))
                except Exception as e:  # NotSteerable, a blocked agent or a closed pane. The daemon goes on.
                    payload['error'] = str(e)
                emit(s, JOBS_LEFT, TargetKind.AGENT_SESSION, agent_session.id, payload=payload)
                self._tracer.event(JOBS_LEFT, agent_session=agent_session.id, jobs=len(jobs))
            s.commit()

    def _wind_down(self) -> None:
        """Wind down the implementers of stopped campaigns and the agents that someone cancelled (``CANCEL_REQUESTED``).

        An implementer of a stopped campaign is told to commit and stop (``WRAP_UP``), and ended after the grace period
        (``STOP_GRACE_SECONDS``), or at once when the stop request asked to ``kill``. The runs then close and their
        commits are scored. An implementer in the middle of a turn takes the prompt only when the turn ends, which can be
        after the grace period. An implementer that the daemon ends (``STOP_ENDED``) without its finding closes its run as
        cut by the stop (``CAMPAIGN_STOPPED_ERROR``), not as a failed attempt. Sweeps of a stopped campaign are not
        changed, because their timeouts limit them and the closing review is one of them.

        A cancelled run or agent of any role (``agent_cancel``) is ended at once, or after the wrap-up prompt and the
        grace period when the cancel asked for it. Its run closes with ``CANCELLED_ERROR``. A cancelled request never
        starts, and its queued run fails. Both park the run's idea. A starting agent is left for a later tick, once it
        runs; if its start fails, the run is not launched again (``RunController._relaunch``).
        """
        with self._db.session() as s:
            requested = select(Event.target_id).where(Event.kind.in_(CANCEL_REQUESTED))
            on_agents = requested.where(Event.target_kind == TargetKind.AGENT_SESSION)
            on_runs = requested.where(Event.target_kind == TargetKind.RUN)
            runs_of_agents = select(AgentSession.run_id).where(
                AgentSession.id.in_(on_agents), AgentSession.run_id.is_not(None)
            )
            winding = (AgentSessionState.RUNNING, AgentSessionState.ENDING)  # ending: its kill is sent on each tick
            cancelled = AgentSession.state.in_((AgentSessionState.REQUESTED, *winding)) & (
                AgentSession.id.in_(on_agents)
                | AgentSession.run_id.in_(on_runs)
                | AgentSession.run_id.in_(runs_of_agents)
            )
            stopping = (
                select(AgentSession.id)
                .join(Campaign, AgentSession.campaign_id == Campaign.id)
                .where(AgentSession.run_id.is_not(None), AgentSession.state.in_(winding))
                .where(Campaign.status == CampaignStatus.STOPPED)
            )
            agent_sessions = s.scalars(
                select(AgentSession).where(cancelled | AgentSession.id.in_(stopping)).order_by(AgentSession.id)
            ).all()
            for agent_session in agent_sessions:
                if (cancel := agent_cancel(s, agent_session)) is not None:
                    reason, error = f'cancelled by {cancel.actor}', CANCELLED_ERROR
                    what = 'Your run was cancelled' if agent_session.run_id is not None else 'This sweep was cancelled'
                    cause, wrap_up = f'by {cancel.actor}', bool(cancel.payload.get('wrap_up'))
                else:
                    campaign = s.get_one(Campaign, agent_session.campaign_id)
                    reason, error = campaign.stopped_reason or CAMPAIGN_STOPPED_ERROR, CAMPAIGN_STOPPED_ERROR
                    what, cause, request = 'The campaign stopped', reason, requested_stop(s, campaign)
                    wrap_up = request is None or not request.payload.get('kill')
                if agent_session.state is AgentSessionState.REQUESTED:
                    self._close(s, agent_session, error)
                    if agent_session.run_id is not None:
                        self._fail_run(s, s.get_one(Run, agent_session.run_id), error)
                    continue
                told = s.scalar(
                    select(Event.at).where(
                        Event.target == TargetRef(TargetKind.AGENT_SESSION, agent_session.id),
                        Event.kind == WRAP_UP,
                    )
                )
                if not wrap_up or agent_session.state is AgentSessionState.ENDING:
                    self._end(s, agent_session, reason, error)
                elif told is None:
                    text = WRAP_UP_PROMPT.format(what=what, cause=cause, minutes=STOP_GRACE_SECONDS / 60)
                    self._wrap_up(s, agent_session, reason, text)
                elif utcnow() - told >= timedelta(seconds=STOP_GRACE_SECONDS):
                    self._end(s, agent_session, reason, error)
            s.commit()

    def _wrap_up(self, s: Session, agent_session: AgentSession, reason: str, text: str) -> None:
        """Send the wrap-up prompt. The grace period starts even if the prompt fails, as for a headless implementer."""
        payload: dict[str, object] = {'reason': reason}
        try:
            runtime_for(s, agent_session, self._runs_dir, self._runtime_factory).prompt(handle_for(agent_session), text)
        except Exception as e:  # NotSteerable (subprocess), a blocked agent or a closed pane.
            payload['error'] = str(e)
        emit(s, WRAP_UP, TargetKind.AGENT_SESSION, agent_session.id, payload=payload)
        self._tracer.event(WRAP_UP, agent_session=agent_session.id, **{k: str(v) for k, v in payload.items()})

    def _end(self, s: Session, agent_session: AgentSession, reason: str, error: str) -> None:
        """Kill a wound-down agent, once ``ending``: its executor stops it, and it reports.

        The row is ``ending``, with ``STOP_ENDED``, in a commit before the first kill. SQLite has one writer at a time,
        so a finding that the agent writes either committed before, and the agent finished first, or is refused.
        """
        if agent_session.state is not AgentSessionState.ENDING:
            agent_session.state = AgentSessionState.ENDING
            emit(s, STOP_ENDED, TargetKind.AGENT_SESSION, agent_session.id, payload={'reason': reason, 'error': error})
            self._tracer.event('agent_session.ended', agent_session=agent_session.id, reason=reason)
        # before the kill: the run that the kill ends reads it when it closes, and the executor writes in a session of
        # its own, which waits for this one's write lock
        s.commit()
        self._controller.executor.cancel(AgentKey(agent_session.id))  # on each tick until it ends: a kill can fail

    def _triggers(
        self, s: Session, campaign: Campaign, cfg: CampaignConfig, answer: Go | Stop, held: str | None
    ) -> None:
        """Stop the campaign if the answer is a stop. Otherwise queue runs and check every role's triggers.

        ``held`` is why the stop rule holds launches: then only a stop request gets here. ``[spend] closing_review
        = "skip"`` then requests no closing review and posts an escalation instead.

        Triggers requested everything that is pending when the campaign stops, so it is all cancelled. Requested rows
        are closed and queued runs fail with ``CAMPAIGN_STOPPED_ERROR``. Their ideas go back to ``ready``. The
        ``[memory]`` documents are synced, so that the closing sweep can search the campaign's reports. Then the
        ``stopped`` triggers fire once (``Trigger.ON_STOP``): by default, the watchdog's closing review. A stopped
        campaign gets nothing more from the triggers. Rows that a human inserts after the stop still launch. Implementers in
        flight are told to stop (``_wind_down``), and their commits are scored.
        """
        if isinstance(answer, Go):
            self._queue(s, campaign, cfg)
            for name, role in cfg.roles.sweeping().items():
                self._request(s, campaign, cfg, name, role)
            return
        reason = answer.reason
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
        for agent_session in s.scalars(
            select(AgentSession).where(
                AgentSession.campaign_id == campaign.id, AgentSession.state == AgentSessionState.REQUESTED
            )
        ):
            self._close(s, agent_session, CAMPAIGN_STOPPED_ERROR)

    def _request(
        self,
        s: Session,
        campaign: Campaign,
        cfg: CampaignConfig,
        name: RoleName,
        role: SweepRole,
        *,
        stopping: bool = False,
    ) -> None:
        """Insert requested rows when the role's triggers fire. One batch per call, however many triggers fire.

        The triggers fire in the order of the role's list (``piloop.plugins.trigger``): those with ``ON_STOP`` when the
        campaign is ``stopping``, else the others. A fire that needs a row of its
        own (``Fire.alone``) is requested alone when it comes first, and otherwise waits for a later tick. The others
        share one batch: the largest count of rows, with every reason. The first row gets the events of the fires. A
        trigger that raises is traced and does not fire.
        """
        latest = s.scalars(
            select(AgentSession)
            .where(AgentSession.campaign_id == campaign.id, AgentSession.kind == name)
            .order_by(AgentSession.id.desc())
            .limit(1)
        ).first()
        if latest is not None and latest.state is AgentSessionState.REQUESTED:
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
        rows = [AgentSession(kind=name, campaign_id=campaign.id, reason=reason) for _ in range(count)]
        s.add_all(rows)
        s.flush()
        for fire in fires:
            for kind, payload in fire.events:
                emit(s, kind, TargetKind.AGENT_SESSION, rows[0].id, payload=payload)
        self._tracer.event('agent_session.requested', campaign=campaign.name, kind=name, reason=reason, count=count)

    def _tick(self, s: Session, campaign: Campaign, cfg: CampaignConfig, name: RoleName, role: SweepRole) -> RoleTick:
        """Return what the triggers of role ``name`` read in this tick."""
        last = s.scalars(
            select(AgentSession)
            .where(AgentSession.campaign_id == campaign.id, AgentSession.kind == name)
            .order_by(AgentSession.id.desc())
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
            stall_step=lambda before: runs_without_keep(s, campaign, before) // stall(cfg)[0],
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

    def _jobs(self, s: Session, agent_session: AgentSession) -> list[str]:
        """Return the jobs that the implementer's runtime reports in flight, or none if the runtime fails (traced)."""
        try:
            return runtime_for(s, agent_session, self._runs_dir, self._runtime_factory).jobs(handle_for(agent_session))
        except Exception as e:  # A Herdr error must not stop the tick. The next tick checks the anomaly again.
            self._tracer.event('agent_session.jobs_failed', agent_session=agent_session.id, error=str(e))
            return []

    def _backlog(self, s: Session, campaign: Campaign, cfg: CampaignConfig, name: RoleName, buffer: int) -> Backlog:
        """Return the campaign's backlog as role ``name`` fills it: ``buffer`` beyond the run slots, its own sweeps."""
        target = cfg.run_slots + buffer
        if cfg.stop.max_runs is not None:
            target = min(target, cfg.stop.max_runs - runs(s, campaign))
        sweeps = count(
            s,
            select(AgentSession).where(
                AgentSession.campaign_id == campaign.id,
                AgentSession.kind == name,
                AgentSession.state.in_(LIVE),
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

    def _free_slots(
        self, s: Session, campaign: Campaign, cfg: CampaignConfig, roles: dict[RoleName, ImplementerRole]
    ) -> int:
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
                    target=TargetRef(TargetKind.IDEA, idea.id),
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
        self,
        s: Session,
        campaign: Campaign,
        cfg: CampaignConfig,
        name: RoleName,
        role: ImplementerRole,
        query: Select[Idea],
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

    def _explore(
        self, s: Session, campaign: Campaign, name: RoleName, role: ImplementerRole, query: Select[Idea]
    ) -> Idea | None:
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
        return next((i for i in pinned if branches(campaign, s.get(Candidate, i.base_candidate_id), trunk)), None)

    def _enqueue(
        self,
        s: Session,
        campaign: Campaign,
        cfg: CampaignConfig,
        role: RoleName,
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
            branch=branches(campaign, pinned, computed),
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
            AgentSession(
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
        """Return the harnesses at their ``max_concurrent`` cap, counting live agents and the implementers of queued runs.

        A queued run's harness was chosen when it was queued (``_enqueue``), so its requested implementer holds its place.
        A requested sweep gets its harness only when it is taken (``_take``).
        """
        if not cfg.max_concurrent:
            return set()
        queued = (AgentSession.state == AgentSessionState.REQUESTED) & AgentSession.kind.in_(cfg.roles.doing_runs())
        on = select(AgentSession.harness).where(
            AgentSession.campaign_id == campaign.id,
            AgentSession.state.in_(LIVE) | queued,
            AgentSession.harness.in_(cfg.max_concurrent),
        )
        agents = Counter(s.scalars(on))
        return {harness for harness, cap in cfg.max_concurrent.items() if agents[harness] >= cap}

    def _take(self, s: Session, campaign: Campaign, cfg: CampaignConfig) -> list[tuple[AgentSessionId, RunId | None]]:
        """Mark requested rows ``starting``, up to each role's ``max_concurrent``. Refuse a run role's row without a run.

        A sweep's harness is chosen here. A harness at its cap (``_full``) is skipped for the next one in the rotation.
        A sweep pinned to that harness stays requested. A run's harness was chosen when it was queued (``_reroute``).
        A request for a role that ``piloop.toml`` no longer has is refused, and its queued run fails
        (``ROLE_REMOVED``), so that its idea goes back to the queue.

        Returns ``(agent_session_id, run_id)`` pairs. The run is what an implementer executes. Nothing is taken while the startup
        breaker holds (``_paused``).
        """
        taken: list[tuple[AgentSessionId, RunId | None]] = []
        if self._paused(s, campaign):
            return taken
        roles = cfg.roles.all()
        unknown = select(AgentSession).where(
            AgentSession.campaign_id == campaign.id,
            AgentSession.state == AgentSessionState.REQUESTED,
            AgentSession.kind.not_in(roles),
        )
        for agent_session in s.scalars(unknown):
            self._close(s, agent_session, f'no [roles.{agent_session.kind}] in piloop.toml')
            if agent_session.run_id is not None:
                self._fail_run(s, s.get_one(Run, agent_session.run_id), ROLE_REMOVED)
        full = self._full(s, campaign, cfg)
        for kind, rc in roles.items():
            mine = (AgentSession.campaign_id == campaign.id, AgentSession.kind == kind)
            active = count(s, select(AgentSession).where(*mine, AgentSession.state.in_(LIVE)))
            requested = s.scalars(
                select(AgentSession)
                .where(*mine, AgentSession.state == AgentSessionState.REQUESTED)
                .order_by(AgentSession.id)
                .limit(max(rc.max_concurrent - active, 0))
            ).all()
            rotation = cfg.role_harnesses(kind)
            for agent_session in requested:
                if isinstance(rc, ImplementerRole):
                    if agent_session.run_id is None:
                        self._close(s, agent_session, NO_RUN_ERROR)
                        continue
                    if not self._reroute(s, cfg, agent_session, rotation, full):
                        continue
                else:
                    harness = agent_session.harness or free_harness(rotation, rotate(s, agent_session, rotation), full)
                    if harness is None or harness in full:
                        continue
                    agent_session.harness = harness
                agent_session.state = AgentSessionState.STARTING
                taken.append((agent_session.id, agent_session.run_id))
                full = self._full(s, campaign, cfg)
        return taken

    def _reroute(
        self, s: Session, cfg: CampaignConfig, agent_session: AgentSession, rotation: list[str], full: set[str]
    ) -> bool:
        """Return True if a run's implementer can launch. Move it to the next free harness if its harness left the rotation.

        The harness was in the rotation when the run was queued (``_enqueue``), but ``piloop init`` can change the
        rotation later. A harness that the idea pins does not change. Returns False, and the implementer stays requested,
        while every harness of the rotation is at its cap.
        """
        run = s.get_one(Run, agent_session.run_id)
        if run.idea.harness is not None or run.harness in rotation:
            return True
        harness = free_harness(rotation, rotate(s, agent_session, rotation), full)
        if harness is None:
            return False
        emit(s, 'run.rerouted', TargetKind.RUN, run.id, payload={'from': run.harness, 'to': harness})
        run.harness = agent_session.harness = harness
        run.model = agent_session.model = cfg.role_models(agent_session.kind)[harness]
        return True

    def _paused(self, s: Session, campaign: Campaign) -> bool:
        """Return True if ``STARTUP_BREAKER`` agents of the campaign failed at startup within ``STARTUP_WINDOW``.

        Requested rows wait, and launch when those failures are older than the window. The window is the back-off.
        Each pause posts one escalation (priority 1) with the load average and the failures' errors, which end with
        the agents' last screens. The escalation is updated in place while the pause holds, so an acknowledgement
        means that the human saw it. A new escalation is posted only after launches resume and pause again.
        """
        agent_sessions = select(AgentSession.id).where(AgentSession.campaign_id == campaign.id)
        deaths = s.scalars(
            select(Event)
            .where(
                Event.kind == STARTUP_ANOMALY,
                Event.target_kind == TargetKind.AGENT_SESSION,
                Event.target_id.in_(agent_sessions),
            )
            .where(Event.at >= utcnow() - STARTUP_WINDOW)
            .order_by(Event.id)
        ).all()
        latest = s.scalars(
            select(Event)
            .where(Event.kind.in_((LAUNCH_PAUSED, LAUNCH_RESUMED)))
            .where(Event.target == TargetRef(TargetKind.CAMPAIGN, campaign.id))
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
            target=TargetRef(TargetKind.CAMPAIGN, campaign.id),
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

    def _sweep(self, agent_session_id: AgentSessionId) -> None:
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
            agent_session = s.get_one(AgentSession, agent_session_id)
            campaign = s.get_one(Campaign, agent_session.campaign_id)
            cfg = config_of(campaign)
            role = role_of(s, agent_session)
            in_campaign = self._in_campaign(campaign, agent_session.kind)
            base = checkout_candidate(s, campaign)
            values = ', '.join(
                f'{t.primary.name}={"unmeasured" if (v := score(base, campaign.evaluator_id, t.primary)) is None else v}'
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
            lines += self._context(s, campaign, cfg, agent_session, now)
            incidents = assigned(s, agent_session_id)
            sections = [*lines, '']
            if incidents:
                sections += ['## Incidents to investigate', *incidents, '']
            sections.append(overview(s, campaign, cfg, now, partial(self._jobs, s)))
            if not incidents:
                sections.append(pack(s, campaign, cfg, workspace=workspace_dir(self._home, campaign.id)))
            brief = compose_brief(
                role,
                campaign,
                Agent(role=agent_session.kind, session=agent_session_id),
                reason=agent_session.reason or 'requested',
                context='\n'.join(sections),
                incident=bool(incidents),
                wake=lessons_overview(s, campaign, cfg.wake_budget(role.kind)),
            )
            campaign_path, campaign_id, base_sha = Path(campaign.path), campaign.id, base.commit_sha
        log_dir = self._agent_sessions_dir / str(agent_session_id)
        if in_campaign:
            artifact: Artifact = Checkout(path=campaign_path)
            cwd = campaign_path
        else:
            links = worktree_links(cfg, campaign_path, campaign_dir(self._home, campaign_id))
            artifact, cwd = GitTree(repo=campaign_path, commit=base_sha, links=links), log_dir / 'worktree'
        self._controller.launcher.launch(agent_session_id, brief=brief, cwd=cwd, log_dir=log_dir, artifact=artifact)

    def _context(
        self, s: Session, campaign: Campaign, cfg: CampaignConfig, agent_session: AgentSession, now: datetime
    ) -> list[str]:
        """Return the context lines of a sweep: a built-in role's own, by the class of its table.

        Another role that defines ideas (``files = "ideas"``) is told its ``max_ideas``, which ``low_water`` counts on.
        Research's context says it with the backlog.
        """
        match cfg.roles.sweeping()[agent_session.kind]:
            case ResearchRole() as research:
                return self._research_context(s, campaign, cfg, agent_session.kind, research, now)
            case ScoutRole(seeds=seeds):
                return scout.context(s, campaign.id, agent_session.kind, agent_session.id, seeds, now)
            case SweepRole(files=Files.IDEAS, max_ideas=most):
                return [f'- New ideas of your own in this sweep: at most {most} (`max_ideas`).']
            case _:
                return []

    def _research_context(
        self, s: Session, campaign: Campaign, cfg: CampaignConfig, name: RoleName, role: ResearchRole, now: datetime
    ) -> list[str]:
        """Return the context lines of a research sweep: the seeds, its share of the backlog, the stall mark.

        The backlog's target has the ``buffer`` of the role's first ``low_water`` trigger, or none without one.
        """
        buffer = next((t.buffer for t in role.triggers if isinstance(t, LowWaterTrigger)), 0)
        backlog = self._backlog(s, campaign, cfg, name, buffer)
        most = role.max_ideas
        without, (mark, why) = runs_without_keep(s, campaign), stall(cfg)
        pivot = min(most, 1) if without >= mark else 0  # A stall gets its pivot whatever the backlog holds.
        own = max(min(backlog.short + most, most), pivot)  # This sweep's share of the shortfall.
        seeds = count(s, select(Idea).where(Idea.campaign_id == campaign.id, Idea.status == IdeaStatus.SEED))
        stalled = ': stalled, so one of your own ideas pivots (section 3)' if pivot else ''
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

    def _in_campaign(self, campaign: Campaign, kind: RoleName) -> bool:
        """Return whether a sweep of ``kind`` works in the campaign checkout (``workdir = "repo"``)."""
        role = config_of(campaign).roles.sweeping().get(kind)
        return role is not None and role.workdir is Workdir.REPO

    def _reattach_sweep(self, agent_session_id: AgentSessionId) -> None:
        """Wait for the agent of a sweep that a previous daemon process started, until it ends."""
        self._controller.launcher.reattach(agent_session_id)

    def _collect(self) -> None:
        """Forget finished threads. If a thread died before its launch recorded anything, close its row here."""
        for agent_session_id, fut in list(self._futures.items()):
            if fut.done():
                del self._futures[agent_session_id]
                if (exc := fut.exception()) is not None:
                    self._tracer.event('agent_session.thread_died', agent_session=agent_session_id, error=str(exc))
                    self._close_dead(agent_session_id, f'{type(exc).__name__}: {exc}')
        self._scoring = {key: fut for key, fut in self._scoring.items() if not fut.done()}  # These never raise.

    def _close_dead(self, agent_session_id: AgentSessionId, error: str) -> None:
        with self._db.session() as s:
            self._close(s, s.get_one(AgentSession, agent_session_id), error)
            s.commit()

    def active(self) -> list[AgentSessionId]:
        """Return the ids of the implementers that are executing now."""
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
        """Wait up to ``timeout`` seconds for the implementers in flight to finish."""
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
        self._controller.hand_over()
        wait([*self._futures.values(), *self._scoring.values(), *self._archiving()], timeout=timeout)
        self._pool.shutdown(wait=False, cancel_futures=True)

    def _archiving(self) -> list[Future[None]]:
        """The archiver's job in flight: it prunes and evicts, so the next daemon must not start under it."""
        return self._archiver.running() if self._archiver is not None else []

    def stop(self, timeout: float) -> None:
        """Stop ticking, kill the agents in flight, fail their runs and release the thread pool.

        The executor stops first: no job is submitted after it, and it stops every job. Each job still reports, which
        closes its agent's row as stopped and lets its run keep what it made. ``timeout`` limits the wait for them.
        Then the rows that are still open are written here, as ``reap`` does for a crashed daemon.
        """
        self._stop.set()
        self._controller.stop(timeout)
        self.wait(timeout)
        self._fail_open(STOPPED_ERROR, reaped=False)
        wait(self._archiving(), timeout=timeout)
        self._pool.shutdown(wait=False, cancel_futures=True)


@dataclass(frozen=True)
class RoleTick:
    """The ``Tick`` of one sweeping role of one campaign: what its triggers read in this tick."""

    s: Session
    campaign: Campaign
    role: RoleName
    last: AgentSession | None
    max_concurrent: int
    interval: float
    backlog: Callable[[int], Backlog]
    incidents: Callable[[], list[Incident]]
    stall_step: Callable[[datetime | None], int]

    def _mine(self) -> Select[AgentSession]:
        return select(AgentSession).where(AgentSession.campaign_id == self.campaign.id, AgentSession.kind == self.role)

    def live(self) -> int:
        return count(self.s, self._mine().where(AgentSession.state.in_(LIVE)))

    def killed_streak(self) -> int:
        states = self.s.scalars(self._mine().with_only_columns(AgentSession.state).order_by(AgentSession.id.desc()))
        return len(list(itertools.takewhile(lambda state: state is AgentSessionState.KILLED, states)))

    def empty_sweeps(self, states: tuple[AgentSessionState, ...]) -> list[AgentSession]:
        """See ``Tick.empty_sweeps``.

        The new-results back-off of ``low_water`` skips killed sweeps. A killed sweep stopped early, so its empty
        result says nothing about what was left to propose. The idle rest counts killed sweeps. Otherwise, a harness
        that dies every time would be launched again on every tick.
        """
        signed = defined_by(self.s, self.campaign.id)
        ended = self.s.scalars(self._mine().where(AgentSession.state.in_(states)).order_by(AgentSession.id.desc()))
        return list(itertools.takewhile(lambda agent_session: f'{self.role}#{agent_session.id}' not in signed, ended))

    def routine(self) -> AgentSession | None:
        given = exists().where(
            Event.kind == ANOMALY_EVENT,
            Event.target_kind == TargetKind.AGENT_SESSION,
            Event.target_id == AgentSession.id,
        )
        return self.s.scalars(self._mine().where(~given).order_by(AgentSession.id.desc()).limit(1)).first()


def done_by(role: RoleName) -> ColumnElement[bool]:
    """Select the runs of ``role``.

    ``likely()`` tells SQLite that most runs match, as most runs are the implementer's. Without it, SQLite drives a join
    with ``ideas`` from ``runs`` and scans every run of every campaign.
    """
    return func.likely(Run.role == role, type_=Boolean)


def branches(campaign: Campaign, pinned: Candidate | None, computed: Candidate | None) -> bool:
    """Return True if a run on ``pinned`` is a branch run: the pinned base's bar is not the current head.

    A pin to a non-keep whose nearest keep is the head is not a branch. Its run is judged against the head
    (``ledger.bar_of``). Nor is a pin whose commit has the head's (``git.contains``), such as a compose that
    cherry-picked it: a keep of it builds on the head.
    """
    if pinned is None or computed is None:
        return pinned is not None
    if bar_of(pinned).id == computed.id:
        return False
    return not git.contains(git.open_repo(Path(campaign.path)), pinned.commit_sha, computed.commit_sha)


def steer_line(comment: Comment) -> str:
    """Format the one line that a running implementer gets for a comment: who wrote what, and how to read the rest.

    The line quotes the first line of the body, cut at ``STEER_LINE_CHARS``. The implementer can read the full text with
    the SQL tool that its brief names, so a long decision does not fill the pane.
    """
    body = comment.body.strip()
    first = body.splitlines()[0] if body else ''
    if len(first) > STEER_LINE_CHARS:
        first = first[: STEER_LINE_CHARS - 1] + '…'
    return (
        f'[piloop] {comment.kind} on {comment.target} by {comment.author}: {first}'
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
            f'- agent_session#{e.target_id} ({e.payload.get("kind")} on {e.payload.get("harness")}): {e.payload.get("error")}'
            for e in deaths[-STARTUP_BREAKER:]
        ),
    ]
    return '\n'.join(lines)


def free_harness(rotation: Sequence[str], turn: str, full: Collection[str]) -> str | None:
    """Return ``turn``, the rotation's pick, unless it is ``full``. Then return the first harness that is not full."""
    return turn if turn not in full else next((h for h in rotation if h not in full), None)


def launches(campaign: Campaign, cfg: CampaignConfig, held: str | None) -> bool:
    """Whether the tick launches the campaign's requested agents, given why the stop rule holds (``held``).

    An active campaign launches when nothing holds it: no hold and no pause. A stopped campaign launches its closing
    review and what a human requests, paused or not. While the stop rule holds, it launches unless ``[spend]
    closing_review`` is ``wait``.
    """
    if campaign.status is CampaignStatus.ACTIVE:
        return held is None and campaign.paused is None
    return held is None or cfg.spend is None or cfg.spend.closing_review is not ClosingReview.WAIT
