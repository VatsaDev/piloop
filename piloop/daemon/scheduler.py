"""The daemon loop: check each role's triggers for each campaign, launch requested agents and reap stale state.

A tick has two phases. First, the triggers (https://piloop.autoresearch.inc/concepts/roles) insert ``agent_sessions`` rows in state ``requested``. Then the
requested rows launch in id order, as far as the role's ``max_concurrent`` allows, while the executor has a free agent
slot (``Executor.capacity``). A free slot goes to the campaign with the fewest live agents, as the evaluation queue
gives its slots. Anyone can insert a requested row by hand, for example the CLI or an agent. Such a row launches in
the same second phase.

Before both phases, the tick sends new comments to the running agents through the executor (``Executor.prompt``). An
agent gets the ``note``, ``finding`` and ``decision`` comments on itself (``piloop prompt``), and an implementer also
those on its run or its idea that were written since the run started, one ``[piloop]`` line each (``steer_line``). An
event on the agent records each outcome (``PromptOutcome``): ``comment.queued``, then ``comment.delivered`` or
``comment.undelivered``. A comment with an outcome is never sent again. An agent that is still ``starting`` gets the
comments once it runs. A failed delivery is not retried.
"""

import itertools
import math
import os
import sys
import threading
from collections import Counter
from collections.abc import Callable, Collection, Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from functools import partial
from pathlib import Path
from queue import Empty, SimpleQueue
from typing import Literal

from pydantic import Field, ValidationError
from sqlalchemy import Boolean, ColumnElement, Select, delete, exists, func, or_, select
from sqlalchemy.orm import Session

from piloop import deprecated
from piloop.actor import Agent, System
from piloop.agents import scout
from piloop.agents.launcher import rotate
from piloop.agents.refill import usage_of
from piloop.agents.results import AgentEnded, PromptOutcome, StartupAnomaly, WrapUp, queue_prompt
from piloop.agents.roles import compose_brief, role_of
from piloop.campaign import spend
from piloop.campaign.anomaly import AnomalyReported
from piloop.campaign.campaigns import WORKSPACE, vcs_of, workspace_dir
from piloop.campaign.ideas import defined_by, park_stranded, runnable, supersede, waiting
from piloop.campaign.memory import sync_documents
from piloop.campaign.spend import SpendError
from piloop.campaign.stop import (
    CampaignClosed,
    CancelRequested,
    Ending,
    StopRequested,
    agent_cancel,
    answer_done,
    asked,
    attempts,
    cancelled,
    check as check_stop,
    held as held_now,
    limits,
    park_cancelled,
    requested,
    runs,
    runs_without_keep,
    settled,
    stall,
    timeout as agent_timeout,
    unclosed,
)
from piloop.config import CampaignConfig, ClosingReview, Spend
from piloop.daemon.archiver import Archiver, Archiving
from piloop.daemon.run_controller import (
    RUNS,
    RunController,
    branches,
    close_run,
    finding_of,
    handed_off,
    run_place,
    wrap_up_lead,
)
from piloop.db.events import emit, has, in_order
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
    EventModel,
    Idea,
    IdeaStatus,
    Pause,
    Run,
    RunEnd,
    RunStatus,
    campaign_runs,
    count,
    utcnow,
)
from piloop.db.requests import pending
from piloop.db.session import Database
from piloop.db.triggers import DAEMON_ACTOR
from piloop.ids import (
    AgentSessionId,
    CandidateId,
    CommentId,
    IdeaId,
    RoleName,
    RunId,
    TargetKind,
    TargetRef,
    TrackName,
)
from piloop.otel import Tracer
from piloop.plugins.executor.contract import AgentKey, LeftJob, Prompt
from piloop.plugins.role.contract import Files, ImplementerRole, SweepRole, ideas_taken
from piloop.plugins.role.research import ResearchRole
from piloop.plugins.role.scout import ScoutRole
from piloop.plugins.stop_rule.contract import GO, Fact, Go, Hold, Stop
from piloop.plugins.trigger.contract import TRIGGERS, Backlog, Fire, Incident, TrackBacklog
from piloop.plugins.trigger.low_water import LowWaterTrigger
from piloop.plugins.vcs.contract import NotARepository
from piloop.reports.overview import anomalies, assigned, overview, reported
from piloop.reports.pack import pack
from piloop.reports.wake import lessons_overview
from piloop.runtime.factory import RuntimeFactory, handle_for, make_runtime, runtime_for
from piloop.scoring.ledger import checkout_candidate, due, head, head_unmeasured, rejudges, score
from piloop.usage import Usage

AGENT_SESSIONS = 'agent_sessions'
"""The directory under the home of each sweep's place: ``agent_sessions/<id>``."""
NO_RUN_ERROR = 'an implementer needs a run; the scheduler requests them from ready ideas'
PUSHED_KINDS = tuple(kind for kind in CommentKind if kind is not CommentKind.ESCALATION)
"""Comment kinds that are sent to a running implementer. Escalations are not sent, because they are for the human."""
STEER_LINE_CHARS = 200
"""The number of characters of a comment's first line that a ``[piloop]`` line quotes. The implementer can query the rest."""
STOP_GRACE_SECONDS = 600
"""The seconds that an implementer that was told to wrap up has to save its work before the daemon ends it."""
WAIT_SECONDS = 120.0
"""How long ``Scheduler.wait`` waits by default before it fails: generous for a loaded host, since it returns as soon
as nothing is in flight."""
WRAP_UP_PROMPT = (
    '[piloop] {what} ({cause}). Save what you have now, with a summary that says where it stands.'
    ' Leave your finding and stop. This session ends in {minutes:g} minutes.'
)
EventModel.retire('agent_session.jobs_left')  # 0.2.0 listed the jobs of an idle implementer; its finding ends it now
FINDING_GRACE_SECONDS = 60
"""The seconds after its finding that the daemon ends an implementer, and every process it left: a last commit takes seconds."""
FOUND = 'left its finding'
"""The ``reason`` of the ``AgentEnded`` of an implementer that the daemon ended after its finding."""
TIMEOUT = 'role timeout'
"""The ``reason`` of the ``WrapUp`` of an implementer whose role timeout is ``wrap_up_lead`` away."""
STARTUP_BREAKER = 3
"""The number of a campaign's agents that can fail at startup within ``STARTUP_WINDOW`` before its launches pause.

A host that is too loaded to start agents makes every new agent fail, and each failure costs a start window."""
STARTUP_WINDOW = timedelta(minutes=10)


class LaunchPaused(EventModel):
    """The startup breaker paused a campaign's launches."""

    kind: Literal['launch.paused'] = 'launch.paused'
    comment: CommentId
    """The escalation."""
    deaths: int
    load: str
    """The load average when launches paused."""


class LaunchResumed(EventModel):
    """A campaign's paused launches resumed. It ends the pause that ``LaunchPaused`` started."""

    kind: Literal['launch.resumed'] = 'launch.resumed'
    comment: CommentId
    """The escalation of the pause."""


class RunQueued(EventModel):
    """The daemon queued a run of an idea."""

    kind: Literal['run.queued'] = 'run.queued'
    idea: IdeaId
    attempt: int
    harness: str
    track: TrackName
    priority_pick: IdeaId | None = None
    """The idea that the priority alone would have picked, when it is another one."""
    pinned_base: CandidateId | None = None
    """The idea's pinned base, when it has one."""
    computed_base: CandidateId | None = None
    """The base that the daemon would have picked, when the idea is pinned."""


class RunRerouted(EventModel):
    """The daemon moved a queued run to another harness of the rotation, because its own was at its cap."""

    kind: Literal['run.rerouted'] = 'run.rerouted'
    from_: str = Field(alias='from')
    to: str


class TimeCut(StrEnum):
    """What set an implementer's timeout, as the wrap-up prompt says it."""

    ROLE = 'the role timeout of {timeout:g}s'
    STOP = "the campaign's stop, {timeout:.0f}s after you started"


@dataclass(frozen=True)
class TimeLeft:
    """The time an implementer has left before its ``timeout`` ends it, in ``minutes``."""

    timeout: float
    minutes: float
    cut: TimeCut
    """What set the timeout."""


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
        self._runs_dir = home / RUNS
        self._agent_sessions_dir = home / AGENT_SESSIONS
        self._runtime_factory = runtime_factory
        # No bound: a run's thread waits in the evaluation queue while it scores, and a bound would let those waits
        # hold back agent launches. The roles' max_concurrent and the executor's slots bound the work itself.
        self._pool = ThreadPoolExecutor(max_workers=sys.maxsize, thread_name_prefix='agent')
        self._futures: dict[AgentSessionId, Future[None]] = {}
        self._scoring: dict[tuple[TargetKind, int], Future[None]] = {}
        self._prompted: set[int] = set()
        """The comments that this daemon process handed to the executor (``_push_comments``)."""
        """Scoring outside an implementer: a run's resumed scoring by run id, or a campaign's re-measure by campaign id."""
        self._stop = threading.Event()
        self._wake: SimpleQueue[None] = SimpleQueue()
        """Ends the wait of ``serve_forever`` between ticks (``interrupt``)."""
        self._archiver = (
            Archiver(db, archiving.cas, home, archiving.policy, tracer, self._pool, self._stop)
            if archiving is not None
            else None
        )
        """Archives the workspaces in the background. The daemon always passes ``archiving``; a test that does not
        archives nothing."""

    def reap(self) -> None:
        """Hand the jobs that a previous daemon process left to the executor (``Executor.adopt``). Fail the rest.

        The executor takes over what it can find: ``workers`` gives every live lease a full period, so nothing is queued
        again only because of the restart, and its worker re-attaches to each agent that still has its worktree, and
        reports the rest gone, which ends their runs ``worker_lost``, not as attempts. For each agent it adopts,
        the daemon waits for its end as if no daemon had stopped (``RunController.reattach``, or ``_reattach_sweep`` for
        a sweep). Both an in-place restart (``hand_over``) and a crash leave agents for this. An agent that it does not
        adopt is killed, and its row and its run fail. A closed run whose commits were not all scored
        (``runs.scoring``) is scored now (``RunController.resume``), whatever its idea's status. An idea that stayed
        ``scheduled`` does not run again, and no dependent idea takes it before its keeps are known. The worktrees of
        failed runs and sweeps are removed. The evaluation queue starts empty: its jobs were the previous daemon's, and
        what still needs a measurement asks again.
        """
        with self._tracer.span('piloop.reap'):
            # only reads: a job of an adopted agent stores its linked inputs, which can take a while, and a write
            # lock held meanwhile would make every command that writes fail with "database is locked"
            with self._db.session() as s:
                live = s.scalars(select(AgentSession).where(AgentSession.state.in_(LIVE))).all()
                runs = {w.id: w.run_id for w in live}
                left = [self._left(s, w) for w in live]
            with self._db.session() as s:
                s.execute(delete(Evaluation))
                s.commit()
            refused = set(self._controller.executor.adopt(left))
            adopted = [
                job.key.agent_session for job in left if isinstance(job.key, AgentKey) and job.key not in refused
            ]
            self._fail_open(RunEnd.STALE, reaped=True, sparing=adopted)
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
        '0.4.0',
        'an agent that a 0.2 daemon started (before leases): delete _running with Launcher.left and LeftJob.job',
    )
    def _running(self, s: Session, w: AgentSession) -> LeftJob | None:
        """The ``LeftJob`` of a running agent with its job, if its worktree exists, for an executor to adopt it.

        Only an agent that a daemon before leases started needs it: so an upgrade does not lose its work, it keeps
        running, and its run is scored when it ends.
        """
        campaign = s.get(Campaign, w.campaign_id) if w.campaign_id is not None else None
        if campaign is None:
            return None
        sources = self._controller.sources
        if w.run_id is not None:
            run = s.get_one(Run, w.run_id)
            if run.status is not RunStatus.RUNNING or not (self._runs_dir / str(w.run_id) / 'worktree').is_dir():
                return None
            artifact = sources.source(campaign, run.base.revision)
            return self._controller.launcher.left(s, w, artifact, run_place(w.run_id), w.run_id)
        if not (self._agent_sessions_dir / str(w.id) / 'worktree').is_dir():
            return None
        artifact = sources.source(campaign, checkout_candidate(s, campaign).revision)
        return self._controller.launcher.left(s, w, artifact, sweep_place(w.id), None)

    def _fail_open(self, end: RunEnd, *, reaped: bool, sparing: Collection[AgentSessionId] = ()) -> None:
        """End each run in flight as ``end`` and queue its idea again. Close each live agent except ``sparing``.

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
                self._fail_run(s, run, end)
                if reaped:
                    self._remove_checkouts(self._runs_dir / str(run.id), run.idea.campaign)
            for agent_session in s.scalars(select(AgentSession).where(AgentSession.state.in_(LIVE), ~spared)):
                # a ``starting`` row that its daemon left before its start window has no process
                if reaped and agent_session.runtime is not None:
                    self._kill_stale(s, agent_session)
                if reaped and agent_session.run_id is None and agent_session.campaign_id is not None:
                    campaign = s.get_one(Campaign, agent_session.campaign_id)
                    self._remove_checkouts(self._agent_sessions_dir / str(agent_session.id), campaign)
                self._close(s, agent_session, end)
            s.commit()

    def _fail_run(self, s: Session, run: Run, end: RunEnd) -> None:
        """End ``run`` as ``end`` and return its idea to the queue, unless a human parked or rejected it meanwhile.

        A run that the daemon stopped (``RunEnd.attempt``) is not an attempt, so its idea always goes back to
        ``ready``. A running run whose implementer left its finding is not failed: it is handed off (``_hand_off``).
        """
        agent_session = s.scalars(
            select(AgentSession).where(AgentSession.run_id == run.id).order_by(AgentSession.id.desc()).limit(1)
        ).first()
        if (
            run.status is RunStatus.RUNNING
            and agent_session is not None
            and finding_of(s, run, agent_session) is not None
        ):
            self._hand_off(s, run)
            return
        close_run(s, run, end, None)
        idea = run.idea
        if idea.status is IdeaStatus.SCHEDULED and end is RunEnd.CANCELLED:
            park_cancelled(s, run)
        elif idea.status is IdeaStatus.SCHEDULED:
            idea.status = IdeaStatus.READY if attempts(idea) < idea.max_runs else IdeaStatus.DONE
        self._tracer.event('run.aborted', run=run.id, end=end)

    def _hand_off(self, s: Session, run: Run) -> None:
        """Close a run whose implementer left its finding as finished, and leave its commits to be scored on the next start.

        The implementer reported that it is done. A job of its own may still keep the run open, but the run is not cut: its
        idea stays ``scheduled``, so no second run redoes the work. The run thread pins the commits
        (``RunController._keep_cut_work``), and ``reap`` scores them and settles the idea (``runs.scoring``). Its status is
        what the run's end would give (``run_outcome``).
        """
        end, error = handed_off(s, run)
        close_run(s, run, end, error).scoring = True
        self._tracer.event('run.handed_off', run=run.id, end=end)

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
            agent_session.usage = found
        elif found is not None:
            self._tracer.event('agent_session.usage_unknown', agent_session=agent_session.id, error=str(found))

    def _remove_checkouts(self, agent_dir: Path, campaign: Campaign) -> None:
        """Remove the checkouts under a run's or a sweep's directory. If the campaign's repo is gone, record an event."""
        try:
            repo = vcs_of(campaign).open(Path(campaign.path))
        except NotARepository:
            self._tracer.event('worktrees.orphaned', agent_dir=str(agent_dir), campaign_path=campaign.path)
            return
        for checkout in repo.checkouts(agent_dir):
            repo.discard(checkout)

    @deprecated.until(
        '0.4.0', 'a kill of an agent that a 0.2 daemon started (before leases): delete _kill_stale with _running'
    )
    def _kill_stale(self, s: Session, agent_session: AgentSession) -> None:
        """Ask the runtime to kill an agent that no executor adopted, which outlives its daemon. Best effort.

        Such an agent was started before leases, or its job ended with no report. Once every agent is an executor's,
        its executor stops it.
        """
        try:
            runtime_for(agent_session, self._runtime_factory).kill(handle_for(agent_session))
        except Exception as e:  # The agent can be gone already. The database row is what matters.
            self._tracer.event('agent_session.kill_failed', agent_session=agent_session.id, error=str(e))

    def tick(self) -> list[AgentSessionId]:
        """Send new comments to running implementers, run every campaign's triggers and launch requested agents.

        Returns the ids of the launched agent sessions. Before the triggers, the tick rejects the ideas that a newer idea
        superseded (``piloop.campaign.ideas.supersede``), and parks those after a prerequisite that ended without a keep
        (``park_stranded``). This also happens for a stopped campaign, which requests
        nothing and is measured again only on request (``ledger.due``). Then the tick asks the stop rule
        (``piloop.campaign.stop.check``) of each active campaign, and of each stopped one whose closing review waits
        for a hold (``spend.review_waits``). A campaign that it holds gets only the first step. A paused campaign
        (``piloop pause``) gets no triggers, does not stop at a limit of its stop rule, and launches nothing, and with
        ``all`` it is not measured again either. A stop request stops a held or paused campaign too. A stopped
        campaign launches its closing review, and what a human requests, whatever the pause; while the stop rule
        holds, as ``[spend] closing_review`` says (``launches``). The
        archiver starts the next archiving of a workspace that is due, or its upkeep, on a background thread
        (``piloop.daemon.archiver``). First of all, the executor gets the live limits that changed (``RunController.configure``).
        """
        self._collect()
        self._controller.configure()
        self._controller.executor.tick(utcnow())
        self._controller.stop_cancelled()
        self._controller.dispatch()
        self._push_comments()
        self._wind_down()
        self._read_spend()
        if self._archiver is not None:
            self._archiver.tick()
        launching: list[tuple[Campaign, CampaignConfig]] = []
        with self._db.session() as s:
            for campaign in s.scalars(select(Campaign)).all():
                try:
                    cfg = campaign.config
                except ValidationError as e:
                    self._tracer.event('campaign.invalid_config', campaign=campaign.name, error=str(e))
                    continue
                for idea_id, by in supersede(s, campaign.id).items():
                    self._tracer.event('idea.superseded', campaign=campaign.name, idea=idea_id, by=by)
                for idea_id, prerequisite in park_stranded(s, campaign.id).items():
                    reason = f'after idea#{prerequisite}, which ended without a keep'
                    self._tracer.event('idea.parked', campaign=campaign.name, idea=idea_id, reason=reason)
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
                if campaign.status is CampaignStatus.STOPPED and unclosed(s, campaign) and settled(s, campaign):
                    self._close_campaign(s, campaign, cfg, cfg.roles.sweeping())
                    held = held_now(s, campaign, cfg)  # a closing review may wait for the hold
                if launches(campaign, cfg, held):
                    launching.append((campaign, cfg))
            launched = self._launch(s, launching)
            s.commit()
        unsubmitted = self._controller.launcher.unsubmitted
        for agent_session_id, run_id in launched:
            unsubmitted.add(agent_session_id)
            body = (
                partial(self._controller.execute, run_id, agent_session_id)
                if run_id
                else partial(self._sweep, agent_session_id)
            )
            future = self._futures[agent_session_id] = self._pool.submit(body)
            # a body that ends before it submits (a run that fails first) must not hold the slot
            future.add_done_callback(lambda _, i=agent_session_id: unsubmitted.discard(i))
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
            todo: list[tuple[int, Path, Spend, float | None]] = []
            for campaign in s.scalars(select(Campaign)):
                try:
                    cfg = campaign.config
                except ValidationError:
                    continue  # The tick records this error for each campaign.
                if cfg.spend is None or not spend.due(campaign, cfg.spend):
                    continue
                if campaign.status is CampaignStatus.ACTIVE or spend.review_waits(s, campaign, cfg):
                    todo.append((campaign.id, Path(campaign.path), cfg.spend, limits(cfg).get(Fact.SPEND)))
        for campaign_id, path, budget, limit in todo:
            try:
                reading: float | SpendError = spend.read(budget.command, path)
            except SpendError as e:
                reading = e
            with self._db.session() as s:
                spend.record(s, s.get_one(Campaign, campaign_id), budget, limit, reading)
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
        """Hand each running agent's executor the comments that the agent has no outcome for (``Executor.prompt``).

        The events table records each outcome: ``PromptOutcome`` events on the agent, with the comment id. A
        comment is handed over once per daemon process (``_prompted``). One without an outcome after a restart goes
        again, and the executor answers it as it did the first time. An agent whose row is still ``starting`` (brief
        composed, process not yet up) gets the comments once it runs. A failed delivery is recorded and not retried: the
        comment is in the brief of the idea's next run anyway, and the agent would fail the same way each time.
        A tick that sends something makes one span.
        """
        with self._db.session() as s:
            running = s.scalars(
                select(AgentSession).where(AgentSession.state == AgentSessionState.RUNNING).order_by(AgentSession.id)
            ).all()
            pending = [
                (w.id, Prompt(c.id, steer_line(c)))
                for w in running
                for c in self._unseen(s, w)
                if c.id not in self._prompted
            ]
        if not pending:
            return
        with self._tracer.span('piloop.push', comments=len(pending)):
            for agent_session_id, prompt in pending:
                self._prompted.add(prompt.id)
                try:
                    self._controller.executor.prompt(AgentKey(agent_session_id), prompt)
                except Exception as e:  # the executor did not take it (a locked database): the next tick tries again
                    self._prompted.discard(prompt.id)
                    self._tracer.event(
                        'comment.unsent', comment=prompt.id, agent_session=agent_session_id, error=str(e)
                    )

    def _unseen(self, s: Session, agent_session: AgentSession) -> list[Comment]:
        """Return the comments for the agent that have no outcome yet: on the agent, or on an implementer's run or idea.

        For a run, only the comments since the run started: the brief has the older ones, because the transaction that
        composes the brief sets ``Run.started_at``. The comparison uses whole seconds. A raw INSERT sets ``created_at``
        to ``CURRENT_TIMESTAMP``, which has second precision, but ``started_at`` is a Python timestamp. So a comment
        written in the same second as the brief can reach the implementer twice. That is better than never. The agent's
        own comments are not sent back. An implementer signs as ``implementer#<id>`` (its ``PILOOP_ACTOR``) or ``run#<id>``.
        """
        on_agent = (Comment.target_kind == TargetKind.AGENT_SESSION) & (Comment.target_id == agent_session.id)
        own: tuple[str, ...] = (Agent(role=agent_session.kind, session=agent_session.id).signature,)
        since = agent_session.started_at
        run = s.get(Run, agent_session.run_id) if agent_session.run_id is not None else None
        if run is not None:
            on_run = (Comment.target_kind == TargetKind.RUN) & (Comment.target_id == run.id)
            on_idea = (Comment.target_kind == TargetKind.IDEA) & (Comment.target_id == run.idea_id)
            on_agent = on_agent | on_run | on_idea
            own, since = (*own, f'run#{run.id}'), run.started_at or since
        on_agent_session = Event.target == TargetRef(TargetKind.AGENT_SESSION, agent_session.id)
        settled = {p.comment for _, p in in_order(s, PromptOutcome, on_agent_session)}
        comments = s.scalars(
            select(Comment)
            .where(on_agent, Comment.kind.in_(PUSHED_KINDS))
            .where(func.strftime('%Y-%m-%d %H:%M:%S', Comment.created_at) >= since.strftime('%Y-%m-%d %H:%M:%S'))
            .order_by(Comment.id)
        ).all()
        return [c for c in comments if c.id not in settled and c.author not in own]

    def _wind_down(self) -> None:
        """End the implementers that left their finding, and wind down those of stopped campaigns and cancelled agents.

        An implementer is ended ``FINDING_GRACE_SECONDS`` after its finding, with every process that it left running: it
        reported that it is done, and a job that it forgot must not hold its slot to the role timeout. Its run is
        finished (``run_outcome``).

        A wrap-up prompt (``WrapUp``) is a note on the agent, which its executor delivers. An implementer whose role
        timeout is ``wrap_up_lead`` away is told to save its work and stop. Its timeout ends it.

        An implementer of a stopped campaign is told to save its work and stop, and ended after the grace period
        (``STOP_GRACE_SECONDS``), or at once when the stop request asked to end it ``now``. The runs then close and their
        commits are scored. An implementer that the daemon ends (``AgentEnded``) without its finding closes its run as
        cut by the stop (``RunEnd.CAMPAIGN_STOPPED``), not as a failed attempt. Sweeps of a stopped campaign are not
        changed, because their timeouts limit them and the closing review is one of them.

        A cancelled run or agent of any role (a pending ``CancelRequested``) is ended at once, or after the wrap-up prompt
        and the grace period when the cancel asked for it. Its run ends ``RunEnd.CANCELLED``. A cancelled request never
        starts, and its queued run fails. Both park the run's idea. A starting agent is left for a later tick, once it
        runs; if its start fails, the run is not launched again (``RunController._relaunch``). An ``ending`` agent gets
        its kill again on each tick until it ends: a kill can fail. Then the stops and cancels that are carried out are
        answered (``stop.answer_done``).
        """
        with self._db.session() as s:
            cancels = {
                agent_session.id: (request, cancel)
                for request, cancel in pending(s, CancelRequested)
                for agent_session in s.scalars(
                    select(AgentSession).where(
                        cancelled(request, cancel),
                        AgentSession.state.in_((AgentSessionState.REQUESTED, AgentSessionState.RUNNING)),
                    )
                )
            }
            winding = (AgentSessionState.RUNNING, AgentSessionState.ENDING)
            stopping = (
                select(AgentSession.id)
                .join(Campaign, AgentSession.campaign_id == Campaign.id)
                .where(AgentSession.run_id.is_not(None), AgentSession.state == AgentSessionState.RUNNING)
                .where(Campaign.status == CampaignStatus.STOPPED)
            )
            implementers = select(AgentSession).where(AgentSession.run_id.is_not(None), AgentSession.state.in_(winding))
            found = [w.id for w in s.scalars(implementers) if self._found(s, w)]
            agent_sessions = s.scalars(
                select(AgentSession)
                .where(
                    AgentSession.id.in_(cancels)
                    | AgentSession.id.in_(stopping)
                    | AgentSession.id.in_(found)
                    | (AgentSession.state == AgentSessionState.ENDING)
                )
                .order_by(AgentSession.id)
            ).all()
            for agent_session in agent_sessions:
                if agent_session.id in found:
                    self._end(s, agent_session, FOUND, RunEnd.FINISHED)
                    continue
                if agent_session.state is AgentSessionState.ENDING:
                    self._kill(s, agent_session)
                    continue
                if (cancel := cancels.get(agent_session.id)) is not None:
                    request, asked = cancel
                    reason, end = f'cancelled by {request.actor}', RunEnd.CANCELLED
                    what = 'Your run was cancelled' if agent_session.run_id is not None else 'This sweep was cancelled'
                    cause, ending = f'by {request.actor}', asked.ending
                else:
                    campaign = s.get_one(Campaign, agent_session.campaign_id)
                    reason, end = campaign.stopped_reason or 'campaign stopped', RunEnd.CAMPAIGN_STOPPED
                    what, cause = 'The campaign stopped', reason
                    on_campaign = Event.target == TargetRef(TargetKind.CAMPAIGN, campaign.id)
                    now = [stop for _, stop in pending(s, StopRequested, on_campaign) if stop.ending is Ending.NOW]
                    ending = Ending.NOW if now else Ending.WRAP_UP
                if agent_session.state is AgentSessionState.REQUESTED:
                    self._close(s, agent_session, reason)
                    if agent_session.run_id is not None:
                        self._fail_run(s, s.get_one(Run, agent_session.run_id), end)
                    continue
                told = s.scalar(
                    select(Event.at).where(
                        Event.target == TargetRef(TargetKind.AGENT_SESSION, agent_session.id), has(WrapUp)
                    )
                )
                if ending is Ending.NOW:
                    self._end(s, agent_session, reason, end)
                elif told is None:
                    text = WRAP_UP_PROMPT.format(what=what, cause=cause, minutes=STOP_GRACE_SECONDS / 60)
                    self._wrap_up(s, agent_session, reason, text)
                elif utcnow() - told >= timedelta(seconds=STOP_GRACE_SECONDS):
                    self._end(s, agent_session, reason, end)
            for agent_session in s.scalars(implementers.where(AgentSession.state == AgentSessionState.RUNNING)):
                if (left := self._time_left(s, agent_session)) is not None:
                    cause = left.cut.value.format(timeout=left.timeout)
                    text = WRAP_UP_PROMPT.format(what='Your time is almost up', cause=cause, minutes=left.minutes)
                    self._wrap_up(s, agent_session, TIMEOUT, text)
            answer_done(s)
            s.commit()

    @staticmethod
    def _found(s: Session, implementer: AgentSession) -> bool:
        """Whether the implementer left its finding ``FINDING_GRACE_SECONDS`` ago or more."""
        assert implementer.run_id is not None
        finding = finding_of(s, s.get_one(Run, implementer.run_id), implementer)
        return finding is not None and utcnow() - finding.created_at >= timedelta(seconds=FINDING_GRACE_SECONDS)

    @staticmethod
    def _time_left(s: Session, implementer: AgentSession) -> TimeLeft | None:
        """The time an implementer that was not told to wrap up has left, once it is ``wrap_up_lead`` or less.

        Its timeout is its role's, cut at the campaign's stop (``stop.timeout``), as when it started.
        """
        campaign = s.get_one(Campaign, implementer.campaign_id)
        cfg = campaign.config
        role = cfg.roles.doing_runs().get(implementer.kind)
        told = select(Event.id).where(Event.target == TargetRef(TargetKind.AGENT_SESSION, implementer.id), has(WrapUp))
        if role is None or implementer.started_at is None or s.scalar(told.limit(1)) is not None:
            return None
        timeout = agent_timeout(s, campaign, cfg, role.timeout, implementer.started_at)
        left = timeout - (utcnow() - implementer.started_at).total_seconds()
        cut = TimeCut.ROLE if timeout == role.timeout else TimeCut.STOP
        return TimeLeft(timeout, round(left / 60, 1), cut) if left <= wrap_up_lead(timeout) else None

    def _wrap_up(self, s: Session, agent_session: AgentSession, reason: str, text: str) -> None:
        """Leave the wrap-up prompt as a note on the agent, which its executor delivers (``_push_comments``).

        The grace period starts now, also when the note never reaches the agent.
        """
        told = WrapUp(reason=reason, comment=queue_prompt(s, agent_session.id, text, DAEMON_ACTOR))
        emit(s, told, TargetRef(TargetKind.AGENT_SESSION, agent_session.id), System.DAEMON)
        self._tracer.event(told.kind, agent_session=agent_session.id, reason=reason, comment=told.comment)

    def _end(self, s: Session, agent_session: AgentSession, reason: str, end: RunEnd) -> None:
        """Kill a wound-down agent, once ``ending``: its executor stops it, and it reports.

        The row is ``ending``, with ``AgentEnded``, in a commit before the first kill. SQLite has one writer at a time,
        so a finding that the agent writes either committed before, and the agent finished first, or is refused.
        """
        if agent_session.state is not AgentSessionState.ENDING:
            agent_session.state, agent_session.error = AgentSessionState.ENDING, reason
            emit(
                s,
                AgentEnded(reason=reason, end=end),
                TargetRef(TargetKind.AGENT_SESSION, agent_session.id),
                System.DAEMON,
            )
            self._tracer.event('agent_session.ended', agent_session=agent_session.id, reason=reason)
        self._kill(s, agent_session)

    def _kill(self, s: Session, agent_session: AgentSession) -> None:
        """Commit, then send the kill of an ``ending`` agent to its executor. Sent on each tick until it ends."""
        # before the kill: the run that the kill ends reads it when it closes, and the executor writes in a session of
        # its own, which waits for this one's write lock
        s.commit()
        self._controller.executor.cancel(AgentKey(agent_session.id))

    def _triggers(
        self, s: Session, campaign: Campaign, cfg: CampaignConfig, answer: Go | Stop, held: str | None
    ) -> None:
        """Stop the campaign if the answer is a stop. Otherwise queue runs and check every role's triggers.

        ``held`` is why the stop rule holds launches: then only a stop request gets here. ``[spend] closing_review
        = "skip"`` then closes the campaign with no closing review (``CampaignClosed``) and posts an escalation.

        Triggers requested everything that is pending when the campaign stops, so it is all cancelled. Requested rows
        are closed and queued runs end ``RunEnd.CAMPAIGN_STOPPED``. Their ideas go back to ``ready``. Implementers in
        flight are told to stop (``_wind_down``), and their commits are scored. Then the daemon closes the campaign
        (``_close_campaign``). A stopped campaign gets nothing more from the triggers. Rows that a human inserts after
        the stop still launch.
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
        s.flush()
        answer_done(s)  # a stop with ``now`` waits for ``_wind_down`` to end the running implementers
        if held is not None and cfg.spend is not None and cfg.spend.closing_review is ClosingReview.SKIP:
            spend.review_skipped(s, campaign, held)
            self._close_campaign(s, campaign, cfg, {})

    def _close_campaign(
        self, s: Session, campaign: Campaign, cfg: CampaignConfig, sweeping: Mapping[RoleName, SweepRole]
    ) -> None:
        """Close a stopped campaign: sync its ``[memory]`` documents, fire the ``stopped`` triggers of ``sweeping``.

        The tick closes a campaign that the daemon stopped once it ``settled``, with every sweeping role: by default,
        the watchdog's closing review, which can then search the campaign's reports. ``[spend] closing_review =
        "skip"`` closes it at the stop, with none. The stop and the close are events (``unclosed``), so a daemon that
        restarts in between still closes it, once.
        """
        try:
            with s.begin_nested():
                synced = sync_documents(s, campaign, cfg, DAEMON_ACTOR)
        except OSError as e:  # the repo could not be read: the closing review still happens
            self._tracer.event('documents.sync_failed', campaign=campaign.name, error=f'{type(e).__name__}: {e}')
        else:
            self._tracer.event('documents.synced', campaign=campaign.name, result=synced.line())
        for name, role in sweeping.items():
            self._request(s, campaign, cfg, name, role, stopping=True)
        s.flush()  # the status change's event first: the close comes after the stop
        emit(s, CampaignClosed(), TargetRef(TargetKind.CAMPAIGN, campaign.id), System.DAEMON)

    def _drain(self, s: Session, campaign: Campaign) -> None:
        mine = Idea.campaign_id == campaign.id
        for run in s.scalars(select(Run).join(Idea).where(mine, Run.status == RunStatus.QUEUED)):
            self._fail_run(s, run, RunEnd.CAMPAIGN_STOPPED)
        for agent_session in s.scalars(
            select(AgentSession).where(
                AgentSession.campaign_id == campaign.id, AgentSession.state == AgentSessionState.REQUESTED
            )
        ):
            self._close(s, agent_session, RunEnd.CAMPAIGN_STOPPED)

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
                    'trigger.failed', campaign=campaign.name, role=name, trigger=TRIGGERS.name_of(trigger), error=str(e)
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
            for event in fire.events:
                emit(s, event, TargetRef(TargetKind.AGENT_SESSION, rows[0].id), System.DAEMON)
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
            Incident(a.name, a.reported(now)) for a in anomalies(s, campaign, cfg) if a.incident and a.key not in given
        ]
        return found[::-1]

    def _backlog(self, s: Session, campaign: Campaign, cfg: CampaignConfig, name: RoleName, buffer: int) -> Backlog:
        """Return the campaign's backlog as role ``name`` fills it: ``buffer`` beyond the run slots, its own sweeps."""
        target = cfg.run_slots + buffer
        if (max_runs := limits(cfg).get(Fact.RUNS)) is not None:
            target = min(target, math.floor(max_runs) - runs(s, campaign))
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

        For each role, this is its ``max_concurrent`` minus its open runs. The sum is never more than the stop rule's
        limit on runs allows.
        """
        open_runs = campaign_runs(campaign.id).where(Run.status.in_(OPEN_RUNS))
        free = sum(
            max(role.max_concurrent - count(s, open_runs.where(done_by(name))), 0) for name, role in roles.items()
        )
        if (max_runs := limits(cfg).get(Fact.RUNS)) is not None:
            free = min(free, math.floor(max_runs) - runs(s, campaign))
        return max(free, 0)

    def _queue(self, s: Session, campaign: Campaign, cfg: CampaignConfig) -> None:
        """Queue a run, and request its agent, for each free slot with a runnable idea, within the limit on runs.

        Each role that does runs fills its own slots with the ideas that it takes (``takes``), in the order of
        ``Roles.doing_runs``. Its open runs count against its own ``max_concurrent``. Its ``explore_every`` and the
        track shares apply to its own runs. The limit on runs and the attempts of an idea count the runs of every role.

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
        queued = RunQueued(
            idea=idea.id,
            attempt=attempt,
            harness=harness,
            track=idea.track,
            priority_pick=by_priority.id if by_priority is not None and by_priority is not idea else None,
            pinned_base=base.id if pin is not None else None,
            computed_base=computed.id if pin is not None and computed is not None else None,
        )
        emit(s, queued, TargetRef(TargetKind.RUN, run.id), System.DAEMON)
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

    def _launch(
        self, s: Session, launching: Sequence[tuple[Campaign, CampaignConfig]]
    ) -> list[tuple[AgentSessionId, RunId | None]]:
        """Take the requested agents of ``launching`` while the executor has a free agent slot (``_take``).

        Each slot goes to the campaign with the fewest live agents, so that a campaign with many requests never holds
        back another. Returns ``(agent_session_id, run_id)`` pairs.
        """
        # before the executor's count: a job submitted in between then counts twice, for one tick, never in neither
        unsubmitted = len(self._controller.launcher.unsubmitted)
        free = self._controller.executor.capacity().agents
        if free is not None:
            free -= unsubmitted
        live = Counter(s.scalars(select(AgentSession.campaign_id).where(AgentSession.state.in_(LIVE))).all())
        left = list(launching)
        taken: list[tuple[AgentSessionId, RunId | None]] = []
        while left and (free is None or free > 0):
            campaign, cfg = nxt = min(left, key=lambda entry: (live[entry[0].id], entry[0].id))
            got = self._take(s, campaign, cfg, most=None if free is None else 1)
            if free is None or not got:
                left.remove(nxt)
            else:
                free -= len(got)
            live[campaign.id] += len(got)
            taken += got
        return taken

    def _take(
        self, s: Session, campaign: Campaign, cfg: CampaignConfig, *, most: int | None = None
    ) -> list[tuple[AgentSessionId, RunId | None]]:
        """Mark requested rows ``starting``, up to each role's ``max_concurrent``, and ``most`` in all.

        A run role's row without a run is refused.

        A sweep's harness is chosen here. A harness at its cap (``_full``) is skipped for the next one in the rotation.
        A sweep pinned to that harness stays requested. A run's harness was chosen when it was queued (``_reroute``).
        A request for a role that ``piloop.toml`` no longer has is refused, and its queued run fails
        (``RunEnd.ROLE_REMOVED``), so that its idea goes back to the queue.

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
                self._fail_run(s, s.get_one(Run, agent_session.run_id), RunEnd.ROLE_REMOVED)
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
                if most is not None and len(taken) >= most:
                    return taken
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
        emit(s, RunRerouted(from_=run.harness, to=harness), TargetRef(TargetKind.RUN, run.id), System.DAEMON)
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
        deaths = in_order(
            s,
            StartupAnomaly,
            Event.target_kind == TargetKind.AGENT_SESSION,
            Event.target_id.in_(agent_sessions),
            Event.at >= utcnow() - STARTUP_WINDOW,
        )
        on_campaign = TargetRef(TargetKind.CAMPAIGN, campaign.id)
        latest = s.scalars(
            select(Event)
            .where(has(LaunchPaused) | has(LaunchResumed), Event.target == on_campaign)
            .order_by(Event.id.desc())
            .limit(1)
        ).first()
        pause = latest.payload if latest is not None and isinstance(latest.payload, LaunchPaused) else None
        if len(deaths) < STARTUP_BREAKER:
            if pause is not None:
                emit(s, LaunchResumed(comment=pause.comment), on_campaign, System.DAEMON)
            return False
        escalation = s.get(Comment, pause.comment) if pause is not None else None
        if pause is None or escalation is None:
            self._escalate_pause(s, campaign, deaths)
        else:
            escalation.body = pause_body(deaths, pause.load)
        return True

    def _escalate_pause(self, s: Session, campaign: Campaign, deaths: Sequence[tuple[Event, StartupAnomaly]]) -> None:
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
        paused = LaunchPaused(comment=comment.id, deaths=len(deaths), load=load)
        emit(s, paused, TargetRef(TargetKind.CAMPAIGN, campaign.id), System.DAEMON)
        self._tracer.event(paused.kind, campaign=campaign.name, deaths=len(deaths), load=load)

    def _sweep(self, agent_session_id: AgentSessionId) -> None:
        """Run a sweep, an agent without a run, in a worktree at the head, with a brief made from its row.

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
            cfg = campaign.config
            role = role_of(s, agent_session)
            timeout = agent_timeout(s, campaign, cfg, cfg.role(agent_session.kind).timeout, utcnow())
            base = checkout_candidate(s, campaign)
            values = ', '.join(
                f'{t.primary.name}={"unmeasured" if (v := score(base, campaign.evaluator_id, t.primary)) is None else v}'
                for t in cfg.all_tracks
            )
            now = utcnow()
            cwd = (
                f'- Your cwd is a detached checkout of candidate#{base.id}, the head ({values}): read and run the'
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
            sections.append(overview(s, campaign, cfg, now))
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
        sources = self._controller.sources  # outside the session: it puts the linked inputs into the CAS
        artifact = sources.agent(campaign, base.revision)
        place = sweep_place(agent_session_id)
        self._controller.launcher.launch(agent_session_id, brief=brief, place=place, artifact=artifact, timeout=timeout)

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
        while self._wake.empty():
            self.tick()
            also()
            try:
                self._wake.get(timeout=tick_seconds)
            except Empty:
                continue
            return

    def interrupt(self) -> None:
        """Make ``serve_forever`` return after its current tick, or at once from its wait. Safe in a signal handler.

        A handler runs on the main thread, also inside the loop's wait. ``Event.set`` there could wait forever for the
        lock that the interrupted ``Event.wait`` holds. ``SimpleQueue.put`` is reentrant: it cannot.
        """
        self._wake.put(None)

    def wait(self, timeout: float = WAIT_SECONDS) -> None:
        """Wait for the agents and the scoring in flight to finish. It returns as soon as they have.

        Raises:
            TimeoutError: some are still in flight after ``timeout`` seconds. The message names them.
        """
        self._await_in_flight(timeout)
        if self._futures or self._scoring:
            left = [f'agent_session#{i}' for i in self._futures] + [f'{kind}#{i}' for kind, i in self._scoring]
            raise TimeoutError(f'still in flight after {timeout:g} s: {", ".join(left)}')

    def _await_in_flight(self, timeout: float) -> None:
        """Wait up to ``timeout`` seconds for the agents and the scoring in flight, then forget the finished ones."""
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
        self.interrupt()
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
        self.interrupt()
        self._controller.stop(timeout)
        self._await_in_flight(timeout)  # what is still open after it is written below
        self._fail_open(RunEnd.DAEMON_STOPPED, reaped=False)
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

    def cancel(self, agent_session: AgentSession, /) -> CancelRequested | None:
        cancel = agent_cancel(self.s, agent_session)
        return cancel[1] if cancel is not None else None

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
            has(AnomalyReported), Event.target_kind == TargetKind.AGENT_SESSION, Event.target_id == AgentSession.id
        )
        return self.s.scalars(self._mine().where(~given).order_by(AgentSession.id.desc()).limit(1)).first()


def sweep_place(agent_session_id: AgentSessionId) -> str:
    """The place of a sweep under the daemon's home: its log directory, with its worktree in it."""
    return f'{AGENT_SESSIONS}/{agent_session_id}'


def done_by(role: RoleName) -> ColumnElement[bool]:
    """Select the runs of ``role``.

    ``likely()`` tells SQLite that most runs match, as most runs are the implementer's. Without it, SQLite drives a join
    with ``ideas`` from ``runs`` and scans every run of every campaign.
    """
    return func.likely(Run.role == role, type_=Boolean)


def steer_line(comment: Comment) -> str:
    """Format what a running agent gets for a comment. A comment on the agent itself is its text, as written.

    Any other is one line: who wrote what, and how to read the rest. The line quotes the first line of the body, cut at
    ``STEER_LINE_CHARS``. The implementer can read the full text with the SQL tool that its brief names, so a long
    decision does not fill its turn.
    """
    if comment.target_kind is TargetKind.AGENT_SESSION:
        return comment.body
    body = comment.body.strip()
    first = body.splitlines()[0] if body else ''
    if len(first) > STEER_LINE_CHARS:
        first = first[: STEER_LINE_CHARS - 1] + '…'
    return (
        f'[piloop] {comment.kind} on {comment.target} by {comment.author}: {first}'
        f' — full text: SELECT body FROM comments WHERE id={comment.id}'
    )


def pause_body(deaths: Sequence[tuple[Event, StartupAnomaly]], load: str) -> str:
    """Format the pause escalation: the number of failures, the load average and the latest failures' errors."""
    minutes = STARTUP_WINDOW.total_seconds() / 60
    lines = [
        f'Launches paused: {len(deaths)} agents died at startup in the last {minutes:g} minutes. Load average'
        f' {load} on {os.cpu_count()} CPUs when launches paused. Launches resume automatically when fewer than'
        f' {STARTUP_BREAKER} deaths are that recent. If the host is overloaded, stop what overloads it. The latest'
        ' deaths:',
        *(f'- {e.target} ({death.role} on {death.harness}): {death.error}' for e, death in deaths[-STARTUP_BREAKER:]),
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
