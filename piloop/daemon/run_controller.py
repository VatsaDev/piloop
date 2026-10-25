"""Execute one run: make a worktree, run the implementer, close the run, score its commits and settle the idea.

The run closes when its implementer ends, so the implementer slot is free while the daemon measures the commits. The idea stays
``scheduled`` until its candidates are judged, because a dependent idea builds on the keeps of its prerequisites.
"""

import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Final, Literal, assert_never

from pydantic import ValidationError
from sqlalchemy import func, select, update
from sqlalchemy.orm import Session, undefer

from piloop.actor import Actor, Agent, System
from piloop.agents.launcher import Launcher
from piloop.agents.results import (
    AgentEnded,
    CampaignResults,
    DaemonStopped,
    HandedOver,
    Lost,
    NotStarted,
    PromptOutcome,
    Ran,
    record,
)
from piloop.agents.roles import compose_brief, role
from piloop.campaign.campaigns import WORKSPACE, Sources, campaign_dir, checkout_of, record_of, repository, vcs_of
from piloop.campaign.stop import attempts, park_cancelled, run_cancel, timeout as agent_timeout
from piloop.config import TRANSCRIPT, CampaignConfig, Track
from piloop.db.events import emit, has, in_order, latest
from piloop.db.models import (
    AgentSession,
    AgentSessionState,
    Campaign,
    Candidate,
    Cause,
    Comment,
    CommentKind,
    Evaluation,
    Evaluator,
    Event,
    EventModel,
    Idea,
    IdeaStatus,
    Limit,
    Measurement,
    Origin,
    Run,
    RunEnd,
    RunStatus,
    latest_agent_session,
    utcnow,
)
from piloop.db.session import Database, write_transaction
from piloop.db.triggers import DAEMON_ACTOR
from piloop.ids import (
    AgentSessionId,
    CampaignId,
    CandidateId,
    CommentId,
    EvaluatorId,
    IdeaId,
    Revision,
    RunId,
    TargetKind,
    TargetRef,
)
from piloop.net.tokens import Tokens
from piloop.otel import Span, Tracer
from piloop.plugins import limits
from piloop.plugins.cas.contract import CAS, Cas
from piloop.plugins.executor.contract import (
    DEADLINE_ENV,
    EXECUTORS,
    Bar,
    EvaluationJob,
    EvaluationKey,
    Executor,
    ExecutorConfig,
    Host,
    Sample,
)
from piloop.plugins.network.contract import NETWORKS, Network, NetworkHost
from piloop.plugins.runtime.contract import ExitStatus
from piloop.plugins.vcs.contract import Repository, RunVersion, Transfer, Version
from piloop.plugins.verdict_rule.contract import Outcome
from piloop.reports.history import history
from piloop.reports.wake import lessons_overview
from piloop.runtime.factory import RuntimeFactory, make_runtime
from piloop.scoring import queue
from piloop.scoring.ledger import (
    MOVED,
    CandidateJudged,
    Lineage,
    RejudgeFailed,
    Trunk,
    answer_rejudge,
    bar_of,
    current_evaluator,
    head,
    judge,
    measured,
    raises_bar,
    rebase,
    remeasure_asked,
    reproposed,
    resolve,
    score,
    side,
    unjudged,
)
from piloop.scoring.verdict import UNMEASURED, judgeable, label, override, pending, refusal, rejudgeable, write
from piloop.settings import StorageSettings

RECOVERED_REASON = 'recovered: what the implementer of run#{run} left unsaved, never declared ready; not measured'
CUT_REASON = 'recovered: where run#{run} was when a daemon stop cut it, never declared ready; not measured'
SKIPPED_REASON = 'not evaluated: its version asked to be skipped'
type Unmeasured = Literal[Origin.RECOVERED, Origin.SKIPPED]
"""A version that the daemon does not measure: its recovered work, or one that asked to be skipped."""
UNMEASURED_REASONS: Final[dict[Unmeasured, str]] = {Origin.RECOVERED: RECOVERED_REASON, Origin.SKIPPED: SKIPPED_REASON}
NO_CHANGES = 'implementer saved no versions'
INTERRUPTED = (
    'implementer stopped without a version or a finding of its own; its unsaved changes are a recovered candidate'
)
RUNS = 'runs'
"""The directory under the home of each run's place: ``runs/<id>``, its implementer's log directory and worktree."""


class RecoveryRequeued(EventModel):
    """``_settle`` requeued an idea from the last commit of a run that did not succeed.

    Counting these events caps the requeues (the ``recovery_requeues`` of the run's role): candidates cannot, because
    two runs that leave the same tree share one candidate.
    """

    kind: Literal['idea.recovery_requeued'] = 'idea.recovery_requeued'
    run: RunId
    candidate: CandidateId


class RunStarted(EventModel):
    """A run's implementer was launched, with its brief: the run is ``running``."""

    kind: Literal['run.started'] = 'run.started'
    harness: str
    model: str


class RunRelaunched(EventModel):
    """A run's implementer failed to start, and the daemon launched another."""

    kind: Literal['run.relaunched'] = 'run.relaunched'
    agent_session: AgentSessionId
    """The implementer that failed to start."""
    launches: int


class RunReceived(EventModel):
    """The run's versions came into the repo of record, oldest first: what ``resume`` scores after a stop."""

    kind: Literal['run.received'] = 'run.received'
    revisions: list[Revision]


class RunFinished(EventModel):
    """A run closed (``close_run``): how it ended, and the text that says why."""

    kind: Literal['run.finished'] = 'run.finished'
    end: RunEnd
    error: str | None


WRAP_UP_LEAD_SECONDS = 600
"""The most seconds before its role timeout that the daemon tells an implementer to wrap up (``wrap_up_lead``)."""
STARTUP_RELAUNCHES = 2
"""How many times a run is queued again with a new implementer after its implementer failed to start. Then the run fails.

A failure at startup is a fault of the host or the harness, not an attempt at the idea. So it does not count as an
attempt or toward ``max_runs``."""


class RunController:
    """Run one ``Run`` to completion on the calling thread. ``Launcher.launch`` runs the agent.

    It owns the daemon's executor (``[plugins.executor]``), which runs every agent and every evaluation, and the core
    operations that apply their results (``CampaignResults``).
    """

    def __init__(
        self,
        db: Database,
        tracer: Tracer,
        home: Path,
        runtime_factory: RuntimeFactory = make_runtime,
        *,
        executor: ExecutorConfig,
        cas: Cas | None = None,
        reserve: int = StorageSettings().reserve,
        network: Network | None = None,
    ) -> None:
        self._db = db
        self._tracer = tracer
        self._runs_dir = home / RUNS
        self._home = home
        self._units_dir = home / 'units'
        self._cas = cas if cas is not None else CAS.selected({}).open(home)
        self.sources = Sources(self._cas, home, db)
        """The artifacts of jobs: a campaign's repo of record, and its linked inputs in the CAS."""
        self._config = executor
        """The executor's ``[plugins.executor]``, as ``config.toml`` has it."""
        self._limits: tuple[tuple[str, str, datetime], ...] = ()
        """The limits that the executor has, as read: ``configure`` hands it the parameters when they change."""
        results = CampaignResults(db, tracer, home, self._cas, lambda: self.launcher.cancelled.is_set())
        network = network if network is not None else NETWORKS.selected({}).open(NetworkHost(Tokens(db)))
        host = Host(
            db,
            results,
            tracer,
            home,
            self._cas,
            partial(checkout_of, db),
            partial(record_of, db),
            runtime_factory,
            reserve,
            network,
        )
        self.executor: Executor[ExecutorConfig] = self._config.open(host)
        self.launcher = Launcher(db, tracer, home, self.executor)
        self.configure()
        self._jobs: dict[int, EvaluationJob] = {}
        """The job of each evaluation that this daemon asked for, by its queue row, until the row goes: a lost one is
        submitted again. A row without one is another daemon's: a reap drops it, and what needs it asks again."""

    def configure(self) -> None:
        """Hand the executor its config with the live limits applied (``Executor.configure``), when one changed.

        The daemon calls it on each pass, so a limit holds from the next one. Limits that its parameters refuse (a
        ``config.toml`` that changed under them) leave the parameters as they were, and the event says why.
        """
        with self._db.session() as s:
            rows = s.scalars(select(Limit).order_by(Limit.key)).all()
        seen = tuple((r.key, json.dumps(r.value), r.at) for r in rows)
        if seen == self._limits:
            return
        self._limits = seen
        try:
            config = limits.applied(self._config, rows, EXECUTORS.name)
        except ValidationError as e:
            self._tracer.event('limits.refused', error=str(e))
            return
        self.executor.configure(config)
        self._tracer.event('limits.applied', limits=len(rows))

    def _wait(self, evaluation_id: int) -> bool:
        """Wait until the evaluation leaves the queue, and return True; False once the daemon stops.

        While it waits for a slot, and one is free, it submits the next ones (``dispatch``): a lost evaluation waits
        again, and nothing else may end to free a slot for it.
        """

        def done_or_dispatchable(s: Session) -> bool:
            evaluation = s.get(Evaluation, evaluation_id)
            if evaluation is None or evaluation.started_at is not None:
                return evaluation is None
            return (free := self.executor.capacity().evaluations) is None or free > 0

        while self._db.wait_until(done_or_dispatchable, give_up=self.launcher.cancelled.is_set):
            with self._db.session() as s:
                if s.get(Evaluation, evaluation_id) is None:
                    return True
            self.dispatch()
        return False

    def dispatch(self) -> None:
        """Submit the next evaluations of the queue while the executor has free slots, in the queue's order.

        The order and the fairness are the queue's (``queue.waiting``), whichever the executor. Called after each
        request, after each evaluation that ends, and on each pass.
        """
        with self.launcher.lock:
            if self.launcher.cancelled.is_set():
                return
            free = self.executor.capacity().evaluations
            with self._db.session() as s:
                write_transaction(s)
                starting: list[EvaluationJob] = []
                for evaluation in queue.waiting(s):
                    if free is not None and len(starting) >= free:
                        break
                    if (job := self._jobs.get(evaluation.id)) is not None:
                        evaluation.started_at = utcnow()
                        starting.append(job)
                s.commit()
            for job in starting:
                self.executor.submit(job)

    def stop(self, timeout: float) -> None:
        """Submit no more jobs, and stop the executor's: each still reports. Waits up to ``timeout`` for them.

        Nothing submits a job after this (``launcher.lock``).
        """
        self.launcher.cancel()
        self.executor.stop(timeout)

    def stop_cancelled(self) -> None:
        """Cancel each running evaluation that someone cancelled (``piloop eval cancel``).

        Its executor stops the evaluator, and the result records nothing (``queue.cancel`` recorded the crash).
        """
        with self._db.session() as s:
            cancelled = s.scalars(
                select(Evaluation.id).where(Evaluation.cancelled_by.is_not(None), Evaluation.started_at.is_not(None))
            ).all()
        for evaluation_id in cancelled:
            self.executor.cancel(EvaluationKey(evaluation_id))

    def hand_over(self) -> None:
        """Stop for an in-place restart. The agents keep running for the next daemon, whose executor adopts them."""
        self.launcher.handover.set()
        self.launcher.cancel()
        self.executor.hand_over()

    def close(self) -> None:
        """Release the executor. The daemon calls it last, before it closes the database."""
        self.executor.close()

    def execute(self, run_id: RunId, agent_session_id: AgentSessionId) -> None:
        """Make the worktree, run the implementer, evaluate and record. Never raises: failures are recorded on the run."""
        self._guarded(run_id, agent_session_id, lambda span: self._execute(run_id, agent_session_id, span))

    def reattach(self, run_id: RunId, agent_session_id: AgentSessionId) -> None:
        """Finish a run whose agent a previous daemon process started (``Launcher.reattach``). Never raises.

        The daemon watches the agent in the run's worktree until it ends. Then it scores and settles the run as
        ``execute`` does.
        """
        self._guarded(run_id, agent_session_id, lambda span: self._finish(run_id, agent_session_id, None, span))

    def _guarded(self, run_id: RunId, agent_session_id: AgentSessionId, body: Callable[[Span], None]) -> None:
        """Run ``body``. A failure fails the run, and closes its implementer if the failure came before the launch."""
        with self._tracer.span('piloop.run', run=run_id) as span:
            try:
                body(span)
            except Exception as e:
                span.error(e)
                error = f'{type(e).__name__}: {e}'
                with self._db.session() as s:
                    idea_id = s.get_one(Run, run_id).idea_id
                    unstarted = (AgentSession.id == agent_session_id) & (
                        AgentSession.state == AgentSessionState.STARTING
                    )
                    s.execute(
                        update(AgentSession)
                        .where(unstarted)
                        .values(state=AgentSessionState.KILLED, ended_at=utcnow(), error=error)
                    )
                    s.commit()
                self._close(span, run_id, RunEnd.FAILED, error)
                self._settle(span, run_id, idea_id, RunEnd.FAILED, startup_failed=False, last=None)

    def _execute(self, run_id: RunId, agent_session_id: AgentSessionId, span: Span) -> None:
        with self.launcher.lock:
            if self.launcher.cancelled.is_set():  # Queued after stop(). The scheduler already failed the row.
                self._stopped(span, run_id)
                return
            with self._db.session() as s:
                run = s.get_one(Run, run_id)
                idea = run.idea
                campaign = idea.campaign
                cfg = campaign.config
                base = s.get_one(Candidate, run.base_candidate_id)
                brief = run.brief or compose_brief(
                    role(cfg, run.role, Path(campaign.path)),
                    campaign,
                    Agent(role=run.role, session=agent_session_id),
                    reason=f'idea#{idea.id} is ready',
                    context=self._context(
                        s,
                        cfg,
                        idea,
                        base,
                        Agent(role=run.role, session=agent_session_id).signature,
                        agent_timeout(s, campaign, cfg, cfg.role(run.role).timeout, utcnow()),
                    ),
                    track=cfg.track(idea.track),
                    wake=lessons_overview(s, campaign, cfg.wake_budget(run.role)),
                )
                run.brief = brief
                run.status = RunStatus.RUNNING
                # A relaunch keeps the start time, so comments since then are sent to the new implementer again.
                run.started_at = run.started_at or utcnow()
                run.log_dir = str(self._runs_dir / str(run_id))
                emit(
                    s,
                    RunStarted(harness=run.harness, model=run.model),
                    TargetRef(TargetKind.RUN, run_id),
                    System.DAEMON,
                )
                s.commit()
        self._finish(run_id, agent_session_id, brief, span)

    def _finish(self, run_id: RunId, agent_session_id: AgentSessionId, brief: str | None, span: Span) -> None:
        """Run the agent, then record the outcome, score the commits and settle the idea.

        With a ``brief``, a new agent starts in a new worktree. With None, the daemon re-attaches to the agent that is
        already in the worktree. A handed-over agent leaves the worktree and the rows to the next daemon.
        """
        with self._db.session() as s:
            run = s.get_one(Run, run_id)
            campaign = run.idea.campaign
            cfg = campaign.config
            base_revision = s.get_one(Candidate, run.base_candidate_id).revision
            timeout = agent_timeout(s, campaign, cfg, cfg.role(run.role).timeout, utcnow())
            harness, model, idea_id, campaign_id = run.harness, run.model, run.idea_id, campaign.id
            vcs, campaign_path = vcs_of(campaign), Path(campaign.path)
        log_dir = self._runs_dir / str(run_id)
        repo = vcs.open(campaign_path)
        span.set(idea=idea_id, agent_session=agent_session_id, harness=harness, model=model)
        if brief is None:
            ended = self.launcher.reattach(agent_session_id)
        else:
            ended = self.launcher.launch(
                agent_session_id,
                brief=brief,
                place=run_place(run_id),
                artifact=self.sources.agent(self._campaign(campaign_id), base_revision),
                timeout=timeout,
                capture=run_id,
            )
        # a daemon stop owns the rows, also when it came just after the report applied
        stopping = self.launcher.cancelled.is_set()
        match ended:
            case HandedOver():  # the next daemon waits for the agent again
                span.set(status='handed_over')
            case DaemonStopped(work=work):
                self._cut(span, run_id, idea_id, agent_session_id, cfg.name, repo, base_revision, work)
            case Ran(work=work) if stopping:
                self._cut(span, run_id, idea_id, agent_session_id, cfg.name, repo, base_revision, work)
            case NotStarted() | Lost() if stopping:
                self._cut(span, run_id, idea_id, agent_session_id, cfg.name, repo, base_revision, None)
            case Lost(reason=reason):
                self._close(span, run_id, RunEnd.WORKER_LOST, reason)
                self._settle(span, run_id, idea_id, RunEnd.WORKER_LOST, startup_failed=False, last=None)
            case NotStarted():
                if not self._relaunch(span, run_id, agent_session_id, log_dir):
                    with self._db.session() as s:
                        end = RunEnd.CANCELLED if run_cancel(s, run_id) is not None else RunEnd.NOT_STARTED
                    self._close(span, run_id, end, None)
                    self._settle(span, run_id, idea_id, end, startup_failed=True, last=None)
            case Ran():
                self._ran(span, run_id, idea_id, campaign_id, agent_session_id, repo, base_revision, ended, log_dir)
            case _:
                assert_never(ended)

    def _cut(
        self,
        span: Span,
        run_id: RunId,
        idea_id: IdeaId,
        agent_session_id: AgentSessionId,
        name: str,
        repo: Repository,
        base_revision: Revision,
        work: Transfer | None,
    ) -> None:
        """Keep the work of a run that a daemon stop cut (``_keep_cut_work``). The stop writes the rows."""
        cut = not self._left_finding(run_id, idea_id, agent_session_id)  # else the stop closed it as handed off
        self._keep_cut_work(run_id, name, repo, self._received(run_id, repo, base_revision, work), cut=cut)
        self._stopped(span, run_id)

    def _stopped(self, span: Span, run_id: RunId) -> None:
        """Trace the end that a daemon stop writes on the run (``Scheduler._fail_run``), which owns the row."""
        with self._db.session() as s:
            run, agent = s.get_one(Run, run_id), latest_agent_session(s, run_id)
            if run.status is RunStatus.RUNNING and agent is not None and finding_of(s, run, agent) is not None:
                end, error = handed_off(s, run)
            else:
                end, error = RunEnd.DAEMON_STOPPED, None
        span.set(status=end.status, end=end, error=error)

    def _ran(
        self,
        span: Span,
        run_id: RunId,
        idea_id: IdeaId,
        campaign_id: CampaignId,
        agent_session_id: AgentSessionId,
        repo: Repository,
        base_revision: Revision,
        ended: Ran,
        log_dir: Path,
    ) -> None:
        """Close the run of an agent that worked (``run_outcome``), score its versions and settle its idea."""
        outcome = ended.process
        versions = self._received(run_id, repo, base_revision, ended.work)
        finding = self._left_finding(run_id, idea_id, agent_session_id)
        with self._db.session() as s:
            asked = open_escalation(s, s.get_one(Run, run_id))
            stop_ended = s.scalar(
                select(Event.id).where(
                    Event.target == TargetRef(TargetKind.AGENT_SESSION, agent_session_id), has(AgentEnded)
                )
            )
            cancelled = run_cancel(s, run_id) is not None
        facts = RunFacts(
            finding=finding,
            ended=stop_ended is not None,
            cancelled=cancelled,
            timed_out=outcome.status is ExitStatus.TIMED_OUT,
            error=outcome.error,
            asked=asked,
            versions=len(versions),
            recovered_only=bool(versions) and all(v.recovered for v in versions),
        )
        end, error = run_outcome(facts)
        self._close(span, run_id, end, error)
        # Score all commits, even if the implementer then timed out or crashed.
        last = self._score(run_id, campaign_id, repo, versions, log_dir) if versions else None
        self._settle(span, run_id, idea_id, end, startup_failed=False, last=last)

    def _relaunch(self, span: Span, run_id: RunId, agent_session_id: AgentSessionId, log_dir: Path) -> bool:
        """Queue the run again with a new implementer, up to ``STARTUP_RELAUNCHES`` times, after its implementer failed to start.

        The run stays open, so it keeps its slot, attempt number and brief. The ``implementer#<id>`` in the brief becomes the
        new implementer's id. The new implementer signs its notes and finding with its own id, and only its own finding counts
        (``_left_finding``). The scheduler launches the new agent session row like any other. The old implementer's transcript is
        renamed so that the new one does not overwrite it. A request made while the daemon stops waits for the next
        daemon, as every request does. A cancelled run (``run_cancel``) is not queued again.
        """
        with self.launcher.lock:
            with self._db.session() as s:
                run = s.get_one(Run, run_id)
                launches = (
                    s.scalar(select(func.count()).select_from(AgentSession).where(AgentSession.run_id == run_id)) or 0
                )
                if launches > STARTUP_RELAUNCHES or run_cancel(s, run_id) is not None:
                    return False
                run.status = RunStatus.QUEUED
                agent_session = AgentSession(
                    kind=run.role,
                    campaign_id=run.idea.campaign_id,
                    run_id=run_id,
                    harness=run.harness,
                    model=run.model,
                    reason=f'run#{run_id} relaunched: {run.role}#{agent_session_id} failed to start',
                )
                s.add(agent_session)
                s.flush()
                # Edit the brief, do not compose it again: a human may have changed it (``UPDATE runs SET brief``).
                signed = rf'\b({re.escape(run.role)})#{agent_session_id}\b'
                run.brief = re.sub(signed, rf'\g<1>#{agent_session.id}', run.brief or '')
                relaunched = RunRelaunched(agent_session=agent_session_id, launches=launches)
                emit(s, relaunched, TargetRef(TargetKind.RUN, run_id), System.DAEMON)
                s.commit()
            transcript = log_dir / TRANSCRIPT
            if transcript.exists():
                transcript.rename(log_dir / f'transcript-implementer{agent_session_id}.txt')
        span.set(status='relaunched')
        return True

    def _keep_cut_work(self, run_id: RunId, name: str, repo: Repository, versions: list[Version], *, cut: bool) -> None:
        """Keep the work of a run that a daemon stop interrupted, unsaved work included.

        What the agent left unsaved was saved on its own (``AgentJob.capture``).

        The stop owns the run's row. A run that the stop ``cut`` is not an attempt (``RunEnd.attempt``), so its
        commits are not measured: each new one is ``recovered`` (``CUT_REASON``), and its idea is requeued
        from the last one (``_requeue``), as after a timed-out run. A run whose implementer had left its finding was
        handed off: the stop closed it as finished, and the next daemon scores the pinned commits (``resume``).
        """
        if not versions:
            return
        for i, version in enumerate(versions):
            repo.keep(version.revision, RunVersion(name, run_id, i))
        if not cut:
            return
        with self._db.session() as s:
            run = s.get_one(Run, run_id)
            candidate = run.base
            for version in versions:
                candidate, created = resolve(
                    s, run.idea.campaign_id, version, parent=candidate, run_id=run_id, origin=Origin.RECOVERED
                )
                if created:
                    candidate.verdict_reason = CUT_REASON.format(run=run_id)
            if _requeuable(s, run):
                _requeue(s, run, candidate, RunEnd.DAEMON_STOPPED)
            s.commit()

    def _received(
        self, run_id: RunId, repo: Repository, base_revision: Revision, work: Transfer | None
    ) -> list[Version]:
        """Take the run's versions into the repo of record (``Repository.receive``), and return them, oldest first.

        They are recorded (``RunReceived``), for ``resume``. The worker saved the unsaved work first, as a version of its own, so its candidate is ``recovered``
        (``_score``).
        """
        versions = repo.receive(self._cas, work, base_revision) if work is not None else []
        with self._db.session() as s:
            received = RunReceived(revisions=[v.revision for v in versions])
            emit(s, received, TargetRef(TargetKind.RUN, run_id), System.DAEMON)
            s.commit()
        return versions

    def _campaign(self, campaign_id: CampaignId) -> Campaign:
        """The campaign's row, for ``Sources``, which runs outside every transaction: minutes the first time."""
        with self._db.session() as s:
            return s.get_one(Campaign, campaign_id, options=[undefer(Campaign.config)])

    def _left_finding(self, run_id: RunId, idea_id: IdeaId, agent_session_id: AgentSessionId) -> bool:
        """Return True if the agent signed a ``finding`` on its idea or its run (``finding_of``)."""
        with self._db.session() as s:
            return finding_of(s, s.get_one(Run, run_id), s.get_one(AgentSession, agent_session_id)) is not None

    def resume(self, run_id: RunId) -> None:
        """Score the commits of a closed run and settle its idea, after a daemon stopped while it scored them.

        The versions are the ones that the run received (``RunReceived``). Never raises. If scoring cannot resume, for example because
        the repo is gone, the run settles as failed. The idea then goes back to the queue if it has attempts left.
        """
        with self._tracer.span('piloop.resume', run=run_id) as span:
            with self._db.session() as s:
                run = s.get_one(Run, run_id)
                campaign = run.idea.campaign
                end, idea_id, campaign_id = run.end, run.idea_id, campaign.id
                name, vcs, campaign_path = campaign.config.name, vcs_of(campaign), Path(campaign.path)
                received = latest(s, RunReceived, Event.target == TargetRef(TargetKind.RUN, run_id))
            last = None
            try:
                repo = vcs.open(campaign_path)
                if received is not None:
                    versions = [repo.version(revision) for revision in received[1].revisions]
                else:
                    versions = repo.kept(name, run_id)
                span.set(versions=len(versions))
                last = self._score(run_id, campaign_id, repo, versions, self._runs_dir / str(run_id))
            except Exception as e:
                span.error(e)
                end = RunEnd.FAILED
            assert end is not None  # a run whose commits are scored is closed
            self._settle(span, run_id, idea_id, end, startup_failed=False, last=last)

    def remeasure(self, candidate_id: CandidateId) -> None:
        """Measure a candidate with the campaign's current evaluator, then judge it again if it is ``rejudgeable``.

        Never raises. If the evaluation cannot start, for example because the commit is gone, a crashed measurement is
        recorded. It is a result like any other, so the candidate is not tried again with this evaluator unless someone
        asks (``ledger.request_remeasure``). What someone asked for is a paired evaluation: ``repeats`` samples, each
        next to one of its bar's (``bar_of`` its parent). Then a ``crash``, ``discard`` or ``interesting`` that the
        daemon judged is judged again on the pairs (``_rejudge``), as after ``piloop measurement add``. What runs need,
        the head and the bars, gets one sample.
        """
        with self._db.session() as s:
            candidate = s.get_one(Candidate, candidate_id)
            campaign = s.get_one(Campaign, candidate.campaign_id)
            cfg = campaign.config
            evaluator_id = current_evaluator(s, campaign, cfg).id
            s.commit()
            asked = remeasure_asked(s, candidate)
            parent = candidate.parent
            # a merge that no run judges is measured as its run would: paired with its head when the track repeats
            paired = cfg.track(candidate.run.idea.track).repeats if unjudged(candidate) and candidate.run else None
            bar = bar_of(parent).id if (asked is not None or paired) and parent is not None else None
            repeats = asked.repeats if asked is not None else paired or 1
            campaign_id, revision = campaign.id, candidate.revision
            vcs, campaign_path = vcs_of(campaign), Path(campaign.path)
        eval_dir = campaign_dir(self._home, campaign_id) / 'measure' / f'{candidate_id}-e{evaluator_id}'
        try:
            repo = vcs.open(campaign_path)
            if not self._measure(
                None, campaign_id, cfg, revision, candidate_id, evaluator_id, eval_dir, bar=bar, repeats=repeats
            ):
                return  # a stop killed the evaluator: there is nothing new to judge
        except Exception as e:
            with self._db.session() as s:
                error = Sample(metrics={}, seconds=0.0, error=f'{type(e).__name__}: {e}')
                record(s, self._units_dir, None, candidate_id, evaluator_id, error)
                s.commit()
            return
        self._rejudge(candidate_id, repo, eval_dir)

    def rejudge(self, candidate_id: CandidateId) -> None:
        """Judge again a candidate that someone measured by hand (``ledger.rejudges``), with all its measurements.

        Never raises. When the judgement fails, for example because the repo is gone, a ``RejudgeFailed`` event records
        the error, and the request is answered, so it is not tried on every pass.
        """
        with self._db.session() as s:
            candidate = s.get_one(Candidate, candidate_id)
            campaign = s.get_one(Campaign, candidate.campaign_id)
            campaign_id, vcs, campaign_path = campaign.id, vcs_of(campaign), Path(campaign.path)
        try:
            eval_dir = campaign_dir(self._home, campaign_id) / 'measure' / f'{candidate_id}-rejudge'
            self._rejudge(candidate_id, vcs.open(campaign_path), eval_dir)
        except Exception as e:
            with self._db.session() as s:
                failed = RejudgeFailed(error=f'{type(e).__name__}: {e}')
                emit(s, failed, TargetRef(TargetKind.CANDIDATE, candidate_id), System.DAEMON)
                answer_rejudge(s, candidate_id)
                s.commit()
            self._tracer.event(failed.kind, candidate=candidate_id, error=str(e))

    def _rejudge(self, candidate_id: CandidateId, repo: Repository, log_dir: Path) -> None:
        """Judge again a run's candidate that is ``rejudgeable``, now that it has a new measurement.

        It is judged as its run would have judged it (``_judge``): against the bar of its parent, a keep moves the head
        only while that bar is the head, and else it is rebased onto the head (``_follow``). A crash that crashes again
        stays a crash.
        """
        with self._db.session() as s:
            candidate = s.get_one(Candidate, candidate_id)
            run = candidate.run
            if run is None or not (rejudgeable(candidate) or unjudged(candidate)):
                answer_rejudge(s, candidate_id)  # its verdict stays: nothing to judge
                s.commit()
                return
            assert candidate.parent is not None  # a run's candidate descends from the run's base
            cfg = run.idea.campaign.config
            scope = Scope(
                run.id,
                candidate.campaign_id,
                cfg,
                cfg.track(run.idea.track),
                repo,
                current_evaluator(s, run.idea.campaign, cfg).id,
                side(run),
            )
            s.commit()
            bar_id, revision = bar_of(candidate.parent).id, candidate.revision
        with self._tracer.span('piloop.candidate', run=scope.run_id, revision=revision, candidate=candidate_id) as span:
            notes = list(repo.version(revision).notes)
            judged = self._judge(scope, candidate_id, bar_id, span, judge_now=True, unmeasured=None, notes=notes)
        if judged.merged is not None:
            self._follow(scope, judged.merged, log_dir)

    def _score(
        self, run_id: RunId, campaign_id: CampaignId, repo: Repository, versions: list[Version], log_dir: Path
    ) -> CandidateId | None:
        """Turn each new version into a candidate, or find its existing candidate, then measure and judge it.

        Return the id of the candidate of the run's last commit, unless scoring was cancelled. A tree that is already a candidate keeps that candidate, and its verdict unless another run
        proposes it again (``ledger.reproposed``).

        The daemon scores with the campaign's current config, not the config at launch. A run that outlives a config
        change must not move the campaign back to the old evaluator. All commits are candidates and pinned first, so a
        commit that waits for the evaluations before it is a ``pending`` candidate, and after a stop, ``resume`` finds
        all of them. It judges the candidates that the stop left pending, including rebased ones
        (``_follow``).

        Each candidate is judged on the run's track (``ledger.judge``) against a bar. The bar starts at the run's base,
        or at the nearest keep if the base is not a keep (``ledger.bar_of``). Each keep becomes the new bar. A run that
        is not a branch run keeps on the trunk only while its bar is the head. If the head has moved past the bar, a
        keep is rebased onto the head and the merge is judged there (``_judge``, ``_follow``). A commit whose tree is
        already a candidate is that candidate. It is measured only if the current evaluator has not measured it, and it
        keeps its verdict, unless another run reached it (``ledger.reproposed``): then it is judged again on this
        run's bar.

        A version that asks to be skipped (``Version.skip``) is not measured: a new candidate is ``skipped``, with no
        verdict, and moves no bar. So a later version that does not ask, with the same tree, is measured and judged like
        a new one. Its notes are applied when it is judged (``_judge``). The version the worker saved of the
        implementer's unsaved work (``Version.recovered``) is not measured either: it is ``recovered``, and the same tree
        saved later is measured and judged like a new one.
        """
        with self._db.session() as s:
            run = s.get_one(Run, run_id)
            cfg = run.idea.campaign.config
            scope = Scope(
                run_id,
                campaign_id,
                cfg,
                cfg.track(run.idea.track),
                repo,
                current_evaluator(s, run.idea.campaign, cfg).id,
                side(run),
            )
            bar_id = bar_of(run.base).id
            candidate = run.base
            ids: list[CandidateId] = []
            for version in versions:
                candidate, _ = resolve(s, campaign_id, version, parent=candidate, run_id=run_id, origin=Origin.RUN)
                ids.append(candidate.id)
            s.commit()
        for i, version in enumerate(versions):
            repo.keep(version.revision, RunVersion(cfg.name, run_id, i))
        for i, (version, candidate_id) in enumerate(zip(versions, ids, strict=True)):
            revision, unmeasured = version.revision, unmeasured_of(version)
            with self._db.session() as s:
                candidate = s.get_one(Candidate, candidate_id)
                again = candidate.origin in UNMEASURED or reproposed(candidate, s.get_one(Run, run_id))
                judge_now = pending(candidate) or (unmeasured is None and again)
                needs_measurement = unmeasured is None and not measured(candidate, scope.evaluator_id)
            with self._tracer.span('piloop.candidate', run=run_id, revision=revision, candidate=candidate_id) as span:
                if unmeasured is not None:
                    span.set(status=unmeasured)
                elif not needs_measurement:
                    span.set(status='known')
                    self._tracer.event('candidate.known', run=run_id, revision=revision, candidate=candidate_id)
                elif not self._measure(
                    run_id,
                    campaign_id,
                    cfg,
                    revision,
                    candidate_id,
                    scope.evaluator_id,
                    log_dir / f'eval-{i}',
                    bar=bar_id if scope.track.repeats else None,
                    repeats=scope.track.repeats or 1,
                ):
                    span.set(status='cancelled')
                    return None
                judged = self._judge(
                    scope,
                    candidate_id,
                    bar_id,
                    span,
                    judge_now=judge_now,
                    unmeasured=unmeasured,
                    notes=list(version.notes),
                )
            if judged.merged is not None and not self._follow(scope, judged.merged, log_dir):
                return None
            if judged.advanced:
                bar_id = candidate_id
        with self._db.session() as s:
            shas = [v.revision for v in versions]
            mine = (
                Candidate.run_id == run_id,
                Candidate.origin == Origin.RUN,
                Candidate.verdict.is_(None),
                Candidate.revision.not_in(shas),
            )
            left = s.scalars(select(Candidate.id).where(*mine).order_by(Candidate.id)).all()
        for candidate_id in left:  # Rebased candidates that a daemon stop left unjudged.
            if not self._follow(scope, candidate_id, log_dir):
                return None
        return ids[-1]

    def _follow(self, scope: Scope, candidate_id: CandidateId, log_dir: Path) -> bool:
        """Measure a rebased candidate and judge it against its parent, the head it was merged onto.

        If the head moved again and the merge keeps against its parent, the merge is rebased in turn (``_judge``). This
        repeats until a merge is judged on the head or a merge conflicts. Returns False if the run was cancelled.
        """
        current: CandidateId | None = candidate_id
        while current is not None:
            with self._db.session() as s:
                candidate = s.get_one(Candidate, current)
                revision, parent_id = candidate.revision, candidate.parent_id
                needs_measurement = not measured(candidate, scope.evaluator_id)
                judge_now = pending(candidate)
            assert parent_id is not None  # A rebased candidate's parent is the head it was merged onto.
            with self._tracer.span('piloop.candidate', run=scope.run_id, revision=revision, candidate=current) as span:
                span.set(rebased=True)
                if needs_measurement and not self._measure(
                    scope.run_id,
                    scope.campaign_id,
                    scope.cfg,
                    revision,
                    current,
                    scope.evaluator_id,
                    log_dir / f'eval-rebased-{current}',
                    bar=parent_id if scope.track.repeats else None,
                    repeats=scope.track.repeats or 1,
                ):
                    span.set(status='cancelled')
                    return False
                judged = self._judge(scope, current, parent_id, span, judge_now=judge_now, unmeasured=None, notes=[])
            current = judged.merged
        return True

    def _measure(
        self,
        run_id: RunId | None,
        campaign_id: CampaignId,
        cfg: CampaignConfig,
        revision: Revision,
        candidate_id: CandidateId,
        evaluator_id: EvaluatorId,
        eval_dir: Path,
        *,
        bar: CandidateId | None = None,
        repeats: int = 1,
    ) -> bool:
        """Measure ``revision`` on the executor, and wait until its measurement is recorded. False if the daemon stopped.

        With ``bar``, it is a paired evaluation: ``repeats`` samples of ``revision``, each next to one of the bar's
        (``EvaluationJob.bar``). Without, ``repeats`` samples of ``revision`` alone.

        A request for a candidate that already waits or runs under the evaluator waits for that evaluation and submits
        nothing of its own (``queue.request``). It is submitted when the queue gives it a slot (``dispatch``), and its
        result records the measurement (``CampaignResults``). An evaluation that cannot start, because the commit cannot
        be checked out say, is a crash like one that fails, so every request ends with a measurement. A cancelled
        evaluation records nothing: ``queue.cancel`` recorded its crash.
        """
        artifact = self.sources.source(self._campaign(campaign_id), revision)
        with self.launcher.lock:
            if self.launcher.cancelled.is_set():
                return False
            with self._db.session() as s:  # two requests for one tree must not both insert it
                evaluation, mine = queue.request(s, candidate_id, evaluator_id)
                job = None
                if mine:
                    paired = None
                    if bar is not None:
                        bar_commit = s.get_one(Candidate, bar).revision
                        paired = Bar(candidate=bar, artifact=artifact.model_copy(update={'revision': bar_commit}))
                    job = EvaluationJob(
                        evaluation=evaluation.id,
                        candidate=candidate_id,
                        run=run_id,
                        runner=s.get_one(Evaluator, evaluator_id).runner,
                        artifact=artifact,
                        place=eval_dir.relative_to(self._home).as_posix(),
                        command=cfg.evaluate,
                        timeout=cfg.evaluate_timeout,
                        required=[m.name for m in cfg.metrics],
                        bar=paired,
                        repeats=repeats,
                    )
                s.commit()
                evaluation_id = evaluation.id
            if job is not None:
                self._jobs[evaluation_id] = job
        self.dispatch()
        with self._tracer.span('piloop.measure', run=run_id, revision=revision, candidate=candidate_id) as span:
            span.set(evaluation=evaluation_id, joined=not mine)
            measured = self._wait(evaluation_id)
            self._jobs.pop(evaluation_id, None)
            self.dispatch()  # its slot is free
            if not measured or self.launcher.cancelled.is_set():  # a stop's evaluation records nothing
                span.set(status='cancelled')
                return False
            with self._db.session() as s:
                measurement = s.scalars(
                    select(Measurement)
                    .where(Measurement.candidate_id == candidate_id, Measurement.evaluator_id == evaluator_id)
                    .order_by(Measurement.id.desc())
                    .limit(1)
                ).one()  # what it measured, or the crash that a cancel recorded
                span.set(
                    measurement=measurement.id,
                    error=measurement.error,
                    **{f'metric.{k}': v for k, v in measurement.metrics.items()},
                )
        return True

    def _judge(
        self,
        scope: Scope,
        candidate_id: CandidateId,
        bar_id: CandidateId,
        span: Span,
        *,
        judge_now: bool,
        unmeasured: Unmeasured | None,
        notes: list[str],
    ) -> Judged:
        """Set the daemon's verdict on a candidate while it is ``judgeable``, if ``judge_now``. Return the result.

        A known candidate keeps its verdict, because the same tree again (a revert) says nothing new about it, unless
        another run proposes it again (``_score``). ``judgeable`` is read in the transaction that judges, so a keep
        that another run made meanwhile, of the same tree, stays. An ``unmeasured`` commit gets no verdict: its origin
        is ``recovered`` or ``skipped`` instead, with the reason of ``UNMEASURED_REASONS``.

        A keep against a bar that is no longer the head stays off the trunk, unless the run is a branch run. The head
        moved while the run worked or while the daemon measured. The candidate becomes ``interesting``
        (``Cause.MOVED``) and is merged onto the head in the same transaction (``ledger.rebase``). The merge is judged
        next (``Judged.merged``). See ``_verdict``.

        Otherwise the commit's ``Piloop-Note`` ``notes`` are added after the verdict, signed by the run. They turn a
        discard or an interesting candidate into a noted ``interesting`` (``Cause.NOTE``). A keep or a crash keeps
        its verdict (``verdict.refusal``).
        """
        merged: CandidateId | None = None
        moved = False
        with self._db.session() as s:
            candidate = s.get_one(Candidate, candidate_id)
            if judge_now and judgeable(candidate):
                match unmeasured:
                    case Origin() as origin:
                        candidate.origin = origin
                        candidate.verdict_reason = UNMEASURED_REASONS[origin].format(run=scope.run_id)
                    case None:
                        verdict, reason, onto = self._verdict(s, scope, candidate, bar_id)
                        cause = Cause.MOVED if onto is not None else None
                        write(candidate, verdict, cause=cause, reason=reason, by=System.DAEMON)
                        if onto is not None:
                            moved = True
                            merged = rebase(s, scope.cfg, scope.repo, candidate, onto).rebased
                        judged = CandidateJudged(
                            verdict=verdict,
                            score=score(candidate, scope.evaluator_id, scope.track.primary),
                            reason=candidate.verdict_reason,
                        )
                        emit(s, judged, TargetRef(TargetKind.CANDIDATE, candidate_id), System.DAEMON)
                        answer_rejudge(s, candidate_id)
                        why = '; '.join(notes)
                        if why and not moved and refusal(candidate, Outcome.INTERESTING, Cause.NOTE) is None:
                            by = implementer_of(s, scope.run_id)
                            override(candidate, Outcome.INTERESTING, cause=Cause.NOTE, reason=why, by=by)
                    case _:
                        assert_never(unmeasured)
                s.commit()
            advanced = raises_bar(candidate, scope.side) or moved
            span.set(
                verdict=label(candidate),
                summary=candidate.summary,
                score=score(candidate, scope.evaluator_id, scope.track.primary),
            )
        return Judged(advanced=advanced, merged=merged)

    def _verdict(
        self, s: Session, scope: Scope, candidate: Candidate, bar_id: CandidateId
    ) -> tuple[Outcome, str, Candidate | None]:
        """Return the rule's verdict and reason against the run's bar, and the head to rebase onto if needed.

        A rebase is needed for a keep against a bar that is no longer the head, unless the run is a branch run. A
        candidate whose history already contains the head needs no merge. It is judged against the head.
        """
        trunk = head(s, s.get_one(Campaign, candidate.campaign_id), excluding=candidate)
        verdict, reason = judge(
            candidate,
            s.get_one(Candidate, bar_id),
            scope.evaluator_id,
            scope.cfg,
            scope.track,
            self._units_dir,
            head=trunk,
            side=scope.side,
        )
        match scope.side, verdict, trunk:
            case Trunk(), Outcome.KEEP, Candidate() as moved if moved.id != bar_id:
                if not scope.repo.contains(candidate.revision, moved.revision):
                    return Outcome.INTERESTING, MOVED.format(head=moved.id) + reason, moved
                again, why = judge(
                    candidate,
                    moved,
                    scope.evaluator_id,
                    scope.cfg,
                    scope.track,
                    self._units_dir,
                    head=moved,
                    side=Trunk(),
                )
                return again, f'the head candidate#{moved.id} is in its history: {why}', None
            case _:
                return verdict, reason, None

    def _close(self, span: Span, run_id: RunId, end: RunEnd, error: str | None) -> None:
        """Record the run's end when its implementer ends, so the slot is free while the commits are scored (``scoring``)."""
        with self.launcher.lock:
            if self.launcher.cancelled.is_set():  # stop() fails the row. Do not open it again.
                self._stopped(span, run_id)
                return
            with self._db.session() as s:
                close_run(s, s.get_one(Run, run_id), end, error).scoring = True
                s.commit()
        span.set(status=end.status, end=end, error=error)

    def _settle(
        self,
        span: Span,
        run_id: RunId,
        idea_id: IdeaId,
        end: RunEnd,
        *,
        startup_failed: bool,
        last: CandidateId | None,
    ) -> None:
        """After the run's commits are judged, set the idea to ``ready`` if it has attempts left, else to ``done``.

        The run's ``scoring`` ends here, whatever the idea's status. If no agent of the run started to work
        (``startup_failed``), the idea is ``parked`` instead, attempts left or not. It was never tried, a new run would
        fail the same way, and ``done`` would let the ideas that wait for it start without its result.

        A run that did not succeed and made commits requeues its idea from them, attempts left or not, up to
        ``recovery_requeues`` times of the run's role (``RecoveryRequeued`` events): the idea's base becomes the
        candidate of the ``last`` commit, so a new implementer starts from it and finishes the work (``_requeue``).

        A run whose implementer's escalation still waits for an answer (``open_escalation``) parks its idea first, whatever
        its status. A new run would stop on the same question, and ``done`` would start the ideas that wait for it.
        """
        with self.launcher.lock:
            if self.launcher.cancelled.is_set():  # stop() returns the idea to the queue; the run's end stands
                return
            with self._db.session() as s:
                run, idea = s.get_one(Run, run_id), s.get_one(Idea, idea_id)
                run.scoring = False
                if idea.status is IdeaStatus.SCHEDULED:  # A human may have parked or rejected it meanwhile.
                    if run.end is RunEnd.CANCELLED:
                        park_cancelled(s, run)
                    elif (asked := open_escalation(s, run)) is not None:
                        idea.status = IdeaStatus.PARKED
                        s.add(
                            Comment(
                                target=TargetRef(TargetKind.IDEA, idea.id),
                                author=DAEMON_ACTOR,
                                kind=CommentKind.NOTE,
                                body=f'parked: run#{run_id} ended while its escalation comment#{asked} waits for an'
                                ' answer. Answer it with a decision on this idea, which the next run reads in its'
                                ' brief, then set the idea ready',
                            )
                        )
                    elif last is not None and end is not RunEnd.FINISHED and _requeuable(s, run):
                        _requeue(s, run, s.get_one(Candidate, last), end)
                    elif startup_failed:
                        idea.status = IdeaStatus.PARKED
                        s.add(
                            Comment(
                                target=TargetRef(TargetKind.IDEA, idea.id),
                                author=DAEMON_ACTOR,
                                kind=CommentKind.NOTE,
                                body=f'parked: no agent of run#{run_id} started to work; fix what stopped them (the'
                                ' errors of its implementers, their anomaly.startup events), then set it ready',
                            )
                        )
                    elif end is not RunEnd.FINISHED and attempts(idea) < idea.max_runs:
                        idea.status = IdeaStatus.READY
                    else:
                        idea.status = IdeaStatus.DONE
                s.commit()

    def _context(
        self, s: Session, cfg: CampaignConfig, idea: Idea, base: Candidate, author: str, timeout: float
    ) -> str:
        """Return the implementer's context: its worktree, its time, the idea, its base, and the comments on the idea and its runs.

        A comment on a run is about the idea too. Examples are a human's decision during attempt 1, or a watchdog's
        finding on why it failed. The next attempt is a new run, so without these comments it would start with no
        context. All runs of the idea are included: the earlier attempts and this run, which can get comments while
        it is queued.
        """
        track = cfg.track(idea.track)
        evaluator_id = idea.campaign.evaluator_id
        on_idea = Comment.target == TargetRef(TargetKind.IDEA, idea.id)
        on_its_runs = (Comment.target_kind == TargetKind.RUN) & Comment.target_id.in_(
            select(Run.id).where(Run.idea_id == idea.id)
        )
        comments = s.scalars(select(Comment).where(on_idea | on_its_runs).order_by(Comment.id)).all()
        at_base = ', '.join(f'{t.primary.name}={score(base, evaluator_id, t.primary)}' for t in cfg.all_tracks)
        lines = [
            '- Your cwd is your checkout: make every edit, run every evaluation and save every version there.'
            f' Only its versions are scored. The campaign repo, {idea.campaign.path}, is read-only for you: read'
            ' its history and candidates, never edit, run or save in it.',
            f'- Your session ends {round(timeout / 60, 1):g} minutes after it starts, at `${DEADLINE_ENV}` (Unix time).'
            f' The daemon tells you to wrap up {round(wrap_up_lead(timeout) / 60, 1):g} minutes before, but a message'
            ' reaches you only between two tool calls: start no command that can run past the deadline. Save each'
            ' step that runs, and leave your finding before the end.',
            f'- Starting from candidate#{base.id}: {at_base} — {base.summary}',
        ]
        if base.run_id is not None and s.get_one(Run, base.run_id).idea_id == idea.id:
            unmeasured = ' It was never measured.' if base.origin is Origin.RECOVERED else ''
            lines.append(
                f'- candidate#{base.id} is where run#{base.run_id}, an earlier run of this idea, stopped.{unmeasured}'
                ' Read its diff against its parent, finish the work, and save what you would ship.'
            )
        if (bar := bar_of(base)) is not base:
            at_bar = ', '.join(f'{t.primary.name}={score(bar, evaluator_id, t.primary)}' for t in cfg.all_tracks)
            lines.append(
                f'- candidate#{base.id} is no keep: your versions are judged against candidate#{bar.id}, its nearest'
                f' keep ({at_bar}).'
            )
        if cfg.multitrack:
            guards = ', '.join(
                f'{m.name} (margin {m.margin:g}' + (f', floor {m.floor:g})' if m.floor is not None else ')')
                for m in track.guardrails
            )
            lines.append(
                f'- Your track is {track.name}: a keep must improve {track.primary.name} ({track.primary.direction} is'
                f' better) with confidence and hold {guards}.'
            )
        lines += [
            f'- Shared workspace for artifacts and notes to future implementers: `./{WORKSPACE}/` (in no version). Put what'
            f' your idea produces there under `./{WORKSPACE}/idea{idea.id}/`: it is archived when the idea is done, and'
            ' its local copy goes some days later. The files of a done idea come back with'
            f' `piloop restore {idea.campaign.name} <idea id>|<workspace path>`.',
            f'- Leave a finding on your idea when you are done, piped into the SQL tool: INSERT INTO comments'
            f" (target_kind, target_id, author, kind, body) VALUES ('idea', {idea.id}, '{author}', 'finding', '...').",
            '- Ask for what you cannot do or decide yourself with an escalation, never a note: INSERT INTO comments'
            f" (target_kind, target_id, author, kind, priority, body) VALUES ('idea', {idea.id}, '{author}',"
            " 'escalation', 1, '...').",
            '- The history below is what was tried in this campaign: do not repeat it; combine or go elsewhere.',
        ]
        lines += ['', f'## Your idea (idea#{idea.id}): {idea.title}', idea.hypothesis.rstrip()]
        if idea.provenance:
            lines += ['', f'Proposed by {idea.created_by}, prompted by: {idea.provenance.strip()}']
        if idea.plan:
            lines += ['', '### Plan', idea.plan.rstrip()]
        if comments:
            lines += ['', '## Comments on this idea and its runs', *(_comment_line(c) for c in comments)]
        return '\n'.join([*lines, '', history(s, idea.campaign, cfg)])


@dataclass(frozen=True)
class Scope:
    """The scope for scoring the candidates of one run: the run, the campaign's config at scoring time and the track."""

    run_id: RunId
    campaign_id: CampaignId
    cfg: CampaignConfig
    track: Track
    repo: Repository
    evaluator_id: EvaluatorId
    side: Lineage


@dataclass(frozen=True)
class Judged:
    """The result of ``_judge``: whether the run's bar moves to the candidate, and the merge to judge next."""

    advanced: bool
    """It kept against the run's bar, on the trunk or for a rebase. The run's next commit is judged against it."""
    merged: CandidateId | None
    """The candidate of its merge onto the head, if it was rebased and the merge is a new tree."""


def wrap_up_lead(timeout: float) -> float:
    """How many seconds before its role ``timeout`` an implementer is told to wrap up: a quarter, at most 10 minutes."""
    return min(WRAP_UP_LEAD_SECONDS, timeout / 4)


def _comment_line(c: Comment) -> str:
    where = f' on run#{c.target_id}' if c.target_kind is TargetKind.RUN else ''
    return f'- {c.author} ({c.kind}{where}): {c.body}'


def finding_of(s: Session, run: Run, agent_session: AgentSession) -> Comment | None:
    """Return the first ``finding`` that ``agent_session`` signed on its run's idea or on the run, as ``<role>#<id>`` or ``run#<id>``.

    A finding is the implementer's report that it is done. Only the implementer's own finding counts: a relaunched run's dead
    implementer did not finish it.
    """
    on_idea = Comment.target == TargetRef(TargetKind.IDEA, run.idea_id)
    on_run = Comment.target == TargetRef(TargetKind.RUN, run.id)
    signed = (Agent(role=agent_session.kind, session=agent_session.id).signature, f'run#{run.id}')
    return s.scalars(
        select(Comment)
        .where(on_idea | on_run, Comment.kind == CommentKind.FINDING, Comment.author.in_(signed))
        .order_by(Comment.id)
        .limit(1)
    ).first()


@dataclass(frozen=True)
class RunFacts:
    """The facts that decide how a run ended (``run_outcome``), gathered when its implementer ends."""

    finding: bool = False
    """The implementer left its own finding (``finding_of``): its report that it is done."""
    ended: bool = False
    """The daemon ended the implementer (``AgentEnded``)."""
    cancelled: bool = False
    """Someone cancelled the run (``run_cancel``)."""
    timed_out: bool = False
    """The implementer ran into its role's timeout."""
    error: str | None = None
    """How the implementer's process failed, a timeout included."""
    asked: CommentId | None = None
    """The implementer's escalation that waits for an answer (``open_escalation``)."""
    versions: int = 0
    recovered_only: bool = False
    """There are versions, and each is the one that the worker saved of unsaved work (``Version.recovered``)."""


def run_outcome(facts: RunFacts) -> tuple[RunEnd, str | None]:
    """Return how a run that ``facts`` describe ended, and the error that details it: the first case that matches."""
    match facts:
        # A finding is the implementer's report that it is done; nothing that ends its process after it undoes that. But a
        # question it left open blocks the run, or a retry would stop on it and done would release the waiting ideas.
        case RunFacts(finding=True, asked=int() as comment):
            return RunEnd.AWAITING_ANSWER, f'comment#{comment}'
        case RunFacts(finding=True):
            return RunEnd.FINISHED, None
        # A cancel, whether the daemon ended the implementer or it ended by itself first.
        case RunFacts(cancelled=True):
            return RunEnd.CANCELLED, None
        # A stop ended it, whatever the kill left in the outcome.
        case RunFacts(ended=True):
            return RunEnd.CAMPAIGN_STOPPED, None
        # It ran into its role's timeout.
        case RunFacts(timed_out=True, error=error):
            return RunEnd.TIMED_OUT, error
        # Its process failed.
        case RunFacts(error=str() as error):
            return RunEnd.FAILED, error
        # Its question is in the inbox. A commit does not make this a success.
        case RunFacts(asked=int() as comment):
            return RunEnd.AWAITING_ANSWER, f'comment#{comment}'
        # No commit and no finding: nothing was tried.
        case RunFacts(versions=0):
            return RunEnd.FAILED, NO_CHANGES
        # Only what the implementer left uncommitted: it did not finish.
        case RunFacts(recovered_only=True):
            return RunEnd.FAILED, INTERRUPTED
        # It committed and ended cleanly.
        case _:
            return RunEnd.FINISHED, None


def close_run(s: Session, run: Run, end: RunEnd, error: str | None) -> Run:
    """Close ``run`` as ``end``: its status, end, error and finish time, and ``RunFinished``. The caller commits.

    Every writer of a run's end goes through here.
    """
    run.status, run.end, run.error, run.finished_at = end.status, end, error, utcnow()
    emit(s, RunFinished(end=end, error=error), TargetRef(TargetKind.RUN, run.id), System.DAEMON)
    return run


def handed_off(s: Session, run: Run) -> tuple[RunEnd, str | None]:
    """The end and error of a run that a stop hands off because its implementer left its finding (``run_outcome``)."""
    return run_outcome(RunFacts(finding=True, asked=open_escalation(s, run)))


def open_escalation(s: Session, run: Run) -> CommentId | None:
    """Return the id of the first escalation that an implementer of ``run`` wrote on its run or idea and no reply answered.

    The implementer asked for something that it cannot do itself, and the answer has not reached it. An ack is not an
    answer: an orchestrator can ack and reply while the implementer exits, and the reply then never reaches it. So
    an escalation is answered when it is acked and a later comment that is not the daemon's reached an agent of the run
    (``comment.delivered``). An implementer signs as ``<role>#<id>`` or ``run#<id>``. Escalations of other agents on the
    same idea do not block the run.
    """
    sessions = s.scalars(select(AgentSession).where(AgentSession.run_id == run.id)).all()
    signed = [Agent(role=w.kind, session=w.id).signature for w in sessions]
    on_idea = Comment.target == TargetRef(TargetKind.IDEA, run.idea_id)
    on_run = Comment.target == TargetRef(TargetKind.RUN, run.id)
    asked = s.scalars(
        select(Comment)
        .where(
            on_idea | on_run,
            Comment.kind == CommentKind.ESCALATION,
            Comment.author.in_([*signed, f'run#{run.id}']),
        )
        .order_by(Comment.id)
    ).all()
    on_agents = Event.target_kind == TargetKind.AGENT_SESSION, Event.target_id.in_([w.id for w in sessions])
    read = [p.comment for _, p in in_order(s, PromptOutcome, *on_agents) if p.kind == 'comment.delivered']
    replies = s.scalars(select(Comment.id).where(Comment.id.in_(read), Comment.author != DAEMON_ACTOR)).all()
    return next((e.id for e in asked if e.acked_at is None or not any(r > e.id for r in replies)), None)


def _requeues(s: Session, idea: Idea) -> int:
    """How many times ``_settle`` requeued the idea from a run's commits (its ``RecoveryRequeued`` events)."""
    query = select(func.count()).where(Event.target == TargetRef(TargetKind.IDEA, idea.id), has(RecoveryRequeued))
    return s.scalar(query) or 0


def _requeuable(s: Session, run: Run) -> bool:
    """Return whether the idea of ``run`` may be requeued from its commits.

    The idea is still ``scheduled`` (a human may have parked it meanwhile), and the run's role allows one more requeue.
    A role that ``piloop init`` removed while the run ran requeues nothing.
    """
    role = run.idea.campaign.config.roles.doing_runs().get(run.role)
    scheduled = run.idea.status is IdeaStatus.SCHEDULED
    return scheduled and role is not None and _requeues(s, run.idea) < role.recovery_requeues


def _requeue(s: Session, run: Run, last: Candidate, end: RunEnd) -> None:
    """Requeue the idea of a run that ended ``end``, not finished, from its ``last`` version, with a note. The caller commits.

    When the head moved past that version, a run from it would be a branch run, whose keeps never move the head. The
    idea then starts on the head, and the note names the change to take.
    """
    idea = run.idea
    trunk = head(s, idea.campaign)
    moved = not run.branch and trunk is not None and branches(idea.campaign, last, trunk)
    idea.status, idea.base_candidate_id = IdeaStatus.READY, None if moved else last.id
    emit(s, RecoveryRequeued(run=run.id, candidate=last.id), TargetRef(TargetKind.IDEA, idea.id), System.DAEMON)
    body = (
        f'requeued on the head: run#{run.id} ended {end} after its versions, and the head moved past them. To start'
        f' from them, take the change from {bar_of(run.base).revision} to {last.revision} onto the head'
        if moved
        else f'requeued from candidate#{last.id}: run#{run.id} ended {end} after its versions; the next run starts'
        ' from its last one to finish the work'
    )
    s.add(Comment(target=TargetRef(TargetKind.IDEA, idea.id), author=DAEMON_ACTOR, kind=CommentKind.NOTE, body=body))


def branches(campaign: Campaign, pinned: Candidate | None, computed: Candidate | None) -> bool:
    """Return True if a run on ``pinned`` is a branch run: the pinned base's bar is not the current head.

    A pin to a non-keep whose nearest keep is the head is not a branch. Its run is judged against the head
    (``ledger.bar_of``). Nor is a pin whose version has the head's (``Repository.contains``), such as a compose that
    cherry-picked it: a keep of it builds on the head.
    """
    if pinned is None or computed is None:
        return pinned is not None
    if bar_of(pinned).id == computed.id:
        return False
    return not repository(campaign).contains(pinned.revision, computed.revision)


def unmeasured_of(version: Version) -> Unmeasured | None:
    """Return why the daemon does not measure ``version``: it is the saved unsaved work, or it asks to be skipped."""
    if version.recovered:
        return Origin.RECOVERED
    if version.skip:
        return Origin.SKIPPED
    return None


def run_place(run_id: int) -> str:
    """The place of a run's implementer under the daemon's home: its log directory, ``runs/<id>``."""
    return f'{RUNS}/{run_id}'


def implementer_of(s: Session, run_id: RunId) -> Actor:
    """The run's newest implementer, which a note from its finding is by; the daemon for a run that has none."""
    w = s.scalars(
        select(AgentSession).where(AgentSession.run_id == run_id).order_by(AgentSession.id.desc()).limit(1)
    ).first()
    return Agent(role=w.kind, session=w.id) if w is not None else System.DAEMON
