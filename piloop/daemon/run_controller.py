"""Execute one run: make a worktree, run the implementer, close the run, score its commits and settle the idea.

The run closes when its implementer ends, so the implementer slot is free while the daemon measures the commits. The idea stays
``scheduled`` until its candidates are judged, because a dependent idea builds on the keeps of its prerequisites.
"""

import re
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import assert_never

from git import Repo
from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from piloop import deprecated, git
from piloop.actor import Actor, Agent, System
from piloop.agents.launcher import AGENT_REFS, Launcher
from piloop.agents.results import (
    STOP_ENDED,
    CampaignResults,
    DaemonStopped,
    HandedOver,
    Lost,
    NotStarted,
    Ran,
    record,
)
from piloop.agents.roles import compose_brief, role
from piloop.campaign.campaigns import WORKSPACE, campaign_dir, worktree_links
from piloop.campaign.stop import attempts, park_cancelled, run_cancel
from piloop.config import TRANSCRIPT, CampaignConfig, Track, config_of
from piloop.db.events import emit
from piloop.db.models import (
    AgentSession,
    AgentSessionState,
    Campaign,
    Candidate,
    Comment,
    CommentKind,
    Evaluation,
    Evaluator,
    Event,
    Idea,
    IdeaStatus,
    Measurement,
    Run,
    RunEnd,
    RunStatus,
    Verdict,
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
    RunId,
    TargetKind,
    TargetRef,
)
from piloop.otel import Span, Tracer
from piloop.plugins.executor.contract import Commits, EvaluationJob, EvaluationKey, Executor, GitTree, Host, selected
from piloop.plugins.host import PluginTable
from piloop.plugins.runtime.contract import ExitStatus
from piloop.reports.history import history
from piloop.reports.wake import lessons_overview
from piloop.runtime.factory import RuntimeFactory, make_runtime
from piloop.scoring import queue
from piloop.scoring.evaluate import EvalResult
from piloop.scoring.ledger import (
    DAEMON,
    MOVED,
    REJUDGE_FAILED,
    SKIPPED,
    bar_of,
    current_evaluator,
    head,
    judge,
    judgeable,
    judged_again,
    measured,
    note,
    raises_bar,
    rebase,
    rejudgeable,
    reproposed,
    resolve,
    score,
    skipped,
)

UNCOMMITTED_SUMMARY = 'piloop: uncommitted implementer changes'
RECOVERED_TRAILER = 'Piloop-Recovered'
"""The trailer of the commit the daemon makes of what an implementer left uncommitted: ``Piloop-Recovered: run#<id>``."""
RECOVERED_REASON = 'recovered: what the implementer of run#{run} left uncommitted, never declared ready; not measured'
NO_CHANGES = 'implementer produced no commits'
INTERRUPTED = (
    'implementer stopped without a commit or a finding of its own; its uncommitted changes are a recovered candidate'
)
RECOVERY_REQUEUED = 'idea.recovery_requeued'
"""The event on an idea that ``_settle`` requeued from recovered work, with ``{run, candidate}``. Counting these events
caps the requeues (the ``recovery_requeues`` of the run's role): candidates cannot, because two runs that leave the
same tree share one candidate."""
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
        executor: PluginTable | None = None,
        entire_version: str | None = None,
    ) -> None:
        self._db = db
        self._tracer = tracer
        self._runs_dir = home / 'runs'
        self._home = home
        self._units_dir = home / 'units'
        provider, parameters = selected(executor)
        results = CampaignResults(db, tracer, self._units_dir, lambda: self.launcher.cancelled.is_set())
        host = Host(db, results, tracer, home, runtime_factory, entire_version)
        self.executor: Executor = provider(host, parameters)
        self.launcher = Launcher(db, tracer, home, self.executor)
        self._jobs: dict[int, EvaluationJob] = {}
        """The job of each evaluation that this daemon asked for, by its queue row, until the row goes: a lost one is
        submitted again. A row without one is another daemon's: a reap drops it, and what needs it asks again."""

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
        request, after each evaluation that ends, and on each tick.
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
                self._settle(span, run_id, idea_id, RunEnd.FAILED, startup_failed=False, recovered=None)

    def _execute(self, run_id: RunId, agent_session_id: AgentSessionId, span: Span) -> None:
        with self.launcher.lock:
            if self.launcher.cancelled.is_set():  # Queued after stop(). The scheduler already failed the row.
                span.set(status='cancelled')
                return
            with self._db.session() as s:
                run = s.get_one(Run, run_id)
                idea = run.idea
                campaign = idea.campaign
                cfg = config_of(campaign)
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
                        self._runs_dir / str(run_id) / 'worktree',
                    ),
                    track=cfg.track(idea.track),
                    wake=lessons_overview(s, campaign, cfg.wake_budget(run.role)),
                )
                run.brief = brief
                run.status = RunStatus.RUNNING
                # A relaunch keeps the start time, so comments since then are sent to the new implementer again.
                run.started_at = run.started_at or utcnow()
                run.log_dir = str(self._runs_dir / str(run_id))
                emit(s, 'run.started', TargetKind.RUN, run_id, payload={'harness': run.harness, 'model': run.model})
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
            cfg = config_of(campaign)
            base_sha = s.get_one(Candidate, run.base_candidate_id).commit_sha
            harness, model, campaign_path, idea_id, campaign_id = (
                run.harness,
                run.model,
                Path(campaign.path),
                run.idea_id,
                campaign.id,
            )
        log_dir = self._runs_dir / str(run_id)
        repo = git.open_repo(campaign_path)
        wt_path = log_dir / 'worktree'
        links = worktree_links(cfg, campaign_path, campaign_dir(self._home, campaign_id))
        span.set(idea=idea_id, agent_session=agent_session_id, harness=harness, model=model)
        if brief is None:
            ended = self.launcher.reattach(agent_session_id)
        else:
            ended = self.launcher.launch(
                agent_session_id,
                brief=brief,
                cwd=wt_path,
                log_dir=log_dir,
                artifact=GitTree(repo=campaign_path, commit=base_sha, links=links),
                commits=commits_of(run_id, agent_session_id),
            )
        # a daemon stop owns the rows, also when it came just after the report applied
        stopping = self.launcher.cancelled.is_set()
        match ended:
            case HandedOver():  # the next daemon waits for the agent again
                span.set(status='handed_over')
            case DaemonStopped(head=head):
                self._cut(span, run_id, idea_id, agent_session_id, cfg.name, repo, base_sha, head)
            case Ran(head=head) if stopping:
                self._cut(span, run_id, idea_id, agent_session_id, cfg.name, repo, base_sha, head)
            case NotStarted() | Lost() if stopping:
                self._cut(span, run_id, idea_id, agent_session_id, cfg.name, repo, base_sha, None)
            case Lost(reason=reason):
                self._close(span, run_id, RunEnd.WORKER_LOST, reason)
                self._settle(span, run_id, idea_id, RunEnd.WORKER_LOST, startup_failed=False, recovered=None)
            case NotStarted():
                if not self._relaunch(span, run_id, agent_session_id, log_dir):
                    with self._db.session() as s:
                        end = RunEnd.CANCELLED if run_cancel(s, run_id) is not None else RunEnd.NOT_STARTED
                    self._close(span, run_id, end, None)
                    self._settle(span, run_id, idea_id, end, startup_failed=True, recovered=None)
            case Ran():
                self._ran(span, run_id, idea_id, campaign_id, agent_session_id, repo, base_sha, ended, log_dir)
            case _:
                assert_never(ended)

    def _cut(
        self,
        span: Span,
        run_id: RunId,
        idea_id: IdeaId,
        agent_session_id: AgentSessionId,
        name: str,
        repo: Repo,
        base_sha: str,
        head: str | None,
    ) -> None:
        """Keep the work of a run that a daemon stop cut (``_keep_cut_work``). The stop writes the rows."""
        cut = not self._left_finding(run_id, idea_id, agent_session_id)  # else the stop closed it as handed off
        self._keep_cut_work(run_id, name, repo, base_sha, head, noted=cut)
        self._release(repo, agent_session_id, head)
        span.set(status='cancelled')

    def _ran(
        self,
        span: Span,
        run_id: RunId,
        idea_id: IdeaId,
        campaign_id: CampaignId,
        agent_session_id: AgentSessionId,
        repo: Repo,
        base_sha: str,
        ended: Ran,
        log_dir: Path,
    ) -> None:
        """Close the run of an agent that worked (``run_outcome``), score its commits and settle its idea."""
        outcome = ended.process
        shas = _commits(repo, base_sha, ended.head)
        finding = self._left_finding(run_id, idea_id, agent_session_id)
        with self._db.session() as s:
            asked = open_escalation(s, s.get_one(Run, run_id))
            stop_ended = s.scalar(
                select(Event.id).where(
                    Event.target == TargetRef(TargetKind.AGENT_SESSION, agent_session_id),
                    Event.kind == STOP_ENDED,
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
            commits=len(shas),
            recovered_only=bool(shas) and all(repo.commit(sha).summary == UNCOMMITTED_SUMMARY for sha in shas),
        )
        end, error = run_outcome(facts)
        self._close(span, run_id, end, error)
        # Score all commits, even if the implementer then timed out or crashed.
        recovered = self._score(run_id, campaign_id, repo, shas, log_dir) if shas else None
        self._release(repo, agent_session_id, ended.head)
        self._settle(span, run_id, idea_id, end, startup_failed=False, recovered=recovered)

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
                emit(
                    s,
                    'run.relaunched',
                    TargetKind.RUN,
                    run_id,
                    payload={'agent_session': agent_session_id, 'launches': launches},
                )
                s.commit()
            transcript = log_dir / TRANSCRIPT
            if transcript.exists():
                transcript.rename(log_dir / f'transcript-implementer{agent_session_id}.txt')
        span.set(status='relaunched')
        return True

    def _keep_cut_work(
        self, run_id: RunId, name: str, repo: Repo, base_sha: str, head: str | None, *, noted: bool
    ) -> None:
        """Pin the work of a run that a daemon stop interrupted, uncommitted work included, and add a note on the run.

        What the agent left uncommitted was committed, and ``head`` pinned (``commits_of``).

        The stop owns the rows. A run that the stop cut is not an attempt (``RunEnd.attempt``): its work is not
        scored, and the note (``noted``) goes into the brief of the idea's next run, which can start from those
        commits. A run whose implementer had left its finding was handed off: the stop closed it as finished, and the next
        daemon scores the pinned commits (``resume``), so no note sends a new run to them.
        """
        shas = _commits(repo, base_sha, head)
        if not shas:
            return
        prefix = f'refs/piloop/{name}/r{run_id}/'
        for i, sha in enumerate(shas):
            git.pin(repo, f'{prefix}{i}', sha)
        if not noted:
            return
        with self._db.session() as s:
            s.add(
                Comment(
                    target=TargetRef(TargetKind.RUN, run_id),
                    author=DAEMON_ACTOR,
                    kind=CommentKind.NOTE,
                    body=f'interrupted by a daemon stop: its {len(shas)} commits are pinned, unscored, at {prefix}<n>'
                    f' (head {shas[-1]}). To start from them, run git cherry-pick {base_sha}..{shas[-1]}',
                )
            )
            s.commit()

    def _release(self, repo: Repo, agent_session_id: AgentSessionId, head: str | None) -> None:
        """Delete the ref at which the agent's head was pinned (``commits_of``), now that the run's refs pin its commits."""
        if head is not None:
            git.unpin(repo, f'{AGENT_REFS}{agent_session_id}')

    def _left_finding(self, run_id: RunId, idea_id: IdeaId, agent_session_id: AgentSessionId) -> bool:
        """Return True if the agent signed a ``finding`` on its idea or its run (``left_finding``)."""
        with self._db.session() as s:
            return left_finding(s, s.get_one(Run, run_id), s.get_one(AgentSession, agent_session_id))

    def resume(self, run_id: RunId) -> None:
        """Score the commits of a closed run and settle its idea, after a daemon stopped while it scored them.

        The commits are the ones that ``_score`` pinned. Never raises. If scoring cannot resume, for example because
        the repo is gone, the run settles as failed. The idea then goes back to the queue if it has attempts left.
        """
        with self._tracer.span('piloop.resume', run=run_id) as span:
            with self._db.session() as s:
                run = s.get_one(Run, run_id)
                campaign = run.idea.campaign
                end, idea_id, campaign_id = run.end, run.idea_id, campaign.id
                name, campaign_path = config_of(campaign).name, Path(campaign.path)
            recovered = None
            try:
                repo = git.open_repo(campaign_path)
                shas = git.pinned(repo, f'refs/piloop/{name}/r{run_id}/')
                span.set(commits=len(shas))
                recovered = self._score(run_id, campaign_id, repo, shas, self._runs_dir / str(run_id))
            except Exception as e:
                span.error(e)
                end = RunEnd.FAILED
            assert end is not None  # a run whose commits are scored is closed
            self._settle(span, run_id, idea_id, end, startup_failed=False, recovered=recovered)

    def remeasure(self, candidate_id: CandidateId) -> None:
        """Measure a candidate with the campaign's current evaluator. Its verdict does not change, unless it is a crash.

        Never raises. If the evaluation cannot start, for example because the commit is gone, a crashed measurement is
        recorded. It is a result like any other, so the candidate is not tried again with this evaluator unless someone
        asks (``ledger.request_remeasure``). A ``crash`` that the daemon judged, from a fault of the host for example,
        is judged again with the new measurement (``_rejudge``).
        """
        with self._db.session() as s:
            candidate = s.get_one(Candidate, candidate_id)
            campaign = s.get_one(Campaign, candidate.campaign_id)
            cfg = config_of(campaign)
            evaluator_id = current_evaluator(s, campaign, cfg).id
            s.commit()
            campaign_id, campaign_path, sha = campaign.id, Path(campaign.path), candidate.commit_sha
        eval_dir = campaign_dir(self._home, campaign_id) / 'measure' / f'{candidate_id}-e{evaluator_id}'
        try:
            repo = git.open_repo(campaign_path)
            if not self._measure(None, campaign_id, cfg, repo, sha, candidate_id, evaluator_id, eval_dir):
                return  # a stop killed the evaluator: there is nothing new to judge
        except Exception as e:
            with self._db.session() as s:
                error = EvalResult({}, 0.0, f'{type(e).__name__}: {e}')
                record(s, self._units_dir, None, candidate_id, evaluator_id, error)
                s.commit()
            return
        self._rejudge(candidate_id, repo, eval_dir)

    def rejudge(self, candidate_id: CandidateId) -> None:
        """Judge again a candidate that someone measured by hand (``ledger.rejudges``), with all its measurements.

        Never raises. When the judgement fails, for example because the repo is gone, a ``REJUDGE_FAILED`` event with
        the error answers the request, so it is not tried on every tick.
        """
        with self._db.session() as s:
            candidate = s.get_one(Candidate, candidate_id)
            campaign_id, campaign_path = candidate.campaign_id, Path(s.get_one(Campaign, candidate.campaign_id).path)
        try:
            eval_dir = campaign_dir(self._home, campaign_id) / 'measure' / f'{candidate_id}-rejudge'
            self._rejudge(candidate_id, git.open_repo(campaign_path), eval_dir, rejudgeable)
        except Exception as e:
            with self._db.session() as s:
                payload: dict[str, object] = {'error': f'{type(e).__name__}: {e}'}
                emit(s, REJUDGE_FAILED, TargetKind.CANDIDATE, candidate_id, payload=payload)
                s.commit()
            self._tracer.event(REJUDGE_FAILED, candidate=candidate_id, error=str(e))

    def _rejudge(
        self, candidate_id: CandidateId, repo: Repo, log_dir: Path, again: Callable[[Candidate], bool] = judged_again
    ) -> None:
        """Judge again a run's candidate that ``again`` selects, now that it has a new measurement.

        It is judged as its run would have judged it (``_judge``): against the bar of its parent, a keep moves the head
        only while that bar is the head, and else it is rebased onto the head (``_follow``). A crash that crashes again
        stays a crash.
        """
        with self._db.session() as s:
            candidate = s.get_one(Candidate, candidate_id)
            run = candidate.run
            if run is None or not again(candidate):
                return
            assert candidate.parent is not None  # a run's candidate descends from the run's base
            cfg = config_of(run.idea.campaign)
            scope = Scope(
                run.id,
                candidate.campaign_id,
                cfg,
                cfg.track(run.idea.track),
                repo,
                current_evaluator(s, run.idea.campaign, cfg).id,
                run.branch,
            )
            s.commit()
            bar_id, sha = bar_of(candidate.parent).id, candidate.commit_sha
        with self._tracer.span('piloop.candidate', run=scope.run_id, commit=sha, candidate=candidate_id) as span:
            notes = git.trailers(repo, sha, 'Piloop-Note')
            judged = self._judge(
                scope, candidate_id, bar_id, span, judge_now=True, skip=False, recovered=False, notes=notes
            )
        if judged.merged is not None:
            self._follow(scope, judged.merged, log_dir)

    def _score(
        self, run_id: RunId, campaign_id: CampaignId, repo: Repo, shas: list[str], log_dir: Path
    ) -> CandidateId | None:
        """Turn each new commit into a candidate, or find its existing candidate, then measure and judge it.

        Return the id of the candidate of the run's recovery commit (``RECOVERED_TRAILER``), if it has one and scoring
        was not cancelled. A tree that is already a candidate keeps that candidate, and its verdict unless another run
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

        A commit with ``Piloop-Eval: skip`` is not measured. A new candidate gets the daemon's ``unmeasured``
        verdict, which moves no bar. So a later commit without the trailer that has the same tree is measured and
        judged like a new one. ``Piloop-Note`` trailers are applied when the commit is judged
        (``_judge``). The commit the daemon made of the implementer's uncommitted work (``RECOVERED_TRAILER``) is not
        measured either: it gets the daemon's ``recovered`` verdict, which moves no bar, and the same tree committed
        later is measured and judged like a new one.
        """
        with self._db.session() as s:
            run = s.get_one(Run, run_id)
            cfg = config_of(run.idea.campaign)
            scope = Scope(
                run_id,
                campaign_id,
                cfg,
                cfg.track(run.idea.track),
                repo,
                current_evaluator(s, run.idea.campaign, cfg).id,
                run.branch,
            )
            bar_id = bar_of(run.base).id
            candidate = run.base
            ids: list[CandidateId] = []
            for sha in shas:
                candidate, _ = resolve(s, campaign_id, repo, sha, parent=candidate, run_id=run_id)
                ids.append(candidate.id)
            s.commit()
        for i, sha in enumerate(shas):
            git.pin(repo, f'refs/piloop/{cfg.name}/r{run_id}/{i}', sha)
        recovered_id: CandidateId | None = None
        for i, (sha, candidate_id) in enumerate(zip(shas, ids, strict=True)):
            skip = git.eval_skipped(repo, sha)
            recovered = bool(git.trailers(repo, sha, RECOVERED_TRAILER))
            with self._db.session() as s:
                candidate = s.get_one(Candidate, candidate_id)
                unmeasured = skip or recovered
                again = skipped(candidate) or reproposed(candidate, s.get_one(Run, run_id))
                judge_now = candidate.verdict is Verdict.PENDING or (not unmeasured and again)
                needs_measurement = not unmeasured and not measured(candidate, scope.evaluator_id)
            with self._tracer.span('piloop.candidate', run=run_id, commit=sha, candidate=candidate_id) as span:
                if unmeasured:
                    span.set(status='recovered' if recovered else 'skipped')
                elif not needs_measurement:
                    span.set(status='known')
                    self._tracer.event('candidate.known', run=run_id, commit=sha, candidate=candidate_id)
                elif not self._measure(
                    run_id, campaign_id, cfg, repo, sha, candidate_id, scope.evaluator_id, log_dir / f'eval-{i}'
                ):
                    span.set(status='cancelled')
                    return None
                judged = self._judge(
                    scope,
                    candidate_id,
                    bar_id,
                    span,
                    judge_now=judge_now,
                    skip=skip,
                    recovered=recovered,
                    notes=git.trailers(repo, sha, 'Piloop-Note'),
                )
            if recovered:  # by its trailer: the tree may be a known candidate, which keeps its verdict
                recovered_id = candidate_id
            if judged.merged is not None and not self._follow(scope, judged.merged, log_dir):
                return None
            if judged.advanced:
                bar_id = candidate_id
        with self._db.session() as s:
            mine = (Candidate.run_id == run_id, Candidate.verdict == Verdict.PENDING, Candidate.commit_sha.not_in(shas))
            left = s.scalars(select(Candidate.id).where(*mine).order_by(Candidate.id)).all()
        for candidate_id in left:  # Rebased candidates that a daemon stop left unjudged.
            if not self._follow(scope, candidate_id, log_dir):
                return None
        return recovered_id

    def _follow(self, scope: Scope, candidate_id: CandidateId, log_dir: Path) -> bool:
        """Measure a rebased candidate and judge it against its parent, the head it was merged onto.

        If the head moved again and the merge keeps against its parent, the merge is rebased in turn (``_judge``). This
        repeats until a merge is judged on the head or a merge conflicts. Returns False if the run was cancelled.
        """
        current: CandidateId | None = candidate_id
        while current is not None:
            with self._db.session() as s:
                candidate = s.get_one(Candidate, current)
                sha, parent_id = candidate.commit_sha, candidate.parent_id
                needs_measurement = not measured(candidate, scope.evaluator_id)
                judge_now = candidate.verdict is Verdict.PENDING
            assert parent_id is not None  # A rebased candidate's parent is the head it was merged onto.
            with self._tracer.span('piloop.candidate', run=scope.run_id, commit=sha, candidate=current) as span:
                span.set(rebased=True)
                if needs_measurement and not self._measure(
                    scope.run_id,
                    scope.campaign_id,
                    scope.cfg,
                    scope.repo,
                    sha,
                    current,
                    scope.evaluator_id,
                    log_dir / f'eval-rebased-{current}',
                ):
                    span.set(status='cancelled')
                    return False
                judged = self._judge(
                    scope, current, parent_id, span, judge_now=judge_now, skip=False, recovered=False, notes=[]
                )
            current = judged.merged
        return True

    def _measure(
        self,
        run_id: RunId | None,
        campaign_id: CampaignId,
        cfg: CampaignConfig,
        repo: Repo,
        sha: str,
        candidate_id: CandidateId,
        evaluator_id: EvaluatorId,
        eval_dir: Path,
    ) -> bool:
        """Measure ``sha`` on the executor, and wait until its measurement is recorded. False if the daemon stopped.

        A request for a candidate that already waits or runs under the evaluator waits for that evaluation and submits
        nothing of its own (``queue.request``). It is submitted when the queue gives it a slot (``dispatch``), and its
        result records the measurement (``CampaignResults``). An evaluation that cannot start, because the commit cannot
        be checked out say, is a crash like one that fails, so every request ends with a measurement. A cancelled
        evaluation records nothing: ``queue.cancel`` recorded its crash.
        """
        with self.launcher.lock:
            if self.launcher.cancelled.is_set():
                return False
            with self._db.session() as s:  # two requests for one tree must not both insert it
                evaluation, mine = queue.request(s, candidate_id, evaluator_id)
                job = None
                if mine:
                    links = worktree_links(cfg, Path(repo.working_dir), campaign_dir(self._home, campaign_id))
                    job = EvaluationJob(
                        evaluation=evaluation.id,
                        candidate=candidate_id,
                        run=run_id,
                        runner=s.get_one(Evaluator, evaluator_id).runner,
                        artifact=GitTree(repo=Path(repo.working_dir), commit=sha, links=links),
                        cwd=eval_dir / 'worktree',
                        log_dir=eval_dir,
                        command=cfg.evaluate,
                        timeout=cfg.evaluate_timeout,
                        required=[m.name for m in cfg.metrics],
                        units_file=self._units_dir / f'{uuid.uuid4().hex}.pending',
                    )
                s.commit()
                evaluation_id = evaluation.id
            if job is not None:
                self._jobs[evaluation_id] = job
        self.dispatch()
        with self._tracer.span('piloop.measure', run=run_id, commit=sha, candidate=candidate_id) as span:
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
        skip: bool,
        recovered: bool,
        notes: list[str],
    ) -> Judged:
        """Set the daemon's verdict on a candidate while it is ``judgeable``, if ``judge_now``. Return the result.

        A known candidate keeps its verdict, because the same tree again (a revert) says nothing new about it, unless
        another run proposes it again (``_score``). ``judgeable`` is read in the transaction that judges, so a keep
        that another run made meanwhile, of the same tree, stays. With
        ``skip``, the verdict is ``unmeasured`` (``SKIPPED``) instead of the rule's verdict, and when ``recovered``, it
        is ``recovered`` (``RECOVERED_REASON``).

        A keep against a bar that is no longer the head stays off the trunk, unless the run is a branch run. The head
        moved while the run worked or while the daemon measured. The candidate becomes ``interesting`` (``MOVED``) and
        is merged onto the head in the same transaction (``ledger.rebase``). The merge is judged next
        (``Judged.merged``). See ``_verdict``.

        Otherwise the commit's ``Piloop-Note`` ``notes`` are added after the verdict, signed by the run. They turn a
        discard or an interesting candidate into a noted ``interesting`` (``ledger.note``). A keep or a crash keeps
        its verdict.
        """
        merged: CandidateId | None = None
        moved = False
        with self._db.session() as s:
            candidate = s.get_one(Candidate, candidate_id)
            if judge_now and judgeable(candidate):
                if recovered:
                    candidate.verdict = Verdict.RECOVERED
                    candidate.verdict_reason = RECOVERED_REASON.format(run=scope.run_id)
                elif skip:
                    candidate.verdict, candidate.verdict_reason = Verdict.UNMEASURED, SKIPPED
                else:
                    verdict, reason, onto = self._verdict(s, scope, candidate, bar_id)
                    candidate.verdict, candidate.verdict_reason = verdict, reason
                    if onto is not None:
                        moved = True
                        found = rebase(s, scope.cfg, scope.repo, candidate, onto)
                        merged = found.id if found is not None else None
                candidate.verdict_by = DAEMON
                emit(
                    s,
                    'candidate.judged',
                    TargetKind.CANDIDATE,
                    candidate_id,
                    payload={
                        'verdict': candidate.verdict,
                        'score': score(candidate, scope.evaluator_id, scope.track.primary),
                        'reason': candidate.verdict_reason,
                    },
                )
                if not (skip or recovered):
                    why = '; '.join(notes)
                    if why and not moved and candidate.verdict in (Verdict.DISCARD, Verdict.INTERESTING):
                        note(candidate, why, implementer_of(s, scope.run_id))
                s.commit()
            verdict, advanced = candidate.verdict, raises_bar(candidate, branch=scope.branch) or moved
            span.set(
                verdict=verdict,
                summary=candidate.summary,
                score=score(candidate, scope.evaluator_id, scope.track.primary),
            )
        return Judged(verdict, advanced=advanced, merged=merged)

    def _verdict(
        self, s: Session, scope: Scope, candidate: Candidate, bar_id: CandidateId
    ) -> tuple[Verdict, str, Candidate | None]:
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
            branch=scope.branch,
        )
        if verdict is not Verdict.KEEP or scope.branch or trunk is None or trunk.id == bar_id:
            return verdict, reason, None
        if git.contains(scope.repo, candidate.commit_sha, trunk.commit_sha):
            verdict, reason = judge(
                candidate, trunk, scope.evaluator_id, scope.cfg, scope.track, self._units_dir, head=trunk, branch=False
            )
            return verdict, f'the head candidate#{trunk.id} is in its history: {reason}', None
        return Verdict.INTERESTING, MOVED.format(head=trunk.id) + reason, trunk

    def _close(self, span: Span, run_id: RunId, end: RunEnd, error: str | None) -> None:
        """Record the run's end when its implementer ends, so the slot is free while the commits are scored (``scoring``)."""
        with self.launcher.lock:
            if self.launcher.cancelled.is_set():  # stop() failed the row. Do not open it again.
                span.set(status='cancelled')
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
        recovered: CandidateId | None,
    ) -> None:
        """After the run's commits are judged, set the idea to ``ready`` if it has attempts left, else to ``done``.

        The run's ``scoring`` ends here, whatever the idea's status. If no agent of the run started to work
        (``startup_failed``), the idea is ``parked`` instead, attempts left or not. It was never tried, a new run would
        fail the same way, and ``done`` would let the ideas that wait for it start without its result.

        A run that did not succeed and left ``recovered`` work requeues its idea from that work, attempts left or not,
        up to ``recovery_requeues`` times of the run's role (``RECOVERY_REQUEUED`` events): the idea's base becomes the
        recovered candidate, so a new implementer starts from it and finishes it, and that run is judged as any other. A
        note on the idea says so.

        A run whose implementer's escalation still waits for an answer (``open_escalation``) parks its idea first, whatever
        its status. A new run would stop on the same question, and ``done`` would start the ideas that wait for it.
        """
        with self.launcher.lock:
            if self.launcher.cancelled.is_set():  # stop() returns the idea to the queue.
                span.set(status='cancelled')
                return
            with self._db.session() as s:
                run, idea = s.get_one(Run, run_id), s.get_one(Idea, idea_id)
                run.scoring = False
                # a role that piloop init removed while the run ran requeues nothing
                role = config_of(idea.campaign).roles.doing_runs().get(run.role)
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
                    elif (
                        recovered is not None
                        and end is not RunEnd.FINISHED
                        and role is not None
                        and _requeues(s, idea) < role.recovery_requeues
                    ):
                        idea.status, idea.base_candidate_id = IdeaStatus.READY, recovered
                        payload: dict[str, object] = {'run': run_id, 'candidate': recovered}
                        emit(s, RECOVERY_REQUEUED, TargetKind.IDEA, idea.id, payload=payload)
                        s.add(
                            Comment(
                                target=TargetRef(TargetKind.IDEA, idea.id),
                                author=DAEMON_ACTOR,
                                kind=CommentKind.NOTE,
                                body=f'requeued from candidate#{recovered}: run#{run_id} ended {end} with work its'
                                f' implementer never committed, recovered as candidate#{recovered}; the next run starts'
                                ' from it to finish it',
                            )
                        )
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
        self, s: Session, cfg: CampaignConfig, idea: Idea, base: Candidate, author: str, worktree: Path
    ) -> str:
        """Return the implementer's context: its worktree, the idea, its base, and the comments on the idea and its runs.

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
            f'- Your cwd is your worktree, {worktree}: make every edit, run every evaluation and make every commit'
            f' there. Only its commits are scored. The campaign repo, {idea.campaign.path}, is read-only for you: read'
            ' its history and candidates, never edit, run or commit in it.',
            f'- Starting from candidate#{base.id}: {at_base} — {base.summary}',
        ]
        if base.verdict is Verdict.RECOVERED:
            lines.append(
                f'- candidate#{base.id} is work that an earlier run of this idea left uncommitted when its implementer'
                ' stopped. It was never measured. Read its diff against its parent, finish it, and commit what you'
                ' would ship.'
            )
        if (bar := bar_of(base)) is not base:
            at_bar = ', '.join(f'{t.primary.name}={score(bar, evaluator_id, t.primary)}' for t in cfg.all_tracks)
            lines.append(
                f'- candidate#{base.id} is no keep: your commits are judged against candidate#{bar.id}, its nearest'
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
            f'- Shared workspace for artifacts and notes to future implementers: `./{WORKSPACE}/` (not committed). Put what'
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
    repo: Repo
    evaluator_id: EvaluatorId
    branch: bool


@dataclass(frozen=True)
class Judged:
    """The result of ``_judge``: the verdict, whether the run's bar moves to it, and the merge to judge next."""

    verdict: Verdict
    advanced: bool
    """It kept against the run's bar, on the trunk or for a rebase. The run's next commit is judged against it."""
    merged: CandidateId | None
    """The candidate of its merge onto the head, if it was rebased and the merge is a new tree."""


def _comment_line(c: Comment) -> str:
    where = f' on run#{c.target_id}' if c.target_kind is TargetKind.RUN else ''
    return f'- {c.author} ({c.kind}{where}): {c.body}'


def left_finding(s: Session, run: Run, agent_session: AgentSession) -> bool:
    """Return True if ``implementer`` signed a ``finding`` on its run's idea or on the run, as ``<role>#<id>`` or ``run#<id>``.

    A finding is the implementer's report that it is done. Only the implementer's own finding counts: a relaunched run's dead
    implementer did not finish it.
    """
    on_idea = Comment.target == TargetRef(TargetKind.IDEA, run.idea_id)
    on_run = Comment.target == TargetRef(TargetKind.RUN, run.id)
    signed = (*deprecated.signatures(Agent(role=agent_session.kind, session=agent_session.id)), f'run#{run.id}')
    found = s.scalar(
        select(Comment.id)
        .where(on_idea | on_run, Comment.kind == CommentKind.FINDING, Comment.author.in_(signed))
        .limit(1)
    )
    return found is not None


@dataclass(frozen=True)
class RunFacts:
    """The facts that decide how a run ended (``run_outcome``), gathered when its implementer ends."""

    finding: bool = False
    """The implementer left its own finding (``left_finding``): its report that it is done."""
    ended: bool = False
    """The daemon ended the implementer for a stop or a cancel (``STOP_ENDED``)."""
    cancelled: bool = False
    """Someone cancelled the run (``run_cancel``)."""
    timed_out: bool = False
    """The implementer ran into its role's timeout."""
    error: str | None = None
    """How the implementer's process failed, a timeout included."""
    asked: CommentId | None = None
    """The implementer's escalation that waits for an answer (``open_escalation``)."""
    commits: int = 0
    recovered_only: bool = False
    """There are commits, and each is the daemon's commit of uncommitted work (``UNCOMMITTED_SUMMARY``)."""


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
        case RunFacts(commits=0):
            return RunEnd.FAILED, NO_CHANGES
        # Only what the implementer left uncommitted: it did not finish.
        case RunFacts(recovered_only=True):
            return RunEnd.FAILED, INTERRUPTED
        # It committed and ended cleanly.
        case _:
            return RunEnd.FINISHED, None


def close_run(s: Session, run: Run, end: RunEnd, error: str | None) -> Run:
    """Close ``run`` as ``end``: its status, end, error and finish time, and the ``run.finished`` event. The caller commits.

    Every writer of a run's end goes through here.
    """
    run.status, run.end, run.error, run.finished_at = end.status, end, error, utcnow()
    emit(s, 'run.finished', TargetKind.RUN, run.id, payload={'status': run.status, 'end': end, 'error': error})
    return run


def open_escalation(s: Session, run: Run) -> CommentId | None:
    """Return the id of the first escalation that an implementer of ``run`` wrote on its run or idea and nobody acknowledged.

    The implementer asked for something that it cannot do itself, and the answer has not come. An implementer signs as
    ``<role>#<id>`` or ``run#<id>``. Escalations of other agents on the same idea do not block the run.
    """
    sessions = s.scalars(select(AgentSession).where(AgentSession.run_id == run.id))
    signed = [name for w in sessions for name in deprecated.signatures(Agent(role=w.kind, session=w.id))]
    on_idea = Comment.target == TargetRef(TargetKind.IDEA, run.idea_id)
    on_run = Comment.target == TargetRef(TargetKind.RUN, run.id)
    return s.scalar(
        select(Comment.id)
        .where(
            on_idea | on_run,
            Comment.kind == CommentKind.ESCALATION,
            Comment.acked_at.is_(None),
            Comment.author.in_([*signed, f'run#{run.id}']),
        )
        .order_by(Comment.id)
        .limit(1)
    )


def _requeues(s: Session, idea: Idea) -> int:
    """How many times ``_settle`` requeued the idea from recovered work (its ``RECOVERY_REQUEUED`` events)."""
    query = select(func.count()).where(
        Event.target == TargetRef(TargetKind.IDEA, idea.id), Event.kind == RECOVERY_REQUEUED
    )
    return s.scalar(query) or 0


def commits_of(run_id: RunId, agent_session_id: AgentSessionId) -> Commits:
    """How a run's implementer's commits come back: the uncommitted work on a commit of its own, the head pinned.

    The commit has the ``RECOVERED_TRAILER``, and the head is pinned at ``AGENT_REFS``.
    """
    return Commits(
        message=f'{UNCOMMITTED_SUMMARY}\n\n{RECOVERED_TRAILER}: run#{run_id}', ref=f'{AGENT_REFS}{agent_session_id}'
    )


def _commits(repo: Repo, base_sha: str, head: str | None) -> list[str]:
    """Return the run's commits up to the ``head`` its worker returned, oldest first: none without a head.

    The worker committed the uncommitted work first, on its own commit, never amended into the implementer's last
    commit, which is scored as the implementer made it. That commit has the ``RECOVERED_TRAILER``, so its candidate is
    ``recovered`` (``_score``). The commits are the first-parent history from the base: a line that the implementer
    merges in is not the run's, so a merge's parent is the commit that it merged onto, and the merged line's commits
    are not scored again.
    """
    if head is None:
        return []
    return [c.hexsha for c in repo.iter_commits(f'{base_sha}..{head}', first_parent=True)][::-1]


def implementer_of(s: Session, run_id: RunId) -> Actor:
    """The run's newest implementer, which a note from its finding is by; the daemon for a run that has none."""
    w = s.scalars(
        select(AgentSession).where(AgentSession.run_id == run_id).order_by(AgentSession.id.desc()).limit(1)
    ).first()
    return Agent(role=w.kind, session=w.id) if w is not None else System.DAEMON
