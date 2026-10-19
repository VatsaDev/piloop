"""Execute one run: make a worktree, run the worker, close the run, score its commits and settle the idea.

The run closes when its worker ends, so the worker slot is free while the daemon measures the commits. The idea stays
``scheduled`` until its candidates are judged, because a dependent idea builds on the keeps of its prerequisites.
"""

import re
import subprocess
import threading
import uuid
from collections.abc import Callable
from contextlib import ExitStack
from dataclasses import dataclass
from functools import partial
from pathlib import Path

from git import Repo
from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from piloop import git
from piloop.agents.launcher import TRANSCRIPT, Launcher
from piloop.agents.roles import compose_brief, role
from piloop.campaign.project import WORKSPACE, project_dir, worktree_links
from piloop.campaign.stop import BLOCKED, CAMPAIGN_STOPPED_ERROR, NOT_STARTED, attempts
from piloop.config import ProjectConfig, Track, config_of
from piloop.db.events import emit
from piloop.db.models import (
    Candidate,
    Comment,
    CommentKind,
    Event,
    Idea,
    IdeaStatus,
    Measurement,
    Project,
    Run,
    RunStatus,
    TargetKind,
    Verdict,
    Worker,
    WorkerState,
    utcnow,
)
from piloop.db.session import Database
from piloop.db.triggers import DAEMON_ACTOR
from piloop.otel import Span, Tracer
from piloop.proc import kill_group
from piloop.reports.history import history
from piloop.reports.wake import lessons_overview
from piloop.runtime import WorkerStatus
from piloop.runtime.factory import RuntimeFactory, make_runtime
from piloop.scoring.evaluate import EvalResult, run_evaluate
from piloop.scoring.ledger import (
    DAEMON,
    MOVED,
    SKIPPED,
    bar_of,
    current_evaluator,
    head,
    judge,
    judged_again,
    measured,
    note,
    raises_bar,
    rebase,
    resolve,
    save_units,
    score,
    skipped,
)

UNCOMMITTED_SUMMARY = 'piloop: uncommitted worker changes'
RECOVERED_TRAILER = 'Piloop-Recovered'
"""The trailer of the commit the daemon makes of what a worker left uncommitted: ``Piloop-Recovered: run#<id>``."""
RECOVERED_REASON = 'recovered: what the worker of run#{run} left uncommitted, never declared ready; not measured'
NO_CHANGES = 'worker produced no commits'
INTERRUPTED = (
    'worker stopped without a commit or a finding of its own; its uncommitted changes are a recovered candidate'
)
RECOVERY_REQUEUED = 'idea.recovery_requeued'
"""The event on an idea that ``_settle`` requeued from recovered work, with ``{run, candidate}``. Counting these events
caps the requeues (the ``recovery_requeues`` of the run's role): candidates cannot, because two runs that leave the
same tree share one candidate."""
STOP_ENDED = 'worker.stop_ended'
"""Event on a worker of a stopped project that the daemon ended when the grace period after its wrap-up prompt was
over, with the stop's ``reason``. A worker in the middle of a turn takes a message only when the turn ends, so it may
never have read the prompt. The scheduler commits the event before the kill, and the run closes as cut by the stop
(``CAMPAIGN_STOPPED_ERROR``) unless its worker left its finding."""
STARTUP_RELAUNCHES = 2
"""How many times a run is queued again with a new worker after its worker failed to start. Then the run fails.

A failure at startup is a fault of the host or the harness, not an attempt at the idea. So it does not count as an
attempt or toward ``max_runs``."""


class RunExecutor:
    """Run one ``Run`` to completion on the calling thread. ``Launcher.launch`` runs the agent."""

    def __init__(
        self, db: Database, tracer: Tracer, home: Path, runtime_factory: RuntimeFactory = make_runtime
    ) -> None:
        self._db = db
        self._tracer = tracer
        self._runs_dir = home / 'runs'
        self._home = home
        self._units_dir = home / 'units'
        self.launcher = Launcher(db, tracer, home, runtime_factory)
        self._evaluating: dict[int, subprocess.Popen[bytes]] = {}
        """The evaluator child of each thread that measures now, by thread id. Runs and re-measures run in parallel."""

    def cancel(self) -> None:
        """Cancel the launcher and kill the evaluators. After this, every child is dead or dies when it is spawned.

        Both happen under ``launcher.lock``. ``_track_evaluation`` and the writes to run rows hold the same lock.
        """
        with self.launcher.lock:
            self.launcher.cancelled.set()
            for proc in self._evaluating.values():
                kill_group(proc)

    def hand_over(self) -> None:
        """Cancel for an in-place restart. The agents keep running for the next daemon (``Launcher.handover``)."""
        self.launcher.handover.set()
        self.cancel()

    def execute(self, run_id: int, worker_id: int) -> None:
        """Make the worktree, run the worker, evaluate and record. Never raises: failures are recorded on the run."""
        self._guarded(run_id, worker_id, lambda span: self._execute(run_id, worker_id, span))

    def reattach(self, run_id: int, worker_id: int) -> None:
        """Finish a run whose agent a previous daemon process started (``Launcher.reattach``). Never raises.

        The daemon watches the agent in the run's worktree until it ends. Then it scores and settles the run as
        ``execute`` does.
        """
        self._guarded(run_id, worker_id, lambda span: self._finish(run_id, worker_id, None, span))

    def _guarded(self, run_id: int, worker_id: int, body: Callable[[Span], None]) -> None:
        """Run ``body``. A failure fails the run, and closes its worker if the failure came before the launch."""
        with self._tracer.span('piloop.run', run=run_id) as span:
            try:
                body(span)
            except Exception as e:
                span.error(e)
                error = f'{type(e).__name__}: {e}'
                with self._db.session() as s:
                    idea_id = s.get_one(Run, run_id).idea_id
                    unstarted = (Worker.id == worker_id) & (Worker.state == WorkerState.STARTING)
                    s.execute(
                        update(Worker).where(unstarted).values(state=WorkerState.KILLED, ended_at=utcnow(), error=error)
                    )
                    s.commit()
                self._close(span, run_id, RunStatus.FAILED, error)
                self._settle(span, run_id, idea_id, RunStatus.FAILED, startup_failed=False, recovered=None)

    def _execute(self, run_id: int, worker_id: int, span: Span) -> None:
        with self.launcher.lock:
            if self.launcher.cancelled.is_set():  # Queued after stop(). The scheduler already failed the row.
                span.set(status='cancelled')
                return
            with self._db.session() as s:
                run = s.get_one(Run, run_id)
                idea = run.idea
                project = idea.project
                cfg = config_of(project)
                base = s.get_one(Candidate, run.base_candidate_id)
                brief = run.brief or compose_brief(
                    role(cfg, run.role, Path(project.path)),
                    project,
                    worker_id,
                    reason=f'idea#{idea.id} is ready',
                    context=self._context(s, cfg, idea, base, f'{run.role}#{worker_id}'),
                    track=cfg.track(idea.track),
                    wake=lessons_overview(s, project, cfg.wake_budget(run.role)),
                )
                run.brief = brief
                run.status = RunStatus.RUNNING
                # A relaunch keeps the start time, so comments since then are sent to the new worker again.
                run.started_at = run.started_at or utcnow()
                run.log_dir = str(self._runs_dir / str(run_id))
                emit(s, 'run.started', TargetKind.RUN, run_id, payload={'harness': run.harness, 'model': run.model})
                s.commit()
        self._finish(run_id, worker_id, brief, span)

    def _finish(self, run_id: int, worker_id: int, brief: str | None, span: Span) -> None:
        """Run the agent, then record the outcome, score the commits and settle the idea.

        With a ``brief``, a new agent starts in a new worktree. With None, the daemon re-attaches to the agent that is
        already in the worktree. A handed-over agent leaves the worktree and the rows to the next daemon.
        """
        with self._db.session() as s:
            run = s.get_one(Run, run_id)
            project = run.idea.project
            cfg = config_of(project)
            base_sha = s.get_one(Candidate, run.base_candidate_id).commit_sha
            harness, model, project_path, idea_id, project_id = (
                run.harness,
                run.model,
                Path(project.path),
                run.idea_id,
                project.id,
            )
        log_dir = self._runs_dir / str(run_id)
        repo = git.open_repo(project_path)
        wt_path = log_dir / 'worktree'
        links = worktree_links(cfg, project_path, project_dir(self._home, project_id))
        span.set(idea=idea_id, worker=worker_id, harness=harness, model=model)
        with ExitStack() as cleanup:
            cleanup.callback(git.remove_worktree, repo, wt_path)
            if brief is None:
                outcome = self.launcher.reattach(worker_id, cwd=wt_path, log_dir=log_dir, links=links)
            else:
                make_cwd = partial(git.add_worktree, repo, wt_path, base_sha, links)
                outcome = self.launcher.launch(
                    worker_id, brief=brief, cwd=wt_path, log_dir=log_dir, make_cwd=make_cwd, links=links
                )
            if outcome.handed_over:  # The next daemon re-attaches to the agent in the worktree.
                cleanup.pop_all()
                span.set(status='handed_over')
                return
            if outcome.startup_failed:
                if not self._relaunch(span, run_id, worker_id, log_dir):
                    self._close(span, run_id, RunStatus.FAILED, NOT_STARTED)
                    self._settle(span, run_id, idea_id, RunStatus.FAILED, startup_failed=True, recovered=None)
                return
            wt = Repo(wt_path)
            if self.launcher.cancelled.is_set():
                cut = not self._left_finding(run_id, idea_id, worker_id)  # else the stop closed it as handed off
                self._keep_cut_work(run_id, cfg.name, repo, wt, base_sha, git.linked(wt_path, links), noted=cut)
                span.set(status='cancelled')
                return
            shas = self._commits(wt, base_sha, git.linked(wt_path, links), run_id)
            finding = self._left_finding(run_id, idea_id, worker_id)
            with self._db.session() as s:
                asked = open_escalation(s, s.get_one(Run, run_id))
                stop_ended = s.scalar(
                    select(Event.id).where(
                        Event.target_kind == TargetKind.WORKER, Event.target_id == worker_id, Event.kind == STOP_ENDED
                    )
                )
            if stop_ended is not None and not finding:  # the stop cut it, whatever the kill left in the outcome
                status, error = RunStatus.FAILED, CAMPAIGN_STOPPED_ERROR
            elif outcome.status is WorkerStatus.TIMED_OUT:
                status, error = RunStatus.TIMED_OUT, outcome.error
            elif outcome.error is not None:
                status, error = RunStatus.FAILED, outcome.error
            elif asked is not None:  # Its question is in the inbox. A finding or a commit does not make this a success.
                status, error = RunStatus.FAILED, f'{BLOCKED} comment#{asked}'
            elif not shas:  # A reported negative result is a result. No commit and no finding is a failure.
                status, error = (RunStatus.SUCCEEDED, None) if finding else (RunStatus.FAILED, NO_CHANGES)
            elif not finding and all(wt.commit(sha).summary == UNCOMMITTED_SUMMARY for sha in shas):
                status, error = RunStatus.FAILED, INTERRUPTED
            else:
                status, error = RunStatus.SUCCEEDED, None
            self._close(span, run_id, status, error)
            # Score all commits, even if the worker then timed out or crashed.
            recovered = self._score(run_id, project_id, repo, shas, log_dir) if shas else None
        self._settle(span, run_id, idea_id, status, startup_failed=False, recovered=recovered)

    def _relaunch(self, span: Span, run_id: int, worker_id: int, log_dir: Path) -> bool:
        """Queue the run again with a new worker, up to ``STARTUP_RELAUNCHES`` times, after its worker failed to start.

        The run stays open, so it keeps its slot, attempt number and brief. The ``worker#<id>`` in the brief becomes the
        new worker's id. The new worker signs its notes and finding with its own id, and only its own finding counts
        (``_left_finding``). The scheduler launches the new worker row like any other. The old worker's transcript is
        renamed so that the new one does not overwrite it. Also returns True if the daemon stopped meanwhile, because
        the stop then owns the rows.
        """
        with self.launcher.lock:
            if self.launcher.cancelled.is_set():
                span.set(status='cancelled')
                return True
            with self._db.session() as s:
                run = s.get_one(Run, run_id)
                launches = s.scalar(select(func.count()).select_from(Worker).where(Worker.run_id == run_id)) or 0
                if launches > STARTUP_RELAUNCHES:
                    return False
                run.status = RunStatus.QUEUED
                worker = Worker(
                    kind=run.role,
                    project_id=run.idea.project_id,
                    run_id=run_id,
                    harness=run.harness,
                    model=run.model,
                    reason=f'run#{run_id} relaunched: {run.role}#{worker_id} failed to start',
                )
                s.add(worker)
                s.flush()
                # Edit the brief, do not compose it again: a human may have changed it (``UPDATE runs SET brief``).
                signed = rf'\b({re.escape(run.role)})#{worker_id}\b'
                run.brief = re.sub(signed, rf'\g<1>#{worker.id}', run.brief or '')
                emit(s, 'run.relaunched', TargetKind.RUN, run_id, payload={'worker': worker_id, 'launches': launches})
                s.commit()
            transcript = log_dir / TRANSCRIPT
            if transcript.exists():
                transcript.rename(log_dir / f'transcript-worker{worker_id}.txt')
        span.set(status='relaunched')
        return True

    def _keep_cut_work(
        self, run_id: int, name: str, repo: Repo, wt: Repo, base_sha: str, ignore: list[str], *, noted: bool
    ) -> None:
        """Pin the work of a run that a daemon stop interrupted, uncommitted work included, and add a note on the run.

        The stop owns the rows. A run that the stop cut is not an attempt (``stop.NOT_ATTEMPTS``): its work is not
        scored, and the note (``noted``) goes into the brief of the idea's next run, which can start from those
        commits. A run whose worker had left its finding was handed off: the stop closed it as finished, and the next
        daemon scores the pinned commits (``resume``), so no note sends a new run to them.
        """
        shas = self._commits(wt, base_sha, ignore, run_id)
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
                    target_kind=TargetKind.RUN,
                    target_id=run_id,
                    author=DAEMON_ACTOR,
                    kind=CommentKind.NOTE,
                    body=f'interrupted by a daemon stop: its {len(shas)} commits are pinned, unscored, at {prefix}<n>'
                    f' (head {shas[-1]}). To start from them, run git cherry-pick {base_sha}..{shas[-1]}',
                )
            )
            s.commit()

    def _left_finding(self, run_id: int, idea_id: int, worker_id: int) -> bool:
        """Return True if the agent signed a ``finding`` on its idea or its run (``left_finding``)."""
        with self._db.session() as s:
            return left_finding(s, s.get_one(Run, run_id), s.get_one(Worker, worker_id))

    def _commits(self, wt: Repo, base_sha: str, ignore: list[str], run_id: int) -> list[str]:
        """Return the new commits in the worktree, oldest first. Uncommitted work is committed first, on its own.

        It is never amended into the worker's last commit, which is scored as the worker made it. Its commit has the
        ``RECOVERED_TRAILER``, so its candidate is ``recovered`` (``_score``).
        """
        if git.is_dirty(wt, ignore):
            git.commit_all(wt, f'{UNCOMMITTED_SUMMARY}\n\n{RECOVERED_TRAILER}: run#{run_id}', ignore)
        return [c.hexsha for c in wt.iter_commits(f'{base_sha}..HEAD')][::-1]

    def resume(self, run_id: int) -> None:
        """Score the commits of a closed run and settle its idea, after a daemon stopped while it scored them.

        The commits are the ones that ``_score`` pinned. Never raises. If scoring cannot resume, for example because
        the repo is gone, the run settles as failed. The idea then goes back to the queue if it has attempts left.
        """
        with self._tracer.span('piloop.resume', run=run_id) as span:
            with self._db.session() as s:
                run = s.get_one(Run, run_id)
                project = run.idea.project
                status, idea_id, project_id = run.status, run.idea_id, project.id
                name, project_path = config_of(project).name, Path(project.path)
            recovered = None
            try:
                repo = git.open_repo(project_path)
                shas = git.pinned(repo, f'refs/piloop/{name}/r{run_id}/')
                span.set(commits=len(shas))
                recovered = self._score(run_id, project_id, repo, shas, self._runs_dir / str(run_id))
            except Exception as e:
                span.error(e)
                status = RunStatus.FAILED
            self._settle(span, run_id, idea_id, status, startup_failed=False, recovered=recovered)

    def remeasure(self, candidate_id: int) -> None:
        """Measure a candidate with the project's current evaluator. Its verdict does not change, unless it is a crash.

        Never raises. If the evaluation cannot start, for example because the commit is gone, a crashed measurement is
        recorded. It is a result like any other, so the candidate is not tried again with this evaluator unless someone
        asks (``ledger.request_remeasure``). A ``crash`` that the daemon judged, from a fault of the host for example,
        is judged again with the new measurement (``_rejudge``).
        """
        with self._db.session() as s:
            candidate = s.get_one(Candidate, candidate_id)
            project = s.get_one(Project, candidate.project_id)
            cfg = config_of(project)
            evaluator_id = current_evaluator(s, project, cfg).id
            s.commit()
            project_id, project_path, sha = project.id, Path(project.path), candidate.commit_sha
        eval_dir = project_dir(self._home, project_id) / 'measure' / f'{candidate_id}-e{evaluator_id}'
        try:
            repo = git.open_repo(project_path)
            if not self._measure(None, project_id, cfg, repo, sha, candidate_id, evaluator_id, eval_dir):
                return  # a stop killed the evaluator: there is nothing new to judge
        except Exception as e:
            self._record(None, candidate_id, evaluator_id, EvalResult({}, 0.0, f'{type(e).__name__}: {e}'))
            return
        self._rejudge(candidate_id, repo, eval_dir)

    def _rejudge(self, candidate_id: int, repo: Repo, log_dir: Path) -> None:
        """Judge again a ``crash`` of a run that the daemon judged, now that it has a new measurement.

        It is judged as its run would have judged it (``_judge``): against the bar of its parent, a keep moves the head
        only while that bar is the head, and else it is rebased onto the head (``_follow``). A crash that crashes again
        stays a crash.
        """
        with self._db.session() as s:
            candidate = s.get_one(Candidate, candidate_id)
            run = candidate.run
            if run is None or not judged_again(candidate):
                return
            assert candidate.parent is not None  # a run's candidate descends from the run's base
            cfg = config_of(run.idea.project)
            scope = Scope(
                run.id,
                candidate.project_id,
                cfg,
                cfg.track(run.idea.track),
                repo,
                current_evaluator(s, run.idea.project, cfg).id,
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

    def _score(self, run_id: int, project_id: int, repo: Repo, shas: list[str], log_dir: Path) -> int | None:
        """Turn each new commit into a candidate, or find its existing candidate, then measure and judge it.

        Return the id of the candidate of the run's recovery commit (``RECOVERED_TRAILER``), if it has one and scoring
        was not cancelled. A tree that is already a candidate keeps that candidate and its verdict.

        The daemon scores with the project's current config, not the config at launch. A run that outlives a config
        change must not move the project back to the old evaluator. All commits are candidates and pinned first, so a
        commit that waits for the evaluations before it is a ``pending`` candidate, and after a stop, ``resume`` finds
        all of them. It judges the candidates that the stop left pending, including rebased ones
        (``_follow``).

        Each candidate is judged on the run's track (``ledger.judge``) against a bar. The bar starts at the run's base,
        or at the nearest keep if the base is not a keep (``ledger.bar_of``). Each keep becomes the new bar. A run that
        is not a branch run keeps on the trunk only while its bar is the head. If the head has moved past the bar, a
        keep is rebased onto the head and the merge is judged there (``_judge``, ``_follow``). A commit whose tree is
        already a candidate is that candidate. It is measured only if the current evaluator has not measured it, and it
        keeps its verdict.

        A commit with ``Piloop-Eval: skip`` is not measured. A new candidate gets the daemon's ``unmeasured``
        verdict, which moves no bar. So a later commit without the trailer that has the same tree is measured and
        judged like a new one. ``Piloop-Note`` trailers are applied when the commit is judged
        (``_judge``). The commit the daemon made of the worker's uncommitted work (``RECOVERED_TRAILER``) is not
        measured either: it gets the daemon's ``recovered`` verdict, which moves no bar, and the same tree committed
        later is measured and judged like a new one.
        """
        with self._db.session() as s:
            run = s.get_one(Run, run_id)
            cfg = config_of(run.idea.project)
            scope = Scope(
                run_id,
                project_id,
                cfg,
                cfg.track(run.idea.track),
                repo,
                current_evaluator(s, run.idea.project, cfg).id,
                run.branch,
            )
            bar_id = bar_of(run.base).id
            candidate = run.base
            ids: list[int] = []
            for sha in shas:
                candidate, _ = resolve(s, project_id, repo, sha, parent=candidate, run_id=run_id)
                ids.append(candidate.id)
            s.commit()
        for i, sha in enumerate(shas):
            git.pin(repo, f'refs/piloop/{cfg.name}/r{run_id}/{i}', sha)
        recovered_id: int | None = None
        for i, (sha, candidate_id) in enumerate(zip(shas, ids, strict=True)):
            skip = git.eval_skipped(repo, sha)
            recovered = bool(git.trailers(repo, sha, RECOVERED_TRAILER))
            with self._db.session() as s:
                candidate = s.get_one(Candidate, candidate_id)
                unmeasured = skip or recovered
                judge_now = candidate.verdict is Verdict.PENDING or (not unmeasured and skipped(candidate))
                needs_measurement = not unmeasured and not measured(candidate, scope.evaluator_id)
            with self._tracer.span('piloop.candidate', run=run_id, commit=sha, candidate=candidate_id) as span:
                if unmeasured:
                    span.set(status='recovered' if recovered else 'skipped')
                elif not needs_measurement:
                    span.set(status='known')
                    self._tracer.event('candidate.known', run=run_id, commit=sha, candidate=candidate_id)
                elif not self._measure(
                    run_id, project_id, cfg, repo, sha, candidate_id, scope.evaluator_id, log_dir / f'eval-{i}'
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

    def _follow(self, scope: Scope, candidate_id: int, log_dir: Path) -> bool:
        """Measure a rebased candidate and judge it against its parent, the head it was merged onto.

        If the head moved again and the merge keeps against its parent, the merge is rebased in turn (``_judge``). This
        repeats until a merge is judged on the head or a merge conflicts. Returns False if the run was cancelled.
        """
        current: int | None = candidate_id
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
                    scope.project_id,
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
        run_id: int | None,
        project_id: int,
        cfg: ProjectConfig,
        repo: Repo,
        sha: str,
        candidate_id: int,
        evaluator_id: int,
        eval_dir: Path,
    ) -> bool:
        """Run the evaluate command on ``sha`` in a clean worktree and record the measurement. False if cancelled."""
        links = worktree_links(cfg, Path(repo.working_dir), project_dir(self._home, project_id))
        eval_dir.mkdir(parents=True, exist_ok=True)
        eval_wt = eval_dir / 'worktree'
        self._units_dir.mkdir(parents=True, exist_ok=True)
        with self._tracer.span('piloop.measure', run=run_id, commit=sha, candidate=candidate_id) as span:
            try:
                git.add_worktree(repo, eval_wt, sha, links)
                result = run_evaluate(
                    cfg.evaluate,
                    cwd=eval_wt,
                    log_dir=eval_dir,
                    timeout=cfg.evaluate_timeout,
                    required={m.name for m in cfg.metrics},
                    track=self._track_evaluation,
                    units_file=self._units_dir / f'{uuid.uuid4().hex}.pending',
                )
            finally:
                git.remove_worktree(repo, eval_wt)
            if self.launcher.cancelled.is_set():  # The evaluator was killed, so there is nothing to record.
                span.set(status='cancelled')
                return False
            span.set(
                measurement=self._record(run_id, candidate_id, evaluator_id, result),
                error=result.error,
                **{f'metric.{k}': v for k, v in result.metrics.items()},
            )
        return True

    def _record(self, run_id: int | None, candidate_id: int, evaluator_id: int, result: EvalResult) -> int:
        """Store one evaluation as a measurement, with its per-unit results, and return its id."""
        with self._db.session() as s:
            measurement = Measurement(
                candidate_id=candidate_id,
                evaluator_id=evaluator_id,
                metrics=result.metrics,
                error=result.error,
                seconds=result.seconds,
            )
            s.add(measurement)
            s.flush()
            save_units(self._units_dir, measurement.id, result.units)
            payload: dict[str, object] = {
                'candidate': candidate_id,
                'metrics': result.metrics,
                'error': result.error,
                'run': run_id,
            }
            emit(s, 'measurement.recorded', TargetKind.MEASUREMENT, measurement.id, payload=payload)
            s.commit()
            return measurement.id

    def _judge(
        self,
        scope: Scope,
        candidate_id: int,
        bar_id: int,
        span: Span,
        *,
        judge_now: bool,
        skip: bool,
        recovered: bool,
        notes: list[str],
    ) -> Judged:
        """Set the daemon's verdict on a new candidate, unless someone already judged it. Return the result.

        A known candidate keeps its verdict, because the same tree again (a revert) says nothing new about it. With
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
        merged: int | None = None
        moved = False
        with self._db.session() as s:
            candidate = s.get_one(Candidate, candidate_id)
            if judge_now and candidate.verdict_by in (None, DAEMON):
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
                        note(candidate, why, f'run#{candidate.run_id}')
                s.commit()
            verdict, advanced = candidate.verdict, raises_bar(candidate, branch=scope.branch) or moved
            span.set(
                verdict=verdict,
                summary=candidate.summary,
                score=score(candidate, scope.evaluator_id, scope.track.primary),
            )
        return Judged(verdict, advanced=advanced, merged=merged)

    def _verdict(
        self, s: Session, scope: Scope, candidate: Candidate, bar_id: int
    ) -> tuple[Verdict, str, Candidate | None]:
        """Return the rule's verdict and reason against the run's bar, and the head to rebase onto if needed.

        A rebase is needed for a keep against a bar that is no longer the head, unless the run is a branch run. A
        candidate whose history already contains the head needs no merge. It is judged against the head.
        """
        trunk = head(s, s.get_one(Project, candidate.project_id), excluding=candidate)
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

    def _track_evaluation(self, proc: subprocess.Popen[bytes] | None) -> None:
        """Remember this thread's evaluator child so that ``cancel`` can kill it. A later child is killed at once."""
        with self.launcher.lock:
            if proc is None:
                self._evaluating.pop(threading.get_ident(), None)
                return
            self._evaluating[threading.get_ident()] = proc
            if self.launcher.cancelled.is_set():
                kill_group(proc)

    def _close(self, span: Span, run_id: int, status: RunStatus, error: str | None) -> None:
        """Record the run's outcome when its worker ends, so the slot is free while the commits are scored (``scoring``)."""
        with self.launcher.lock:
            if self.launcher.cancelled.is_set():  # stop() failed the row. Do not open it again.
                span.set(status='cancelled')
                return
            with self._db.session() as s:
                run = s.get_one(Run, run_id)
                run.status, run.error, run.finished_at, run.scoring = status, error, utcnow(), True
                emit(s, 'run.finished', TargetKind.RUN, run_id, payload={'status': status, 'error': error})
                s.commit()
        span.set(status=status, error=error)

    def _settle(
        self, span: Span, run_id: int, idea_id: int, status: RunStatus, *, startup_failed: bool, recovered: int | None
    ) -> None:
        """After the run's commits are judged, set the idea to ``ready`` if it has attempts left, else to ``done``.

        The run's ``scoring`` ends here, whatever the idea's status. If no agent of the run started to work
        (``startup_failed``), the idea is ``parked`` instead, attempts left or not. It was never tried, a new run would
        fail the same way, and ``done`` would let the ideas that wait for it start without its result.

        A run that did not succeed and left ``recovered`` work requeues its idea from that work, attempts left or not,
        up to ``recovery_requeues`` times of the run's role (``RECOVERY_REQUEUED`` events): the idea's base becomes the
        recovered candidate, so a new worker starts from it and finishes it, and that run is judged as any other. A
        note on the idea says so.

        A run whose worker's escalation still waits for an answer (``open_escalation``) parks its idea first, whatever
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
                role = config_of(idea.project).roles.doing_runs().get(run.role)
                if idea.status is IdeaStatus.SCHEDULED:  # A human may have parked or rejected it meanwhile.
                    if (asked := open_escalation(s, run)) is not None:
                        idea.status = IdeaStatus.PARKED
                        s.add(
                            Comment(
                                target_kind=TargetKind.IDEA,
                                target_id=idea.id,
                                author=DAEMON_ACTOR,
                                kind=CommentKind.NOTE,
                                body=f'parked: run#{run_id} ended while its escalation comment#{asked} waits for an'
                                ' answer. Answer it with a decision on this idea, which the next run reads in its'
                                ' brief, then set the idea ready',
                            )
                        )
                    elif (
                        recovered is not None
                        and status is not RunStatus.SUCCEEDED
                        and role is not None
                        and _requeues(s, idea) < role.recovery_requeues
                    ):
                        idea.status, idea.base_candidate_id = IdeaStatus.READY, recovered
                        payload: dict[str, object] = {'run': run_id, 'candidate': recovered}
                        emit(s, RECOVERY_REQUEUED, TargetKind.IDEA, idea.id, payload=payload)
                        s.add(
                            Comment(
                                target_kind=TargetKind.IDEA,
                                target_id=idea.id,
                                author=DAEMON_ACTOR,
                                kind=CommentKind.NOTE,
                                body=f'requeued from candidate#{recovered}: run#{run_id} ended {status} with work its'
                                f' worker never committed, recovered as candidate#{recovered}; the next run starts'
                                ' from it to finish it',
                            )
                        )
                    elif startup_failed:
                        idea.status = IdeaStatus.PARKED
                        s.add(
                            Comment(
                                target_kind=TargetKind.IDEA,
                                target_id=idea.id,
                                author=DAEMON_ACTOR,
                                kind=CommentKind.NOTE,
                                body=f'parked: no agent of run#{run_id} started to work; fix what stopped them (the'
                                ' errors of its workers, their anomaly.startup events), then set it ready',
                            )
                        )
                    elif status is not RunStatus.SUCCEEDED and attempts(idea) < idea.max_runs:
                        idea.status = IdeaStatus.READY
                    else:
                        idea.status = IdeaStatus.DONE
                s.commit()

    def _context(self, s: Session, cfg: ProjectConfig, idea: Idea, base: Candidate, author: str) -> str:
        """Return the worker's context: the idea, its base, and the comments on the idea and on all its runs.

        A comment on a run is about the idea too. Examples are a human's decision during attempt 1, or a watchdog's
        finding on why it failed. The next attempt is a new run, so without these comments it would start with no
        context. All runs of the idea are included: the earlier attempts and this run, which can get comments while
        it is queued.
        """
        track = cfg.track(idea.track)
        evaluator_id = idea.project.evaluator_id or 0
        on_idea = (Comment.target_kind == TargetKind.IDEA) & (Comment.target_id == idea.id)
        on_its_runs = (Comment.target_kind == TargetKind.RUN) & Comment.target_id.in_(
            select(Run.id).where(Run.idea_id == idea.id)
        )
        comments = s.scalars(select(Comment).where(on_idea | on_its_runs).order_by(Comment.id)).all()
        at_base = ', '.join(f'{t.primary.name}={score(base, evaluator_id, t.primary)}' for t in cfg.all_tracks)
        lines = [f'- Starting from candidate#{base.id}: {at_base} — {base.summary}']
        if base.verdict is Verdict.RECOVERED:
            lines.append(
                f'- candidate#{base.id} is work that an earlier run of this idea left uncommitted when its worker'
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
            f'- Shared workspace for artifacts and notes to future workers: `./{WORKSPACE}/` (not committed). Put what'
            f' your idea produces there under `./{WORKSPACE}/idea{idea.id}/`: it is archived when the idea is done, and'
            ' its local copy goes some days later. The files of a done idea come back with'
            f' `piloop restore {idea.project.name} <idea id>|<workspace path>`.',
            f'- Leave a finding on your idea when you are done: INSERT INTO comments (target_kind, target_id, author,'
            f" kind, body) VALUES ('idea', {idea.id}, '{author}', 'finding', '...').",
            '- Ask for what you cannot do or decide yourself with an escalation, never a note: INSERT INTO comments'
            f" (target_kind, target_id, author, kind, priority, body) VALUES ('idea', {idea.id}, '{author}',"
            " 'escalation', 1, '...').",
            '- The history below is what was tried in this project: do not repeat it; combine or go elsewhere.',
        ]
        lines += ['', f'## Your idea (idea#{idea.id}): {idea.title}', idea.hypothesis.rstrip()]
        if idea.provenance:
            lines += ['', f'Proposed by {idea.created_by}, prompted by: {idea.provenance.strip()}']
        if idea.plan:
            lines += ['', '### Plan', idea.plan.rstrip()]
        if comments:
            lines += ['', '## Comments on this idea and its runs', *(_comment_line(c) for c in comments)]
        return '\n'.join([*lines, '', history(s, idea.project, cfg)])


@dataclass(frozen=True)
class Scope:
    """The scope for scoring the candidates of one run: the run, the project's config at scoring time and the track."""

    run_id: int
    project_id: int
    cfg: ProjectConfig
    track: Track
    repo: Repo
    evaluator_id: int
    branch: bool


@dataclass(frozen=True)
class Judged:
    """The result of ``_judge``: the verdict, whether the run's bar moves to it, and the merge to judge next."""

    verdict: Verdict
    advanced: bool
    """It kept against the run's bar, on the trunk or for a rebase. The run's next commit is judged against it."""
    merged: int | None
    """The candidate of its merge onto the head, if it was rebased and the merge is a new tree."""


def _comment_line(c: Comment) -> str:
    where = f' on run#{c.target_id}' if c.target_kind is TargetKind.RUN else ''
    return f'- {c.author} ({c.kind}{where}): {c.body}'


def left_finding(s: Session, run: Run, worker: Worker) -> bool:
    """Return True if ``worker`` signed a ``finding`` on its run's idea or on the run, as ``<role>#<id>`` or ``run#<id>``.

    A finding is the worker's report that it is done. Only the worker's own finding counts: a relaunched run's dead
    worker did not finish it.
    """
    on_idea = (Comment.target_kind == TargetKind.IDEA) & (Comment.target_id == run.idea_id)
    on_run = (Comment.target_kind == TargetKind.RUN) & (Comment.target_id == run.id)
    signed = (f'{worker.kind}#{worker.id}', f'run#{run.id}')
    found = s.scalar(
        select(Comment.id)
        .where(on_idea | on_run, Comment.kind == CommentKind.FINDING, Comment.author.in_(signed))
        .limit(1)
    )
    return found is not None


def open_escalation(s: Session, run: Run) -> int | None:
    """Return the id of the first escalation that a worker of ``run`` wrote on its run or idea and nobody acknowledged.

    The worker asked for something that it cannot do itself, and the answer has not come. A worker signs as
    ``<role>#<id>`` or ``run#<id>``. Escalations of other agents on the same idea do not block the run.
    """
    signed = [f'{w.kind}#{w.id}' for w in s.scalars(select(Worker).where(Worker.run_id == run.id))]
    on_idea = (Comment.target_kind == TargetKind.IDEA) & (Comment.target_id == run.idea_id)
    on_run = (Comment.target_kind == TargetKind.RUN) & (Comment.target_id == run.id)
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
        Event.target_kind == TargetKind.IDEA, Event.target_id == idea.id, Event.kind == RECOVERY_REQUEUED
    )
    return s.scalar(query) or 0
