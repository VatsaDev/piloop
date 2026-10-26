"""What the ``optimization`` campaign type does in the daemon's campaigns' pass (``Planner``).

Its run queue, its runs' ends and its measurements.
"""

import math
from collections import Counter
from collections.abc import Collection
from concurrent.futures import Future
from datetime import datetime
from functools import partial
from typing import ClassVar, Literal

from pydantic import Field
from sqlalchemy import Boolean, ColumnElement, Select, func, or_, select
from sqlalchemy.orm import Session

from piloop.actor import System
from piloop.agents.launcher import rotate
from piloop.campaign.ideas import park_stranded, runnable, supersede, waiting
from piloop.campaign.launches import free_harness, full_harnesses, remove_checkouts
from piloop.campaign.stop import (
    attempts,
    park_cancelled,
)
from piloop.config import CampaignConfig
from piloop.daemon.run_controller import (
    RUNS,
    branches,
    close_run,
    finding_of,
    handed_off,
)
from piloop.db.events import emit
from piloop.db.models import (
    LIVE,
    OPEN_RUNS,
    AgentSession,
    Campaign,
    Candidate,
    Comment,
    EventModel,
    Idea,
    ReadBy,
    Run,
    RunEnd,
    RunStatus,
    campaign_runs,
    count,
)
from piloop.db.triggers import DAEMON_ACTOR
from piloop.ids import (
    AgentSessionId,
    CandidateId,
    CommentKind,
    IdeaId,
    IdeaStatus,
    RoleName,
    TargetKind,
    TargetRef,
    TrackName,
)
from piloop.plugins.campaign_type.contract import CannotLaunch, Steering, Take, TypeHost
from piloop.plugins.fact_source import facts
from piloop.plugins.fact_source.facts import Facts
from piloop.plugins.role.contract import ImplementerRole, ideas_taken
from piloop.plugins.trigger.contract import Backlog, TrackBacklog
from piloop.scoring.ledger import (
    due,
    head,
    head_unmeasured,
    rejudges,
)

NO_RUN_ERROR = 'an implementer needs a run; the scheduler requests them from ready ideas'


class RunQueued(EventModel):
    """The daemon queued a run of an idea."""

    READ_BY: ClassVar[ReadBy] = ReadBy.READERS
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

    READ_BY: ClassVar[ReadBy] = ReadBy.READERS
    kind: Literal['run.rerouted'] = 'run.rerouted'
    from_: str = Field(alias='from')
    to: str


def done_by(role: RoleName) -> ColumnElement[bool]:
    """Select the runs of ``role``.

    ``likely()`` tells SQLite that most runs match, as most runs are the implementer's. Without it, SQLite drives a join
    with ``ideas`` from ``runs`` and scans every run of every campaign.
    """
    return func.likely(Run.role == role, type_=Boolean)


class OptimizationPlanner:
    """The ``optimization`` type at work in one daemon process: the hooks of the campaigns' pass (``TypeHost``)."""

    def __init__(self, host: TypeHost) -> None:
        self._host = host
        self._tracer = host.tracer
        self._controller = host.controller
        self._scoring: dict[tuple[TargetKind, int], Future[None]] = {}
        """Scoring outside an implementer: a run's resumed scoring by run id, or a campaign's re-measure by campaign id."""

    def tidy(self, s: Session, campaign: Campaign) -> None:
        """Reject the ideas that a newer idea superseded, and park those after a prerequisite without a keep."""
        for idea_id, by in supersede(s, campaign.id).items():
            self._tracer.event('idea.superseded', campaign=campaign.name, idea=idea_id, by=by)
        for idea_id, prerequisite in park_stranded(s, campaign.id).items():
            reason = f'after idea#{prerequisite}, which ended without a keep'
            self._tracer.event('idea.parked', campaign=campaign.name, idea=idea_id, reason=reason)

    def drain(self, s: Session, campaign: Campaign) -> None:
        """Fail the campaign's queued runs at its stop: their requested implementers are closed by the pass."""
        mine = Idea.campaign_id == campaign.id
        for run in s.scalars(select(Run).join(Idea).where(mine, Run.status == RunStatus.QUEUED)):
            self._fail_run(s, run, RunEnd.CAMPAIGN_STOPPED)

    def ended(self, s: Session, agent_session: AgentSession, end: RunEnd) -> None:
        """A requested agent closed without starting, for ``end``: its queued run fails."""
        if agent_session.run_id is not None:
            self._fail_run(s, s.get_one(Run, agent_session.run_id), end)

    def take(self, s: Session, agent_session: AgentSession, rotation: list[str], full: set[str]) -> Take | CannotLaunch:
        """Whether a requested implementer launches now.

        One without a run is refused, and one whose harness is at its cap waits (``_reroute``).
        """
        if agent_session.run_id is None:
            return CannotLaunch(NO_RUN_ERROR)
        cfg = s.get_one(Campaign, agent_session.campaign_id).config
        return Take.LAUNCH if self._reroute(s, cfg, agent_session, rotation, full) else Take.WAIT

    def fail_open(self, s: Session, end: RunEnd, *, reaped: bool, sparing: Collection[AgentSessionId]) -> None:
        """End each run in flight as ``end`` and queue its idea again, but those of the agents of ``sparing``.

        Queued runs are not changed: they are requests. With ``reaped``, their worktrees are removed: a previous daemon
        left them. Otherwise a live run thread removes its own worktrees in its ``finally`` block: removing a worktree
        while an evaluator still runs in it is worse than leaving it a moment longer.
        """
        spared = AgentSession.id.in_(sparing)
        launched = select(AgentSession.run_id).where(
            AgentSession.state.in_(LIVE), AgentSession.run_id.is_not(None), ~spared
        )
        kept = select(AgentSession.run_id).where(spared, AgentSession.run_id.is_not(None))
        in_flight = (Run.status == RunStatus.RUNNING) & Run.id.not_in(kept)
        for run in s.scalars(select(Run).where(in_flight | Run.id.in_(launched))).all():
            self._fail_run(s, run, end)
            if reaped:
                remove_checkouts(self._host.home / RUNS / str(run.id), run.idea.campaign, self._tracer)

    def steered_by(self, s: Session, agent_session: AgentSession) -> Steering | None:
        """An implementer's run and idea, since the run started: it signs as ``run#<id>`` too."""
        run = s.get(Run, agent_session.run_id) if agent_session.run_id is not None else None
        if run is None:
            return None
        targets = (TargetRef(TargetKind.RUN, run.id), TargetRef(TargetKind.IDEA, run.idea_id))
        since = run.started_at or agent_session.started_at
        assert since is not None, 'a running implementer started'
        return Steering(targets, since, (f'run#{run.id}',))

    def found_at(self, s: Session, agent_session: AgentSession) -> datetime | None:
        """When the implementer left its finding on its run."""
        assert agent_session.run_id is not None
        finding = finding_of(s, s.get_one(Run, agent_session.run_id), agent_session)
        return None if finding is None else finding.created_at

    def resume_unscored(self) -> None:
        """Score each closed run whose commits were not all scored (``runs.scoring``): a previous daemon stopped first."""
        with self._controller.launcher.db.session() as s:
            unscored = s.scalars(select(Run.id).where(Run.scoring).order_by(Run.id)).all()
        for run_id in unscored:
            self._tracer.event('run.unscored', run=run_id)
            self._scoring[TargetKind.RUN, run_id] = self._host.submit(partial(self._controller.resume, run_id))

    def in_flight(self) -> dict[str, Future[None]]:
        """The scoring in flight, by what it scores, after it forgets what ended. Scoring never raises."""
        self._scoring = {key: fut for key, fut in self._scoring.items() if not fut.done()}
        return {f'{kind}#{i}': fut for (kind, i), fut in self._scoring.items()}

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

    def remeasure(self, s: Session, campaign: Campaign) -> None:
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
            self._scoring[key] = self._host.submit(partial(self._controller.rejudge, again[0].id))
            return
        if not (todo := due(s, campaign)):
            return
        self._tracer.event('candidate.remeasure', campaign=campaign.name, candidate=todo[0].id, left=len(todo))
        self._scoring[key] = self._host.submit(partial(self._controller.remeasure, todo[0].id))

    def backlog(self, s: Session, campaign: Campaign, name: RoleName, left: Facts, buffer: int) -> Backlog:
        """Return the campaign's backlog as role ``name`` fills it: ``buffer`` beyond the run slots, its own sweeps.

        It is never more than the runs that the stop rule ``left``.
        """
        cfg = campaign.config
        target = cfg.run_slots + buffer
        if (runs_left := left.get(facts.RUNS)) is not None:
            target = min(target, runs_left)
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
            free=self._free_slots(s, campaign, cfg.roles.doing_runs(), left.get(facts.RUNS)),
            sweeps=sweeps,
            per_sweep=cfg.roles.sweeping()[name].max_ideas,
            on_hold=head_unmeasured(s, campaign),
        )

    def _free_slots(
        self, s: Session, campaign: Campaign, roles: dict[RoleName, ImplementerRole], runs_left: int | None
    ) -> int:
        """Return the runs that ``roles`` can start now.

        For each role, this is its ``max_concurrent`` minus its open runs. The sum is never more than the runs that the
        stop rule leaves (``runs_left``).
        """
        open_runs = campaign_runs(campaign.id).where(Run.status.in_(OPEN_RUNS))
        free = sum(
            max(role.max_concurrent - count(s, open_runs.where(done_by(name))), 0) for name, role in roles.items()
        )
        return max(free if runs_left is None else min(free, runs_left), 0)

    def queue(self, s: Session, campaign: Campaign, left: Facts) -> None:
        """Queue a run, and request its agent, for each free slot with a runnable idea, within the runs ``left``.

        Each role that does runs fills its own slots with the ideas that it takes (``takes``), in the order of
        ``Roles.doing_runs``. Its open runs count against its own ``max_concurrent``. Its ``explore_every`` and the
        track shares apply to its own runs. The runs that the stop rule leaves and the attempts of an idea count the
        runs of every role.

        A ``ready`` idea with an ``after`` link to an idea that is not ``done`` is skipped and not changed. It stays
        ``ready`` and is taken when every prerequisite has finished. A runnable idea whose track is not in
        ``piloop.toml`` is parked with a note that says why (``_park_strays``). An idea on a track with a share of 0
        waits. While a new evaluator has not measured the head (``_remeasure`` measures it first), only a pinned idea
        has a base: an unpinned run starts from the head and is judged against it.
        When an exploration run is due (``_explore``), the slot goes first to an idea pinned to an alternative base.
        Otherwise it goes to the most urgent idea of the track furthest below its share (``_pick``). An idea pinned to
        a harness at its cap (``_full``) waits. When every harness of a role's rotation is at its cap, the role queues
        nothing more in this pass.
        """
        cfg = campaign.config
        self._park_strays(s, campaign, cfg)
        live = [t.name for t in cfg.all_tracks if t.share > 0]
        query = select(Idea).where(Idea.campaign_id == campaign.id, runnable(), Idea.track.in_(live))
        if head_unmeasured(s, campaign):
            query = query.where(Idea.base_candidate_id.is_not(None))
        runs_left = left.get(facts.RUNS)
        for name, role in cfg.roles.doing_runs().items():
            takes = query
            if role.takes is not None:
                taken = s.connection().exec_driver_sql(ideas_taken(role.takes)).scalars().all()
                takes = query.where(Idea.id.in_(taken))
            for _ in range(self._free_slots(s, campaign, {name: role}, runs_left)):
                full = full_harnesses(s, campaign, cfg)
                fits = takes.where(or_(Idea.harness.is_(None), Idea.harness.not_in(full)))
                explored = self._explore(s, campaign, name, role, fits)
                if explored is not None:
                    idea, by_priority = explored, explored
                else:
                    idea, by_priority = self._pick(s, campaign, cfg, name, role, fits)
                if idea is None or not self._enqueue(s, campaign, cfg, name, idea, full, by_priority=by_priority):
                    break
                runs_left = None if runs_left is None else runs_left - 1

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
        return next((i for i in pinned if branches(s, campaign, s.get(Candidate, i.base_candidate_id), trunk)), None)

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
        is easy to find. If the pin's bar (``ledger.bar_of``) is not the head, the run is a branch run (``runs.branch``)
        and is judged against that bar. ``run.queued`` also records ``track``, and ``priority_pick`` if the share rule
        took a different idea than priority order alone (``by_priority``). This shows how often the share rule changes
        the pick.

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
            branch=branches(s, campaign, pinned, computed),
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
