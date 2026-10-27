"""The control operations: the changes that the orchestrator or a human asks of a campaign, besides the daemon's own.

Each one is a typed function. It validates its input, makes the change and writes its event in the caller's session,
and the caller commits once. ``Refused`` says why an operation changed nothing. The CLI is a thin wrapper over these
functions, and any other client (an API, a remote controller) calls the same ones, so nothing here assumes the caller
is on the daemon's machine.

A change to a column that the change log covers (``piloop.db.triggers.changelog``) gets its event from the trigger,
signed by the connection's actor, so an operation writes no second event for it. A reason that no column holds goes
into a ``note`` on the row. An operation emits the events of the changes that no trigger logs.
"""

from collections.abc import Sequence
from contextlib import closing
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal, assert_never

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from piloop.actor import Actor
from piloop.agents.roles import files_of
from piloop.campaign.campaigns import repository
from piloop.campaign.ideas import Definition, NotASeed, develop
from piloop.campaign.stop import held, request_stop, settled, unclosed
from piloop.config import CampaignConfig
from piloop.db.events import emit
from piloop.db.models import (
    ENDED,
    OPEN_RUNS,
    AgentSession,
    Base,
    Campaign,
    Candidate,
    Comment,
    Idea,
    IdeaLink,
    Measurement,
    Origin,
    Pause,
    Run,
)
from piloop.db.session import write_transaction
from piloop.ids import (
    AgentSessionId,
    CampaignId,
    CampaignStatus,
    CandidateId,
    Cause,
    CommentKind,
    Ending,
    IdeaId,
    IdeaStatus,
    LinkKind,
    RoleName,
    TargetKind,
    TargetRef,
    TrackName,
)
from piloop.plugins.role.contract import Files
from piloop.plugins.vcs.contract import VcsError
from piloop.plugins.verdict_rule.contract import Outcome, Trunk
from piloop.scoring.api import Report
from piloop.scoring.ledger import (
    MOVED,
    Rebased,
    RejudgeRequested,
    follow_up,
    head,
    node,
    on_moved,
    record,
    track_of,
    verdict_rule,
)
from piloop.scoring.verdict import Refusal, label, override, pending, refusal, rejudgeable, write


class Refused(Exception):
    """An operation refused its input and changed nothing. The message says why."""


def _get[M: Base](s: Session, model: type[M], key: int, name: str) -> M:
    found = s.get(model, key)
    if found is None:
        raise Refused(f'no {name}#{key}')
    return found


def _reason(reason: str) -> str:
    if not reason.strip():
        raise Refused('a reason is required: say why, with ids')
    return reason.strip()


def pause(s: Session, campaign_id: CampaignId, *, evals: bool, reason: str) -> Campaign:
    """Pause the campaign: nothing launches, and with ``evals`` the daemon starts no measurement either.

    No run is queued, no role is requested and no requested row launches, so research files no ideas. A sweep that
    runs already files its ideas as ``proposed`` (``review_first``). Running work goes on and its commits are scored.
    Nothing is escalated. Without ``evals``, the daemon still measures what a new evaluator or a request needs, so the
    pause can end on a measured head. A pause is state in the database, so the daemon holds wherever the caller is.

    Raises:
        Refused: the reason is blank, or the campaign is paused that way already.
    """
    campaign = _get(s, Campaign, campaign_id, 'campaign')
    why = _reason(reason)
    scope = Pause.ALL if evals else Pause.LAUNCHES
    if campaign.paused is scope:
        raise Refused(f'campaign {campaign.name} is paused already ({scope}): {campaign.paused_reason}')
    campaign.paused, campaign.paused_reason = scope, why
    return campaign


def hold_on_failed_baseline(s: Session, candidate: Candidate, logs: Path | None) -> None:
    """Pause the launches of an unpaused campaign when ``candidate`` is its baseline and each evaluation of it failed.

    Nothing can be judged against a baseline without a score, and no agent should spend on a campaign that has none.
    One that an older evaluator measured has one, and its campaign goes on. The pause measures what is asked, so it
    can end on a measured baseline. The reason is the last error, with ``logs``, where that evaluation left its logs,
    and what to do. The caller commits.
    """
    errors = [m.error for m in candidate.measurements]
    campaign = s.get_one(Campaign, candidate.campaign_id)
    if candidate.origin is not Origin.BASELINE or campaign.paused is not None or None in errors:
        return
    where = '' if logs is None else f'; its logs are in {logs}'
    fix = f'fix it, then `piloop candidate remeasure {candidate.id}` and `piloop resume {campaign.name}`'
    pause(s, campaign.id, evals=False, reason=f'the baseline failed to evaluate: {errors[-1]}{where}; {fix}')


def resume(s: Session, campaign_id: CampaignId) -> Pause:
    """Resume a paused campaign, and return what it held. The daemon schedules it again at once.

    Raises:
        Refused: the campaign is not paused.
    """
    campaign = _get(s, Campaign, campaign_id, 'campaign')
    if campaign.paused is None:
        raise Refused(f'campaign {campaign.name} is not paused')
    held = campaign.paused
    campaign.paused, campaign.paused_reason = None, None
    return held


def stop(s: Session, campaign_id: CampaignId, actor: Actor, *, reason: str, ending: Ending) -> Campaign:
    """Ask the daemon to stop the campaign at once (``request_stop``), as when a ``[stop]`` condition holds.

    On a stopped campaign, an ending ``now`` ends the implementers that still wind down.

    Raises:
        Refused: the campaign is stopped already, and the ending is not ``now``.
    """
    campaign = _get(s, Campaign, campaign_id, 'campaign')
    if campaign.status is CampaignStatus.STOPPED and ending is not Ending.NOW:
        raise Refused(
            f'{campaign.name} is stopped already ({campaign.stopped_reason});'
            ' `piloop campaign stop --kill` ends its implementers at once'
        )
    request_stop(s, campaign, actor, reason=reason, ending=ending)
    return campaign


class StillStopping(Refused):
    """``retire`` refused a campaign whose stop is not over: ``agents`` are its open agents, by id."""

    def __init__(self, campaign: Campaign, agents: Sequence[AgentSessionId]) -> None:
        self.agents = tuple(agents)
        named = f'; agents {", ".join(f"#{i}" for i in agents)} are not over' if agents else ''
        super().__init__(
            f'{campaign.name} is still stopping{named}: retire it once its closing review and runs are over'
        )


def retire(s: Session, campaign_id: CampaignId, actor: Actor, *, reason: str) -> Campaign:
    """Retire a stopped campaign for good: it changes no more, and another campaign may take its name.

    Its rows, events and archives stay, and it is found by its id. Listings leave it out unless asked. The reason goes
    into a note on the campaign. It takes the write lock before it checks, so no agent is requested or launched between
    the check and the change: the session must have no open transaction.

    Raises:
        Refused: the reason is blank, or the campaign is not stopped, or is retired already.
        StillStopping: the stop is not over: the closing review has not started, a run is not scored, or an agent of
            the campaign is open (requested, starting, running or ending).
    """
    why = _reason(reason)
    write_transaction(s)
    s.expire_all()  # read inside the lock what an earlier read of this session may have cached
    campaign = _get(s, Campaign, campaign_id, 'campaign')
    match campaign.status:
        case CampaignStatus.ACTIVE:
            raise Refused(f'{campaign.name} is active: stop it first, with `piloop campaign stop {campaign.name}`')
        case CampaignStatus.RETIRED:
            raise Refused(f'campaign #{campaign.id} ({campaign.name}) is retired already')
        case CampaignStatus.STOPPED:
            pass
        case never:
            assert_never(never)
    open_ = s.scalars(
        select(AgentSession.id).where(AgentSession.campaign_id == campaign.id, AgentSession.state.not_in(ENDED))
    ).all()
    if unclosed(s, campaign) or not settled(s, campaign) or open_:
        raise StillStopping(campaign, open_)
    campaign.status = CampaignStatus.RETIRED
    target = TargetRef(TargetKind.CAMPAIGN, campaign.id)
    s.add(Comment(target=target, author=actor.signature, kind=CommentKind.NOTE, body=f'retired: {why}'))
    return campaign


def request_agent(
    s: Session, campaign_id: CampaignId, role: RoleName, *, harness: str | None, model: str | None, reason: str
) -> AgentSession:
    """Ask the daemon for one more agent of a sweeping role: it launches it at once, within ``max_concurrent``.

    Raises:
        Refused: the campaign has no sweeping role ``role``.
    """
    campaign = _get(s, Campaign, campaign_id, 'campaign')
    roles = campaign.config.roles
    sweeping = roles.sweeping()
    if role not in sweeping:
        by_itself = (
            ' The daemon launches implementers by itself, one for each queued run.'
            if role in roles.doing_runs()
            else ''
        )
        raise Refused(f'{campaign.name} has no sweeping role {role!r}; it has {", ".join(sweeping)}.{by_itself}')
    agent = AgentSession(kind=role, campaign_id=campaign.id, harness=harness, model=model, reason=reason)
    s.add(agent)
    s.flush()
    return agent


def set_parent(s: Session, campaign_id: CampaignId, parent_id: CampaignId | None) -> Campaign:
    """Put the campaign under ``parent_id`` in the tree of campaigns, or make it a root with None, and return it.

    A campaign has at most one parent, a campaign of the same piloop home. The tree is for reading: it changes nothing
    about how either campaign runs. The ``campaigns_parent`` triggers refuse a cycle, whoever writes the column, and
    the ``campaign.parent_id`` event is the trigger's.

    Raises:
        Refused: a campaign is missing, the parent is the campaign's already, or the parent is the campaign itself or
            one of its descendants.
    """
    campaign = _get(s, Campaign, campaign_id, 'campaign')
    parent = None if parent_id is None else _get(s, Campaign, parent_id, 'campaign')
    if campaign.parent_id == parent_id:
        where = 'a root' if parent is None else f'under {parent.name}'
        raise Refused(f'{campaign.name} is {where} already')
    if parent is None:
        campaign.parent_id = None
        return campaign
    try:
        with s.begin_nested():
            campaign.parent_id = parent.id
    except IntegrityError as e:
        raise Refused(f'{campaign.name} cannot be under {parent.name}: {e.orig}') from e
    return campaign


def review_first(campaign: Campaign, actor: Actor, *, new: bool) -> str | None:
    """Return why an idea that ``actor`` adds (``new``) or develops waits for a review (``proposed``), or None.

    Only an agent of a sweeping role waits: while the campaign is paused, and for a new idea while its role's
    ``max_ideas`` is 0 (a sweep reads it once, in its brief; the operator may have set it to 0 since). The orchestrator
    and humans paused the campaign themselves.
    """
    if (agent := actor.launched) is None:
        return None
    name = agent.role
    role = campaign.config.roles.sweeping().get(name)
    if role is None:
        return None
    if campaign.paused is not None:
        return f'campaign {campaign.name} is paused'
    if new and role.max_ideas == 0:
        return f'[plugins.role.{name}] max_ideas = 0'
    return None


@dataclass(frozen=True)
class Overridden:
    """What ``set_verdict`` did: the verdict that the candidate had, and its merge onto the head for a keep off it."""

    old: str
    rebased: Rebased | None = None


def set_verdict(
    s: Session,
    candidate_id: CandidateId,
    to: Outcome,
    *,
    cause: Literal[Cause.NOTE] | None = None,
    reason: str,
    actor: Actor,
) -> Overridden:
    """Set ``actor``'s verdict on a candidate, and return what that did. The one way to override a verdict.

    The verdict is ``actor``'s now, so the daemon never changes it. The ``candidate.verdict`` event (a trigger) has the
    reason. A keep of a trunk run that does not build on the head (``missing_head``) goes the way of the daemon's
    keep against a bar that is no longer the head: the head is the newest trunk keep, and a keep that lacks the
    head's changes would drop them from the trunk. So the candidate is ``interesting`` (``Cause.MOVED``), and the
    verdict rule says what follows (``ledger.on_moved``). By default its merge onto the head is a pending candidate that
    the daemon measures and judges against the head (``ledger.unjudged``), and a merge that conflicts files a seed to
    compose it. A keep of a branch run never moves the
    head. With ``Cause.NOTE``, it keeps a regressed result in view as a lead: an ``interesting`` verdict whose reason
    is followed by the one that it notes.

    Raises:
        Refused: the reason is blank, ``verdict.refusal`` refuses it, or it is a keep off the head of a candidate that
            no run made, which no seed can compose.
    """
    candidate = _get(s, Candidate, candidate_id, 'candidate')
    why = _reason(reason)
    name = f'candidate#{candidate.id}'
    match refused := refusal(candidate, to, cause):
        case Refusal.PENDING:
            raise Refused(f'{name} is pending: the daemon judges it now; set its verdict once it is judged')
        case Refusal.NOT_JUDGED:
            raise Refused(f'{name} is {candidate.origin}, which is never judged: only a judgement is overridden')
        case Refusal.SAME:
            raise Refused(f'{name} is {to} already ({candidate.verdict_reason or "no reason"})')
        case Refusal.NO_RESULT:
            raise Refused(
                f'{name} is {candidate.verdict} ({candidate.verdict_reason or "no reason"}):'
                ' only a measured discard or interesting takes a note'
            )
        case None:
            pass
        case _:
            assert_never(refused)
    old = label(candidate)
    if to is Outcome.KEEP and (trunk := missing_head(s, candidate)) is not None:
        if candidate.run is None:
            raise Refused(
                f'{name} does not build on the head candidate#{trunk.id}: as a keep it would drop what'
                ' the head kept. Mark it interesting, or add an idea that merges it onto the head'
            )
        reason = MOVED.format(head=trunk.id) + f'kept by hand: {why}'
        write(candidate, Outcome.INTERESTING, cause=Cause.MOVED, reason=reason, by=actor)
        campaign = s.get_one(Campaign, candidate.campaign_id)
        with closing(repository(campaign)) as repo:
            return Overridden(old, on_moved(s, campaign.config, repo, candidate, trunk))
    override(candidate, to, cause=cause, reason=why, by=actor)
    return Overridden(old)


def missing_head(s: Session, candidate: Candidate) -> Candidate | None:
    """Return the head if a keep of ``candidate`` would be a trunk keep without it, else None.

    A candidate builds on the head when the head is among its ancestors (``parent_id``): a commit of a run that
    started from it. Else the vcs decides (``Repository.contains``): a merge onto the head, or a compose that took it.
    A keep that would not raise the trunk's bar (``VerdictRule.raises_bar``), such as a branch run's, never moves the
    head.

    Raises:
        Refused: the vcs cannot tell, for example because the campaign repo is gone.
    """
    campaign = s.get_one(Campaign, candidate.campaign_id)
    kept = replace(node(s, candidate), verdict=Outcome.KEEP)
    if not verdict_rule(campaign.config).raises_bar(kept, Trunk()):
        return None
    trunk = head(s, campaign)
    ancestor: Candidate | None = candidate
    while trunk is not None and ancestor is not None and ancestor.id != trunk.id:
        ancestor = ancestor.parent
    if trunk is None or ancestor is not None:
        return None
    try:
        with closing(repository(campaign)) as repo:
            builds = repo.contains(candidate.revision, trunk.revision)
    except VcsError as e:
        raise Refused(
            f'the vcs cannot tell if candidate#{candidate.id} builds on the head candidate#{trunk.id}: {e}'
        ) from e
    return None if builds else trunk


@dataclass(frozen=True)
class Added:
    """What ``add_measurement`` recorded, and what it does to the candidate's verdict."""

    measurement: Measurement
    rejudged: bool
    """Whether the daemon judges the candidate again (``RejudgeRequested``)."""
    effect: str
    """What happens to the verdict, in one line: when the daemon judges it again, or why the verdict stays."""


def add_measurement(s: Session, candidate_id: CandidateId, report: Report, *, actor: Actor) -> Added:
    """Record ``report`` as a measurement of the candidate under the campaign's current evaluator.

    The daemon judges the candidate again when its verdict is the daemon's ``crash``, ``discard`` or ``interesting``
    on a run, or it is a run's commit that the daemon never measured (``verdict.rejudgeable``), with every measurement
    under the current evaluator. It does that when it measures
    the campaign again: not while the campaign is paused with its evaluations, or held by its stop rule. Any
    other verdict stays, and a keep's mean, which later runs from it must beat, includes the new measurement. Emits
    ``MeasurementRecorded``, shaped like the daemon's, and the request (``RejudgeRequested``).

    Raises:
        Refused: the campaign has no evaluator yet, the candidate is pending (the daemon is measuring and judging it),
            the report names a metric that ``piloop.toml`` does not, or a report without an error lacks one.
    """
    candidate = _get(s, Candidate, candidate_id, 'candidate')
    campaign = s.get_one(Campaign, candidate.campaign_id)
    if campaign.evaluator_id is None:
        raise Refused(f'campaign {campaign.name} has no evaluator yet: run `piloop init`')
    if pending(candidate):
        raise Refused(f'candidate#{candidate.id} is pending: the daemon judges it now; add the measurement after that')
    cfg = campaign.config
    names = {m.name for m in cfg.metrics}
    if unknown := sorted(report.metrics.keys() - names):
        raise Refused(f'piloop.toml has no metric {", ".join(unknown)}: its metrics are {", ".join(sorted(names))}')
    missing = sorted(names - report.metrics.keys())
    if report.error is None and missing:
        raise Refused(f'the report has no {", ".join(missing)}: give every metric, or the error')
    measurement = record(
        s,
        candidate,
        campaign.evaluator_id,
        actor,
        metrics=report.metrics,
        error=report.error,
        seconds=report.seconds,
    )
    if not rejudgeable(candidate):
        return Added(measurement, False, _stays(candidate))
    asked = RejudgeRequested(measurement=measurement.id)
    emit(s, asked, TargetRef(TargetKind.CANDIDATE, candidate.id), actor)
    later = measuring_held(s, campaign, cfg)
    return Added(measurement, True, f'the daemon judges it again with all its measurements{later}')


def _stays(candidate: Candidate) -> str:
    if candidate.verdict is Outcome.KEEP:
        return 'its keep stays; the runs that start from it must beat its new mean'
    return f'its verdict stays: {label(candidate)}, set by {candidate.verdict_by or "nobody"}'


def measuring_held(s: Session, campaign: Campaign, cfg: CampaignConfig) -> str:
    """Return `` once …``: when the daemon measures the campaign, as the scheduler's pass decides, or '' for now.

    A stopped campaign is measured too, but only what someone asked for (``ledger.due``).
    """
    if campaign.paused is Pause.ALL:
        return f' once campaign {campaign.name} is resumed (its evaluations are paused)'
    if (why := held(s, campaign, cfg)) is not None:
        return f' once the stop rule no longer holds it ({why})'
    return ''


def files(campaign: Campaign, actor: Actor) -> Files | None:
    """Return what ``actor`` may file in ``campaign`` (``[plugins.role.<name>] files``); None when it is not a role.

    Raises:
        Refused: the actor's role files nothing.
    """
    filed = files_of(campaign.config, actor)
    if filed is Files.NOTHING:
        raise Refused(
            f"{actor} files no ideas or seeds: say the hunch in your run's finding, with pointers."
            ' Research reads the findings of the implementers.'
        )
    return filed


def add_idea(
    s: Session,
    campaign_id: CampaignId,
    title: str,
    *,
    actor: Actor,
    hypothesis: str = '',
    plan: str | None = None,
    provenance: str | None = None,
    family: str | None = None,
    priority: int = 2,
    harness: str | None = None,
    base: CandidateId | None = None,
    proposed: bool = False,
    track: TrackName | None = None,
    after: Sequence[IdeaId] = (),
    supersedes: Sequence[IdeaId] = (),
) -> tuple[Idea, str | None]:
    """Add a defined idea, with its links, and return it with why it waits for a review (``review_first``), or None.

    The idea is ``ready``, or ``proposed`` with ``proposed`` or a review first. The idea and its links are written in
    the caller's transaction, so the daemon never sees the idea without them. Emits ``idea.created``.

    Raises:
        Refused: the actor files only seeds or nothing, the base or the track is not the campaign's, or a link is
            refused (``link``).
    """
    campaign = _get(s, Campaign, campaign_id, 'campaign')
    if files(campaign, actor) is Files.SEEDS:
        raise Refused(
            f'{actor} files seeds, not ideas: `piloop idea seed {campaign.name} "<hunch>" --from "<pointers>"`.'
            ' Research develops each seed with the whole campaign in view.'
        )
    check_base(s, campaign, base)
    held = review_first(campaign, actor, new=True)
    idea = Idea(
        campaign_id=campaign.id,
        track=track_for(campaign, track),
        title=title,
        hypothesis=hypothesis or title,
        plan=plan,
        provenance=provenance,
        family=family,
        priority=priority,
        harness=harness,
        base_candidate_id=base,
        status=IdeaStatus.PROPOSED if proposed or held is not None else IdeaStatus.READY,
        created_by=actor.signature,
    )
    s.add(idea)
    s.flush()
    _link_all(s, idea, after, supersedes)
    return idea, held


def file_seed(
    s: Session,
    campaign_id: CampaignId,
    hunch: str,
    *,
    actor: Actor,
    provenance: str,
    base: CandidateId | None = None,
    track: TrackName | None = None,
) -> Idea:
    """File a seed: a hunch with pointers, for research to develop or reject. Emits ``idea.created``.

    Raises:
        Refused: the actor files nothing, the pointers are blank, or the base or the track is not the campaign's.
    """
    campaign = _get(s, Campaign, campaign_id, 'campaign')
    files(campaign, actor)
    if not provenance.strip():
        raise Refused('a seed needs pointers: --from "<ids, paths, numbers>"')
    check_base(s, campaign, base)
    idea = Idea(
        campaign_id=campaign.id,
        track=track_for(campaign, track),
        title=hunch,
        hypothesis=hunch,
        provenance=provenance,
        base_candidate_id=base,
        status=IdeaStatus.SEED,
        created_by=actor.signature,
    )
    s.add(idea)
    s.flush()
    return idea


def develop_seed(
    s: Session,
    idea_id: IdeaId,
    definition: Definition,
    *,
    actor: Actor,
    title: str | None = None,
    family: str | None = None,
    base: CandidateId | None = None,
    priority: int | None = None,
    proposed: bool = False,
    after: Sequence[IdeaId] = (),
    supersedes: Sequence[IdeaId] = (),
) -> tuple[Idea, str | None]:
    """Develop a seed into a defined idea (``ideas.develop``) with its links, and return it with why it waits.

    Raises:
        Refused: the actor files only seeds or nothing, the idea is not a seed, the definition lacks a part, the base
            is not the campaign's, or a link is refused (``link``): a link from a seed can close a cycle.
    """
    idea = _get(s, Idea, idea_id, 'idea')
    if files(idea.campaign, actor) is Files.SEEDS:
        raise Refused(f'{actor} files seeds; research develops them')
    check_base(s, idea.campaign, base)
    held = review_first(idea.campaign, actor, new=False)
    try:
        develop(
            idea,
            definition,
            title=title,
            family=family,
            base=base,
            priority=priority,
            proposed=proposed or held is not None,
        )
    except (NotASeed, ValueError) as e:
        raise Refused(str(e)) from e
    _link_all(s, idea, after, supersedes)
    return idea, held


def note_candidate(
    s: Session,
    candidate_id: CandidateId,
    reason: str,
    *,
    actor: Actor,
    follow_ups: Sequence[str] = (),
    track: TrackName | None = None,
) -> tuple[Candidate, list[Idea]]:
    """Keep a regressed result in view as a lead (``set_verdict``, ``Cause.NOTE``), with a seed pinned to it per follow-up.

    The follow-ups are on ``track``, else on the track of the candidate's run. Returns the candidate and the new seeds.

    Raises:
        Refused: the candidate is missing or not a measured discard or interesting, the reason is blank, the actor
            files nothing and gave follow-ups, or the track is not the campaign's.
    """
    candidate = _get(s, Candidate, candidate_id, 'candidate')
    campaign = s.get_one(Campaign, candidate.campaign_id)
    if follow_ups:
        files(campaign, actor)
    set_verdict(s, candidate.id, Outcome.INTERESTING, cause=Cause.NOTE, reason=reason, actor=actor)
    on = track_for(campaign, track) if track is not None else track_of(campaign.config, candidate).name
    return candidate, follow_up(s, candidate, follow_ups, reason, actor, track=on)


def _link_all(s: Session, idea: Idea, after: Sequence[IdeaId], supersedes: Sequence[IdeaId]) -> None:
    for kind, targets in ((LinkKind.AFTER, after), (LinkKind.SUPERSEDES, supersedes)):
        for target in targets:
            _link(s, idea, kind, target)


SETTABLE = (IdeaStatus.PROPOSED, IdeaStatus.READY, IdeaStatus.PARKED, IdeaStatus.REJECTED, IdeaStatus.DONE)
"""The statuses that ``set_idea_status`` sets. ``seed`` is filed and ``scheduled`` is the daemon's."""
STOPPING = (IdeaStatus.PARKED, IdeaStatus.REJECTED)


def open_run(idea: Idea) -> Run | None:
    """Return the idea's run that is queued, running or being scored, or None.

    While it is open, the daemon still owns the idea: it settles the idea's status when the run's commits are judged,
    and it judges them on the idea's track. So only parking or rejecting the idea is safe meanwhile.
    """
    return next((r for r in idea.runs if r.status in OPEN_RUNS or r.scoring), None)


def check_status(idea: Idea, status: IdeaStatus) -> None:
    """Refuse a status that ``set_idea_status`` would refuse, before a client asks for the reason.

    Raises:
        Refused: ``status`` is the idea's already, or not one of ``SETTABLE``. A seed is defined first (``piloop idea
            develop``), and an idea that is scheduled or has an open run (``open_run``) can only be parked or rejected:
            ``ready`` would queue a second run, and ``done`` would start the ideas after it before its run is judged.
    """
    if idea.status is status:
        raise Refused(f'idea#{idea.id} is {status} already')
    if status not in SETTABLE:
        raise Refused(f'{status} is not set by hand: give {", ".join(SETTABLE)}')
    if idea.status is IdeaStatus.SEED and status not in STOPPING:
        raise Refused(
            f'idea#{idea.id} is a seed: research defines it first (piloop idea develop {idea.id} …), or park or reject it'
        )
    if status not in STOPPING and (busy := _busy(idea)) is not None:
        raise Refused(f'idea#{idea.id} {busy}; park or reject it, or wait for the run to end')


def _busy(idea: Idea) -> str | None:
    """Say why the daemon still owns the idea (``open_run``), or None."""
    if (run := open_run(idea)) is not None:
        return f'has an open run: run#{run.id} is {"being scored" if run.scoring else run.status}'
    if idea.status is IdeaStatus.SCHEDULED:
        return 'is scheduled: its run is open'
    return None


def set_idea_status(s: Session, idea_id: IdeaId, status: IdeaStatus, *, reason: str, actor: Actor) -> IdeaStatus:
    """Set the idea's status, with a ``note`` on it that says why, and return the status it had.

    The ``idea.status`` event is the trigger's. Parking or rejecting an idea with an open run lets the run finish; the
    daemon then leaves the idea as it is.

    Raises:
        Refused: the reason is blank, or ``check_status`` refuses.
    """
    idea = _get(s, Idea, idea_id, 'idea')
    why = _reason(reason)
    check_status(idea, status)
    old = idea.status
    _note(s, idea, f'{old} → {status}: {why}', actor)
    idea.status = status
    return old


def link(s: Session, idea_id: IdeaId, kind: LinkKind, target_id: IdeaId, *, reason: str, actor: Actor) -> None:
    """Link the idea to ``target``: it runs *after* it, or it *supersedes* it, with a ``note`` that says why.

    The link is written in the caller's transaction, so the daemon sees it or queues the idea first, never in between.
    The ``idea.linked`` event is the trigger's.

    Raises:
        Refused: the reason is blank, or ``_link`` refuses.
    """
    idea = _get(s, Idea, idea_id, 'idea')
    why = _reason(reason)
    _link(s, idea, kind, target_id)
    _note(s, idea, f'{kind} idea#{target_id}: {why}', actor)


def _link(s: Session, idea: Idea, kind: LinkKind, target_id: IdeaId) -> None:
    """Add the link from ``idea``: every way to link ideas goes through here.

    Raises:
        Refused: the target is missing, the two are one idea or in two campaigns, the link exists, the idea is
            scheduled or has an open run (an ``after`` link comes too late for its run), or the link closes a cycle of
            its kind.
    """
    target = _get(s, Idea, target_id, 'idea')
    if idea.id == target.id:
        raise Refused(f'idea#{idea.id} cannot link to itself')
    if idea.campaign_id != target.campaign_id:
        raise Refused(f'idea#{target.id} is in another campaign than idea#{idea.id}')
    if any(lk.kind is kind and lk.target_id == target.id for lk in idea.links):
        raise Refused(f'idea#{idea.id} links {kind} idea#{target.id} already')
    if kind is LinkKind.AFTER and (busy := _busy(idea)) is not None:
        raise Refused(f'idea#{idea.id} {busy}: it started without idea#{target.id}')
    if _reaches(s, target.id, idea.id, kind):
        raise Refused(f'a cycle: idea#{target.id} already links {kind} idea#{idea.id}, through its {kind} links')
    idea.links.append(IdeaLink(target_id=target.id, kind=kind))
    s.flush()


def _reaches(s: Session, start: IdeaId, goal: IdeaId, kind: LinkKind) -> bool:
    """Return whether ``kind`` links lead from idea ``start`` to idea ``goal``."""
    seen, todo = {start}, [start]
    while todo:
        step = select(IdeaLink.target_id).where(IdeaLink.idea_id.in_(todo), IdeaLink.kind == kind)
        todo = [t for t in s.scalars(step) if t not in seen]
        if goal in todo:
            return True
        seen.update(todo)
    return False


def unlink(s: Session, idea_id: IdeaId, kind: LinkKind, target_id: IdeaId, *, reason: str, actor: Actor) -> None:
    """Delete the idea's ``kind`` link to ``target``, with a ``note`` that says why. The trigger logs ``idea.unlinked``.

    Raises:
        Refused: the reason is blank, the idea is missing, or it has no such link.
    """
    idea = _get(s, Idea, idea_id, 'idea')
    why = _reason(reason)
    found = next((lk for lk in idea.links if lk.kind is kind and lk.target_id == target_id), None)
    if found is None:
        raise Refused(f'idea#{idea.id} has no {kind} link to idea#{target_id}')
    idea.links.remove(found)
    _note(s, idea, f'no longer {kind} idea#{target_id}: {why}', actor)


def set_base(
    s: Session, idea_id: IdeaId, candidate_id: CandidateId | None, *, reason: str, actor: Actor
) -> CandidateId | None:
    """Pin the idea to start from ``candidate_id``, or clear the pin with None, and return the old base.

    A cleared pin runs from the head at the time the run starts. A run already open keeps its base, and the next
    attempt takes the new one. A ``note`` says why; the ``idea.base_candidate_id`` event is the trigger's.

    Raises:
        Refused: the reason is blank, the idea is missing, the candidate is not one of its campaign, or it is the base.
    """
    idea = _get(s, Idea, idea_id, 'idea')
    why = _reason(reason)
    check_base(s, idea.campaign, candidate_id)
    old = idea.base_candidate_id
    if old == candidate_id:
        raise Refused(f'idea#{idea.id} starts from {_base(candidate_id)} already')
    _note(s, idea, f'base {_base(old)} → {_base(candidate_id)}: {why}', actor)
    idea.base_candidate_id = candidate_id
    return old


def _base(candidate_id: CandidateId | None) -> str:
    return 'the head' if candidate_id is None else f'candidate#{candidate_id}'


def check_base(s: Session, campaign: Campaign, candidate_id: CandidateId | None) -> None:
    """Refuse a base that is not unset or a candidate of ``campaign``.

    Raises:
        Refused: no such candidate in the campaign.
    """
    if candidate_id is None:
        return
    if s.scalar(select(Candidate.campaign_id).where(Candidate.id == candidate_id)) != campaign.id:
        raise Refused(f'no candidate#{candidate_id} in campaign {campaign.name}')


def set_track(s: Session, idea_id: IdeaId, track: TrackName, *, reason: str, actor: Actor) -> TrackName:
    """Move the idea to another track of its campaign, with a ``note`` that says why, and return the old track.

    The ``idea.track`` event is the trigger's.

    Raises:
        Refused: the reason is blank, the idea is missing, the track is not in ``piloop.toml``, it is the idea's track,
            or the idea is scheduled or has an open run: the run's commits are judged on the idea's track.
    """
    idea = _get(s, Idea, idea_id, 'idea')
    why = _reason(reason)
    track = track_for(idea.campaign, track)
    if idea.track == track:
        raise Refused(f'idea#{idea.id} is on track {track} already')
    if (busy := _busy(idea)) is not None:
        raise Refused(f'idea#{idea.id} {busy}: its commits are judged on track {idea.track}')
    old = idea.track
    _note(s, idea, f'track {old} → {track}: {why}', actor)
    idea.track = track
    return old


def track_for(campaign: Campaign, track: TrackName | None) -> TrackName:
    """Return the track for an idea: ``track`` when ``piloop.toml`` has it, else the only track.

    Raises:
        Refused: no track is given and the campaign has several, or the campaign has no such track.
    """
    tracks = [t.name for t in campaign.config.all_tracks]
    if track is None and len(tracks) == 1:
        return tracks[0]
    if track is None or track not in tracks:
        given = 'name one with --track' if track is None else f'there is no track {track!r}'
        raise Refused(f'{campaign.name} has tracks {", ".join(tracks)}: {given}')
    return track


def _note(s: Session, idea: Idea, body: str, actor: Actor) -> None:
    s.add(Comment(target=TargetRef(TargetKind.IDEA, idea.id), author=actor.signature, kind=CommentKind.NOTE, body=body))
