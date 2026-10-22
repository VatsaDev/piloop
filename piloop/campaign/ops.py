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
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, assert_never

from git import GitError
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from piloop import git
from piloop.actor import Actor, Agent, System
from piloop.agents.roles import files_of
from piloop.campaign.ideas import Definition, NotASeed, develop
from piloop.campaign.stop import held
from piloop.config import CampaignConfig, config_of
from piloop.db.events import emit
from piloop.db.models import (
    OPEN_RUNS,
    Base,
    Campaign,
    Candidate,
    Comment,
    CommentKind,
    Idea,
    IdeaLink,
    IdeaStatus,
    LinkKind,
    Measurement,
    Pause,
    Run,
    TargetKind,
    Verdict,
)
from piloop.plugins.role.contract import Files
from piloop.scoring.ledger import REJUDGE_REQUESTED, NoteRefused, follow_up, head, note, rejudgeable, track_of


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


def pause(s: Session, campaign_id: int, *, evals: bool, reason: str) -> Campaign:
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


def resume(s: Session, campaign_id: int) -> Pause:
    """Resume a paused campaign, and return what it held. The daemon schedules it again on its next tick.

    Raises:
        Refused: the campaign is not paused.
    """
    campaign = _get(s, Campaign, campaign_id, 'campaign')
    if campaign.paused is None:
        raise Refused(f'campaign {campaign.name} is not paused')
    held = campaign.paused
    campaign.paused, campaign.paused_reason = None, None
    return held


def set_parent(s: Session, campaign_id: int, parent_id: int | None) -> Campaign:
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
    match actor:
        case Agent(role=name):
            role = config_of(campaign).roles.sweeping().get(name)
        case System():
            return None
        case _:
            assert_never(actor)
    if role is None:
        return None
    if campaign.paused is not None:
        return f'campaign {campaign.name} is paused'
    if new and role.max_ideas == 0:
        return f'[roles.{name}] max_ideas = 0'
    return None


OVERRIDES = (Verdict.KEEP, Verdict.INTERESTING, Verdict.DISCARD, Verdict.CRASH)
"""The judgements: what ``set_verdict`` sets, and what it overrides. The other verdicts are states: ``pending`` (the
daemon judges it now), ``reference`` (an import), ``unmeasured`` and ``recovered`` (nothing was measured)."""


def set_verdict(s: Session, candidate_id: int, verdict: Verdict, *, reason: str, actor: Actor) -> Verdict:
    """Override a candidate's judgement with ``actor``'s, and return the verdict it had.

    The verdict is ``actor``'s now, so the daemon never changes it. The ``candidate.verdict`` event (a trigger) has the
    reason. A keep of a trunk run must build on the head (``missing_head``): the head is the newest trunk keep, and a
    keep that lacks the head's changes would drop them from the trunk. A keep of a branch run never moves the head.

    Raises:
        Refused: the reason is blank, the old or the new verdict is not a judgement, the candidate has that verdict
            already, or it would be a trunk keep that does not build on the head.
    """
    candidate = _get(s, Candidate, candidate_id, 'candidate')
    why = _reason(reason)
    if verdict not in OVERRIDES:
        raise Refused(f'{verdict} is not a judgement: give {", ".join(OVERRIDES)}')
    if candidate.verdict is Verdict.PENDING:
        raise Refused(f'candidate#{candidate.id} is pending: the daemon judges it now; override it once it is judged')
    if candidate.verdict not in OVERRIDES:
        raise Refused(
            f'candidate#{candidate.id} is {candidate.verdict}, not a judgement: only a judgement is overridden'
        )
    if candidate.verdict is verdict:
        raise Refused(f'candidate#{candidate.id} is {verdict} already ({candidate.verdict_reason or "no reason"})')
    if verdict is Verdict.KEEP and (trunk := missing_head(s, candidate)) is not None:
        raise Refused(
            f'candidate#{candidate.id} does not build on the head candidate#{trunk.id}: as a keep it would drop what'
            ' the head kept. Mark it interesting, or add an idea that merges it onto the head'
        )
    old = candidate.verdict
    candidate.verdict, candidate.verdict_by, candidate.verdict_reason = verdict, actor.signature, why
    return old


def missing_head(s: Session, candidate: Candidate) -> Candidate | None:
    """Return the head if a keep of ``candidate`` would be a trunk keep without it, else None.

    A candidate builds on the head when the head is among its ancestors (``parent_id``): a commit of a run that
    started from it. Else git decides (``git.contains``): a merge onto the head, or a compose that cherry-picked it.
    A candidate of a branch run is a branch keep, which never moves the head.

    Raises:
        Refused: git cannot tell, for example because the campaign repo is gone.
    """
    if candidate.run is not None and candidate.run.branch:
        return None
    campaign = s.get_one(Campaign, candidate.campaign_id)
    trunk = head(s, campaign)
    ancestor: Candidate | None = candidate
    while trunk is not None and ancestor is not None and ancestor.id != trunk.id:
        ancestor = ancestor.parent
    if trunk is None or ancestor is not None:
        return None
    try:
        builds = git.contains(git.open_repo(Path(campaign.path)), candidate.commit_sha, trunk.commit_sha)
    except GitError as e:
        raise Refused(
            f'git cannot tell if candidate#{candidate.id} builds on the head candidate#{trunk.id}: {e}'
        ) from e
    return None if builds else trunk


class Report(BaseModel):
    """A measurement made outside the daemon, for example by running the evaluate command by hand."""

    model_config = ConfigDict(extra='forbid')

    metrics: dict[str, Annotated[float, Field(strict=True, allow_inf_nan=False)]]
    """Each metric's value, as the evaluate command's ``METRIC`` lines would give it."""
    seconds: Annotated[float, Field(ge=0, allow_inf_nan=False)] | None = None
    """How long the evaluation took."""
    error: str | None = None
    """Why the evaluation failed. A failed measurement may lack metrics."""


@dataclass(frozen=True)
class Added:
    """What ``add_measurement`` recorded, and what it does to the candidate's verdict."""

    measurement: Measurement
    rejudged: bool
    """Whether the daemon judges the candidate again (``REJUDGE_REQUESTED``)."""
    effect: str
    """What happens to the verdict, in one line: when the daemon judges it again, or why the verdict stays."""


def add_measurement(s: Session, candidate_id: int, report: Report, *, actor: Actor) -> Added:
    """Record ``report`` as a measurement of the candidate under the campaign's current evaluator.

    The daemon judges the candidate again when its verdict is the daemon's ``crash``, ``discard`` or ``interesting`` on
    a run (``ledger.rejudgeable``), with every measurement under the current evaluator. It does that when it measures
    the campaign again: not while the campaign is paused with its evaluations, or held by its stop rule. Any
    other verdict stays, and a keep's mean, which later runs from it must beat, includes the new measurement. Emits
    ``measurement.recorded``, shaped like the daemon's, and the request (``REJUDGE_REQUESTED``).

    Raises:
        Refused: the campaign has no evaluator yet, the candidate is pending (the daemon is measuring and judging it),
            the report names a metric that ``piloop.toml`` does not, or a report without an error lacks one.
    """
    candidate = _get(s, Candidate, candidate_id, 'candidate')
    campaign = s.get_one(Campaign, candidate.campaign_id)
    if campaign.evaluator_id is None:
        raise Refused(f'campaign {campaign.name} has no evaluator yet: run `piloop init`')
    if candidate.verdict is Verdict.PENDING:
        raise Refused(f'candidate#{candidate.id} is pending: the daemon judges it now; add the measurement after that')
    cfg = config_of(campaign)
    names = {m.name for m in cfg.metrics}
    if unknown := sorted(report.metrics.keys() - names):
        raise Refused(f'piloop.toml has no metric {", ".join(unknown)}: its metrics are {", ".join(sorted(names))}')
    missing = sorted(names - report.metrics.keys())
    if report.error is None and missing:
        raise Refused(f'the report has no {", ".join(missing)}: give every metric, or the error')
    measurement = Measurement(
        candidate_id=candidate.id,
        evaluator_id=campaign.evaluator_id,
        metrics=report.metrics,
        error=report.error,
        seconds=report.seconds,
    )
    s.add(measurement)
    s.flush()
    payload: dict[str, object] = {
        'candidate': candidate.id,
        'metrics': report.metrics,
        'error': report.error,
        'run': None,
    }
    emit(s, 'measurement.recorded', TargetKind.MEASUREMENT, measurement.id, actor=actor, payload=payload)
    if not rejudgeable(candidate):
        return Added(measurement, False, _stays(candidate))
    emit(s, REJUDGE_REQUESTED, TargetKind.CANDIDATE, candidate.id, actor=actor, payload={'measurement': measurement.id})
    later = measuring_held(s, campaign, cfg)
    return Added(measurement, True, f'the daemon judges it again with all its measurements{later}')


def _stays(candidate: Candidate) -> str:
    if candidate.verdict is Verdict.KEEP:
        return 'its keep stays; the runs that start from it must beat its new mean'
    return f'its verdict stays: {candidate.verdict}, set by {candidate.verdict_by or "nobody"}'


def measuring_held(s: Session, campaign: Campaign, cfg: CampaignConfig) -> str:
    """Return `` once …``: when the daemon measures the campaign, as the scheduler's tick decides, or '' for now.

    A stopped campaign is measured too, but only what someone asked for (``ledger.due``).
    """
    if campaign.paused is Pause.ALL:
        return f' once campaign {campaign.name} is resumed (its evaluations are paused)'
    if (why := held(s, campaign, cfg)) is not None:
        return f' once the stop rule no longer holds it ({why})'
    return ''


def files(campaign: Campaign, actor: Actor) -> Files | None:
    """Return what ``actor`` may file in ``campaign`` (``[roles.<name>] files``); None when it is not a role.

    Raises:
        Refused: the actor's role files nothing.
    """
    filed = files_of(config_of(campaign), actor)
    if filed is Files.NOTHING:
        raise Refused(
            f"{actor} files no ideas or seeds: say the hunch in your run's finding, with pointers."
            ' Research reads the findings of the implementers.'
        )
    return filed


def add_idea(
    s: Session,
    campaign_id: int,
    title: str,
    *,
    actor: Actor,
    hypothesis: str = '',
    plan: str | None = None,
    provenance: str | None = None,
    family: str | None = None,
    priority: int = 2,
    harness: str | None = None,
    base: int | None = None,
    proposed: bool = False,
    track: str | None = None,
    after: Sequence[int] = (),
    supersedes: Sequence[int] = (),
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
    _create(s, idea, actor)
    _link_all(s, idea, after, supersedes)
    return idea, held


def file_seed(
    s: Session,
    campaign_id: int,
    hunch: str,
    *,
    actor: Actor,
    provenance: str,
    base: int | None = None,
    track: str | None = None,
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
    _create(s, idea, actor)
    return idea


def develop_seed(
    s: Session,
    idea_id: int,
    definition: Definition,
    *,
    actor: Actor,
    title: str | None = None,
    family: str | None = None,
    base: int | None = None,
    priority: int | None = None,
    proposed: bool = False,
    after: Sequence[int] = (),
    supersedes: Sequence[int] = (),
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
    candidate_id: int,
    reason: str,
    *,
    actor: Actor,
    follow_ups: Sequence[str] = (),
    track: str | None = None,
) -> tuple[Candidate, list[Idea]]:
    """Keep a regressed result in view as a lead (``ledger.note``), with a seed pinned to it per follow-up.

    The follow-ups are on ``track``, else on the track of the candidate's run. Returns the candidate and the new seeds.

    Raises:
        Refused: the candidate is missing or not a measured discard or interesting, the reason is blank, the actor
            files nothing and gave follow-ups, or the track is not the campaign's.
    """
    candidate = _get(s, Candidate, candidate_id, 'candidate')
    campaign = s.get_one(Campaign, candidate.campaign_id)
    if follow_ups:
        files(campaign, actor)
    try:
        note(candidate, reason, actor)
    except NoteRefused as e:
        raise Refused(str(e)) from e
    on = track_for(campaign, track) if track is not None else track_of(config_of(campaign), candidate).name
    return candidate, follow_up(s, candidate, follow_ups, reason, actor, track=on)


def _create(s: Session, idea: Idea, actor: Actor) -> None:
    s.add(idea)
    s.flush()
    emit(s, 'idea.created', TargetKind.IDEA, idea.id, actor=actor)


def _link_all(s: Session, idea: Idea, after: Sequence[int], supersedes: Sequence[int]) -> None:
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


def set_idea_status(s: Session, idea_id: int, status: IdeaStatus, *, reason: str, actor: Actor) -> IdeaStatus:
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


def link(s: Session, idea_id: int, kind: LinkKind, target_id: int, *, reason: str, actor: Actor) -> None:
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


def _link(s: Session, idea: Idea, kind: LinkKind, target_id: int) -> None:
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


def _reaches(s: Session, start: int, goal: int, kind: LinkKind) -> bool:
    """Return whether ``kind`` links lead from idea ``start`` to idea ``goal``."""
    seen, todo = {start}, [start]
    while todo:
        step = select(IdeaLink.target_id).where(IdeaLink.idea_id.in_(todo), IdeaLink.kind == kind)
        todo = [t for t in s.scalars(step) if t not in seen]
        if goal in todo:
            return True
        seen.update(todo)
    return False


def unlink(s: Session, idea_id: int, kind: LinkKind, target_id: int, *, reason: str, actor: Actor) -> None:
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


def set_base(s: Session, idea_id: int, candidate_id: int | None, *, reason: str, actor: Actor) -> int | None:
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


def _base(candidate_id: int | None) -> str:
    return 'the head' if candidate_id is None else f'candidate#{candidate_id}'


def check_base(s: Session, campaign: Campaign, candidate_id: int | None) -> None:
    """Refuse a base that is not unset or a candidate of ``campaign``.

    Raises:
        Refused: no such candidate in the campaign.
    """
    if candidate_id is None:
        return
    if s.scalar(select(Candidate.campaign_id).where(Candidate.id == candidate_id)) != campaign.id:
        raise Refused(f'no candidate#{candidate_id} in campaign {campaign.name}')


def set_track(s: Session, idea_id: int, track: str, *, reason: str, actor: Actor) -> str:
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


def track_for(campaign: Campaign, track: str | None) -> str:
    """Return the track for an idea: ``track`` when ``piloop.toml`` has it, else the only track.

    Raises:
        Refused: no track is given and the campaign has several, or the campaign has no such track.
    """
    tracks = [t.name for t in config_of(campaign).all_tracks]
    if track is None and len(tracks) == 1:
        return tracks[0]
    if track not in tracks:
        given = 'name one with --track' if track is None else f'there is no track {track!r}'
        raise Refused(f'{campaign.name} has tracks {", ".join(tracks)}: {given}')
    return track


def _note(s: Session, idea: Idea, body: str, actor: Actor) -> None:
    s.add(
        Comment(
            target_kind=TargetKind.IDEA, target_id=idea.id, author=actor.signature, kind=CommentKind.NOTE, body=body
        )
    )
