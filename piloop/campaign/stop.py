"""Stop conditions: decide when a campaign's stop rule, or a request, ends the campaign.

The scheduler checks every active campaign once per pass. The reason goes into ``campaigns.stopped_reason`` and into
the trigger of the closing watchdog. A stop or a cancel that someone asks for is an event that the daemon acts on and
answers (``piloop.db.requests``).
"""

import math
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Literal, assert_never

from sqlalchemy import ColumnElement, exists, func, select
from sqlalchemy.orm import Session

from piloop.actor import Actor
from piloop.campaign.spend import review_waits
from piloop.config import CampaignConfig
from piloop.db.events import emit, has, in_order, latest
from piloop.db.models import (
    ATTEMPTS,
    FINISHED_RUNS,
    LIVE,
    OPEN_RUNS,
    AgentSession,
    AgentSessionState,
    Campaign,
    CampaignStatus,
    Candidate,
    Comment,
    CommentKind,
    Event,
    EventModel,
    Idea,
    IdeaStatus,
    Measurement,
    Run,
    campaign_runs,
    count,
    utcnow,
)
from piloop.db.requests import answer, pending
from piloop.db.triggers import DAEMON_ACTOR
from piloop.db.triggers.changelog import CampaignPausedChanged, CampaignStatusChanged
from piloop.ids import RunId, TargetKind, TargetRef
from piloop.plugins.stop_rule.contract import STOP_RULES, Fact, Go, Hold, Progress, Stop, StopCase
from piloop.scoring.ledger import head, keeps, score
from piloop.scoring.verdict import pending_clause
from piloop.usage.totals import campaign_tokens


class Ending(StrEnum):
    """How the daemon ends the agents of a stop or a cancel that someone requested."""

    WRAP_UP = 'wrap_up'
    """After the wrap-up prompt and its grace period: the agent may commit first."""
    NOW = 'now'
    """At once."""


class StopRequested(EventModel):
    """Someone asked to stop a campaign (``piloop campaign stop``).

    On its next pass, the daemon stops an active campaign as for a ``[stop]`` condition (``check``). It ends the
    campaign's implementers as ``ending`` says, also when the campaign was stopped already. An ask
    (``piloop.db.requests``).
    """

    kind: Literal['campaign.stop_requested'] = 'campaign.stop_requested'
    reason: str
    """Why, or empty."""
    ending: Ending


class CancelRequested(EventModel):
    """Someone asked to cancel a run (``piloop run cancel``) or an agent (``piloop agent cancel``).

    The daemon closes a requested row with its queued run. It ends a running agent with everything it started, as
    ``ending`` says. A starting agent waits until it runs, and a run whose agent then fails to start is not launched
    again. The run ends ``cancelled`` unless its implementer left its finding, and its commits are scored. An ask
    (``piloop.db.requests``).
    """

    kind: Literal['run.cancel_requested', 'agent_session.cancel_requested']
    ending: Ending


CANCELLABLE = (AgentSessionState.REQUESTED, *LIVE)
"""The states of an agent that a cancel acts on: requested, starting (once it runs) and running."""


def attempt() -> ColumnElement[bool]:
    """Select the runs that were an attempt at their idea: the open runs, and those whose end is an attempt."""
    return Run.end.is_(None) | Run.end.in_(ATTEMPTS)


def scored() -> ColumnElement[bool]:
    """Select the runs that have a candidate that is measured or waiting for a verdict."""
    return exists().where(
        Candidate.run_id == Run.id,
        pending_clause() | exists().where(Measurement.candidate_id == Candidate.id),
    )


def unsettled() -> ColumnElement[bool]:
    """Select the runs whose outcome can still change: the open runs, and the runs that are scored now (``scoring``)."""
    return Run.status.in_(OPEN_RUNS) | Run.scoring


def counted() -> ColumnElement[bool]:
    """Select the runs that count toward the run limits, ``max_runs`` and ``plateau``: those that reached the evaluator.

    Such a run has a candidate that is measured or waiting for its verdict (``scored``). A run that stopped with no
    version, or with only versions that asked to be skipped, does not count: it stopped at a cheap check, or a harness
    or host fault ended it before it saved one. It says nothing about the idea's effect on the metric. ``[spend]``,
    ``max_tokens`` and ``max_hours`` limit what such runs cost. This is the one rule for both limits.
    """
    return attempt() & scored()


def attempts(idea: Idea) -> int:
    """Count the runs of ``idea`` that were an attempt at it. ``ideas.max_runs`` caps this number."""
    return sum(run.end is None or run.end.attempt for run in idea.runs)


def request_stop(s: Session, campaign: Campaign, actor: Actor, *, reason: str, ending: Ending) -> None:
    """Ask the daemon to stop ``campaign`` (``StopRequested``). The caller commits."""
    emit(s, StopRequested(reason=reason, ending=ending), TargetRef(TargetKind.CAMPAIGN, campaign.id), actor)


class CampaignClosed(EventModel):
    """The daemon closed a stopped campaign: it requested the closing sweeps, the ``stopped`` triggers.

    It closes a campaign once the runs that the stop wound down ended and their commits are judged (``settled``), so
    the closing review sees the final head. ``[spend] closing_review = "skip"`` closes it at the stop, with no sweep.
    """

    kind: Literal['campaign.closed'] = 'campaign.closed'


def unclosed(s: Session, campaign: Campaign) -> bool:
    """Whether the daemon stopped ``campaign`` after it last closed it (``CampaignClosed``).

    A ``stopped`` that someone set by hand (an UPDATE of ``campaigns.status``) is not the daemon's stop: it gets no
    closing sweep.
    """
    on_campaign = Event.target == TargetRef(TargetKind.CAMPAIGN, campaign.id)
    last = select(func.max(Event.id)).where(on_campaign)
    stopped = s.scalar(last.where(Event.actor == DAEMON_ACTOR, has(CampaignStatusChanged, new=CampaignStatus.STOPPED)))
    closed = s.scalar(last.where(has(CampaignClosed)))
    return stopped is not None and stopped > (closed or 0)


def settled(s: Session, campaign: Campaign) -> bool:
    """Whether no run of ``campaign`` is open, and the commits of each one that ended are judged."""
    busy = select(Run.id).join(Idea).where(Idea.campaign_id == campaign.id, Run.status.in_(OPEN_RUNS) | Run.scoring)
    return s.scalar(busy.limit(1)) is None


def request_cancel(s: Session, target: Run | AgentSession, actor: Actor, *, ending: Ending) -> None:
    """Ask the daemon to cancel a run or an agent (``CancelRequested``). The caller commits."""
    match target:
        case Run():
            on = TargetRef(TargetKind.RUN, target.id)
            cancel = CancelRequested(kind='run.cancel_requested', ending=ending)
        case AgentSession():
            on = TargetRef(TargetKind.AGENT_SESSION, target.id)
            cancel = CancelRequested(kind='agent_session.cancel_requested', ending=ending)
        case _:
            assert_never(target)
    emit(s, cancel, on, actor)


def cancelled(ask: Event, cancel: CancelRequested) -> ColumnElement[bool]:
    """Select the agents that a cancel acts on: a run's agents, or an agent and the other agents of its run."""
    match cancel.kind:
        case 'run.cancel_requested':
            return AgentSession.run_id == ask.target_id
        case 'agent_session.cancel_requested':
            run = select(AgentSession.run_id).where(AgentSession.id == ask.target_id).scalar_subquery()
            return (AgentSession.id == ask.target_id) | (AgentSession.run_id == run)
        case _:
            assert_never(cancel.kind)


def run_cancel(s: Session, run_id: RunId) -> tuple[Event, CancelRequested] | None:
    """Return the latest cancel of a run, open or answered: requested on the run, or on any of its agents, or None.

    A cancel on an agent holds for its run, so an implementer that a relaunch puts in the place of one that failed to start
    is cancelled too.
    """
    agents = select(AgentSession.id).where(AgentSession.run_id == run_id)
    on_agent = (Event.target_kind == TargetKind.AGENT_SESSION) & Event.target_id.in_(agents)
    return latest(s, CancelRequested, (Event.target == TargetRef(TargetKind.RUN, run_id)) | on_agent)


def agent_cancel(s: Session, agent_session: AgentSession) -> tuple[Event, CancelRequested] | None:
    """Return the latest cancel of an agent, or None: its run's (``run_cancel``) for an implementer, its own for a sweep."""
    if agent_session.run_id is not None:
        return run_cancel(s, agent_session.run_id)
    return latest(s, CancelRequested, Event.target == TargetRef(TargetKind.AGENT_SESSION, agent_session.id))


ACTING = (AgentSessionState.REQUESTED, AgentSessionState.STARTING, AgentSessionState.RUNNING)
"""The states of an agent that a cancel still has to act on. An ``ending`` one is ended already."""


def answer_done(s: Session) -> None:
    """Answer each open stop and cancel that the daemon carried out. The caller commits.

    A stop is carried out once its campaign is stopped and, with ``now``, none of the campaign's implementers is starting
    or running. A cancel is carried out once none of its agents (``cancelled``) is requested, starting or running: the
    daemon closed or ended each one, or it ended on its own.
    """
    for asked, stop in pending(s, StopRequested, Event.target_id.in_(select(Campaign.id))):
        campaign = s.get_one(Campaign, asked.target_id)
        if campaign.status is not CampaignStatus.STOPPED:
            continue
        match stop.ending:
            case Ending.WRAP_UP:
                done = True
            case Ending.NOW:
                running = (AgentSessionState.STARTING, AgentSessionState.RUNNING)
                implementers = select(AgentSession).where(
                    AgentSession.campaign_id == campaign.id,
                    AgentSession.run_id.is_not(None),
                    AgentSession.state.in_(running),
                )
                done = not count(s, implementers)
            case _:
                assert_never(stop.ending)
        if done:
            answer(s, asked)
    for asked, cancel in pending(s, CancelRequested):
        if not count(s, select(AgentSession).where(cancelled(asked, cancel), AgentSession.state.in_(ACTING))):
            answer(s, asked)


def park_cancelled(s: Session, run: Run) -> None:
    """Park the idea of a cancelled run, with a note on the idea: a run is cancelled because it is not wanted now."""
    cancel = run_cancel(s, run.id)
    run.idea.status = IdeaStatus.PARKED
    by = f' by {cancel[0].actor}' if cancel is not None else ''
    body = f'parked: run#{run.id} was cancelled{by}; set the idea ready to run it again'
    s.add(
        Comment(target=TargetRef(TargetKind.IDEA, run.idea_id), author=DAEMON_ACTOR, kind=CommentKind.NOTE, body=body)
    )


def limits(cfg: CampaignConfig) -> dict[Fact, float]:
    """The limits of the campaign's stop rule, by fact (``StopRule.limits``)."""
    return STOP_RULES.selected(cfg.plugins).limits()


def check(s: Session, campaign: Campaign, cfg: CampaignConfig) -> Go | Hold | Stop:
    """Ask the campaign's stop rule whether ``campaign`` goes on, holds, or stops now.

    The rule is ``[plugins.stop_rule]``, else ``limits``. Only the facts that it reads are gathered
    (``StopRule.reads``). ``head`` is the first track's primary at the head: config validation rejects a limit on it
    for a campaign with more than one track. A stop request is not the rule's: see ``requested``.
    """
    rule = STOP_RULES.selected(cfg.plugins)
    primary = cfg.all_tracks[0].primary
    reads = set(rule.reads())
    trunk = head(s, campaign) if Fact.HEAD in reads else None
    counting = Fact.RUNS in reads
    spending = Fact.SPEND in reads
    progress = Progress(
        head=score(trunk, campaign.evaluator_id, primary) if trunk is not None else None,
        runs=runs(s, campaign) if counting else None,
        unsettled=(_over_runs(s, campaign, func.count(Run.id), attempt(), unsettled()) or 0) if counting else None,
        hours=hours(s, campaign, utcnow()) if Fact.HOURS in reads else None,
        tokens=campaign_tokens(s, campaign.id) if Fact.TOKENS in reads else None,
        without_keep=runs_without_keep(s, campaign) if Fact.WITHOUT_KEEP in reads else None,
        keeps=kept(s, campaign) if Fact.KEEPS in reads else None,
        spend=campaign.spend_usd if spending else None,
        spend_error=campaign.spend_error if spending else None,
    )
    return rule.check(StopCase(primary.name, primary.direction, progress))


def timeout(s: Session, campaign: Campaign, cfg: CampaignConfig, role_timeout: float, started_at: datetime) -> float:
    """Return the seconds that an agent of ``campaign`` that started at ``started_at`` may run.

    That is its ``role_timeout``, cut where the stop rule's limit on ``hours`` stops the active campaign
    (``StopRule.limits``): a run that starts minutes before the stop ends with it. A stopped campaign cuts nothing, so
    its closing review has its whole timeout.
    """
    limit = STOP_RULES.selected(cfg.plugins).limits().get(Fact.HOURS)
    if campaign.status is not CampaignStatus.ACTIVE or limit is None:
        return role_timeout
    now = utcnow()
    left = (limit - (hours(s, campaign, now) or 0.0)) * 3600
    return max(0.0, min(role_timeout, (now - started_at).total_seconds() + left))


def requested(s: Session, campaign: Campaign) -> Stop | None:
    """Return the stop that someone requested and the daemon has not carried out, or None.

    It wins over every answer of the rule. ``answer_done`` answers it once the campaign is stopped.
    """
    asks = pending(s, StopRequested, Event.target == TargetRef(TargetKind.CAMPAIGN, campaign.id))
    if not asks:
        return None
    asked, stop = asks[0]
    return Stop(f'stopped by {asked.actor}: {stop.reason}' if stop.reason else f'stopped by {asked.actor}')


def asked(s: Session, campaign: Campaign, cfg: CampaignConfig) -> bool:
    """Whether piloop asks the campaign's stop rule now.

    It asks about an active campaign, and about a stopped one whose closing review waits for the hold to end
    (``spend.review_waits``). The daemon reads the spend of the same campaigns.
    """
    return campaign.status is CampaignStatus.ACTIVE or review_waits(s, campaign, cfg)


def held(s: Session, campaign: Campaign, cfg: CampaignConfig) -> str | None:
    """Return why the stop rule holds the campaign's launches and evaluations now, or None.

    A campaign that piloop does not ask about (``asked``) is not held.
    """
    if not asked(s, campaign, cfg):
        return None
    answer = check(s, campaign, cfg)
    return answer.reason if isinstance(answer, Hold) else None


def kept(s: Session, campaign: Campaign) -> int:
    """Count the campaign's candidates of runs that are kept, on the trunk or on a branch. The baseline is not one."""
    return s.scalar(keeps(campaign).where(Candidate.run_id.is_not(None)).with_only_columns(func.count())) or 0


def _over_runs[T](s: Session, campaign: Campaign, aggregate: ColumnElement[T], *where: ColumnElement[bool]) -> T | None:
    return s.scalar(campaign_runs(campaign.id).with_only_columns(aggregate).where(*where))


def runs(s: Session, campaign: Campaign) -> int:
    """Count the campaign's runs, of every role, that hold a place under ``max_runs``.

    These are the runs that count (``counted``) and the unsettled attempts, which may still count. So the queue never
    starts more runs than ``max_runs``.
    """
    return _over_runs(s, campaign, func.count(Run.id), counted() | (attempt() & unsettled())) or 0


def hours_due(s: Session, campaign: Campaign, cfg: CampaignConfig, now: datetime) -> datetime | None:
    """When ``hours`` reaches the limit that the stop rule sets on it, for a campaign that runs and is not paused.

    None without such a limit, before the first run, or while time does not count. The pass then checks the rule.
    """
    limit = limits(cfg).get(Fact.HOURS)
    if limit is None or campaign.status is not CampaignStatus.ACTIVE or campaign.paused is not None:
        return None
    spent = hours(s, campaign, now)
    return None if spent is None else now + timedelta(hours=max(limit - spent, 0))


def hours(s: Session, campaign: Campaign, now: datetime) -> float | None:
    """Return the hours from the start of the campaign's first run to ``now``, or None if no run started.

    The time that the campaign was paused (``piloop pause``) does not count, so a pause does not use up ``max_hours``.
    The pauses are the ``campaign.paused`` events of the change log.
    """
    first = _over_runs(s, campaign, func.min(Run.started_at))
    return None if first is None else (now - first - paused_for(s, campaign, first, now)) / timedelta(hours=1)


def paused_for(s: Session, campaign: Campaign, since: datetime, now: datetime) -> timedelta:
    """Return how long the campaign was paused between ``since`` and ``now``."""
    changes = in_order(s, CampaignPausedChanged, Event.target == TargetRef(TargetKind.CAMPAIGN, campaign.id))
    total, start = timedelta(), None
    for at, new in ((e.at, change.new) for e, change in changes):
        if new is not None and start is None:
            start = at
        elif new is None and start is not None:
            total += max(at - max(start, since), timedelta())
            start = None
    if start is not None:
        total += max(now - max(start, since), timedelta())
    return total


def runs_without_keep(s: Session, campaign: Campaign, before: datetime | None = None) -> int:
    """Count the finished runs that count (``counted``) since the latest keep or reactivation. ``plateau`` uses this.

    With ``before``, only the runs of that window that finished by then count: 0 if the window opened later.

    The window starts when the run of the latest keep finished, or when the campaign was last set ``active`` again
    after a stop (``piloop init``, or an UPDATE), whichever is later. A reactivated campaign gets a fresh window, so
    a ``plateau`` stop does not fire again at once. An evaluator change alone does not reset the window.
    """
    last_keep = s.scalar(
        keeps(campaign).join(Run, Candidate.run_id == Run.id).with_only_columns(func.max(Run.finished_at))
    )
    reactivated = s.scalar(
        select(func.max(Event.at)).where(
            has(CampaignStatusChanged, new=CampaignStatus.ACTIVE),
            Event.target == TargetRef(TargetKind.CAMPAIGN, campaign.id),
        )
    )
    since = max((t for t in (last_keep, reactivated) if t is not None), default=None)
    after = (Run.finished_at > since,) if since is not None else ()
    by = (Run.finished_at <= before,) if before is not None else ()
    return _over_runs(s, campaign, func.count(Run.id), Run.status.in_(FINISHED_RUNS), counted(), *after, *by) or 0


def stall(cfg: CampaignConfig) -> tuple[int, str]:
    """Return the number of runs without a keep after which research must pivot, and how it was derived.

    The number is half of the stop rule's limit on runs without a keep, so half of the campaign's patience remains after
    the pivot. Without that limit, it is two rounds of the run slots (``CampaignConfig.run_slots``).
    """
    if (plateau := limits(cfg).get(Fact.WITHOUT_KEEP)) is not None:
        return math.ceil(plateau / 2), f"half of the stop rule's limit of {plateau:g} runs without a keep"
    slots = ' + '.join(f'[roles.{name}] max_concurrent' for name in cfg.roles.doing_runs())
    return 2 * cfg.run_slots, f'two rounds of {slots} = {cfg.run_slots}'
