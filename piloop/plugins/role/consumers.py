"""The role capability's machinery: it steers the agents that run, and winds them down.

It hands each running agent the comments for it (``prompts``), and tells an agent to wrap up and ends it when its
campaign stops, someone cancels it, its role timeout comes, or it left its finding (``wind_down``).
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from piloop.actor import Agent, System
from piloop.agents.results import (
    AgentEnded,
    AgentSessionRunning,
    PromptOutcome,
    WrapUp,
    queue_prompt,
)
from piloop.campaign.launches import close
from piloop.campaign.stop import (
    CancelRequested,
    StopRequested,
    answer_done,
    cancelled,
    seconds,
    timeout as agent_timeout,
)
from piloop.daemon.loop import Consumer, Kinds, Phase, Soonest, Turn
from piloop.daemon.run_controller import (
    wrap_up_lead,
)
from piloop.db.events import emit, has, in_order
from piloop.db.models import (
    AgentSession,
    AgentSessionState,
    Campaign,
    Comment,
    Event,
    RunEnd,
)
from piloop.db.requests import pending
from piloop.db.triggers import DAEMON_ACTOR
from piloop.db.triggers.changelog import (
    CampaignStatusChanged,
    Commented,
)
from piloop.ids import (
    CampaignId,
    CampaignStatus,
    Ending,
    TargetKind,
    TargetRef,
)
from piloop.plugins.campaign_type.contract import Planner
from piloop.plugins.contribution import Contribution, DaemonHost
from piloop.plugins.executor.contract import AgentKey, Prompt
from piloop.plugins.role.contract import PUSHED_KINDS
from piloop.settings import AgentSettings

WRAP_UP_PROMPT = (
    '[piloop] {what} ({cause}). Save what you have now, with a summary that says where it stands.'
    ' Leave your finding and stop. This session ends in {minutes:g} minutes.'
)


FOUND = 'left its finding'
"""The ``reason`` of the ``AgentEnded`` of an implementer that the daemon ended after its finding."""


TIMEOUT = 'role timeout'
"""The ``reason`` of the ``WrapUp`` of an implementer whose role timeout is ``wrap_up_lead`` away."""


class TimeCut(StrEnum):
    """What set an implementer's timeout, as the wrap-up prompt says it."""

    ROLE = 'the role timeout of {timeout:g}s'
    STOP = "the campaign's stop, {timeout:.0f}s after you started"


WINDING: Kinds = frozenset({Commented, StopRequested, CancelRequested, CampaignStatusChanged, AgentSessionRunning})
"""What wakes the wind-down: a finding, a stop or a cancel, and an agent that runs, to tell it."""


@dataclass(frozen=True)
class TimeLeft:
    """The time an implementer has left before its ``timeout`` ends it, in ``minutes``."""

    timeout: float
    minutes: float
    cut: TimeCut
    """What set the timeout."""


def steer_line(comment: Comment, chars: int) -> str:
    """Format what a running agent gets for a comment. A comment on the agent itself is its text, as written.

    Any other is one line: who wrote what, and how to read the rest. The line quotes the first line of the body, cut at
    ``chars`` (``[agents] steer_line_chars``). The implementer can read the full text with the SQL tool that its brief
    names, so a long decision does not fill its turn.
    """
    if comment.target_kind is TargetKind.AGENT_SESSION:
        return comment.body
    body = comment.body.strip()
    first = body.splitlines()[0] if body else ''
    if len(first) > chars:
        first = first[: chars - 1] + '…'
    return (
        f'[piloop] {comment.kind} on {comment.target} by {comment.author}: {first}'
        f' — full text: SELECT body FROM comments WHERE id={comment.id}'
    )


def contribution(host: DaemonHost) -> Contribution:
    """The consumers ``prompts`` and ``wind_down``, which act on the agents in flight."""
    agents = Agents(host)
    return Contribution(
        consumers=(
            Consumer('prompts', frozenset({Commented, AgentSessionRunning}), agents.prompts, Phase.IN_FLIGHT),
            Consumer('wind_down', WINDING, agents.wind_down, Phase.IN_FLIGHT),
        )
    )


class Agents:
    """The agents of one daemon process, as the role capability steers and ends them."""

    def __init__(self, host: DaemonHost) -> None:
        self._host = host
        self._db, self._tracer, self._controller = host.db, host.tracer, host.controller
        self._daemon, self._agents = host.daemon, host.agents
        self._prompted: set[int] = set()
        """The comments that this daemon process handed to the executor (``_push_comments``)."""

    def _planner_by_id(self, s: Session, campaign_id: CampaignId | None) -> Planner:
        return self._host.planners[s.get_one(Campaign, campaign_id).type]

    def prompts(self, turn: Turn) -> datetime | None:
        """Push the new comments (``_push_comments``). One that the executor did not take is tried again soon."""
        return turn.now + self._daemon.prompt_retry if self._push_comments() else None

    def _push_comments(self) -> list[int]:
        """Hand each running agent's executor the comments that the agent has no outcome for (``Executor.prompt``).

        The events table records each outcome: ``PromptOutcome`` events on the agent, with the comment id. A
        comment is handed over once per daemon process (``_prompted``). One without an outcome after a restart goes
        again, and the executor answers it as it did the first time. An agent whose row is still ``starting`` (brief
        composed, process not yet up) gets the comments once it runs. A failed delivery is recorded and not retried: the
        comment is in the brief of the idea's next run anyway, and the agent would fail the same way each time.
        A push that sends something makes one span. Returns the comments that the executor did not take, to try again.
        """
        with self._db.session() as s:
            running = s.scalars(
                select(AgentSession).where(AgentSession.state == AgentSessionState.RUNNING).order_by(AgentSession.id)
            ).all()
            pending = [
                (w.id, Prompt(c.id, steer_line(c, self._agents.steer_line_chars)))
                for w in running
                for c in self._unseen(s, w)
                if c.id not in self._prompted
            ]
        refused: list[int] = []
        if not pending:
            return refused
        with self._tracer.span('piloop.push', comments=len(pending)):
            for agent_session_id, prompt in pending:
                self._prompted.add(prompt.id)
                try:
                    self._controller.executor.prompt(AgentKey(agent_session_id), prompt)
                except Exception as e:  # the executor did not take it (a locked database): it is tried again
                    self._prompted.discard(prompt.id)
                    refused.append(prompt.id)
                    self._tracer.event(
                        'comment.unsent', comment=prompt.id, agent_session=agent_session_id, error=str(e)
                    )
        return refused

    def _unseen(self, s: Session, agent_session: AgentSession) -> list[Comment]:
        """Return the comments for the agent that have no outcome yet: on the agent, or on an implementer's run or idea.

        A comment on the agent reaches it once: a sweep's brief carries those written before it started, and records
        them delivered. On a run or an idea, only the comments since the run started: the brief has the older ones,
        because the transaction that composes the brief sets ``Run.started_at``. The comparison uses whole seconds. A
        raw INSERT sets ``created_at`` to ``CURRENT_TIMESTAMP``, which has second precision, but ``started_at`` is a
        Python timestamp. So a comment written in the same second as the brief can reach the implementer twice. That is
        better than never. The agent's own comments are not sent back. An implementer signs as ``implementer#<id>`` (its
        ``PILOOP_ACTOR``) or ``run#<id>``.
        """
        pushed = (Comment.target_kind == TargetKind.AGENT_SESSION) & (Comment.target_id == agent_session.id)
        own: tuple[str, ...] = (Agent(role=agent_session.kind, session=agent_session.id).signature,)
        steering = (
            self._planner_by_id(s, agent_session.campaign_id).steered_by(s, agent_session)
            if agent_session.campaign_id is not None
            else None
        )
        if steering is not None:
            on_its = or_(*((Comment.target_kind == t.kind) & (Comment.target_id == t.id) for t in steering.targets))
            since = steering.since.strftime('%Y-%m-%d %H:%M:%S')
            pushed = pushed | (on_its & (func.strftime('%Y-%m-%d %H:%M:%S', Comment.created_at) >= since))
            own = (*own, *steering.signatures)
        on_agent_session = Event.target == TargetRef(TargetKind.AGENT_SESSION, agent_session.id)
        settled = {p.comment for _, p in in_order(s, PromptOutcome, on_agent_session)}
        comments = s.scalars(select(Comment).where(pushed, Comment.kind.in_(PUSHED_KINDS)).order_by(Comment.id)).all()
        return [c for c in comments if c.id not in settled and c.author not in own]

    def wind_down(self, turn: Turn) -> datetime | None:
        """End the implementers that left their finding, and wind down those of stopped campaigns and cancelled agents.

        An implementer is ended ``finding_grace`` (``[agents]``) after its finding, with every process that it left running: it
        reported that it is done, and a job that it forgot must not hold its slot to the role timeout. Its run is
        finished (``run_outcome``).

        A wrap-up prompt (``WrapUp``) is a note on the agent, which its executor delivers. An implementer whose role
        timeout is ``wrap_up_lead`` away is told to save its work and stop. Its timeout ends it.

        An implementer of a stopped campaign is told to save its work and stop, and ended after the grace period
        (``wrap_up_grace``), or at once when the stop request asked to end it ``now``. The runs then close and their
        commits are scored. An implementer that the daemon ends (``AgentEnded``) without its finding closes its run as
        cut by the stop (``RunEnd.CAMPAIGN_STOPPED``), not as a failed attempt. Sweeps of a stopped campaign are not
        changed, because their timeouts limit them and the closing review is one of them.

        A cancelled run or agent of any role (a pending ``CancelRequested``) is ended at once, or after the wrap-up prompt
        and the grace period when the cancel asked for it. Its run ends ``RunEnd.CANCELLED``. A cancelled request never
        starts, and its queued run fails. Both park the run's idea. A starting agent is left for a later pass, once it
        runs; if its start fails, the run is not launched again (``RunController._relaunch``). An ``ending`` agent gets
        its kill again every ``[daemon] kill_retry`` until it ends: a kill can fail. Then the stops and cancels that are carried
        out are answered (``stop.answer_done``).

        Returns when a grace period or a wrap-up lead ends, or when to send an ending agent's kill again.
        """
        now, soonest = turn.now, Soonest()
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
            graces = {w.id: self._finding_grace(s, w) for w in s.scalars(implementers)}
            found = [i for i, at in graces.items() if at is not None and at <= now]
            soonest.add(*graces.values())
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
                    soonest.add(now + self._daemon.kill_retry)
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
                    at_once = [stop for _, stop in pending(s, StopRequested, on_campaign) if stop.ending is Ending.NOW]
                    ending = Ending.NOW if at_once else Ending.WRAP_UP
                if agent_session.state is AgentSessionState.REQUESTED:
                    close(s, agent_session, reason, self._tracer)
                    if agent_session.run_id is not None:
                        self._planner_by_id(s, agent_session.campaign_id).ended(s, agent_session, end)
                    continue
                told = s.scalar(
                    select(Event.at).where(
                        Event.target == TargetRef(TargetKind.AGENT_SESSION, agent_session.id), has(WrapUp)
                    )
                )
                grace = self._timings(s, agent_session).wrap_up_grace
                if ending is Ending.NOW:
                    self._end(s, agent_session, reason, end)
                elif told is None:
                    text = WRAP_UP_PROMPT.format(what=what, cause=cause, minutes=grace.total_seconds() / 60)
                    self._wrap_up(s, agent_session, reason, text)
                    soonest.add(now + grace)
                elif now - told >= grace:
                    self._end(s, agent_session, reason, end)
                else:
                    soonest.add(told + grace)
            for agent_session in s.scalars(implementers.where(AgentSession.state == AgentSessionState.RUNNING)):
                left = self._time_left(s, agent_session, now)
                if isinstance(left, TimeLeft):
                    cause = left.cut.value.format(timeout=left.timeout)
                    text = WRAP_UP_PROMPT.format(what='Your time is almost up', cause=cause, minutes=left.minutes)
                    self._wrap_up(s, agent_session, TIMEOUT, text)
                else:  # when its lead comes, or None once it was told
                    soonest.add(left)
            answer_done(s)
            s.commit()
        return soonest.at

    def _timings(self, s: Session, agent_session: AgentSession) -> AgentSettings:
        """The home's ``[agents]``, with what the agent's campaign sets in its own."""
        if agent_session.campaign_id is None:
            return self._agents
        return s.get_one(Campaign, agent_session.campaign_id).config.agents.over(self._agents)

    def _finding_grace(self, s: Session, implementer: AgentSession) -> datetime | None:
        """When the grace after the implementer's finding ends (``finding_grace``), or None without one."""
        found = self._planner_by_id(s, implementer.campaign_id).found_at(s, implementer)
        return None if found is None else found + self._timings(s, implementer).finding_grace

    def _time_left(self, s: Session, implementer: AgentSession, now: datetime) -> TimeLeft | datetime | None:
        """What an implementer that was not told to wrap up has left, once it is ``wrap_up_lead`` or less.

        Its timeout is its role's, cut at the campaign's stop (``stop.timeout``), as when it started. Before the lead,
        when it comes. None when it was told, or has no timeout to wrap up before.
        """
        campaign = s.get_one(Campaign, implementer.campaign_id)
        cfg = campaign.config
        role = cfg.roles.doing_runs().get(implementer.kind)
        told = select(Event.id).where(Event.target == TargetRef(TargetKind.AGENT_SESSION, implementer.id), has(WrapUp))
        if role is None or implementer.started_at is None or s.scalar(told.limit(1)) is not None:
            return None
        timeout = seconds(agent_timeout(s, campaign, cfg, role.timeout, implementer.started_at))
        left = timeout - (now - implementer.started_at).total_seconds()
        lead = wrap_up_lead(timeout, self._agents.wrap_up_lead)
        if left > lead:
            return now + timedelta(seconds=left - lead)
        cut = TimeCut.ROLE if timeout == role.timeout else TimeCut.STOP
        return TimeLeft(timeout, round(left / 60, 1), cut)

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
        """Commit, then send the kill of an ``ending`` agent to its executor. Sent on each wind-down until it ends."""
        # before the kill: the run that the kill ends reads it when it closes, and the executor writes in a session of
        # its own, which waits for this one's write lock
        s.commit()
        self._controller.executor.cancel(AgentKey(agent_session.id))
