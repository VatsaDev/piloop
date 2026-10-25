"""The facts of one campaign that its views share, computed from the database.

The sections (``piloop.plugins.section``), ``piloop watch`` (``piloop/watch/panels.py``), the API's status and
``piloop wake`` read them here, so they always say the same thing: the summary line, the head's scores, the queue, the
live agents and their output, the inbox threads, the progress against the stop rule, and the anomalies.

``anomalies`` also drives the ``anomaly`` trigger of the watchdog. An incident anomaly that is not yet assigned to a
watchdog requests an incident sweep. An ``anomaly.reported`` event on the row of that watchdog records the assignment
(see ``reported`` and ``assigned``). So each anomaly is investigated once, and a daemon restart does not repeat it.
"""

import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import assert_never

from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session

from piloop.agents.results import StartupAnomaly
from piloop.agents.roles import harness_of
from piloop.campaign import inbox
from piloop.campaign.anomaly import AnomalyKind, AnomalyReported
from piloop.campaign.ideas import runnable
from piloop.campaign.inbox import campaign_of
from piloop.campaign.stop import held, hours, kept, limits, runs, runs_without_keep
from piloop.config import TRANSCRIPT, CampaignConfig, Metric, Track
from piloop.db.events import in_order
from piloop.db.models import (
    LIVE,
    OPEN_RUNS,
    AgentSession,
    AgentSessionState,
    Campaign,
    CampaignStatus,
    Candidate,
    Comment,
    Event,
    Idea,
    IdeaStatus,
    Origin,
    Pause,
    Run,
    RunEnd,
    campaign_runs,
    count,
)
from piloop.ids import AgentSessionId, CampaignId, EvaluatorId, TargetKind, TargetRef
from piloop.plugins.metric import Direction
from piloop.plugins.role.contract import Files
from piloop.plugins.stop_rule.contract import Fact
from piloop.runtime.factory import handle_for, runtime_for
from piloop.scoring.ledger import (
    baseline,
    head,
    keeps,
    metrics,
    references,
    score,
    track_of,
)
from piloop.text import NONE, age, cell, metric
from piloop.usage import human
from piloop.usage.totals import campaign_tokens

DETAIL_CHARS = 160
LINKED_CHARS = 800
"""How much of each linked comment ``piloop inbox show`` prints under an item."""


INCIDENTS = frozenset(
    {
        AnomalyKind.RUN_FAILED,
        AnomalyKind.DEAD_AT_STARTUP,
    }
)
"""Kinds that request an incident sweep. The progress review weighs the other kinds, which are costs, not failures.

An anomaly of an agent that investigates incidents is not an incident (``Anomaly.investigator``).
"""


@dataclass(frozen=True)
class Anomaly:
    """One problem that the daemon found, with the time it began and the evidence in one line."""

    kind: AnomalyKind
    ref: TargetRef
    """The row that the anomaly is about: ``run#36``, ``agent_session#12``, ``measurement#117``, ``campaign#3``."""
    at: datetime
    """When the anomaly began, for example the end of a failed run or the exit of an agent."""
    detail: str
    investigator: bool = False
    """The anomaly is about an agent of a role that investigates incidents (``Trigger.INVESTIGATES``). It is no
    incident: the agent that it would request could fail the same way, and so on. The startup breaker of the scheduler
    escalates that fault instead."""

    @property
    def key(self) -> tuple[TargetRef, AnomalyKind]:
        """Identify the anomaly across passes: the same row with the same kind is one anomaly."""
        return self.ref, self.kind

    @property
    def name(self) -> str:
        """The anomaly in a few words: ``run#36 failed``."""
        return f'{self.ref} {self.kind}'

    @property
    def incident(self) -> bool:
        return self.kind in INCIDENTS and not self.investigator

    def line(self, now: datetime) -> str:
        tag = '[incident] ' if self.incident else ''
        return f'- {tag}{self.name}, {age(self.at, now)} ago: {self.detail}'

    def reported(self, now: datetime) -> AnomalyReported:
        """The event that gives the anomaly to an agent, on the agent's row."""
        return AnomalyReported(subject=self.ref, anomaly=self.kind, line=self.line(now))


def summary(s: Session, campaign: Campaign, cfg: CampaignConfig) -> str:
    """Return the one-line summary of the campaign.

    The format is ``<name> · head <metric> <score> (candidate#<id>, <delta> vs baseline) · <kept> kept / <total>
    candidates``. With several tracks, the head shows the primary of each track:
    ``head candidate#9: accuracy 0.9100 (+0.0200), seconds 3.1000 (-0.4000)``. Imported references are not counted
    as candidates. ``· 3 starting points`` follows when there are any. ``· 1.2M tok`` follows when the usage of an
    agent is known: it is the total for all roles so far.
    """
    at = head_scores(s, campaign, cfg)
    total = count(s, produced(campaign))
    kept = count(s, keeps(campaign))
    imported = len(references(s, campaign))
    if at is None:
        head_text = 'no trunk keep'
    elif not cfg.multitrack:
        trunk, [(track, value, delta)] = at
        vs_baseline = f', {metric(delta, signed=True)} vs baseline' if delta is not None else ''
        head_text = f'head {track.primary.name} {_four(value)} (candidate#{trunk.id}{vs_baseline})'
    else:
        trunk, values = at
        head_text = f'head candidate#{trunk.id}: ' + ', '.join(
            f'{track.primary.name} {_four(value)}' + (f' ({metric(delta, signed=True)})' if delta is not None else '')
            for track, value, delta in values
        )
    spent = campaign_tokens(s, campaign.id)
    tokens = f' · {human(spent)} tok' if spent else ''
    starts = f' · {imported} starting point{"s" if imported != 1 else ""}' if imported else ''
    return f'{campaign.name} · {head_text} · {kept} kept / {total} candidates{starts}{tokens}'


def head_scores(
    s: Session, campaign: Campaign, cfg: CampaignConfig
) -> tuple[Candidate, list[tuple[Track, float | None, float | None]]] | None:
    """Return the head and, for each track, its primary score and its delta against the baseline.

    A score is None when the current evaluator has not measured it. A delta is also None when the head is the
    baseline. Returns None when there is no trunk keep.
    """
    trunk = head(s, campaign)
    if trunk is None:
        return None
    evaluator_id = campaign.evaluator_id
    first = baseline(s, campaign)
    values: list[tuple[Track, float | None, float | None]] = []
    for track in cfg.all_tracks:
        value = score(trunk, evaluator_id, track.primary)
        base = score(first, evaluator_id, track.primary) if first.id != trunk.id else None
        values.append((track, value, None if value is None or base is None else value - base))
    return trunk, values


def stopped(campaign: Campaign) -> str:
    """Return `` · stopped: <reason>`` if the campaign stopped, then `` · paused (<what>): <reason>`` if it is paused."""
    stop = f' · stopped: {campaign.stopped_reason}' if campaign.status is CampaignStatus.STOPPED else ''
    return stop + (f' · {paused(campaign)}' if campaign.paused is not None else '')


def paused(campaign: Campaign) -> str:
    """Return ``paused (<what it holds>): <reason>``. The campaign must be paused."""
    return f'paused ({holds(campaign)}): {campaign.paused_reason}'


def holds(campaign: Campaign) -> str:
    """Return what the pause of a paused campaign holds: ``launches``, or ``launches and evaluations``."""
    return 'launches and evaluations' if campaign.paused is Pause.ALL else 'launches'


def progress(s: Session, campaign: Campaign, cfg: CampaignConfig, now: datetime) -> str:
    """Return the runs and hours so far, then each limit of the stop rule against its fact: ``run 7/200 · 2.1h/12h``.

    A limit on the spend shows it: ``$412.30/$1,000.00 spend``. When the stop rule holds launches, the line ends with
    why: `` (launches held: spend limit reached)``.
    """
    limited = limits(cfg)
    shown = [Fact.RUNS, Fact.HOURS, *(fact for fact in limited if fact not in (Fact.RUNS, Fact.HOURS))]
    parts = [_against(s, campaign, cfg, now, fact, limited.get(fact)) for fact in shown]
    hold = held(s, campaign, cfg)
    return ' · '.join(parts) + (f' (launches held: {hold})' if hold is not None else '')


def _against(
    s: Session, campaign: Campaign, cfg: CampaignConfig, now: datetime, fact: Fact, limit: float | None
) -> str:
    """One fact of the campaign, against its limit when there is one."""
    of = '' if limit is None else f'/{limit:g}'
    match fact:
        case Fact.RUNS:
            return f'run {runs(s, campaign)}{of}'
        case Fact.HOURS:
            return f'{hours(s, campaign, now) or 0:.1f}h{of}{"" if limit is None else "h"}'
        case Fact.TOKENS:
            return f'{human(campaign_tokens(s, campaign.id))}{"" if limit is None else f"/{human(round(limit))}"} tok'
        case Fact.WITHOUT_KEEP:
            return f'{runs_without_keep(s, campaign)}{of} runs without a keep'
        case Fact.KEEPS:
            return f'{kept(s, campaign)}{of} keeps'
        case Fact.HEAD:  # A limit on the head is only allowed with one track.
            at = head_scores(s, campaign, cfg)
            value = at[1][0][1] if at is not None else None
            sign = '≥' if cfg.all_tracks[0].primary.direction is Direction.HIGHER else '≤'
            return f'head {num(value)}' if limit is None else f'target {sign} {metric(limit)} (head {num(value)})'
        case Fact.SPEND:
            usd = '$?' if campaign.spend_usd is None else f'${campaign.spend_usd:,.2f}'
            return f'{usd}{"" if limit is None else f"/${limit:,.2f}"} spend'
        case _:
            assert_never(fact)


def produced(campaign: Campaign) -> Select[Candidate]:
    """Select the candidates of the campaign: the baseline and the commits of its runs, not imported references."""
    return select(Candidate).where(Candidate.campaign_id == campaign.id, Candidate.origin != Origin.REFERENCE)


def live_agent_sessions(s: Session, campaign: Campaign) -> Sequence[AgentSession]:
    """Return the agents of the campaign that are starting or running, oldest first."""
    return s.scalars(
        select(AgentSession)
        .where(AgentSession.campaign_id == campaign.id, AgentSession.state.in_(LIVE))
        .order_by(AgentSession.id)
    ).all()


def on(s: Session, w: AgentSession) -> str:
    """Describe what the agent works on: the idea of its run, or else the reason for its request."""
    if w.run_id is not None:
        idea = s.get_one(Run, w.run_id).idea
        return f'idea#{idea.id} {cell(idea.title)}'
    return cell(w.reason or NONE)


def agent_output(s: Session, w: AgentSession, lines: int) -> list[str]:
    """Return the last ``lines`` non-empty lines of output of the agent.

    The first choice is the text in the session file of the harness, found in the same way as its usage
    (``UsageSpec``). If the harness has no usage spec, or the agent has written no text yet, the output that the
    runtime reads is used. The last choice is the transcript saved when the agent ended.
    """
    definition = harness_of(s, w)
    said = definition.usage.resolved.said(w.session_id, lines) if definition and definition.usage else []
    return said or _filled(_live_output(w, lines))[-lines:] or _filled(_transcript(w.log_dir))[-lines:]


def _live_output(w: AgentSession, lines: int) -> str:
    try:
        return runtime_for(w).read(handle_for(w), lines=lines)
    except Exception:  # Not launched yet, runtime missing or agent gone: the transcript is the fallback.
        return ''


def _transcript(log_dir: str | None) -> str:
    path = Path(log_dir) / TRANSCRIPT if log_dir else None
    return path.read_text(errors='replace') if path is not None and path.is_file() else ''


def _filled(text: str) -> list[str]:
    """Return the lines of ``text`` that are not blank, without trailing blanks."""
    return [line.rstrip() for line in text.splitlines() if line.strip()]


def queued_ideas(s: Session, campaign: Campaign) -> tuple[Sequence[Idea], Sequence[Idea]]:
    """Return every ``ready`` idea in scheduling order, and the ones not blocked by a prerequisite."""
    mine = select(Idea).where(Idea.campaign_id == campaign.id).order_by(Idea.priority, Idea.id)
    waiting = s.scalars(mine.where(Idea.status == IdeaStatus.READY)).all()
    ready = s.scalars(mine.where(runnable())).all()
    return waiting, ready


def scored(c: Candidate, evaluator_id: EvaluatorId | None, primary: Metric) -> tuple[str, str]:
    """Format the primary score of the candidate and its delta to its parent, each ``—`` when unknown."""
    value = score(c, evaluator_id, primary)
    parent = score(c.parent, evaluator_id, primary) if c.parent is not None else None
    delta = metric(value - parent, signed=True) if value is not None and parent is not None else NONE
    return NONE if value is None else metric(value), delta


def block(text: str, chars: int = LINKED_CHARS) -> str:
    """Return whole lines of ``text`` up to ``chars`` characters, then ``… (N more lines)`` for the rest.

    If the first line is longer than ``chars``, it is cut short, so that a one-line comment always shows.
    """
    lines = text.strip().splitlines() or [NONE]
    kept: list[str] = []
    used = 0
    for line in lines:
        if used + len(line) > chars:
            break
        kept.append(line)
        used += len(line) + 1
    if not kept:
        kept = [lines[0][: chars - 1] + '…']
    omitted = len(lines) - len(kept)
    return '\n'.join([*kept, f'… ({omitted} more lines)'] if omitted else kept)


def item_lines(s: Session, c: Comment, now: datetime) -> tuple[str, list[str]]:
    """Format one comment and its thread as ``piloop inbox show`` prints them: a title line and the lines below.

    The body is shown in full. The linked comments follow (the comments it cites as comment#<id>, the comments that
    cite it, and its replies), each cut by ``block``.
    """
    if c.priority is None:
        state = 'not in the inbox'
    elif c.acked_at is None:
        state = f'p{c.priority}, unacknowledged'
    else:
        state = f'p{c.priority}, acknowledged by {c.acked_by} {age(c.acked_at, now)} ago'
    title = f'comment#{c.id} {c.kind} on {c.target} by {c.author}, {age(c.created_at, now)} ago · {state}'
    lines = c.body.strip().splitlines()
    rest = inbox.thread(s, c)
    if rest:
        lines += ['', 'linked: the comments it cites, those citing it, its replies']
    for other in rest:
        head = f'- comment#{other.id} {other.kind} by {other.author}, {age(other.created_at, now)} ago: '
        lines += (head + block(other.body).replace('\n', '\n  ')).splitlines()
    return title, lines


def anomalies(s: Session, campaign: Campaign, cfg: CampaignConfig) -> list[Anomaly]:
    """Return every anomaly of ``campaign``, newest first."""
    found = [
        *_failed_runs(s, campaign),
        *_dead_at_startup(s, campaign, cfg),
        *_same_as_parent(s, campaign, cfg),
        *_idle_queue(s, campaign, cfg),
    ]
    return sorted(found, key=lambda a: a.at, reverse=True)


def reported(s: Session, campaign_id: CampaignId) -> set[tuple[TargetRef, AnomalyKind]]:
    """Return the keys of the anomalies that are already assigned to a watchdog of the campaign."""
    rows = in_order(
        s,
        AnomalyReported,
        Event.target_kind == TargetKind.AGENT_SESSION,
        Event.target_id.in_(select(AgentSession.id).where(AgentSession.campaign_id == campaign_id)),
    )
    return {(a.subject, a.anomaly) for _, a in rows}


def assigned(s: Session, agent_session_id: AgentSessionId) -> list[str]:
    """Return the anomaly lines assigned to a watchdog, in order. A progress sweep has none."""
    rows = in_order(s, AnomalyReported, Event.target == TargetRef(TargetKind.AGENT_SESSION, agent_session_id))
    return [a.line for _, a in rows]


def known(c: Candidate) -> datetime:
    """Return the time of the first successful measurement of the candidate, else its creation time."""
    return min((m.created_at for m in c.measurements if m.error is None), default=c.created_at)


def latest_events(s: Session, campaign: Campaign, limit: int) -> Sequence[Event]:
    """Return the last ``limit`` events of the campaign, newest first."""
    mine = select(Event).where(campaign_of(Event.target_kind, Event.target_id) == campaign.id)
    return s.scalars(mine.order_by(Event.id.desc()).limit(limit)).all()


def _failed_runs(s: Session, campaign: Campaign) -> list[Anomaly]:
    return [
        Anomaly(
            AnomalyKind.RUN_FAILED,
            TargetRef(TargetKind.RUN, run.id),
            run.finished_at or run.created_at,
            f'{run.end} on {run.harness} (idea#{run.idea_id}): {cell(run.error or "no error recorded", DETAIL_CHARS)}',
        )
        for run in s.scalars(campaign_runs(campaign.id).where(Run.end.in_((RunEnd.FAILED, RunEnd.TIMED_OUT))))
    ]


def _dead_at_startup(s: Session, campaign: Campaign, cfg: CampaignConfig) -> list[Anomaly]:
    """Find agents of any role that did not start to work (``StartupAnomaly``)."""
    investigators = {name for name, role in cfg.roles.sweeping().items() if any(t.INVESTIGATES for t in role.triggers)}
    events = in_order(
        s,
        StartupAnomaly,
        Event.target_kind == TargetKind.AGENT_SESSION,
        Event.target_id.in_(select(AgentSession.id).where(AgentSession.campaign_id == campaign.id)),
    )
    found: list[Anomaly] = []
    for e, p in events:
        error = cell(p.error or 'no error recorded', DETAIL_CHARS)
        detail = f'{p.role} on {p.harness} failed to start ({p.seconds}s): {error}'
        found.append(Anomaly(AnomalyKind.DEAD_AT_STARTUP, e.target, e.at, detail, p.role in investigators))
    return found


def _same_as_parent(s: Session, campaign: Campaign, cfg: CampaignConfig) -> list[Anomaly]:
    """Find candidates whose primary score is exactly the score of their parent.

    If the other metrics are also equal (except a timing metric), the change did not affect what the evaluation sees.
    """
    evaluator_id = campaign.evaluator_id
    found: list[Anomaly] = []
    children = select(Candidate).where(Candidate.campaign_id == campaign.id, Candidate.parent_id.is_not(None))
    for c in s.scalars(children.order_by(Candidate.id)):
        assert c.parent is not None
        primary = track_of(cfg, c).primary.name
        mine, theirs = metrics(c, evaluator_id), metrics(c.parent, evaluator_id)
        if primary not in mine or mine[primary] != theirs.get(primary):
            continue
        rows = [m for m in c.measurements if m.evaluator_id == evaluator_id and m.error is None]
        others = [name for name in mine if name != primary]
        same = sum(mine[name] == theirs.get(name) for name in others)
        detail = (
            f'scored what candidate#{c.parent_id} did on {primary} and on {same} of {len(others)} other metrics;'
            f' {minutes(sum(m.seconds or 0 for m in rows))} of evaluation'
        )
        at = min(m.created_at for m in rows)
        found.append(Anomaly(AnomalyKind.SAME_AS_PARENT, TargetRef(TargetKind.CANDIDATE, c.id), at, detail))
    return found


def _idle_queue(s: Session, campaign: Campaign, cfg: CampaignConfig) -> list[Anomaly]:
    """Find an active campaign, not paused, with a free implementer slot and no runnable idea, since the last run moved."""
    if campaign.status is not CampaignStatus.ACTIVE or campaign.paused is not None:
        return []
    open_runs = campaign_runs(campaign.id).where(Run.status.in_(OPEN_RUNS))
    free = cfg.run_slots - count(s, open_runs)
    if (max_runs := limits(cfg).get(Fact.RUNS)) is not None:
        free = min(free, math.floor(max_runs) - runs(s, campaign))
    ready = count(s, select(Idea).where(Idea.campaign_id == campaign.id, runnable()))
    if free <= 0 or ready:
        return []
    moments = [campaign.created_at]
    for column in (Run.created_at, Run.finished_at):
        latest = s.scalar(campaign_runs(campaign.id).with_only_columns(func.max(column)))
        if latest is not None:
            moments.append(latest)
    kinds = defining(cfg)
    pending = s.scalars(
        select(AgentSession)
        .where(
            AgentSession.campaign_id == campaign.id,
            AgentSession.kind.in_(kinds),
            AgentSession.state.in_((AgentSessionState.REQUESTED, *LIVE)),
        )
        .order_by(AgentSession.id)
    ).all()
    sweeps = (
        ', '.join(f'{w.kind}#{w.id} {w.state}' for w in pending)
        or f'no {" or ".join(kinds) or "defining"} sweep pending'
    )
    detail = f'{free} free implementer slots and no runnable idea; {sweeps}'
    return [Anomaly(AnomalyKind.IDLE_QUEUE, TargetRef(TargetKind.CAMPAIGN, campaign.id), max(moments), detail)]


def defining(cfg: CampaignConfig) -> list[str]:
    """Return the sweeping roles that define ideas (``files = "ideas"``): the ones that fill the queue."""
    return [name for name, role in cfg.roles.sweeping().items() if role.files is Files.IDEAS]


def minutes(seconds: float) -> str:
    """Format a duration in minutes: ``2.5 min``."""
    return f'{seconds / 60:.1f} min'


def num(value: float | None) -> str:
    """Format a score, ``—`` when unknown."""
    return NONE if value is None else metric(value)


def _four(value: float | None) -> str:
    return 'unmeasured' if value is None else metric(value)
