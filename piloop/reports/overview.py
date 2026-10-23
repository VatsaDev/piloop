"""The campaign overview: the state of one campaign, computed from the database.

``piloop status`` prints it, the daemon puts it in the brief of each sweep, and ``piloop watch`` draws its parts
(``piloop/watch/panels.py``). So the three always say the same thing, and a sweep does not have to query the database
for basic facts. It shows the goal and the progress toward the stop conditions, the tracks, the trajectory of the
trunk, the runs, the queue and the live agents, the recent candidates, the spend, the anomalies, the inbox, the latest
decisions and the recent events. For an agent, it adds a summary of the tools and the schema.

``anomalies`` also drives the ``anomaly`` trigger of the watchdog. An incident anomaly that is not yet assigned to a
watchdog requests an incident sweep. An ``anomaly.reported`` event on the row of that watchdog records the assignment
(see ``reported`` and ``assigned``). So each anomaly is investigated once, and a daemon restart does not repeat it.
"""

import re
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from statistics import median

from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session

from piloop import deprecated, git
from piloop.actor import Agent
from piloop.agents.results import StartupAnomaly
from piloop.campaign import inbox
from piloop.campaign.anomaly import AnomalyKind, AnomalyReported
from piloop.campaign.ideas import defined_by, links, runnable, superseded_by, yields
from piloop.campaign.inbox import campaign_of
from piloop.campaign.stop import attempt, held, hours, runs, runs_without_keep
from piloop.config import TRANSCRIPT, CampaignConfig, Metric, Track, harness_of
from piloop.db.events import in_order
from piloop.db.models import (
    ENDED,
    FINISHED_RUNS,
    LIVE,
    OPEN_RUNS,
    AgentSession,
    AgentSessionState,
    Base,
    Campaign,
    CampaignStatus,
    Candidate,
    Comment,
    CommentKind,
    Event,
    Idea,
    IdeaStatus,
    Measurement,
    Pause,
    Run,
    RunEnd,
    Verdict,
    campaign_runs,
    count,
    latest_agent_session,
)
from piloop.ids import AgentSessionId, CampaignId, EvaluatorId, TargetKind, TargetRef
from piloop.plugins.metric import Direction
from piloop.plugins.role.contract import Files
from piloop.reports.history import IDEAS_IN_HISTORY
from piloop.runtime.factory import handle_for, runtime_for, view_of
from piloop.scoring.ledger import (
    NOTE,
    baseline,
    branches,
    head,
    keeps,
    metrics,
    references,
    score,
    track_of,
    trunk_keeps,
)
from piloop.text import NONE, cell, metric
from piloop.usage import campaign_tokens, human

STEPS_SHOWN = 8
ANOMALIES_SHOWN = 12
REFS_SHOWN = 8
BLOCKED_SHOWN = 5
QUEUE_SHOWN = 8
CANDIDATES_SHOWN = 8
COMMENTS_SHOWN = 8
EVENTS_SHOWN = 5
EVALS_IN_TREND = 5
"""How many of the first and of the latest measurements the evaluation-time trend compares."""
DETAIL_CHARS = 160
LINKED_CHARS = 800
"""How much of each linked comment ``piloop inbox show`` prints under an item."""
SILENT_SECONDS = 3600
"""How long an implementer with jobs in flight can go without a comment or a commit before it is ``silent``.

One hour allows several progress checks on a long job, and is a small part of ``[roles.implementer] timeout``.
"""
LIVE_LINES = 40
"""How many extra screen lines to read when the session file of an agent has no text yet.

The extra lines get past the input box and the status bar of the harness to the output above them.
"""
CHROME = re.compile(r'\s*([─━═]{3,}|[❯›](\s|$)|.*\besc to interrupt\b|Worked for \d)')
"""A line that the harness draws, not output of the agent.

It matches the rule of an input box (Claude Code, pi), the prompt (Claude Code ``❯``, Codex ``›``), the
``esc to interrupt`` of a spinner and the ``Worked for 3m 2s`` of Codex. Everything from the last such line down is
cut, which also removes the status bar under the prompt.
"""


INCIDENTS = frozenset(
    {
        AnomalyKind.RUN_FAILED,
        AnomalyKind.DEAD_AT_STARTUP,
        AnomalyKind.SILENT,
    }
)
"""Kinds that request an incident sweep. The progress review weighs the other kinds, which are costs, not failures.

An anomaly of an agent that investigates incidents is not an incident (``Anomaly.investigator``).
"""

type JobsProbe = Callable[[AgentSession], list[str]]
"""Return the jobs that a running implementer has in flight (``Runtime.jobs``). Tests pass their own function."""


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
        """Identify the anomaly across ticks: the same row with the same kind is one anomaly."""
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


def overview(
    s: Session, campaign: Campaign, cfg: CampaignConfig, now: datetime, jobs: JobsProbe, *, tools: bool = True
) -> str:
    """Render the overview as markdown, as of ``now``.

    ``tools`` adds the summary of tools and schema that an agent needs and a human does not.
    """
    sections = [
        f'## Campaign overview (computed at {now:%Y-%m-%d %H:%M} UTC)',
        headline(s, campaign, cfg, now),
        '### Goal',
        *_goal(s, campaign, cfg, now),
        *(['### Tracks', *tracks_lines(s, campaign, cfg, now)] if cfg.multitrack else []),
        '### Trajectory',
        *_trajectory(s, campaign, cfg, now),
        '### Now',
        *_now(s, campaign, cfg, now),
        *_candidates(s, campaign, cfg, now),
        '### Anomalies the daemon detected',
        *_anomalies(anomalies(s, campaign, cfg, now, jobs), now),
        *_comments(s, campaign, now),
        '### Recent events',
        _events(s, campaign, now),
    ]
    if tools:
        sections += ['### Tools', *TOOLS, *_schema()]
    return '\n'.join(sections) + '\n'


def age(then: datetime, now: datetime) -> str:
    """Format an age as ``12s``, ``3m``, ``2h`` or ``1d``, in the largest whole unit, rounded down.

    A clock skew gives ``0s``, not a negative age.
    """
    seconds = max(0, int((now - then).total_seconds()))
    for unit, size in (('d', 86400), ('h', 3600), ('m', 60)):
        if seconds >= size:
            return f'{seconds // size}{unit}'
    return f'{seconds}s'


def headline(s: Session, campaign: Campaign, cfg: CampaignConfig, now: datetime) -> str:
    """Return ``summary``, then the age of the evaluator and the stop reason."""
    evaluator = campaign.evaluator
    evaluator_age = f' · evaluator #{evaluator.id}, {age(evaluator.created_at, now)} old' if evaluator else ''
    return f'{summary(s, campaign, cfg)}{evaluator_age}{stopped(campaign)}'


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
    holds = 'launches and evaluations' if campaign.paused is Pause.ALL else 'launches'
    return f'paused ({holds}): {campaign.paused_reason}'


def progress(s: Session, campaign: Campaign, cfg: CampaignConfig, now: datetime) -> str:
    """Return the runs and hours so far, with each ``[stop]`` condition against its limit: ``run 7/200 · 2.1h/12h``.

    If there is a ``[spend]`` budget, the spend follows (``$412.30/$1,000.00 spend``). When the stop rule holds
    launches, the line ends with why: `` (launches held: spend limit reached)``.
    """
    stop = cfg.stop
    parts = [
        f'run {runs(s, campaign)}{"" if stop.max_runs is None else f"/{stop.max_runs}"}',
        f'{hours(s, campaign, now) or 0:.1f}h{"" if stop.max_hours is None else f"/{stop.max_hours:g}h"}',
    ]
    if stop.max_tokens is not None:
        parts.append(f'{human(campaign_tokens(s, campaign.id))}/{human(stop.max_tokens)} tok')
    if stop.plateau is not None:
        parts.append(f'{runs_without_keep(s, campaign)}/{stop.plateau} runs without a keep')
    if stop.target is not None:  # A target is only allowed with one track.
        at = head_scores(s, campaign, cfg)
        value = at[1][0][1] if at is not None else None
        sign = '≥' if cfg.all_tracks[0].primary.direction is Direction.HIGHER else '≤'
        parts.append(f'target {sign} {metric(stop.target)} (head {_num(value)})')
    if (budget := cfg.spend) is not None:
        usd = '$?' if campaign.spend_usd is None else f'${campaign.spend_usd:,.2f}'
        parts.append(f'{usd}/${budget.limit:,.2f} spend')
    hold = held(s, campaign, cfg)
    return ' · '.join(parts) + (f' (launches held: {hold})' if hold is not None else '')


def produced(campaign: Campaign) -> Select[Candidate]:
    """Select the candidates of the campaign: the baseline and the commits of its runs, not imported references."""
    return select(Candidate).where(Candidate.campaign_id == campaign.id, Candidate.verdict != Verdict.REFERENCE)


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


def agent_output(s: Session, w: AgentSession, campaign_path: Path, lines: int) -> list[str]:
    """Return the last ``lines`` non-empty lines of output of the agent.

    The first choice is the text in the session file of the harness, found in the same way as its usage
    (``UsageSpec``). If the harness has no usage spec, or the agent has written no text yet, the screen from the
    runtime is used, without the lines that the harness draws. The last choice is the transcript saved when the agent
    ended.
    """
    definition = harness_of(s, w)
    said = definition.usage.resolved.said(w.session_id, lines) if definition and definition.usage else []
    return (
        said
        or above_chrome(_live_output(s, w, campaign_path, lines + LIVE_LINES))[-lines:]
        or above_chrome(_transcript(w.log_dir))[-lines:]
    )


def _live_output(s: Session, w: AgentSession, campaign_path: Path, lines: int) -> str:
    try:
        return runtime_for(s, w, campaign_path).read(handle_for(w), lines=lines)
    except Exception:  # Not launched yet, runtime missing or agent gone: the transcript is the fallback.
        return ''


def _transcript(log_dir: str | None) -> str:
    path = Path(log_dir) / TRANSCRIPT if log_dir else None
    return path.read_text(errors='replace') if path is not None and path.is_file() else ''


def above_chrome(text: str) -> list[str]:
    """Return the non-empty lines above the last ``CHROME`` line, without any other ``CHROME`` line."""
    lines = [line.rstrip() for line in text.splitlines() if line.strip()]
    furniture = [i for i, line in enumerate(lines) if CHROME.match(line)]
    if furniture:
        lines = [line for line in lines[: furniture[-1]] if not CHROME.match(line)]
    return lines


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


def anomalies(s: Session, campaign: Campaign, cfg: CampaignConfig, now: datetime, jobs: JobsProbe) -> list[Anomaly]:
    """Return every anomaly of ``campaign``, newest first. ``jobs`` is called only for silent implementers."""
    found = [
        *_failed_runs(s, campaign),
        *_dead_at_startup(s, campaign, cfg),
        *_same_as_parent(s, campaign, cfg),
        *_idle_queue(s, campaign, cfg),
        *_silent(s, campaign, now, jobs),
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


def _goal(s: Session, campaign: Campaign, cfg: CampaignConfig, now: datetime) -> list[str]:
    primaries = {t.primary.name for t in cfg.all_tracks}
    others = ', '.join(f'{m.name} ({m.direction}, {m.role})' for m in cfg.metrics if m.name not in primaries)
    also = f'; also measured: {others}' if others else ''
    state = f'stopped — {campaign.stopped_reason}' if campaign.status is CampaignStatus.STOPPED else 'active'
    if campaign.paused is not None:
        state += f', {paused(campaign)}'
    stop = cfg.stop
    limits = (stop.target, stop.max_runs, stop.max_hours, stop.max_tokens, stop.plateau)
    open_ended = '' if any(v is not None for v in limits) else ' (no [stop] condition: open-ended)'
    if cfg.multitrack:
        tracks = '; '.join(f'{t.name} ({_metric(t.primary)}, share {t.share:g})' for t in cfg.all_tracks)
        objective = f'- Tracks, each a primary its keeps improve while holding the others: {tracks}{also}'
    else:
        primary = cfg.all_tracks[0].primary
        objective = f'- Primary metric: {primary.name}, {primary.direction} is better{also}'
    return [
        objective,
        f'- Campaign: {state}. Progress: {progress(s, campaign, cfg, now)}{open_ended}.',
        "- The goal in words is the campaign's brief.md: weigh the metric against it.",
    ]


def _metric(m: Metric) -> str:
    """Format a primary metric as a guardrail of another track: ``accuracy, higher, margin 0.01, floor 0.8``."""
    margin = f', margin {metric(m.margin)}' if m.margin is not None else ''
    floor = f', floor {metric(m.floor)}' if m.floor is not None else ''
    return f'{m.name}, {m.direction}{margin}{floor}'


def tracks_lines(s: Session, campaign: Campaign, cfg: CampaignConfig, now: datetime) -> list[str]:
    """Return one line per track.

    Each line shows the primary at the head against the start and its floor, the trunk keeps of the track and the
    latest one, its share of the implementer slots against its open runs, and its runnable ideas.
    """
    evaluator_id = campaign.evaluator_id
    trunk = head(s, campaign)
    first = baseline(s, campaign)
    steps = s.scalars(trunk_keeps(campaign).where(Candidate.run_id.is_not(None)).order_by(Candidate.id)).all()
    open_runs = campaign_runs(campaign.id).where(Run.status.in_(OPEN_RUNS)).with_only_columns(Idea.track)
    running = Counter(s.scalars(open_runs))
    ready = Counter(s.scalars(select(Idea.track).where(Idea.campaign_id == campaign.id, runnable())))
    slots = cfg.run_slots
    lines = []
    for track in cfg.all_tracks:
        m = track.primary
        value = score(trunk, evaluator_id, m) if trunk is not None else None
        start = score(first, evaluator_id, m)
        gain = f' ({value - start:+g} on the start)' if value is not None and start is not None else ''
        floor = ''
        if m.floor is not None:
            floor = f'; floor {metric(m.floor)}' + (
                f', {metric(m.gain(value, m.floor))} to spare' if value is not None else ''
            )
        mine = [c for c in steps if track_of(cfg, c).name == track.name]
        latest = f', the latest candidate#{mine[-1].id} {age(_known(mine[-1]), now)} ago' if mine else ''
        lines.append(
            f'- {track.name}: {m.name} {_num(value)} at the head{gain}{floor}; {len(mine)} trunk keeps{latest};'
            f' share {track.share:g} of {slots} slots, {running[track.name]} running; {ready[track.name]} runnable'
            ' ideas.'
        )
    return lines


def _trajectory(s: Session, campaign: Campaign, cfg: CampaignConfig, now: datetime) -> list[str]:
    """Show the trunk: the start, then each trunk keep in order, with its gain on its track."""
    evaluator_id = campaign.evaluator_id
    steps = s.scalars(trunk_keeps(campaign).order_by(Candidate.id)).all()
    if not steps:
        return ["- No trunk keep: the baseline's verdict was overridden and nothing was kept since."]
    first, last = steps[0], steps[-1]
    gains = []
    for track in cfg.all_tracks:
        a, b = score(first, evaluator_id, track.primary), score(last, evaluator_id, track.primary)
        if a is None or b is None:
            gains.append(f'{track.primary.name} unmeasured')
            continue
        relative = f', {(b - a) / abs(a):+.1%}' if a else ''
        gains.append(f'{track.primary.name} {metric(a)} → {metric(b)} ({metric(b - a, signed=True)}{relative})')
    finished = campaign_runs(campaign.id).where(
        Run.status.in_(FINISHED_RUNS), attempt(), Run.finished_at > _known(last)
    )
    lines = [
        f'- Start candidate#{first.id}, head candidate#{last.id}: {"; ".join(gains)}; {len(steps) - 1} trunk keeps.',
        f'- Last trunk keep {age(_known(last), now)} ago; {count(s, finished)} runs finished since.',
        f'- Trunk keeps (latest {STEPS_SHOWN}; when first measured, the gain on its track over the head before it):',
    ]
    before = dict(zip([c.id for c in steps[1:]], steps, strict=False))
    for c in steps[-STEPS_SHOWN:]:
        track = track_of(cfg, c)
        value, previous = score(c, evaluator_id, track.primary), before.get(c.id)
        prior = score(previous, evaluator_id, track.primary) if previous is not None else None
        if previous is None:
            delta = ' (start)'
        else:
            delta = f' ({value - prior:+g})' if value is not None and prior is not None else ''
        on = f' [{track.name}] {track.primary.name}' if cfg.multitrack and previous is not None else ''
        idea = f' idea#{c.run.idea_id}' if c.run is not None else ''
        lines.append(
            f'  - {_known(c):%m-%d %H:%M} candidate#{c.id}{on} {_num(value)}{delta}{idea}: {cell(c.summary or "—", 80)}'
        )
    return lines + _branches(s, campaign, cfg)


def _known(c: Candidate) -> datetime:
    """Return the time of the first successful measurement of the candidate, else its creation time."""
    return min((m.created_at for m in c.measurements if m.error is None), default=c.created_at)


def _values(c: Candidate, cfg: CampaignConfig, evaluator_id: EvaluatorId | None) -> str:
    """Format the primary of each track on ``c``: ``accuracy 0.91, seconds 3.1``. One track shows the score only."""
    if not cfg.multitrack:
        return _num(score(c, evaluator_id, cfg.all_tracks[0].primary))
    return ', '.join(f'{t.primary.name} {_num(score(c, evaluator_id, t.primary))}' for t in cfg.all_tracks)


def _branches(s: Session, campaign: Campaign, cfg: CampaignConfig) -> list[str]:
    """List the lineages of keeps if there is more than one, then the latest notes: other places a run can start."""
    found = branches(s, campaign)
    evaluator_id = campaign.evaluator_id
    lines = []
    if len(found) > 1:
        lines.append('- Lineages (latest keep of each; a branch keep never moves the head):')
        lines += [
            f'  - {"branch" if b.branched else "trunk"} from candidate#{b.root.id} ({b.root.verdict}):'
            f' latest candidate#{b.latest.id} {_values(b.latest, cfg, evaluator_id)}, {b.keeps} kept'
            for b in found
        ]
    noted = s.scalars(
        select(Candidate)
        .where(
            Candidate.campaign_id == campaign.id,
            Candidate.verdict == Verdict.INTERESTING,
            Candidate.verdict_reason.startswith(NOTE),
        )
        .order_by(Candidate.id.desc())
        .limit(STEPS_SHOWN)
    ).all()
    if noted:
        lines.append('- Notes (regressed, kept in view as leads; pin one with base_candidate_id):')
        lines += [
            f'  - candidate#{c.id} {_num(score(c, evaluator_id, track_of(cfg, c).primary))} by {c.verdict_by}:'
            f' {cell((c.verdict_reason or "").removeprefix(NOTE), 100)}'
            for c in noted
        ]
    return lines


def _now(s: Session, campaign: Campaign, cfg: CampaignConfig, now: datetime) -> list[str]:
    return [
        *_runs_now(s, campaign, cfg, now),
        *_queue(s, campaign, now),
        *_agents(s, campaign, cfg, now),
        _evaluations(s, campaign),
    ]


def _runs_now(s: Session, campaign: Campaign, cfg: CampaignConfig, now: datetime) -> list[str]:
    open_runs = s.scalars(campaign_runs(campaign.id).where(Run.status.in_(OPEN_RUNS)).order_by(Run.id)).all()
    lines = [f'- Implementer slots: {len(open_runs)} of {cfg.run_slots} busy.']
    for run in open_runs:
        agent_session = latest_agent_session(s, run.id)
        state = f'{run.status}'
        if run.started_at is not None:
            state += f' {age(run.started_at, now)}'
        if agent_session is not None and agent_session.ended_at is not None:
            state += f', agent_session#{agent_session.id} ended {age(agent_session.ended_at, now)} ago'
        lines.append(f'  - run#{run.id} idea#{run.idea_id} "{cell(run.idea.title, 60)}" ({run.harness}): {state}')
    return lines


def _queue(s: Session, campaign: Campaign, now: datetime) -> list[str]:
    """Show the idea counts by status, what blocks the blocked ideas, and the runnable ideas in queue order."""
    by_status = select(Idea.status, func.count()).where(Idea.campaign_id == campaign.id).group_by(Idea.status)
    counts: dict[IdeaStatus, int] = {status: n for status, n in s.execute(by_status)}
    waiting, ready = queued_ideas(s, campaign)
    blocked = [i for i in waiting if i not in ready]
    waits = [
        f'idea#{i.id} on ' + ', '.join(f'idea#{t.id} ({t.status})' for t in i.after if t.status is not IdeaStatus.DONE)
        for i in blocked[:BLOCKED_SHOWN]
    ]
    more = f', {len(blocked) - BLOCKED_SHOWN} more' if len(blocked) > BLOCKED_SHOWN else ''
    shown = f' ({"; ".join(waits)}{more})' if waits else ''
    rest = ', '.join(f'{counts.get(status, 0)} {status}' for status in IdeaStatus if status is not IdeaStatus.READY)
    newer = superseded_by(s, [i.id for i in ready[:QUEUE_SHOWN]])
    queue = [
        f'  - idea#{i.id} p{i.priority} "{cell(i.title, 60)}" by {i.created_by}, {age(i.created_at, now)} ago'
        + ('' if (linked := links(i, newer)) == NONE else f'; {linked}')
        for i in ready[:QUEUE_SHOWN]
    ]
    return [f'- Ideas: {len(ready)} runnable, {len(blocked)} blocked{shown}; {rest}.', *queue]


def _agents(s: Session, campaign: Campaign, cfg: CampaignConfig, now: datetime) -> list[str]:
    """Show the live agents with their last line, the agents per role, the yield of each source and the tokens."""
    rows = s.scalars(
        select(AgentSession).where(AgentSession.campaign_id == campaign.id).order_by(AgentSession.id)
    ).all()
    path = Path(campaign.path)
    live = [
        f'  - {w.kind}#{w.id} {w.harness or NONE}/{w.model or NONE} {age(w.started_at, now)}'
        f'{f", {view}" if (view := view_of(w)) else ""}, on {on(s, w)}: '
        + (cell(output[-1]) if (output := agent_output(s, w, path, 1)) else NONE)
        for w in rows
        if w.state in LIVE
    ]
    requested = [f'{w.kind}#{w.id}' for w in rows if w.state is AgentSessionState.REQUESTED]
    per_kind: list[str] = []
    spend: list[str] = []
    for kind in dict.fromkeys([*cfg.roles.all(), *(w.kind for w in rows)]):
        mine = [w for w in rows if w.kind == kind]
        killed = sum(w.state is AgentSessionState.KILLED for w in mine)
        per_kind.append(f'{len(mine)} {kind}' + (f' ({killed} killed)' if killed else ''))
        spend.append(f'{kind} {human(sum(_tokens(w) for w in mine))}')
    defined = defined_by(s, campaign.id)
    barren = []
    for kind in defining(cfg):
        sweeps = [w for w in rows if w.kind == kind and w.state in ENDED]
        empty = sum(f'{kind}#{w.id}' not in defined for w in sweeps)
        barren.append(f'; {empty} of {len(sweeps)} ended {kind} sweeps defined no idea')
    hour = sum(_tokens(w) for w in rows if w.ended_at is not None and now - w.ended_at <= timedelta(hours=1))
    by_source = '; '.join(y.line() for y in yields(s, campaign.id)) or 'none yet'
    return [
        f'- Agents live: {len(live) or "none"}' + (f'; requested: {", ".join(requested)}' if requested else ''),
        *live,
        f'- Agents so far: {", ".join(per_kind)}{"".join(barren)}.',
        f'- Ideas by source: {by_source}.',
        f'- Tokens: {human(sum(_tokens(w) for w in rows))} ({", ".join(spend)}); {human(hour)} by agents that ended in'
        ' the last hour.',
    ]


def _evaluations(s: Session, campaign: Campaign) -> str:
    """Show the number of evaluations, their duration now against at first, and the candidates without a verdict."""
    evaluator_id = campaign.evaluator_id
    mine = select(Candidate.id).where(Candidate.campaign_id == campaign.id)
    rows = s.scalars(
        select(Measurement)
        .where(Measurement.candidate_id.in_(mine), Measurement.evaluator_id == evaluator_id)
        .order_by(Measurement.id)
    ).all()
    crashed = sum(m.error is not None for m in rows)
    seconds = [m.seconds for m in rows if m.seconds is not None]
    waiting = count(
        s, select(Candidate).where(Candidate.campaign_id == campaign.id, Candidate.verdict == Verdict.PENDING)
    )
    timing = ''
    if seconds:
        timing = (
            f'; median {_minutes(median(seconds))}, first {EVALS_IN_TREND} {_minutes(median(seconds[:EVALS_IN_TREND]))},'
            f' latest {EVALS_IN_TREND} {_minutes(median(seconds[-EVALS_IN_TREND:]))}; {_minutes(sum(seconds))} in all'
        )
    return f'- Evaluations: {len(rows)} ({crashed} crashed){timing}; {waiting} candidates waiting for a verdict.'


def _candidates(s: Session, campaign: Campaign, cfg: CampaignConfig, now: datetime) -> list[str]:
    """List the last ``CANDIDATES_SHOWN`` candidates of the campaign, oldest first, scored against their parent."""
    total = count(s, produced(campaign))
    recent = s.scalars(produced(campaign).order_by(Candidate.id.desc()).limit(CANDIDATES_SHOWN)).all()[::-1]
    on = "each on its track's primary" if cfg.multitrack else cfg.all_tracks[0].primary.name
    lines = [f'### Recent candidates ({len(recent)} of {total}; {on}, vs parent)']
    for c in recent:
        track = track_of(cfg, c)
        value, delta = scored(c, campaign.evaluator_id, track.primary)
        run = f'run#{c.run_id}' if c.run_id else 'no run'
        tag = f' [{track.name}]' if cfg.multitrack else ''
        lines.append(
            f'- candidate#{c.id} {c.verdict}{tag} {value} ({delta}), {run}, {age(c.created_at, now)} ago:'
            f' {cell(c.summary or NONE)}'
        )
    return lines


def _anomalies(found: list[Anomaly], now: datetime) -> list[str]:
    """List incidents one per line, newest first. Group each other kind on one line, because it is a cost."""
    if not found:
        return ['- None.']
    incidents = [a for a in found if a.incident]
    older = len(incidents) - ANOMALIES_SHOWN
    lines = [a.line(now) for a in incidents[:ANOMALIES_SHOWN]]
    if older > 0:
        lines.append(f'- … and {older} older incidents.')
    for kind in AnomalyKind:
        same = [a for a in found if a.kind is kind and not a.incident]
        if len(same) == 1:
            lines.append(same[0].line(now))
        elif same:
            refs = ', '.join(str(a.ref) for a in same[:REFS_SHOWN]) + (', …' if len(same) > REFS_SHOWN else '')
            lines.append(f'- {len(same)} × {kind} ({refs}); the latest, {age(same[0].at, now)} ago: {same[0].detail}')
    return lines


def _comments(s: Session, campaign: Campaign, now: datetime) -> list[str]:
    """List the inbox and the latest decisions and escalations, newest first.

    A sweep reads this so it does not repeat an escalation, and a human sees what waits for an acknowledgement. The
    findings of every role are in the history (``piloop.reports.history``).
    """
    unacked = Comment.priority.is_not(None) & Comment.acked_at.is_(None)
    said = unacked | Comment.kind.in_((CommentKind.DECISION, CommentKind.ESCALATION))
    rows = s.scalars(
        select(Comment)
        .where(said, campaign_of(Comment.target_kind, Comment.target_id) == campaign.id)
        .order_by(Comment.id.desc())
        .limit(COMMENTS_SHOWN)
    ).all()
    waiting = len(inbox.items(s, campaign_id=campaign.id))
    lines = [
        f'- comment#{c.id} {c.kind}{f" (p{c.priority}, unacknowledged)" if c.priority is not None and c.acked_at is None else ""}'
        f' on {c.target} by {c.author}, {age(c.created_at, now)} ago: {cell(c.body, DETAIL_CHARS)}'
        for c in rows
    ]
    return [
        f'### Inbox ({waiting} unacknowledged), decisions and escalations',
        *(lines or ['- None.']),
    ]


def latest_events(s: Session, campaign: Campaign, limit: int) -> Sequence[Event]:
    """Return the last ``limit`` events of the campaign, newest first."""
    mine = select(Event).where(campaign_of(Event.target_kind, Event.target_id) == campaign.id)
    return s.scalars(mine.order_by(Event.id.desc()).limit(limit)).all()


def _events(s: Session, campaign: Campaign, now: datetime) -> str:
    latest = latest_events(s, campaign, EVENTS_SHOWN)
    return '- ' + (' · '.join(f'{e.kind} {e.target} {age(e.at, now)} ago' for e in latest) or 'None.')


TOOLS = [
    '- `piloop sql "<one statement>"` (the full command is under Context) takes nothing else: no flags, no --help.'
    ' It prints a table cut to the terminal width, so prefix `COLUMNS=400` for wide rows, or select one'
    " concatenated column (`a || ' | ' || b`); at most 500 rows.",
    "- Scores: `json_extract(m.metrics, '$.<metric>')`; a candidate's score is the mean over its measurements with"
    ' `error IS NULL AND evaluator_id = campaigns.evaluator_id`.'
    f' `piloop history` prints the latest {IDEAS_IN_HISTORY} ideas with their outcomes.',
    f'- Transcripts: `<runs.log_dir>/{TRANSCRIPT}` (an implementer), `<agent_sessions.log_dir>/{TRANSCRIPT}` and `brief.md`'
    ' beside it (any agent); the whole session: `cd <repo> && entire checkpoint explain <agent_sessions.checkpoint_id>`.',
    '- Code: `git -C <repo> show <candidates.commit_sha>`; a candidate against its parent:'
    ' `git -C <repo> diff <parent commit_sha> <commit_sha>`.',
    "- Who changed what: `SELECT at, actor, kind, payload FROM events WHERE target_kind='idea' AND target_id=12`.",
    '- Full-text search: `ideas_fts` (title, hypothesis, plan), `comments_fts` (body); see the procedures above.',
    '- Tables and columns:',
]


def _schema() -> list[str]:
    """List every table with its columns, from the models, so that an agent does not query the schema."""
    return [f'  - {t.name}({", ".join(c.name for c in t.columns)})' for t in Base.metadata.sorted_tables]


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
            f' {_minutes(sum(m.seconds or 0 for m in rows))} of evaluation'
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
    if cfg.stop.max_runs is not None:
        free = min(free, cfg.stop.max_runs - runs(s, campaign))
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


def _silent(s: Session, campaign: Campaign, now: datetime, jobs: JobsProbe) -> list[Anomaly]:
    """Find running implementers with jobs in flight that have not commented or committed for ``SILENT_SECONDS``.

    The silence starts at the latest of: the start of the run, a comment signed ``implementer#<id>`` or ``run#<id>``, and
    the HEAD commit of its worktree. The commits of an implementer become candidates only when it ends, so the worktree is
    checked directly. The runtime is asked for jobs last, and only for implementers that are already silent. Each implementer
    gets one anomaly. If an implementer goes silent again later, the progress review notices it.
    """
    limit = timedelta(seconds=SILENT_SECONDS)
    rows = s.scalars(
        select(AgentSession)
        .where(
            AgentSession.campaign_id == campaign.id,
            AgentSession.state == AgentSessionState.RUNNING,
            AgentSession.run_id.is_not(None),
        )
        .order_by(AgentSession.id)
    )
    found: list[Anomaly] = []
    for agent_session in rows:
        run = s.get_one(Run, agent_session.run_id)
        signed = (*deprecated.signatures(Agent(role=agent_session.kind, session=agent_session.id)), f'run#{run.id}')
        moments = [
            ('its run started', run.started_at or agent_session.started_at),
            ('its last comment', s.scalar(select(func.max(Comment.created_at)).where(Comment.author.in_(signed)))),
            ('its last commit', git.head_time(Path(run.log_dir) / 'worktree') if run.log_dir else None),
        ]
        what, last = max(((w, m) for w, m in moments if m is not None), key=lambda p: p[1])
        if now - last <= limit:
            continue
        running = jobs(agent_session)
        if not running:
            continue
        detail = (
            f'run#{run.id} idea#{run.idea_id}: no comment or commit for {age(last, now)} ({what});'
            f' {len(running)} jobs in flight, e.g. {cell(running[0], DETAIL_CHARS)}'
        )
        ref = TargetRef(TargetKind.AGENT_SESSION, agent_session.id)
        found.append(Anomaly(AnomalyKind.SILENT, ref, last + limit, detail))
    return found


def defining(cfg: CampaignConfig) -> list[str]:
    """Return the sweeping roles that define ideas (``files = "ideas"``): the ones that fill the queue."""
    return [name for name, role in cfg.roles.sweeping().items() if role.files is Files.IDEAS]


def _tokens(w: AgentSession) -> int:
    return w.usage.total if w.usage else 0


def _minutes(seconds: float) -> str:
    return f'{seconds / 60:.1f} min'


def _num(value: float | None) -> str:
    return NONE if value is None else metric(value)


def _four(value: float | None) -> str:
    return 'unmeasured' if value is None else metric(value)
