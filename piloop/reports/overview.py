"""The facts of one campaign that its views share, computed from the database.

The sections (``piloop.plugins.section``), ``piloop watch`` (``piloop/watch/panels.py``), the API's status and
``piloop wake`` read them here, so they always say the same thing: the summary line, the head's scores, the queue, the
live agents and their output, the inbox threads, and the progress against the stop rule.
"""

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import assert_never

from covalence import Cid
from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from piloop.agents.roles import harness_of
from piloop.campaign.ideas import runnable
from piloop.campaign.inbox import campaign_of
from piloop.campaign.stop import held, hours, kept, limits, runs, runs_without_keep
from piloop.config import TRANSCRIPT, CampaignConfig, Metric, Track
from piloop.db.models import (
    LIVE,
    AgentSession,
    Campaign,
    Candidate,
    Event,
    Idea,
    Origin,
    Pause,
    Run,
    count,
)
from piloop.ids import CampaignStatus, EvaluatorId, IdeaStatus
from piloop.plugins.cas.contract import Cas
from piloop.plugins.metric import Direction
from piloop.plugins.role.contract import Files
from piloop.plugins.stop_rule.contract import Fact
from piloop.runtime.factory import handle_for, runtime_for
from piloop.scoring.ledger import (
    baseline,
    head,
    keeps,
    references,
    score,
)
from piloop.store import chunks
from piloop.store.tree import Missing
from piloop.text import NONE, cell, metric
from piloop.usage import UsageReader, human
from piloop.usage.totals import campaign_tokens

DETAIL_CHARS = 160


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


def agent_output(s: Session, w: AgentSession, lines: int, cas: Cas) -> list[str]:
    """Return the last ``lines`` non-empty lines of output of the agent.

    The first choice is the text in the latest snapshot of its session file in ``cas`` (``session_file``), on any
    machine. If there is none, or no text in it, the output that the runtime reads is used. The last choice is the
    transcript saved when the agent ended.
    """
    definition = harness_of(s, w)
    reader = definition.usage.resolved if definition and definition.usage else None
    said = (
        list(_said_in(_Snapshot(w.session_file, reader, cas), lines))
        if reader is not None and w.session_file is not None
        else []
    )
    return said or _filled(_live_output(w, lines))[-lines:] or _filled(_transcript(w.log_dir))[-lines:]


@dataclass(frozen=True)
class _Snapshot:
    """A snapshot of a session file, known by its address alone: where it is read from does not change its bytes."""

    session_file: Cid
    reader: UsageReader = field(compare=False)
    cas: Cas = field(compare=False)


@lru_cache(maxsize=256)
def _said_in(snapshot: _Snapshot, lines: int) -> tuple[str, ...]:
    """The last ``lines`` of the agent's text in a snapshot.

    Read from the CAS once for each snapshot: a watch draws the same snapshot many times.
    """
    try:
        session = chunks.read(snapshot.cas, snapshot.session_file)
    except Missing:  # a part is gone from the store: the screen or the transcript tells more
        return ()
    return () if session is None else tuple(snapshot.reader.said(session, lines))


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


def known(c: Candidate) -> datetime:
    """Return the time of the first successful measurement of the candidate, else its creation time."""
    return min((m.created_at for m in c.measurements if m.error is None), default=c.created_at)


def latest_events(s: Session, campaign: Campaign, limit: int) -> Sequence[Event]:
    """Return the last ``limit`` events of the campaign, newest first."""
    mine = select(Event).where(campaign_of(Event.target_kind, Event.target_id) == campaign.id)
    return s.scalars(mine.order_by(Event.id.desc()).limit(limit)).all()


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
