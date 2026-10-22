"""What ``piloop watch`` shows, as data: the header, the campaigns, and for each panel its rows and the row detail.

There are four panels (inbox, agents, ideas, candidates). Each is a ``Panel``: a title, the rows (``Item``: a key and
the cells) and the detail of one row for the main pane. Every function reads in the session that it gets, so a frame
is one short read. Nothing here knows Textual: ``piloop/watch/app.py`` draws it. The facts and the one-line cells come
from ``piloop/reports/overview.py``, which ``piloop status`` prints, so the two show the same thing.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol

from rich.console import Group, RenderableType
from rich.text import Text
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.actor import Actor
from piloop.campaign import inbox
from piloop.campaign.ideas import links, superseded_by
from piloop.config import CampaignConfig, config_of
from piloop.db.models import AgentSession, Campaign, CampaignStatus, Candidate, Comment, Idea, IdeaStatus, Verdict
from piloop.ids import CampaignId, TargetKind, TargetRef
from piloop.plugins.runtime.contract import AgentStatus
from piloop.reports.overview import (
    age,
    agent_output,
    head_scores,
    item_lines,
    latest_events,
    live_agent_sessions,
    on,
    paused,
    progress,
    queued_ideas,
    scored,
    stopped,
    summary,
    tracks_lines,
)
from piloop.runtime.factory import handle_for, runtime_for, view_of
from piloop.scoring.ledger import metrics, references, score, track_of, trunk_keeps
from piloop.text import NONE, cell, metric
from piloop.usage import campaign_tokens, human

AGENT_LINES = 60
"""How many lines of the selected agent's own output its detail shows."""
CANDIDATE_ROWS = 50
SPARK_POINTS = 40
SPARKS = '▁▂▃▄▅▆▇█'
STATUS_CHARS = 40
"""How much of a stopped campaign's reason the campaigns screen shows. ``piloop status`` shows all of it."""
IDEA_ORDER = (IdeaStatus.SEED, IdeaStatus.PROPOSED, IdeaStatus.READY, IdeaStatus.SCHEDULED, IdeaStatus.PARKED)
"""The statuses that the ideas panel shows, in this order: the seeds for research, then the ideas that wait for a
review. The panel leaves out done and rejected ideas."""
STATES = {
    AgentStatus.WORKING: '●',
    AgentStatus.IDLE: '○',
    AgentStatus.BLOCKED: '◐',
}
"""The mark for an agent's state, as the runtime reports it. Any other state (gone, unknown) is ``·``."""
BLOCKED = 'blocked on a dialog'

type Cell = str | Text


@dataclass(frozen=True)
class Item:
    """One row of a panel: the id it stands for and its cells."""

    key: int
    cells: tuple[Cell, ...]


@dataclass(frozen=True)
class Context:
    """What every panel of one campaign reads: the campaign, its config, the time of the frame and the watcher."""

    campaign: Campaign
    cfg: CampaignConfig
    now: datetime
    me: Actor
    """The watcher's actor. The inbox leaves out what it wrote: a reply waits for the orchestrator, not its author."""


class Panel(Protocol):
    """A panel's data: its title, its rows, a row's detail."""

    def title(self, s: Session, ctx: Context, items: Sequence[Item]) -> str:
        """Return the panel's name and the counts over ``items``, the rows shown."""
        ...

    def items(self, s: Session, ctx: Context) -> list[Item]:
        """Return every row, in the panel's order."""
        ...

    def detail(self, s: Session, ctx: Context, key: int) -> RenderableType:
        """Return row ``key`` in full, for the main pane."""
        ...


def header(s: Session, campaign: Campaign, cfg: CampaignConfig, now: datetime) -> Text:
    """Return the summary line (head, kept, stopped), the progress line against ``[stop]`` (with the spend), and one per track."""
    text = Text(f'{summary(s, campaign, cfg)}{stopped(campaign)}\n', style='bold')
    text.append(progress(s, campaign, cfg, now), style='dim')
    if cfg.multitrack:
        text.append('\n' + '\n'.join(tracks_lines(s, campaign, cfg, now)), style='dim')
    return text


def campaigns(s: Session, now: datetime, me: Actor) -> list[Item]:
    """Return one row per campaign, active ones first: status, head, delta to baseline, agents, queue, inbox, spend."""
    rows = []
    every = sorted(s.scalars(select(Campaign)).all(), key=lambda p: (p.status is not CampaignStatus.ACTIVE, p.id))
    for p in every:
        cfg = config_of(p)
        at = head_scores(s, p, cfg)
        values = at[1] if at is not None else []
        _, ready = queued_ideas(s, p)
        spent = campaign_tokens(s, p.id)
        latest = next(iter(latest_events(s, p, 1)), None)
        rows.append(
            Item(
                p.id,
                (
                    p.name,
                    cell(f'stopped: {p.stopped_reason}', STATUS_CHARS)
                    if p.status is CampaignStatus.STOPPED
                    else cell(paused(p), STATUS_CHARS)
                    if p.paused is not None
                    else str(p.status),
                    ', '.join(f'{_four(value)} {track.primary.name}' for track, value, _ in values) or NONE,
                    ', '.join(NONE if delta is None else metric(delta, signed=True) for _, _, delta in values) or NONE,
                    f'{len(live_agent_sessions(s, p))} agents',
                    f'{len(ready)} ready',
                    f'{len(waiting(s, p.id, me))} inbox',
                    f'{human(spent)} tok' if spent else NONE,
                    NONE if latest is None else f'{latest.kind} {age(latest.at, now)} ago',
                ),
            )
        )
    return rows


CAMPAIGN_COLUMNS = ('campaign', 'status', 'head', 'vs baseline', 'agents', 'ready', 'inbox', 'tokens', 'last event')


def waiting(s: Session, campaign_id: CampaignId, me: Actor) -> list[Comment]:
    """Return the campaign's unacknowledged inbox items that someone other than ``me`` wrote, most urgent first."""
    return [c for c, _ in inbox.items(s, campaign_id=campaign_id) if c.author != me.signature]


def lines(*parts: str) -> Group:
    """Return plain lines that are never read as Rich markup, because bodies are data."""
    return Group(*(Text(p) for p in parts))


class InboxPanel:
    """Panel 1: the unacknowledged inbox, most urgent first. The detail is the item in full, with its thread."""

    def title(self, s: Session, ctx: Context, items: Sequence[Item]) -> str:
        return f'Inbox {len(items)}'

    def items(self, s: Session, ctx: Context) -> list[Item]:
        return [
            Item(
                c.id,
                (f'p{c.priority}', str(c.kind), c.author, age(c.created_at, ctx.now), cell(c.body)),
            )
            for c in waiting(s, ctx.campaign.id, ctx.me)
        ]

    def detail(self, s: Session, ctx: Context, key: int) -> RenderableType:
        comment = s.get(Comment, key)
        if comment is None:
            return Text(f'comment#{key} is gone')
        title, body = item_lines(s, comment, ctx.now)
        return Group(Text(title, style='bold'), Text(), lines(*body))


class AgentsPanel:
    """Panel 2: the running agents, with the state that the runtime reports. The detail is the agent's last output."""

    def title(self, s: Session, ctx: Context, items: Sequence[Item]) -> str:
        blocked = sum(1 for i in items if i.cells[2] == AgentStatus.BLOCKED)
        return f'Agents {len(items)}{f" · {blocked} blocked" if blocked else ""}'

    def items(self, s: Session, ctx: Context) -> list[Item]:
        path = Path(ctx.campaign.path)
        rows = []
        for w in live_agent_sessions(s, ctx.campaign):
            state = state_of(s, w, path)
            rows.append(
                Item(
                    w.id,
                    (
                        f'{STATES.get(state, "·") if state else "·"} #{w.id}',
                        str(w.kind),
                        str(state or NONE),
                        f'{w.harness}/{w.model}' if w.harness else NONE,
                        age(w.started_at, ctx.now),
                        on(s, w),
                    ),
                )
            )
        return rows

    def detail(self, s: Session, ctx: Context, key: int) -> RenderableType:
        w = s.get_one(AgentSession, key)
        path = Path(ctx.campaign.path)
        state = state_of(s, w, path)
        where = ' '.join(p for p in (w.runtime, view_of(w)) if p) or NONE
        facts = [
            f'{w.kind}-{w.id} · {w.harness}/{w.model} · {w.state} {age(w.started_at, ctx.now)} · {where}',
            f'on: {on(s, w)}',
        ]
        if w.reason:
            facts.append(f'why: {cell(w.reason)}')
        if state is AgentStatus.BLOCKED:
            facts.append(f'{BLOCKED}: steering is refused until it is answered; enter brings it to the front')
        output = agent_output(s, w, path, AGENT_LINES)
        return Group(
            Text(facts[0], style='bold'),
            lines(*facts[1:]),
            Text(),
            Text(f'the last {len(output)} lines it wrote', style='dim'),
            lines(*output) if output else Text(NONE),
        )


def state_of(s: Session, w: AgentSession, campaign_path: Path) -> AgentStatus | None:
    """Return what the runtime says the agent does now. None when it cannot say (not launched, runtime missing)."""
    try:
        return runtime_for(s, w, campaign_path).status(handle_for(w))
    except Exception:  # not launched yet, or the runtime is missing: the row shows · and the rest of the screen draws
        return None


class IdeasPanel:
    """Panel 3: seeds, then proposed, ready, scheduled and parked ideas. The detail is the idea, runs and comments."""

    def title(self, s: Session, ctx: Context, items: Sequence[Item]) -> str:
        counts = {status: sum(1 for i in items if i.cells[1] == status) for status in IDEA_ORDER}
        return 'Ideas ' + ' · '.join(f'{n} {status}' for status, n in counts.items() if n) if items else 'Ideas 0'

    def items(self, s: Session, ctx: Context) -> list[Item]:
        rank = {status: i for i, status in enumerate(IDEA_ORDER)}
        ideas = sorted(
            s.scalars(select(Idea).where(Idea.campaign_id == ctx.campaign.id, Idea.status.in_(IDEA_ORDER))).all(),
            key=lambda i: (rank[i.status], i.priority, i.id),
        )
        newer = superseded_by(s, [i.id for i in ideas])
        return [
            Item(i.id, (f'P{i.priority}', str(i.status), f'#{i.id}', cell(i.title), links(i, newer))) for i in ideas
        ]

    def detail(self, s: Session, ctx: Context, key: int) -> RenderableType:
        idea = s.get_one(Idea, key)
        comments = s.scalars(
            select(Comment).where(Comment.target == TargetRef(TargetKind.IDEA, idea.id)).order_by(Comment.id)
        ).all()
        body = [
            f'status {idea.status} · priority {idea.priority} · by {idea.created_by} {age(idea.created_at, ctx.now)} ago'
            f' · links: {links(idea, superseded_by(s, [idea.id]))}',
            '',
            'hypothesis:',
            *idea.hypothesis.strip().splitlines(),
        ]
        if idea.plan:
            body += ['', 'plan:', *idea.plan.strip().splitlines()]
        body += ['', f'runs ({len(idea.runs)}):']
        body += [
            f'- run#{r.id} {r.status} {r.harness}/{r.model}{f": {cell(r.error)}" if r.error else ""}' for r in idea.runs
        ]
        body += ['', f'comments ({len(comments)}):']
        body += [f'- comment#{c.id} {c.kind} by {c.author}: {cell(c.body)}' for c in comments]
        return Group(Text(f'idea#{idea.id} {idea.title}', style='bold'), lines(*body))


class CandidatesPanel:
    """Panel 4: the recent candidates, newest first, under the sparkline of each track. The detail is the verdict."""

    def title(self, s: Session, ctx: Context, items: Sequence[Item]) -> str:
        starts = sum(1 for i in items if i.cells[1] == Verdict.REFERENCE)
        points = f' · {starts} starting point{"s" if starts != 1 else ""}' if starts else ''
        lines = kept_scores(s, ctx.campaign, ctx.cfg)
        if not ctx.cfg.multitrack:
            return f'Candidates {sparkline(lines[ctx.cfg.all_tracks[0].name])}{points}'
        return 'Candidates ' + ' · '.join(f'{name} {sparkline(values)}' for name, values in lines.items()) + points

    def items(self, s: Session, ctx: Context) -> list[Item]:
        """Return the campaign's own candidates, newest first, then the imported references, which are never kept."""
        evaluator = ctx.campaign.evaluator_id
        recent = s.scalars(
            select(Candidate)
            .where(Candidate.campaign_id == ctx.campaign.id, Candidate.verdict != Verdict.REFERENCE)
            .order_by(Candidate.id.desc())
            .limit(CANDIDATE_ROWS)
        ).all()
        rows = []
        for c in [*recent, *references(s, ctx.campaign)]:
            rows.append(
                Item(
                    c.id,
                    (
                        f'#{c.id}',
                        str(c.verdict),
                        *scored(c, evaluator, track_of(ctx.cfg, c).primary),
                        f'run#{c.run_id}' if c.run_id else NONE,
                        age(c.created_at, ctx.now),
                        cell(c.summary or NONE),
                    ),
                )
            )
        return rows

    def detail(self, s: Session, ctx: Context, key: int) -> RenderableType:
        c = s.get_one(Candidate, key)
        evaluator = ctx.campaign.evaluator_id
        mine = metrics(c, evaluator)
        theirs = metrics(c.parent, evaluator) if c.parent is not None else {}
        body = [
            f'verdict {c.verdict} by {c.verdict_by or NONE}: {c.verdict_reason or NONE}',
            f'parent {f"candidate#{c.parent_id}" if c.parent_id else NONE} · run {f"#{c.run_id}" if c.run_id else NONE}'
            f' · commit {c.commit_sha[:12]} · {age(c.created_at, ctx.now)} ago',
            '',
            f'metrics under {NONE if evaluator is None else f"evaluator#{evaluator}"} (vs parent):',
        ]
        for name in sorted(mine):
            delta = f' ({metric(mine[name] - theirs[name], signed=True)})' if name in theirs else ''
            body.append(f'  {name} {metric(mine[name])}{delta}')
        if not mine:
            body.append(f'  {NONE}')
        body += ['', 'summary:', *(c.summary or NONE).strip().splitlines()]
        return Group(Text(f'candidate#{c.id}', style='bold'), lines(*body))


PANELS: dict[str, Panel] = {
    'inbox': InboxPanel(),
    'agents': AgentsPanel(),
    'ideas': IdeasPanel(),
    'candidates': CandidatesPanel(),
}
"""The four panels, in their numbered order."""


def kept_scores(s: Session, campaign: Campaign, cfg: CampaignConfig) -> dict[str, list[float]]:
    """Return, for each track, its primary metric at the head after each of the last ``SPARK_POINTS`` trunk keeps.

    Each trunk keep was the head in turn. A branch keep never was the head, so it does not move the line.
    """
    kept = s.scalars(trunk_keeps(campaign).order_by(Candidate.id)).all()[-SPARK_POINTS:]
    evaluator_id = campaign.evaluator_id
    return {t.name: [v for c in kept if (v := score(c, evaluator_id, t.primary)) is not None] for t in cfg.all_tracks}


def _four(value: float | None) -> str:
    return NONE if value is None else metric(value)


def sparkline(values: list[float]) -> str:
    """Return ``▁▃▅█``, scaled from the smallest value to the largest, or ``—`` for no values."""
    if not values:
        return NONE
    low, span = min(values), (max(values) - min(values)) or 1
    marks = ''.join(SPARKS[round((v - low) / span * (len(SPARKS) - 1))] for v in values)
    return f'{marks} {metric(values[0])} → {metric(values[-1])}'
