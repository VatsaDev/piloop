"""Experimental: the context of a scout sweep (``[plugins.role.scout]``), which files outside techniques as seeds.

A scout sweep runs ``prompts/scout.md`` and signs ``<role>#<agent session id>``. So its seeds have their own row in the
keeps-by-source count (``piloop.campaign.ideas.yields``), next to research's own ideas.

The scout's memory is the database. The technique library is the ``techniques`` table. The watch list is computed,
not stored: the X handles and GitHub repos linked from the role's seeds that research developed or that kept. A
source stays on the list until ``WATCH_RUNS`` sweeps of the role have started since one of its seeds last paid off. A
source with a keep stays.
"""

import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from importlib.resources import files
from typing import ClassVar

from pydantic import Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.db.events import has
from piloop.db.models import AgentSession, Candidate, Event, EventModel, Idea, Run, Technique, count
from piloop.db.triggers.changelog import IdeaStatusChanged
from piloop.ids import AgentSessionId, CampaignId, IdeaStatus, RoleName, TargetKind
from piloop.plugins.role.contract import Sweep, SweepRole
from piloop.plugins.trigger.contract import TriggerEntry
from piloop.plugins.verdict_rule.contract import Outcome
from piloop.text import cell

EventModel.retire('scout.requested')  # 0.1.0 requested a sweep with it; a sweep is an agent_sessions row now

WATCH_RUNS = 5
"""How many scout sweeps a source stays on the watch list after its last payoff. A source with a keep stays."""
WINDOW = timedelta(days=30)
"""How far back the first scout sweep searches."""
MIN_WINDOW = timedelta(days=7)
"""The shortest window of a later sweep, whatever the interval: papers and repos take longer than posts to appear."""
LIBRARY_SHOWN = 30
MECHANISM_CHARS = 200
SOURCE = (
    re.compile(r'https?://(?:www\.|mobile\.)?(?:x|twitter)\.com/(?P<handle>[A-Za-z0-9_]{1,15})/status/'),
    re.compile(r'https?://(?:www\.)?github\.com/(?P<repo>[A-Za-z0-9-]+/[A-Za-z0-9_.-]*[A-Za-z0-9_-])'),
)
"""The sources of a seed to watch: the X author of a post and the GitHub repo. An arXiv link names no author."""


def sources(text: str) -> list[str]:
    """Return the sources linked in ``text``, once each, in order: ``@handle`` for X, ``owner/repo`` for GitHub."""
    found = [
        f'@{m["handle"]}' if 'handle' in m.groupdict() else m['repo'].removesuffix('.git')
        for pattern in SOURCE
        for m in pattern.finditer(text)
    ]
    return list(dict.fromkeys(found))


@dataclass
class Watched:
    """One source on the watch list: the scout seeds that came from it, and whether any of them kept."""

    source: str
    ideas: list[int] = field(default_factory=list)
    kept: bool = False
    paid: datetime | None = None
    """When one of its seeds last paid off: it was developed, or a candidate of its idea kept."""
    since: int = 0
    """The number of scout sweeps started after that."""

    def line(self) -> str:
        how = 'kept' if self.kept else f'developed, {WATCH_RUNS - self.since} more scout sweeps unless one pays off'
        return f'- {self.source} ({how}; {", ".join(f"idea#{i}" for i in self.ideas)})'


def watch_list(s: Session, campaign_id: CampaignId, role: RoleName) -> list[Watched]:
    """Return the sources that later sweeps of ``role`` query directly: kept ones first, then the latest payoff first."""
    seeds = s.scalars(select(Idea).where(Idea.campaign_id == campaign_id, Idea.created_by.like(f'{role}#%'))).all()
    developed: dict[int, datetime] = {
        idea_id: at
        for idea_id, at in s.execute(
            select(Event.target_id, Event.at).where(
                has(IdeaStatusChanged, old=IdeaStatus.SEED, new=(IdeaStatus.READY, IdeaStatus.PROPOSED)),
                Event.target_kind == TargetKind.IDEA,
                Event.target_id.in_([i.id for i in seeds]),
            )
        )
    }
    keeps = s.execute(
        select(Run.idea_id, Candidate.created_at)
        .join(Candidate, Candidate.run_id == Run.id)
        .where(Run.idea_id.in_(list(developed)), Candidate.verdict == Outcome.KEEP)
    )
    kept: defaultdict[int, list[datetime]] = defaultdict(list)
    for idea_id, at in keeps:
        kept[idea_id].append(at)
    by_source: dict[str, Watched] = {}
    for idea in seeds:
        if idea.id not in developed:
            continue
        paid = max([developed[idea.id], *kept[idea.id]])
        for name in sources(idea.provenance or ''):
            entry = by_source.setdefault(name, Watched(name))
            entry.ideas.append(idea.id)
            entry.kept |= bool(kept[idea.id])
            entry.paid = paid if entry.paid is None else max(entry.paid, paid)
    scouts = select(AgentSession).where(AgentSession.campaign_id == campaign_id, AgentSession.kind == role)
    for entry in by_source.values():
        assert entry.paid is not None  # every entry comes from a developed seed
        entry.since = count(s, scouts.where(AgentSession.started_at > entry.paid))
    watched = [e for e in by_source.values() if e.kept or e.since < WATCH_RUNS]
    return sorted(watched, key=lambda e: (not e.kept, -(e.paid.timestamp() if e.paid else 0)))


def window(
    s: Session, campaign_id: CampaignId, role: RoleName, agent_session_id: AgentSessionId, now: datetime
) -> datetime:
    """Return the start of the search window: the role's previous sweep's start, at least ``MIN_WINDOW`` back."""
    previous = s.scalar(
        select(AgentSession.started_at)
        .where(
            AgentSession.campaign_id == campaign_id,
            AgentSession.id < agent_session_id,
            AgentSession.started_at.is_not(None),
        )
        .where(AgentSession.kind == role)
        .order_by(AgentSession.id.desc())
        .limit(1)
    )
    return now - WINDOW if previous is None else min(previous, now - MIN_WINDOW)


class ScoutRole(SweepRole):
    """``[plugins.role.scout]``: the scout files outside techniques as seeds for the measured losses. It is off by default.

    Every ``interval`` seconds, a scout sweep states the open problems of the campaign from the evidence, and searches
    X, GitHub and arXiv for outside techniques that address them. It files at most ``seeds`` of them as seeds, signed
    ``scout#<agent session id>``, and puts the rest in the technique library (the ``techniques`` table). It never files a
    defined idea. The search tool limits what one sweep spends on xAI (``scripts/xai_search_mcp.py --max-usd``).

    Experimental: the scout.
    """

    PROMPT = files(__package__) / 'prompts' / 'scout.md'
    IDEAS: ClassVar[str] = (
        "- File a seed: `{piloop} idea seed {campaign} '<technique: its mechanism, one line>' --from '<link>; targets"
        " <the measured loss it might reduce, with its size>; <its substance: a number, code, an ablation>'`: research"
        ' develops it into an idea or rejects it with the reason.'
        '\n- File a technique into the library: pipe `INSERT INTO techniques (campaign_id, title, mechanism, url,'
        " created_by) VALUES (<campaign id>, '<technique>', '<mechanism and its substance, one line>', '<link>',"
        " '<your signature>')` into `{piloop} sql -`."
    )

    triggers: list[TriggerEntry] = Field(default=[{'provider': 'cron'}], validate_default=True)
    """The timer: a scout sweep every ``interval`` seconds."""
    interval: float = Field(default=0, ge=0)
    """The number of seconds between two scout sweeps, counted from the start of the last one. The first sweep counts
    from the registration of the campaign. 0 (the default) turns the scout off."""
    seeds: int = Field(default=3, ge=1)
    """The maximum number of seeds one scout sweep may file. The other techniques it finds go to the library."""
    writes: list[str] = [
        'seeds via idea seed (at most the count under Context)',
        'techniques (the library), by INSERT',
        'one comments(finding) on the campaign',
    ]
    """What a scout sweep may write, as its brief states it."""
    timeout: float = 900
    """The number of seconds one sweep may run before it is stopped."""

    def context(self, sweep: Sweep) -> list[str]:
        """The scout's lines: its seed limit, its search window, the watch list and the technique library."""
        s, campaign_id, role, now = sweep.s, sweep.campaign.id, sweep.agent_session.kind, sweep.now
        library = s.scalars(
            select(Technique)
            .where(Technique.campaign_id == campaign_id)
            .order_by(Technique.id.desc())
            .limit(LIBRARY_SHOWN)
        ).all()
        total = count(s, select(Technique).where(Technique.campaign_id == campaign_id))
        watched = watch_list(s, campaign_id, role)
        stamp = now.strftime('%Y-%m-%d-%H%M')
        older = f' ({total - len(library)} older: query `techniques`)' if total > len(library) else ''
        since = window(s, campaign_id, role, sweep.agent_session.id, now).strftime('%Y-%m-%d')
        return [
            '- This is a scout sweep: follow the scout brief at the top; develop no seed and add no idea.',
            f'- Seeds you may file: at most {self.seeds}; every other technique worth keeping goes to the library.',
            f'- Search window: since {since} (posts, repos, papers).',
            f'- Write the problem statement to `./workspace/scout/{stamp}-problem.md` before your first search.',
            '',
            '## Scout: watch list (sources whose seeds research developed; query each directly)',
            *([w.line() for w in watched] or ['- Empty: no scout seed was developed yet.']),
            '',
            f'## Scout: technique library ({total} techniques, newest first{older})',
            *(
                [f'- technique#{t.id} {cell(t.title)}: {cell(t.mechanism, MECHANISM_CHARS)} — {t.url}' for t in library]
                or ['- Empty.']
            ),
        ]
