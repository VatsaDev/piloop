"""Experimental: the context of a scout sweep (``[roles.scout]``), which files outside techniques as seeds.

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

from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.db.models import (
    AgentSession,
    Candidate,
    Event,
    Idea,
    IdeaStatus,
    Run,
    TargetKind,
    Technique,
    Verdict,
    count,
)
from piloop.text import cell

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


def watch_list(s: Session, campaign_id: int, role: str) -> list[Watched]:
    """Return the sources that later sweeps of ``role`` query directly: kept ones first, then the latest payoff first."""
    seeds = s.scalars(select(Idea).where(Idea.campaign_id == campaign_id, Idea.created_by.like(f'{role}#%'))).all()
    developed: dict[int, datetime] = {
        idea_id: at
        for idea_id, at in s.execute(
            select(Event.target_id, Event.at).where(
                Event.kind == 'idea.status',
                Event.target_kind == TargetKind.IDEA,
                Event.target_id.in_([i.id for i in seeds]),
                Event.payload['old'].as_string() == IdeaStatus.SEED.value,
                Event.payload['new'].as_string().in_((IdeaStatus.READY.value, IdeaStatus.PROPOSED.value)),
            )
        )
    }
    keeps = s.execute(
        select(Run.idea_id, Candidate.created_at)
        .join(Candidate, Candidate.run_id == Run.id)
        .where(Run.idea_id.in_(list(developed)), Candidate.verdict == Verdict.KEEP)
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


def window(s: Session, campaign_id: int, role: str, agent_session_id: int, now: datetime) -> datetime:
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


def context(s: Session, campaign_id: int, role: str, agent_session_id: int, seeds: int, now: datetime) -> list[str]:
    """Return the scout's lines of the context block: its seed limit, its window, the watch list and the library."""
    library = s.scalars(
        select(Technique).where(Technique.campaign_id == campaign_id).order_by(Technique.id.desc()).limit(LIBRARY_SHOWN)
    ).all()
    total = count(s, select(Technique).where(Technique.campaign_id == campaign_id))
    watched = watch_list(s, campaign_id, role)
    stamp = now.strftime('%Y-%m-%d-%H%M')
    older = f' ({total - len(library)} older: query `techniques`)' if total > len(library) else ''
    return [
        '- This is a scout sweep: follow the scout brief at the top; develop no seed and add no idea.',
        f'- Seeds you may file: at most {seeds}; every other technique worth keeping goes to the library.',
        f'- Search window: since {window(s, campaign_id, role, agent_session_id, now).strftime("%Y-%m-%d")} (posts, repos, papers).',
        f'- Write the problem statement to `./workspace/scout/{stamp}-problem.md` before your first search.',
        f'- Sweep started at: {now.strftime("%Y-%m-%d %H:%M:%S")} (UTC; the research pack below is as of then).',
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
