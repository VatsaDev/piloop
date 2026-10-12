"""Ideas: their links, the actions on them, where they come from and what each source produced.

Links (``idea_links``) are *after* and *supersedes*. The rules are on ``Idea`` in the models. This module holds
their SQL and their text. The scheduler uses it to find the ``ready`` ideas that can run and to reject superseded
ideas. The status and history renderers use it to show the links in one cell. Add the rule for a new kind of link
here.

``review`` and ``reprioritise`` do to an idea what ``prompts/sql_procedures.md`` tells an agent to do by SQL.
``yields`` measures each source by its keeps, not by the number of ideas it inserted. The overview and the research
pack show it.

Seeds: every role except the worker can file a hunch with pointers (``seed``). Research develops a seed into a
defined idea (``develop``) or rejects it. ``defined_by`` returns the agents that added a defined idea to the project.
This is how piloop measures the work of a research sweep.
"""

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass

from sqlalchemy import ColumnElement, case, distinct, exists, func, select
from sqlalchemy.orm import Session, aliased

from piloop.db.models import (
    Candidate,
    Comment,
    CommentKind,
    Event,
    Idea,
    IdeaLink,
    IdeaStatus,
    LinkKind,
    Run,
    TargetKind,
    Verdict,
    WorkerKind,
)
from piloop.db.triggers import DAEMON_ACTOR

NEVER_RAN = (IdeaStatus.SEED, IdeaStatus.PROPOSED, IdeaStatus.READY)
"""The statuses in which the daemon can reject a superseded idea. The idea must also have no run.

A failed attempt puts an idea back to ``ready`` if it has attempts left. The daemon does not cancel such an idea."""


def unblocked() -> ColumnElement[bool]:
    """Select the ideas whose ``after`` links all point to ``done`` ideas.

    A prerequisite in any other status, including ``rejected`` and ``parked``, still blocks. The orchestrator or a
    human then decides if the dependent idea is worth running without it.
    """
    return ~_blocked_by(IdeaStatus.DONE)


def runnable() -> ColumnElement[bool]:
    """Select the ideas that are ``ready`` and unblocked. The queue trigger takes only these."""
    return (Idea.status == IdeaStatus.READY) & unblocked()


def waiting() -> ColumnElement[bool]:
    """Select the ``ready`` ideas that are blocked only by ``scheduled`` prerequisites.

    They become runnable when those prerequisites finish. ``low_water`` counts them as backlog, together with the
    runnable ideas, because no research sweep is needed before they run. Without this, a long run with many ideas
    ``after`` it starts a sweep on every tick.
    """
    return (Idea.status == IdeaStatus.READY) & ~unblocked() & ~_blocked_by(IdeaStatus.DONE, IdeaStatus.SCHEDULED)


def _blocked_by(*settled: IdeaStatus) -> ColumnElement[bool]:
    """Select the ideas that have an ``after`` link to an idea whose status is not in ``settled``."""
    link, target = aliased(IdeaLink), aliased(Idea)
    return exists().where(
        link.idea_id == Idea.id,
        link.kind == LinkKind.AFTER,
        target.id == link.target_id,
        target.status.not_in(settled),
    )


def supersede(s: Session, project_id: int) -> dict[int, int]:
    """Reject each idea of the project that a newer idea supersedes and that never ran.

    The daemon runs this on each tick. An idea never ran if it has no ``runs`` row and is ``seed``, ``proposed`` or
    ``ready``. A superseded idea that ran keeps its status and its candidates, even if a failed attempt put it back
    to ``ready``. The new idea is judged against those candidates. Research cannot change the status of other ideas.
    It inserts a superseding idea instead, and this function rejects the old one.

    The change-log trigger logs the status change (``idea.status``, actor ``daemon``). That event does not say which
    idea superseded it, so a ``note`` on the rejected idea says it.

    Returns:
        ``{rejected idea id: superseding idea id}``.
    """
    link = aliased(IdeaLink)
    superseded = (
        select(Idea)
        .where(
            Idea.project_id == project_id,
            Idea.status.in_(NEVER_RAN),
            ~exists().where(Run.idea_id == Idea.id),
            exists().where(link.target_id == Idea.id, link.kind == LinkKind.SUPERSEDES),
        )
        .order_by(Idea.id)
    )
    rejected: dict[int, int] = {}
    for idea in s.scalars(superseded):
        newest = select(func.max(IdeaLink.idea_id)).where(
            IdeaLink.target_id == idea.id, IdeaLink.kind == LinkKind.SUPERSEDES
        )
        by = s.scalar(newest)
        assert by is not None  # The EXISTS clause above found at least one such link.
        idea.status = IdeaStatus.REJECTED
        s.add(
            Comment(
                target_kind=TargetKind.IDEA,
                target_id=idea.id,
                author=DAEMON_ACTOR,
                kind=CommentKind.NOTE,
                body=f'rejected: superseded by idea#{by}. It does not run, even if idea#{by} has not run yet.',
            )
        )
        rejected[idea.id] = by
    return rejected


def links(idea: Idea, superseded_by: dict[int, list[int]]) -> str:
    """Format the idea's links as one short cell, or ``—`` if there are none.

    The parts are ``blocked by idea#3 (rejected), idea#5 (ready)``, ``after idea#1`` (prerequisites that are done),
    ``supersedes idea#2, idea#4`` and ``superseded by idea#7``, joined with ``; ``. ``superseded_by`` maps an idea id
    to the ideas that supersede it. The caller computes it once for all the rows it renders.
    """
    parts: list[str] = []
    blocking = [t for t in idea.after if t.status is not IdeaStatus.DONE]
    done = [t for t in idea.after if t.status is IdeaStatus.DONE]
    if blocking:
        parts.append('blocked by ' + ', '.join(f'idea#{t.id} ({t.status})' for t in blocking))
    if done:
        parts.append('after ' + _refs(t.id for t in done))
    if idea.supersedes:
        parts.append('supersedes ' + _refs(t.id for t in idea.supersedes))
    if idea.id in superseded_by:
        parts.append('superseded by ' + _refs(superseded_by[idea.id]))
    return '; '.join(parts) or '—'


def _refs(ids: Iterable[int]) -> str:
    return ', '.join(f'idea#{i}' for i in ids)


def superseded_by(s: Session, idea_ids: list[int]) -> dict[int, list[int]]:
    """Map each of ``idea_ids`` that newer ideas supersede to the ids of those ideas, oldest first."""
    rows = s.execute(
        select(IdeaLink.target_id, IdeaLink.idea_id)
        .where(IdeaLink.target_id.in_(idea_ids), IdeaLink.kind == LinkKind.SUPERSEDES)
        .order_by(IdeaLink.idea_id)
    ).all()
    out: dict[int, list[int]] = {}
    for old, new in rows:
        out.setdefault(old, []).append(new)
    return out


PRIORITIES = range(5)
"""From 0 (urgent) to 4 (someday)."""


def review(s: Session, idea_id: int, status: IdeaStatus, reason: str, *, author: str) -> Idea:
    """Approve (``ready``), park or reject the idea, and add a ``note`` that says why. Commits once."""
    idea = s.get_one(Idea, idea_id)
    s.add(
        Comment(
            target_kind=TargetKind.IDEA,
            target_id=idea_id,
            author=author,
            kind=CommentKind.NOTE,
            body=f'{idea.status} → {status}: {reason.strip()}',
        )
    )
    idea.status = status
    s.commit()
    return idea


def reprioritise(s: Session, idea_id: int, by: int) -> Idea:
    """Change the idea's priority by ``by`` and commit. A negative ``by`` is more urgent. The result stays in 0 to 4."""
    idea = s.get_one(Idea, idea_id)
    idea.priority = min(max(idea.priority + by, PRIORITIES.start), PRIORITIES.stop - 1)
    s.commit()
    return idea


SEEDING = frozenset({WorkerKind.WATCHDOG.value, 'scout'})
"""Sources that file only seeds. A watchdog sees one incident, and a scout sees one outside technique.

Their hunches go to research, which reads the whole campaign history before it gives one to a worker (``develop``)."""


def source(created_by: str) -> str:
    """Return where an idea came from: its signature without the agent id (``research#12`` → ``research``)."""
    return created_by.partition('#')[0]


def seeds_only(actor: str) -> bool:
    """Return True if ``actor`` is a watchdog or a scout (``SEEDING``). These file seeds, never defined ideas."""
    return source(actor) in SEEDING


def files_nothing(actor: str) -> bool:
    """Return True if ``actor`` is a worker. A worker files neither ideas nor seeds.

    A worker writes its hunch in its run's finding, and the research pack shows it. Seeds from workers rarely
    produced a keep, so workers do not file them.
    """
    return source(actor) == WorkerKind.WORKER.value


@dataclass(frozen=True)
class Definition:
    """The parts that turn a seed into an idea that a worker can run. The worker reads each part in its brief."""

    hypothesis: str
    """The mechanism and why it should move the metric."""
    evidence: str
    """What supports it, with ids and paths: runs, findings, candidates, loss tables, documents."""
    check: str
    """The cheapest measurement that can disprove it, and the result that stops the run."""
    metric: str
    """What decides success: the primary metric, or the part of it that the idea targets."""
    effect: str
    """The expected size of the effect in the metric's units, and why that much."""
    plan: str | None = None
    """More steps for the worker, after the check."""

    def missing(self) -> list[str]:
        """Return the names of the required parts that are blank."""
        pieces = ('hypothesis', 'evidence', 'check', 'metric', 'effect')
        return [name for name in pieces if not getattr(self, name).strip()]


class NotASeed(Exception):
    """``develop`` got an idea that is no longer a seed. Another sweep or a human already took it."""


def develop(
    idea: Idea,
    definition: Definition,
    *,
    title: str | None = None,
    family: str | None = None,
    base: int | None = None,
    priority: int | None = None,
    proposed: bool = False,
) -> Idea:
    """Turn the seed ``idea`` into a defined idea. The caller commits.

    The idea becomes ``ready``, or ``proposed`` if a human must approve it. The original hunch and its pointers are
    kept. The definition goes first in the hypothesis. The plan starts with the check, the metric and the expected
    effect, so the worker's brief has all of it. The change log records the connection's actor on the status change
    (``idea.status``). ``defined_by`` uses this to count a sweep's work.
    """
    if idea.status is not IdeaStatus.SEED:
        raise NotASeed(f'idea#{idea.id} is {idea.status}, not a seed: someone took it already')
    if gaps := definition.missing():
        raise ValueError(f'a defined idea needs {", ".join(gaps)}')
    idea.hypothesis = f'{definition.hypothesis.strip()}\n\nSeed ({idea.created_by}): {idea.hypothesis}'
    steps = [
        f'First check, stop if it fails: {definition.check.strip()}',
        f'Success: {definition.metric.strip()}; expected effect: {definition.effect.strip()}',
        *([definition.plan.strip()] if definition.plan and definition.plan.strip() else []),
    ]
    idea.plan = '\n'.join(steps)
    pointers = f'; seed pointers: {idea.provenance}' if idea.provenance else ''
    idea.provenance = f'{definition.evidence.strip()}{pointers}'
    idea.title = title or idea.title
    idea.family = family or idea.family
    idea.base_candidate_id = base if base is not None else idea.base_candidate_id
    idea.priority = priority if priority is not None else idea.priority
    idea.status = IdeaStatus.PROPOSED if proposed else IdeaStatus.READY
    return idea


def defined_by(s: Session, project_id: int) -> set[str]:
    """Return the signatures that added a defined idea to the project, by inserting it or by developing a seed.

    The agent that filed a seed did not define anything. A research sweep that did neither added no ideas, whatever
    else it wrote.
    """
    from_seed = (Event.target_kind == TargetKind.IDEA) & (Event.kind == 'idea.status')
    from_seed &= Event.payload['old'].as_string() == IdeaStatus.SEED.value
    was_seed = exists().where(from_seed, Event.target_id == Idea.id)
    inserted = select(Idea.created_by).where(Idea.project_id == project_id, Idea.status != IdeaStatus.SEED, ~was_seed)
    developed = (
        select(Event.actor)
        .join(Idea, Idea.id == Event.target_id)
        .where(
            from_seed,
            Event.payload['new'].as_string().in_((IdeaStatus.READY.value, IdeaStatus.PROPOSED.value)),
            Idea.project_id == project_id,
        )
    )
    return set(s.scalars(inserted)) | set(s.scalars(developed))


@dataclass(frozen=True)
class Yield:
    """What one source's ideas produced: how many it filed, how many ran, and their candidates."""

    source: str
    ideas: int
    ran: int
    keeps: int
    interesting: int

    def line(self) -> str:
        return f'{self.source} {self.ideas} ideas, {self.ran} ran → {self.keeps} keep, {self.interesting} interesting'


def yields(s: Session, project_id: int) -> list[Yield]:
    """Count, for each source, the ideas, the ideas with a run, and their ``keep`` and ``interesting`` candidates.

    The result is sorted by keeps, then interesting candidates, then ideas, most first. A verdict counts as it is
    now, including overrides.
    """
    verdict = Candidate.verdict
    rows = s.execute(
        select(
            Idea.created_by,
            func.count(distinct(Run.id)),
            func.count(distinct(case((verdict == Verdict.KEEP, Candidate.id)))),
            func.count(distinct(case((verdict == Verdict.INTERESTING, Candidate.id)))),
        )
        .outerjoin(Run, Run.idea_id == Idea.id)
        .outerjoin(Candidate, Candidate.run_id == Run.id)
        .where(Idea.project_id == project_id)
        .group_by(Idea.id)
    )
    tally: defaultdict[str, list[int]] = defaultdict(lambda: [0, 0, 0, 0])
    for created_by, runs, keeps, interesting in rows:
        counts = tally[source(created_by)]
        counts[0] += 1
        counts[1] += runs > 0
        counts[2] += keeps
        counts[3] += interesting
    found = [Yield(name, *counts) for name, counts in tally.items()]
    return sorted(found, key=lambda y: (-y.keeps, -y.interesting, -y.ideas, y.source))
