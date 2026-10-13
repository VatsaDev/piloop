"""The human's inbox: every comment with a ``priority`` that nobody has acknowledged yet.

A comment addressed to the human has a priority from 0 (urgent) to 4 (someday). Examples are decisions that the
orchestrator took for the human, escalations and findings. Other comments have no priority and form the log.
Nothing waits for the human. An item is a record to review, and ``piloop inbox ack`` only marks it as seen.

The orchestrator is any harness that runs the ``piloop-operate`` skill. It learns of new items from the events
table: ``piloop inbox wait`` blocks until ``news`` returns something, so the orchestrator loops on one command.

The inbox also lists seeds (``seeds``): hunches filed for research that research has not yet developed or rejected.
They need no action from the human. They are listed so that the human can see what goes to research.
"""

import re
from collections.abc import Collection, Sequence

from sqlalchemy import ColumnElement, ScalarSelect, case, select
from sqlalchemy.orm import Mapped, Session, aliased

from piloop.db.events import ANOMALY_EVENT
from piloop.db.models import (
    Candidate,
    Comment,
    CommentKind,
    Event,
    Idea,
    IdeaStatus,
    Measurement,
    Project,
    ProjectStatus,
    Run,
    TargetKind,
    Worker,
    utcnow,
)
from piloop.scoring.ledger import SKIPPED
from piloop.text import cell

CITATION = re.compile(r'comment#(\d+)')
"""How a comment refers to another comment in its body. ``thread`` follows these references in both directions."""
COMMENT_EVENTS = frozenset(f'comment.{kind}' for kind in CommentKind)
"""The events that a new comment writes.

An escalation writes two events: one from the persistent trigger and one from the connection's trigger."""


def items(
    s: Session, *, project_id: int | None = None, kinds: Collection[CommentKind] = (), acked: bool = False
) -> list[tuple[Comment, Project | None]]:
    """Return the inbox items with their projects, most urgent first, then oldest first.

    Only unacknowledged items are returned, unless ``acked`` is True. ``project_id`` and ``kinds`` filter the items.
    An item whose target has no project (a reply to a reply) appears only when ``project_id`` is None.
    """
    owner = project_of(Comment.target_kind, Comment.target_id)
    query = (
        select(Comment, Project)
        .outerjoin(Project, Project.id == owner)
        .where(Comment.priority.is_not(None))
        .order_by(Comment.priority, Comment.id)
    )
    if not acked:
        query = query.where(Comment.acked_at.is_(None))
    if project_id is not None:
        query = query.where(owner == project_id)
    if kinds:
        query = query.where(Comment.kind.in_(kinds))
    return [(c, p) for c, p in s.execute(query)]


def seeds(s: Session, *, project_id: int | None = None) -> list[tuple[Idea, Project]]:
    """Return the seeds that wait for research, oldest first, with their projects. ``project_id`` filters them."""
    query = select(Idea, Project).join(Project).where(Idea.status == IdeaStatus.SEED).order_by(Idea.id)
    if project_id is not None:
        query = query.where(Idea.project_id == project_id)
    return [(i, p) for i, p in s.execute(query)]


def seed_line(idea: Idea, project: Project) -> str:
    """Format one seed as the inbox prints it: who filed it, its title and its provenance."""
    return (
        f'seed idea#{idea.id} ({project.name}) by {idea.created_by}: {cell(idea.title)} — {cell(idea.provenance or "")}'
    )


def news(s: Session, after: int, *, project_id: int | None = None, me: str) -> tuple[int, list[str]]:
    """Return the events after event#``after`` that the orchestrator must look at, one line each, and the last id.

    The lines report:

    - new unacknowledged inbox items not written by ``me``. The orchestrator's own decisions are for the human.
    - seeds that someone else filed.
    - projects that the daemon stopped.
    - anomalies that the daemon gave to a watchdog.
    - verdicts on measured candidates. Discards of skipped commits are not reported.

    ``project_id`` filters all of them. The returned id is the cursor for the next call, so no event is missed.
    """
    events = s.scalars(select(Event).where(Event.id > after).order_by(Event.id)).all()
    if not events:
        return after, []
    fresh = {e.target_id for e in events if e.kind in COMMENT_EVENTS}
    lines = [
        f'comment#{c.id} {c.kind} p{c.priority} on {c.target_kind}#{c.target_id}'
        f' ({project.name if project else "—"}) by {c.author}: {cell(c.body)}'
        for c, project in items(s, project_id=project_id)
        if c.id in fresh and c.author != me
    ]
    for e in events:
        if e.kind == 'project.status' and e.payload.get('new') == ProjectStatus.STOPPED:
            project = s.get(Project, e.target_id)
            if project is not None and project_id in (None, project.id):
                lines.append(f'project {project.name} stopped: {e.payload.get("stopped_reason")}')
        elif e.kind == ANOMALY_EVENT:
            worker = s.get(Worker, e.target_id)
            if worker is not None and project_id in (None, worker.project_id):
                lines.append(f'anomaly, watchdog worker#{worker.id} requested: {e.payload.get("anomaly")}')
        elif e.kind == 'idea.created' and e.actor != me:
            idea = s.get(Idea, e.target_id)
            if idea is not None and idea.status is IdeaStatus.SEED and project_id in (None, idea.project_id):
                lines.append(seed_line(idea, idea.project))
        elif e.kind == 'candidate.judged' and e.payload.get('reason') != SKIPPED:
            candidate = s.get(Candidate, e.target_id)
            if candidate is not None and project_id in (None, candidate.project_id):
                lines.append(
                    f'candidate#{candidate.id} {e.payload.get("verdict")}: {cell(str(e.payload.get("reason")))}'
                )
    return events[-1].id, lines


def ack(s: Session, ids: Sequence[int], actor: str) -> list[str]:
    """Acknowledge the inbox items ``ids`` as ``actor`` and commit. Return one line per id that says what happened.

    An id that is not an inbox item, or that is already acknowledged, is reported and not changed.
    """
    now = utcnow()
    lines = []
    for comment_id in ids:
        comment = s.get(Comment, comment_id)
        if comment is None or comment.priority is None:
            lines.append(f'comment#{comment_id} is not in the inbox')
        elif comment.acked_at is not None:
            lines.append(f'comment#{comment_id} was acknowledged already, by {comment.acked_by}')
        else:
            comment.acked_at, comment.acked_by = now, actor
            lines.append(f'comment#{comment_id} acknowledged')
    s.commit()
    return lines


def reply(s: Session, comment_id: int, body: str, *, author: str, priority: int) -> Comment:
    """Answer inbox item ``comment_id`` as ``author`` with a ``decision`` on the root of its thread, and commit.

    The root is the first comment up the reply chain that is not a reply. A reply to a reply has no project, because
    ``project_of`` resolves only one level, so ``piloop inbox wait <project>`` would not show it. The priority makes
    the answer an inbox item, so ``news`` reports it to every orchestrator except its author until it is acknowledged.
    """
    root = s.get_one(Comment, comment_id)
    while root.target_kind is TargetKind.COMMENT and (parent := s.get(Comment, root.target_id)) is not None:
        root = parent
    answer = Comment(
        target_kind=TargetKind.COMMENT,
        target_id=root.id,
        author=author,
        kind=CommentKind.DECISION,
        priority=priority,
        body=body.strip(),
    )
    s.add(answer)
    s.commit()
    return answer


def thread(s: Session, comment: Comment) -> list[Comment]:
    """Return the comments linked to an item, in order: those it cites, those that cite it, and its replies.

    A citation is ``comment#<id>`` in the body, for example ``Re: comment#12`` in a decision. Other comments on the
    same target are not part of the thread. A project collects every research finding, and the answer would be
    lost among them.
    """
    ref = f'comment#{comment.id}'
    cited = Comment.id.in_(int(n) for n in CITATION.findall(comment.body))
    citing = Comment.body.op('GLOB', is_comparison=True)(f'*{ref}') | Comment.body.op('GLOB', is_comparison=True)(
        f'*{ref}[^0-9]*'
    )
    replies = (Comment.target_kind == TargetKind.COMMENT) & (Comment.target_id == comment.id)
    return list(
        s.scalars(select(Comment).where(cited | citing | replies, Comment.id != comment.id).order_by(Comment.id))
    )


def project_of(target_kind: Mapped[TargetKind], target_id: Mapped[int]) -> ColumnElement[int | None]:
    """Return the project that the target of a comment or event belongs to.

    A ``comment`` target, such as a reply or a ``comment.escalation`` event, is resolved one level through that
    comment's target. A reply to a reply has no project.
    """
    return case(
        (target_kind == TargetKind.COMMENT, _owner_of_comment(target_id)),
        *_owner_branches(target_kind, target_id),
    )


def _owner_of_comment(comment_id: Mapped[int]) -> ColumnElement[int | None]:
    parent = aliased(Comment)
    return (
        select(case(*_owner_branches(parent.target_kind, parent.target_id)))
        .where(parent.id == comment_id)
        .scalar_subquery()
    )


def _owner_branches(
    target_kind: Mapped[TargetKind], target_id: Mapped[int]
) -> list[tuple[ColumnElement[bool], Mapped[int] | ScalarSelect[int]]]:
    return [
        (target_kind == TargetKind.PROJECT, target_id),
        (target_kind == TargetKind.IDEA, select(Idea.project_id).where(Idea.id == target_id).scalar_subquery()),
        (
            target_kind == TargetKind.RUN,
            select(Idea.project_id).join(Run.idea).where(Run.id == target_id).scalar_subquery(),
        ),
        (
            target_kind == TargetKind.CANDIDATE,
            select(Candidate.project_id).where(Candidate.id == target_id).scalar_subquery(),
        ),
        (
            target_kind == TargetKind.MEASUREMENT,
            select(Candidate.project_id)
            .join(Measurement.candidate)
            .where(Measurement.id == target_id)
            .scalar_subquery(),
        ),
        (target_kind == TargetKind.WORKER, select(Worker.project_id).where(Worker.id == target_id).scalar_subquery()),
    ]
