"""The evaluation queue (``evaluations``): the evaluations that the daemon runs now or next, one row each.

The daemon runs at most ``max_evaluations`` evaluations at the same time, across all campaigns. A free slot goes to the
campaign with the fewest running evaluations, and in that campaign to its head first, then to the oldest request. So a
campaign with many requests never holds back the requests of another, and inside a campaign each request waits only
for the ones before it. The queue is in the database, so ``piloop eval`` reads and cancels it without the daemon.

A remeasure that someone asked for (``ledger.request_remeasure``) enters the queue only when the daemon gets to it, one
per campaign at a time. Until then ``requested`` lists it, so ``piloop eval queue`` shows it from the moment it is asked.
"""

from collections import Counter
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from piloop.db.events import emit
from piloop.db.models import Campaign, Candidate, Evaluation, Event, Measurement, TargetKind, utcnow
from piloop.scoring import ledger
from piloop.scoring.ledger import head

CANCELLED = 'evaluation cancelled by {actor}'
"""The error of the crashed measurement that ``cancel`` records."""


def request(s: Session, candidate_id: int, evaluator_id: int) -> tuple[Evaluation, bool]:
    """Queue an evaluation of the candidate under the evaluator, or find the one that waits or runs. The caller commits.

    Returns ``(evaluation, created)``. The requests for one candidate and evaluator share one evaluation, so a tree is
    evaluated once.
    """
    found = s.scalar(
        select(Evaluation).where(Evaluation.candidate_id == candidate_id, Evaluation.evaluator_id == evaluator_id)
    )
    if found is not None:
        return found, False
    evaluation = Evaluation(candidate_id=candidate_id, evaluator_id=evaluator_id)
    s.add(evaluation)
    s.flush()
    return evaluation, True


def waiting(s: Session) -> list[Evaluation]:
    """Return the evaluations that wait for a slot, in the order in which they get one if none ends meanwhile."""
    rows = list(s.scalars(select(Evaluation).where(Evaluation.started_at.is_(None)).order_by(Evaluation.id)))
    campaign = {e.id: e.candidate.campaign_id for e in rows}
    running = Counter(
        s.scalars(
            select(Candidate.campaign_id)
            .join(Evaluation, Evaluation.candidate_id == Candidate.id)
            .where(Evaluation.started_at.is_not(None))
        )
    )
    heads = {
        p: trunk.id if (trunk := head(s, s.get_one(Campaign, p))) is not None else None for p in set(campaign.values())
    }
    order: list[Evaluation] = []
    while rows:
        nxt = min(rows, key=lambda e: (running[campaign[e.id]], e.candidate_id != heads[campaign[e.id]], e.id))
        order.append(nxt)
        rows.remove(nxt)
        running[campaign[nxt.id]] += 1
    return order


def start(s: Session, evaluation: Evaluation, capacity: int) -> bool:
    """Start the waiting ``evaluation`` if a slot is free and it is the next one. Return whether it started.

    The caller commits.
    """
    running = s.scalar(select(func.count()).select_from(Evaluation).where(Evaluation.started_at.is_not(None))) or 0
    if running >= capacity or waiting(s)[0].id != evaluation.id:
        return False
    evaluation.started_at = utcnow()
    return True


def listing(s: Session) -> list[Evaluation]:
    """Return the running evaluations, oldest start first, then the waiting ones in the order of ``waiting``."""
    running = select(Evaluation).where(Evaluation.started_at.is_not(None)).order_by(Evaluation.started_at)
    return [*s.scalars(running), *waiting(s)]


def requested(s: Session) -> list[tuple[Candidate, datetime]]:
    """Return each remeasure that someone asked for and that has no evaluation in the queue yet, with when it was asked.

    In campaign order, then oldest request first (``ledger.requested``). An evaluation that someone cancelled does not
    answer a request that came after the cancel, so the request is listed while that evaluation stops.
    """
    queued = set(
        s.execute(
            select(Evaluation.candidate_id, Evaluation.evaluator_id).where(Evaluation.cancelled_by.is_(None))
        ).all()
    )
    kind = (Event.kind == ledger.REMEASURE_REQUESTED, Event.target_kind == TargetKind.CANDIDATE)
    asked = dict(s.execute(select(Event.target_id, func.max(Event.at)).where(*kind).group_by(Event.target_id)).all())
    return [
        (candidate, asked[candidate.id])
        for campaign in s.scalars(select(Campaign).order_by(Campaign.id))
        for candidate in ledger.requested(s, campaign)
        if (candidate.id, campaign.evaluator_id or 0) not in queued
    ]


def cancel(s: Session, candidate_id: int, actor: str) -> int:
    """Cancel what the candidate waits for: its evaluations that wait or run, or its remeasure that is not queued yet.

    Signed by ``actor``. Return how many it cancelled. The caller commits.

    Each records its crashed measurement now (``CANCELLED``), so the candidate is not evaluated again unless someone
    asks, even if the daemon restarts first. That measurement also answers a remeasure request (``requested``). A
    waiting evaluation leaves the queue. A running one gets ``cancelled_by``, and the daemon stops its evaluator and
    records nothing more.
    """
    found = s.scalars(
        select(Evaluation).where(Evaluation.candidate_id == candidate_id, Evaluation.cancelled_by.is_(None))
    ).all()
    evaluators = [e.evaluator_id for e in found]
    if not found and (asked := [c for c, _ in requested(s) if c.id == candidate_id]):
        evaluator_id = s.get_one(Campaign, asked[0].campaign_id).evaluator_id
        evaluators += [evaluator_id] if evaluator_id is not None else []
    for evaluator_id in evaluators:
        error = CANCELLED.format(actor=actor)
        measurement = Measurement(candidate=s.get_one(Candidate, candidate_id), evaluator_id=evaluator_id, error=error)
        s.add(measurement)
        s.flush()
        payload: dict[str, object] = {'candidate': candidate_id, 'metrics': {}, 'error': error, 'run': None}
        emit(s, 'measurement.recorded', TargetKind.MEASUREMENT, measurement.id, payload=payload, actor=actor)
    for evaluation in found:
        if evaluation.started_at is None:
            s.delete(evaluation)
        else:
            evaluation.cancelled_by = actor
    return len(evaluators)
