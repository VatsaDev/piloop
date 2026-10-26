"""What a candidate's verdict is, and what may change it: the one place that decides who may set which verdict.

``candidates.verdict`` is only a judgement (an ``Outcome``), NULL until the daemon judges the candidate.
``candidates.origin`` says where it comes from, and ``candidates.cause`` why an ``interesting`` verdict is not the
rule's own.
"""

from enum import StrEnum
from typing import Final, Literal

from sqlalchemy import ColumnElement

from piloop.actor import Actor, System
from piloop.db.models import JUDGED, Candidate, Origin
from piloop.ids import Cause
from piloop.plugins.verdict_rule.contract import Outcome
from piloop.scoring.api import CandidateView

REJUDGED: Final = frozenset({Outcome.CRASH, Outcome.DISCARD, Outcome.INTERESTING})
"""The daemon's verdicts that a measurement by hand has judged again. A keep stays: runs started from it as a bar."""
UNMEASURED: Final = frozenset({Origin.RECOVERED, Origin.SKIPPED})
"""The origins of the candidates that the daemon never measures. A commit of the same tree without the mark is
measured and judged like a new one."""
PENDING: Final = 'pending'
"""How a candidate that waits for its verdict shows (``label``)."""


class Refusal(StrEnum):
    """Why a verdict cannot be set by hand."""

    PENDING = 'pending'
    """The daemon judges the candidate now."""
    NOT_JUDGED = 'not_judged'
    """Its origin is never judged: a reference, recovered or skipped candidate."""
    SAME = 'same'
    """It has that verdict already."""
    NO_RESULT = 'no_result'
    """A note needs a measured result that regressed: a discard or an interesting candidate."""


def pending(c: Candidate) -> bool:
    """Whether the candidate waits for the daemon's verdict."""
    return c.verdict is None and c.origin in JUDGED


def pending_clause() -> ColumnElement[bool]:
    """Select the candidates that ``pending`` is true of."""
    return Candidate.verdict.is_(None) & Candidate.origin.in_(JUDGED)


def daemon_owned(c: Candidate) -> bool:
    """Whether the daemon set the verdict. The daemon never overwrites a verdict by anyone else."""
    return c.verdict_by == System.DAEMON.signature


def rejudgeable(c: Candidate) -> bool:
    """Return whether the daemon judges the candidate again after a measurement by hand.

    Only a candidate of a run: one whose verdict is the daemon's and one of ``REJUDGED``, or a commit that the daemon
    never measured (``UNMEASURED``), which the measurement is the first of. Anyone else's verdict stays.
    """
    return c.run_id is not None and ((daemon_owned(c) and c.verdict in REJUDGED) or c.origin in UNMEASURED)


def judgeable(c: Candidate) -> bool:
    """Return whether the daemon may set its verdict on ``c`` now: no keep, and nobody else's verdict.

    That is a candidate without a verdict that is not a reference: a pending one, or a recovered or skipped one that a
    later commit of the same tree has measured. Or it is a verdict of the daemon that a measurement can change
    (``REJUDGED``). A keep stays: runs started from it as their bar, and it may be the head. The daemon checks it in the
    transaction that judges, so another run's keep that landed during a measurement is never judged over.
    """
    if c.verdict_by is not None and not daemon_owned(c):
        return False
    match c.origin, c.verdict:
        case Origin.REFERENCE, _:
            return False
        case _, None:
            return True
        case _:
            return c.verdict in REJUDGED


def label(c: Candidate) -> str:
    """The verdict as a person reads it: the verdict, ``pending``, or the origin of a candidate that is never judged."""
    match c.verdict:
        case None if c.origin in JUDGED:
            return PENDING
        case None:
            return c.origin
        case verdict:
            return verdict


def refusal(c: Candidate, to: Outcome, cause: Literal[Cause.NOTE] | None) -> Refusal | None:
    """Return why ``c``'s verdict cannot be set to ``to`` by hand, or None when it can.

    Only a judgement is overridden. A note (``Cause.NOTE``) turns a measured discard or interesting candidate into an
    ``interesting`` lead, and may be added to one that has a note already.
    """
    match c.verdict, cause:
        case None, _:
            return Refusal.PENDING if c.origin in JUDGED else Refusal.NOT_JUDGED
        case Outcome.DISCARD | Outcome.INTERESTING, Cause.NOTE:
            return None
        case _, Cause.NOTE:
            return Refusal.NO_RESULT
        case verdict, None if verdict is to:
            return Refusal.SAME
        case _:
            return None


def override(c: Candidate, to: Outcome, *, cause: Literal[Cause.NOTE] | None, reason: str, by: Actor) -> None:
    """Set ``by``'s verdict on ``c``, which ``refusal`` allows. A note keeps the reason that it notes after its own."""
    match cause:
        case Cause.NOTE if c.verdict_reason:
            reason = f'{reason}; {c.verdict_reason}'
        case _:
            pass
    write(c, to, cause=cause, reason=reason, by=by)


def write(c: Candidate, to: Outcome, *, cause: Cause | None, reason: str, by: Actor) -> None:
    """Set the verdict with its cause, reason and author. A recovered or skipped candidate that is judged is a run's."""
    c.verdict, c.cause, c.verdict_reason, c.verdict_by = to, cause, reason, by.signature
    if c.origin in UNMEASURED:
        c.origin = Origin.RUN


def candidate_view(c: Candidate) -> CandidateView:
    """``c`` as the API answers it."""
    return CandidateView(id=c.id, summary=c.summary, label=label(c), cause=c.cause, verdict_reason=c.verdict_reason)
