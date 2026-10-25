from collections import Counter
from datetime import timedelta
from typing import ClassVar

from pydantic import Field
from sqlalchemy import exists

from piloop.campaign.stop import attempt
from piloop.db.models import FINISHED_RUNS, Candidate, Measurement, Run, campaign_runs
from piloop.ids import TargetKind, TargetRef
from piloop.plugins.trigger.contract import TRIGGERS, Fire, Pass, Trigger
from piloop.plugins.trigger.kit.incidents import Incident, given, report


class LeakingFunnel(Trigger):
    """Fewer than ``measured`` of the last ``window`` attempts under the current evaluator reached a measurement.

    A gate that most runs cannot pass shows on the first day, not after days: the agent gets it to investigate. It
    fires once for each evaluator. An attempt that is still open does not count.
    """

    INVESTIGATES: ClassVar[bool] = True

    window: int = Field(default=10, ge=1)
    """How many of the latest finished attempts it looks at."""
    measured: int = Field(default=5, ge=1)
    """It fires when fewer of them than this reached a measurement under the current evaluator."""

    def fire(self, p: Pass) -> Fire | None:
        evaluator = p.campaign.evaluator
        if evaluator is None or any(e.at >= evaluator.created_at for e, _ in given(p, TRIGGERS.name_of(self))):
            return None
        reached = exists().where(
            Candidate.run_id == Run.id,
            Measurement.candidate_id == Candidate.id,
            Measurement.evaluator_id == evaluator.id,
            Measurement.error.is_(None),
        )
        finished = campaign_runs(p.campaign.id).where(
            attempt(), Run.status.in_(FINISHED_RUNS), Run.finished_at >= evaluator.created_at
        )
        rows = p.s.execute(
            finished.with_only_columns(Run.id, Run.end, Run.finished_at, reached.label('reached'))
            .order_by(Run.finished_at.desc(), Run.id.desc())
            .limit(self.window)
        ).all()
        hits = sum(row.reached for row in rows)
        if len(rows) < self.window or hits >= self.measured:
            return None
        ends = ', '.join(f'{n} {end}' for end, n in Counter(row.end for row in rows if not row.reached).items())
        detail = (
            f'{hits} of the last {self.window} attempts were measured under evaluator#{evaluator.id}; the others'
            f' ended: {ends}'
        )
        incident = Incident(TargetRef(TargetKind.CAMPAIGN, p.campaign.id), rows[0].finished_at, 'leaks', detail)
        return report(p, self, [incident])

    def due_in(self, p: Pass) -> timedelta | None:
        return None
