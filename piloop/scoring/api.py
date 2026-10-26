"""The optimization routes on candidates, measurements and evaluations: their models, and the client that calls them.

They build on core's API (``piloop.api``): its base models, and ``Client`` for the routes of every campaign.
"""

from datetime import datetime
from typing import Annotated, Literal

from pydantic import ConfigDict, Field

from piloop.api.client import Client, segment
from piloop.api.contract import Params, Reasoned
from piloop.api.views import IdeaView, View
from piloop.ids import CandidateId, Cause, MeasurementId, TrackName
from piloop.plugins.verdict_rule.contract import Outcome

REPEATS = 3
"""The pairs of a remeasure that asks for no number: the 95% bound of 3 pairs is near their worst pair."""


class ImportBody(Params):
    """A version of the campaign's repo to register as a reference candidate."""

    version: str
    """A version as the campaign's vcs names one: a revision, a branch, a tag."""
    summary: str | None = None
    """A summary of the candidate. Default: the version's own summary."""


class RemeasureBody(Params):
    """How to measure a candidate again."""

    repeats: int = Field(REPEATS, ge=1)
    """How many pairs of the candidate and its bar."""


class NoteBody(Params):
    """Why a regressed result is worth starting from, and the seeds to pin to it."""

    reason: str
    follow_ups: tuple[str, ...] = ()
    """A seed pinned to the candidate for each, for research to develop."""
    track: TrackName | None = None
    """The track of the follow-ups. Default: the track of the candidate's run, else the first track."""


class VerdictBody(Reasoned):
    """A candidate's verdict, set by hand."""

    verdict: Outcome


class Report(Params):
    """A measurement made outside the daemon, for example by running the evaluate command by hand."""

    model_config = ConfigDict(extra='forbid')

    metrics: dict[str, Annotated[float, Field(strict=True, allow_inf_nan=False)]]
    """Each metric's value, as the evaluate command's ``METRIC`` lines would give it."""
    seconds: Annotated[float, Field(ge=0, allow_inf_nan=False)] | None = None
    """How long the evaluation took."""
    error: str | None = None
    """Why the evaluation failed. A failed measurement may lack metrics."""


class CandidateView(View):
    """A candidate, as the change that wrote it left it."""

    id: CandidateId
    summary: str | None
    label: str
    """The verdict as a person reads it: the verdict, ``pending``, or the origin of a candidate that is never judged."""
    cause: Cause | None
    verdict_reason: str | None


class CandidateAnswer(View):
    """A candidate that a change wrote, read at ``cursor``."""

    cursor: int
    candidate: CandidateView


class Remeasured(CandidateAnswer):
    """A candidate that the daemon measures again."""

    bar: CandidateId | None
    """The bar that it is measured next to. Null: alone."""
    held: str | None
    """What the daemon waits for before it measures, such as a resume. Null: it measures soon."""
    rejudged: bool
    """Whether the daemon judges the candidate again after the measurements. Else its verdict stays."""


class Noted(CandidateAnswer):
    """A regressed candidate kept in view as a lead."""

    seeds: list[IdeaView]
    """The follow-ups: seeds pinned to the candidate."""


class Merge(View):
    """A keep off the head, merged onto it: a candidate that the daemon measures and judges against that head."""

    candidate: CandidateId
    head: CandidateId


class VerdictChanged(CandidateAnswer):
    """A candidate whose verdict was set by hand."""

    was: str
    """The verdict that it had, as ``label`` reads it."""
    merge: Merge | None


class MeasurementAdded(View):
    """A measurement recorded by hand, read at ``cursor``."""

    cursor: int
    measurement: MeasurementId
    rejudged: bool
    """Whether the daemon judges the candidate again (``RejudgeRequested``)."""
    effect: str
    """What happens to the verdict, in one line: when the daemon judges it again, or why the verdict stays."""


class RunningSince(View):
    """An evaluation that has a slot of the executor."""

    kind: Literal['running'] = 'running'
    since: datetime


class WaitingSince(View):
    """An evaluation that waits for a slot, or a remeasure that someone asked for and that is not queued yet."""

    kind: Literal['waiting'] = 'waiting'
    since: datetime
    """When it was asked for."""


class CancelledBy(View):
    """A running evaluation that someone cancelled: the daemon stops its evaluator."""

    kind: Literal['cancelled'] = 'cancelled'
    by: str
    """Who cancelled it, as the actor signs."""


type EvaluationState = Annotated[RunningSince | WaitingSince | CancelledBy, Field(discriminator='kind')]


class QueuedEvaluation(View):
    """One evaluation of the queue."""

    candidate: CandidateId
    campaign: str
    """The name of the candidate's campaign."""
    summary: str | None
    state: EvaluationState


class EvaluationQueue(View):
    """The running evaluations, oldest start first, then the waiting ones in the order in which they get a slot."""

    evaluations: list[QueuedEvaluation]


class ScoringClient(Client):
    """A client of one daemon that also calls the optimization routes on candidates, measurements and evaluations."""

    def import_candidate(self, campaign: str, body: ImportBody) -> CandidateAnswer:
        return self.post(f'/v1/campaigns/{segment(campaign)}/candidates', body, CandidateAnswer)

    def remeasure(self, candidate: CandidateId, body: RemeasureBody) -> Remeasured:
        return self.post(f'/v1/candidates/{candidate}/remeasure', body, Remeasured)

    def note(self, candidate: CandidateId, body: NoteBody) -> Noted:
        return self.post(f'/v1/candidates/{candidate}/note', body, Noted)

    def set_verdict(self, candidate: CandidateId, body: VerdictBody) -> VerdictChanged:
        return self.post(f'/v1/candidates/{candidate}/verdict', body, VerdictChanged)

    def add_measurement(self, candidate: CandidateId, body: Report) -> MeasurementAdded:
        return self.post(f'/v1/candidates/{candidate}/measurements', body, MeasurementAdded)

    def evaluations(self) -> EvaluationQueue:
        return self._get('/v1/evaluations', Params(), EvaluationQueue)

    def cancel_evaluation(self, candidate: CandidateId) -> CandidateAnswer:
        return self.post(f'/v1/candidates/{candidate}/evaluation/cancel', None, CandidateAnswer)
