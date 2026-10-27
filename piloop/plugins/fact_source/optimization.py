"""``optimization``: the built-in fact source of an optimization campaign's research, from piloop's tables."""

from collections import Counter
from collections.abc import Callable
from datetime import datetime
from enum import IntEnum

from sqlalchemy import ColumnElement, Select, case, exists, func, select, true
from sqlalchemy.orm import Session

from piloop.campaign.stop import attempt, kept, runs, runs_without_keep, scored, unsettled
from piloop.db.models import Campaign, Candidate, Idea, Measurement, Run, campaign_runs, count
from piloop.plugins.fact_source.contract import FactSource
from piloop.plugins.fact_source.facts import (
    ATTEMPTS,
    EVALUATIONS,
    FUNNEL,
    HEAD,
    KEEPS,
    METRIC_SUMS,
    OPTIMIZATION,
    RUNS,
    UNSETTLED,
    WITHOUT_KEEP,
    Fact,
    FactValue,
    Funnel,
)
from piloop.plugins.verdict_rule.contract import Outcome
from piloop.scoring.ledger import head, score

type Gatherer = Callable[[Session, Campaign, datetime], FactValue]


def _head(s: Session, campaign: Campaign, now: datetime) -> float | None:
    trunk = head(s, campaign)
    return score(trunk, campaign.evaluator_id, campaign.config.all_tracks[0].primary) if trunk is not None else None


def _runs(s: Session, campaign: Campaign, now: datetime) -> int:
    return runs(s, campaign)


def _unsettled(s: Session, campaign: Campaign, now: datetime) -> int:
    return count(s, campaign_runs(campaign.id).where(attempt(), unsettled()))


def _without_keep(s: Session, campaign: Campaign, now: datetime) -> int:
    return runs_without_keep(s, campaign)


def _keeps(s: Session, campaign: Campaign, now: datetime) -> int:
    return kept(s, campaign)


def _attempts(s: Session, campaign: Campaign, now: datetime) -> int:
    return count(s, campaign_runs(campaign.id).where(attempt()))


def _measurements(campaign: Campaign) -> Select[Measurement]:
    """Select the campaign's measurements, under every evaluator."""
    mine = select(Measurement).join(Candidate, Measurement.candidate_id == Candidate.id)
    return mine.where(Candidate.campaign_id == campaign.id)


def _evaluations(s: Session, campaign: Campaign, now: datetime) -> int:
    return count(s, _measurements(campaign))


def _metric_sums(s: Session, campaign: Campaign, now: datetime) -> dict[str, float]:
    each = func.json_each(Measurement.metrics).table_valued('key', 'value')
    sums = _measurements(campaign).join(each, true()).with_only_columns(each.c.key, func.total(each.c.value))
    return {str(name): total for name, total in s.execute(sums.group_by(each.c.key)).all()}


class Stage(IntEnum):
    """How far an attempt got (``Funnel``). Each stage includes the ones before it."""

    ATTEMPTED = 0
    REACHED = 1
    """It left a candidate that is measured or waits for its verdict (``scored``)."""
    MEASURED = 2
    """One of its candidates has a measurement without an error."""
    KEPT = 3


def _stage() -> ColumnElement[int]:
    """The ``Stage`` of a run that was an attempt."""
    mine = Candidate.run_id == Run.id
    return case(
        (exists().where(mine, Candidate.verdict == Outcome.KEEP), Stage.KEPT.value),
        (
            exists().where(mine, Measurement.candidate_id == Candidate.id, Measurement.error.is_(None)),
            Stage.MEASURED.value,
        ),
        (scored(), Stage.REACHED.value),
        else_=Stage.ATTEMPTED.value,
    )


def _funnel(s: Session, campaign: Campaign, now: datetime) -> Funnel:
    # the attempts that ended and whose commits are judged
    rows = s.execute(
        campaign_runs(campaign.id)
        .where(attempt(), ~unsettled())
        .with_only_columns(_stage().label('stage'), Run.end, func.count(Run.id))
        .group_by('stage', Run.end)
    ).all()
    at_least = {at: sum(n for got, _, n in rows if got >= at) for at in Stage}
    unmeasured: Counter[str] = Counter()
    for got, end, n in rows:
        if got < Stage.MEASURED:
            unmeasured[str(end)] += n
    return Funnel(
        ideas=count(s, select(Idea).where(Idea.campaign_id == campaign.id)),
        attempts=at_least[Stage.ATTEMPTED],
        reached=at_least[Stage.REACHED],
        measured=at_least[Stage.MEASURED],
        keeps=at_least[Stage.KEPT],
        unmeasured=dict(unmeasured.most_common()),
    )


GATHERERS: dict[Fact[FactValue], Gatherer] = {
    HEAD: _head,
    RUNS: _runs,
    UNSETTLED: _unsettled,
    WITHOUT_KEEP: _without_keep,
    KEEPS: _keeps,
    EVALUATIONS: _evaluations,
    METRIC_SUMS: _metric_sums,
    ATTEMPTS: _attempts,
    FUNNEL: _funnel,
}
"""How the source gathers each of its facts (``OPTIMIZATION``)."""


class OptimizationFacts(FactSource):
    """``optimization``: the research of the campaign: its head, runs, keeps, evaluations and funnel.

    It is on by default (``DEFAULT``). A campaign that lists its ``[[facts]]`` lists it too, to keep these facts.
    """

    DEFAULT = True

    def supplies(self) -> tuple[Fact[FactValue], ...]:
        """``OPTIMIZATION``."""
        return OPTIMIZATION

    def gather(
        self, s: Session, campaign: Campaign, wanted: frozenset[Fact[FactValue]], now: datetime
    ) -> dict[Fact[FactValue], FactValue]:
        """Each fact in ``wanted``, from piloop's tables (``GATHERERS``)."""
        return {fact: GATHERERS[fact](s, campaign, now) for fact in wanted}
