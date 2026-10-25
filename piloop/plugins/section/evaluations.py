from statistics import median

from sqlalchemy import select

from piloop.api.views import Figure, Figures, Rendered
from piloop.db.models import Candidate, Measurement, count
from piloop.plugins.section.contract import Rendering, Section, Surface
from piloop.scoring.verdict import pending_clause

EVALS_IN_TREND = 5
"""How many of the first and of the latest measurements the trend of the evaluation time compares."""


class Evaluations(Section):
    """The evaluations under the current evaluator: how many crashed, their minutes, and the candidates without a verdict.

    The minutes of the latest evaluations against the first show whether evaluations got slower.
    """

    SHOWN_IN = frozenset({Surface.STATUS, Surface.SWEEP})
    ORDER = 170

    def render(self, r: Rendering) -> Rendered:
        s, campaign = r.s, r.campaign
        mine = select(Candidate.id).where(Candidate.campaign_id == campaign.id)
        rows = s.scalars(
            select(Measurement)
            .where(Measurement.candidate_id.in_(mine), Measurement.evaluator_id == campaign.evaluator_id)
            .order_by(Measurement.id)
        ).all()
        seconds = [m.seconds for m in rows if m.seconds is not None]
        figures = [
            Figure(label='evaluations', value=len(rows)),
            Figure(label='crashed', value=sum(m.error is not None for m in rows)),
        ]
        if seconds:
            figures += [
                Figure(label='median', value=round(median(seconds) / 60, 1), unit='min'),
                Figure(
                    label=f'first {EVALS_IN_TREND}', value=round(median(seconds[:EVALS_IN_TREND]) / 60, 1), unit='min'
                ),
                Figure(
                    label=f'latest {EVALS_IN_TREND}', value=round(median(seconds[-EVALS_IN_TREND:]) / 60, 1), unit='min'
                ),
                Figure(label='in all', value=round(sum(seconds) / 60, 1), unit='min'),
            ]
        waiting = count(s, select(Candidate).where(Candidate.campaign_id == campaign.id, pending_clause()))
        figures.append(Figure(label='waiting for a verdict', value=waiting, unit='candidates'))
        return Rendered(title='Evaluations', blocks=(Figures(figures=tuple(figures)),))
