from piloop.api.views import Figure, Figures, Rendered, Table
from piloop.campaign.stop import gather
from piloop.plugins.fact_source.facts import FUNNEL
from piloop.plugins.section.contract import Rendering, Section, Surface


class RunFunnel(Section):
    """The run funnel: ideas → attempts → reached the evaluator → measured → keeps, and how the unmeasured attempts ended.

    It counts the attempts that ended and whose commits are judged. Many attempts that end before the evaluator point
    at a gate in the brief that runs cannot pass; many that reach it unmeasured point at the evaluator.
    """

    SHOWN_IN = frozenset({Surface.STATUS, Surface.SWEEP})
    ORDER = 175

    def render(self, r: Rendering) -> Rendered | None:
        funnel = gather(r.s, r.campaign, r.campaign.config, [FUNNEL], r.now).get(FUNNEL)
        if funnel is None:  # a campaign without the optimization source
            return None
        stages = (
            Figure(label='ideas', value=funnel.ideas),
            Figure(label='attempts', value=funnel.attempts),
            Figure(label='reached the evaluator', value=funnel.reached),
            Figure(label='measured', value=funnel.measured),
            Figure(label='keeps', value=funnel.keeps),
        )
        unmeasured = Table(columns=('ended', 'unmeasured attempts'), rows=tuple(funnel.unmeasured.items()))
        return Rendered(title='Funnel', blocks=(Figures(figures=stages), *((unmeasured,) if funnel.unmeasured else ())))
