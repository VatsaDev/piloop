from piloop.api.views import Rendered, Text
from piloop.plugins.section.contract import Rendering, Section, Surface
from piloop.reports.history import history


class History(Section):
    """What was tried: the ideas and their outcomes, the kept lineage, the starting points and the latest findings."""

    SHOWN_IN = frozenset({Surface.RUN})
    ORDER = 920

    def render(self, r: Rendering) -> Rendered:
        return Rendered(title='History', blocks=(Text(text=history(r.s, r.campaign, r.campaign.config).strip()),))
