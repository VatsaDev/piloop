from piloop.api.views import Lines, Rendered
from piloop.plugins.section.contract import Rendering, Section, Surface


class Incidents(Section):
    """The anomalies that the agent who reads got to investigate. Nothing for every other reader."""

    SHOWN_IN = frozenset({Surface.SWEEP})
    ORDER = 20

    def render(self, r: Rendering) -> Rendered | None:
        return Rendered(title='Incidents to investigate', blocks=(Lines(lines=r.given),)) if r.given else None
