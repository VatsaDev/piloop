from piloop.api.views import Lines, Rendered
from piloop.plugins.section.contract import Rendering, Section, Surface
from piloop.reports.overview import latest_events
from piloop.text import age

EVENTS_SHOWN = 5


class Events(Section):
    """The latest events of the campaign, newest first."""

    SHOWN_IN = frozenset({Surface.STATUS, Surface.SWEEP})
    ORDER = 210

    def render(self, r: Rendering) -> Rendered:
        latest = latest_events(r.s, r.campaign, EVENTS_SHOWN)
        lines = [f'- {e.kind} {e.target} {age(e.at, r.now)} ago' for e in latest] or ['- None.']
        return Rendered(title='Recent events', blocks=(Lines(lines=tuple(lines)),))
