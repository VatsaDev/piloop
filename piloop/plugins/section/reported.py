from sqlalchemy import select

from piloop.api.views import Lines, Rendered
from piloop.campaign.anomaly import AnomalyReported
from piloop.db.events import in_order
from piloop.db.models import AgentSession, Event
from piloop.ids import TargetKind
from piloop.plugins.section.contract import Rendering, Section, Surface
from piloop.text import age

REPORTED_SHOWN = 12


class Reported(Section):
    """The latest incidents that the triggers gave to agents, newest first, each with the agent that got it."""

    SHOWN_IN = frozenset({Surface.STATUS, Surface.SWEEP})
    ORDER = 190

    def render(self, r: Rendering) -> Rendered:
        mine = select(AgentSession.id).where(AgentSession.campaign_id == r.campaign.id)
        rows = in_order(r.s, AnomalyReported, Event.target_kind == TargetKind.AGENT_SESSION, Event.target_id.in_(mine))
        latest = rows[::-1][:REPORTED_SHOWN]
        lines = [f'- {a.subject} ({a.trigger}), given to {e.target} {age(e.at, r.now)} ago' for e, a in latest]
        lines = lines or ['- None.']
        if (older := len(rows) - len(latest)) > 0:
            lines.append(f'- … and {older} older incidents.')
        return Rendered(title='Incidents given to agents', blocks=(Lines(lines=tuple(lines)),))
