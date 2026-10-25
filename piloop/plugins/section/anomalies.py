from piloop.api.views import Lines, Rendered
from piloop.campaign.anomaly import AnomalyKind
from piloop.plugins.section.contract import Rendering, Section, Surface
from piloop.reports.overview import anomalies
from piloop.text import age

ANOMALIES_SHOWN = 12
REFS_SHOWN = 8


class Anomalies(Section):
    """The anomalies that the daemon detected.

    Each incident is on its own line, newest first. Each other kind is on one line, because it is a cost.
    """

    SHOWN_IN = frozenset({Surface.STATUS, Surface.SWEEP})
    ORDER = 190

    def render(self, r: Rendering) -> Rendered:
        found = anomalies(r.s, r.campaign, r.campaign.config)
        lines = ['- None.'] if not found else []
        incidents = [a for a in found if a.incident]
        lines += [a.line(r.now) for a in incidents[:ANOMALIES_SHOWN]]
        if (older := len(incidents) - ANOMALIES_SHOWN) > 0:
            lines.append(f'- … and {older} older incidents.')
        for kind in AnomalyKind:
            same = [a for a in found if a.kind is kind and not a.incident]
            if len(same) == 1:
                lines.append(same[0].line(r.now))
            elif same:
                refs = ', '.join(str(a.ref) for a in same[:REFS_SHOWN]) + (', …' if len(same) > REFS_SHOWN else '')
                lines.append(
                    f'- {len(same)} × {kind} ({refs}); the latest, {age(same[0].at, r.now)} ago: {same[0].detail}'
                )
        return Rendered(title='Anomalies the daemon detected', blocks=(Lines(lines=tuple(lines)),))
