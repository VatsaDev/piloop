from piloop.api.views import Rendered, Table, Value
from piloop.db.models import Candidate, count
from piloop.plugins.section.contract import Rendering, Section, Surface
from piloop.reports.overview import produced
from piloop.scoring.ledger import score, track_of
from piloop.scoring.verdict import label
from piloop.text import age, cell

CANDIDATES_SHOWN = 8


class Candidates(Section):
    """The latest candidates, oldest first, each with its verdict and its primary score against its parent's."""

    SHOWN_IN = frozenset({Surface.STATUS, Surface.SWEEP})
    ORDER = 180

    def render(self, r: Rendering) -> Rendered:
        s, campaign = r.s, r.campaign
        cfg = campaign.config
        evaluator_id = campaign.evaluator_id
        total = count(s, produced(campaign))
        recent = s.scalars(produced(campaign).order_by(Candidate.id.desc()).limit(CANDIDATES_SHOWN)).all()[::-1]
        on = "each on its track's primary" if cfg.multitrack else cfg.all_tracks[0].primary.name
        rows: list[tuple[Value, ...]] = []
        for c in recent:
            track = track_of(cfg, c)
            value = score(c, evaluator_id, track.primary)
            parent = score(c.parent, evaluator_id, track.primary) if c.parent is not None else None
            delta = value - parent if value is not None and parent is not None else None
            rows.append(
                (
                    f'candidate#{c.id}',
                    label(c),
                    *([track.name] if cfg.multitrack else []),
                    value,
                    delta,
                    f'run#{c.run_id}' if c.run_id else 'no run',
                    age(c.created_at, r.now),
                    cell(c.summary or ''),
                )
            )
        columns = ('candidate', 'verdict', *(['track'] if cfg.multitrack else []), 'score', 'vs parent', 'run', 'age')
        table = Table(columns=(*columns, 'summary'), rows=tuple(rows))
        return Rendered(title=f'Recent candidates ({len(recent)} of {total}; {on})', blocks=(table,))
