from collections import Counter

from sqlalchemy import select

from piloop.api.views import Lines, Rendered
from piloop.campaign.ideas import runnable
from piloop.db.models import OPEN_RUNS, Idea, Run, campaign_runs
from piloop.plugins.section.contract import Rendering, Section, Surface
from piloop.reports.overview import known, num
from piloop.scoring.ledger import baseline, head, score, track_of, trunk_keeps
from piloop.text import age, metric


class Tracks(Section):
    """One line per track. Nothing with one track.

    A line shows the track's primary at the head against the start and its floor, its trunk keeps, its share of the
    implementer slots against its open runs, and its runnable ideas.
    """

    SHOWN_IN = frozenset({Surface.STATUS, Surface.SWEEP})
    ORDER = 120

    def render(self, r: Rendering) -> Rendered | None:
        s, campaign = r.s, r.campaign
        cfg = campaign.config
        if not cfg.multitrack:
            return None
        evaluator_id = campaign.evaluator_id
        trunk = head(s, campaign)
        first = baseline(s, campaign)
        steps = [c for c in trunk_keeps(s, campaign) if c.run_id is not None]
        open_runs = campaign_runs(campaign.id).where(Run.status.in_(OPEN_RUNS)).with_only_columns(Idea.track)
        running = Counter(s.scalars(open_runs))
        ready = Counter(s.scalars(select(Idea.track).where(Idea.campaign_id == campaign.id, runnable())))
        lines = []
        for track in cfg.all_tracks:
            m = track.primary
            value = score(trunk, evaluator_id, m) if trunk is not None else None
            start = score(first, evaluator_id, m)
            gain = f' ({value - start:+g} on the start)' if value is not None and start is not None else ''
            floor = ''
            if m.floor is not None:
                floor = f'; floor {metric(m.floor)}' + (
                    f', {metric(m.gain(value, m.floor))} to spare' if value is not None else ''
                )
            mine = [c for c in steps if track_of(cfg, c).name == track.name]
            latest = f', the latest candidate#{mine[-1].id} {age(known(mine[-1]), r.now)} ago' if mine else ''
            lines.append(
                f'- {track.name}: {m.name} {num(value)} at the head{gain}{floor}; {len(mine)} trunk keeps{latest};'
                f' share {track.share:g} of {cfg.run_slots} slots, {running[track.name]} running;'
                f' {ready[track.name]} runnable ideas.'
            )
        return Rendered(title='Tracks', blocks=(Lines(lines=tuple(lines)),))
