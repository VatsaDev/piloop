from piloop.api.views import Lines, Rendered
from piloop.campaign.stop import limits
from piloop.config import Metric
from piloop.plugins.section.contract import Rendering, Section, Surface
from piloop.plugins.stop_rule.contract import Fact
from piloop.reports.overview import progress
from piloop.text import metric


class Goal(Section):
    """The goal: the primary metric of each track, the other metrics, the state, and the progress against the stop rule."""

    SHOWN_IN = frozenset({Surface.STATUS, Surface.SWEEP})
    ORDER = 110

    def render(self, r: Rendering) -> Rendered:
        campaign = r.campaign
        cfg = campaign.config
        primaries = {t.primary.name for t in cfg.all_tracks}
        others = ', '.join(f'{m.name} ({m.direction}, {m.role})' for m in cfg.metrics if m.name not in primaries)
        also = f'; also measured: {others}' if others else ''
        state = campaign.status + (', paused' if campaign.paused is not None else '')  # the headline has the reasons
        open_ended = '' if limits(cfg).keys() - {Fact.SPEND} else ' (no limit stops it: open-ended)'
        if cfg.multitrack:
            tracks = '; '.join(f'{t.name} ({_metric(t.primary)}, share {t.share:g})' for t in cfg.all_tracks)
            objective = f'- Tracks, each a primary its keeps improve while holding the others: {tracks}{also}'
        else:
            primary = cfg.all_tracks[0].primary
            objective = f'- Primary metric: {primary.name}, {primary.direction} is better{also}'
        lines = (
            objective,
            f'- Campaign: {state}. Progress: {progress(r.s, campaign, cfg, r.now)}{open_ended}.',
            "- The goal in words is the campaign's brief.md: weigh the metric against it.",
        )
        return Rendered(title='Goal', blocks=(Lines(lines=lines),))


def _metric(m: Metric) -> str:
    """Format a primary metric as a guardrail of another track: ``accuracy, higher, margin 0.01, floor 0.8``."""
    margin = f', margin {metric(m.margin)}' if m.margin is not None else ''
    floor = f', floor {metric(m.floor)}' if m.floor is not None else ''
    return f'{m.name}, {m.direction}{margin}{floor}'
