from typing import assert_never

from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.api.views import Lines, Rendered
from piloop.campaign.stop import attempt
from piloop.config import CampaignConfig
from piloop.db.models import FINISHED_RUNS, Campaign, Candidate, Cause, Run, campaign_runs, count
from piloop.ids import EvaluatorId
from piloop.plugins.section.contract import Rendering, Section, Surface
from piloop.plugins.verdict_rule.contract import Branch, Trunk
from piloop.reports.overview import known, num
from piloop.scoring.ledger import baseline, lineages, score, track_of, trunk_keeps
from piloop.scoring.verdict import label
from piloop.text import age, cell, metric

STEPS_SHOWN = 8
"""How many of the latest trunk keeps, and of the latest notes, it lists."""


class Trajectory(Section):
    """The trunk: the start, the head, the gain of each track between them, and the latest trunk keeps.

    Then come the lineages of keeps when there is more than one, and the latest notes: other places a run can start.
    """

    SHOWN_IN = frozenset({Surface.STATUS, Surface.SWEEP})
    ORDER = 130

    def render(self, r: Rendering) -> Rendered:
        s, campaign = r.s, r.campaign
        cfg = campaign.config
        evaluator_id = campaign.evaluator_id
        steps = trunk_keeps(s, campaign)
        if not steps:
            lines = ["- No trunk keep: the baseline's verdict was overridden and nothing was kept since."]
            return Rendered(title='Trajectory', blocks=(Lines(lines=tuple(lines)),))
        first, last = steps[0], steps[-1]
        gains = []
        for track in cfg.all_tracks:
            a, b = score(first, evaluator_id, track.primary), score(last, evaluator_id, track.primary)
            if a is None or b is None:
                gains.append(f'{track.primary.name} unmeasured')
                continue
            relative = f', {(b - a) / abs(a):+.1%}' if a else ''
            gains.append(f'{track.primary.name} {metric(a)} → {metric(b)} ({metric(b - a, signed=True)}{relative})')
        finished = campaign_runs(campaign.id).where(
            Run.status.in_(FINISHED_RUNS), attempt(), Run.finished_at > known(last)
        )
        lines = [
            f'- Start candidate#{first.id}, head candidate#{last.id}: {"; ".join(gains)}; {len(steps) - 1} trunk keeps.',
            f'- Last trunk keep {age(known(last), r.now)} ago; {count(s, finished)} runs finished since.',
            f'- Trunk keeps (latest {STEPS_SHOWN}; when first measured, the gain on its track over the head before it):',
        ]
        before = dict(zip([c.id for c in steps[1:]], steps, strict=False))
        for c in steps[-STEPS_SHOWN:]:
            track = track_of(cfg, c)
            value, previous = score(c, evaluator_id, track.primary), before.get(c.id)
            prior = score(previous, evaluator_id, track.primary) if previous is not None else None
            if previous is None:
                delta = ' (start)'
            else:
                delta = f' ({value - prior:+g})' if value is not None and prior is not None else ''
            on = f' [{track.name}] {track.primary.name}' if cfg.multitrack and previous is not None else ''
            idea = f' idea#{c.run.idea_id}' if c.run is not None else ''
            lines.append(
                f'  - {known(c):%m-%d %H:%M} candidate#{c.id}{on} {num(value)}{delta}{idea}:'
                f' {cell(c.summary or "—", 80)}'
            )
        lines += _branches(s, campaign, cfg)
        return Rendered(title='Trajectory', blocks=(Lines(lines=tuple(lines)),))


def _branches(s: Session, campaign: Campaign, cfg: CampaignConfig) -> list[str]:
    """List the lineages of keeps if there is more than one, then the latest notes."""
    found = lineages(s, campaign)
    evaluator_id = campaign.evaluator_id
    lines = []
    if len(found) > 1:
        lines.append('- Lineages (latest keep of each; a branch keep never moves the head):')
        for line in found:
            match line.lineage:
                case Trunk():
                    root = baseline(s, campaign)
                    start = f'trunk from candidate#{root.id} ({label(root)})'
                case Branch(root=root_id):
                    root = s.get_one(Candidate, root_id)
                    start = f'branch from candidate#{root.id} ({label(root)})'
                case _:
                    assert_never(line.lineage)
            lines.append(
                f'  - {start}: latest candidate#{line.latest.id} {_values(line.latest, cfg, evaluator_id)},'
                f' {line.keeps} kept'
            )
    noted = s.scalars(
        select(Candidate)
        .where(Candidate.campaign_id == campaign.id, Candidate.cause == Cause.NOTE)
        .order_by(Candidate.id.desc())
        .limit(STEPS_SHOWN)
    ).all()
    if noted:
        lines.append('- Notes (regressed, kept in view as leads; pin one with base_candidate_id):')
        lines += [
            f'  - candidate#{c.id} {num(score(c, evaluator_id, track_of(cfg, c).primary))} by {c.verdict_by}:'
            f' {cell(c.verdict_reason or "", 100)}'
            for c in noted
        ]
    return lines


def _values(c: Candidate, cfg: CampaignConfig, evaluator_id: EvaluatorId | None) -> str:
    """Format the primary of each track on ``c``: ``accuracy 0.91, seconds 3.1``. One track shows the score only."""
    if not cfg.multitrack:
        return num(score(c, evaluator_id, cfg.all_tracks[0].primary))
    return ', '.join(f'{t.primary.name} {num(score(c, evaluator_id, t.primary))}' for t in cfg.all_tracks)
