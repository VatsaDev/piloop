from sqlalchemy import select

from piloop.api.views import Lines, Rendered
from piloop.db.models import Candidate
from piloop.plugins.section.contract import Rendering, Section, Surface
from piloop.reports.overview import minutes
from piloop.scoring.ledger import metrics, track_of
from piloop.text import age

REFS_SHOWN = 8


class SameAsParent(Section):
    """The candidates whose primary score is exactly their parent's: a cost, since the evaluation saw no change.

    When the other metrics are equal too, except a timing metric, the change did not affect what the evaluation sees.
    One candidate gets a line of its own, and several get one line with the latest.
    """

    SHOWN_IN = frozenset({Surface.STATUS, Surface.SWEEP})
    ORDER = 185

    def render(self, r: Rendering) -> Rendered | None:
        campaign = r.campaign
        cfg, evaluator_id = campaign.config, campaign.evaluator_id
        found: list[str] = []
        latest = None
        children = select(Candidate).where(Candidate.campaign_id == campaign.id, Candidate.parent_id.is_not(None))
        for c in r.s.scalars(children.order_by(Candidate.id.desc())):
            assert c.parent is not None
            primary = track_of(cfg, c).primary.name
            mine, theirs = metrics(c, evaluator_id), metrics(c.parent, evaluator_id)
            if primary not in mine or mine[primary] != theirs.get(primary):
                continue
            found.append(f'candidate#{c.id}')
            if latest is None:
                rows = [m for m in c.measurements if m.evaluator_id == evaluator_id and m.error is None]
                others = [name for name in mine if name != primary]
                same = sum(mine[name] == theirs.get(name) for name in others)
                at = min(m.created_at for m in rows)
                latest = (
                    f'{age(at, r.now)} ago: scored what candidate#{c.parent_id} did on {primary} and on {same} of'
                    f' {len(others)} other metrics; {minutes(sum(m.seconds or 0 for m in rows))} of evaluation'
                )
        if latest is None:
            return None
        if len(found) == 1:
            line = f'- {found[0]}, {latest}'
        else:
            refs = ', '.join(found[:REFS_SHOWN]) + (', …' if len(found) > REFS_SHOWN else '')
            line = f'- {len(found)} candidates ({refs}); the latest, {latest}'
        return Rendered(title='Candidates that scored exactly their parent', blocks=(Lines(lines=(line,)),))
