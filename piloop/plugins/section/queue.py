from sqlalchemy import func, select

from piloop.api.views import Lines, Rendered
from piloop.campaign.ideas import links, superseded_by
from piloop.db.models import Idea, IdeaStatus
from piloop.plugins.section.contract import Rendering, Section, Surface
from piloop.reports.overview import queued_ideas
from piloop.text import NONE, age, cell

BLOCKED_SHOWN = 5
QUEUE_SHOWN = 8


class Queue(Section):
    """The ideas by status, what blocks the blocked ones, and the runnable ideas in queue order."""

    SHOWN_IN = frozenset({Surface.STATUS, Surface.SWEEP})
    ORDER = 150

    def render(self, r: Rendering) -> Rendered:
        s, campaign = r.s, r.campaign
        by_status = select(Idea.status, func.count()).where(Idea.campaign_id == campaign.id).group_by(Idea.status)
        counts: dict[IdeaStatus, int] = {status: n for status, n in s.execute(by_status)}
        waiting, ready = queued_ideas(s, campaign)
        blocked = [i for i in waiting if i not in ready]
        waits = [
            f'idea#{i.id} on '
            + ', '.join(f'idea#{t.id} ({t.status})' for t in i.after if t.status is not IdeaStatus.DONE)
            for i in blocked[:BLOCKED_SHOWN]
        ]
        more = f', {len(blocked) - BLOCKED_SHOWN} more' if len(blocked) > BLOCKED_SHOWN else ''
        shown = f' ({"; ".join(waits)}{more})' if waits else ''
        rest = ', '.join(f'{counts.get(status, 0)} {status}' for status in IdeaStatus if status is not IdeaStatus.READY)
        newer = superseded_by(s, [i.id for i in ready[:QUEUE_SHOWN]])
        queue = [
            f'  - idea#{i.id} p{i.priority} "{cell(i.title, 60)}" by {i.created_by}, {age(i.created_at, r.now)} ago'
            + ('' if (linked := links(i, newer)) == NONE else f'; {linked}')
            for i in ready[:QUEUE_SHOWN]
        ]
        lines = (f'- Ideas: {len(ready)} runnable, {len(blocked)} blocked{shown}; {rest}.', *queue)
        return Rendered(title='Queue', blocks=(Lines(lines=lines),))
