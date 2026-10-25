from piloop.api.views import Lines, Rendered
from piloop.db.models import OPEN_RUNS, Run, campaign_runs, latest_agent_session
from piloop.plugins.section.contract import Rendering, Section, Surface
from piloop.text import age, cell


class Runs(Section):
    """The busy implementer slots, and each open run.

    A run shows its idea, its harness, its state and since when, and when its agent ended if it did.
    """

    SHOWN_IN = frozenset({Surface.STATUS, Surface.SWEEP})
    ORDER = 140

    def render(self, r: Rendering) -> Rendered:
        s, campaign = r.s, r.campaign
        open_runs = s.scalars(campaign_runs(campaign.id).where(Run.status.in_(OPEN_RUNS)).order_by(Run.id)).all()
        lines = [f'- Implementer slots: {len(open_runs)} of {campaign.config.run_slots} busy.']
        for run in open_runs:
            agent_session = latest_agent_session(s, run.id)
            state = f'{run.status}'
            if run.started_at is not None:
                state += f' {age(run.started_at, r.now)}'
            if agent_session is not None and agent_session.ended_at is not None:
                state += f', agent_session#{agent_session.id} ended {age(agent_session.ended_at, r.now)} ago'
            lines.append(f'  - run#{run.id} idea#{run.idea_id} "{cell(run.idea.title, 60)}" ({run.harness}): {state}')
        return Rendered(title='Runs', blocks=(Lines(lines=tuple(lines)),))
