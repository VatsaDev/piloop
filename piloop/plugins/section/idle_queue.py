from sqlalchemy import func, select

from piloop.api.views import Lines, Rendered
from piloop.campaign.ideas import runnable
from piloop.campaign.stop import left
from piloop.db.models import (
    LIVE,
    OPEN_RUNS,
    AgentSession,
    AgentSessionState,
    CampaignStatus,
    Idea,
    Run,
    campaign_runs,
    count,
)
from piloop.plugins.fact_source.facts import RUNS
from piloop.plugins.section.contract import Rendering, Section, Surface
from piloop.reports.overview import defining
from piloop.text import age


class IdleQueue(Section):
    """Free implementer slots with no runnable idea, in an active campaign that is not paused: a cost.

    The line says since when (the last time a run started or finished) and which sweeps that define ideas are pending.
    """

    SHOWN_IN = frozenset({Surface.STATUS, Surface.SWEEP})
    ORDER = 155

    def render(self, r: Rendering) -> Rendered | None:
        s, campaign = r.s, r.campaign
        cfg = campaign.config
        if campaign.status is not CampaignStatus.ACTIVE or campaign.paused is not None:
            return None
        free = cfg.run_slots - count(s, campaign_runs(campaign.id).where(Run.status.in_(OPEN_RUNS)))
        if (runs_left := left(s, campaign, cfg).get(RUNS)) is not None:
            free = min(free, runs_left)
        if free <= 0 or count(s, select(Idea).where(Idea.campaign_id == campaign.id, runnable())):
            return None
        moments = [campaign.created_at]
        for column in (Run.created_at, Run.finished_at):
            latest = s.scalar(campaign_runs(campaign.id).with_only_columns(func.max(column)))
            if latest is not None:
                moments.append(latest)
        kinds = defining(cfg)
        pending = s.scalars(
            select(AgentSession)
            .where(
                AgentSession.campaign_id == campaign.id,
                AgentSession.kind.in_(kinds),
                AgentSession.state.in_((AgentSessionState.REQUESTED, *LIVE)),
            )
            .order_by(AgentSession.id)
        ).all()
        sweeps = (
            ', '.join(f'{w.kind}#{w.id} {w.state}' for w in pending)
            or f'no {" or ".join(kinds) or "defining"} sweep pending'
        )
        line = f'- {free} free implementer slots and no runnable idea since {age(max(moments), r.now)} ago; {sweeps}'
        return Rendered(title='Idle queue', blocks=(Lines(lines=(line,)),))
