from datetime import timedelta
from typing import ClassVar

from sqlalchemy import select

from piloop.agents.results import StartupAnomaly
from piloop.db.events import in_order
from piloop.db.models import AgentSession, Event
from piloop.ids import TargetKind
from piloop.plugins.trigger.contract import Fire, Pass, Trigger
from piloop.plugins.trigger.kit.incidents import DETAIL_CHARS, Incident, investigate
from piloop.text import cell


class DeadAtStartup(Trigger):
    """An agent of the campaign did not start to work, and no agent got it yet. The agent gets it to investigate.

    An agent of a role with a trigger that investigates (``INVESTIGATES``) is left out: the agent that it would request
    could die the same way, and so on. The startup breaker of the daemon pauses the launches instead.
    """

    INVESTIGATES: ClassVar[bool] = True

    def fire(self, p: Pass) -> Fire | None:
        roles = p.campaign.config.roles.sweeping().items()
        investigators = {name for name, role in roles if any(t.INVESTIGATES for t in role.triggers)}
        events = in_order(
            p.s,
            StartupAnomaly,
            Event.target_kind == TargetKind.AGENT_SESSION,
            Event.target_id.in_(select(AgentSession.id).where(AgentSession.campaign_id == p.campaign.id)),
        )
        found = [
            Incident(
                e.target,
                e.at,
                'dead at startup',
                f'{a.role} on {a.harness} failed to start ({a.seconds}s): {cell(a.error or "no error recorded", DETAIL_CHARS)}',
            )
            for e, a in events
            if a.role not in investigators
        ]
        return investigate(p, self, found)

    def due_in(self, p: Pass) -> timedelta | None:
        return None
