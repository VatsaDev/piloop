"""What the incident triggers share: an incident, the ones that an agent of the campaign got already, and the fire."""

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select

from piloop.campaign.anomaly import AnomalyReported
from piloop.db.events import in_order
from piloop.db.models import AgentSession, Event
from piloop.ids import TargetKind, TargetRef
from piloop.plugins.trigger.contract import TRIGGERS, Fire, Pass, Trigger
from piloop.text import age

DETAIL_CHARS = 160
"""How much of an error an incident's line shows."""


@dataclass(frozen=True)
class Incident:
    """A problem that a trigger found: the row it is about, since when, and what happened."""

    subject: TargetRef
    """The row: ``run#36``, ``agent_session#12``."""
    at: datetime
    what: str
    """What happened, after the row: ``failed``, ``dead at startup``."""
    detail: str
    """The evidence in one line."""

    def line(self, now: datetime) -> str:
        """The incident as a brief and the inbox show it: ``- run#36 failed, 1h ago: <detail>``."""
        return f'- {self.subject} {self.what}, {age(self.at, now)} ago: {self.detail}'


def given(p: Pass, trigger: str) -> list[tuple[Event, AnomalyReported]]:
    """The incidents of ``trigger`` that agents of the campaign got, oldest first: each is investigated once."""
    rows = in_order(
        p.s,
        AnomalyReported,
        Event.target_kind == TargetKind.AGENT_SESSION,
        Event.target_id.in_(select(AgentSession.id).where(AgentSession.campaign_id == p.campaign.id)),
    )
    return [(e, a) for e, a in rows if a.trigger == trigger]


def investigate(p: Pass, trigger: Trigger, found: list[Incident]) -> Fire | None:
    """Request an agent for the incidents of ``found`` whose row no agent got yet, or None when there are none."""
    known = {a.subject for _, a in given(p, TRIGGERS.name_of(trigger))}
    return report(p, trigger, [i for i in found if i.subject not in known])


def report(p: Pass, trigger: Trigger, new: list[Incident]) -> Fire | None:
    """Request an agent for the incidents of ``new``, oldest first, or None when there are none.

    The fire is alone: it shares its row only with the fires of other incident triggers. The row gets one
    ``anomaly.reported`` event for each incident, which makes its sweep an incident sweep.
    """
    if not new:
        return None
    name = TRIGGERS.name_of(trigger)
    ordered = sorted(new, key=lambda i: i.at)
    events = tuple(AnomalyReported(subject=i.subject, trigger=name, line=i.line(p.now)) for i in ordered)
    return Fire(f'{name}: ' + ', '.join(f'{i.subject} {i.what}' for i in ordered), alone=True, events=events)
