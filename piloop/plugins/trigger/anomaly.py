from typing import ClassVar, Literal

from piloop.db.events import ANOMALY_EVENT
from piloop.plugins.trigger.contract import Fire, Tick, Trigger


class AnomalyTrigger(Trigger):
    """The daemon detected an incident that no agent of the campaign got yet. The agent gets it to investigate.

    Incidents are the incident-class anomalies of ``piloop.reports.overview``. The row gets the new anomalies, one ``anomaly.reported`` event each. For the watchdog, that makes it an incident
    sweep.
    """

    INVESTIGATES: ClassVar[bool] = True
    kind: Literal['anomaly'] = 'anomaly'

    def fire(self, tick: Tick) -> Fire | None:
        incidents = tick.incidents()
        if not incidents:
            return None
        payloads: list[dict[str, object]] = [{'key': i.key, 'anomaly': i.line} for i in incidents]
        events = tuple((ANOMALY_EVENT, payload) for payload in payloads)
        return Fire('anomaly: ' + ', '.join(i.key for i in incidents), alone=True, events=events)
