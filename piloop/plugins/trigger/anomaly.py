from datetime import timedelta
from typing import ClassVar

from piloop.plugins.trigger.contract import Fire, Pass, Trigger


class AnomalyTrigger(Trigger):
    """The daemon detected an incident that no agent of the campaign got yet. The agent gets it to investigate.

    Incidents are the incident-class anomalies of ``piloop.reports.overview``. The row gets the new anomalies, one ``anomaly.reported`` event each. For the watchdog, that makes it an incident
    sweep.
    """

    INVESTIGATES: ClassVar[bool] = True

    def fire(self, p: Pass) -> Fire | None:
        incidents = p.incidents()
        if not incidents:
            return None
        events = tuple(i.event for i in incidents)
        return Fire('anomaly: ' + ', '.join(i.name for i in incidents), alone=True, events=events)

    def due_in(self, p: Pass) -> timedelta | None:
        return None
