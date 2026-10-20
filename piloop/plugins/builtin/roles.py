"""The roles of piloop itself, as providers of the ``piloop.roles`` group: the defaults of ``piloop.config.Roles``."""

from piloop.config import ResearchRole, ScoutRole, WatchdogRole, WorkerRole

WORKER = WorkerRole()
WATCHDOG = WatchdogRole()
RESEARCH = ResearchRole()
SCOUT = ScoutRole()
