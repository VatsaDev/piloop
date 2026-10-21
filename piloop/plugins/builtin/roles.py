"""The roles of piloop itself, as providers of the ``piloop.roles`` group: the defaults of ``piloop.config.Roles``."""

from piloop.config import ImplementerRole, ResearchRole, ScoutRole, WatchdogRole

IMPLEMENTER = ImplementerRole()
WATCHDOG = WatchdogRole()
RESEARCH = ResearchRole()
SCOUT = ScoutRole()
