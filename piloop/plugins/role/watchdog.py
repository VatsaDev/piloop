from importlib.resources import files

from pydantic import Field

from piloop.plugins.role.contract import SweepRole
from piloop.plugins.trigger.contract import TriggerEntry


class WatchdogRole(SweepRole):
    """``[roles.watchdog]``: the watchdog investigates the anomalies that the daemon detects and reviews progress."""

    PROMPT = files(__package__) / 'prompts' / 'watchdog.md'
    INCIDENT_PROMPT = files(__package__) / 'prompts' / 'watchdog_incident.md'
    ROUTINE_PROMPT = files(__package__) / 'prompts' / 'watchdog_progress.md'

    triggers: list[TriggerEntry] = Field(
        default=[{'provider': 'anomaly'}, {'provider': 'cron'}, {'provider': 'stopped'}], validate_default=True
    )
    """An anomaly requests an incident sweep, the timer requests a progress review, and the campaign's stop requests
    the closing review."""
    interval: float = Field(default=1800, ge=0)
    """The number of seconds between two progress reviews, counted from the last review. A sweep that investigates an
    anomaly is not a review. 0 turns the reviews off."""
    writes: list[str] = [
        'comments(escalation, finding, note)',
        'lessons on the campaign once it stopped',
        'seeds via idea seed',
    ]
    """What a watchdog may write, as its brief states it."""
