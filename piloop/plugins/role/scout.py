from importlib.resources import files
from typing import ClassVar

from pydantic import Field

from piloop.plugins.role.contract import SweepRole
from piloop.plugins.trigger.contract import TriggerEntry
from piloop.plugins.trigger.cron import CronTrigger


class ScoutRole(SweepRole):
    """``[roles.scout]``: the scout files outside techniques as seeds for the measured losses. It is off by default.

    Every ``interval`` seconds, a scout sweep states the open problems of the campaign from the evidence, and searches
    X, GitHub and arXiv for outside techniques that address them. It files at most ``seeds`` of them as seeds, signed
    ``scout#<agent session id>``, and puts the rest in the technique library (the ``techniques`` table). It never files a
    defined idea. The search tool limits what one sweep spends on xAI (``scripts/xai_search_mcp.py --max-usd``).

    Experimental: the scout.
    """

    PROMPT = files(__package__) / 'prompts' / 'scout.md'
    IDEAS: ClassVar[str] = (
        "- File a seed: `{piloop} idea seed {campaign} '<technique: its mechanism, one line>' --from '<link>; targets"
        " <the measured loss it might reduce, with its size>; <its substance: a number, code, an ablation>'`: research"
        ' develops it into an idea or rejects it with the reason.'
        '\n- File a technique into the library: pipe `INSERT INTO techniques (campaign_id, title, mechanism, url,'
        " created_by) VALUES (<campaign id>, '<technique>', '<mechanism and its substance, one line>', '<link>',"
        " '<your signature>')` into `{piloop} sql -`."
    )

    triggers: list[TriggerEntry] = [CronTrigger()]
    """The timer: a scout sweep every ``interval`` seconds."""
    interval: float = Field(default=0, ge=0)
    """The number of seconds between two scout sweeps, counted from the start of the last one. The first sweep counts
    from the registration of the campaign. 0 (the default) turns the scout off."""
    seeds: int = Field(default=3, ge=1)
    """The maximum number of seeds one scout sweep may file. The other techniques it finds go to the library."""
    writes: list[str] = [
        'seeds via idea seed (at most the count under Context)',
        'techniques (the library), by INSERT',
        'one comments(finding) on the campaign',
    ]
    """What a scout sweep may write, as its brief states it."""
    timeout: float = 900
    """The number of seconds one sweep may run before it is stopped."""
