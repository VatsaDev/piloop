"""The ``piloop wake`` command: print what a new orchestrator session needs, from this home's database."""

from typing import Annotated

import cyclopts

from piloop.cli import common, local
from piloop.cli.common import CampaignRef
from piloop.db.models import utcnow
from piloop.reports.wake import orchestrator_overview

WAKE_BUDGET = 2000
"""The tokens that `piloop wake` prints by default."""


def wake(
    ref: CampaignRef | None = None,
    /,
    *,
    budget: Annotated[
        int, cyclopts.Parameter(help='Tokens to print, about 4 characters each. 0 prints nothing.')
    ] = WAKE_BUDGET,
) -> None:
    """Print what the orchestrator of a campaign needs first, in a fixed budget.

    The latest lines of the campaign's journal, its open inbox, its head and the current lessons on its repo. Each
    section says how to read what did not fit. Without a campaign, it uses the campaign that the current directory is
    in, else the only active campaign, else the whole home. Run it at the start of every orchestrator session.
    """
    with local.session() as s:
        match local.campaigns(s, ref):
            case [one]:
                campaign = one
            case _:
                campaign = None
        text = orchestrator_overview(s, campaign, budget, utcnow())
    if text:
        common.console.print(text, markup=False, highlight=False, soft_wrap=True)
