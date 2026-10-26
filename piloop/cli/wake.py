"""The ``piloop wake`` command: print what a new orchestrator session needs, from this home's database."""

from typing import Annotated

import cyclopts

from piloop.cli import common, local
from piloop.db.models import utcnow
from piloop.reports.wake import orchestrator_overview

WAKE_BUDGET = 2000
"""The tokens that `piloop wake` prints by default."""


def wake(
    *,
    budget: Annotated[
        int, cyclopts.Parameter(help='Tokens to print, about 4 characters each. 0 prints nothing.')
    ] = WAKE_BUDGET,
) -> None:
    """Print what an orchestrator session needs first, in a fixed budget.

    The latest journal lines, the open inbox, the head of each active campaign and the current lessons on their repos.
    Each section says how to read what did not fit. Run it at the start of every orchestrator session.
    """
    with local.session() as s:
        text = orchestrator_overview(s, budget, utcnow())
    if text:
        common.console.print(text, markup=False, highlight=False, soft_wrap=True)
