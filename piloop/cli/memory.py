"""The orchestrator's memory: ``piloop journal`` appends a line, ``piloop wake`` prints what a new session needs."""

from typing import Annotated

import cyclopts

from piloop.cli import common
from piloop.db.models import JOURNAL_LINE_BYTES, Journal, utcnow
from piloop.reports.wake import orchestrator_overview

WAKE_BUDGET = 2000
"""The tokens that `piloop wake` prints by default."""


def journal(line: Annotated[str, cyclopts.Parameter(help=f'At most {JOURNAL_LINE_BYTES} bytes.')], /) -> None:
    """Add one line to the orchestrator's journal: a decision, a plan or a result, with its ids.

    The journal is how a new orchestrator session picks up the work: `piloop wake` prints the latest lines first.
    """
    line = ' '.join(line.split())
    if not 0 < len(line.encode()) <= JOURNAL_LINE_BYTES:
        raise SystemExit(f'a journal line is 1 to {JOURNAL_LINE_BYTES} bytes, not {len(line.encode())}: say less')
    with common.session() as s:
        entry = Journal(author=common.settings.actor, line=line)
        s.add(entry)
        s.commit()
        common.console.print(f'journal#{entry.id}', markup=False)


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
    with common.session() as s:
        text = orchestrator_overview(s, budget, utcnow())
    if text:
        common.console.print(text, markup=False, highlight=False, soft_wrap=True)
