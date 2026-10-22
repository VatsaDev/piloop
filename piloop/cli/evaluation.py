"""The ``piloop eval`` commands: list the evaluation queue, and cancel the evaluation of a candidate."""

from typing import Annotated

import cyclopts
from rich.table import Table
from rich.text import Text

from piloop.cli import common
from piloop.db.models import Campaign, Candidate, utcnow
from piloop.plugins.executor.contract import DEFAULT_EXECUTOR, selected
from piloop.reports.overview import age
from piloop.scoring import queue
from piloop.text import cell

app = cyclopts.App(name='eval', help='List or cancel the evaluations that the daemon runs or that wait for a slot.')


@app.command(name='queue')
def eval_queue() -> None:
    """List the running evaluations, then the waiting ones in the order in which they get a slot.

    The last line counts them and the executor's slots (`evaluations` in `[plugins.executor]`, 2 by default). A free
    slot goes to the campaign with the fewest running evaluations, and in that campaign to its head first, then to the
    oldest request. A requested remeasure shows as waiting at once.
    """
    with common.session() as s:
        now = utcnow()
        evaluations = queue.listing(s)
        rows: list[tuple[Candidate, str]] = []
        for e in evaluations:
            if e.cancelled_by is not None:
                state = f'cancelled by {e.cancelled_by}'
            elif e.started_at is not None:
                state = f'running {age(e.started_at, now)}'
            else:
                state = f'waiting {age(e.created_at, now)}'
            rows.append((e.candidate, state))
        rows += [(c, f'waiting {age(asked, now)}') for c, asked in queue.requested(s)]
        table = Table('candidate', 'campaign', 'state', 'summary', box=None)
        for candidate, state in rows:
            campaign = s.get_one(Campaign, candidate.campaign_id).name
            table.add_row(f'candidate#{candidate.id}', campaign, state, Text(cell(candidate.summary or '')))
        running = sum(e.started_at is not None for e in evaluations)
    if rows:
        common.console.print(table)
    common.console.print(
        f'{running} running, {len(rows) - running} waiting; {_slots()}; `piloop eval cancel <candidate>`',
        markup=False,
    )


@app.command(name='cancel')
def eval_cancel(
    candidate_id: Annotated[int, cyclopts.Parameter(help='The candidate whose evaluation to cancel.')],
    /,
) -> None:
    """Cancel the evaluation of a candidate: a waiting one never starts, and the daemon stops a running one.

    It also cancels a requested remeasure. The candidate gets a crashed measurement that names you, at once, so a run's
    candidate is judged a `crash`, and a daemon restart does not run it again. It is evaluated again only when someone
    asks (`piloop candidate remeasure`).
    """
    with common.session() as s:
        if not queue.cancel(s, candidate_id, common.settings.actor):
            raise SystemExit(f'candidate#{candidate_id} has no evaluation that waits or runs: see `piloop eval queue`')
        s.commit()
    common.console.print(f'candidate#{candidate_id}: its evaluation is cancelled', markup=False)


def _slots() -> str:
    """The executor's slots for evaluations, from ``config.toml``: ``2 slot(s) (workers evaluations)``."""
    table = common.settings.plugins.get('executor')
    provider, parameters = selected(table)
    name = table.provider if table is not None else DEFAULT_EXECUTOR
    slots = provider.capacity(parameters)
    return f'no slot limit ({name})' if slots is None else f'{slots} slot(s) ({name} evaluations)'
