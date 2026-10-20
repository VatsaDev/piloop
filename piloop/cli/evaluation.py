"""The ``piloop eval`` commands: list the evaluation queue, and cancel the evaluation of a candidate."""

from typing import Annotated

import cyclopts
from rich.table import Table
from rich.text import Text

from piloop.cli import common
from piloop.db.models import Campaign, utcnow
from piloop.reports.overview import age
from piloop.scoring import queue
from piloop.text import cell

app = cyclopts.App(name='eval', help='List or cancel the evaluations that the daemon runs or that wait for a slot.')


@app.command(name='queue')
def eval_queue() -> None:
    """List the running evaluations, then the waiting ones in the order in which they get a slot.

    The daemon runs `max_evaluations` evaluations at the same time, across all campaigns. A free slot goes to the
    campaign with the fewest running evaluations, and in that campaign to its head first, then to the oldest request.
    """
    with common.session() as s:
        rows = queue.listing(s)
        now = utcnow()
        table = Table('candidate', 'campaign', 'state', 'summary', box=None)
        for e in rows:
            if e.cancelled_by is not None:
                state = f'cancelled by {e.cancelled_by}'
            elif e.started_at is not None:
                state = f'running {age(e.started_at, now)}'
            else:
                state = f'waiting {age(e.created_at, now)}'
            table.add_row(
                f'candidate#{e.candidate_id}',
                s.get_one(Campaign, e.candidate.campaign_id).name,
                state,
                Text(cell(e.candidate.summary or '')),
            )
        running = sum(e.started_at is not None for e in rows)
    if rows:
        common.console.print(table)
    common.console.print(
        f'{running} running, {len(rows) - running} waiting; {common.settings.max_evaluations} slot(s)'
        ' (max_evaluations); `piloop eval cancel <candidate>`',
        markup=False,
    )


@app.command(name='cancel')
def eval_cancel(
    candidate_id: Annotated[int, cyclopts.Parameter(help='The candidate whose evaluation to cancel.')],
    /,
) -> None:
    """Cancel the evaluation of a candidate: a waiting one never starts, and the daemon stops a running one.

    The candidate gets a crashed measurement that names you, at once, so a run's candidate is judged a `crash`, and a
    daemon restart does not run it again. It is evaluated again only when someone asks (`piloop candidate remeasure`).
    """
    with common.session() as s:
        if not queue.cancel(s, candidate_id, common.settings.actor):
            raise SystemExit(f'candidate#{candidate_id} has no evaluation that waits or runs: see `piloop eval queue`')
        s.commit()
    common.console.print(f'candidate#{candidate_id}: its evaluation is cancelled', markup=False)
