"""The ``piloop eval`` commands, clients of the API: list the evaluation queue, and cancel the evaluation of a candidate."""

from datetime import UTC, datetime
from typing import Annotated, assert_never

import cyclopts
from rich.table import Table
from rich.text import Text

from piloop.cli import common
from piloop.cli.client import daemon_http
from piloop.ids import CandidateId
from piloop.scoring.api import CancelledBy, EvaluationState, RunningSince, ScoringClient, WaitingSince
from piloop.text import age, cell

app = cyclopts.App(name='eval', help='List or cancel the evaluations that the daemon runs or that wait for a slot.')


@app.command(name='queue')
def eval_queue() -> None:
    """List the running evaluations, then the waiting ones in the order in which they get a slot.

    The last line counts them. A free slot of the executor goes to the campaign with the fewest running evaluations,
    and in that campaign to its head first, then to the oldest request. A requested remeasure shows as waiting at once.
    """
    with ScoringClient.connect(daemon_http()) as client:
        evaluations = client.evaluations().evaluations
    now = datetime.now(UTC)
    table = Table('candidate', 'campaign', 'state', 'summary', box=None)
    for e in evaluations:
        table.add_row(f'candidate#{e.candidate}', e.campaign, _state(e.state, now), Text(cell(e.summary or '')))
    waiting = sum(isinstance(e.state, WaitingSince) for e in evaluations)
    if evaluations:
        common.console.print(table)
    common.console.print(
        f'{len(evaluations) - waiting} running, {waiting} waiting; `piloop eval cancel <candidate>`',
        markup=False,
    )


def _state(state: EvaluationState, now: datetime) -> str:
    match state:
        case RunningSince(since=since):
            return f'running {age(since, now)}'
        case WaitingSince(since=since):
            return f'waiting {age(since, now)}'
        case CancelledBy(by=by):
            return f'cancelled by {by}'
        case _:
            assert_never(state)


@app.command(name='cancel')
def eval_cancel(
    candidate_id: Annotated[CandidateId, cyclopts.Parameter(help='The candidate whose evaluation to cancel.')],
    /,
) -> None:
    """Cancel the evaluation of a candidate: a waiting one never starts, and the daemon stops a running one.

    It also cancels a requested remeasure. The candidate gets a crashed measurement that names you, at once, so a run's
    candidate is judged a `crash`, and a daemon restart does not run it again. It is evaluated again only when someone
    asks (`piloop candidate remeasure`). A baseline that never measured pauses its campaign, as a failed one does.
    """
    with ScoringClient.connect(daemon_http()) as client:
        client.cancel_evaluation(candidate_id)
    common.console.print(f'candidate#{candidate_id}: its evaluation is cancelled', markup=False)
