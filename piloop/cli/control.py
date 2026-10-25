"""The commands that steer a campaign as a whole, clients of the API: ``pause``, ``resume`` and ``campaign stop``."""

from typing import Annotated

import cyclopts

from piloop.api.contract import PauseBody, StopBody
from piloop.cli import common
from piloop.cli.client import api, campaign_name
from piloop.cli.common import CampaignRef
from piloop.ids import Ending


def pause(
    ref: CampaignRef,
    /,
    *,
    reason: Annotated[str, cyclopts.Parameter(help='Why, with ids: what the pause waits for.')],
    evals: Annotated[
        bool,
        cyclopts.Parameter(help='Hold the measurements that the daemon starts too: after a new evaluator, or asked.'),
    ] = False,
) -> None:
    """Pause a campaign: nothing new launches, and nothing is escalated. `piloop resume` ends the pause.

    The daemon queues no run, requests no role and launches no requested row, so research files no ideas. A sweep that
    runs already files its ideas as proposed. Running work goes on, and its commits are scored. The daemon still
    measures again what a new evaluator or `piloop candidate remeasure` needs, so the pause can end on a measured head.
    `--evals` holds those measurements too. The pause is in the database: it holds wherever you run the command.
    """
    with api() as client:
        state = client.pause(campaign_name(client, ref), PauseBody(reason=reason, evals=evals)).campaign
    common.console.print(
        f'campaign {state.name} paused ({state.paused}): {state.paused_reason}; running work goes on;'
        f' `piloop resume {state.name}` ends it',
        markup=False,
    )


def resume(ref: CampaignRef, /) -> None:
    """Resume a paused campaign. The daemon queues runs and requests roles again at once."""
    with api() as client:
        state = client.resume(campaign_name(client, ref)).campaign
    common.console.print(f'campaign {state.name} resumed', markup=False)


def stop(
    ref: CampaignRef,
    /,
    *,
    kill: Annotated[
        bool, cyclopts.Parameter(help='End the running implementers at once, without the wrap-up prompt.', negative='')
    ] = False,
    reason: Annotated[str, cyclopts.Parameter(help='Why, for `stopped_reason` and the closing review.')] = '',
) -> None:
    """Stop a campaign. The daemon stops it at once, as when a `[stop]` condition holds.

    This works also while the campaign is paused or held at its `[spend]` limit. A paused campaign gets its closing
    review; a held one as `[spend] closing_review` says. It drops the queued runs and the requested agents, and the
    `stopped` triggers request the closing review. Each
    running implementer is told to save its work and stop, and is ended 10 minutes later. What the implementers saved is scored.
    With `--kill`, the implementers are ended at once, and what they committed is still scored. On a stopped campaign,
    `--kill` ends the implementers that are still winding down. The campaign's `piloop.toml` is not changed.
    """
    body = StopBody(reason=reason, ending=Ending.NOW if kill else Ending.WRAP_UP)
    with api() as client:
        state = client.stop(campaign_name(client, ref), body).campaign
    how = 'ends its implementers now' if kill else 'tells its implementers to save their work and stop'
    common.console.print(f'stop requested for {state.name}: the daemon {how}', markup=False)
