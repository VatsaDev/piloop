"""The ``piloop journal`` command, a client of the API: append a line to the journal of a campaign."""

from typing import Annotated

import cyclopts
from pydantic import ValidationError

from piloop.api.client import Client, segment
from piloop.api.contract import Selection, defaulted
from piloop.cli import common
from piloop.cli.client import cwd, daemon_http
from piloop.plugins.memory.local.wire import JOURNAL_LINE_BYTES, JournalBody, Journaled


def journal(
    line: Annotated[str, cyclopts.Parameter(help=f'At most {JOURNAL_LINE_BYTES} bytes.')],
    /,
    *,
    campaign: Annotated[
        str | None,
        cyclopts.Parameter(
            help='The campaign name, or a path inside its repo. Else the campaign of the current directory, else the'
            ' only active campaign.'
        ),
    ] = None,
) -> None:
    """Add one line to the journal of the campaign: a decision, a plan or a result, with its ids.

    The journal is how a new orchestrator session picks up the work: `piloop wake` prints the latest lines first.
    """
    try:
        body = JournalBody(line=line)
    except ValidationError as e:
        raise SystemExit(e.errors()[0]['msg']) from e
    with Client.connect(daemon_http()) as client:
        match defaulted(client.status(Selection(campaign=campaign, cwd=cwd())).campaigns):
            case [one]:
                path = f'/v1/campaigns/{segment(one.name)}/journal'
            case []:
                raise SystemExit('the home has no campaign: `piloop init <repo>` registers one')
            case several:
                raise SystemExit(f'name the campaign with --campaign, one of: {", ".join(c.name for c in several)}')
        common.console.print(f'journal#{client.post(path, body, Journaled).journal}', markup=False)
