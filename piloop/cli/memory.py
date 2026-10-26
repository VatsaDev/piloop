"""The ``piloop journal`` command, a client of the API: append a line to the orchestrator's journal."""

from typing import Annotated

import cyclopts
from pydantic import ValidationError

from piloop.api.client import Client
from piloop.api.contract import JOURNAL_LINE_BYTES, JournalBody
from piloop.cli import common
from piloop.cli.client import daemon_http


def journal(line: Annotated[str, cyclopts.Parameter(help=f'At most {JOURNAL_LINE_BYTES} bytes.')], /) -> None:
    """Add one line to the orchestrator's journal: a decision, a plan or a result, with its ids.

    The journal is how a new orchestrator session picks up the work: `piloop wake` prints the latest lines first.
    """
    try:
        body = JournalBody(line=line)
    except ValidationError as e:
        raise SystemExit(e.errors()[0]['msg']) from e
    with Client.connect(daemon_http()) as client:
        common.console.print(f'journal#{client.journal(body).journal}', markup=False)
