"""What the journal's route takes and answers, for the route and for ``piloop journal``."""

from typing import Annotated

from pydantic import AfterValidator
from pydantic_core import PydanticCustomError

from piloop.api.contract import Params
from piloop.api.views import View
from piloop.ids import JournalId

JOURNAL_LINE_BYTES = 280
"""The longest journal line, in UTF-8 bytes: one decision, plan or result, with its ids."""


def _journal_line(line: str) -> str:
    """``line`` with each run of whitespace as one space. Refuses one of no byte or of more than ``JOURNAL_LINE_BYTES``."""
    collapsed = ' '.join(line.split())
    if not 0 < (size := len(collapsed.encode())) <= JOURNAL_LINE_BYTES:
        raise PydanticCustomError(
            'journal_line',
            'a journal line is 1 to {most} bytes, not {size}: say less',
            {'most': JOURNAL_LINE_BYTES, 'size': size},
        )
    return collapsed


class JournalBody(Params):
    """A line of the orchestrator's journal: a decision, a plan or a result, with its ids."""

    line: Annotated[str, AfterValidator(_journal_line)]


class Journaled(View):
    """A line appended to the orchestrator's journal, read at ``cursor``."""

    cursor: int
    journal: JournalId
