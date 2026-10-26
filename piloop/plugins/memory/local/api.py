"""The route of ``local``: ``POST /v1/journal``, which appends a line to the orchestrator's journal."""

from fastapi import APIRouter, Depends

from piloop.daemon.api import REFUSALS_DOCUMENTED, Acting, Writing, actor_access, reader
from piloop.db import feed as db_feed
from piloop.plugins.memory.local.tables import Journal
from piloop.plugins.memory.local.wire import JournalBody, Journaled

router = APIRouter(prefix='/v1', dependencies=[Depends(reader)], responses=REFUSALS_DOCUMENTED)
"""The journal's route, beside the API's."""


@router.post('/journal', operation_id='journal', dependencies=[Depends(actor_access)])
def journal(s: Writing, me: Acting, body: JournalBody) -> Journaled:
    """Append a line to the orchestrator's journal, signed by the caller's actor. ``piloop wake`` prints it."""
    entry = Journal(author=me.signature, line=body.line)
    s.add(entry)
    s.commit()
    with db_feed.snapshot(s) as cursor:
        return Journaled(cursor=cursor, journal=entry.id)
