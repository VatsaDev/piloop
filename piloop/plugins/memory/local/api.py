"""The route of ``local``: ``POST /v1/campaigns/{campaign}/journal``, a line of the journal of a campaign."""

from fastapi import APIRouter, Depends

from piloop.daemon.api import REFUSALS_DOCUMENTED, Acting, Addressed, Writing, actor_access, reader
from piloop.db import feed as db_feed
from piloop.plugins.memory.local.tables import Journal
from piloop.plugins.memory.local.wire import JournalBody, Journaled

router = APIRouter(prefix='/v1', dependencies=[Depends(reader)], responses=REFUSALS_DOCUMENTED)
"""The journal's route, beside the API's."""


@router.post('/campaigns/{campaign}/journal', operation_id='journal', dependencies=[Depends(actor_access)])
def journal(s: Writing, me: Acting, campaign: Addressed, body: JournalBody) -> Journaled:
    """Append a line to the journal of the campaign, signed by the caller's actor. ``piloop wake`` prints it."""
    entry = Journal(campaign_id=campaign.id, author=me.signature, line=body.line)
    s.add(entry)
    s.commit()
    with db_feed.snapshot(s) as cursor:
        return Journaled(cursor=cursor, journal=entry.id)
