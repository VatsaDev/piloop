"""The API's routes: FastAPI path operations under ``/v1``, each calling the function that owns its answer.

``router`` is the API, and ``with_api`` puts it on a server with what its routes read with. The routes share their
lookups as dependencies (``Selected``, ``Narrowed``) over one session per request. The OpenAPI document of the router
(``openapi``) is ``docs/reference/openapi.json``.
"""

import os
import time
from collections.abc import Generator
from dataclasses import dataclass
from http import HTTPStatus
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, Query, Request
from fastapi.concurrency import run_in_threadpool
from sqlalchemy.orm import Session

from piloop.api.contract import Access, EventsQuery, InboxQuery, Located, Problem, Selection
from piloop.api.views import Batch, CampaignOverview, DaemonState, Inbox, Overview, ProblemDetail, Status
from piloop.campaign.campaigns import UnknownCampaign, named, selected, workspace_dir
from piloop.db import feed as db_feed
from piloop.db.models import Campaign, utcnow
from piloop.db.session import Database
from piloop.ids import CampaignId
from piloop.net import Refusal
from piloop.net.server import App, authorized, changed
from piloop.plugins.limits import Origin, live_keys
from piloop.plugins.section.contract import Rendering, rendered
from piloop.reports import feed
from piloop.settings import Settings


@dataclass(frozen=True)
class Handlers:
    """What the routes read with: the daemon's database and settings, and the daemon itself."""

    db: Database
    cfg: Settings
    daemon: DaemonState


def handlers(request: Request) -> Handlers:
    """The ``Handlers`` that ``with_api`` put on the server's state."""
    found = request.app.state.handlers
    assert isinstance(found, Handlers)  # with_api sets them before the server serves
    return found


Api = Annotated[Handlers, Depends(handlers)]


def session(api: Api) -> Generator[Session]:
    """One session of the daemon's database for the request."""
    with api.db.session() as s:
        yield s


Reading = Annotated[Session, Depends(session)]


def campaigns_selected(s: Reading, query: Annotated[Selection, Query()]) -> list[Campaign]:
    """The campaigns that the query selects. Refuses a campaign that it names and the home does not have."""
    try:
        return selected(s, query.campaign, _cwd(query))
    except UnknownCampaign as e:
        raise Refusal(HTTPStatus.NOT_FOUND, Problem.UNKNOWN_CAMPAIGN, str(e)) from e


def campaign_narrowed(s: Reading, query: Annotated[Located, Query()]) -> CampaignId | None:
    """The campaign that the query narrows to, or None for every campaign. Refuses one that the home does not have."""
    if query.campaign is None:
        return None
    try:
        return named(s, query.campaign, _cwd(query)).id
    except UnknownCampaign as e:
        raise Refusal(HTTPStatus.NOT_FOUND, Problem.UNKNOWN_CAMPAIGN, str(e)) from e


Selected = Annotated[list[Campaign], Depends(campaigns_selected)]
Narrowed = Annotated[CampaignId | None, Depends(campaign_narrowed)]

router = APIRouter(
    prefix='/v1',
    dependencies=[Depends(authorized(Access.READ))],
    responses={HTTPStatus.NOT_FOUND: {'model': ProblemDetail}, HTTPStatus.CONFLICT: {'model': ProblemDetail}},
)
"""The API. Every route reads; ``Refusal`` answers 404 or 409 with a ``ProblemDetail``."""


@router.get('/status', operation_id='status')
def status(api: Api, s: Reading, campaigns: Selected) -> Status:
    """The main facts of each campaign, at a cursor."""
    with db_feed.snapshot(s) as cursor:
        return Status(cursor=cursor, daemon=api.daemon, campaigns=feed.status_view(s, campaigns))


@router.get('/overview', operation_id='overview')
def overview(api: Api, s: Reading, campaigns: Selected) -> Overview:
    """The sections of each campaign's ``[status] sections``, and the live keys that config.toml or a limit sets."""
    now = utcnow()
    return Overview(
        daemon=api.daemon,
        limits=live_keys(s, api.cfg.providers, (Origin.CONFIG, Origin.LIMIT)),
        campaigns=[
            CampaignOverview(
                id=c.id,
                name=c.name,
                path=c.path,
                parent=feed.parent_of(s, c),
                sections=rendered(c.config.status.shown(), Rendering(s, now, c, workspace_dir(api.cfg.home, c.id))),
            )
            for c in campaigns
        ],
    )


@router.get('/inbox', operation_id='inbox')
def inbox(s: Reading, campaign: Narrowed, query: Annotated[InboxQuery, Query()]) -> Inbox:
    """The inbox items and the seeds, at a cursor."""
    with db_feed.snapshot(s) as cursor:
        return feed.inbox_view(s, cursor, campaign_id=campaign, kinds=query.kind, acked=query.acked)


@router.get('/events', operation_id='events')
async def events(request: Request, api: Api, campaign: Narrowed, query: Annotated[EventsQuery, Query()]) -> Batch:
    """The events after a cursor. With wait, it answers once an event matches, or at the end of the wait with none.

    Each read runs in the thread pool, in a session of its own. Between two reads the route awaits the database's
    next wake and holds no thread. Each read starts at the cursor that the last one moved to, past what the filters
    left out.
    """
    deadline, seen = time.monotonic() + query.wait, 0
    batch = await run_in_threadpool(_events, api.db, query, campaign, query.after)
    while not (batch.events or batch.more) and (left := deadline - time.monotonic()) > 0:
        if (woke := await changed(request, api.db.changed, seen, left)) is None:
            break
        seen = woke
        batch = await run_in_threadpool(_events, api.db, query, campaign, batch.next)
    return batch


def _events(db: Database, query: EventsQuery, campaign: CampaignId | None, after: int) -> Batch:
    with db.session() as s:
        try:
            return feed.read(s, after, campaign_id=campaign, kinds=query.kind, limit=query.limit)
        except db_feed.CursorAhead as e:
            raise Refusal(HTTPStatus.CONFLICT, Problem.CURSOR_AHEAD, str(e)) from e


def with_api(server: App, api: Handlers) -> App:
    """``server`` with the API's routes, which read with ``api``."""
    server.include_router(router)
    server.state.handlers = api
    return server


def daemon_state(started: str, listen: str | None) -> DaemonState:
    """This process, as the daemon that answers."""
    return DaemonState(pid=os.getpid(), started=started, listen=listen)


def openapi() -> dict[str, object]:
    """The OpenAPI document of ``router``: what ``GET /v1/openapi.json`` answers, and ``docs/reference/openapi.json``."""
    described = FastAPI(title='piloop', version='1')
    described.include_router(router)
    return described.openapi()


def _cwd(query: Located) -> Path | None:
    return None if query.cwd is None else Path(query.cwd)
