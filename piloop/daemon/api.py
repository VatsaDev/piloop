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
from pathlib import Path as FilePath
from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Path, Query, Request, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from pydantic import TypeAdapter, ValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.actor import Actor, Person, System
from piloop.api.contract import (
    Access,
    AckBody,
    BaseBody,
    DevelopBody,
    EventsQuery,
    IdeaBody,
    InboxQuery,
    Located,
    NewsQuery,
    PauseBody,
    Problem,
    Reasoned,
    ReplyBody,
    SeedBody,
    Selection,
    StatusBody,
    StopBody,
    TrackBody,
)
from piloop.api.views import (
    Acked,
    Batch,
    CampaignAnswer,
    CampaignOverview,
    DaemonState,
    Defined,
    IdeaAnswer,
    Inbox,
    News,
    Overview,
    ProblemDetail,
    Replied,
    Status,
    StatusChanged,
    Thread,
    TrackChanged,
)
from piloop.campaign import inbox as inbox_module, ops
from piloop.campaign.campaigns import UnknownCampaign, named, selected, workspace_dir
from piloop.campaign.ideas import Definition, idea_view
from piloop.db import feed as db_feed
from piloop.db.models import Campaign, Comment, Idea, utcnow
from piloop.db.session import Database
from piloop.ids import CampaignId, CommentId, IdeaId, LinkKind
from piloop.net import Refusal
from piloop.net.server import OWNER, App, asked, authorized, changed
from piloop.net.tokens import Caller
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

reader = authorized(Access.READ)
"""The access of every route: one callable, so that FastAPI runs it once per request for each dependency on it."""


def person(caller: Caller) -> Actor:
    """Who ``caller`` is: the person at the terminal on the socket, else the person whom its token or network names."""
    if caller is OWNER:
        return System.HUMAN
    try:
        return Person(name=caller.name)
    except ValidationError as e:
        raise HTTPException(403, f'{caller.name!r} is no name that signs a write: name the caller another way') from e


def acting(request: Request, caller: Annotated[Caller, Depends(reader)]) -> Actor:
    """Who the caller acts as: the actor that it asks for (``asked``), when it may act as it, else itself.

    ``human`` is the caller itself, so a client that a person whom the network names runs signs as that person. The
    caller may ask for itself or the orchestrator, and ``admin`` for any actor: a launched agent calls on the socket
    with its ``PILOOP_ACTOR`` until it has a token of its own. Acting as an actor grants nothing: core checks the
    access of every route as before.
    """
    me, named = person(caller), asked(request)
    if named is None:
        return me
    try:
        actor = ACTOR.validate_python(named)
    except ValidationError as e:
        raise HTTPException(422, f'{named!r} is not an actor, such as orchestrator or implementer#12') from e
    if actor is System.HUMAN:
        return me
    if caller.access is not Access.ADMIN and actor not in (me, System.ORCHESTRATOR):
        raise HTTPException(403, f'{caller.name} has {caller.access} access, which may not act as {actor}')
    return actor


ACTOR: TypeAdapter[Actor] = TypeAdapter(Actor)
Acting = Annotated[Actor, Depends(acting)]
actor_access = authorized(Access.ACT)
"""The access of every route that writes."""


def writing(api: Api, me: Acting) -> Generator[Session]:
    """One session of the daemon's database for the request, whose writes sign as the caller's actor."""
    with api.db.acting(me) as s:
        yield s


Writing = Annotated[Session, Depends(writing)]


def campaign_addressed(s: Writing, campaign: Annotated[str, Path(description="The campaign's name.")]) -> Campaign:
    """The campaign that the path names. Refuses one that the home does not have."""
    if (found := s.scalars(select(Campaign).where(Campaign.name == campaign)).first()) is None:
        raise Refusal(HTTPStatus.NOT_FOUND, Problem.UNKNOWN_CAMPAIGN, str(UnknownCampaign(campaign)))
    return found


def comment_addressed(s: Writing, comment: CommentId) -> Comment:
    """The comment that the path names. Refuses one that the home does not have."""
    if (found := s.get(Comment, comment)) is None:
        raise Refusal(HTTPStatus.NOT_FOUND, Problem.UNKNOWN_COMMENT, f'no comment#{comment}')
    return found


def idea_addressed(s: Writing, idea: IdeaId) -> Idea:
    """The idea that the path names. Refuses one that the home does not have."""
    return _idea(s, idea)


def target_addressed(s: Writing, target: IdeaId) -> Idea:
    """The idea that the path links to. Refuses one that the home does not have."""
    return _idea(s, target)


def _idea(s: Session, idea: IdeaId) -> Idea:
    if (found := s.get(Idea, idea)) is None:
        raise Refusal(HTTPStatus.NOT_FOUND, Problem.UNKNOWN_IDEA, f'no idea#{idea}')
    return found


Addressed = Annotated[Campaign, Depends(campaign_addressed)]
Commented = Annotated[Comment, Depends(comment_addressed)]
AddressedIdea = Annotated[Idea, Depends(idea_addressed)]
Target = Annotated[Idea, Depends(target_addressed)]


router = APIRouter(
    prefix='/v1',
    dependencies=[Depends(reader)],
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


@router.get('/inbox/news', operation_id='news')
async def news(
    request: Request, api: Api, me: Acting, campaign: Narrowed, query: Annotated[NewsQuery, Query()]
) -> News:
    """What needs the orchestrator after a cursor, but what the caller itself wrote. With wait, a long-poll."""
    deadline, seen = time.monotonic() + query.wait, 0
    found = await run_in_threadpool(_news, api.db, campaign, me, query.after)
    while not found.items and (left := deadline - time.monotonic()) > 0:
        if (woke := await changed(request, api.db.changed, seen, left)) is None:
            break
        seen = woke
        found = await run_in_threadpool(_news, api.db, campaign, me, found.next)
    return found


@router.get('/inbox/{comment}', operation_id='thread')
def thread(s: Reading, comment: CommentId) -> Thread:
    """An inbox item in full, and the comments that it cites, that cite it, or that reply to it."""
    if (found := s.get(Comment, comment)) is None:
        raise Refusal(HTTPStatus.NOT_FOUND, Problem.UNKNOWN_COMMENT, f'no comment#{comment}')
    return inbox_module.thread_of(s, found)


@router.post('/campaigns/{campaign}/pause', operation_id='pause', dependencies=[Depends(actor_access)])
def pause(s: Writing, campaign: Addressed, body: PauseBody) -> CampaignAnswer:
    """Pause a campaign: nothing new launches, and nothing is escalated. Running work goes on."""
    ops.pause(s, campaign.id, evals=body.evals, reason=body.reason)
    s.commit()
    return _campaign_answer(s, campaign)


@router.post('/campaigns/{campaign}/resume', operation_id='resume', dependencies=[Depends(actor_access)])
def resume(s: Writing, campaign: Addressed) -> CampaignAnswer:
    """Resume a paused campaign. The daemon queues runs and requests roles again at once."""
    ops.resume(s, campaign.id)
    s.commit()
    return _campaign_answer(s, campaign)


@router.post('/campaigns/{campaign}/stop', operation_id='stop', dependencies=[Depends(actor_access)])
def stop(s: Writing, me: Acting, campaign: Addressed, body: StopBody) -> CampaignAnswer:
    """Stop a campaign at once, as when a ``[stop]`` condition holds. The daemon ends its agents as ``ending`` says."""
    ops.stop(s, campaign.id, me, reason=body.reason, ending=body.ending)
    s.commit()
    return _campaign_answer(s, campaign)


@router.post('/campaigns/{campaign}/ideas', operation_id='add_idea', dependencies=[Depends(actor_access)])
def add_idea(s: Writing, me: Acting, campaign: Addressed, body: IdeaBody) -> Defined:
    """Add a defined idea, which the daemon schedules in priority order once it is ready."""
    idea, review = ops.add_idea(
        s,
        campaign.id,
        body.title,
        actor=me,
        hypothesis=body.hypothesis,
        plan=body.plan,
        provenance=body.provenance,
        family=body.family,
        priority=body.priority,
        harness=body.harness,
        base=body.base,
        proposed=body.proposed,
        track=body.track,
        after=body.after,
        supersedes=body.supersedes,
    )
    s.commit()
    with db_feed.snapshot(s) as cursor:
        return Defined(cursor=cursor, idea=idea_view(idea), review=review)


@router.post('/campaigns/{campaign}/seeds', operation_id='file_seed', dependencies=[Depends(actor_access)])
def file_seed(s: Writing, me: Acting, campaign: Addressed, body: SeedBody) -> IdeaAnswer:
    """File a seed, for research to develop into a defined idea or to reject."""
    idea = ops.file_seed(
        s, campaign.id, body.hunch, actor=me, provenance=body.provenance, base=body.base, track=body.track
    )
    return _idea_answer(s, idea)


@router.post('/ideas/{idea}/develop', operation_id='develop', dependencies=[Depends(actor_access)])
def develop(s: Writing, me: Acting, idea: AddressedIdea, body: DevelopBody) -> Defined:
    """Develop a seed into a defined idea that the daemon can schedule."""
    definition = Definition(body.hypothesis, body.evidence, body.check, body.metric, body.effect, body.plan)
    developed, review = ops.develop_seed(
        s,
        idea.id,
        definition,
        actor=me,
        title=body.title,
        family=body.family,
        base=body.base,
        priority=body.priority,
        proposed=body.proposed,
        after=body.after,
        supersedes=body.supersedes,
    )
    s.commit()
    with db_feed.snapshot(s) as cursor:
        return Defined(cursor=cursor, idea=idea_view(developed), review=review)


@router.post('/ideas/{idea}/status', operation_id='set_status', dependencies=[Depends(actor_access)])
def set_status(s: Writing, me: Acting, idea: AddressedIdea, body: StatusBody) -> StatusChanged:
    """Set an idea's status, with a note on the idea that says why."""
    was = ops.set_idea_status(s, idea.id, body.status, reason=body.reason, actor=me)
    s.commit()
    with db_feed.snapshot(s) as cursor:
        return StatusChanged(cursor=cursor, idea=idea_view(idea), was=was)


@router.put('/ideas/{idea}/links/{kind}/{target}', operation_id='link', dependencies=[Depends(actor_access)])
def link(s: Writing, me: Acting, idea: AddressedIdea, kind: LinkKind, target: Target, body: Reasoned) -> IdeaAnswer:
    """Link an idea to another of its campaign: it runs after the target, or supersedes it."""
    ops.link(s, idea.id, kind, target.id, reason=body.reason, actor=me)
    return _idea_answer(s, idea)


@router.delete('/ideas/{idea}/links/{kind}/{target}', operation_id='unlink', dependencies=[Depends(actor_access)])
def unlink(s: Writing, me: Acting, idea: AddressedIdea, kind: LinkKind, target: IdeaId, body: Reasoned) -> IdeaAnswer:
    """Delete an idea's link."""
    ops.unlink(s, idea.id, kind, target, reason=body.reason, actor=me)
    return _idea_answer(s, idea)


@router.post('/ideas/{idea}/base', operation_id='set_base', dependencies=[Depends(actor_access)])
def set_base(s: Writing, me: Acting, idea: AddressedIdea, body: BaseBody) -> IdeaAnswer:
    """Set the candidate that an idea's runs start from, or clear the pin."""
    ops.set_base(s, idea.id, body.base, reason=body.reason, actor=me)
    return _idea_answer(s, idea)


@router.post('/ideas/{idea}/track', operation_id='set_track', dependencies=[Depends(actor_access)])
def set_track(s: Writing, me: Acting, idea: AddressedIdea, body: TrackBody) -> TrackChanged:
    """Move an idea to another track of its campaign, with a note on the idea that says why."""
    was = ops.set_track(s, idea.id, body.track, reason=body.reason, actor=me)
    s.commit()
    with db_feed.snapshot(s) as cursor:
        return TrackChanged(cursor=cursor, idea=idea_view(idea), was=was)


def _idea_answer(s: Session, idea: Idea) -> IdeaAnswer:
    s.commit()
    with db_feed.snapshot(s) as cursor:
        return IdeaAnswer(cursor=cursor, idea=idea_view(idea))


@router.post('/inbox/ack', operation_id='ack', dependencies=[Depends(actor_access)])
def ack(s: Writing, me: Acting, body: AckBody) -> Acked:
    """Acknowledge inbox items, to say that the caller has seen them. Nothing waits for an acknowledgement."""
    results = inbox_module.ack(s, body.ids, me)
    with db_feed.snapshot(s) as cursor:
        return Acked(cursor=cursor, results=results)


@router.post('/inbox/{comment}/reply', operation_id='reply', dependencies=[Depends(actor_access)])
def reply(s: Writing, me: Acting, comment: Commented, body: ReplyBody) -> Replied:
    """Answer an inbox item with a ``decision`` on the root of its thread, which is an inbox item of its own."""
    answer = inbox_module.reply(s, comment.id, body.body, author=me, priority=body.priority)
    with db_feed.snapshot(s) as cursor:
        return Replied(cursor=cursor, comment=inbox_module.comment_view(answer))


def _campaign_answer(s: Session, campaign: Campaign) -> CampaignAnswer:
    with db_feed.snapshot(s) as cursor:
        return CampaignAnswer(cursor=cursor, campaign=feed.status_view(s, [campaign])[0])


def _news(db: Database, campaign: CampaignId | None, me: Actor, after: int | None) -> News:
    with db.session() as s:
        return inbox_module.news(s, db_feed.newest(s) if after is None else after, campaign_id=campaign, me=me)


def _events(db: Database, query: EventsQuery, campaign: CampaignId | None, after: int) -> Batch:
    with db.session() as s:
        try:
            return feed.read(s, after, campaign_id=campaign, kinds=query.kind, limit=query.limit)
        except db_feed.CursorAhead as e:
            raise Refusal(HTTPStatus.CONFLICT, Problem.CURSOR_AHEAD, str(e)) from e


def with_api(server: App, api: Handlers) -> App:
    """``server`` with the API's routes, which read with ``api``. An operation's refusal answers 409, ``refused``."""
    server.include_router(router)
    server.add_exception_handler(ops.Refused, _refused)
    server.state.handlers = api
    return server


def _refused(_request: Request, e: Exception) -> Response:
    return JSONResponse({'error': Problem.REFUSED, 'detail': str(e)}, status_code=HTTPStatus.CONFLICT)


def daemon_state(started: str, listen: str | None) -> DaemonState:
    """This process, as the daemon that answers."""
    return DaemonState(pid=os.getpid(), started=started, listen=listen)


def openapi() -> dict[str, object]:
    """The OpenAPI document of ``router``: what ``GET /v1/openapi.json`` answers, and ``docs/reference/openapi.json``."""
    described = FastAPI(title='piloop', version='1')
    described.include_router(router)
    return described.openapi()


def _cwd(query: Located) -> FilePath | None:
    return None if query.cwd is None else FilePath(query.cwd)
