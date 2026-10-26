"""The API's routes: FastAPI path operations under ``/v1``, each calling the function that owns its answer.

``ROUTERS`` are the API, and ``with_api`` puts it on a server with what its routes read with. The routes share their
lookups as dependencies (``Selected``, ``Narrowed``) over one session per request. The OpenAPI document of the API
(``openapi``) is ``docs/reference/openapi.json``.
"""

import os
import shutil
import time
from collections.abc import Generator
from dataclasses import dataclass
from http import HTTPStatus
from pathlib import Path as FilePath
from typing import Annotated, assert_never

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Path, Query, Request, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from pydantic import SecretStr, TypeAdapter, ValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.actor import Actor, Person, System
from piloop.agents.results import PromptOutcome, queue_prompt
from piloop.api.contract import (
    Access,
    AckBody,
    BaseBody,
    CancelBody,
    Checkout,
    Clone,
    DevelopBody,
    EventsQuery,
    IdeaBody,
    InboxQuery,
    JournalBody,
    LaunchBody,
    LimitBody,
    Located,
    MessageBody,
    NewsQuery,
    PauseBody,
    Problem,
    PromptBody,
    Reasoned,
    Registration,
    ReplyBody,
    SecretBody,
    SeedBody,
    Selection,
    StatusBody,
    StopBody,
    TokenBody,
    TrackBody,
)
from piloop.api.views import (
    Acked,
    AgentAnswer,
    Batch,
    CampaignAnswer,
    CampaignOverview,
    CampaignSynced,
    DaemonState,
    Defined,
    Delivered,
    Delivery,
    IdeaAnswer,
    Inbox,
    InvalidConfig,
    Journaled,
    LiveKeys,
    Messaged,
    News,
    Overview,
    Pending,
    ProblemDetail,
    Prompted,
    Queued,
    Registered,
    Replied,
    RunAnswer,
    SecretChanged,
    SecretList,
    SecretView,
    Status,
    StatusChanged,
    Thread,
    TokenList,
    TokenMade,
    TrackChanged,
    Undelivered,
)
from piloop.campaign import inbox as inbox_module, ops
from piloop.campaign.campaigns import (
    CampaignError,
    UnknownCampaign,
    clone,
    named,
    register,
    repository,
    selected,
    sync,
    workspace_dir,
)
from piloop.campaign.ideas import Definition, idea_view
from piloop.campaign.memory import sync_documents
from piloop.campaign.stop import CANCELLABLE, request_cancel
from piloop.config import ConfigError
from piloop.db import feed as db_feed
from piloop.db.events import has, latest
from piloop.db.models import (
    OPEN_RUNS,
    AgentSession,
    Campaign,
    Candidate,
    Comment,
    Event,
    Idea,
    Journal,
    Run,
    utcnow,
)
from piloop.db.session import Database
from piloop.ids import (
    AgentSessionId,
    CampaignId,
    CandidateId,
    CommentId,
    IdeaId,
    KeyOrigin,
    LinkKind,
    RunId,
    TargetKind,
    TargetRef,
)
from piloop.mcp import EnvName
from piloop.net import Refusal, tokens
from piloop.net.server import OWNER, App, asked, authorized, changed
from piloop.net.tokens import Caller
from piloop.plugins import limits as limits_module
from piloop.plugins.cas.contract import Cas
from piloop.plugins.limits import live_keys
from piloop.plugins.secrets import contract as secrets
from piloop.plugins.section.contract import Rendering, rendered
from piloop.plugins.vcs.contract import VCS, CloneFailed
from piloop.reports import feed
from piloop.scoring import queue
from piloop.scoring.api import (
    CandidateAnswer,
    EvaluationQueue,
    ImportBody,
    MeasurementAdded,
    Merge,
    NoteBody,
    Noted,
    RemeasureBody,
    Remeasured,
    Report,
    VerdictBody,
    VerdictChanged,
)
from piloop.scoring.ledger import ImportRefused, RemeasureRefused, bar_of, import_reference, request_remeasure
from piloop.scoring.verdict import candidate_view, rejudgeable
from piloop.settings import Settings


@dataclass(frozen=True)
class Handlers:
    """What the routes read with: the daemon's database, settings and CAS, and the daemon itself."""

    db: Database
    cfg: Settings
    daemon: DaemonState
    cas: Cas


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
admin_access = authorized(Access.ADMIN)
"""The access of every route that reads or changes the home's own settings."""


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


def candidate_addressed(s: Writing, candidate: CandidateId) -> Candidate:
    """The candidate that the path names. Refuses one that the home does not have."""
    if (found := s.get(Candidate, candidate)) is None:
        raise Refusal(HTTPStatus.NOT_FOUND, Problem.UNKNOWN_CANDIDATE, f'no candidate#{candidate}')
    return found


def agent_addressed(s: Writing, agent: AgentSessionId) -> AgentSession:
    """The agent that the path names. Refuses one that the home does not have."""
    return _agent(s, agent)


def run_addressed(s: Writing, run: RunId) -> Run:
    """The run that the path names. Refuses one that the home does not have."""
    if (found := s.get(Run, run)) is None:
        raise Refusal(HTTPStatus.NOT_FOUND, Problem.UNKNOWN_RUN, f'no run#{run}')
    return found


def _agent(s: Session, agent: AgentSessionId) -> AgentSession:
    if (found := s.get(AgentSession, agent)) is None:
        raise Refusal(HTTPStatus.NOT_FOUND, Problem.UNKNOWN_AGENT, f'no agent_session#{agent}')
    return found


Addressed = Annotated[Campaign, Depends(campaign_addressed)]
AddressedCandidate = Annotated[Candidate, Depends(candidate_addressed)]
AddressedAgent = Annotated[AgentSession, Depends(agent_addressed)]
AddressedRun = Annotated[Run, Depends(run_addressed)]
Commented = Annotated[Comment, Depends(comment_addressed)]
AddressedIdea = Annotated[Idea, Depends(idea_addressed)]
Target = Annotated[Idea, Depends(target_addressed)]


FORBIDDEN_RESPONSE: dict[int | str, dict[str, object]] = {HTTPStatus.FORBIDDEN: {'model': ProblemDetail}}
"""What a route answers to an actor that may not make its change (``forbidden``), which another actor may."""

REFUSALS_DOCUMENTED: dict[int | str, dict[str, object]] = {
    HTTPStatus.NOT_FOUND: {'model': ProblemDetail},
    HTTPStatus.CONFLICT: {'model': ProblemDetail},
}
"""What a route answers when it refuses: ``Refusal``'s 404 or 409, with a ``ProblemDetail``."""
router = APIRouter(
    prefix='/v1',
    dependencies=[Depends(reader)],
    responses=REFUSALS_DOCUMENTED,
)
"""The kernel's routes: the views, the feed, the inbox, campaigns, agents and the home's settings. Every route reads."""
optimization = APIRouter(
    prefix='/v1',
    dependencies=[Depends(reader)],
    responses=REFUSALS_DOCUMENTED,
)
"""The research model's routes: ideas, candidates, measurements, evaluations and runs, on a router of their own."""
ROUTERS = (router, optimization)
"""The API: every router that ``with_api`` serves."""


@router.get('/status', operation_id='status')
def status(api: Api, s: Reading, me: Acting, campaigns: Selected) -> Status:
    """The main facts of each campaign, at a cursor."""
    with db_feed.snapshot(s) as cursor:
        return Status(cursor=cursor, daemon=api.daemon, campaigns=feed.status_view(s, campaigns, me))


@router.get('/overview', operation_id='overview')
def overview(api: Api, s: Reading, campaigns: Selected) -> Overview:
    """The sections of each campaign's ``[status] sections``, and the live keys that config.toml or a limit sets."""
    now = utcnow()
    shown: list[CampaignOverview | InvalidConfig] = []
    for c in campaigns:
        match feed.configured(s, c):
            case InvalidConfig() as invalid:
                shown.append(invalid)
            case cfg:
                rendering = Rendering(s, now, c, workspace_dir(api.cfg.home, c.id), api.cas)
                sections = rendered(cfg.status.shown(), rendering)
                shown.append(
                    CampaignOverview(id=c.id, name=c.name, path=c.path, parent=feed.parent_of(s, c), sections=sections)
                )
    return Overview(
        daemon=api.daemon, limits=live_keys(s, api.cfg.providers, (KeyOrigin.CONFIG, KeyOrigin.LIMIT)), campaigns=shown
    )


@router.get('/inbox', operation_id='inbox')
def inbox(s: Reading, me: Acting, campaign: Narrowed, query: Annotated[InboxQuery, Query()]) -> Inbox:
    """The inbox items that the caller did not write, and the seeds, at a cursor."""
    with db_feed.snapshot(s) as cursor:
        return feed.inbox_view(s, cursor, me, campaign_id=campaign, kinds=query.kind, acked=query.acked)


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
    """The thread of a comment, the same for each comment of it: the root in full, its replies, and its citations."""
    if (found := s.get(Comment, comment)) is None:
        raise Refusal(HTTPStatus.NOT_FOUND, Problem.UNKNOWN_COMMENT, f'no comment#{comment}')
    return inbox_module.thread_of(s, found)


@router.post('/campaigns', operation_id='register_campaign', dependencies=[Depends(actor_access)])
def register_campaign(api: Api, s: Writing, me: Acting, body: Registration) -> Registered:
    """Register a campaign: a checkout where it is, or a repository that the daemon clones under its home first.

    It adds the campaign with its head version as the baseline candidate, and syncs the ``[memory]`` documents. The
    daemon then measures the baseline: its ``measurement.recorded`` event comes after the answer's cursor, and a
    baseline that fails pauses the campaign. A clone that does not register is deleted.
    """
    campaign, baseline = _registered(api, s, me, body)
    documents = sync_documents(s, campaign, campaign.config, me)
    s.flush()
    cursor = db_feed.newest(s)  # read in the write: no event of the baseline's measurement can come before it
    view = feed.status_view(s, [campaign], me)[0]
    s.commit()
    return Registered(cursor=cursor, campaign=view, baseline=baseline.id, documents=documents)


def _registered(api: Handlers, s: Session, me: Actor, body: Registration) -> tuple[Campaign, Candidate]:
    match body:
        case Checkout():
            return register(s, FilePath(body.path), me)
        case Clone(clone=remote):
            path = clone(api.cfg.home, VCS.selected(api.cfg.providers), remote.url, _credential(api, remote.secret))
            try:
                return register(s, path, me)
            except Exception:
                shutil.rmtree(path)
                raise
        case _:
            assert_never(body)


def _credential(api: Handlers, secret: EnvName | None) -> SecretStr | None:
    """The value of the home's secret ``secret``. Refuses a name that the home has no secret of."""
    if secret is None:
        return None
    if (value := secrets.HomeSecrets(api.db, _secrets(api)).get(secret)) is None:
        raise Refusal(
            HTTPStatus.CONFLICT, Problem.REFUSED, f'the home has no secret {secret}: see `piloop secret list`'
        )
    return value


@router.post('/campaigns/{campaign}/sync', operation_id='sync_campaign', dependencies=[Depends(actor_access)])
def sync_campaign(s: Writing, me: Acting, campaign: Addressed) -> CampaignSynced:
    """Read the campaign's ``piloop.toml`` again, and sync its ``[memory]`` documents.

    A changed evaluate command, metric set or link makes a new evaluator. A stopped campaign whose stop conditions
    changed is active again.
    """
    was = campaign.status
    sync(s, campaign)
    documents = sync_documents(s, campaign, campaign.config, me)
    s.commit()
    with db_feed.snapshot(s) as cursor:
        return CampaignSynced(
            cursor=cursor, campaign=feed.status_view(s, [campaign], me)[0], was=was, documents=documents
        )


@router.post('/campaigns/{campaign}/messages', operation_id='message', dependencies=[Depends(actor_access)])
def message(s: Writing, me: Acting, campaign: Addressed, body: MessageBody) -> Messaged:
    """Write to the campaign's orchestrator as the caller's actor: a ``message``, which wakes ``piloop inbox wait``."""
    sent = inbox_module.message(s, campaign.id, body.body, author=me, priority=body.priority)
    with db_feed.snapshot(s) as cursor:
        return Messaged(cursor=cursor, comment=inbox_module.comment_view(sent))


@router.post('/campaigns/{campaign}/pause', operation_id='pause', dependencies=[Depends(actor_access)])
def pause(s: Writing, me: Acting, campaign: Addressed, body: PauseBody) -> CampaignAnswer:
    """Pause a campaign: nothing new launches, and nothing is escalated. Running work goes on."""
    ops.pause(s, campaign.id, evals=body.evals, reason=body.reason)
    s.commit()
    return _campaign_answer(s, campaign, me)


@router.post('/campaigns/{campaign}/resume', operation_id='resume', dependencies=[Depends(actor_access)])
def resume(s: Writing, me: Acting, campaign: Addressed) -> CampaignAnswer:
    """Resume a paused campaign. The daemon queues runs and requests roles again at once."""
    ops.resume(s, campaign.id)
    s.commit()
    return _campaign_answer(s, campaign, me)


@router.post('/campaigns/{campaign}/stop', operation_id='stop', dependencies=[Depends(actor_access)])
def stop(s: Writing, me: Acting, campaign: Addressed, body: StopBody) -> CampaignAnswer:
    """Stop a campaign at once, as when a ``[stop]`` condition holds. The daemon ends its agents as ``ending`` says."""
    ops.stop(s, campaign.id, me, reason=body.reason, ending=body.ending)
    s.commit()
    return _campaign_answer(s, campaign, me)


@optimization.post('/campaigns/{campaign}/ideas', operation_id='add_idea', dependencies=[Depends(actor_access)])
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


@optimization.post('/campaigns/{campaign}/seeds', operation_id='file_seed', dependencies=[Depends(actor_access)])
def file_seed(s: Writing, me: Acting, campaign: Addressed, body: SeedBody) -> IdeaAnswer:
    """File a seed, for research to develop into a defined idea or to reject."""
    idea = ops.file_seed(
        s, campaign.id, body.hunch, actor=me, provenance=body.provenance, base=body.base, track=body.track
    )
    return _idea_answer(s, idea)


@optimization.post('/ideas/{idea}/develop', operation_id='develop', dependencies=[Depends(actor_access)])
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


@optimization.post('/ideas/{idea}/status', operation_id='set_status', dependencies=[Depends(actor_access)])
def set_status(s: Writing, me: Acting, idea: AddressedIdea, body: StatusBody) -> StatusChanged:
    """Set an idea's status, with a note on the idea that says why."""
    was = ops.set_idea_status(s, idea.id, body.status, reason=body.reason, actor=me)
    s.commit()
    with db_feed.snapshot(s) as cursor:
        return StatusChanged(cursor=cursor, idea=idea_view(idea), was=was)


@optimization.put('/ideas/{idea}/links/{kind}/{target}', operation_id='link', dependencies=[Depends(actor_access)])
def link(s: Writing, me: Acting, idea: AddressedIdea, kind: LinkKind, target: Target, body: Reasoned) -> IdeaAnswer:
    """Link an idea to another of its campaign: it runs after the target, or supersedes it."""
    ops.link(s, idea.id, kind, target.id, reason=body.reason, actor=me)
    return _idea_answer(s, idea)


@optimization.delete('/ideas/{idea}/links/{kind}/{target}', operation_id='unlink', dependencies=[Depends(actor_access)])
def unlink(s: Writing, me: Acting, idea: AddressedIdea, kind: LinkKind, target: IdeaId, body: Reasoned) -> IdeaAnswer:
    """Delete an idea's link."""
    ops.unlink(s, idea.id, kind, target, reason=body.reason, actor=me)
    return _idea_answer(s, idea)


@optimization.post('/ideas/{idea}/base', operation_id='set_base', dependencies=[Depends(actor_access)])
def set_base(s: Writing, me: Acting, idea: AddressedIdea, body: BaseBody) -> IdeaAnswer:
    """Set the candidate that an idea's runs start from, or clear the pin."""
    ops.set_base(s, idea.id, body.base, reason=body.reason, actor=me)
    return _idea_answer(s, idea)


@optimization.post('/ideas/{idea}/track', operation_id='set_track', dependencies=[Depends(actor_access)])
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


@optimization.post(
    '/campaigns/{campaign}/candidates', operation_id='import_candidate', dependencies=[Depends(actor_access)]
)
def import_candidate(s: Writing, me: Acting, campaign: Addressed, body: ImportBody) -> CandidateAnswer:
    """Register the files of a version as a reference candidate: a starting point for an idea."""
    candidate = import_reference(
        s, campaign, campaign.config, repository(campaign), body.version, summary=body.summary, actor=me
    )
    s.commit()
    with db_feed.snapshot(s) as cursor:
        return CandidateAnswer(cursor=cursor, candidate=candidate_view(candidate))


@optimization.post('/candidates/{candidate}/remeasure', operation_id='remeasure', dependencies=[Depends(actor_access)])
def remeasure(s: Writing, me: Acting, candidate: AddressedCandidate, body: RemeasureBody) -> Remeasured:
    """Ask the daemon to measure a candidate again, next to its bar, with the current evaluator."""
    request_remeasure(s, candidate, me, body.repeats)
    s.commit()
    campaign = s.get_one(Campaign, candidate.campaign_id)
    held = ops.measuring_held(s, campaign, campaign.config).strip() or None
    bar = None if candidate.parent is None else bar_of(s, candidate.parent).id
    with db_feed.snapshot(s) as cursor:
        return Remeasured(
            cursor=cursor, candidate=candidate_view(candidate), bar=bar, held=held, rejudged=rejudgeable(candidate)
        )


@optimization.post('/candidates/{candidate}/note', operation_id='note', dependencies=[Depends(actor_access)])
def note(s: Writing, me: Acting, candidate: AddressedCandidate, body: NoteBody) -> Noted:
    """Keep a regressed result in view as a lead, with follow-up seeds pinned to it."""
    _, seeds = ops.note_candidate(s, candidate.id, body.reason, actor=me, follow_ups=body.follow_ups, track=body.track)
    s.commit()
    with db_feed.snapshot(s) as cursor:
        return Noted(cursor=cursor, candidate=candidate_view(candidate), seeds=[idea_view(i) for i in seeds])


@optimization.post('/candidates/{candidate}/verdict', operation_id='set_verdict', dependencies=[Depends(actor_access)])
def set_verdict(s: Writing, me: Acting, candidate: AddressedCandidate, body: VerdictBody) -> VerdictChanged:
    """Override a candidate's judgement, with the reason. The verdict is then the caller's."""
    done = ops.set_verdict(s, candidate.id, body.verdict, reason=body.reason, actor=me)
    s.commit()
    rebased = done.rebased
    merged = None if rebased is None or rebased.rebased is None else Merge(candidate=rebased.rebased, head=rebased.head)
    with db_feed.snapshot(s) as cursor:
        return VerdictChanged(cursor=cursor, candidate=candidate_view(candidate), was=done.old, merge=merged)


@optimization.post(
    '/candidates/{candidate}/measurements', operation_id='add_measurement', dependencies=[Depends(actor_access)]
)
def add_measurement(s: Writing, me: Acting, candidate: AddressedCandidate, body: Report) -> MeasurementAdded:
    """Record a measurement of a candidate under the campaign's current evaluator, and have its verdict judged again."""
    added = ops.add_measurement(s, candidate.id, body, actor=me)
    s.commit()
    with db_feed.snapshot(s) as cursor:
        return MeasurementAdded(
            cursor=cursor, measurement=added.measurement.id, rejudged=added.rejudged, effect=added.effect
        )


@optimization.get('/evaluations', operation_id='evaluations')
def evaluations(s: Reading) -> EvaluationQueue:
    """The evaluation queue: the running evaluations, then the waiting ones in the order in which they get a slot."""
    return queue.view(s)


@optimization.post(
    '/candidates/{candidate}/evaluation/cancel',
    operation_id='cancel_evaluation',
    dependencies=[Depends(actor_access)],
)
def cancel_evaluation(s: Writing, me: Acting, candidate: AddressedCandidate) -> CandidateAnswer:
    """Cancel a candidate's evaluation, waiting or running, or its requested remeasure: it measures a crash at once.

    A baseline that never measured pauses its campaign, as when it fails (``ops.hold_on_failed_baseline``).
    """
    if not queue.cancel(s, candidate.id, me):
        raise Refusal(
            HTTPStatus.CONFLICT,
            Problem.REFUSED,
            f'candidate#{candidate.id} has no evaluation that waits or runs: see `piloop eval queue`',
        )
    ops.hold_on_failed_baseline(s, candidate, None)
    s.commit()
    with db_feed.snapshot(s) as cursor:
        return CandidateAnswer(cursor=cursor, candidate=candidate_view(candidate))


@router.get('/limits', operation_id='limits', dependencies=[Depends(admin_access)])
def limits(api: Api, s: Reading) -> LiveKeys:
    """Each live key: its value, and where it comes from (the default, ``config.toml``, or a limit)."""
    return _live_keys(api, s)


@router.put('/limits/{key}', operation_id='set_limit', dependencies=[Depends(admin_access)])
def set_limit(api: Api, s: Writing, me: Acting, key: str, body: LimitBody) -> LiveKeys:
    """Set a live key. It wins over ``config.toml`` at once, and holds until it is unset."""
    limits_module.set_limit(s, api.cfg.providers, key, body.value, reason=body.reason, actor=me.signature)
    s.commit()
    return _live_keys(api, s)


@router.delete('/limits/{key}', operation_id='unset_limit', dependencies=[Depends(admin_access)])
def unset_limit(api: Api, s: Writing, key: str) -> LiveKeys:
    """Remove a limit: ``config.toml`` holds again at once."""
    if not limits_module.unset_limit(s, key):
        raise Refusal(HTTPStatus.NOT_FOUND, Problem.UNKNOWN_LIMIT, f'{key} has no limit: see `piloop limits`')
    s.commit()
    return _live_keys(api, s)


def _live_keys(api: Handlers, s: Session) -> LiveKeys:
    with db_feed.snapshot(s) as cursor:
        return LiveKeys(cursor=cursor, keys=live_keys(s, api.cfg.providers, tuple(KeyOrigin)))


@router.get('/tokens', operation_id='list_tokens', dependencies=[Depends(admin_access)])
def list_tokens(s: Reading) -> TokenList:
    """The bearer tokens: name, access and when each was made. Never the secrets."""
    return tokens.listing(s)


@router.post('/tokens', operation_id='create_token', dependencies=[Depends(admin_access)])
def create_token(s: Writing, body: TokenBody) -> TokenMade:
    """Make a token, and answer its secret: the only time that it is shown, since the home keeps its hash only."""
    made = tokens.made(s, body.name, body.access)
    s.commit()
    return made


@router.delete('/tokens/{name}', operation_id='revoke_token', dependencies=[Depends(admin_access)])
def revoke_token(s: Writing, name: str) -> TokenList:
    """Delete a token: the next call with it gets 401."""
    try:
        tokens.revoke(s, name)
    except tokens.NoToken as e:
        raise Refusal(HTTPStatus.NOT_FOUND, Problem.UNKNOWN_TOKEN, str(e)) from e
    s.commit()
    return tokens.listing(s)


@router.get('/secrets', operation_id='list_secrets', dependencies=[Depends(admin_access)])
def list_secrets(api: Api, s: Reading) -> SecretList:
    """The secrets: the name, whose it is, and who set it last and when. Never a value."""
    names = {row.id: row.name for row in s.execute(select(Campaign.id, Campaign.name))}
    return SecretList(
        secrets=[
            SecretView(
                name=found.secret.name,
                campaign=None if found.secret.owner is None else names[found.secret.owner],
                set_at=found.set_at,
                set_by=None if found.set_by is None else found.set_by.signature,
            )
            for found in secrets.listed(s, _secrets(api))
        ]
    )


@router.put(
    '/secrets/{name}', operation_id='set_secret', dependencies=[Depends(admin_access)], responses=FORBIDDEN_RESPONSE
)
def set_secret(api: Api, s: Writing, me: Acting, name: EnvName, campaign: Narrowed, body: SecretBody) -> SecretChanged:
    """Set a secret of the home, or of the campaign named, in place of any. Agents get it from their next start.

    An agent sets only its own campaign's secrets.
    """
    secrets.set_secret(s, _secrets(api), me, campaign, name, body.value)
    s.commit()
    return _secret_changed(s, name, campaign)


@router.delete(
    '/secrets/{name}', operation_id='remove_secret', dependencies=[Depends(admin_access)], responses=FORBIDDEN_RESPONSE
)
def remove_secret(api: Api, s: Writing, me: Acting, name: EnvName, campaign: Narrowed) -> SecretChanged:
    """Delete a secret of the home, or of the campaign named. An agent that starts after it no longer gets it."""
    if not secrets.remove_secret(s, _secrets(api), me, campaign, name):
        raise Refusal(
            HTTPStatus.NOT_FOUND,
            Problem.UNKNOWN_SECRET,
            f'{_whose(s, campaign)} has no secret {name}: see `piloop secret list`',
        )
    s.commit()
    return _secret_changed(s, name, campaign)


def _secrets(api: Handlers) -> secrets.SecretsProvider:
    return secrets.SECRETS.selected(api.cfg.providers)


def _secret_changed(s: Session, name: str, owner: secrets.Owner) -> SecretChanged:
    campaign = None if owner is None else s.get_one(Campaign, owner).name
    with db_feed.snapshot(s) as cursor:
        return SecretChanged(cursor=cursor, name=name, campaign=campaign)


def _whose(s: Session, owner: secrets.Owner) -> str:
    return 'the home' if owner is None else f'campaign {s.get_one(Campaign, owner).name}'


@router.post('/campaigns/{campaign}/agents', operation_id='launch', dependencies=[Depends(actor_access)])
def launch(s: Writing, campaign: Addressed, body: LaunchBody) -> AgentAnswer:
    """Ask the daemon for one more agent of a sweeping role: it launches it at once, within ``max_concurrent``."""
    agent = ops.request_agent(s, campaign.id, body.role, harness=body.harness, model=body.model, reason=body.reason)
    s.commit()
    with db_feed.snapshot(s) as cursor:
        return AgentAnswer(cursor=cursor, agent=agent.id, role=agent.kind)


@router.post('/agents/{agent}/prompt', operation_id='prompt', dependencies=[Depends(actor_access)])
async def prompt(request: Request, api: Api, me: Acting, agent: AgentSessionId, body: PromptBody) -> Prompted:
    """Queue a message for the next turn of a running agent: a note on it, which the daemon delivers.

    With wait, it answers once something came of the note, or at the end of the wait with ``pending``. The note is
    written first, in the thread pool; between two reads of its outcome the route holds no thread.
    """
    comment = await run_in_threadpool(_queue_prompt, api.db, me, agent, body.text)
    deadline, seen = time.monotonic() + body.wait, 0
    answer = await run_in_threadpool(_prompted, api.db, agent, comment)
    while isinstance(answer.delivery, Pending) and (left := deadline - time.monotonic()) > 0:
        if (woke := await changed(request, api.db.changed, seen, left)) is None:
            break
        seen = woke
        answer = await run_in_threadpool(_prompted, api.db, agent, comment)
    return answer


def _queue_prompt(db: Database, me: Actor, agent: AgentSessionId, text: str) -> CommentId:
    with db.acting(me) as s:
        _agent(s, agent)
        if (comment := queue_prompt(s, agent, text, me.signature)) is None:
            raise Refusal(HTTPStatus.CONFLICT, Problem.REFUSED, f'agent_session#{agent} is not running')
        s.commit()
        return comment


def _prompted(db: Database, agent: AgentSessionId, comment: CommentId) -> Prompted:
    on_agent = Event.target == TargetRef(TargetKind.AGENT_SESSION, agent)
    with db.session() as s, db_feed.snapshot(s) as cursor:
        found = latest(s, PromptOutcome, on_agent, has(PromptOutcome, comment=comment))
        return Prompted(cursor=cursor, comment=comment, delivery=_delivery(None if found is None else found[1]))


def _delivery(outcome: PromptOutcome | None) -> Delivery:
    match outcome:
        case None:
            return Pending()
        case PromptOutcome(kind='comment.queued'):
            return Queued()
        case PromptOutcome(kind='comment.delivered'):
            return Delivered()
        case _:
            return Undelivered(error=outcome.error)


@router.post('/agents/{agent}/cancel', operation_id='cancel_agent', dependencies=[Depends(actor_access)])
def cancel_agent(s: Writing, me: Acting, agent: AddressedAgent, body: CancelBody) -> AgentAnswer:
    """Cancel an agent of any role: a requested one never starts, a running one ends as ``ending`` says.

    For an implementer, this cancels its run. The daemon acts at once.
    """
    if agent.state not in CANCELLABLE:
        raise Refusal(
            HTTPStatus.CONFLICT, Problem.REFUSED, f'agent_session#{agent.id} is not requested, starting or running'
        )
    request_cancel(s, agent, me, ending=body.ending)
    s.commit()
    with db_feed.snapshot(s) as cursor:
        return AgentAnswer(cursor=cursor, agent=agent.id, role=agent.kind)


@optimization.post('/runs/{run}/cancel', operation_id='cancel_run', dependencies=[Depends(actor_access)])
def cancel_run(s: Writing, me: Acting, run: AddressedRun, body: CancelBody) -> RunAnswer:
    """Cancel a queued or running run: a queued one never starts, a running implementer ends as ``ending`` says."""
    if run.status not in OPEN_RUNS:
        raise Refusal(HTTPStatus.CONFLICT, Problem.REFUSED, f'run#{run.id} is not queued or running')
    request_cancel(s, run, me, ending=body.ending)
    s.commit()
    with db_feed.snapshot(s) as cursor:
        return RunAnswer(cursor=cursor, run=run.id)


@router.post('/journal', operation_id='journal', dependencies=[Depends(actor_access)])
def journal(s: Writing, me: Acting, body: JournalBody) -> Journaled:
    """Append a line to the orchestrator's journal, signed by the caller's actor. ``piloop wake`` prints it."""
    entry = Journal(author=me.signature, line=body.line)
    s.add(entry)
    s.commit()
    with db_feed.snapshot(s) as cursor:
        return Journaled(cursor=cursor, journal=entry.id)


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


def _campaign_answer(s: Session, campaign: Campaign, me: Actor) -> CampaignAnswer:
    with db_feed.snapshot(s) as cursor:
        return CampaignAnswer(cursor=cursor, campaign=feed.status_view(s, [campaign], me)[0])


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
    """``server`` with the API's routes, which read with ``api``. An operation's refusal answers 409, ``refused``.

    Raises:
        PluginError: A contributed route has the method and path of one of the API's.
    """
    for routes in ROUTERS:
        server.mount_router(routes)
    for refused in REFUSALS:
        server.add_exception_handler(refused, _refused)
    server.add_exception_handler(secrets.Denied, _forbidden)
    server.state.handlers = api
    return server


REFUSALS: tuple[type[Exception], ...] = (
    ops.Refused,
    ImportRefused,
    RemeasureRefused,
    limits_module.Refused,
    tokens.NameTaken,
    CampaignError,
    ConfigError,
    CloneFailed,
)
"""What an operation raises when it refuses its input and changes nothing: each answers 409, ``refused``."""


def _refused(_request: Request, e: Exception) -> Response:
    return JSONResponse({'error': Problem.REFUSED, 'detail': str(e)}, status_code=HTTPStatus.CONFLICT)


def _forbidden(_request: Request, e: Exception) -> Response:
    return JSONResponse({'error': Problem.FORBIDDEN, 'detail': str(e)}, status_code=HTTPStatus.FORBIDDEN)


def daemon_state(started: str, listen: str | None) -> DaemonState:
    """This process, as the daemon that answers."""
    return DaemonState(pid=os.getpid(), started=started, listen=listen)


def openapi() -> dict[str, object]:
    """The OpenAPI document of ``ROUTERS``: what ``GET /v1/openapi.json`` answers, and ``docs/reference/openapi.json``."""
    described = FastAPI(title='piloop', version='1')
    for routes in ROUTERS:
        described.include_router(routes)
    return described.openapi()


def _cwd(query: Located) -> FilePath | None:
    return None if query.cwd is None else FilePath(query.cwd)
