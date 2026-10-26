"""The API's client: one method per route, which sends the route's query model and reads back its answer model.

The models are the ones that the routes use (``piloop.api.contract``, ``piloop.api.views``), so the client needs no
second set. A capability with routes of its own subclasses it. ``tests/net/test_api.py`` checks each method's method,
path and models against the server's routes.
"""

import errno
import socket as sockets
from collections.abc import Generator
from contextlib import AbstractContextManager, contextmanager
from datetime import timedelta
from http import HTTPStatus
from pathlib import Path
from typing import Final, Self
from urllib.parse import quote

import httpx2 as httpx
from pydantic import BaseModel, SecretStr, ValidationError

from piloop.actor import Actor
from piloop.api.contract import (
    MAX_POLL_SECONDS,
    AckBody,
    BaseBody,
    CancelBody,
    Clone,
    DevelopBody,
    EventsQuery,
    IdeaBody,
    InboxQuery,
    LaunchBody,
    LimitBody,
    Located,
    MessageBody,
    NewsQuery,
    Params,
    PauseBody,
    Polled,
    PromptBody,
    Reasoned,
    Registration,
    ReplyBody,
    RetireBody,
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
    CampaignSynced,
    Defined,
    IdeaAnswer,
    Inbox,
    LiveKeys,
    Messaged,
    News,
    Overview,
    ProblemDetail,
    Prompted,
    Registered,
    Replied,
    RunAnswer,
    SecretChanged,
    SecretList,
    Status,
    StatusChanged,
    Thread,
    TokenList,
    TokenMade,
    TrackChanged,
)
from piloop.ids import AgentSessionId, CommentId, IdeaId, LinkKind, RunId

TIMEOUT_SECONDS = 30
CLONE_WAIT: Final = timedelta(minutes=15)
"""How long a registration that clones a URL waits for its answer: longer than a vcs provider's clone may take, such
as git's 10 minutes."""
"""How long a call waits for its answer, beyond a long-poll's own wait."""
SOCKET_ACTOR_HEADER = 'x-piloop-actor'
"""The header in which a client on the home's socket asks to act as its ``PILOOP_ACTOR``. A network carries the ask its
own way (``Network.actor``)."""


class NoDaemon(ConnectionError):
    """No daemon serves the home: the message says how to start one."""


class Refused(Exception):
    """The daemon refused a call: ``error`` says why, as a ``Problem``, and the message what to do."""

    def __init__(self, error: str | None, detail: str) -> None:
        super().__init__(detail)
        self.error = error


class Client:
    """A client of one daemon, over ``http``."""

    def __init__(self, http: httpx.Client) -> None:
        self._http = http

    @classmethod
    @contextmanager
    def connect(cls, http: httpx.Client) -> Generator[Self]:
        """A client over ``http`` until the block ends.

        ``http`` is the home's socket (``socket_http``), or a network's client of a server (``NetworkConfig.client``),
        which carries the actor.
        """
        with http:
            yield cls(http)

    @classmethod
    def on(cls, socket: Path, actor: Actor, token: SecretStr | None = None) -> AbstractContextManager[Self]:
        """A client of the daemon on the Unix socket ``socket``, acting as ``actor`` with ``token`` (``socket_http``)."""
        return cls.connect(socket_http(socket, actor, token))

    def status(self, query: Selection) -> Status:
        return self._get('/v1/status', query, Status)

    def overview(self, query: Selection) -> Overview:
        return self._get('/v1/overview', query, Overview)

    def inbox(self, query: InboxQuery) -> Inbox:
        return self._get('/v1/inbox', query, Inbox)

    def events(self, query: EventsQuery) -> Batch:
        return self._get('/v1/events', query, Batch)

    def news(self, query: NewsQuery) -> News:
        return self._get('/v1/inbox/news', query, News)

    def thread(self, comment: CommentId) -> Thread:
        return self._get(f'/v1/inbox/{comment}', Params(), Thread)

    def register_campaign(self, body: Registration) -> Registered:
        return self.post('/v1/campaigns', body, Registered)

    def sync_campaign(self, campaign: str) -> CampaignSynced:
        return self.post(f'/v1/campaigns/{segment(campaign)}/sync', None, CampaignSynced)

    def message(self, campaign: str, body: MessageBody) -> Messaged:
        return self.post(f'/v1/campaigns/{segment(campaign)}/messages', body, Messaged)

    def pause(self, campaign: str, body: PauseBody) -> CampaignAnswer:
        return self.post(f'/v1/campaigns/{segment(campaign)}/pause', body, CampaignAnswer)

    def resume(self, campaign: str) -> CampaignAnswer:
        return self.post(f'/v1/campaigns/{segment(campaign)}/resume', None, CampaignAnswer)

    def stop(self, campaign: str, body: StopBody) -> CampaignAnswer:
        return self.post(f'/v1/campaigns/{segment(campaign)}/stop', body, CampaignAnswer)

    def retire(self, campaign: str, body: RetireBody) -> CampaignAnswer:
        return self.post(f'/v1/campaigns/{segment(campaign)}/retire', body, CampaignAnswer)

    def add_idea(self, campaign: str, body: IdeaBody) -> Defined:
        return self.post(f'/v1/campaigns/{segment(campaign)}/ideas', body, Defined)

    def file_seed(self, campaign: str, body: SeedBody) -> IdeaAnswer:
        return self.post(f'/v1/campaigns/{segment(campaign)}/seeds', body, IdeaAnswer)

    def develop(self, idea: IdeaId, body: DevelopBody) -> Defined:
        return self.post(f'/v1/ideas/{idea}/develop', body, Defined)

    def set_status(self, idea: IdeaId, body: StatusBody) -> StatusChanged:
        return self.post(f'/v1/ideas/{idea}/status', body, StatusChanged)

    def link(self, idea: IdeaId, kind: LinkKind, target: IdeaId, body: Reasoned) -> IdeaAnswer:
        return self._send('PUT', f'/v1/ideas/{idea}/links/{kind}/{target}', body, IdeaAnswer)

    def unlink(self, idea: IdeaId, kind: LinkKind, target: IdeaId, body: Reasoned) -> IdeaAnswer:
        return self._send('DELETE', f'/v1/ideas/{idea}/links/{kind}/{target}', body, IdeaAnswer)

    def set_base(self, idea: IdeaId, body: BaseBody) -> IdeaAnswer:
        return self.post(f'/v1/ideas/{idea}/base', body, IdeaAnswer)

    def set_track(self, idea: IdeaId, body: TrackBody) -> TrackChanged:
        return self.post(f'/v1/ideas/{idea}/track', body, TrackChanged)

    def limits(self) -> LiveKeys:
        return self._get('/v1/limits', Params(), LiveKeys)

    def set_limit(self, key: str, body: LimitBody) -> LiveKeys:
        return self._send('PUT', f'/v1/limits/{segment(key)}', body, LiveKeys)

    def unset_limit(self, key: str) -> LiveKeys:
        return self._send('DELETE', f'/v1/limits/{segment(key)}', None, LiveKeys)

    def list_tokens(self) -> TokenList:
        return self._get('/v1/tokens', Params(), TokenList)

    def create_token(self, body: TokenBody) -> TokenMade:
        return self.post('/v1/tokens', body, TokenMade)

    def revoke_token(self, name: str) -> TokenList:
        return self._send('DELETE', f'/v1/tokens/{segment(name)}', None, TokenList)

    def list_secrets(self) -> SecretList:
        return self._get('/v1/secrets', Params(), SecretList)

    def set_secret(self, name: str, query: Located, body: SecretBody) -> SecretChanged:
        return self._send('PUT', f'/v1/secrets/{segment(name)}', body, SecretChanged, query)

    def remove_secret(self, name: str, query: Located) -> SecretChanged:
        return self._send('DELETE', f'/v1/secrets/{segment(name)}', None, SecretChanged, query)

    def launch(self, campaign: str, body: LaunchBody) -> AgentAnswer:
        return self.post(f'/v1/campaigns/{segment(campaign)}/agents', body, AgentAnswer)

    def prompt(self, agent: AgentSessionId, body: PromptBody) -> Prompted:
        return self.post(f'/v1/agents/{agent}/prompt', body, Prompted)

    def cancel_agent(self, agent: AgentSessionId, body: CancelBody) -> AgentAnswer:
        return self.post(f'/v1/agents/{agent}/cancel', body, AgentAnswer)

    def cancel_run(self, run: RunId, body: CancelBody) -> RunAnswer:
        return self.post(f'/v1/runs/{run}/cancel', body, RunAnswer)

    def ack(self, body: AckBody) -> Acked:
        return self.post('/v1/inbox/ack', body, Acked)

    def reply(self, comment: CommentId, body: ReplyBody) -> Replied:
        return self.post(f'/v1/inbox/{comment}/reply', body, Replied)

    def _get[A: BaseModel](self, path: str, query: Params, answer: type[A]) -> A:
        """GET ``path`` with ``query`` as its query string, and read the answer as ``answer``.

        Raises:
            Refused: The daemon refused the call.
        """
        params = query.model_dump(mode='json', exclude_defaults=True)
        response = self._http.get(path, params=params, timeout=_timeout(query))
        return _answer(response, answer)

    def post[A: BaseModel](self, path: str, body: Params | None, answer: type[A]) -> A:
        """POST ``body`` as JSON to ``path``, and read the answer as ``answer``. A plugin's command calls its routes so.

        Raises:
            Refused: The daemon refused the call.
        """
        return self._send('POST', path, body, answer)

    def _send[A: BaseModel](
        self, method: str, path: str, body: Params | None, answer: type[A], query: Params | None = None
    ) -> A:
        """Send ``body`` as JSON and ``query`` as the query string to ``path`` with ``method``; read back ``answer``.

        Raises:
            Refused: The daemon refused the call.
        """
        content = None if body is None else body.model_dump_json()
        headers = None if body is None else {'content-type': 'application/json'}
        params = None if query is None else query.model_dump(mode='json', exclude_defaults=True)
        response = self._http.request(
            method, path, content=content, headers=headers, params=params, timeout=_timeout(body)
        )
        return _answer(response, answer)


def _timeout(params: Params | None) -> float:
    """How long a call waits for its answer: a long-poll's own wait and ``TIMEOUT_SECONDS``, or a clone's."""
    match params:
        case Polled(wait=wait):
            return TIMEOUT_SECONDS + min(wait, MAX_POLL_SECONDS)
        case Clone():
            return CLONE_WAIT.total_seconds()
        case _:
            return TIMEOUT_SECONDS


def socket_http(socket: Path, actor: Actor, token: SecretStr | None = None) -> httpx.Client:
    """An HTTP client of the daemon on the Unix socket ``socket``, which asks to act as ``actor``.

    With ``token``, a launched agent's (``Settings.agent_token``), the daemon knows it as that agent. Without, as the
    home's owner.

    Raises:
        NoDaemon: Nothing listens on the socket.
    """
    if not answers(socket):
        raise NoDaemon(
            f'no daemon listens on {socket}: start it with `piloop serve --detach`, or set PILOOP_HOME;'
            ' a daemon of an older piloop serves no socket: `piloop restart`'
        )
    transport = httpx.HTTPTransport(uds=str(socket))
    headers = {SOCKET_ACTOR_HEADER: actor.signature}
    if token is not None:
        headers['Authorization'] = f'Bearer {token.get_secret_value()}'
    return httpx.Client(transport=transport, base_url='http://piloop', headers=headers)


def segment(name: str) -> str:
    """A name, such as a campaign's or a key, as one segment of a path."""
    return quote(name, safe='')


def _answer[A: BaseModel](response: httpx.Response, answer: type[A]) -> A:
    if response.is_success:
        return answer.model_validate_json(response.content)
    raise _refused(response)


class _Invalid(BaseModel):
    """One parameter that FastAPI refused: where, and why."""

    loc: list[str | int]
    msg: str


class _Invalids(BaseModel):
    """FastAPI's answer to parameters that its models refuse (422)."""

    detail: list[_Invalid]


def _refused(response: httpx.Response) -> Refused:
    """What a refusal says: a route's ``ProblemDetail``, FastAPI's 422, a route that the daemon lacks, or a crash."""
    if response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY:
        invalid = _Invalids.model_validate_json(response.content)
        return Refused(None, '; '.join(f'{i.loc[-1]}: {i.msg}' for i in invalid.detail))
    try:
        problem = ProblemDetail.model_validate_json(response.content)
    except ValidationError:  # not a body of the API's, such as a crash's 500
        return Refused(None, f'the daemon answered {response.status_code}: {response.text}')
    if problem.error is None and response.status_code == HTTPStatus.NOT_FOUND:  # FastAPI's own: no such route
        return Refused(
            None, f'the daemon has no such route ({problem.detail}): it runs an older piloop; `piloop restart`'
        )
    return Refused(problem.error, problem.detail)


def answers(socket: Path) -> bool:
    """Whether a daemon listens on ``socket``: one that a killed daemon left refuses the connection.

    httpcore leaks the socket of a Unix connect that fails (``connect_unix_socket`` of
    https://github.com/encode/httpcore/blob/master/httpcore/_backends/sync.py closes none), so the client tries one
    first.
    """
    with sockets.socket(sockets.AF_UNIX, sockets.SOCK_STREAM) as probe:
        try:
            probe.connect(str(socket))
        except FileNotFoundError, ConnectionRefusedError:
            return False
        except OSError as e:
            if e.errno != errno.ENOTSOCK:  # macOS, for a file that is no socket; Linux refuses the connection
                raise
            return False
    return True
