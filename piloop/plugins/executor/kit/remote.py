"""The leases library over HTTP: ``Remote``, a worker's ``LeaseClient``, and ``lease_routes``, the server's half.

Each ``LeaseClient`` call is one ``POST /v1/executor/<call>`` of JSON, on the server of ``piloop serve --listen``. The
route makes the call on ``Local``, bound to the caller's principal: the worker is who its token says, never what its
body says. A refusal has a typed body, ``RefusalBody``. What ``Remote`` does with an answer:

| Status | The worker |
|---|---|
| 409 with an ``error`` of ``CONFLICTS`` | raises its exception: ``StaleLease`` stops the job, ``NotAddressed`` drops an answer to a message that was not its, ``MissingBlobs`` loses the job whose report names blobs that the server's CAS lacks, ``NotSent`` fails the job whose versions the server could not send, ``Dismissed`` ends the worker (no such pool) |
| 408, 429, 5xx, or no answer (``RETRIED``) | tries again with backoff, with the same body and so the same request id |
| any other 4xx | ``Dismissed``: stops its jobs, keeps their reports, and exits. 401: the token is unknown or revoked. 403: a token named ``serve``. 404: the server's executor takes no workers. 412: the worker's CAS is not the server's. 422 or 426: another piloop |

``messages`` and ``send`` are long-polls: the server awaits a commit between two reads (``Route.long_poll``), and holds
no thread.

After the GitHub Actions runner (``acquirejob``, ``renewjob``) and Buildkite agent v3 (``accept``, ``start``,
``finish``). A grant names its job by address in the CAS, and the job names its data so: the worker reads and writes the
CAS itself, and no bytes come through these routes. ``send`` asks the server to put the versions that the worker's
clone lacks into the CAS, and answers their addresses (``transfers``).
``secrets`` answers the values of the secrets that an agent's job names, which never go into the CAS.
"""

import math
import threading
from collections.abc import Callable, Mapping, Sequence
from enum import StrEnum
from functools import partial
from http import HTTPStatus
from importlib.metadata import version
from time import monotonic

import httpx2 as httpx
from covalence import Cid
from pydantic import BaseModel, Field, RootModel, ValidationError

from piloop.api.contract import MAX_POLL_SECONDS, Polled
from piloop.db.models import NoLease
from piloop.ids import Revision
from piloop.net import Changed, Refusal, Route
from piloop.net.tokens import Caller
from piloop.otel import Tracer
from piloop.plugins.cas.contract import BlobAddress, Cas
from piloop.plugins.executor.contract import (
    AgentReport,
    EvaluationReport,
    HarnessSession,
    JobSecrets,
    MissingBlobs,
    RepoId,
    Report,
)
from piloop.plugins.executor.kit.capacity import HostSample
from piloop.plugins.executor.kit.client import PROTOCOL, Dismissed, Local, Registration, WorkerInfo
from piloop.plugins.executor.kit.clones import NotSent
from piloop.plugins.executor.kit.door import RELAY_SECONDS, Call, Relayed
from piloop.plugins.executor.kit.leases import (
    DEFAULT_TIMING,
    SERVE,
    Answer,
    Drain,
    Grant,
    JobHandle,
    Leases,
    Message,
    NotAddressed,
    PromptMessage,
    Revoke,
    StaleLease,
    Work,
)
from piloop.plugins.executor.kit.relay import LeaseCall, Run
from piloop.plugins.executor.kit.transfers import Send
from piloop.plugins.vcs.contract import MAX_HAVE

FIRST_BACKOFF = 0.2
MAX_BACKOFF = 5.0
"""Seconds between two tries of a call that got no answer: doubling from the first, up to the most."""
ATTEMPT_SECONDS = 30.0
"""The longest one try waits for its answer, beyond a long-poll's own wait."""
UNREACHABLE_EVERY = 60.0
"""How often a worker whose server does not answer says so again (``worker.unreachable``)."""


class Failure(StrEnum):
    """Why a lease route refused a call: the ``error`` of its body."""

    STALE_LEASE = 'stale_lease'
    NOT_ADDRESSED = 'not_addressed'
    MISSING_BLOBS = 'missing_blobs'
    NOT_SENT = 'not_sent'
    DISMISSED = 'dismissed'
    RESERVED = 'reserved'
    PROTOCOL = 'protocol'
    OTHER_CAS = 'other_cas'


CONFLICTS: dict[type[Exception], Failure] = {
    StaleLease: Failure.STALE_LEASE,
    NotAddressed: Failure.NOT_ADDRESSED,
    MissingBlobs: Failure.MISSING_BLOBS,
    NotSent: Failure.NOT_SENT,
    Dismissed: Failure.DISMISSED,
}
"""What a lease call raises, and the ``error`` of the 409 that it becomes. ``Remote`` raises it again."""
RAISED: dict[Failure, type[Exception]] = {failure: exception for exception, failure in CONFLICTS.items()}
RETRIED = frozenset({HTTPStatus.REQUEST_TIMEOUT, HTTPStatus.TOO_MANY_REQUESTS})
"""The 4xx that a proxy in front of the server answers for a moment, tried again as a 5xx is."""


class Acquire(BaseModel):
    """The body of ``acquire``."""

    request_id: str
    sample: HostSample


class Acquired(RootModel[Grant | NoLease]):
    """The answer of ``acquire``: a lease, or why there is none."""


class Started(BaseModel):
    """The body of ``started``."""

    lease: int
    handle: JobHandle
    session_id: str | None


class Heartbeat(BaseModel):
    """The body of ``heartbeat``."""

    leases: dict[int, HarnessSession]
    sample: HostSample


class Answers(RootModel[dict[int, Answer]]):
    """The answer of ``heartbeat``: one for each lease."""


class Reported(BaseModel):
    """The body of ``report``."""

    lease: int
    request_id: str
    report: Report


class Receipt(RootModel[dict[str, object]]):
    """The answer of ``report``: what it applied."""


class Lost(BaseModel):
    """The body of ``lost``."""

    lease: int
    reason: str


class Read(Polled):
    """The body of ``messages``: the worker's cursor, and how long to wait for a message after it."""

    after: int


class Messages(RootModel[list[Message]]):
    """The answer of ``messages``, oldest first."""


class Queued(BaseModel):
    """The body of ``queued``."""

    message: int
    error: str | None


class SecretsAsked(BaseModel):
    """The body of ``secrets``."""

    lease: int


class SendAsked(Polled):
    """The body of ``send``: versions of a repo of record, the versions that the worker's clone has, and the wait."""

    repo: RepoId
    revisions: list[Revision]
    have: list[Revision] = Field(max_length=MAX_HAVE)


class Sent(RootModel[list[BlobAddress]]):
    """The answer of ``send``: the addresses in the CAS to take, in order. Empty: the clone has the versions already."""


def lease_routes(leases: Leases, changed: Changed, cas: Cas, send: Send, cli: Run) -> list[Route]:
    """The routes of the ``LeaseClient`` calls, each made on ``Local`` as the caller, ``send`` and ``cli``.

    ``changed`` is ``Database.changed`` of the leases' database: what ``messages`` and ``send`` await between two
    reads. ``cas`` is the server's, which ``register`` checks that the worker shares (``WorkerInfo.cas_probe``).
    ``send`` puts versions from the repo of record into it (``FirstTransfers.send``). ``cli`` runs a relayed call of an
    agent, on a live lease of the caller (``relay.Runner``).
    """
    return [
        Route('register', WorkerInfo, _as_caller(leases, partial(_register, cas))),
        Route('acquire', Acquire, _as_caller(leases, _acquire)),
        Route('started', Started, _as_caller(leases, _started)),
        Route('heartbeat', Heartbeat, _as_caller(leases, _heartbeat)),
        Route('report', Reported, _as_caller(leases, _report)),
        Route('lost', Lost, _as_caller(leases, _lost)),
        Route.long_poll('messages', Read, _as_caller(leases, _messages), changed),
        Route('queued', Queued, _as_caller(leases, _queued)),
        Route('secrets', SecretsAsked, _as_caller(leases, _secrets)),
        Route.long_poll('send', SendAsked, partial(_send, send), changed),
        Route('cli', LeaseCall, _as_caller(leases, partial(_cli, cli))),
    ]


def _cli(run: Run, worker: Local, body: LeaseCall) -> Relayed:
    return run(worker.relayed(body.lease), body)


def _send(send: Send, caller: Caller, body: SendAsked) -> Sent | None:
    try:
        sent = send(body.repo, body.revisions, body.have)
    except NotSent as e:
        raise Refusal(HTTPStatus.CONFLICT, Failure.NOT_SENT, str(e)) from e
    return None if sent is None else Sent(sent)


def _as_caller[I: BaseModel](
    leases: Leases, call: Callable[[Local, I], BaseModel | None]
) -> Callable[[Caller, I], BaseModel | None]:
    """Make ``call`` on ``Local`` as the caller, and turn what the leases refuse into a ``Refusal``."""

    def handle(caller: Caller, parsed: I) -> BaseModel | None:
        if caller.name == SERVE:
            raise Refusal(
                HTTPStatus.FORBIDDEN, Failure.RESERVED, f'{SERVE} is the worker inside piloop serve: name the token'
                ' after the machine that uses it'
            )  # fmt: skip
        try:
            return call(Local(leases, caller.name), parsed)
        except tuple(CONFLICTS) as e:
            raise Refusal(HTTPStatus.CONFLICT, CONFLICTS[type(e)], str(e)) from e

    return handle


def _register(cas: Cas, worker: Local, info: WorkerInfo) -> Registration:
    if info.protocol != PROTOCOL:
        raise Refusal(
            HTTPStatus.UPGRADE_REQUIRED,
            Failure.PROTOCOL,
            f'the server runs piloop {version("piloop")}, with protocol {PROTOCOL}, and the worker protocol'
            f' {info.protocol}: install the same piloop on both machines',
        )
    if cas.missing([info.cas_probe]):
        raise Refusal(
            HTTPStatus.PRECONDITION_FAILED,
            Failure.OTHER_CAS,
            f"this worker's CAS is not the server's: the server's lacks {info.cas_probe}, which the worker just put"
            " in its own. Give the worker the server's [plugins.cas]",
        )
    return worker.register(info)


def _acquire(worker: Local, body: Acquire) -> Acquired:
    return Acquired(worker.acquire(body.request_id, body.sample))


def _started(worker: Local, body: Started) -> None:
    worker.started(body.lease, body.handle, body.session_id)


def _heartbeat(worker: Local, body: Heartbeat) -> Answers:
    return Answers(worker.heartbeat(body.leases, body.sample))


def _report(worker: Local, body: Reported) -> Receipt:
    return Receipt(worker.report(body.lease, body.request_id, body.report))


def _lost(worker: Local, body: Lost) -> None:
    worker.lost(body.lease, body.reason)


def _messages(worker: Local, body: Read) -> Messages | None:
    """One read, which waits for nothing: the server awaits the next commit between two (``Route.long_poll``)."""
    return Messages(found) if (found := list(worker.messages(body.after, 0))) else None


def _queued(worker: Local, body: Queued) -> None:
    worker.queued(body.message, body.error)


def _secrets(worker: Local, body: SecretsAsked) -> JobSecrets:
    return worker.secrets(body.lease)


class RefusalBody(BaseModel):
    """The body of a refusal (``net.Refusal``), or FastAPI's ``{"detail": …}``, which has no ``error``."""

    error: Failure | None = None
    detail: object = None


class Resend(StrEnum):
    """When ``Remote`` sends a call again."""

    ANSWERLESS = 'answerless'
    """After a call that got no answer, or a 5xx: the call is idempotent, by its request id or its content."""
    UNSENT = 'unsent'
    """Only after a connection that never opened: the call, a relayed ``piloop`` command, must not run twice."""


class Remote:
    """``LeaseClient`` over HTTP, through ``http``: a client of the server that ``Network.client`` makes.

    A call that gets no answer, or a 5xx, is sent again with backoff, with the same body and so the same request id,
    for up to the self-stop of the timing that ``register`` gave (``Timing.self_stop``). The worker's own self-stop
    then holds: a call never outlives the lease that it is about. Once ``stop`` is set (the worker stops), a call
    tries once: what it could not send stays in the worker's spool. The first try without an answer says so at once
    (``worker.unreachable``), and then every ``UNREACHABLE_EVERY`` until an answer comes.
    """

    def __init__(self, http: httpx.Client, stop: threading.Event, tracer: Tracer) -> None:
        self._http = http
        self._stop = stop
        self._tracer = tracer
        self._retry_for = DEFAULT_TIMING.self_stop
        self._lock = threading.Lock()
        self._unanswered_since: float | None = None
        """When the server stopped answering. None while it answers."""
        self._said = -math.inf
        """When the worker last said so."""

    def register(self, info: WorkerInfo) -> Registration:
        registration = Registration.model_validate_json(self._post('register', info))
        self._retry_for = registration.timing.self_stop
        return registration

    def acquire(self, request_id: str, sample: HostSample) -> Grant | NoLease:
        answer = self._post('acquire', Acquire(request_id=request_id, sample=sample))
        return Acquired.model_validate_json(answer).root

    def started(self, lease: int, handle: JobHandle, session_id: str | None) -> None:
        self._post('started', Started(lease=lease, handle=handle, session_id=session_id))

    def heartbeat(self, leases: Mapping[int, HarnessSession], sample: HostSample) -> dict[int, Answer]:
        return Answers.model_validate_json(self._post('heartbeat', Heartbeat(leases=dict(leases), sample=sample))).root

    def report(self, lease: int, request_id: str, report: AgentReport | EvaluationReport) -> dict[str, object]:
        answer = self._post('report', Reported(lease=lease, request_id=request_id, report=report))
        return Receipt.model_validate_json(answer).root

    def lost(self, lease: int, reason: str) -> None:
        self._post('lost', Lost(lease=lease, reason=reason))

    def messages(self, after: int, wait: float) -> Sequence[Work | PromptMessage | Revoke | Drain]:
        wait = min(wait, MAX_POLL_SECONDS)
        answer = self._post('messages', Read(after=after, wait=wait), wait)
        return Messages.model_validate_json(answer).root if answer else []

    def queued(self, message: int, error: str | None) -> None:
        self._post('queued', Queued(message=message, error=error))

    def secrets(self, lease: int) -> JobSecrets:
        return JobSecrets.model_validate_json(self._post('secrets', SecretsAsked(lease=lease)))

    def send(self, repo: RepoId, revisions: Sequence[Revision], have: Sequence[Revision]) -> list[Cid]:
        """``Clones``' fetch: the server puts what ``revisions`` have beyond ``have`` into the CAS, and names it.

        It asks again while the server builds the repo's first transfer, until the worker stops. Raises ``NotSent``.
        """
        body = SendAsked(repo=repo, revisions=list(revisions), have=list(have), wait=MAX_POLL_SECONDS)
        while not (answer := self._post('send', body, body.wait)):
            if self._stop.is_set():
                raise NotSent(f'the worker stops while the server builds the first transfer of repo {repo}')
        return Sent.model_validate_json(answer).root

    def _unanswered(self, path: str, error: Exception) -> None:
        """Say that the server did not answer: at the first try of an outage, then every ``UNREACHABLE_EVERY``."""
        now = monotonic()
        with self._lock:
            if self._unanswered_since is None:
                self._unanswered_since, self._said = now, -math.inf
            if now - self._said < UNREACHABLE_EVERY:
                return
            self._said, since = now, self._unanswered_since
        self._tracer.event('worker.unreachable', call=path, seconds=round(now - since), error=str(error))

    def cli(self, lease: int, call: Call) -> Relayed:
        """``Relay``'s send: run an agent's relayed call on the server, on its lease. Raises ``StaleLease``."""
        body = LeaseCall(lease=lease, argv=call.argv, stdin=call.stdin, token=call.token)
        return Relayed.model_validate_json(self._post('cli', body, RELAY_SECONDS, resend=Resend.UNSENT))

    def _post(self, path: str, body: BaseModel, wait: float = 0, *, resend: Resend = Resend.ANSWERLESS) -> bytes:
        """POST ``body`` until the server answers it, and return the body of its success.

        Raises the exception of a refusal, or the last error once the time is up, or at once when ``resend`` says
        that the call may not be sent again.
        """
        deadline = monotonic() + self._retry_for
        backoff = FIRST_BACKOFF
        content = body.model_dump_json()
        while True:
            left = deadline - monotonic()
            try:
                response = self._http.post(
                    f'/v1/executor/{path}',
                    content=content,
                    headers={'content-type': 'application/json'},
                    timeout=min(ATTEMPT_SECONDS, max(left, 0)) + wait,
                )
            except httpx.TransportError as e:
                error: Exception = e
                again = resend is Resend.ANSWERLESS or isinstance(e, httpx.ConnectError)
            else:
                if response.status_code < HTTPStatus.INTERNAL_SERVER_ERROR and response.status_code not in RETRIED:
                    with self._lock:
                        self._unanswered_since = None
                    return _success(response)
                error, again = _failed(response), resend is Resend.ANSWERLESS
            self._unanswered(path, error)
            if not again or monotonic() + backoff >= deadline or self._stop.wait(backoff):
                raise error
            backoff = min(backoff * 2, MAX_BACKOFF)


def _success(response: httpx.Response) -> bytes:
    """The body of a call that the server handled, or the exception of its refusal."""
    if response.is_success:
        return response.content
    try:
        refused = RefusalBody.model_validate_json(response.content)
    except ValidationError:
        refused = RefusalBody(detail=response.text)
    detail = str(refused.detail)
    if response.status_code == HTTPStatus.CONFLICT and refused.error is not None and refused.error in RAISED:
        raise RAISED[refused.error](detail)
    raise Dismissed(f'the server refused this worker ({response.status_code}): {detail}')


def _failed(response: httpx.Response) -> httpx.HTTPStatusError:
    return httpx.HTTPStatusError(
        f'{response.status_code} from {response.request.url.path}: {response.text}',
        request=response.request,
        response=response,
    )
