"""The leases library over HTTP: ``Remote``, a worker's ``LeaseClient``, and ``lease_routes``, the server's half.

Each ``LeaseClient`` call is one ``POST /v1/executor/<call>`` of JSON, on the server of ``piloop serve --listen``. The
route makes the call on ``Local``, bound to the caller's principal: the worker is who its token says, never what its
body says. A refusal has a typed body, ``RefusalBody``. What ``Remote`` does with an answer:

| Status | The worker |
|---|---|
| 409 with an ``error`` of ``CONFLICTS`` | raises its exception: ``StaleLease`` stops the job, ``NotAddressed`` drops an answer to a message that was not its, ``MissingBlobs`` loses the job whose report names blobs that the server's CAS lacks, ``Dismissed`` ends the worker (no such pool) |
| 408, 429, 5xx, or no answer (``RETRIED``) | tries again with backoff, with the same body and so the same request id |
| any other 4xx | ``Dismissed``: stops its jobs, keeps their reports, and exits. 401: the token is unknown or revoked. 403: a token of another scope, or one named ``serve``. 404: the server's executor takes no workers. 412: the worker's CAS is not the server's. 422 or 426: another piloop |

``messages`` is a long-poll: the server awaits a commit between two reads (``Route.long_poll``), and holds no thread.

After the GitHub Actions runner (``acquirejob``, ``renewjob``) and Buildkite agent v3 (``accept``, ``start``,
``finish``). A job names its data by address in the CAS, which the worker reads and writes itself: no bytes come through
these routes. ``bundle`` asks the server to put a commit that the worker's clone lacks into the CAS, and answers its
address.
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
from pydantic import BaseModel, RootModel, ValidationError

from piloop.db.models import NoLease, TokenScope
from piloop.net import MAX_POLL_SECONDS, Changed, Polled, Refusal, Route
from piloop.net.tokens import Caller
from piloop.otel import Tracer
from piloop.plugins.cas.contract import BlobAddress, Cas
from piloop.plugins.executor.contract import AgentReport, EvaluationReport, MissingBlobs, RepoId, Report
from piloop.plugins.executor.kit.capacity import HostSample
from piloop.plugins.executor.kit.client import PROTOCOL, Dismissed, Local, Registration, WorkerInfo
from piloop.plugins.executor.kit.clones import Fetch
from piloop.plugins.executor.kit.leases import (
    DEFAULT_TIMING,
    SERVE,
    Answer,
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
    DISMISSED = 'dismissed'
    RESERVED = 'reserved'
    PROTOCOL = 'protocol'
    OTHER_CAS = 'other_cas'


CONFLICTS: dict[type[Exception], Failure] = {
    StaleLease: Failure.STALE_LEASE,
    NotAddressed: Failure.NOT_ADDRESSED,
    MissingBlobs: Failure.MISSING_BLOBS,
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

    leases: dict[int, str | None]
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


class BundleAsked(BaseModel):
    """The body of ``bundle``: a commit of a repo of record, and the commits that the worker's clone has."""

    repo: RepoId
    commit: str
    have: list[str]


class BundleMade(RootModel[BlobAddress | None]):
    """The answer of ``bundle``: the bundle's address in the CAS, or None when the clone has the commit already."""


def lease_routes(leases: Leases, changed: Changed, cas: Cas, bundle: Fetch) -> list[Route]:
    """The routes of the ``LeaseClient`` calls, each made on ``Local`` as the caller, and ``bundle``.

    ``changed`` is ``Database.changed`` of the leases' database: what ``messages`` awaits between two reads.
    ``cas`` is the server's, which ``register`` checks that the worker shares (``WorkerInfo.cas_probe``). ``bundle``
    builds a bundle from the repo of record into it (``jobs.bundle_of_record``).
    """
    worker = TokenScope.WORKER
    return [
        Route('register', worker, WorkerInfo, _as_caller(leases, partial(_register, cas))),
        Route('acquire', worker, Acquire, _as_caller(leases, _acquire)),
        Route('started', worker, Started, _as_caller(leases, _started)),
        Route('heartbeat', worker, Heartbeat, _as_caller(leases, _heartbeat)),
        Route('report', worker, Reported, _as_caller(leases, _report)),
        Route('lost', worker, Lost, _as_caller(leases, _lost)),
        Route.long_poll('messages', worker, Read, _as_caller(leases, _messages), changed),
        Route('queued', worker, Queued, _as_caller(leases, _queued)),
        Route('bundle', worker, BundleAsked, partial(_bundle, bundle)),
    ]


def _bundle(bundle: Fetch, caller: Caller, body: BundleAsked) -> BundleMade:
    return BundleMade(bundle(body.repo, body.commit, body.have))


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


class RefusalBody(BaseModel):
    """The body of a refusal (``net.Refusal``), or FastAPI's ``{"detail": …}``, which has no ``error``."""

    error: Failure | None = None
    detail: object = None


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

    def heartbeat(self, leases: Mapping[int, str | None], sample: HostSample) -> dict[int, Answer]:
        return Answers.model_validate_json(self._post('heartbeat', Heartbeat(leases=dict(leases), sample=sample))).root

    def report(self, lease: int, request_id: str, report: AgentReport | EvaluationReport) -> dict[str, object]:
        answer = self._post('report', Reported(lease=lease, request_id=request_id, report=report))
        return Receipt.model_validate_json(answer).root

    def lost(self, lease: int, reason: str) -> None:
        self._post('lost', Lost(lease=lease, reason=reason))

    def messages(self, after: int, wait: float) -> Sequence[Work | PromptMessage | Revoke]:
        wait = min(wait, MAX_POLL_SECONDS)
        answer = self._post('messages', Read(after=after, wait=wait), wait)
        return Messages.model_validate_json(answer).root if answer else []

    def queued(self, message: int, error: str | None) -> None:
        self._post('queued', Queued(message=message, error=error))

    def bundle(self, repo: RepoId, commit: str, have: Sequence[str]) -> Cid | None:
        """``Clones``' fetch: the server puts a bundle of ``commit`` beyond ``have`` into the CAS, and names it."""
        return BundleMade.model_validate_json(
            self._post('bundle', BundleAsked(repo=repo, commit=commit, have=list(have)))
        ).root

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

    def _post(self, path: str, body: BaseModel, wait: float = 0) -> bytes:
        """POST ``body`` until the server answers it, and return the body of its success.

        Raises the exception of a refusal, or the last error once the time is up.
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
            else:
                if response.status_code < HTTPStatus.INTERNAL_SERVER_ERROR and response.status_code not in RETRIED:
                    with self._lock:
                        self._unanswered_since = None
                    return _success(response)
                error = _failed(response)
            self._unanswered(path, error)
            if monotonic() + backoff >= deadline or self._stop.wait(backoff):
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
