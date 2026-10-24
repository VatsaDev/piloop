"""The ``LeaseClient`` contract, on ``Local`` and on ``Remote`` over a real socket: the transport is an adapter.

Each test runs twice. A worker is a name: ``Local`` is bound to it, and ``Remote`` calls with a token of that name.
The last tests are of ``Remote`` alone: what only a network does.
"""

import json
import threading
import time
from collections.abc import Callable, Generator, Sequence
from pathlib import Path
from uuid import uuid4

import httpx2 as httpx
import pytest
from covalence import Cid
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pytest_mock import MockerFixture

from piloop.db.models import Campaign, LeaseState, NoLease, TokenScope
from piloop.db.session import Database
from piloop.ids import CampaignId, CommentId
from piloop.net import MAX_POLL_SECONDS, tokens
from piloop.net.server import Server, app
from piloop.net.tokens import Tokens
from piloop.otel import Tracer
from piloop.plugins.executor.contract import AgentJob, AgentKey, MissingBlobs, Prompt, RepoId
from piloop.plugins.executor.kit import remote as remote_module
from piloop.plugins.executor.kit.client import Dismissed, LeaseClient, Local, WorkerInfo
from piloop.plugins.executor.kit.door import Call, Relayed
from piloop.plugins.executor.kit.leases import (
    Answer,
    Grant,
    Leases,
    NotAddressed,
    PromptMessage,
    Revoke,
    StaleLease,
    StaleReport,
    Work,
)
from piloop.plugins.executor.kit.remote import Remote, lease_routes
from piloop.plugins.network.contract import NetworkHost
from piloop.plugins.network.https import Https, HttpsNetwork
from piloop.plugins.vcs.contract import Revision
from tests.conftest import Dial, disk, wait_until
from tests.fixtures.netfault import CutResponse, Proxy
from tests.test_lease import (
    DONE,
    HANDLE,
    HOST,
    POOLS,
    PROBE,
    RECEIPT,
    TIMING,
    Recorder,
    agent_job,
    evaluation_job,
    events,
    lease_row,
    queue,
)

type Connect = Callable[[str], LeaseClient]
INFO = WorkerInfo(version='0.2.0', pool='test', runners=['process'], sample=HOST, cas_probe=PROBE)


SENT: list[tuple[RepoId, list[str], list[str]]] = []


def echoed(job: AgentJob, call: Call) -> Relayed:
    """The server's half of ``cli``: what it ran, as whom, without a run."""
    return Relayed(stdout=f'{job.agent_session}: {" ".join(call.argv)} <{call.stdin}>', exit_code=3)


def bundled(repo: RepoId, revisions: Sequence[Revision], have: Sequence[Revision]) -> Cid | None:
    """The server's half of ``send``, which records what it was asked: versions that the caller has are None."""
    SENT.append((repo, list(revisions), list(have)))
    return None if set(revisions) <= set(have) else Cid.of(''.join(revisions).encode())


@pytest.fixture
def recorder(db: Database) -> Recorder:
    return Recorder(db)


@pytest.fixture
def leases(db: Database, recorder: Recorder) -> Leases:
    return Leases(db, recorder, TIMING, POOLS)


@pytest.fixture
def network(db: Database) -> HttpsNetwork:
    return Https.model_validate({'listen': '127.0.0.1:0'}).open(NetworkHost(Tokens(db)))


@pytest.fixture
def server(network: HttpsNetwork, leases: Leases, db: Database, tmp_path: Path) -> Generator[Server]:
    listener = network.listener()
    assert listener is not None
    cas = disk(tmp_path / 'server')
    assert cas.put(b'piloop worker cas probe\n') == PROBE, 'the workers share it'
    server = Server(app(network, lease_routes(leases, db.changed, cas, bundled, echoed)), listener)
    server.start()
    yield server
    server.close()


def secret(db: Database, name: str, scope: TokenScope = TokenScope.WORKER) -> str:
    with db.session() as s:
        made = tokens.create(s, name, scope)
        s.commit()
    return made


@pytest.fixture(params=['local', 'remote'])
def connect(request: pytest.FixtureRequest, db: Database, leases: Leases) -> Generator[Connect]:
    """Make the client of a worker, by name."""
    if request.param == 'local':
        yield lambda worker: Local(leases, worker)
        return
    server, dial = request.getfixturevalue('server'), request.getfixturevalue('dial')
    clients: list[httpx.Client] = []

    def remote(worker: str) -> LeaseClient:
        clients.append(http := dial(server.url, secret(db, worker)))
        return Remote(http, threading.Event(), Tracer(None))

    yield remote
    for http in clients:
        http.close()


def test_a_worker_registers_as_its_own_name_and_finds_its_leases_again_with_their_handles(
    connect: Connect, db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    w1 = connect('w1')
    joined = w1.register(INFO)
    assert (joined.worker, joined.held) == ('w1', [])
    queue(db, agent_job(db, campaign, tmp_path))
    grant = w1.acquire(uuid4().hex, HOST)
    assert isinstance(grant, Grant)
    w1.started(grant.lease, HANDLE, 'session-1')
    [held] = w1.register(INFO).held
    assert (held.lease, held.handle, held.job) == (grant.lease, HANDLE, grant.job)


def test_an_acquire_sent_again_with_its_request_id_gets_the_same_lease_and_takes_no_other(
    connect: Connect, db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    w1 = connect('w1')
    w1.register(INFO)
    assert w1.acquire(uuid4().hex, HOST) is NoLease.NOTHING
    first = queue(db, agent_job(db, campaign, tmp_path))
    second = queue(db, agent_job(db, campaign, tmp_path))
    request = uuid4().hex
    grant = w1.acquire(request, HOST)
    assert isinstance(grant, Grant) and grant.lease == first
    assert w1.acquire(request, HOST) == grant, 'the same grant'
    assert lease_row(db, second).state is LeaseState.QUEUED, 'no second lease taken'


def test_a_lease_is_renewed_revoked_and_reported_once_with_its_receipt(
    connect: Connect, db: Database, campaign: Campaign, tmp_path: Path, leases: Leases
) -> None:
    w1 = connect('w1')
    w1.register(INFO)
    job = agent_job(db, campaign, tmp_path)
    lease = queue(db, job)
    grant = w1.acquire(uuid4().hex, HOST)
    assert isinstance(grant, Grant)
    w1.started(lease, HANDLE, None)
    assert w1.heartbeat({lease: 'session-1'}, HOST) == {lease: Answer.RENEWED}
    leases.revoke(AgentKey(job.agent_session))
    assert w1.heartbeat({lease: 'session-1'}, HOST) == {lease: Answer.REVOKED}
    request = uuid4().hex
    assert w1.report(lease, request, DONE) == RECEIPT
    assert w1.report(lease, request, DONE) == RECEIPT, 'sent again: the same receipt'
    assert events(db, StaleReport) == []
    assert w1.heartbeat({lease: None}, HOST) == {lease: Answer.STALE}


def test_a_worker_cannot_act_on_another_workers_lease(
    connect: Connect, db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    """The worker is the caller: a lease taken by w1 is stale to w2, whatever w2 sends."""
    w1, w2 = connect('w1'), connect('w2')
    w1.register(INFO)
    w2.register(INFO)
    lease = queue(db, agent_job(db, campaign, tmp_path))
    assert isinstance(w1.acquire(uuid4().hex, HOST), Grant)
    with pytest.raises(StaleLease):
        w2.started(lease, HANDLE, None)
    assert w2.heartbeat({lease: None}, HOST) == {lease: Answer.STALE}
    with pytest.raises(StaleLease):
        w2.lost(lease, 'not mine')
    with pytest.raises(StaleLease):
        w2.report(lease, uuid4().hex, DONE)
    [(_, _, kept)] = events(db, StaleReport)
    assert (kept.lease, kept.worker) == (lease, 'w2')
    assert lease_row(db, lease).state is LeaseState.ACQUIRED, 'still w1s'


def test_a_worker_reads_work_for_every_worker_and_only_its_own_prompts_and_revokes(
    connect: Connect, db: Database, campaign: Campaign, tmp_path: Path, leases: Leases, recorder: Recorder
) -> None:
    """The GitHub Actions runner's long-poll: it returns at once when a message waits. Each message is an event."""
    w1, w2 = connect('w1'), connect('w2')
    start = w1.register(INFO).cursor
    assert w2.register(INFO).cursor == start, 'a new worker starts at the newest event'
    assert w1.messages(start, 0.01) == [], 'nothing waits: it waits, then returns empty'
    job = agent_job(db, campaign, tmp_path)
    leases.submit(job)
    leases.submit(agent_job(db, campaign, tmp_path))
    works = w1.messages(start, 5)
    assert [type(m) for m in works] == [Work, Work] and w2.messages(start, 5) == works, 'one Work for each job, to each'
    assert w1.messages(works[-1].id, 0.01) == [], 'read past'
    grant = w1.acquire(uuid4().hex, HOST)
    assert isinstance(grant, Grant)
    w1.started(grant.lease, HANDLE, None)
    leases.prompt(AgentKey(job.agent_session), Prompt(CommentId(7), 'try lzma'))
    leases.revoke(AgentKey(job.agent_session))
    prompt, stop = w1.messages(works[-1].id, 5)
    assert isinstance(prompt, PromptMessage) and (prompt.lease, prompt.prompt) == (
        grant.lease,
        Prompt(CommentId(7), 'try lzma'),
    )
    assert isinstance(stop, Revoke) and stop.lease == grant.lease
    assert w2.messages(works[-1].id, 0.01) == [], "another worker's prompts and revokes are not its own"
    with pytest.raises(NotAddressed, match=f'event#{prompt.id} is not a prompt for w2'):
        w2.queued(prompt.id, None)
    with pytest.raises(NotAddressed):
        w1.queued(stop.id, None)
    w1.queued(prompt.id, None)
    w1.queued(prompt.id, None)
    assert [c for c in recorder.calls if c[0] == 'queued'] == [('queued', job.agent_session, (7, None))], 'once'
    assert w1.messages(works[-1].id, 0.01) == [stop], 'a prompt with its outcome is left out, though behind the cursor'


def test_a_long_poll_answers_at_the_commit_that_brings_a_message(
    connect: Connect, db: Database, campaign: Campaign, tmp_path: Path, leases: Leases
) -> None:
    w1 = connect('w1')
    start = w1.register(INFO).cursor
    threading.Timer(0.3, leases.submit, args=(agent_job(db, campaign, tmp_path),)).start()
    began = time.monotonic()
    assert [type(m) for m in w1.messages(start, 20)] == [Work]
    assert time.monotonic() - began < 5, 'woken by the commit, not at the end of its wait'


def test_a_lost_job_ends_its_lease_and_a_second_lost_is_stale(
    connect: Connect, db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    w1 = connect('w1')
    w1.register(INFO)
    lease = queue(db, evaluation_job(db, campaign, tmp_path))
    assert isinstance(w1.acquire(uuid4().hex, HOST), Grant)
    w1.lost(lease, 'gone')
    assert lease_row(db, lease).state is LeaseState.EXPIRED
    with pytest.raises(StaleLease):
        w1.lost(lease, 'gone')


def test_a_report_that_names_blobs_the_cas_lacks_is_refused_once_and_not_tried_again(
    connect: Connect, db: Database, campaign: Campaign, tmp_path: Path, recorder: Recorder, mocker: MockerFixture
) -> None:
    w1 = connect('w1')
    w1.register(INFO)
    lease = queue(db, evaluation_job(db, campaign, tmp_path))
    assert isinstance(w1.acquire(uuid4().hex, HOST), Grant)
    finished = mocker.patch.object(recorder, 'finished', side_effect=MissingBlobs('the CAS lacks x'))
    with pytest.raises(MissingBlobs, match='the CAS lacks x'):
        w1.report(lease, uuid4().hex, DONE)
    assert finished.call_count == 1 and lease_row(db, lease).state is LeaseState.ACQUIRED


def test_a_worker_in_a_pool_that_the_server_lacks_is_dismissed(connect: Connect) -> None:
    with pytest.raises(Dismissed, match="no pool 'gpu'"):
        connect('w1').register(INFO.model_copy(update={'pool': 'gpu'}))


def test_a_report_on_a_lease_that_never_was_is_stale(connect: Connect) -> None:
    w1 = connect('w1')
    w1.register(INFO)
    with pytest.raises(StaleLease, match='there is no lease#99'):
        w1.report(99, uuid4().hex, DONE)


@pytest.fixture
def remote(db: Database, server: Server, dial: Dial) -> Generator[Callable[..., Remote]]:
    clients: list[httpx.Client] = []

    def make(name: str = 'w1', url: str | None = None, scope: TokenScope = TokenScope.WORKER) -> Remote:
        clients.append(http := dial(url or server.url, secret(db, name, scope)))
        return Remote(http, threading.Event(), Tracer(None))

    yield make
    for http in clients:
        http.close()


def test_a_worker_whose_cas_is_not_the_servers_is_dismissed_at_register(remote: Callable[..., Remote]) -> None:
    elsewhere = INFO.model_copy(update={'cas_probe': Cid.of(b'put in another CAS')})
    with pytest.raises(Dismissed, match=r"\(412\): this worker's CAS is not the server's"):
        remote().register(elsewhere)


def test_a_revoked_token_and_the_servers_own_name_are_dismissed(remote: Callable[..., Remote], db: Database) -> None:
    w1 = remote()
    w1.register(INFO)
    with db.session() as s:
        tokens.revoke(s, 'w1')
        s.commit()
    with pytest.raises(Dismissed, match=r'\(401\): the token is unknown or revoked'):
        w1.heartbeat({}, HOST)
    with pytest.raises(Dismissed, match=r'\(403\): serve is the worker inside piloop serve'):
        remote('serve').register(INFO)


def test_a_worker_of_another_protocol_is_dismissed_with_the_servers_version(remote: Callable[..., Remote]) -> None:
    with pytest.raises(
        Dismissed, match=r'\(426\): the server runs piloop .+, with protocol 3, and the worker protocol 1'
    ):
        remote().register(INFO.model_copy(update={'protocol': 1}))


def test_long_polls_hold_no_thread_of_the_server_so_a_heartbeat_still_answers(
    remote: Callable[..., Remote], server: Server, db: Database
) -> None:
    """More long-polls than the server's 40 threads: each awaits a commit outside the pool, so a heartbeat answers
    while every one of them is still parked."""
    workers = [remote(f'w{i}') for i in range(45)]
    cursors = [w.register(INFO).cursor for w in workers]
    polls = [
        threading.Thread(target=w.messages, args=(c, MAX_POLL_SECONDS)) for w, c in zip(workers, cursors, strict=True)
    ]
    for poll in polls:
        poll.start()
    wait_until(lambda: len(db._changed) == len(polls))  # all parked: their first reads are done
    assert workers[0].heartbeat({}, HOST) == {}
    assert all(poll.is_alive() for poll in polls), 'it answered before any long-poll gave up its wait'
    server.close()  # ends the long-polls
    for poll in polls:
        poll.join()


def test_a_call_whose_answer_is_lost_is_sent_again_with_the_same_request_id(
    remote: Callable[..., Remote], server: Server, db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    host, port = server.listener.socket.getsockname()[:2]
    with Proxy((host, port)) as proxy:
        w1 = remote(url=f'http://127.0.0.1:{proxy.port}')
        w1.register(INFO)
        lease = queue(db, evaluation_job(db, campaign, tmp_path))
        proxy.toxic = CutResponse()
        grant = w1.acquire(uuid4().hex, HOST)
        assert proxy.cuts == 1 and isinstance(grant, Grant) and grant.lease == lease, 'the resend got the grant'
        w1.started(lease, HANDLE, None)
        proxy.toxic = CutResponse()
        assert w1.report(lease, uuid4().hex, DONE) == RECEIPT
        assert proxy.cuts == 2 and events(db, StaleReport) == [], 'one report, applied once'


def test_a_call_to_a_server_that_does_not_answer_tries_again_with_backoff_until_the_self_stop(dial: Dial) -> None:
    """Doubling waits, and none that would end past the self-stop. Nothing listens on the discard port."""
    stop = Naps()
    w1 = Remote(dial('http://127.0.0.1:9', 'plt_x'), stop, Tracer(None))
    w1._retry_for = 1.0
    with pytest.raises(httpx.ConnectError):
        w1.heartbeat({}, HOST)
    assert stop.naps == [0.2, 0.4, 0.8]
    stop.set()
    with pytest.raises(httpx.ConnectError):
        w1.heartbeat({}, HOST)
    assert stop.naps == [0.2, 0.4, 0.8, 0.2], 'a stopping worker tries once'


def test_a_worker_whose_server_does_not_answer_says_so_at_the_first_failure_then_once_a_minute(
    dial: Dial, monkeypatch: pytest.MonkeyPatch, tracer: Tracer, spans: InMemorySpanExporter
) -> None:
    """Each call tries again for minutes. The worker's log says at once that the server is gone, not only at the end."""
    stop = Naps()
    monkeypatch.setattr(remote_module, 'monotonic', lambda: sum(stop.naps))
    w1 = Remote(dial('http://127.0.0.1:9', 'plt_x'), stop, tracer)
    w1._retry_for = 150
    with pytest.raises(httpx.ConnectError):
        w1.heartbeat({}, HOST)
    with pytest.raises(httpx.ConnectError):
        w1.acquire(uuid4().hex, HOST)
    unreachable = [s.attributes or {} for s in spans.get_finished_spans() if s.name == 'worker.unreachable']
    said = [(a['call'], a['seconds']) for a in unreachable]
    assert said == [('heartbeat', 0), ('heartbeat', 61), ('heartbeat', 121), ('acquire', 182), ('acquire', 242)]


class Naps(threading.Event):
    """A stop that records each wait, and waits for none."""

    def __init__(self) -> None:
        super().__init__()
        self.naps: list[float] = []

    def wait(self, timeout: float | None = None) -> bool:
        assert timeout is not None
        self.naps.append(timeout)
        return self.is_set()


def answering(*responses: httpx.Response) -> tuple[Remote, list[httpx.Request]]:
    """A ``Remote`` whose server gives ``responses`` in turn, and the requests that it got."""
    sent: list[httpx.Request] = []
    replies = iter(responses)

    def reply(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return next(replies)

    http = httpx.Client(base_url='http://server', transport=httpx.MockTransport(reply))
    return Remote(http, Naps(), Tracer(None)), sent


def test_a_5xx_408_or_429_is_tried_again_with_the_same_body_and_any_other_4xx_dismisses() -> None:
    w1, sent = answering(
        httpx.Response(503, text='busy'),
        httpx.Response(429, text='slow down'),
        httpx.Response(408, text='timeout'),
        httpx.Response(200, json='nothing'),
    )
    assert w1.acquire('r1', HOST) is NoLease.NOTHING
    assert len(sent) == 4 and len({r.content for r in sent}) == 1, 'the same request id'
    w1, sent = answering(httpx.Response(422, text='<html>not a body</html>'))
    with pytest.raises(Dismissed, match=r'\(422\): <html>not a body</html>'):
        w1.acquire('r2', HOST)
    assert len(sent) == 1
    w1, sent = answering(httpx.Response(404, json={'detail': 'Not Found'}))
    with pytest.raises(Dismissed, match=r'\(404\): Not Found'):
        w1.register(INFO)


def test_a_long_poll_waits_no_longer_than_the_server_holds_one() -> None:
    w1, sent = answering(httpx.Response(204))
    assert w1.messages(0, 60) == []
    assert json.loads(sent[0].content) == {'wait': 25, 'after': 0}


def test_a_worker_asks_the_server_for_versions_and_gets_their_address_in_the_cas(remote: Callable[..., Remote]) -> None:
    """No bytes come through the route: the server puts the bundle in the CAS, and the worker reads it from there."""
    worker = remote('w1')
    SENT.clear()
    c1, c2, c3 = Revision('c1'), Revision('c2'), Revision('c3')
    assert worker.send(RepoId(CampaignId(3)), [c2, c3], [c1]) == Cid.of(b'c2c3')
    assert worker.send(RepoId(CampaignId(3)), [c1], [c1]) is None, 'the clone has it'
    assert SENT == [(3, ['c2', 'c3'], ['c1']), (3, ['c1'], ['c1'])], "the server knows the campaign's vcs provider"


def test_an_agents_relayed_call_runs_on_its_live_lease_only_and_a_lost_answer_is_not_sent_again(
    remote: Callable[..., Remote], server: Server, db: Database, campaign: Campaign, tmp_path: Path, leases: Leases
) -> None:
    """The fence scopes the call: another worker's lease, an evaluation's, and a lease that ended are refused. A call
    is a ``piloop`` command, which must not run twice, so only a connection that never opened is tried again."""
    host, port = server.listener.socket.getsockname()[:2]
    with Proxy((host, port)) as proxy:
        w1, w2 = remote('w1', f'http://127.0.0.1:{proxy.port}'), remote('w2')
        w1.register(INFO)
        w2.register(INFO)
        job = agent_job(db, campaign, tmp_path)
        lease = queue(db, job)
        assert isinstance(w1.acquire(uuid4().hex, HOST), Grant)
        call = Call(argv=['sql', '-'], stdin='SELECT 1')
        assert w1.cli(lease, call) == Relayed(stdout=f'{job.agent_session}: sql - <SELECT 1>', exit_code=3)
        with pytest.raises(StaleLease):
            w2.cli(lease, call)
        evaluation = queue(db, evaluation_job(db, campaign, tmp_path))
        assert isinstance(w1.acquire(uuid4().hex, HOST), Grant)
        with pytest.raises(StaleLease, match='an evaluation, which relays nothing'):
            w1.cli(evaluation, call)
        with pytest.raises(StaleLease, match='there is no lease#99'):
            w1.cli(99, call)
        proxy.toxic = CutResponse(b'POST /v1/executor/cli')
        with pytest.raises(httpx.TransportError):
            w1.cli(lease, call)
        assert proxy.cuts == 1
        w1.report(lease, uuid4().hex, DONE)
        with pytest.raises(StaleLease, match='is reported'):
            w1.cli(lease, call)
