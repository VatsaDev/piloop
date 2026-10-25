"""An empty clone starts from the repo's first transfer, which the server builds once, out of any request."""

import subprocess
import threading
import time
from collections.abc import Callable, Generator, Sequence
from functools import partial
from pathlib import Path

import pytest
from covalence import Cid
from git import Repo

from piloop.api.contract import MAX_POLL_SECONDS
from piloop.db.models import Campaign, FirstTransfer
from piloop.db.session import Database
from piloop.ids import Revision
from piloop.net.server import Server, app
from piloop.net.tokens import Tokens
from piloop.otel import Tracer
from piloop.plugins.cas.contract import Cas
from piloop.plugins.executor.contract import RepoId
from piloop.plugins.executor.kit.clones import NotSent
from piloop.plugins.executor.kit.leases import Leases
from piloop.plugins.executor.kit.remote import Remote, lease_routes
from piloop.plugins.executor.kit.transfers import FirstTransfers
from piloop.plugins.network.contract import NetworkHost
from piloop.plugins.network.https import Https
from piloop.plugins.vcs.contract import Repository
from piloop.plugins.vcs.git import Git, GitRepository
from tests.conftest import Dial, disk, wait_until
from tests.test_lease import POOLS, TIMING, Recorder
from tests.test_lease_client import echoed, secret


class Counted(GitRepository):
    """The repo of record, which counts its sends with nothing had, and can be made to fail them or hold them."""

    def __init__(self, path: Path) -> None:
        super().__init__(Repo(path))
        self.full = 0
        self.fail: Exception | None = None
        self.hold = threading.Event()
        self.hold.set()

    def send(self, cas: Cas, revisions: Sequence[Revision], have: Sequence[Revision]) -> Cid | None:
        if not have:
            self.full += 1
            self.hold.wait()
            if self.fail is not None:
                raise self.fail
        return super().send(cas, revisions, have)


@pytest.fixture
def record(campaign: Campaign) -> Counted:
    return Counted(Path(campaign.path))


@pytest.fixture
def store(tmp_path: Path) -> Cas:
    return disk(tmp_path / 'shared')


def transfers(db: Database, store: Cas, record: Repository) -> FirstTransfers:
    return FirstTransfers(db, store, lambda repo: record, Tracer(None))


def answered(send: Callable[[], list[Cid] | None]) -> list[Cid]:
    """What a long-poll gets: ``send`` asked again until it answers."""
    sent: list[Cid] | None = None

    def done() -> bool:
        nonlocal sent
        sent = send()
        return sent is not None

    wait_until(done)
    assert sent is not None
    return sent


def fetched(first: FirstTransfers, repo: RepoId, revisions: Sequence[Revision], have: Sequence[Revision]) -> list[Cid]:
    """A worker's ``Fetch`` through ``first``."""
    return answered(partial(first.send, repo, revisions, have))


def commit(path: Path, name: str) -> Revision:
    (path / name).write_text(name)
    subprocess.run(['git', 'add', name], cwd=path, check=True)
    subprocess.run(['git', 'commit', '-qm', name], cwd=path, check=True)
    return Revision(Repo(path).head.commit.hexsha)


def test_empty_clones_start_from_one_first_transfer_and_take_the_versions_beyond_it(
    tmp_path: Path, db: Database, campaign: Campaign, store: Cas, record: Counted
) -> None:
    """The head moves after the build: a later clone gets the same first transfer, and the rest in its request."""
    repo, path = RepoId(campaign.id), Path(campaign.path)
    first = transfers(db, store, record)
    record.hold.clear()
    head = Revision(record.repo.head.commit.hexsha)
    assert first.send(repo, [head], []) is None, 'it is built out of the request'
    wait_until(lambda: record.full == 1)
    assert first.send(repo, [head], []) is None, 'a retry joins the build'
    record.hold.set()
    assert len(answered(partial(first.send, repo, [head], []))) == 1, 'nothing beyond the head'
    later = commit(path, 'later.txt')
    for n in range(3):
        clone = Git().clone(tmp_path / f'clone{n}')
        clone.fetch(store, [later], partial(fetched, first, repo))
        assert clone.version(later).summary == 'later.txt'
    newer = commit(path, 'newer.txt')
    clone.fetch(store, [newer], partial(fetched, first, repo))  # it names what it has: no first transfer
    assert clone.version(newer).summary == 'newer.txt' and record.full == 1
    with db.session() as s:
        row = s.get(FirstTransfer, repo)
        assert row is not None and row.revision == head
    assert transfers(db, store, record).send(repo, [later], []) is not None, 'a server that restarts reads the row'
    assert record.full == 1


def test_a_failed_build_is_raised_once_and_the_next_request_builds_again(
    db: Database, campaign: Campaign, store: Cas, record: Counted
) -> None:
    repo, head = RepoId(campaign.id), Revision(record.repo.head.commit.hexsha)
    first = transfers(db, store, record)
    record.fail = OSError('no space left on device')
    assert first.send(repo, [head], []) is None
    with pytest.raises(NotSent, match='no space left on device') as raised:
        answered(partial(first.send, repo, [head], []))
    assert isinstance(raised.value.__cause__, OSError)
    record.fail = None
    assert answered(partial(first.send, repo, [head], [])) and record.full == 2


def test_a_first_transfer_that_the_cas_lost_is_built_again(
    tmp_path: Path, db: Database, campaign: Campaign, store: Cas, record: Counted
) -> None:
    repo, head = RepoId(campaign.id), Revision(record.repo.head.commit.hexsha)
    answered(partial(transfers(db, store, record).send, repo, [head], []))
    other = disk(tmp_path / 'another')
    (blob,) = answered(partial(transfers(db, other, record).send, repo, [head], []))
    assert record.full == 2 and other.has(blob)


@pytest.fixture
def worker(db: Database, store: Cas, record: Counted, dial: Dial) -> Generator[Remote]:
    """A worker's client of a server whose ``send`` is ``FirstTransfers``, both on ``store``."""
    network = Https.model_validate({'listen': '127.0.0.1:0'}).open(NetworkHost(Tokens(db)))
    listener = network.listener()
    assert listener is not None
    leases = Leases(db, Recorder(db), store, TIMING, POOLS)
    server = Server(
        app(
            Tokens(db), network, lease_routes(leases, db.changed, store, transfers(db, store, record).send, echoed), ()
        ),
        listener,
    )
    server.start()
    http = dial(server.url, secret(db, 'w1'))
    yield Remote(http, threading.Event(), Tracer(None))
    http.close()
    server.close()


def test_a_worker_waits_on_the_route_until_the_first_transfer_is_built(
    worker: Remote, campaign: Campaign, record: Counted
) -> None:
    """The build's commit wakes the long-poll: the answer comes well before the poll's wait ends. A failed build is
    the worker's ``NotSent``, which fails its job."""
    repo, head = RepoId(campaign.id), Revision(record.repo.head.commit.hexsha)
    record.fail = OSError('no space left on device')
    started = time.monotonic()
    with pytest.raises(NotSent, match='no space left on device'):
        worker.send(repo, [head], [])
    record.fail = None
    record.hold.clear()
    threading.Timer(0.5, record.hold.set).start()
    assert len(worker.send(repo, [head], [])) == 1
    assert time.monotonic() - started < MAX_POLL_SECONDS
