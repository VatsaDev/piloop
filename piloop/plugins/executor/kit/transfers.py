"""The server's half of a worker's fetch: the versions that a clone lacks, from the repo of record into the CAS.

A clone that has versions names its newest (``MAX_HAVE``), and the server puts what it lacks beyond them into the CAS
(``Repository.send``), in the request. An empty clone names none, and lacks everything: the repo's first transfer.
That can take minutes for a large repo, longer than a request lasts, and every new worker asks for it. So the server
builds it once per repo, in a thread of its own, keeps its address in ``first_transfers``, and answers each empty
clone with that blob and the versions beyond it. A request that comes while it builds is a long-poll: the commit of
the row wakes it.

After git's bundle URIs and Mercurial's clone bundles: a new clone starts from a bundle that the server built once,
then fetches what is newer.
"""

import threading
from collections.abc import Callable, Sequence
from concurrent.futures import Future

from covalence import Cid

from piloop.db.models import FirstTransfer
from piloop.db.session import Database
from piloop.ids import Revision
from piloop.otel import Tracer
from piloop.plugins.cas.contract import Cas
from piloop.plugins.executor.contract import RepoId
from piloop.plugins.executor.kit.clones import NotSent
from piloop.plugins.vcs.contract import Repository

type Send = Callable[[RepoId, Sequence[Revision], Sequence[Revision]], list[Cid] | None]
"""What the ``send`` route answers a worker that lacks versions: ``FirstTransfers.send``. None: ask again."""


def send_of_record(
    cas: Cas,
    records: Callable[[RepoId], Repository],
    repo: RepoId,
    revisions: Sequence[Revision],
    have: Sequence[Revision],
) -> list[Cid]:
    """Put what ``revisions`` have beyond ``have`` from the repo of record into the CAS, in one blob or none.

    The repo of record is opened with its campaign's own vcs provider (``Host.records``), never one that a worker names.
    """
    blob = records(repo).send(cas, revisions, have)
    return [] if blob is None else [blob]


class FirstTransfers:
    """What the ``send`` route answers: ``send_of_record``, from the repo's first transfer for an empty clone."""

    def __init__(self, db: Database, cas: Cas, records: Callable[[RepoId], Repository], tracer: Tracer) -> None:
        self._db = db
        self._cas = cas
        self._records = records
        self._tracer = tracer
        self._lock = threading.Lock()
        self._builds: dict[RepoId, Future[None]] = {}
        """The build of each repo's first transfer, from its start until a request sees it done."""

    def send(self, repo: RepoId, revisions: Sequence[Revision], have: Sequence[Revision]) -> list[Cid] | None:
        """What ``revisions`` have beyond ``have``, as ``Fetch`` answers it. None while the first transfer is built.

        Raises:
            NotSent: The build of the first transfer failed. The next request builds it again.
        """
        if have:
            return send_of_record(self._cas, self._records, repo, revisions, have)
        if (first := self._first(repo)) is None:
            return None
        return [first.blob, *send_of_record(self._cas, self._records, repo, revisions, [first.revision])]

    def _first(self, repo: RepoId) -> FirstTransfer | None:
        """The repo's first transfer, or None while it is built: a request that finds neither starts the build.

        A build leaves ``_builds`` once its row is committed, or when a request raises its failure: a request that
        finds no build finds the row.
        """
        with self._lock:
            if (build := self._builds.get(repo)) is not None:
                if not build.done():
                    return None
                del self._builds[repo]
                error = build.exception()
                raise NotSent(f'the first transfer of repo {repo} failed: {type(error).__name__}: {error}') from error
            with self._db.session() as s:
                row = s.get(FirstTransfer, repo)
            if row is not None and self._cas.has(row.blob):
                return row
            self._builds[repo] = build = Future()
        threading.Thread(target=self._build, args=(repo, build), name=f'first-transfer-{repo}', daemon=True).start()
        return None

    def _build(self, repo: RepoId, build: Future[None]) -> None:
        """Put the repo of record's head and all before it into the CAS, and write its row: the commit wakes the polls.

        A failure stays in ``build`` for the next request to raise, with its cause, and wakes the polls too.
        """
        try:
            with self._tracer.span('piloop.first_transfer', repo=repo):
                record = self._records(repo)
                head = record.head().revision
                blob = record.send(self._cas, [head], [])
                assert blob is not None  # with nothing had, the head is sent
            with self._db.session() as s:
                s.merge(FirstTransfer(campaign_id=repo, revision=head, blob=blob))
                s.commit()
        except Exception as e:
            build.set_exception(e)
            self._db.notify()  # no commit wakes the polls
            return
        with self._lock:
            del self._builds[repo]
        build.set_result(None)
