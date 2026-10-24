"""A worker's copies of the repos of record, and the versions in and out of them through the CAS.

A job names its files by address (``Source``): a repo of record, a revision, and its links as manifests in the CAS. A
worker checks the revision out of its own clone of the repo (``Clones``), with the job's vcs provider. A version that
its clone lacks comes through the CAS: the clone names the versions it has, and the server puts the rest into the CAS
(``Repository.send``). The versions an agent saves go back the same way, and core takes them into the repo of record
(``Repository.receive``). No bytes go through the server. The worker in ``piloop serve`` takes this path too: its clone
is the repo of record.
"""

from collections.abc import Callable, Sequence
from functools import partial
from pathlib import Path

from covalence import Cid

from piloop.plugins.cas.contract import Cas
from piloop.plugins.executor.contract import Commits, RepoId, Source
from piloop.plugins.vcs.contract import Repository, Revision, Transfer, Vcs
from piloop.settings import INPUTS_DIR
from piloop.store.tree import Copies, Copy, Manifest, Materialised, Seen, changed, materialise

WORKSPACE = 'workspace'
"""Where the campaign's shared workspace goes in a checkout (``Source.workspace``)."""

type Fetch = Callable[[RepoId, Sequence[Revision], Sequence[Revision]], Cid | None]
"""Ask the server to put what ``revisions`` of ``repo`` have beyond the revisions ``have`` into the CAS, and return its
address: ``Remote.send``."""
type Opener = Callable[[Vcs, Path], Repository]
"""How a machine opens its clone of a repo of record: ``of_record`` or ``own_clone``."""


def of_record(vcs: Vcs, path: Path) -> Repository:
    """The repo of record itself, as the daemon's own worker uses it."""
    return vcs.open(path)


def own_clone(vcs: Vcs, path: Path) -> Repository:
    """A worker's own clone, made empty on first use (``Vcs.clone``)."""
    return vcs.clone(path)


class Clones:
    """A worker's clones, one per repo of record, and the checkouts it makes of them with their links.

    ``repo_of`` says where each clone is, ``opener`` opens it, ``fetch`` fills it through the CAS, and ``home`` holds
    the read-only copies that checkouts share (``Copies``).
    """

    def __init__(self, cas: Cas, repo_of: Callable[[RepoId], Path], opener: Opener, fetch: Fetch, home: Path) -> None:
        self._cas = cas
        self._repo_of = repo_of
        self._opener = opener
        self._fetch = fetch
        self._copies = Copies(cas, home / INPUTS_DIR)
        self._workspaces: dict[Path, dict[str, Copy]] = {}
        """The workspace copies of each checkout, by its path, as ``checkout`` made them: ``changed`` reads only those
        whose stat changed. A worker that restarts reads them all."""
        self._seen: Seen = {}

    def path(self, artifact: Source) -> Path:
        """The worker's clone of the artifact's repo."""
        return self._repo_of(artifact.repo)

    def repository(self, artifact: Source) -> Repository:
        """The worker's clone of the artifact's repo, opened with the artifact's vcs provider."""
        return self._opener(artifact.vcs, self.path(artifact))

    def checkout(self, source: Source, cwd: Path) -> Materialised:
        """Check ``source`` out at ``cwd``, with its links and workspace.

        Its revision and its ``history`` come from the server first when the clone lacks them.

        Return what the links' read-only copies are (``Materialised.copies``, ``rebuilt``).
        """
        repo = self.repository(source)
        repo.fetch(self._cas, [source.revision, *source.history], partial(self._fetch, source.repo))
        repo.checkout(source.revision, cwd)
        links = Materialised()
        for blob in source.links.values():
            done = materialise(self._cas, Manifest.get(self._cas, blob), cwd, into=self._copies)
            links.copies |= done.copies
            links.rebuilt += done.rebuilt
        if source.workspace is not None and not (cwd / WORKSPACE).exists():  # else the tree has its own
            manifest = Manifest.get(self._cas, source.workspace.manifest)
            (cwd / WORKSPACE).mkdir()
            done = materialise(self._cas, manifest, cwd / WORKSPACE, into=self._copies)
            self._workspaces[cwd] = done.copies
            links.rebuilt += done.rebuilt
        return links

    def changed(self, source: Source, cwd: Path) -> dict[str, Cid | None]:
        """The files of the checkout's workspace that the job wrote, changed, added (put into the CAS) or removed."""
        if source.workspace is None or WORKSPACE not in self.ignored(source):
            return {}
        base = Manifest.get(self._cas, source.workspace.manifest)
        return changed(self._cas, cwd / WORKSPACE, base, self._workspaces.get(cwd, {}), self._seen)

    def ignored(self, source: Source) -> list[str]:
        """What the checkout has that is not the agent's work: the links and the workspace that its revision lacks."""
        names = [*source.links, *([WORKSPACE] if source.workspace is not None else [])]
        return self.repository(source).untracked(source.revision, names)

    def bring_back(self, source: Source, cwd: Path, commits: Commits) -> Transfer | None:
        """Save what the agent left unsaved, and put its versions into the CAS (``Repository.capture``)."""
        ignore = self.ignored(source)
        return source.vcs.open(cwd).capture(self._cas, source.revision, commits.summary, commits.run, ignore)

    def remove(self, artifact: Source, cwd: Path) -> None:
        """Remove the checkout at ``cwd``."""
        self._workspaces.pop(cwd, None)
        self.repository(artifact).discard(cwd)
