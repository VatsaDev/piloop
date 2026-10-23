"""A worker's git repos, and commits in and out of them as git bundles in the CAS.

A job names its tree by address (``GitSource``): a repo of record, a commit, and its links as manifests in the CAS. A
worker makes the worktree from its own clone of the repo (``Clones``). A commit that its clone lacks comes as a bundle:
the worker names the commits it has, the server builds a bundle of the rest into the CAS
(``piloop.store.commits.bundle``), as git's own have/want does, and the worker reads it from the CAS by its address. The
commits an agent makes go back the same way, and core imports them into the repo of record
(``piloop.store.commits.unbundle``). No bytes go through the server. The worker in ``piloop serve`` takes this path too:
its clone is the repo of record.
"""

from collections.abc import Callable, Sequence
from pathlib import Path

from covalence import Cid
from git import Repo

from piloop import git
from piloop.plugins.cas.contract import Cas
from piloop.plugins.executor.contract import Bundle, Checkout, GitSource, RepoId
from piloop.settings import INPUTS_DIR
from piloop.store import commits
from piloop.store.tree import Copies, Manifest, Materialised, materialise

WORKSPACE_LINK = 'workspace'
"""Where the campaign's shared workspace is linked into a worktree (``GitSource.workspace``)."""

FETCHED = 'refs/piloop/fetched/'
"""Where a clone pins each commit that came as a bundle: it stays, and the next fetch names it as a commit it has."""

type Fetch = Callable[[RepoId, str, Sequence[str]], Cid | None]
"""Ask the server to put a bundle of ``commit`` of ``repo`` beyond the commits ``have`` into the CAS, and return its
address: ``LeaseClient.bundle``."""


class Clones:
    """A worker's git repos, one per repo of record, and the worktrees it makes of them with their links.

    ``repo_of`` says where each repo is, ``fetch`` fills it from bundles, and ``home`` roots the places that a job names
    (``GitSource.workspace``).
    """

    def __init__(self, cas: Cas, repo_of: Callable[[RepoId], Path], fetch: Fetch, home: Path) -> None:
        self._cas = cas
        self._repo_of = repo_of
        self._fetch = fetch
        self._home = home
        self._copies = Copies(cas, home / INPUTS_DIR)

    def path(self, artifact: GitSource | Checkout) -> Path:
        """The worker's clone of the artifact's repo."""
        return self._repo_of(artifact.repo)

    def checkout(self, source: GitSource, cwd: Path) -> Materialised:
        """Make the worktree of ``source`` at ``cwd``: its commit, fetched first if missing, its links and workspace.

        Return what the links' read-only copies are (``Materialised.copies``, ``rebuilt``).
        """
        repo = git.open_repo(self.path(source))
        if not git.has(repo, source.commit):
            blob = self._fetch(source.repo, source.commit, repo.git.for_each_ref('--format=%(objectname)').split())
            if blob is not None:
                commits.unbundle(self._cas, repo, blob, source.commit)
                git.pin(repo, f'{FETCHED}{source.commit}', source.commit)
        git.add_worktree(repo, cwd, source.commit, {})
        links = Materialised()
        for blob in source.links.values():
            done = materialise(self._cas, Manifest.get(self._cas, blob), cwd, into=self._copies)
            links.copies |= done.copies
            links.rebuilt += done.rebuilt
        if source.workspace is not None:
            workspace = self._home / source.workspace
            workspace.mkdir(parents=True, exist_ok=True)
            if not (cwd / WORKSPACE_LINK).exists():
                (cwd / WORKSPACE_LINK).symlink_to(workspace)
        return links

    def ignored(self, source: GitSource) -> list[str]:
        """What the worktree has that is not the agent's work: the links and the workspace that its commit lacks."""
        names = [*source.links, *([WORKSPACE_LINK] if source.workspace is not None else [])]
        return git.untracked(git.open_repo(self.path(source)), source.commit, names)

    def bring_back(self, source: GitSource, cwd: Path, message: str) -> Bundle | None:
        """Commit what the agent left uncommitted, on its own commit, and bundle its commits into the CAS.

        None when it made no commit. The uncommitted work is never amended into the agent's last commit, which is
        scored as the agent made it.
        """
        wt, ignore = Repo(cwd), self.ignored(source)
        if git.is_dirty(wt, ignore):
            git.commit_all(wt, message, ignore)
        head = wt.head.commit.hexsha
        if head == source.commit:
            return None
        blob = commits.bundle(self._cas, wt, head, [source.commit])
        return Bundle(blob=blob, head=head) if blob is not None else None

    def remove(self, artifact: GitSource | Checkout, cwd: Path) -> None:
        """Remove the worktree at ``cwd``, if it made one."""
        if isinstance(artifact, GitSource):
            git.remove_worktree(git.open_repo(self.path(artifact)), cwd)
