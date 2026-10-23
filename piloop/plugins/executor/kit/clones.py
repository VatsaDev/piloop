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
from piloop.plugins.executor.contract import Bundle, GitSource, RepoId
from piloop.settings import INPUTS_DIR
from piloop.store import commits
from piloop.store.tree import Copies, Copy, Manifest, Materialised, Seen, changed, materialise

WORKSPACE = 'workspace'
"""Where the campaign's shared workspace goes in a worktree (``GitSource.workspace``)."""

FETCHED = 'refs/piloop/fetched/'
"""Where a clone pins each commit that came as a bundle: it stays, and the next fetch names it as a commit it has."""

type Fetch = Callable[[RepoId, Sequence[str], Sequence[str]], Cid | None]
"""Ask the server to put a bundle of commits of ``repo`` beyond the commits ``have`` into the CAS, and return its
address: ``Remote.bundle``."""


class Clones:
    """A worker's git repos, one per repo of record, and the worktrees it makes of them with their links.

    ``repo_of`` says where each repo is, ``fetch`` fills it from bundles, and ``home`` holds the read-only copies that
    worktrees share (``Copies``).
    """

    def __init__(self, cas: Cas, repo_of: Callable[[RepoId], Path], fetch: Fetch, home: Path) -> None:
        self._cas = cas
        self._repo_of = repo_of
        self._fetch = fetch
        self._copies = Copies(cas, home / INPUTS_DIR)
        self._workspaces: dict[Path, dict[str, Copy]] = {}
        """The workspace copies of each worktree, by its path, as ``checkout`` made them: ``changed`` reads only those
        whose stat changed. A worker that restarts reads them all."""
        self._seen: Seen = {}

    def path(self, artifact: GitSource) -> Path:
        """The worker's clone of the artifact's repo."""
        return self._repo_of(artifact.repo)

    def checkout(self, source: GitSource, cwd: Path) -> Materialised:
        """Make the worktree of ``source`` at ``cwd``, with its links and workspace.

        Its commit and its ``history`` come from the server first when the clone lacks them.

        Return what the links' read-only copies are (``Materialised.copies``, ``rebuilt``).
        """
        repo = git.open_repo(self.path(source))
        if missing := [sha for sha in dict.fromkeys([source.commit, *source.history]) if not git.has(repo, sha)]:
            blob = self._fetch(source.repo, missing, repo.git.for_each_ref('--format=%(objectname)').split())
            if blob is not None:
                commits.unbundle(self._cas, repo, blob, missing)
                for sha in missing:
                    if git.has(repo, sha):  # the server leaves out a commit that its repo lacks
                        git.pin(repo, f'{FETCHED}{sha}', sha)
        git.add_worktree(repo, cwd, source.commit, {})
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

    def changed(self, source: GitSource, cwd: Path) -> dict[str, Cid | None]:
        """The files of the worktree's workspace that the job wrote, changed, added (put into the CAS) or removed."""
        if source.workspace is None or WORKSPACE not in self.ignored(source):
            return {}
        base = Manifest.get(self._cas, source.workspace.manifest)
        return changed(self._cas, cwd / WORKSPACE, base, self._workspaces.get(cwd, {}), self._seen)

    def ignored(self, source: GitSource) -> list[str]:
        """What the worktree has that is not the agent's work: the links and the workspace that its commit lacks."""
        names = [*source.links, *([WORKSPACE] if source.workspace is not None else [])]
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
        blob = commits.bundle(self._cas, wt, [head], [source.commit])
        return Bundle(blob=blob, head=head) if blob is not None else None

    def remove(self, artifact: GitSource, cwd: Path) -> None:
        """Remove the worktree at ``cwd``."""
        self._workspaces.pop(cwd, None)
        git.remove_worktree(git.open_repo(self.path(artifact)), cwd)
