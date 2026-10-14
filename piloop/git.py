"""Git operations on the project repo and its worktrees."""

import re
import shutil
import threading
from datetime import datetime
from pathlib import Path

from git import Repo
from git.exc import GitCommandError, InvalidGitRepositoryError, NoSuchPathError

WORKTREE_LOCK = threading.Lock()
"""Held for every worktree command of this process.

``git worktree prune`` in one thread can delete the admin directory that a ``git worktree add`` in another thread is
still filling in. Prune checks for the ``locked`` file, then finds a ``gitdir`` that points at a ``.git`` that does not
exist yet, and the add fails with ``Invalid path``. The daemon adds and removes worktrees from many threads at once,
and every remove prunes."""


def open_repo(path: Path) -> Repo:
    """Open the project repository."""
    return Repo(path)


def add_worktree(repo: Repo, path: Path, sha: str, links: dict[str, Path]) -> Repo:
    """Create a detached worktree at ``sha`` and symlink ``links`` (worktree-relative name → target) into it.

    A directory left from an interrupted run is removed first, whether git knows it or not.
    """
    with WORKTREE_LOCK:
        _remove_worktree(repo, path)
        path.parent.mkdir(parents=True, exist_ok=True)
        repo.git.worktree('add', '--detach', '--quiet', str(path), sha)
    for rel, target in links.items():
        dest = path / rel
        if target.exists() and not dest.exists():
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.symlink_to(target)
    return Repo(path)


def linked(path: Path, links: dict[str, Path]) -> list[str]:
    """The ``links`` that are not part of the worktree's tree: the ones ``add_worktree`` symlinked in or did not create.

    A linked name that the tree also has (``.entire`` after ``piloop init``) belongs to the commit and is not returned.
    """
    return [rel for rel in links if not (path / rel).exists() or (path / rel).is_symlink()]


def remove_worktree(repo: Repo, path: Path) -> None:
    """Remove a worktree. Nothing fails if it is already gone, or if it is a plain directory that git does not know."""
    with WORKTREE_LOCK:
        _remove_worktree(repo, path)


def _remove_worktree(repo: Repo, path: Path) -> None:
    if path.exists():
        if path.resolve() in worktrees_under(repo, path.parent):
            repo.git.worktree('remove', '--force', str(path))
        else:
            shutil.rmtree(path)
    repo.git.worktree('prune')


def worktrees_under(repo: Repo, root: Path) -> list[Path]:
    """The registered worktrees whose path is inside ``root``."""
    root = root.resolve()  # git records realpaths
    out: list[Path] = []
    for line in repo.git.worktree('list', '--porcelain').splitlines():
        if line.startswith('worktree '):
            wt = Path(line.removeprefix('worktree '))
            if wt.is_relative_to(root):
                out.append(wt)
    return out


def pin(repo: Repo, ref: str, sha: str) -> None:
    """Point ``ref`` at ``sha``, so that garbage collection keeps the commit without adding a branch."""
    repo.git.update_ref(ref, sha)


def pinned(repo: Repo, prefix: str) -> list[str]:
    """The commits pinned as ``<prefix><n>``, in ``n`` order."""
    lines = repo.git.for_each_ref('--format=%(refname) %(objectname)', prefix).splitlines()
    refs = [line.split() for line in lines]
    return [sha for _, sha in sorted(refs, key=lambda ref: int(ref[0].removeprefix(prefix)))]


def merge(repo: Repo, onto: str, sha: str, message: str) -> str | list[str]:
    """Merge commit ``sha`` onto ``onto`` without a worktree.

    The merge commit has ``onto`` and ``sha`` as parents. ``git merge-tree --write-tree`` (git 2.38 or later) finds
    the merge base itself, so when the two histories already share commits (an earlier merge of the same run), only
    the new changes are merged.

    Returns:
        The sha of the merge commit, or the paths that conflict.

    Raises:
        GitCommandError: Git failed for a reason other than a conflict, for example a commit the repo does not have.
    """
    argv = ('--write-tree', '--name-only', '--no-messages', onto, sha)
    status, out, err = repo.git.merge_tree(*argv, with_extended_output=True, with_exceptions=False)
    lines = out.splitlines()
    if status == 1 and lines:
        return [line for line in lines[1:] if line]
    if status != 0:
        raise GitCommandError(['git', 'merge-tree', *argv], status, err, out)
    return str(repo.git.commit_tree(lines[0], '-p', onto, '-p', sha, '-m', message))


def contains(repo: Repo, sha: str, ancestor: str) -> bool:
    """Whether commit ``ancestor`` is in the history of ``sha``, or is ``sha``.

    Raises:
        GitCommandError: The repo does not have one of the two commits.
    """
    argv = ('--is-ancestor', ancestor, sha)
    status, out, err = repo.git.merge_base(*argv, with_extended_output=True, with_exceptions=False)
    if status not in (0, 1):
        raise GitCommandError(['git', 'merge-base', *argv], status, err, out)
    return status == 0


def is_dirty(wt: Repo, ignore: list[str]) -> bool:
    """Whether the worktree has uncommitted changes, ignoring the paths in ``ignore``."""
    return bool(wt.git.status('--porcelain', '--', '.', *(f':(exclude){p}' for p in ignore)))


def commit_all(wt: Repo, message: str, ignore: list[str], *, amend: bool = False) -> None:
    """Stage everything except ``ignore`` and commit it, or add it to the previous commit when ``amend`` is true.

    ``git add`` refuses ``:(exclude)`` pathspecs that name ignored files, so the paths in ``ignore`` are unstaged
    after the ``add``.
    """
    wt.git.add('-A', '--', '.')
    wt.git.rm('-rq', '--cached', '--ignore-unmatch', '--', *ignore)
    if amend:
        wt.git.commit('-q', '--amend', '--no-edit')
    else:
        wt.git.commit('-qm', message)


def tree_sha(repo: Repo, sha: str) -> str:
    """The tree that a commit points at. Equal trees have equal contents, whatever their history."""
    return str(repo.commit(sha).tree.hexsha)


def subject_line(repo: Repo, sha: str) -> str:
    """The first line of a commit message."""
    return str(repo.commit(sha).summary)


def eval_skipped(repo: Repo, sha: str) -> bool:
    """Whether the commit's own message has a ``Piloop-Eval: skip`` trailer. Keys are not case-sensitive, as in git."""
    return any(value.lower() == 'skip' for value in trailers(repo, sha, 'Piloop-Eval'))


def trailers(repo: Repo, sha: str, key: str) -> list[str]:
    """The values of the commit's own ``key`` trailers, in order. Keys are not case-sensitive, as in git."""
    return [value.strip() for k, value in repo.commit(sha).trailers_list if k.lower() == key.lower() and value.strip()]


def head_time(path: Path) -> datetime | None:
    """When HEAD of the checkout at ``path`` was committed. None when there is no checkout there."""
    try:
        return Repo(path).head.commit.committed_datetime
    except NoSuchPathError, InvalidGitRepositoryError:
        return None


def moved(path: Path) -> bool:
    """Whether HEAD of the checkout at ``path`` moved after the checkout, for example by a commit or a reset.

    The reflog then has more than one entry. False when there is no checkout there.
    """
    try:
        return len(Repo(path).head.log()) > 1
    except NoSuchPathError, InvalidGitRepositoryError:
        return False


def identity(path: Path) -> str:
    """The repository that ``path`` is, the same for every clone of it: ``projects.repo``.

    It is the URL of the ``origin`` remote (or else of the first remote by name) as ``host/owner/name``, so
    ``git@github.com:o/r.git`` and ``https://github.com/o/r`` are the same repository. Without a remote, it is the
    absolute path.
    """
    try:
        remotes = {remote.name: remote for remote in Repo(path).remotes}
    except InvalidGitRepositoryError, NoSuchPathError:
        return str(path.resolve())
    name = 'origin' if 'origin' in remotes else min(remotes, default=None)
    if name is None:
        return str(path.resolve())
    url = remotes[name].url.strip().rstrip('/').removesuffix('.git')
    url = re.sub(r'^[a-z+]+://', '', url)  # the scheme
    url = re.sub(r'^[^@/]+@', '', url)  # the user
    return re.sub(r'^([^/:]+):(?!\d+/)', r'\1/', url)  # scp-like host:owner/name; a host:port/ stays
