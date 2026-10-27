"""Git operations on a repository and its worktrees: what the ``git`` provider runs."""

import os
import re
import shutil
import subprocess
import threading
from collections.abc import Iterable, Mapping, Sequence
from datetime import timedelta
from pathlib import Path
from typing import Final
from urllib.parse import urlsplit

from git import Repo
from git.exc import GitCommandError, InvalidGitRepositoryError, NoSuchPathError
from pydantic import BaseModel, ConfigDict, SecretStr

from piloop.plugins.vcs.contract import CloneFailed
from piloop.proc import kill_group

WORKTREE_LOCK = threading.Lock()
"""Held for every worktree command of this process.

``git worktree prune`` in one thread can delete the admin directory that a ``git worktree add`` in another thread is
still filling in. Prune checks for the ``locked`` file, then finds a ``gitdir`` that points at a ``.git`` that does not
exist yet, and the add fails with ``Invalid path``. The daemon adds and removes worktrees from many threads at once,
and every remove prunes."""


CREDENTIAL = 'PILOOP_GIT_CREDENTIAL'
"""The variable that holds a clone's credential, in the environment of its git only."""
HELPER = f'!f() {{ test "$1" = get && echo username=x-access-token && echo "password=${CREDENTIAL}"; }}; f'
"""The credential helper of a clone with a credential: it answers git's ``get`` from ``CREDENTIAL``, so the argv names
the variable, never its value. GitHub and GitLab take a token as the password of any user."""


CLONE_TIMEOUT: Final = timedelta(minutes=10)
"""The longest that ``clone`` waits for git, so that a remote that stalls ends the clone instead of holding its thread."""


class Identity(BaseModel):
    """Who a commit is by: git's ``user.name`` and ``user.email``."""

    model_config = ConfigDict(frozen=True, extra='forbid')

    name: str
    email: str


def commit_env(author: Identity, committer: Identity) -> dict[str, str]:
    """The environment of a git command that commits as ``author`` and ``committer``, whatever git's config says."""
    return {
        'GIT_AUTHOR_NAME': author.name,
        'GIT_AUTHOR_EMAIL': author.email,
        'GIT_COMMITTER_NAME': committer.name,
        'GIT_COMMITTER_EMAIL': committer.email,
    }


def clone(url: str, path: Path, credential: SecretStr | None) -> None:
    """``git clone`` of ``url`` into ``path``, with no prompt, in at most ``CLONE_TIMEOUT``.

    With a credential, the helpers of the user's config are cleared first: none of them gets the credential to store.
    An empty credential is none. A clone that takes longer is killed with the processes that git started. A clone that
    fails leaves no directory: ``path`` is made first, so its removal does not depend on how git ended.
    """
    match credential:
        case SecretStr() if secret := credential.get_secret_value():
            config = ['-c', 'credential.helper=', '-c', f'credential.helper={HELPER}']
            env = {CREDENTIAL: secret}
        case _:
            config, env = [], {}
    path.mkdir(parents=True)
    git = subprocess.Popen(
        ['git', *config, 'clone', '--quiet', '--', url, str(path)],
        env={**os.environ, 'GIT_TERMINAL_PROMPT': '0', **env},
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        start_new_session=True,  # its own group: the helpers that git starts die with it
    )
    try:
        _, stderr = git.communicate(timeout=CLONE_TIMEOUT.total_seconds())
    except subprocess.TimeoutExpired as e:
        kill_group(git)
        git.communicate()
        shutil.rmtree(path)
        raise CloneFailed(_redacted(f'git clone of {url} took longer than {CLONE_TIMEOUT}: it was stopped', env)) from e
    if git.returncode:
        shutil.rmtree(path)
        raise CloneFailed(_redacted(f'git clone of {url} failed: {stderr.decode().strip()}', env))


def _redacted(error: str, env: Mapping[str, str]) -> str:
    for value in env.values():
        error = error.replace(value, '***')
    return error


def open_repo(path: Path) -> Repo:
    """Open the campaign repository."""
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


def commits(repo: Repo, shas: Sequence[str]) -> list[str]:
    """Those of ``shas`` that are commits in ``repo``, in one ``git cat-file``: a campaign can name thousands."""
    if not shas:
        return []
    checked = subprocess.run(
        ['git', 'cat-file', '--batch-check=%(objecttype)'],
        cwd=repo.common_dir,
        input=''.join(f'{sha}^{{commit}}\n' for sha in shas).encode(),
        capture_output=True,
        check=True,
    )
    return [sha for sha, kind in zip(shas, checked.stdout.decode().splitlines(), strict=True) if kind == 'commit']


def untracked(repo: Repo, sha: str, names: Iterable[str]) -> list[str]:
    """The ``names`` that commit ``sha`` does not track: what was put in its worktree from elsewhere, not its tree's."""
    names = list(names)
    tracked = set(repo.git.ls_tree('--name-only', sha, '--', *names).splitlines()) if names else set()
    return [name for name in names if name not in tracked]


def remove_worktree(repo: Repo, path: Path) -> None:
    """Remove a worktree. Nothing fails if it is already gone, or if it is a plain directory that git does not know."""
    with WORKTREE_LOCK:
        _remove_worktree(repo, path)


def _remove_worktree(repo: Repo, path: Path) -> None:
    if path.exists():
        if path.resolve() in _worktrees_under(repo, path.parent):
            repo.git.worktree('remove', '--force', str(path))
        else:
            shutil.rmtree(path)
    repo.git.worktree('prune')


def worktrees_under(repo: Repo, root: Path) -> list[Path]:
    """The registered worktrees whose path is inside ``root``.

    Gotcha: in a recent git, a ``git worktree list`` that overlaps a removal can fail (``Invalid path``), so it
    takes ``WORKTREE_LOCK`` too.
    """
    with WORKTREE_LOCK:
        return _worktrees_under(repo, root)


def _worktrees_under(repo: Repo, root: Path) -> list[Path]:
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


def pin_all(repo: Repo, refs: Mapping[str, str]) -> None:
    """``pin`` each ref at its commit, in one ``git update-ref``."""
    if refs:
        lines = ''.join(f'update {ref} {sha}\n' for ref, sha in refs.items())
        subprocess.run(['git', 'update-ref', '--stdin'], cwd=repo.common_dir, input=lines.encode(), check=True)


def pinned(repo: Repo, prefix: str) -> list[str]:
    """The commits pinned as ``<prefix><n>``, in ``n`` order."""
    lines = repo.git.for_each_ref('--format=%(refname) %(objectname)', prefix).splitlines()
    refs = [line.split() for line in lines]
    return [sha for _, sha in sorted(refs, key=lambda ref: int(ref[0].removeprefix(prefix)))]


def merge(repo: Repo, onto: str, sha: str, message: str, by: Identity) -> str | list[str]:
    """Merge commit ``sha`` onto ``onto`` without a worktree, as a commit ``by`` its author and committer.

    The merge commit has ``onto`` and ``sha`` as parents. The merge base is by tree before history (``merge_tree``):
    when the two histories already share commits (an earlier merge of the same run), only the new changes are merged,
    and so they are when ``sha`` started from a copy of a commit of ``onto`` (a rebase: same tree, another sha).

    Returns:
        The sha of the merge commit, or the paths that conflict.

    Raises:
        GitCommandError: Git failed for a reason other than a conflict, for example a commit the repo does not have.
    """
    merged = merge_tree(repo, onto, sha)
    if isinstance(merged, list):
        return merged
    return str(repo.git.commit_tree(merged, '-p', onto, '-p', sha, '-m', message, env=commit_env(by, by)))


def merge_tree(repo: Repo, onto: str, sha: str) -> str | list[str]:
    """Return the tree of a merge of commit ``sha`` onto ``onto``, or the paths that conflict (see ``merge``).

    ``git merge-tree --write-tree`` (git 2.40 or later for ``--merge-base``) finds the merge base by history. A commit
    of ``sha``'s own history with the tree of a commit of ``onto``'s own history is a later common state, so it is the
    merge base instead (``tree_base``). Otherwise both sides would merge the changes since the older shared commit
    again, and conflict where they made the same change twice.

    Raises:
        GitCommandError: Git failed for a reason other than a conflict.
    """
    base = tree_base(repo, onto, sha)
    argv = ('--write-tree', '--name-only', '--no-messages', *([f'--merge-base={base}'] if base else []), onto, sha)
    status, out, err = repo.git.merge_tree(*argv, with_extended_output=True, with_exceptions=False)
    lines = out.splitlines()
    if status == 1 and lines:
        return [line for line in lines[1:] if line]
    if status != 0:
        raise GitCommandError(['git', 'merge-tree', *argv], status, err, out)
    return lines[0]


def tree_base(repo: Repo, onto: str, sha: str) -> str | None:
    """The newest commit only in ``sha``'s history whose tree is the tree of a commit only in ``onto``'s, or None.

    None also when git cannot list a history, for example of a commit the repo does not have: ``merge_tree`` raises.
    """
    status, theirs, _ = repo.git.log('--format=%T', f'^{sha}', onto, with_extended_output=True, with_exceptions=False)
    if status != 0:
        return None
    trees = set(theirs.split())
    mine = repo.git.log('--format=%H %T', f'^{onto}', sha).splitlines()
    return next((commit for commit, tree in (line.split() for line in mine) if tree in trees), None)


def contains(repo: Repo, sha: str, ancestor: str) -> bool:
    """Whether commit ``sha`` has all of commit ``ancestor``: it is in its history, or merging it changes nothing.

    The merge test finds what history misses: a commit that cherry-picked or squashed ``ancestor``'s changes. A commit
    contains itself.

    Raises:
        GitCommandError: The repo does not have one of the two commits.
    """
    argv = ('--is-ancestor', ancestor, sha)
    status, out, err = repo.git.merge_base(*argv, with_extended_output=True, with_exceptions=False)
    if status not in (0, 1):
        raise GitCommandError(['git', 'merge-base', *argv], status, err, out)
    if status == 0:
        return True
    merged = merge_tree(repo, sha, ancestor)
    return isinstance(merged, str) and merged == tree_sha(repo, sha)


def is_dirty(wt: Repo, ignore: list[str]) -> bool:
    """Whether the worktree has uncommitted changes, ignoring the paths in ``ignore``."""
    return bool(wt.git.status('--porcelain', '--', '.', *(f':(exclude){p}' for p in ignore)))


def commit_all(wt: Repo, message: str, ignore: list[str], author: Identity, committer: Identity) -> None:
    """Stage everything except ``ignore`` and commit it, by ``author`` and ``committer``.

    ``git add`` refuses ``:(exclude)`` pathspecs that name ignored files, so the paths in ``ignore`` are unstaged
    after the ``add``.
    """
    wt.git.add('-A', '--', '.')
    if ignore:  # git rm takes no empty pathspec
        wt.git.rm('-rq', '--cached', '--ignore-unmatch', '--', *ignore)
    wt.git.commit('-qm', message, env=commit_env(author, committer))


def staged(wt: Repo) -> bytes:
    """The staged changes as a binary patch onto HEAD, which ``git apply`` restores."""
    return bytes(wt.git.diff('--cached', '--binary', stdout_as_string=False, strip_newline_in_stdout=False))


def tree_sha(repo: Repo, sha: str) -> str:
    """The tree that a commit points at. Equal trees have equal contents, whatever their history."""
    return str(repo.commit(sha).tree.hexsha)


def subject_line(repo: Repo, sha: str) -> str:
    """The first line of a commit message."""
    return str(repo.commit(sha).summary)


def message(repo: Repo, sha: str) -> str:
    """The message of a commit."""
    return str(repo.git.show('-s', '--format=%B', sha))


def trailers(message: str, key: str) -> list[str]:
    """The values of the ``key`` trailers of a commit's ``message``, in order. Keys are not case-sensitive, as in git.

    A trailer is a ``key: value`` line anywhere after the subject. Git reads trailers only in the last paragraph, but
    a harness adds its own (``Co-Authored-By``, a "Generated with" line) in paragraphs after the implementer's. A line that
    starts with whitespace continues the value of the trailer above it, as in git.
    """
    line = re.compile(rf'{re.escape(key)}[ \t]*:(.*)', re.IGNORECASE)
    found: list[list[str]] = []
    folding = False
    for text in message.splitlines()[1:]:
        if folding and text[:1].isspace() and text.strip():
            found[-1].append(text.strip())
        elif match := line.fullmatch(text):
            found.append([match[1].strip()])
            folding = True
        else:
            folding = False
    values = (' '.join(part for part in parts if part) for parts in found)
    return [value for value in values if value]


def changed(path: Path, ignore: list[str]) -> bool:
    """Whether the checkout at ``path`` changed: it has uncommitted changes outside ``ignore``, or its HEAD moved.

    HEAD moved after the checkout, for example by a commit or a reset, when the reflog has more than one entry. False
    when there is no checkout there.
    """
    try:
        wt = Repo(path)
        return len(wt.head.log()) > 1 or is_dirty(wt, ignore)
    except NoSuchPathError, InvalidGitRepositoryError:
        return False


def identity(path: Path) -> str:
    """The repository that ``path`` is, the same for every clone of it: ``campaigns.repo``.

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
    return remote_identity(remotes[name].url)


SCP_LIKE = re.compile(r'(?:[^@/]+@)?(?P<host>[^/:]+):(?P<path>.*)')
"""git's scp-like remote, ``[user@]host:path``: a colon before any slash, and no scheme."""
DEFAULT_PORTS = {'ssh': 22, 'git+ssh': 22, 'ssh+git': 22, 'git': 9418, 'http': 80, 'https': 443}


def remote_identity(url: str) -> str:
    """Return a remote URL as ``host[:port]/owner/name``: no scheme, user or default port, and the host in lower case."""
    url = url.strip()
    if '://' not in url and (scp := SCP_LIKE.fullmatch(url)):
        host, path = scp['host'].lower(), scp['path']
    else:
        parts = urlsplit(url)
        host, path = parts.hostname or '', parts.path
        if parts.port is not None and parts.port != DEFAULT_PORTS.get(parts.scheme.lower()):
            host = f'{host}:{parts.port}'
    path = path.rstrip('/').removesuffix('.git')
    return f'{host}/{path.lstrip("/")}' if host else path
