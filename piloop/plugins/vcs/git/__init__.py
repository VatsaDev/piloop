"""``git``: the default vcs provider. A version is a commit of the campaign's git repository, and its revision the SHA.

Agents work in detached worktrees and save their work as commits, with trailers for what they ask of piloop
(``Piloop-Eval: skip``, ``Piloop-Note``). Commits travel between a worker's replica and the repo of record as git bundles
in the CAS (``bundles``). Refs under ``refs/piloop/`` keep the commits that piloop needs from garbage collection, so
``git show <sha>`` keeps working.
"""

import re
import subprocess
from collections.abc import Sequence
from contextlib import suppress
from pathlib import Path
from typing import assert_never

from covalence import Cid
from git import Repo
from git.exc import BadName, GitCommandError, GitError, InvalidGitRepositoryError, NoSuchPathError
from pydantic import SecretStr

from piloop.actor import Agent
from piloop.ids import Revision, RunId
from piloop.plugins.cas.contract import Cas
from piloop.plugins.host import Check
from piloop.plugins.vcs.contract import (
    MAX_HAVE,
    Captured,
    Conflicts,
    Fetch,
    Keep,
    NotARepository,
    Part,
    RebaseVersion,
    ReferenceVersion,
    Repository,
    RunVersion,
    Transfer,
    UnknownRevision,
    Unsaved,
    Vcs,
    VcsError,
    Version,
)
from piloop.plugins.vcs.git import bundles, commands as git

NEEDED = (2, 40)
"""The oldest git that the provider works with: it merges by tree with ``git merge-tree --merge-base``, new in 2.40."""
SKIP = 'Piloop-Eval'
"""The trailer of a commit not to measure: ``Piloop-Eval: skip``."""
NOTE = 'Piloop-Note'
"""The trailer of a note on a commit, applied when it is judged."""
RECOVERED = 'Piloop-Recovered'
"""The trailer of the commit that ``capture`` makes of what an agent left uncommitted: ``Piloop-Recovered: run#<id>``."""
UNCOMMITTED = 'piloop: uncommitted implementer changes'
"""The subject of the commit that ``capture`` makes of what an agent left uncommitted."""
PROMPTS = Path(__file__).parent / 'prompts'
GITIGNORE = '.gitignore'
"""What the built-in prompts say about each ``Part``, one file each."""
PILOOP = git.Identity(name='piloop', email='piloop@localhost')
"""Who piloop's own commits are by, with the email of ``[plugins.vcs] email``."""
FETCHED = 'refs/piloop/fetched/'
"""Where a replica pins each commit that came as a bundle: it stays, and the next fetch names it as a commit it has."""


def ref(why: Keep) -> str:
    """The ref that pins a commit kept for ``why``."""
    match why:
        case RunVersion(campaign=campaign, run=run, index=index):
            return f'{runs(campaign, run)}{index}'
        case RebaseVersion(campaign=campaign, candidate=candidate):
            return f'refs/piloop/{campaign}/rebase/{candidate}'
        case ReferenceVersion(campaign=campaign, candidate=candidate):
            return f'refs/piloop/{campaign}/reference/{candidate}'
        case _:
            assert_never(why)


def runs(campaign: str, run: RunId) -> str:
    """The prefix of the refs that pin a run's commits, ``<prefix><n>``."""
    return f'refs/piloop/{campaign}/r{run}/'


class GitRepository(Repository):
    """A git checkout: the repo of record, a worker's replica, or a worktree of either."""

    def __init__(self, repo: Repo, committer: git.Identity) -> None:
        self.repo = repo
        self.committer = committer

    def close(self) -> None:
        """End the ``git cat-file`` processes that GitPython keeps for the repo: else only the GC ends them."""
        self.repo.close()

    def head(self) -> Version:
        return self._version(self.repo.head.commit.hexsha)

    def version(self, native: str) -> Version:
        try:
            sha = self.repo.commit(native).hexsha
        except (BadName, ValueError) as e:
            raise UnknownRevision(f'{native!r} names no commit in {self.repo.working_dir}: {e}') from e
        return self._version(sha)

    def _version(self, sha: str) -> Version:
        text = git.message(self.repo, sha)
        return Version(
            revision=Revision(sha),
            tree=git.tree_sha(self.repo, sha),
            summary=git.subject_line(self.repo, sha),
            skip=any(value.lower() == 'skip' for value in git.trailers(text, SKIP)),
            notes=tuple(git.trailers(text, NOTE)),
            recovered=bool(git.trailers(text, RECOVERED)),
        )

    def receive(self, cas: Cas, transfer: Transfer, base: Revision) -> list[Version]:
        """Unbundle, and return the first-parent history from ``base``.

        A line that the agent merged in is not its own: a merge's parent is the commit that it merged onto, and the
        merged line is not scored again.
        """
        bundles.unbundle(cas, self.repo, transfer.blob, [transfer.head])
        line = self.repo.iter_commits(f'{base}..{transfer.head}', first_parent=True)
        return [self._version(c.hexsha) for c in line][::-1]

    def send(self, cas: Cas, revisions: Sequence[Revision], have: Sequence[Revision]) -> Cid | None:
        return bundles.bundle(cas, self.repo, revisions, have)

    def fetch(self, cas: Cas, revisions: Sequence[Revision], ask: Fetch) -> None:
        """The replica names its newest commits, as git's have/want does, and pins those that came (``FETCHED``)."""
        wanted = list(dict.fromkeys(revisions))
        held = set(git.commits(self.repo, wanted))
        if not (missing := [sha for sha in wanted if sha not in held]):
            return
        newest = self.repo.git.for_each_ref('--sort=-committerdate', f'--count={MAX_HAVE}', '--format=%(objectname)')
        for blob in ask(missing, [Revision(sha) for sha in newest.split()]):
            bundles.unbundle(cas, self.repo, blob, missing)
        came = git.commits(self.repo, missing)  # the repo of record leaves out a commit that it lacks
        git.pin_all(self.repo, {f'{FETCHED}{sha}': sha for sha in came})

    def merge(self, onto: Revision, other: Revision, message: str) -> Version | Conflicts:
        merged = git.merge(self.repo, onto, other, message, self.committer)
        return Conflicts(tuple(merged)) if isinstance(merged, list) else self._version(merged)

    def contains(self, revision: Revision, ancestor: Revision) -> bool:
        try:
            return git.contains(self.repo, revision, ancestor)
        except GitError as e:
            raise VcsError(str(e)) from e

    def keep(self, revision: Revision, why: Keep) -> None:
        git.pin(self.repo, ref(why), revision)

    def kept(self, campaign: str, run: RunId) -> list[Version]:
        return [self._version(sha) for sha in git.pinned(self.repo, runs(campaign, run))]

    def checkout(self, revision: Revision, path: Path) -> None:
        """A detached worktree."""
        git.add_worktree(self.repo, path, revision, {})

    def discard(self, path: Path) -> None:
        git.remove_worktree(self.repo, path)

    def checkouts(self, root: Path) -> list[Path]:
        return git.worktrees_under(self.repo, root)

    def untracked(self, revision: Revision, names: Sequence[str]) -> list[str]:
        return git.untracked(self.repo, revision, names)

    def capture(self, cas: Cas, base: Revision, run: RunId, ignore: Sequence[str]) -> Captured:
        """Commit what the agent left uncommitted on its own, never amended into its last commit, then bundle them.

        The commit is by the run, and committed by piloop. When git refuses it, a hook say, the unsaved changes go into
        ``cas`` as a patch onto the agent's last commit.
        """
        unsaved = None
        if git.is_dirty(self.repo, list(ignore)):
            by = f'run#{run}'
            author = self.committer.model_copy(update={'name': by})
            message = f'{UNCOMMITTED}\n\n{RECOVERED}: {by}'
            try:
                git.commit_all(self.repo, message, list(ignore), author, self.committer)
            except GitCommandError as e:
                unsaved = Unsaved(blob=cas.put(git.staged(self.repo)), error=str(e))
        head = self.repo.head.commit.hexsha
        blob = bundles.bundle(cas, self.repo, [head], [base]) if head != base else None
        return Captured(Transfer(blob=blob, head=Revision(head)) if blob is not None else None, unsaved)


class Git(Vcs):
    """Keep versions as commits of a git repository, git 2.40 or later."""

    email: str = PILOOP.email
    """The email of every commit that piloop makes or launches, whatever git's config says: a merge, ``piloop init``'s,
    the save of what an agent left uncommitted, which its run authors, and an agent's own. Their name is ``piloop``, or
    the agent's signature for an agent's own. A worker can have no git config."""

    @property
    def piloop(self) -> git.Identity:
        """Piloop's own identity, with ``email``."""
        return PILOOP.model_copy(update={'email': self.email})

    def init(self, path: Path, summary: str, owned: Sequence[str], ignore: Sequence[str]) -> list[str]:
        """An existing ``.gitignore`` is the campaign's: it is kept as it is."""
        written = [] if (path / GITIGNORE).exists() else [GITIGNORE]
        if written:
            (path / GITIGNORE).write_text(''.join(f'{p}\n' for p in ignore))
        with Repo.init(path, initial_branch='main') if not (path / '.git').exists() else Repo(path) as repo:
            if not repo.head.is_valid():
                repo.git.add('-A')
                repo.git.commit('-qm', summary, env=git.commit_env(self.piloop, self.piloop))
                return written
            owned = [*owned, GITIGNORE]
            # git status leaves out what the repo's ignore file ignores, which git add would refuse to take by name
            # and --no-renames reports a rename as its two paths, not as one "old -> new" record
            status = repo.git.status('--porcelain', '-z', '--no-renames', '--untracked-files=all', '--', *owned)
            entries = [entry for entry in status.split('\0') if entry]
            changed = sorted(entry[3:] for entry in entries)
            # only what the work tree changed: git add refuses a path whose deletion is staged already
            if unstaged := sorted(entry[3:] for entry in entries if entry[1] != ' '):
                repo.git.add('-A', '--', *unstaged)
            if changed:
                by = git.commit_env(self.piloop, self.piloop)
                repo.git.commit('-qm', f'piloop init: {", ".join(changed)}', '--', *changed, env=by)
            return written

    def open(self, path: Path) -> GitRepository:
        try:
            return GitRepository(git.open_repo(path), self.piloop)
        except (InvalidGitRepositoryError, NoSuchPathError) as e:
            raise NotARepository(f'{path} is not a git checkout: {type(e).__name__}: {e}') from e

    def clone(self, url: str, path: Path, credential: SecretStr | None) -> GitRepository:
        """``git clone``. Over HTTPS, the credential is the password of the user ``x-access-token``."""
        git.clone(url, path, credential)
        return self.open(path)

    def replica(self, path: Path) -> GitRepository:
        if not (path / '.git').is_dir():
            Repo.init(path).close()
        return self.open(path)

    def identity(self, path: Path) -> str:
        return git.identity(path)

    def agent_env(self, agent: Agent) -> dict[str, str]:
        """``GIT_AUTHOR_*`` and ``GIT_COMMITTER_*``: its commits are by its signature, with ``email``."""
        by = git.Identity(name=agent.signature, email=self.email)
        return git.commit_env(by, by)

    @classmethod
    def root(cls, path: Path) -> Path | None:
        with suppress(InvalidGitRepositoryError, NoSuchPathError), Repo(path, search_parent_directories=True) as repo:
            return Path(repo.common_dir).resolve().parent
        return None

    def changed(self, path: Path, ignore: Sequence[str]) -> bool:
        """A commit or a reset moved HEAD (the reflog has more than one entry), or the worktree is dirty."""
        return git.changed(path, list(ignore))

    def prompt(self, part: Part) -> str:
        """The text of ``prompts/<part>.md``."""
        return (PROMPTS / f'{part}.md').read_text().removesuffix('\n')

    @classmethod
    def checks(cls) -> list[Check]:
        """The git on ``PATH``, which every campaign and the daemon run, is at least ``NEEDED``."""
        fix = 'install git 2.40 or later'
        try:
            found = subprocess.run(
                ['git', '--version'], capture_output=True, stdin=subprocess.DEVNULL, text=True, timeout=10, check=True
            ).stdout.strip()
        except (OSError, subprocess.SubprocessError) as e:
            return [Check('git', f'`git --version` failed: {e}', fix)]
        if tuple(int(n) for n in re.findall(r'\d+', found)[:2]) < NEEDED:
            return [Check('git', f'{found}; piloop needs 2.40 or later', fix)]
        return [Check('git', found)]
