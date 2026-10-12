from pathlib import Path

import pytest
from git import Repo
from git.exc import GitCommandError

from piloop import git


def test_merge_and_contains_raise_on_a_commit_the_repo_does_not_have(project_path: Path) -> None:
    """A conflict and "not an ancestor" are answers; an unknown commit is an error, never read as either."""
    repo = Repo(project_path)
    head = repo.head.commit.hexsha
    assert git.contains(repo, head, head)
    with pytest.raises(GitCommandError, match='merge-tree'):
        git.merge(repo, head, '0' * 40, 'm')
    with pytest.raises(GitCommandError, match='merge-base'):
        git.contains(repo, head, '0' * 40)


def test_worktree_changes_hold_the_process_lock(
    project_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every worktree command of the daemon's threads holds one lock.

    A ``git worktree prune`` that races a ``git worktree add`` in the same repo deletes the new worktree during the add.
    """
    repo = Repo(project_path)
    real = repo.git
    held: list[tuple[str, bool]] = []

    class Spy:
        def __getattr__(self, name: str) -> object:
            return getattr(real, name)

        def worktree(self, *args: str) -> str:
            held.append((args[0], git.WORKTREE_LOCK.locked()))
            return real.worktree(*args)

    monkeypatch.setattr(repo, 'git', Spy())
    wt = tmp_path / 'workers' / '1' / 'worktree'
    git.add_worktree(repo, wt, repo.head.commit.hexsha, {})
    git.remove_worktree(repo, wt)
    assert held == [('prune', True), ('add', True), ('list', True), ('remove', True), ('prune', True)]
    assert not git.WORKTREE_LOCK.locked()
