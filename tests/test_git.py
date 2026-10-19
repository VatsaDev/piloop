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


@pytest.mark.parametrize(
    'message',
    [
        'work\n\nWhy.\n\nPiloop-Eval: skip\nPiloop-Note: a lead\n\nCo-Authored-By: Claude <noreply@anthropic.com>\n',
        'work\n\nPiloop-Eval: skip\nPiloop-Note: a lead\n\n🤖 Generated with [Claude Code](https://claude.com/claude-code)'
        '\n\nCo-Authored-By: Claude <noreply@anthropic.com>\n',
        'work\n\nWhy, with no blank line before the trailers.\npiloop-eval : SKIP\nPiloop-Note:  a\n  lead\n',
    ],
    ids=['before-co-authored-by', 'before-generated-with', 'in-the-body'],
)
def test_piloop_trailers_count_on_any_line_after_the_subject(project_path: Path, message: str) -> None:
    """hiloopai/piloop-dev#33: a harness puts its own trailers in a paragraph after the worker's, and git reads only
    the last paragraph. A line that only mentions a trailer is not one."""
    repo = Repo(project_path)
    repo.git.commit('-q', '--allow-empty', '-m', message)
    sha = repo.head.commit.hexsha
    assert git.eval_skipped(repo, sha)
    assert git.trailers(repo, sha, 'Piloop-Note') == ['a lead']


def test_a_line_that_mentions_a_trailer_or_the_subject_is_not_one(project_path: Path) -> None:
    repo = Repo(project_path)
    repo.git.commit(
        '-q', '--allow-empty', '-m', 'Piloop-Eval: skip\n\nThe trailer Piloop-Eval: skip is for scaffolding.'
    )
    sha = repo.head.commit.hexsha
    assert not git.eval_skipped(repo, sha)
    assert git.trailers(repo, sha, 'Piloop-Eval') == []
