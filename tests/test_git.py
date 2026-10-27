import base64
import contextlib
import logging
import os
import socket
import subprocess
import threading
from collections.abc import Iterator
from datetime import timedelta
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import ClassVar

import psutil
import pytest
from git import Repo
from git.exc import GitCommandError
from pydantic import SecretStr

from piloop.actor import Agent
from piloop.api.client import CLONE_WAIT
from piloop.ids import IMPLEMENTER, AgentSessionId, Revision, RunId
from piloop.plugins.vcs.contract import CloneFailed, Version
from piloop.plugins.vcs.git import PILOOP, UNCOMMITTED, Git, GitRepository, commands as git
from tests.conftest import disk, git_processes


def test_merge_and_contains_raise_on_a_commit_the_repo_does_not_have(campaign_path: Path) -> None:
    """A conflict and "not an ancestor" are answers; an unknown commit is an error, never read as either."""
    repo = Repo(campaign_path)
    head = repo.head.commit.hexsha
    assert git.contains(repo, head, head)
    with pytest.raises(GitCommandError, match='merge-tree'):
        git.merge(repo, head, '0' * 40, 'm', PILOOP)
    with pytest.raises(GitCommandError, match='merge-base'):
        git.contains(repo, head, '0' * 40)


def test_a_commit_contains_another_by_history_or_by_having_all_its_changes(campaign_path: Path) -> None:
    """A commit that cherry-picked the head has the head's changes, though not its history."""
    repo = Repo(campaign_path)
    base = repo.head.commit.hexsha
    (campaign_path / 'other.txt').write_text('x\n')
    repo.git.add('-A')
    repo.git.commit('-qm', 'the head')
    head = repo.head.commit.hexsha
    repo.git.checkout('-q', '--detach', base)
    (campaign_path / 'solve.txt').write_text('base\nb\n')
    repo.git.commit('-qam', 'a lead')
    lead = repo.head.commit.hexsha
    repo.git.cherry_pick(head)
    picked = repo.head.commit.hexsha
    repo.git.merge('-q', '--no-edit', head)
    merged = repo.head.commit.hexsha
    assert (git.contains(repo, merged, head), git.contains(repo, picked, head)) == (True, True)
    assert (git.contains(repo, lead, head), git.contains(repo, head, picked)) == (False, False), 'a change is missing'


def test_a_merge_starts_from_a_copy_of_the_head_in_the_commits_history(tmp_path: Path) -> None:
    """A run that started from a copy of the head (same tree, another sha) merges cleanly.

    By history, the merge base is the commit before the head, so both sides add ``f.txt`` and conflict. By tree, the
    copy is the head: only the run's change and the head's later change are merged.
    """
    repo = Repo.init(tmp_path / 'r', initial_branch='main')
    root = Path(repo.working_dir)

    def commit(name: str, text: str, message: str = 'change') -> str:
        (root / name).write_text(text)
        repo.git.add('-A')
        repo.git.commit('-qm', message)
        return repo.head.commit.hexsha

    base = commit('a.txt', 'a\n')
    head = commit('f.txt', 'x\n')
    later = commit('g.txt', 'g\n')
    repo.git.checkout('-q', '--detach', base)
    copy = commit('f.txt', 'x\n', 'a rebased copy of the head')
    assert copy != head and git.tree_sha(repo, copy) == git.tree_sha(repo, head)
    run = commit('f.txt', 'y\n')
    for onto in (head, later):
        merged = git.merge(repo, onto, run, 'rebased', PILOOP)
        assert isinstance(merged, str), f'onto {onto}: conflicts in {merged}'
        files = {name: repo.git.show(f'{merged}:{name}') for name in repo.git.ls_tree('--name-only', merged).split()}
        assert files == {'a.txt': 'a', 'f.txt': 'y'} | ({'g.txt': 'g'} if onto == later else {})
        assert [p.hexsha for p in repo.commit(merged).parents] == [onto, run]


def test_worktree_changes_hold_the_process_lock(
    campaign_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every worktree command of the daemon's threads holds one lock.

    A ``git worktree prune`` that races a ``git worktree add`` in the same repo deletes the new worktree during the add.
    """
    repo = Repo(campaign_path)
    real = repo.git
    held: list[tuple[str, bool]] = []

    class Spy:
        def __getattr__(self, name: str) -> object:
            return getattr(real, name)

        def worktree(self, *args: str) -> str:
            held.append((args[0], git.WORKTREE_LOCK.locked()))
            return real.worktree(*args)

    monkeypatch.setattr(repo, 'git', Spy())
    wt = tmp_path / 'agent_sessions' / '1' / 'worktree'
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
def test_piloop_trailers_count_on_any_line_after_the_subject(campaign_path: Path, message: str) -> None:
    """A harness puts its own trailers in a paragraph after the implementer's, and git reads only
    the last paragraph. A line that only mentions a trailer is not one."""
    repo = Repo(campaign_path)
    repo.git.commit('-q', '--allow-empty', '-m', message)
    version = GitRepository(repo, PILOOP).head()
    assert version.skip
    assert version.notes == ('a lead',)


def test_a_line_that_mentions_a_trailer_or_the_subject_is_not_one(campaign_path: Path) -> None:
    repo = Repo(campaign_path)
    repo.git.commit(
        '-q', '--allow-empty', '-m', 'Piloop-Eval: skip\n\nThe trailer Piloop-Eval: skip is for scaffolding.'
    )
    version = GitRepository(repo, PILOOP).head()
    assert not version.skip
    assert git.trailers(git.message(repo, version.revision), 'Piloop-Eval') == []


def test_a_checkout_changed_by_an_edit_a_new_file_or_a_commit_not_by_what_was_linked_into_it(
    campaign_path: Path, tmp_path: Path
) -> None:
    """``changed`` tells an agent that worked from one that left its checkout as it got it."""
    repo = Repo(campaign_path)
    wt_path = tmp_path / 'wt'
    target = tmp_path / 'shared'
    target.mkdir()
    links = {'shared': target}
    git.add_worktree(repo, wt_path, repo.head.commit.hexsha, links)
    wt = Repo(wt_path)
    ignore = git.untracked(repo, repo.head.commit.hexsha, [*links, 'piloop.toml'])
    assert ignore == ['shared'], 'a name that the commit tracks is not left out'
    assert not git.changed(wt_path, ignore), 'a link that .gitignore does not name is no change of the agent'
    assert git.changed(wt_path, []), 'without the links, it is an untracked file'
    (wt_path / 'new.txt').write_text('x')
    assert git.changed(wt_path, ignore)
    git.commit_all(wt, 'add a file', ignore, PILOOP, PILOOP)
    assert git.changed(wt_path, ignore) and not git.is_dirty(wt, ignore), 'HEAD moved'
    assert not git.changed(tmp_path / 'gone', []), 'no checkout'


def test_a_closed_repository_leaves_no_git_process_running(campaign_path: Path) -> None:
    repo = Git().open(campaign_path)
    repo.head()
    assert git_processes(campaign_path), 'reading a version starts the git cat-file that GitPython keeps for the repo'
    repo.close()
    assert git_processes(campaign_path) == []


def by(repo: Repo, sha: str) -> tuple[str, str]:
    """Who commit ``sha`` is by: its author and its committer, as ``name <email>``."""
    author, committer = repo.git.show('-s', '--format=%an <%ae>%n%cn <%ce>', sha).splitlines()
    return author, committer


def test_piloops_own_commits_carry_its_identity_on_a_machine_that_has_none(
    campaign_path: Path, tmp_path: Path, no_git_identity: None
) -> None:
    """A fresh worker has no ``user.*``: the save of an agent's unsaved work, a merge and ``piloop init`` commit anyway.

    The save is by the run that left the work, committed by piloop.
    """
    vcs = Git()
    record = vcs.open(campaign_path)
    base = record.head().revision
    record.checkout(base, tmp_path / 'wt')
    (tmp_path / 'wt' / 'solve.txt').write_text('base\nmore\n')
    captured = vcs.open(tmp_path / 'wt').capture(disk(tmp_path), base, RunId(5), ['data'])
    assert captured.work is not None and captured.unsaved is None
    saved = vcs.open(tmp_path / 'wt').head()
    assert (saved.summary, saved.recovered) == (UNCOMMITTED, True)
    repo = Repo(campaign_path)
    assert by(repo, saved.revision) == ('run#5 <piloop@localhost>', 'piloop <piloop@localhost>')
    merged = record.merge(base, saved.revision, 'a merge')
    assert isinstance(merged, Version) and by(repo, merged.revision) == ('piloop <piloop@localhost>',) * 2
    (tmp_path / 'new').mkdir()
    vcs.init(tmp_path / 'new', 'baseline', [], [])
    assert by(Repo(tmp_path / 'new'), 'HEAD') == ('piloop <piloop@localhost>',) * 2
    (tmp_path / 'other').mkdir()
    Git(email='ops@example.com').init(tmp_path / 'other', 'baseline', [], [])
    assert by(Repo(tmp_path / 'other'), 'HEAD') == ('piloop <ops@example.com>',) * 2, '[plugins.vcs] email'


def test_an_agents_own_commit_is_by_its_signature_on_a_machine_that_has_none(
    campaign_path: Path, tmp_path: Path, no_git_identity: None
) -> None:
    """The variables of an agent's job commit as the agent, with the campaign's email."""
    vcs = Git(email='piloop@example.com')
    vcs.open(campaign_path).checkout(vcs.open(campaign_path).head().revision, tmp_path / 'wt')
    (tmp_path / 'wt' / 'solve.txt').write_text('base\nmore\n')
    env = vcs.agent_env(Agent(role=IMPLEMENTER, session=AgentSessionId(12)))
    subprocess.run(['git', 'commit', '-qam', 'more'], cwd=tmp_path / 'wt', env={**os.environ, **env}, check=True)
    assert (
        by(Repo(campaign_path), vcs.open(tmp_path / 'wt').head().revision)
        == ('implementer#12 <piloop@example.com>',) * 2
    )


def test_unsaved_work_is_saved_when_nothing_is_ignored(campaign_path: Path, tmp_path: Path) -> None:
    """A checkout with no links, of a version that tracks its workspace, has nothing to leave out of the save."""
    vcs = Git()
    base = vcs.open(campaign_path).head().revision
    vcs.open(campaign_path).checkout(base, tmp_path / 'wt')
    (tmp_path / 'wt' / 'solve.txt').write_text('base\nmore\n')
    assert vcs.open(tmp_path / 'wt').capture(disk(tmp_path), base, RunId(5), []).unsaved is None


def test_a_save_that_git_refuses_keeps_the_unsaved_work_in_the_cas_and_brings_the_agents_own_commits_back(
    campaign_path: Path, tmp_path: Path
) -> None:
    """A hook that refuses the commit: the unsaved changes are a patch in the CAS, which restores them on the head."""
    hook = campaign_path / '.git' / 'hooks' / 'pre-commit'
    hook.write_text('#!/bin/sh\necho refused >&2\nexit 1\n')
    hook.chmod(0o755)
    vcs, cas = Git(), disk(tmp_path)
    base = vcs.open(campaign_path).head().revision
    vcs.open(campaign_path).checkout(base, tmp_path / 'wt')
    wt = Repo(tmp_path / 'wt')
    (tmp_path / 'wt' / 'solve.txt').write_text('base\nthe agent\n')
    wt.git.commit('-qam', 'the agents own', '--no-verify')
    head = wt.head.commit.hexsha
    (tmp_path / 'wt' / 'solve.txt').write_text('base\nthe agent\nunsaved\n')
    (tmp_path / 'wt' / 'new.bin').write_bytes(b'\x00\x01')
    captured = vcs.open(tmp_path / 'wt').capture(cas, base, RunId(5), ['data'])
    assert captured.work is not None and captured.work.head == head, 'its own commit still comes back'
    assert captured.unsaved is not None and 'refused' in captured.unsaved.error
    patch = cas.get(captured.unsaved.blob)
    assert patch is not None
    vcs.open(campaign_path).checkout(Revision(head), tmp_path / 'restored')
    subprocess.run(['git', 'apply', '-'], input=patch, cwd=tmp_path / 'restored', check=True)
    assert (tmp_path / 'restored' / 'solve.txt').read_text() == 'base\nthe agent\nunsaved\n'
    assert (tmp_path / 'restored' / 'new.bin').read_bytes() == b'\x00\x01'


SECRET = SecretStr('s3cret-token')


@pytest.fixture
def bare(campaign_path: Path, tmp_path: Path) -> Path:
    """A bare clone of the campaign repo, which a dumb HTTP server can serve as files."""
    path = tmp_path / 'origin.git'
    subprocess.run(['git', 'clone', '-q', '--bare', str(campaign_path), str(path)], check=True)
    subprocess.run(['git', 'update-server-info'], cwd=path, check=True)
    return path


@pytest.mark.parametrize('credential', [None, SECRET])
def test_a_clone_of_a_bare_repo_by_file_url_has_its_head(
    bare: Path, tmp_path: Path, credential: SecretStr | None
) -> None:
    clone = Git().clone(bare.as_uri(), tmp_path / 'clone', credential)
    assert clone.head().revision == Repo(bare).head.commit.hexsha


def test_a_failed_clone_says_why_without_the_credential(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """git's error names the URL, and a URL may hold the credential's text: it is redacted wherever it appears."""
    url = (tmp_path / 'gone' / SECRET.get_secret_value()).as_uri()
    caplog.set_level(logging.DEBUG)
    with pytest.raises(CloneFailed, match='not appear to be a git repository') as failed:
        Git().clone(url, tmp_path / 'clone', SECRET)
    assert SECRET.get_secret_value() not in str(failed.value)
    assert SECRET.get_secret_value() not in caplog.text


class Basic(SimpleHTTPRequestHandler):
    """Serves the files of a bare repo, as git's dumb HTTP protocol reads them, to whoever sends ``password``.

    It notes the argv of each process of the test while git waits for it: what ``ps`` shows every user.
    """

    password = SECRET.get_secret_value()
    sent: ClassVar[list[str]] = []
    argv: ClassVar[list[str]] = []

    def do_GET(self) -> None:
        sent = self.headers.get('Authorization', '')
        self.sent.append(sent)
        for child in psutil.Process().children(recursive=True):
            with contextlib.suppress(psutil.NoSuchProcess):
                self.argv.append(' '.join(child.cmdline()))
        if sent != 'Basic ' + base64.b64encode(f'x-access-token:{self.password}'.encode()).decode():
            self.send_response(401)
            self.send_header('WWW-Authenticate', 'Basic realm="repo"')
            self.send_header('Content-Length', '0')
            self.end_headers()
            return
        super().do_GET()

    def log_message(self, format: str, *args: object) -> None:
        pass


@pytest.fixture
def served(bare: Path) -> Iterator[str]:
    """The URL of ``bare`` over HTTP, behind basic authentication."""
    Basic.sent.clear()
    Basic.argv.clear()
    server = ThreadingHTTPServer(('127.0.0.1', 0), partial(Basic, directory=str(bare.parent)))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f'http://127.0.0.1:{server.server_address[1]}/{bare.name}'
    server.shutdown()
    server.server_close()
    thread.join()


def test_a_clone_over_http_answers_the_servers_challenge_with_the_credential(served: str, tmp_path: Path) -> None:
    clone = Git().clone(served, tmp_path / 'clone', SECRET)
    assert clone.head().summary == 'baseline'
    assert [sent for sent in Basic.sent if sent], 'the credential went in a header'
    assert [argv for argv in Basic.argv if 'clone' in argv], 'git was running'
    assert not [argv for argv in Basic.argv if SECRET.get_secret_value() in argv]


@pytest.mark.parametrize('credential', [None, SecretStr('wrong-token')])
def test_a_clone_that_the_server_refuses_fails_without_a_prompt(
    served: str, tmp_path: Path, credential: SecretStr | None
) -> None:
    """No terminal prompt waits for a user that a daemon does not have."""
    with pytest.raises(CloneFailed, match='clone of http') as failed:
        Git().clone(served, tmp_path / 'clone', credential)
    assert 'wrong-token' not in str(failed.value)
    assert not (tmp_path / 'clone').exists()


def test_a_clone_from_a_remote_that_stalls_is_stopped_at_its_bound_and_leaves_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A remote that accepts the connection and never answers holds no thread past ``CLONE_TIMEOUT``."""
    monkeypatch.setattr(git, 'CLONE_TIMEOUT', timedelta(seconds=1))
    with socket.create_server(('127.0.0.1', 0)) as silent:
        url = f'http://127.0.0.1:{silent.getsockname()[1]}/repo.git'
        with pytest.raises(CloneFailed, match=rf'^git clone of {url} took longer than 0:00:01: it was stopped$'):
            Git().clone(url, tmp_path / 'clone', SecretStr('the-token'))
    assert not (tmp_path / 'clone').exists(), 'the partial directory is deleted'
    assert not [p for p in psutil.process_iter(['cmdline']) if url in ' '.join(p.info['cmdline'] or [])], (
        'git and the helpers that it started are gone'
    )


def test_a_registration_that_clones_waits_longer_than_a_clone_may_take() -> None:
    assert CLONE_WAIT > git.CLONE_TIMEOUT
