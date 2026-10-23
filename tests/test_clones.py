"""Commits in and out of a worker's repos as bundles in the CAS, and the worktree of a job named by address."""

import subprocess
from collections.abc import Sequence
from pathlib import Path

import pytest
from covalence import Cid
from git import GitCommandError, Repo

from piloop.ids import CampaignId
from piloop.plugins.cas.contract import Cas
from piloop.plugins.executor.contract import Checkout, GitSource, RepoId
from piloop.plugins.executor.kit.clones import FETCHED, WORKSPACE_LINK, Clones, Fetch
from piloop.plugins.executor.kit.jobs import remote_site
from piloop.store.commits import bundle, unbundle
from piloop.store.tree import Missing, put_tree
from tests.conftest import disk

REPO = RepoId(CampaignId(1))


def commit(repo: Path, name: str, text: str) -> str:
    (repo / name).write_text(text)
    subprocess.run(['git', 'add', name], cwd=repo, check=True)
    subprocess.run(['git', 'commit', '-qm', name], cwd=repo, check=True)
    return Repo(repo).head.commit.hexsha


@pytest.fixture
def origin(tmp_path: Path) -> Path:
    path = tmp_path / 'origin'
    path.mkdir()
    subprocess.run(['git', 'init', '-q', '-b', 'main'], cwd=path, check=True)
    commit(path, 'a.txt', 'a')
    return path


@pytest.fixture
def store(tmp_path: Path) -> Cas:
    return disk(tmp_path)


def clone(origin: Path, path: Path) -> Path:
    subprocess.run(['git', 'clone', '-q', str(origin), str(path)], check=True)
    return path


def test_a_bundle_carries_the_commits_beyond_what_the_other_side_has(tmp_path: Path, origin: Path, store: Cas) -> None:
    other = clone(origin, tmp_path / 'other')
    base = Repo(origin).head.commit.hexsha
    commit(origin, 'b.txt', 'b')
    head = commit(origin, 'c.txt', 'c')
    blob = bundle(store, Repo(origin), head, [base, 'f' * 40])  # a commit it does not know is left out
    assert blob is not None
    unbundle(store, Repo(other), blob, head)
    assert Repo(other).commit(head).summary == 'c.txt'
    unbundle(store, Repo(other), blob, head)  # it has the head: nothing to do


def test_no_bundle_when_the_other_side_has_every_commit(origin: Path, store: Cas) -> None:
    head = Repo(origin).head.commit.hexsha
    assert bundle(store, Repo(origin), head, [head]) is None


def test_a_bundle_that_needs_a_commit_the_repo_lacks_is_refused(tmp_path: Path, origin: Path, store: Cas) -> None:
    other = clone(origin, tmp_path / 'other')
    middle = commit(origin, 'b.txt', 'b')
    head = commit(origin, 'c.txt', 'c')
    blob = bundle(store, Repo(origin), head, [middle])
    assert blob is not None
    with pytest.raises(GitCommandError):
        unbundle(store, Repo(other), blob, head)


def fetch_from(store: Cas, origin: Path, asked: list[Sequence[str]]) -> Fetch:
    def fetch(repo: RepoId, commit: str, have: Sequence[str]) -> Cid | None:
        assert repo == REPO
        asked.append(have)
        return bundle(store, Repo(origin), commit, have)

    return fetch


def test_a_clone_that_lacks_the_commit_is_filled_from_a_bundle(tmp_path: Path, origin: Path, store: Cas) -> None:
    """A remote worker's clone: the commit comes as a bundle of what it lacks, and is pinned for the next fetch."""
    mine = clone(origin, tmp_path / 'clone')
    known = Repo(mine).head.commit.hexsha
    head = commit(origin, 'b.txt', 'b')
    asked: list[Sequence[str]] = []
    clones = Clones(store, {REPO: mine}.__getitem__, fetch_from(store, origin, asked), tmp_path)
    source = GitSource(repo=REPO, commit=head)
    clones.checkout(source, tmp_path / 'wt')
    assert (tmp_path / 'wt' / 'b.txt').read_text() == 'b' and known in asked[0]
    assert Repo(mine).git.rev_parse(f'{FETCHED}{head}') == head
    clones.remove(source, tmp_path / 'wt')
    clones.checkout(source, tmp_path / 'wt')
    assert len(asked) == 1, 'it has the commit now'


def test_links_are_put_read_only_and_the_workspace_is_linked_in(tmp_path: Path, origin: Path, store: Cas) -> None:
    data = tmp_path / 'inputs'
    (data / 'data').mkdir(parents=True)
    (data / 'data' / 'train.csv').write_text('x,y\n')
    links = {'data': put_tree(store, data, ['data']).manifest.put(store)}
    clones = Clones(store, {REPO: origin}.__getitem__, fetch_from(store, origin, []), tmp_path)
    source = GitSource(repo=REPO, commit=Repo(origin).head.commit.hexsha, links=links, workspace='projects/1/workspace')
    cwd = tmp_path / 'runs' / '1' / 'worktree'
    clones.checkout(source, cwd)
    assert (cwd / 'data' / 'train.csv').read_text() == 'x,y\n'
    assert (cwd / 'data' / 'train.csv').stat().st_mode & 0o222 == 0, 'an input never changes under another job'
    assert (cwd / WORKSPACE_LINK).resolve() == tmp_path / 'projects' / '1' / 'workspace'
    assert clones.ignored(source) == ['data', WORKSPACE_LINK]
    (cwd / 'a.txt').write_text('changed')
    assert clones.bring_back(source, cwd, 'leftovers') is not None, 'the change, without the links'
    assert Repo(cwd).git.show('--name-only', '--format=', 'HEAD').split() == ['a.txt']
    clones.remove(source, cwd)
    assert not cwd.exists()


def test_bring_back_is_none_without_a_commit(tmp_path: Path, origin: Path, store: Cas) -> None:
    clones = Clones(store, {REPO: origin}.__getitem__, fetch_from(store, origin, []), tmp_path)
    source = GitSource(repo=REPO, commit=Repo(origin).head.commit.hexsha)
    clones.checkout(source, tmp_path / 'wt')
    assert clones.bring_back(source, tmp_path / 'wt', 'leftovers') is None
    clones.remove(Checkout(repo=source.repo), origin)  # the checkout itself is never removed
    assert origin.is_dir()


def test_a_commit_that_the_server_cannot_bundle_fails_the_checkout(tmp_path: Path, origin: Path, store: Cas) -> None:
    def nothing(repo: RepoId, commit: str, have: Sequence[str]) -> Cid | None:
        return None

    clones = Clones(store, {REPO: origin}.__getitem__, nothing, tmp_path)
    with pytest.raises(GitCommandError):
        clones.checkout(GitSource(repo=REPO, commit='f' * 40), tmp_path / 'wt')


def test_a_tree_that_tracks_the_workspace_name_keeps_its_own(tmp_path: Path, origin: Path, store: Cas) -> None:
    head = commit(origin, WORKSPACE_LINK, 'a file of the tree')
    clones = Clones(store, {REPO: origin}.__getitem__, fetch_from(store, origin, []), tmp_path)
    source = GitSource(repo=REPO, commit=head, workspace='projects/1/workspace')
    clones.checkout(source, tmp_path / 'wt')
    assert (tmp_path / 'wt' / WORKSPACE_LINK).read_text() == 'a file of the tree'
    assert clones.ignored(source) == []


def test_a_bundle_that_the_cas_lost_is_refused(tmp_path: Path, origin: Path, store: Cas) -> None:
    other = clone(origin, tmp_path / 'other')
    head = commit(origin, 'b.txt', 'b')
    blob = bundle(store, Repo(origin), head, [])
    assert blob is not None
    with pytest.raises(Missing):
        unbundle(disk(tmp_path / 'empty'), Repo(other), blob, head)


def test_a_worker_elsewhere_makes_its_clone_on_first_use_and_fills_it_from_bundles(
    tmp_path: Path, origin: Path, store: Cas
) -> None:
    site = remote_site(tmp_path / 'worker', store, fetch_from(store, origin, []), reserve=0)
    source = GitSource(repo=REPO, commit=Repo(origin).head.commit.hexsha)
    site.clones.checkout(source, tmp_path / 'wt')
    assert (tmp_path / 'wt' / 'a.txt').read_text() == 'a'
    assert site.clones.path(source) == tmp_path / 'worker' / 'clones' / str(REPO)
