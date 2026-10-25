"""Commits in and out of a worker's repos as bundles in the CAS, and the checkout of a job named by address."""

import subprocess
from collections.abc import Sequence
from pathlib import Path

import pytest
from covalence import Cid
from git import GitCommandError, Repo

from piloop.ids import CampaignId, Revision, RunId
from piloop.plugins.cas.contract import Cas
from piloop.plugins.executor.contract import RepoId, Source, Workspace
from piloop.plugins.executor.kit.clones import WORKSPACE, Clones, Fetch, of_record
from piloop.plugins.executor.kit.jobs import remote_site
from piloop.plugins.vcs.git import FETCHED, Git
from piloop.plugins.vcs.git.bundles import bundle, unbundle
from piloop.plugins.vcs.git.commands import has
from piloop.store.tree import Manifest, Missing, put_tree
from tests.conftest import disk

REPO = RepoId(CampaignId(1))
LEFTOVERS = RunId(1)


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
    blob = bundle(store, Repo(origin), [head], [base, 'f' * 40])  # a commit it does not know is left out
    assert blob is not None
    unbundle(store, Repo(other), blob, [head])
    assert Repo(other).commit(head).summary == 'c.txt'
    unbundle(store, Repo(other), blob, [head])  # it has the head: nothing to do


def test_no_bundle_when_the_other_side_has_every_commit(origin: Path, store: Cas) -> None:
    head = Repo(origin).head.commit.hexsha
    assert bundle(store, Repo(origin), [head], [head]) is None


def test_a_bundle_that_needs_a_commit_the_repo_lacks_is_refused(tmp_path: Path, origin: Path, store: Cas) -> None:
    other = clone(origin, tmp_path / 'other')
    middle = commit(origin, 'b.txt', 'b')
    head = commit(origin, 'c.txt', 'c')
    blob = bundle(store, Repo(origin), [head], [middle])
    assert blob is not None
    with pytest.raises(GitCommandError):
        unbundle(store, Repo(other), blob, [head])


def fetch_from(store: Cas, origin: Path, asked: list[Sequence[str]]) -> Fetch:
    def fetch(repo: RepoId, revisions: Sequence[Revision], have: Sequence[Revision]) -> Cid | None:
        assert repo == REPO
        asked.append(have)
        return bundle(store, Repo(origin), revisions, have)

    return fetch


def test_a_clone_that_lacks_the_commit_is_filled_from_a_bundle(tmp_path: Path, origin: Path, store: Cas) -> None:
    """A remote worker's clone: the commit comes as a bundle of what it lacks, and is pinned for the next fetch."""
    mine = clone(origin, tmp_path / 'clone')
    known = Repo(mine).head.commit.hexsha
    head = commit(origin, 'b.txt', 'b')
    asked: list[Sequence[str]] = []
    clones = Clones(store, {REPO: mine}.__getitem__, of_record, fetch_from(store, origin, asked), tmp_path)
    source = Source(vcs=Git(), repo=REPO, revision=head)
    clones.checkout(source, tmp_path / 'wt')
    assert (tmp_path / 'wt' / 'b.txt').read_text() == 'b' and known in asked[0]
    assert Repo(mine).git.rev_parse(f'{FETCHED}{head}') == head
    clones.remove(source, tmp_path / 'wt')
    clones.checkout(source, tmp_path / 'wt')
    assert len(asked) == 1, 'it has the commit now'


def workspace_of(store: Cas, root: Path, files: dict[str, str]) -> Workspace:
    """A workspace of ``files`` at ``root``, as ``Sources.agent`` puts it into the CAS."""
    for rel, text in files.items():
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        (root / rel).write_text(text)
    manifest = put_tree(store, root, list(files)).manifest.put(store)
    return Workspace(place='projects/1/workspace', manifest=manifest)


def test_a_clone_gets_the_commits_of_the_earlier_candidates_that_an_agent_reads_and_none_that_the_server_lacks(
    tmp_path: Path, origin: Path, store: Cas
) -> None:
    """A sibling candidate is no ancestor of the base: ``git show`` and ``git cherry-pick`` need it fetched too."""
    base = Repo(origin).head.commit.hexsha
    sibling = commit(origin, 'sibling.txt', 'a candidate of another run')
    Repo(origin).git.checkout('-q', base)
    head = commit(origin, 'b.txt', 'b')
    mine = clone(origin, tmp_path / 'clone')
    Repo(mine).git.update_ref('-d', 'refs/remotes/origin/main')
    Repo(mine).git.reset('-q', '--hard', base)
    Repo(mine).git.reflog('expire', '--expire=now', '--all')
    Repo(mine).git.gc('-q', '--prune=now')
    assert not has(Repo(mine), sibling)
    clones = Clones(store, {REPO: mine}.__getitem__, of_record, fetch_from(store, origin, []), tmp_path)
    source = Source(vcs=Git(), repo=REPO, revision=head, history=[sibling, head, Revision('f' * 40)])
    clones.checkout(source, tmp_path / 'wt')
    assert Repo(tmp_path / 'wt').git.show(f'{sibling}:sibling.txt') == 'a candidate of another run'
    assert Repo(mine).git.rev_parse(f'{FETCHED}{sibling}') == sibling
    assert not has(Repo(mine), 'f' * 40), 'one that the server lacks is left out'


def test_links_and_the_workspace_are_put_read_only(tmp_path: Path, origin: Path, store: Cas) -> None:
    data = tmp_path / 'inputs'
    (data / 'data').mkdir(parents=True)
    (data / 'data' / 'train.csv').write_text('x,y\n')
    links = {'data': put_tree(store, data, ['data']).manifest.put(store)}
    workspace = workspace_of(store, tmp_path / 'live', {'NOTES.md': 'notes\n'})
    clones = Clones(store, {REPO: origin}.__getitem__, of_record, fetch_from(store, origin, []), tmp_path)
    source = Source(vcs=Git(), repo=REPO, revision=Repo(origin).head.commit.hexsha, links=links, workspace=workspace)
    cwd = tmp_path / 'runs' / '1' / 'worktree'
    clones.checkout(source, cwd)
    assert (cwd / 'data' / 'train.csv').read_text() == 'x,y\n'
    assert (cwd / 'data' / 'train.csv').stat().st_mode & 0o222 == 0, 'an input never changes under another job'
    assert (cwd / WORKSPACE / 'NOTES.md').read_text() == 'notes\n'
    assert (cwd / WORKSPACE / 'NOTES.md').stat().st_mode & 0o222 == 0, 'nor does the workspace that it starts from'
    assert clones.ignored(source) == ['data', WORKSPACE]
    (cwd / 'a.txt').write_text('changed')
    assert clones.bring_back(source, cwd, LEFTOVERS) is not None, 'the change, without the links'
    assert Repo(cwd).git.show('--name-only', '--format=', 'HEAD').split() == ['a.txt']
    clones.remove(source, cwd)
    assert not cwd.exists()


def test_bring_back_is_none_without_a_commit(tmp_path: Path, origin: Path, store: Cas) -> None:
    clones = Clones(store, {REPO: origin}.__getitem__, of_record, fetch_from(store, origin, []), tmp_path)
    source = Source(vcs=Git(), repo=REPO, revision=Repo(origin).head.commit.hexsha)
    clones.checkout(source, tmp_path / 'wt')
    assert clones.bring_back(source, tmp_path / 'wt', LEFTOVERS) is None


def test_a_commit_that_the_server_cannot_bundle_fails_the_checkout(tmp_path: Path, origin: Path, store: Cas) -> None:
    def nothing(repo: RepoId, revisions: Sequence[Revision], have: Sequence[Revision]) -> Cid | None:
        return None

    clones = Clones(store, {REPO: origin}.__getitem__, of_record, nothing, tmp_path)
    with pytest.raises(GitCommandError):
        clones.checkout(Source(vcs=Git(), repo=REPO, revision=Revision('f' * 40)), tmp_path / 'wt')


def test_a_tree_that_tracks_the_workspace_name_keeps_its_own(tmp_path: Path, origin: Path, store: Cas) -> None:
    head = commit(origin, WORKSPACE, 'a file of the tree')
    clones = Clones(store, {REPO: origin}.__getitem__, of_record, fetch_from(store, origin, []), tmp_path)
    workspace = workspace_of(store, tmp_path / 'live', {'NOTES.md': 'x'})
    source = Source(vcs=Git(), repo=REPO, revision=head, workspace=workspace)
    clones.checkout(source, tmp_path / 'wt')
    assert (tmp_path / 'wt' / WORKSPACE).read_text() == 'a file of the tree'
    assert clones.ignored(source) == []
    (tmp_path / 'wt' / WORKSPACE).write_text('the agent changed it')
    assert clones.changed(source, tmp_path / 'wt') == {}, 'a file of the tree comes back as a commit'


@pytest.mark.parametrize('restarted', [False, True], ids=['checked-out', 'after-a-restart'])
def test_what_a_job_writes_changes_adds_or_removes_in_its_workspace_comes_back_and_nothing_else(
    tmp_path: Path, origin: Path, store: Cas, restarted: bool
) -> None:
    """A worker that restarted has no stat of the copies it made: it reads them all, and finds the same."""
    files = {'NOTES.md': 'notes\n', 'research/loss.csv': '1\n', 'idea1/out.npy': 'array'}
    workspace = workspace_of(store, tmp_path / 'live', files)
    clones = Clones(store, {REPO: origin}.__getitem__, of_record, fetch_from(store, origin, []), tmp_path)
    source = Source(vcs=Git(), repo=REPO, revision=Repo(origin).head.commit.hexsha, workspace=workspace)
    cwd = tmp_path / 'wt'
    clones.checkout(source, cwd)
    (cwd / WORKSPACE / 'NOTES.md').chmod(0o644)
    (cwd / WORKSPACE / 'NOTES.md').write_text('notes\nmore\n')  # through its link, as a chmod lets a job do
    (cwd / WORKSPACE / 'idea1' / 'out.npy').unlink()
    (cwd / WORKSPACE / 'idea1' / 'out.npy').write_text('a new array')
    (cwd / WORKSPACE / 'research' / 'new.csv').write_text('2\n')
    (cwd / WORKSPACE / 'link').symlink_to('NOTES.md')
    (cwd / WORKSPACE / 'research' / 'loss.csv').unlink()
    (cwd / WORKSPACE / 'research' / 'kept.csv').write_text('1\n')  # its content moved: a removal and an addition
    if restarted:
        clones = Clones(store, {REPO: origin}.__getitem__, of_record, fetch_from(store, origin, []), tmp_path)
    found = clones.changed(source, cwd)
    assert sorted(found) == ['NOTES.md', 'idea1/out.npy', 'research/kept.csv', 'research/loss.csv', 'research/new.csv']
    assert found['research/loss.csv'] is None, 'a removal, and the symlink is left out'
    assert (notes := found['NOTES.md']) is not None and store.get(notes) == b'notes\nmore\n'
    assert clones.changed(Source(vcs=Git(), repo=REPO, revision=source.revision), cwd) == {}, (
        'a job without a workspace'
    )
    assert Manifest.get(store, workspace.manifest).blobs.isdisjoint({found['research/new.csv']})


def test_a_bundle_that_the_cas_lost_is_refused(tmp_path: Path, origin: Path, store: Cas) -> None:
    other = clone(origin, tmp_path / 'other')
    head = commit(origin, 'b.txt', 'b')
    blob = bundle(store, Repo(origin), [head], [])
    assert blob is not None
    with pytest.raises(Missing):
        unbundle(disk(tmp_path / 'empty'), Repo(other), blob, [head])


def test_a_worker_elsewhere_makes_its_clone_on_first_use_and_fills_it_from_bundles(
    tmp_path: Path, origin: Path, store: Cas
) -> None:
    site = remote_site(tmp_path / 'worker', store, fetch_from(store, origin, []), reserve=0)
    source = Source(vcs=Git(), repo=REPO, revision=Repo(origin).head.commit.hexsha)
    site.clones.checkout(source, tmp_path / 'wt')
    assert (tmp_path / 'wt' / 'a.txt').read_text() == 'a'
    assert site.clones.path(source) == tmp_path / 'worker' / 'clones' / str(REPO)
