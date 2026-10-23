"""Commits in a CAS, as git bundles: how they travel between a worker's clone and the repo of record."""

import subprocess
import tempfile
from collections.abc import Sequence
from pathlib import Path

from covalence import Cid
from git import Repo

from piloop import git
from piloop.plugins.cas.contract import Cas
from piloop.store.tree import Missing


def bundle(cas: Cas, repo: Repo, head: str, have: Sequence[str]) -> Cid | None:
    """Put a git bundle of what ``head`` has beyond ``have`` into ``cas``. None when ``have`` holds all of it.

    The one way commits travel: a chain of packs could replace it, and nothing else would change. ``have`` may name
    commits that ``repo`` lacks: they are left out. The bundle (v3, ``gitformat-bundle``) is written by hand around
    ``git pack-objects``, because ``git bundle create`` needs a ref to name ``head``, and the repo of record is not
    given one.
    """
    known = _commits_of(repo, have)
    if int(repo.git.rev_list('--count', head, *(f'^{sha}' for sha in known))) == 0:
        return None
    header = '# v3 git bundle\n'
    header += f'@object-format={repo.git.rev_parse("--show-object-format")}\n'
    header += ''.join(f'-{sha}\n' for sha in known) + f'{head} HEAD\n\n'
    revs = '\n'.join([head, *(f'^{sha}' for sha in known)]) + '\n'
    with tempfile.TemporaryDirectory(dir=repo.common_dir) as scratch:
        tmp = Path(scratch) / 'bundle'
        with tmp.open('wb') as out:
            out.write(header.encode())
            out.flush()
            subprocess.run(
                ['git', 'pack-objects', '--stdout', '--revs', '-q'],
                cwd=repo.working_tree_dir or repo.git_dir,
                input=revs.encode(),
                stdout=out,
                stderr=subprocess.PIPE,
                check=True,
            )
        return cas.put_file(tmp)


def _commits_of(repo: Repo, shas: Sequence[str]) -> list[str]:
    """Those of ``shas`` that are commits in ``repo``, in one ``git cat-file``: a worker names every commit it has."""
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


def unbundle(cas: Cas, repo: Repo, blob: Cid, head: str) -> None:
    """Import the commits of the bundle ``blob`` up to ``head`` into ``repo``, unless it has ``head``. No ref changes.

    Raises:
        GitCommandError: The bundle needs a commit that ``repo`` lacks, or it is not a bundle.
        Missing: The CAS does not hold it.
    """
    if git.has(repo, head):
        return
    with tempfile.TemporaryDirectory(dir=repo.common_dir) as scratch:
        tmp = Path(scratch) / 'bundle'
        if cas.get_file(blob, tmp) is None:
            raise Missing(f'the bundle {blob} is not in the CAS')
        repo.git.bundle('unbundle', str(tmp))
