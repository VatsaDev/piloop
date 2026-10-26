"""Commits in a CAS, as git bundles: how they travel between a worker's replica and the repo of record."""

import subprocess
import tempfile
from collections.abc import Sequence
from pathlib import Path

from covalence import Cid
from git import Repo

from piloop.plugins.cas.contract import Cas
from piloop.plugins.vcs.git import commands as git
from piloop.store.tree import Missing


def bundle(cas: Cas, repo: Repo, heads: Sequence[str], have: Sequence[str]) -> Cid | None:
    """Put a git bundle of what ``heads`` have beyond ``have`` into ``cas``. None when ``have`` holds all of it.

    The one way commits travel: a chain of packs could replace it, and nothing else would change. ``heads`` and
    ``have`` may name commits that ``repo`` lacks: they are left out. The bundle (v3, ``gitformat-bundle``) is written
    by hand around ``git pack-objects``, because ``git bundle create`` needs a ref to name each head, and the repo of
    record is not given one.
    """
    wanted, known = git.commits(repo, heads), git.commits(repo, have)
    if not wanted or int(repo.git.rev_list('--count', *wanted, *(f'^{sha}' for sha in known))) == 0:
        return None
    header = '# v3 git bundle\n'
    header += f'@object-format={repo.git.rev_parse("--show-object-format")}\n'
    header += ''.join(f'-{sha}\n' for sha in known)
    header += ''.join(f'{sha} refs/heads/{i}\n' for i, sha in enumerate(wanted)) + '\n'
    revs = '\n'.join([*wanted, *(f'^{sha}' for sha in known)]) + '\n'
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


def unbundle(cas: Cas, repo: Repo, blob: Cid, heads: Sequence[str]) -> None:
    """Import the commits of the bundle ``blob`` up to ``heads`` into ``repo``, unless it has them all. No ref changes.

    Raises:
        GitCommandError: The bundle needs a commit that ``repo`` lacks, or it is not a bundle.
        Missing: The CAS does not hold it.
    """
    if len(git.commits(repo, heads)) == len(heads):
        return
    with tempfile.TemporaryDirectory(dir=repo.common_dir) as scratch:
        tmp = Path(scratch) / 'bundle'
        if cas.get_file(blob, tmp) is None:
            raise Missing(f'the bundle {blob} is not in the CAS')
        repo.git.bundle('unbundle', str(tmp))
