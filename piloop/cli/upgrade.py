"""Read how uv installed the CLI, and whether its source has a newer piloop.

``piloop upgrade`` reads the source before it changes anything, and ``piloop doctor`` reports a newer piloop there.
"""

import json
import os
import re
import subprocess
import tomllib
from pathlib import Path

from piloop.proc import child_env

RECEIPT = 'uv-receipt.toml'
"""The file in which uv records a tool install, next to the venv of the tool: its requirement and its entry points."""
CHECK_SECONDS = 30
"""How long ``git ls-remote`` or ``uv tool list --outdated`` may take to tell whether the source has a newer piloop."""
OUTDATED = re.compile(r'^piloop v\S+ \[latest: (?P<latest>\S+)\]$', re.MULTILINE)
"""The line of ``uv tool list --outdated`` for piloop when its index has a newer release."""

type Requirement = dict[str, str]
"""One entry of the receipt's ``requirements``: ``name``, plus ``git``, ``directory`` or ``editable``.

An install from a package index has none of the three.
"""


class InstallError(RuntimeError):
    """piloop cannot upgrade this install, or cannot tell whether a newer piloop is out. The message says why."""


def requirement(tool_dir: Path) -> Requirement:
    """Return the requirement that uv installed piloop from, read from the receipt under ``tool_dir``.

    Raises:
        InstallError: There is no receipt, so piloop is not a uv tool. An example is a checkout run with ``uv sync``,
            which ``git pull`` upgrades.
    """
    receipt = tool_dir / 'piloop' / RECEIPT
    if not receipt.is_file():
        raise InstallError(f'piloop is not installed as a uv tool (no {receipt}); upgrade the checkout with git pull')
    return tomllib.loads(receipt.read_text())['tool']['requirements'][0]


def source(req: Requirement) -> str:
    """Return where the CLI came from, as uv recorded it: a git URL, a local path, else a package index."""
    return next((req[key] for key in ('git', 'directory', 'editable') if key in req), 'a package index')


def newer(tool_dir: Path, direct_url: str | None) -> str | None:
    """Return the newer piloop that the install's source has, or None when the installed one is the newest.

    ``direct_url`` is the PEP 610 ``direct_url.json`` of the installed piloop (``piloop.version.direct_url``). A ``git+``
    install is behind when its branch has moved past the installed commit (``git ls-remote``). An install from a
    package index has no ``direct_url.json``, and is behind when ``uv tool list --outdated`` lists it.

    Raises:
        InstallError: piloop is not a uv tool; it was installed with a version constraint, or from a directory, a tag
            or a commit, which do not move; or the source did not answer.
    """
    req = requirement(tool_dir)
    if 'specifier' in req:  # uv tool list --outdated reports releases past it; uv tool upgrade keeps it
        raise InstallError(f'installed as piloop{req["specifier"]}, which `uv tool upgrade` keeps')
    if direct_url is None:
        found = OUTDATED.search(_output(['uv', 'tool', 'list', '--outdated'], UV_TOOL_DIR=str(tool_dir)))
        return None if found is None else f'piloop {found["latest"]}'
    url = json.loads(direct_url)
    vcs = url.get('vcs_info')
    if vcs is None:
        raise InstallError(f'installed from {url["url"]}: update that checkout')
    rev = vcs.get('requested_revision')
    ref = f'refs/heads/{rev}' if rev else 'HEAD'
    heads: dict[str, str] = {}
    for line in _output(['git', 'ls-remote', url['url'], rev or 'HEAD']).splitlines():
        sha, name = line.split('\t', 1)
        heads[name] = sha
    if ref not in heads:
        raise InstallError(f'{rev} is not a branch of {url["url"]}: a pinned install does not move')
    return None if heads[ref] == vcs['commit_id'] else f'commit {heads[ref][:7]} on {rev or "HEAD"} of {url["url"]}'


def _output(argv: list[str], **env: str) -> str:
    """Return the stdout of ``argv`` run with ``env`` added. Raise ``InstallError`` when it fails or does not end.

    It never waits for a password: no stdin, no git prompt, and ssh in batch mode. A private repo without a key or a
    credential helper fails at once.
    """
    quiet = {
        'GIT_TERMINAL_PROMPT': '0',
        'GIT_SSH_COMMAND': f'{os.environ.get("GIT_SSH_COMMAND", "ssh")} -o BatchMode=yes',
    }
    try:
        done = subprocess.run(
            argv,
            capture_output=True,
            stdin=subprocess.DEVNULL,
            text=True,
            timeout=CHECK_SECONDS,
            env=child_env() | quiet | env,
        )
    except (OSError, subprocess.TimeoutExpired) as e:
        raise InstallError(f'{" ".join(argv)} failed: {e}') from e
    if done.returncode:
        raise InstallError(f'{" ".join(argv)} failed: {done.stderr.strip() or f"exit {done.returncode}"}')
    return done.stdout
