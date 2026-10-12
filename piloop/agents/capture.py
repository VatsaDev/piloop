"""Session capture through Entire. When an agent ends, attach the harness's session by the id the launcher learned.

piloop calls Entire once per agent session. ``entire session attach <id> --agent <name>`` reads the harness's session
file, redacts it and writes the checkpoint ref ``refs/entire/checkpoints/<id[-2:]>/<id>`` into the repo. piloop keeps
the id on the ``workers`` row. No hooks, no config in the tree, nothing per commit.
"""

import json
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

from piloop.db.models import Worker
from piloop.harnesses import Harness
from piloop.proc import STDERR_IN_ERROR, child_env
from piloop.settings import settings

ENTIRE = 'entire'
ENTIRE_DIR = '.entire'
"""Entire's per-repo settings and the ``.gitignore`` for its scratch files.

``settings.json`` stores checkpoints as git refs, never pushes sessions and turns telemetry off. ``piloop init``
commits the directory. Entire reads it from the tree of the worktree it runs in. A worktree of an older tree that does
not have it gets a link to the project's (``piloop.campaign.project.worktree_links``).
"""
ENTIRE_INSTALL = 'curl -fsSL https://entire.io/install.sh | bash'
MIN_ENTIRE = '0.10.6'
"""The oldest Entire CLI that piloop attaches sessions with. An older one is not used."""
VERSION = re.compile(r'\d+(?:\.\d+)+')
"""Finds the version in ``Entire CLI 0.10.6``, the first line of ``entire version``."""


@dataclass(frozen=True)
class Attached:
    """What ``entire session info <sid> --json`` reports about a session that was just attached."""

    checkpoint_id: str
    agent: str
    """Entire's display name for the harness (``Codex``)."""
    tokens: int | None
    """The session's ``tokens.total``."""


class AttachError(RuntimeError):
    """An ``entire`` call failed or gave no checkpoint. The message says why."""


def attachable(worker: Worker, harness: Harness | None) -> tuple[str, str] | None:
    """Return ``(session_id, entire agent)`` of a worker whose session Entire can attach, else None."""
    agent = harness.entire_agent if harness is not None else None
    return (worker.session_id, agent) if agent and worker.session_id else None


def attach_session(session_id: str, agent: str, cwd: Path) -> Attached:
    """Attach an ended harness session to Entire from ``cwd``, then read back the checkpoint it made.

    ``cwd`` is the repo that gets the checkpoint ref, and where Entire looks for the session file first, because pi
    and Claude Code key their stores by the agent's cwd. It is the agent's worktree while that exists, else the project
    checkout. The call is non-interactive, so Entire prints the trailer and amends nothing. The checkpoint id comes
    from ``session info --json`` (``last_checkpoint_id``), not from the printed text.

    Raises:
        AttachError: ``entire`` is not on PATH, exited non-zero, exceeded ``entire_attach_timeout_seconds`` or
            reports no checkpoint for the session.
    """
    _entire('session', 'attach', session_id, '--agent', agent, cwd=cwd)
    info = json.loads(_entire('session', 'info', session_id, '--json', cwd=cwd))
    checkpoint = info.get('last_checkpoint_id')
    if not checkpoint:
        raise AttachError(f'{ENTIRE} session info reports no checkpoint for session {session_id}')
    total = (info.get('tokens') or {}).get('total')
    return Attached(str(checkpoint), str(info.get('agent', agent)), None if total is None else int(total))


def entire_version() -> str:
    """Return the version of the installed Entire CLI. ``entire version`` starts with ``Entire CLI 0.10.6``.

    Raises:
        AttachError: ``entire`` is not on PATH (the message says how to install it), failed or printed no version.
    """
    first = _entire('version', cwd=Path.cwd()).partition('\n')[0]
    found = VERSION.search(first)
    if found is None:
        raise AttachError(f'{ENTIRE} version printed no version: {first[:STDERR_IN_ERROR]}')
    return found.group()


def usable(version: str) -> bool:
    """Return whether Entire ``version`` is at least ``MIN_ENTIRE``."""
    return _parts(version) >= _parts(MIN_ENTIRE)


def _parts(version: str) -> tuple[int, ...]:
    return tuple(int(p) for p in version.split('.'))


def _entire(*args: str, cwd: Path) -> str:
    """Run ``entire <args>`` in ``cwd`` within ``entire_attach_timeout_seconds`` and return its stdout."""
    timeout = settings.entire_attach_timeout_seconds
    what = f'{ENTIRE} {" ".join(args[:2])}'
    try:
        done = subprocess.run(
            [ENTIRE, *args],
            cwd=cwd,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=child_env(),
        )
    except FileNotFoundError as e:
        raise AttachError(f'{ENTIRE} is not on PATH; install it: {ENTIRE_INSTALL}') from e
    except subprocess.TimeoutExpired as e:
        raise AttachError(f'{what} timed out after {timeout:g}s') from e
    if done.returncode:
        raise AttachError(f'{what} exit {done.returncode}: {done.stderr.strip()[:STDERR_IN_ERROR]}')
    return done.stdout
