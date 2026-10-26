"""A harness's JSONL session file: found by a glob under its home, and added up by its tally.

Every built-in harness keeps its transcript this way: one JSON object per line, with token counts for each response.
The harnesses differ only in where the file is, how its folder is named after the agent's cwd, and which records
have the numbers and the agent's text. A provider passes these to ``JsonlSessions``.
"""

import glob
import json
import os
import re
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from piloop.settings import expand_env
from piloop.usage import Usage, UsageError

type Record = dict[str, object]
"""One line of a session file, as ``json.loads`` gives it. The shapes inside are the harness's own."""


@dataclass
class Counts:
    """What a tally adds up: the ``Usage`` fields without the ones the reader adds (turns, source)."""

    input: int = 0
    output: int = 0
    cache_read: int = 0
    cache_write: int = 0
    total: int = 0


class Tally(Protocol):
    """How the records of one file format add up: the records of a session file → (counts, turns).

    Zero turns means that nothing was found.
    """

    def __call__(self, records: Iterable[Record]) -> tuple[Counts, int]:
        """Add up ``records``. Return the counts and the number of responses."""
        ...


type Said = Callable[[Record], str | None]
"""The text that the agent itself wrote in one record of a session file, or None. Tool calls and results are not
text."""

type CwdEncoder = Callable[[Path], str]
"""How a harness names the folder of an agent's cwd."""

TAIL_BYTES = 1024 * 1024
"""How many bytes at the end of a session file ``said`` reads. Tool results fill most of a file, so the agent's last
text can be far from the end."""


def no_cwd(cwd: Path) -> str:
    """The folders are not named after the cwd: match any folder."""
    return '*'


def dash_separators_wrapped(cwd: Path) -> str:
    r"""``/home/me/.piloop/runs/10/worktree`` → ``--home-me-.piloop-runs-10-worktree--``.

    The leading slash is removed, path separators (and ``:``) become dashes, and two dashes go around the result.
    This is what pi's session manager does: ``--${cwd.replace(/^[/\\]/, "").replace(/[/\\:]/g, "-")}--``.
    """
    return f'--{re.sub(r"[/\\:]", "-", str(cwd).lstrip("/\\"))}--'


def dash_non_alnum(cwd: Path) -> str:
    """``/home/me/.piloop/runs/2/worktree`` → ``-home-me--piloop-runs-2-worktree``.

    Every character outside ``[a-zA-Z0-9]`` becomes a dash. Claude Code names its campaign folders this way.
    """
    return re.sub(r'[^a-zA-Z0-9]', '-', str(cwd))


@dataclass(frozen=True)
class JsonlSessions:
    """A harness's session files: found by ``file``, with the agent's cwd encoded by ``cwd``, added up by ``tally``."""

    file: str
    """A glob of the session file. piloop substitutes ``{env:NAME:default}`` (the environment variable ``NAME``, or
    ``default`` when it is unset; a default may start with ``~``), ``{cwd}`` (the agent's cwd, encoded by ``cwd``) and
    ``{session_id}``. When the glob matches nothing, piloop tries again with ``*`` for ``{cwd}``. This searches one
    level of folders, not deeper, and the session id makes the match unique. When several files match, piloop takes
    the newest one."""
    cwd: CwdEncoder
    tally: Tally
    words: Said

    def read(self, session_id: str | None, cwd: Path | None, log_dir: Path | None) -> Usage:
        """Add up the session file. This reader does not use ``log_dir``.

        Raises:
            UsageError: There is no session id, no file for it, or no usage in the file.
        """
        if session_id is None:
            raise UsageError('no session id')
        path = self.session_file(session_id, cwd)
        if path is None:
            raise UsageError(f'no session file for {session_id}: nothing matches {self.pattern(session_id)}')
        counts, turns = self.tally(records(path))
        if not turns:
            raise UsageError(f'no usage records in {path}')
        return Usage(
            input=counts.input,
            output=counts.output,
            cache_read=counts.cache_read,
            cache_write=counts.cache_write,
            total=counts.total,
            turns=turns,
        )

    def said(self, session: bytes, lines: int = 1) -> list[str]:
        """The last ``lines`` non-empty lines of text that the agent wrote in ``session``, oldest first.

        Reads the newest record first, in the last ``TAIL_BYTES``, and stops when it has enough lines. Empty when there
        is no text.
        """
        tail = session[-TAIL_BYTES:].decode(errors='replace').splitlines()
        kept: list[str] = []
        for line in reversed(tail[1:] if len(session) > TAIL_BYTES else tail):  # the cut goes through the first line
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            text = self.words(rec) if isinstance(rec, dict) else None
            kept[:0] = [part.rstrip() for part in (text or '').splitlines() if part.strip()]
            if len(kept) >= lines:
                break
        return kept[-lines:]

    def session_file(self, session_id: str, cwd: Path | None) -> Path | None:
        """Find the file with the encoded cwd, or else with ``*`` for ``{cwd}``: one level of folders, not deeper.

        The search with ``*`` finds a cwd that is encoded in an unexpected way, and an agent whose worktree no longer
        exists (a refill). The session id is a uuid, so the match is unique. When several files match (Codex date
        folders, a copied file), the newest one wins. When ``cwd`` is None, only the search with ``*`` runs.
        """
        folders = [glob.escape(self.cwd(cwd))] if cwd is not None else []
        for folder in [*folders, '*']:
            matches = [Path(p) for p in glob.glob(self.pattern(session_id, folder))]
            if matches:
                return max(matches, key=lambda p: p.stat().st_mtime)
        return None

    def pattern(self, session_id: str, folder: str = '*') -> str:
        """The glob of the session file: ``file`` with its placeholders filled in, and ``~`` as the user's home."""
        expanded = os.path.expanduser(expand_env(self.file))
        return expanded.replace('{cwd}', folder).replace('{session_id}', glob.escape(session_id))


def records(path: Path) -> Iterator[Record]:
    """The JSON objects of a JSONL file. Other lines are skipped, such as the incomplete last line of a killed agent."""
    for line in path.read_text(errors='replace').splitlines():
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if isinstance(rec, dict):
            yield rec


def texts(content: object, kind: str) -> str | None:
    """The ``text`` of every block of type ``kind`` in a message's ``content`` list, one per line, or None."""
    blocks = content if isinstance(content, list) else []
    found = [
        b['text'] for b in blocks if isinstance(b, dict) and b.get('type') == kind and isinstance(b.get('text'), str)
    ]
    return '\n'.join(found) if found else None


def child(rec: Record, key: str) -> Record | None:
    """The object under ``key``, or None when it is missing or not an object. Tallies use it to read nested records."""
    value = rec.get(key)
    return value if isinstance(value, dict) else None


def zero() -> Counts:
    """Empty counts for a tally to fill."""
    return Counts()


def add(counts: Counts, usage: Record, *, input: str, output: str, cache_read: str, cache_write: str) -> None:
    """Add the harness's fields to ``counts``. Each argument is the harness's key for the field. Non-numbers add 0."""
    counts.input += as_int(usage.get(input))
    counts.output += as_int(usage.get(output))
    counts.cache_read += as_int(usage.get(cache_read))
    counts.cache_write += as_int(usage.get(cache_write))


def as_int(value: object) -> int:
    """A count from a JSON value: ints and floats as they are, anything else 0."""
    return int(value) if isinstance(value, int | float) else 0
