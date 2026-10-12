"""The ``jsonl`` reader: a JSONL session file under the home directory, found by a glob and added up by a tally.

Every built-in harness keeps its transcript this way: one JSON object per line, with token counts for each response.
The harnesses differ only in where the file is, how its folder is named after the agent's cwd, and which records
have the numbers. These are the reader's parameters (``UsageSpec.file``, ``cwd``, ``tally``). The encoders and
tallies are registries, so this module names no harness.

While the agent runs, the same file holds what it wrote. ``said`` reads the last line of the agent's text for
``piloop status`` and ``piloop watch``, through a second registry with the same format names.
"""

import glob
import json
import re
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Protocol

from piloop.usage import Usage, UsageError, UsageSpec, reader

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

    Zero turns means that nothing was found. A tally module registers one with ``@tally('<name>')``.
    """

    def __call__(self, records: Iterable[Record]) -> tuple[Counts, int]:
        """Add up ``records``. Return the counts and the number of responses."""
        ...


type Said = Callable[[Record], str | None]
"""The text that the agent itself wrote in one record of a session file, or None. Tool calls and results are not
text."""

type CwdEncoder = Callable[[Path], str]
"""How a harness names the folder of an agent's cwd."""

CWD_ENCODERS: dict[str, CwdEncoder] = {}
"""The cwd encoders by name (``UsageSpec.cwd``)."""
TALLIES: dict[str, Tally] = {}
"""The tallies by name (``UsageSpec.tally``). The modules in ``piloop/usage/tallies/`` fill it."""
SAID: dict[str, Said] = {}
"""How to find the agent's text in each file format, by tally name. A format without an entry shows no text."""
TAIL_BYTES = 1024 * 1024
"""How many bytes at the end of a session file ``said`` reads. Tool results fill most of a file, so the agent's last
text can be far from the end. Reading 1 MiB takes about 10 ms, once for each change of the file."""


def cwd_encoder[E: CwdEncoder](name: str) -> Callable[[E], E]:
    """Register a cwd encoder under ``name``. The type checker rejects a function of the wrong shape."""

    def register(fn: E) -> E:
        CWD_ENCODERS[name] = fn
        return fn

    return register


def tally[T: Tally](name: str) -> Callable[[T], T]:
    """Register a tally under ``name``. The type checker rejects a function of the wrong shape."""

    def register(fn: T) -> T:
        TALLIES[name] = fn
        return fn

    return register


def said[S: Said](name: str) -> Callable[[S], S]:
    """Register how format ``name`` holds the agent's text. The type checker rejects a function of the wrong shape."""

    def register(fn: S) -> S:
        SAID[name] = fn
        return fn

    return register


@cwd_encoder('none')
def no_cwd(cwd: Path) -> str:
    """The folders are not named after the cwd: match any folder."""
    return '*'


@cwd_encoder('dash_separators_wrapped')
def dash_separators_wrapped(cwd: Path) -> str:
    r"""``/home/me/.piloop/runs/10/worktree`` → ``--home-me-.piloop-runs-10-worktree--``.

    The leading slash is removed, path separators (and ``:``) become dashes, and two dashes go around the result.
    This is what pi's session manager does: ``--${cwd.replace(/^[/\\]/, "").replace(/[/\\:]/g, "-")}--``.
    """
    return f'--{re.sub(r"[/\\:]", "-", str(cwd).lstrip("/\\"))}--'


@cwd_encoder('dash_non_alnum')
def dash_non_alnum(cwd: Path) -> str:
    """``/home/me/.piloop/runs/2/worktree`` → ``-home-me--piloop-runs-2-worktree``.

    Every character outside ``[a-zA-Z0-9]`` becomes a dash. Claude Code names its project folders this way.
    """
    return re.sub(r'[^a-zA-Z0-9]', '-', str(cwd))


@reader('jsonl')
class JsonlReader:
    """Finds the session file by the spec's glob, parses its lines, and gives the records to the spec's tally.

    The names in the spec are looked up once, here: ``encode_cwd`` and ``tally`` are the registered objects.
    """

    file: str
    encode_cwd: CwdEncoder
    tally: Tally
    words: Said | None

    def __init__(self, spec: UsageSpec) -> None:
        if spec.file is None:
            raise ValueError('the jsonl usage reader needs `file`, the glob of the session file')
        encode_cwd = CWD_ENCODERS.get(spec.cwd)
        if encode_cwd is None:
            raise ValueError(f'unknown usage cwd encoder {spec.cwd!r}; known: {sorted(CWD_ENCODERS)}')
        if spec.tally not in TALLIES:
            raise ValueError(f'unknown usage tally {spec.tally!r}; known: {sorted(TALLIES)}')
        self.file, self.encode_cwd, self.tally = spec.file, encode_cwd, TALLIES[spec.tally]
        self.words = SAID.get(spec.tally)

    def read(self, session_id: str | None, cwd: Path | None, log_dir: Path | None) -> Usage:
        """Add up the session file. This reader does not use ``log_dir``.

        Raises:
            UsageError: There is no session id, no file for it under the home directory, or no usage in the file.
        """
        if session_id is None:
            raise UsageError('no session id')
        path = self.session_file(session_id, cwd)
        if path is None:
            raise UsageError(f'no session file for {session_id} under {Path.home()}')
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
            source=str(path),
        )

    def said(self, session_id: str | None, lines: int = 1) -> list[str]:
        """The last ``lines`` non-empty lines of text that the agent wrote in its session so far, oldest first.

        Empty when there is no session, no file or no text yet. The file is found by a search of every cwd folder,
        because the row of a running agent does not have its cwd. That is one glob per call, a few milliseconds.
        """
        if self.words is None or session_id is None:
            return []
        path = self.session_file(session_id, None)
        if path is None:
            return []
        try:
            stat = path.stat()
            return list(last_said(path, stat.st_mtime_ns, stat.st_size, self.words, lines))
        except OSError:  # the file was removed between the glob and the read: show nothing this time
            return []

    def session_file(self, session_id: str, cwd: Path | None) -> Path | None:
        """Find the file with the encoded cwd, or else with ``*`` for ``{cwd}``: one level of folders, not deeper.

        The search with ``*`` finds a cwd that is encoded in an unexpected way, and an agent whose worktree no longer
        exists (a refill). The session id is a uuid, so the match is unique. When several files match (Codex date
        folders, a copied file), the newest one wins. When ``cwd`` is None, only the search with ``*`` runs.
        """
        folders = [glob.escape(self.encode_cwd(cwd))] if cwd is not None else []
        for folder in [*folders, '*']:
            pattern = (
                self.file.replace('{home}', str(Path.home()))
                .replace('{cwd}', folder)
                .replace('{session_id}', glob.escape(session_id))
            )
            matches = [Path(p) for p in glob.glob(pattern)]
            if matches:
                return max(matches, key=lambda p: p.stat().st_mtime)
        return None


def records(path: Path) -> Iterator[Record]:
    """The JSON objects of a JSONL file. Other lines are skipped, such as the incomplete last line of a killed agent."""
    for line in path.read_text(errors='replace').splitlines():
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if isinstance(rec, dict):
            yield rec


@lru_cache(maxsize=64)
def last_said(path: Path, mtime_ns: int, size: int, words: Said, count: int) -> tuple[str, ...]:
    """The last ``count`` non-empty lines of the text that ``words`` finds in the last ``TAIL_BYTES`` of the file.

    Reads the newest record first, and stops when it has enough lines. ``mtime_ns`` and ``size`` are part of the cache
    key, so a redraw reads again only the files that changed since the last redraw.
    """
    with path.open('rb') as f:
        f.seek(max(0, size - TAIL_BYTES))
        records = f.read().decode(errors='replace').splitlines()
    kept: list[str] = []
    for line in reversed(records[1:] if size > TAIL_BYTES else records):  # the seek cuts the first line
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        text = words(rec) if isinstance(rec, dict) else None
        kept[:0] = [part.rstrip() for part in (text or '').splitlines() if part.strip()]
        if len(kept) >= count:
            break
    return tuple(kept[-count:])


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
