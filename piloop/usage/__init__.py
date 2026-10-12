"""Token usage of each agent, kept in ``workers.usage``.

piloop reads the usage from the files that the agent's harness leaves when the agent ends. This module knows no
harness. A harness definition has a ``UsageSpec``: the name of a *reader* and the reader's parameters. This module
finds the reader in ``READERS`` and calls it. The one built-in reader, ``jsonl`` (``piloop/usage/jsonl.py``), reads a
JSONL session file under the home directory. Its parameters say where the file is, how the harness names the folder
of the agent's cwd, and which *tally* adds up the records. The tallies of the three built-in harnesses are modules
under ``piloop/usage/tallies/`` that register themselves.

So the usage of a new harness is a spec in ``piloop.toml``. A new file format is one tally module. A new kind of
source (a stdout log, an API) is one reader.

The result always has the same shape, so one ``json_extract`` reads it in SQL. ``total`` is what the harness itself
counts as the session's tokens. The cache fields follow the convention of the harness, and each tally's docstring
says which convention that is.
"""

from collections.abc import Callable
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Protocol, Self

from pydantic import BaseModel, ConfigDict, model_validator
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from piloop.db.models import UsageJson, Worker


@dataclass(frozen=True)
class Usage:
    """The token usage of one session. ``as_json`` gives the value that ``workers.usage`` stores."""

    input: int
    output: int
    cache_read: int
    cache_write: int
    total: int
    turns: int
    """The number of model responses (API calls) in the session."""
    source: str
    """Where the numbers came from: the session file that was read."""

    def as_json(self) -> UsageJson:
        return UsageJson(
            input=self.input,
            output=self.output,
            cache_read=self.cache_read,
            cache_write=self.cache_write,
            total=self.total,
            turns=self.turns,
            source=self.source,
        )


class UsageError(RuntimeError):
    """The usage could not be read.

    The message says why: no session id, no file, no usage in the file, or a harness that is no longer configured.
    """


class UsageSpec(BaseModel):
    """``[harness.<name>.usage]``: how to read the tokens that an agent of this harness spent, after it ended.

    ``reader`` names the code that reads them, and the other keys are its parameters. The built-in harnesses set
    their specs as data. A project can override any key, or give a custom harness a complete spec. piloop checks the
    names when the config loads.
    """

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    reader: str = 'jsonl'
    """The reader to use. ``jsonl`` reads a JSONL session file under the home directory, which the keys below
    describe. It is the only built-in reader."""
    file: str | None = None
    """For ``jsonl``: a glob of the session file. piloop substitutes ``{home}``, ``{cwd}`` (the agent's cwd, encoded
    as ``cwd`` says) and ``{session_id}``. Example: ``{home}/.pi/agent/sessions/{cwd}/*_{session_id}.jsonl``. When the
    glob matches nothing, piloop tries again with ``*`` for ``{cwd}``. This searches one level of folders, not
    deeper, and the session id makes the match unique. When several files match, piloop takes the newest one."""
    cwd: str = 'none'
    """For ``jsonl``: how the harness names the folder of the agent's cwd. ``dash_separators_wrapped`` turns
    ``/home/me/repo`` into ``--home-me-repo--``. ``dash_non_alnum`` turns it into ``-home-me-repo``. ``none`` means
    that the folders are not named after the cwd, and matches any folder (``*``)."""
    tally: str | None = None
    """For ``jsonl``: the file format, which says which records have token counts, how they add up, and which
    records hold the agent's own text. One of ``pi``, ``claude`` or ``codex``."""

    @model_validator(mode='after')
    def _check(self) -> Self:
        resolve(self)  # the reader checks its own parameters; ``resolve`` caches the reader for ``resolved``
        return self

    @property
    def resolved(self) -> UsageReader:
        """The reader that this spec names. Built once: the spec is frozen, so an equal spec gives the same reader."""
        return resolve(self)


class UsageReader(Protocol):
    """One source of token counts, built from a ``UsageSpec``."""

    def read(self, session_id: str | None, cwd: Path | None, log_dir: Path | None) -> Usage:
        """The usage of the ended session. ``cwd`` and ``log_dir`` are the agent's, when they are still known.

        Raises:
            UsageError: Nothing could be read. The message says why.
        """
        ...

    def said(self, session_id: str | None, lines: int = 1) -> list[str]:
        """The last ``lines`` lines of text that the agent wrote so far, while it runs. Empty when there are none.

        Never raises.
        """
        ...


type ReaderFactory = Callable[[UsageSpec], UsageReader]
"""A class or function that builds a reader from its spec. It raises ``ValueError`` for a parameter it cannot use."""

READERS: dict[str, ReaderFactory] = {}
"""The reader factories by name. Each reader module registers its reader here when it is imported."""


def reader[M: ReaderFactory](name: str) -> Callable[[M], M]:
    """Register a reader factory (usually a class) under ``name``. The decorator returns the object unchanged."""

    def register(make: M) -> M:
        READERS[name] = make
        return make

    return register


@cache
def resolve(spec: UsageSpec) -> UsageReader:
    """The reader that ``spec`` names, built from the spec. One reader for each distinct spec.

    Raises:
        ValueError: The reader is unknown, or it refuses one of the spec's parameters. The message says which.
    """
    make = READERS.get(spec.reader)
    if make is None:
        raise ValueError(f'unknown usage reader {spec.reader!r}; known: {sorted(READERS)}')
    return make(spec)


def read_usage(spec: UsageSpec | None, session_id: str | None, cwd: Path | None, log_dir: Path | None) -> Usage | None:
    """The tokens of an ended session, read as ``spec`` says. None when the harness has no spec.

    Raises:
        UsageError: The reader found nothing to add up.
    """
    if spec is None:
        return None
    return spec.resolved.read(session_id, cwd, log_dir)


def project_tokens(s: Session, project_id: int) -> int:
    """The tokens that all agents of the project spent: ``SUM(json_extract(usage, '$.total'))`` over its workers."""
    total = s.scalar(select(func.sum(Worker.usage['total'].as_integer())).where(Worker.project_id == project_id))
    return int(total or 0)


def human(tokens: int) -> str:
    """A short form of a token count: ``1.2M``, ``340k``, ``987``."""
    if tokens >= 1_000_000:
        return f'{tokens / 1_000_000:.1f}M'
    if tokens >= 1_000:
        return f'{tokens / 1_000:.0f}k'
    return str(tokens)


# The built-in readers and tallies register themselves on import. They are imported here, after the names they use
# exist, so that every importer of piloop.usage sees the full registries, whatever it imported first.
from piloop.usage import jsonl, tallies  # noqa: E402, F401
