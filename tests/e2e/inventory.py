"""What a user's piloop home holds, read before and after an upgrade, and the differences between two such readings.

``take`` reads the database (one consistent copy, every row of every table but the full-text shadows), the campaign
repo's git refs, commits and worktrees, and a SHA-256 of every file of the home, of the repo and of the harness
transcripts of its agents. ``differences`` compares two readings after ``Renames`` maps the old names to the new.
"""

import hashlib
import json
import os
import sqlite3
import subprocess
import tempfile
from collections.abc import Iterable, Mapping
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path

from covalence import Cid

type Row = dict[str, object]

SKIPPED_FILES = frozenset({'piloop.db', 'piloop.db-wal', 'piloop.db-shm', 'serve.log', 'serve.pid'})
"""Files of the home that change by design while a daemon runs, or that the database comparison covers."""
SKIPPED_DIRS = frozenset({'.git', '.venv', 'data', '__pycache__', 'store'})
"""The git internals (the refs are compared instead), the linked data and venvs, and the store (``piloop store
verify`` checks it)."""


SIGNED = frozenset({'actor', 'author', 'acked_by', 'cancelled_by', 'created_by', 'updated_by', 'verdict_by'})
"""The columns that hold a signature, ``<role>#<id>``."""


@dataclass(frozen=True)
class Renames:
    """The documented renames of an upgrade: what an old name is called after it."""

    tables: Mapping[str, str] = field(default_factory=dict)
    columns: Mapping[str, str] = field(default_factory=dict)
    """A renamed column: ``<table>.<column>`` for one table, or ``<column>`` for every table that has it."""
    target_kinds: Mapping[str, str] = field(default_factory=dict)
    """Values of ``comments.target_kind`` and ``events.target_kind``."""
    event_prefixes: Mapping[str, str] = field(default_factory=dict)
    """A prefix of ``events.kind``, for example ``project.`` to ``campaign.``. The first that matches applies."""
    values: Mapping[tuple[str, str, str], str] = field(default_factory=dict)
    """``(table, column, old value)`` to the new value, in the new names, for example a role's name."""
    roles: Mapping[str, str] = field(default_factory=dict)
    """A renamed role: in the signatures (``SIGNED``, and a quoted ``"<role>#`` in a payload), and as a
    ``[roles.<role>]`` table of a stored config."""
    payload_keys: Mapping[str, str] = field(default_factory=dict)
    """A renamed top-level key of an event's payload."""
    blobs: frozenset[tuple[str, str]] = frozenset()
    """``(table, column)``, in the new names, whose text moved into the CAS: the new value is the text's CID."""

    def row(self, table: str, row: Row) -> Row:
        """``row`` of ``table`` (its new name) in the new names."""
        named = {c: self.columns.get(f'{table}.{c}', self.columns.get(c, c)) for c in row}
        return {named[c]: self.value(table, named[c], v) for c, v in row.items()}

    def value(self, table: str, column: str, value: object) -> object:
        if not isinstance(value, str):
            return value
        if (table, column) in self.blobs:
            return str(Cid.of(value.encode()))
        if column == 'target_kind':
            value = self.target_kinds.get(value, value)
        if table == 'events' and column == 'kind':
            value = next(
                (n + value.removeprefix(o) for o, n in self.event_prefixes.items() if value.startswith(o)), value
            )
        if column in SIGNED and '#' in value:
            role, _, rest = value.partition('#')
            value = f'{self.roles.get(role, role)}#{rest}'
        if column == 'payload':
            for old, new in self.roles.items():
                value = value.replace(f'"{old}#', f'"{new}#')
            payload = json.loads(value)
            value = json.dumps({self.payload_keys.get(k, k): v for k, v in payload.items()})
        if column == 'config':
            config = json.loads(value)
            if isinstance(roles := config.get('roles'), dict):
                config['roles'] = {self.roles.get(k, k): v for k, v in roles.items()}
            value = json.dumps(config)
        return self.values.get((table, column, value), value)


def same(before: object, after: object) -> bool:
    """Equal, or the same JSON document in another layout (SQLite's ``json_set`` writes it without spaces)."""
    if before == after:
        return True
    if isinstance(before, str) and isinstance(after, str) and before[:1] in '{[' and after[:1] in '{[':
        try:
            return json.loads(before) == json.loads(after)
        except json.JSONDecodeError:
            return False
    return False


@dataclass(frozen=True)
class Worktree:
    path: str
    head: str | None
    dirty: str | None
    """``git status --porcelain`` in it; None when the directory is gone."""


@dataclass(frozen=True)
class Inventory:
    revision: str
    tables: dict[str, list[Row]]
    """Every row of every table, by ``__rowid``."""
    integrity: str
    foreign_key_violations: int
    head: str
    refs: dict[str, str]
    commits: frozenset[str]
    worktrees: dict[str, Worktree]
    files: dict[str, str]
    """SHA-256 (or ``symlink:<target>``) by path: ``home/…``, ``repo/…`` and ``transcripts/…``."""


def take(home: Path, repo: Path, transcripts: Iterable[Path]) -> Inventory:
    """Read ``home``, its campaign ``repo``, and the directories of its agents' harness ``transcripts``."""
    with tempfile.TemporaryDirectory() as tmp:
        copy = Path(tmp) / 'copy.db'
        with (
            closing(sqlite3.connect(f'file:{home / "piloop.db"}?mode=ro', uri=True, timeout=30)) as src,
            closing(sqlite3.connect(copy)) as dst,
        ):
            src.backup(dst)  # one consistent snapshot of a live database
        tables, revision, integrity, violations = read_database(copy)
    worktrees = {}
    for block in git(repo, 'worktree', 'list', '--porcelain').split('\n\n'):
        lines = dict((line.split(' ', 1) + [''])[:2] for line in block.splitlines())
        if 'worktree' in lines:
            path = Path(lines['worktree'])
            dirty = git(path, 'status', '--porcelain') if path.is_dir() else None
            worktrees[str(path)] = Worktree(str(path), lines.get('HEAD'), dirty)
    refs = {}
    for line in git(repo, 'for-each-ref', '--format=%(objectname) %(refname)').splitlines():
        sha, ref = line.split(' ', 1)
        refs[ref] = sha
    files = {f'home/{k}': v for k, v in hashes(home).items() if k not in SKIPPED_FILES}
    files |= {f'repo/{k}': v for k, v in hashes(repo).items()}
    for directory in transcripts:
        files |= {f'transcripts/{directory.name}/{k}': v for k, v in hashes(directory).items()}
    return Inventory(
        revision=revision,
        tables=tables,
        integrity=integrity,
        foreign_key_violations=violations,
        head=git(repo, 'rev-parse', 'HEAD').strip(),
        refs=refs,
        commits=frozenset(git(repo, 'rev-list', '--all').split()),
        worktrees=worktrees,
        files=files,
    )


def read_database(path: Path) -> tuple[dict[str, list[Row]], str, str, int]:
    """Every row of every table of the database at ``path`` but the full-text indexes, its revision, its
    ``integrity_check`` and its number of foreign key violations."""
    with closing(sqlite3.connect(f'file:{path}?mode=ro', uri=True)) as con:
        con.row_factory = sqlite3.Row
        names = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")]
        tables = {
            t: [dict(r) for r in con.execute(f'SELECT rowid AS __rowid, * FROM "{t}" ORDER BY rowid')]
            for t in names
            if not t.startswith(('sqlite_', 'alembic_')) and '_fts' not in t
        }
        revision = con.execute('SELECT version_num FROM alembic_version').fetchone()[0]
        integrity = con.execute('PRAGMA integrity_check').fetchone()[0]
        violations = len(con.execute('PRAGMA foreign_key_check').fetchall())
    return tables, revision, integrity, violations


def git(cwd: Path, *args: str) -> str:
    return subprocess.run(['git', *args], cwd=cwd, capture_output=True, text=True, check=True).stdout


def hashes(root: Path) -> dict[str, str]:
    """The SHA-256 of every file under ``root`` by relative path, but under ``SKIPPED_DIRS``. A symlink is its target."""
    found: dict[str, str] = {}
    for directory, dirs, names in os.walk(root):
        dirs[:] = [d for d in dirs if d not in SKIPPED_DIRS]
        for name in names:
            path = Path(directory) / name
            relative = str(path.relative_to(root))
            if path.is_symlink():
                found[relative] = f'symlink:{os.readlink(path)}'
            elif path.is_file():
                with path.open('rb') as f:
                    found[relative] = hashlib.file_digest(f, 'sha256').hexdigest()
    return found


@dataclass(frozen=True)
class Changed:
    """A cell of a row that the later reading has with another value."""

    table: str
    rowid: object
    column: str
    before: object
    after: object

    def __str__(self) -> str:
        return f'{self.table}#{self.rowid}.{self.column}: {str(self.before)[:80]!r} -> {str(self.after)[:80]!r}'


@dataclass
class Differences:
    """What ``after`` lacks or holds differently, in the new names. New rows, columns, refs and files are not
    differences: an upgrade and a running campaign add them."""

    missing_tables: list[str] = field(default_factory=list)
    missing_rows: list[str] = field(default_factory=list)
    missing_columns: list[str] = field(default_factory=list)
    changed: list[Changed] = field(default_factory=list)
    refs: list[str] = field(default_factory=list)
    """Refs that are gone or point elsewhere."""
    commits: list[str] = field(default_factory=list)
    files: list[str] = field(default_factory=list)
    """Files that are gone or have other bytes."""

    def lost(self) -> list[str]:
        """Everything but the changed cells, which the caller judges against what the upgrade may change."""
        return [*self.missing_tables, *self.missing_rows, *self.missing_columns, *self.refs, *self.commits, *self.files]


def differences(before: Inventory, after: Inventory, renames: Renames) -> Differences:
    out = rows_differences(before.tables, after.tables, renames)
    out.refs = [
        f'{ref} {sha} -> {after.refs.get(ref)}' for ref, sha in before.refs.items() if after.refs.get(ref) != sha
    ]
    out.commits = sorted(before.commits - after.commits)
    out.files = [
        f'{path}: {"gone" if path not in after.files else "other bytes"}'
        for path, digest in before.files.items()
        if after.files.get(path) != digest
    ]
    return out


def rows_differences(before: dict[str, list[Row]], after: dict[str, list[Row]], renames: Renames) -> Differences:
    """The tables, rows, columns and cells of ``before`` that ``after`` lacks or holds differently."""
    out = Differences()
    for old, rows in before.items():
        table = renames.tables.get(old, old)
        if table not in after:
            out.missing_tables.append(f'{old} (as {table}): {len(rows)} rows')
            continue
        now = {r['__rowid']: r for r in after[table]}
        columns = set(after[table][0]) if after[table] else set()
        for row in rows:
            new = renames.row(table, row)
            if (later := now.get(new['__rowid'])) is None:
                out.missing_rows.append(f'{table}#{new["__rowid"]}: {json.dumps(new, default=str)[:200]}')
                continue
            for column, value in new.items():
                if column not in columns:
                    out.missing_columns.append(f'{table}.{column}')
                elif not same(value, later[column]):
                    out.changed.append(Changed(table, new['__rowid'], column, value, later[column]))
    out.missing_columns = sorted(set(out.missing_columns))
    return out
