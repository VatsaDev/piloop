"""Every code block in the docs, the README, AGENTS.md, CONTRIBUTING.md and the skills is checked against the code.

The docs include the generated snippets in ``docs/snippets/generated/``, and every snippet that a page imports exists.

Placeholders: ``<anything>`` (a letter after the ``<``) and ``…`` become ``1`` before a shell or SQL block is checked.
``1`` is a valid project name, id, number and text, so an example with placeholders parses like a real one.
"""

import json
import re
import shlex
import sqlite3
import subprocess
import tomllib
from collections.abc import Callable, Iterator
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

import pytest
from cyclopts import CycloptsError
from pydantic import BaseModel

from piloop.cli.database import sql_cmd
from piloop.cli.main import app
from piloop.config import ProjectConfig
from piloop.db import sql
from piloop.settings import StorageSettings
from piloop.store.config import remote_config
from tests.conftest import at_head

ROOT = Path(__file__).parent.parent
SOURCES = [
    *sorted((ROOT / 'docs').rglob('*.mdx')),
    ROOT / 'README.md',
    ROOT / 'AGENTS.md',
    ROOT / 'CONTRIBUTING.md',
    *sorted((ROOT / 'skills').rglob('SKILL.md')),
]
SNIPPET = re.compile(r"^import (?P<name>\w+) from '(?P<path>/snippets/[^']+)';$", re.MULTILINE)
FENCE = re.compile(r'^(?P<indent>\s*)(?P<fence>`{3,}|~{3,})(?P<info>.*)$')
PLACEHOLDER = re.compile(r'<[A-Za-z][^<>\n]*>|…')
ASSIGNMENT = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*=')
OPERATOR = re.compile(r'^[;&|()<>]+$')
BASE_CONFIG: dict[str, object] = {
    'name': 'demo',
    'evaluate': ['./evaluate.sh'],
    'metrics': [{'name': 'accuracy', 'direction': 'higher', 'role': 'primary'}],
    'model': {'pi': 'anthropic/claude-opus-5-5'},
}
"""The smallest valid ``piloop.toml``. A TOML fragment is merged into it and the result must validate."""


@dataclass(frozen=True)
class Block:
    path: Path
    line: int
    """The line of the opening fence, from 1."""
    language: str
    code: str

    def at(self, offset: int | None = None) -> str:
        """``path:line`` of the opening fence, or of line ``offset`` (from 0) of the code."""
        return f'{self.path.relative_to(ROOT)}:{self.line if offset is None else self.line + 1 + offset}'


def blocks(path: Path) -> Iterator[Block]:
    """The fenced code blocks of a Markdown or MDX file, with the indentation of the fence removed."""
    open_at, fence, indent, language = 0, '', '', ''
    body: list[str] = []
    for n, text in enumerate(path.read_text().splitlines(), 1):
        m = FENCE.match(text)
        if not fence and m:
            open_at, fence, indent, body = n, m['fence'], m['indent'], []
            language = m['info'].split()[0] if m['info'].strip() else ''
        elif fence and m and m['fence'].startswith(fence) and not m['info'].strip():
            yield Block(path, open_at, language, '\n'.join(body) + '\n')
            fence = ''
        elif fence:
            body.append(text.removeprefix(indent))
    assert not fence, f'{path.relative_to(ROOT)}:{open_at}: the code block is never closed'


ALL = [b for path in SOURCES for b in blocks(path)]


def substituted(code: str) -> str:
    return PLACEHOLDER.sub('1', code)


def merged(base: dict[str, object], over: dict[str, object]) -> dict[str, object]:
    """``over`` on top of ``base``. Tables merge key by key. Other values, arrays too, replace the value in ``base``."""
    out = dict(base)
    for key, value in over.items():
        old = out.get(key)
        out[key] = merged(old, value) if isinstance(old, dict) and isinstance(value, dict) else value
    return out


GLOBAL_TABLES: dict[str, type[BaseModel]] = {'storage': StorageSettings}
"""The tables of ``$PILOOP_HOME/config.toml``, the global settings file. Every other TOML block is ``piloop.toml``."""


def check_toml(block: Block, _db: Path) -> None:
    """A whole ``piloop.toml`` validates as it is. A fragment validates when it is merged into ``BASE_CONFIG``.

    A block of ``config.toml`` tables only validates against those tables.
    """
    data = tomllib.loads(block.code)
    if data.keys() <= GLOBAL_TABLES.keys() and data:
        for table, values in data.items():
            GLOBAL_TABLES[table].model_validate(values)
        if (storage := data.get('storage')) is not None:
            settings = StorageSettings.model_validate(storage)
            if settings.remote is not None:
                remote_config(settings.remote)  # the backend checks its own keys, as when the store opens
        return
    ProjectConfig.model_validate(data if {'name', 'evaluate'} <= data.keys() else merged(BASE_CONFIG, data))


def commands(code: str) -> Iterator[tuple[int, list[str]]]:
    """The simple commands of a shell block, as (line offset, argv without leading ``VAR=value`` assignments)."""
    lines = code.split('\n')
    n = 0
    while n < len(lines):
        start, text = n, lines[n]
        while text.endswith('\\') and n + 1 < len(lines):
            n += 1
            text = text[:-1] + lines[n]
        n += 1
        lexer = shlex.shlex(text, posix=True, punctuation_chars=True)
        lexer.whitespace_split = True
        argv: list[str] = []
        for token in [*lexer, ';']:
            if OPERATOR.match(token):
                while argv and ASSIGNMENT.match(argv[0]):
                    argv.pop(0)
                if argv:
                    yield start, argv
                argv = []
            else:
                argv.append(token)


def check_shell(block: Block, db: Path) -> None:
    """The block is valid shell, and every ``piloop`` command parses against the CLI (nothing runs)."""
    code = substituted(block.code)
    shell = subprocess.run([block.language, '-n'], input=code, capture_output=True, text=True, check=False)
    assert shell.returncode == 0, f'{block.at()}: {block.language} -n: {shell.stderr.strip()}'
    for offset, argv in commands(code):
        if argv[0] != 'piloop':
            continue
        try:
            command, bound, _ = app.parse_args(argv[1:], exit_on_error=False, print_error=False)
        except CycloptsError as e:
            raise AssertionError(f'{block.at(offset)}: {shlex.join(argv)}: {e}') from e
        if command is sql_cmd:
            explain(block.at(offset), bound.arguments['statement'], db)


def explain(where: str, statement: str, db: Path) -> None:
    """Compile ``statement`` with EXPLAIN on a migrated database, through the connection that ``piloop sql`` uses."""
    with closing(sql.connect(db, actor='human')) as con:
        try:
            con.execute(f'EXPLAIN {statement}')
        except sqlite3.Error as e:
            raise AssertionError(f'{where}: {e}: {statement.strip().splitlines()[0]}') from e


def statements(code: str) -> Iterator[tuple[int, str]]:
    """The statements of a SQL block, as (line offset, statement).

    Lines that are only a ``--`` comment are dropped. The ``;`` after the last statement is optional.
    """
    start, buffer = 0, ''
    for n, line in enumerate(code.splitlines(keepends=True)):
        if line.lstrip().startswith('--'):
            continue
        if not buffer.strip():
            start = n
        buffer += line
        if sqlite3.complete_statement(buffer):
            yield start, buffer
            buffer = ''
    if buffer.strip():
        yield start, buffer


def check_sql(block: Block, db: Path) -> None:
    """Each statement compiles on a migrated database. A block of CREATE statements runs on an empty database."""
    found = list(statements(substituted(block.code)))
    if all(s.lstrip().upper().startswith('CREATE') for _, s in found):
        with closing(sqlite3.connect(':memory:')) as con:
            con.executescript(block.code)
        return
    for offset, statement in found:
        explain(block.at(offset), statement, db)


def check_python(block: Block, _db: Path) -> None:
    compile(block.code, block.at(), 'exec')


def check_json(block: Block, _db: Path) -> None:
    json.loads(block.code)


def skip(_block: Block, _db: Path) -> None:
    """Plain text, and Mermaid diagrams: rendering a diagram needs a browser."""


CHECKS: dict[str, Callable[[Block, Path], None]] = {
    'toml': check_toml,
    'bash': check_shell,
    'sh': check_shell,
    'sql': check_sql,
    'python': check_python,
    'json': check_json,
    'mermaid': skip,
    'text': skip,
}


@pytest.fixture(scope='module')
def db(tmp_path_factory: pytest.TempPathFactory, schema: Path) -> Path:
    return at_head(schema, tmp_path_factory.mktemp('docs') / 'piloop.db')


@pytest.mark.parametrize('block', [pytest.param(b, id=b.at()) for b in ALL])
def test_code_block(block: Block, db: Path) -> None:
    check = CHECKS.get(block.language)
    assert check is not None, f'no check for the language {block.language!r}: add one to CHECKS'
    try:
        check(block, db)
    except Exception as e:
        e.add_note(f'the code block at {block.at()} starts with: {block.code.splitlines()[0]}')
        raise


def test_snippets_are_imported_and_exist() -> None:
    """Every snippet that a page imports exists and is used, and every generated snippet has a page."""
    imported: set[Path] = set()
    for page in (ROOT / 'docs').rglob('*.mdx'):
        for m in SNIPPET.finditer(text := page.read_text()):
            path = ROOT / 'docs' / m['path'].lstrip('/')
            assert path.is_file(), f'{page.relative_to(ROOT)} imports {m["path"]}, which does not exist'
            assert f'<{m["name"]} />' in text, f'{page.relative_to(ROOT)} imports {m["name"]} and never uses it'
            imported.add(path)
    generated = set((ROOT / 'docs' / 'snippets' / 'generated').glob('*.mdx'))
    assert generated, 'no generated snippets: run `make docs`'
    assert generated <= imported, f'no page imports {sorted(p.name for p in generated - imported)}'
