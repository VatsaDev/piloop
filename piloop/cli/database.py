"""The commands on the database: ``sql``, ``schema`` and ``migrate``."""

import json
import sqlite3
import sys
from contextlib import closing
from pathlib import Path
from typing import Annotated

import cyclopts
from rich.table import Table
from rich.text import Text

from piloop.agents.roles import PROCEDURES, PROMPTS
from piloop.campaign.archive import ManifestNotMigrated, migrate_manifests
from piloop.cli import common
from piloop.db import sql
from piloop.db.session import Database, backups, revision
from piloop.plugins.vcs.contract import VCS, prompted

ORCHESTRATOR_PROCEDURES = 'sql_orchestrator.md'
"""The procedures in ``PROMPTS`` that ``schema`` adds for the orchestrator. No brief has them."""


def sql_cmd(
    statement: Annotated[
        str,
        # allow_leading_hyphen: cyclopts reads a bare `-` as an unknown option otherwise
        cyclopts.Parameter(help='One SQL statement, or - to read it from stdin.', allow_leading_hyphen=True),
    ],
    /,
) -> None:
    """Run one SQL statement on the piloop database.

    These are denied: schema changes, ATTACH, transactions, and writes to `alembic_version` and `session_actor`. Only
    the PRAGMAs that describe the schema are allowed. A query prints at most 500 rows. With `-`, the statement is read
    from stdin: piped in from a quoted heredoc (`<<'EOF'`), an agent's text reaches the database as written, where a
    double-quoted argument would let the shell run what is in backticks or `$(…)` first.
    """
    # TODO: remove `-` and the briefs' heredocs once agents write through typed write verbs and `piloop mcp`.
    if statement == '-':
        if sys.stdin.isatty():
            raise SystemExit("piloop sql - reads the statement from stdin: pipe it in with a quoted heredoc (<<'EOF')")
        statement = sys.stdin.read()
        if not statement.strip():
            raise SystemExit('piloop sql - read no statement from stdin')
    common.refuse_other_schema()
    database = common.settings.home / 'piloop.db'
    with closing(sql.connect(database, actor=common.settings.actor)) as con:
        try:
            result = sql.run(con, statement)
        except sqlite3.Error as e:
            holder = sql.lock_holder(e, database)
            error = sql.with_renamed(str(e))
            raise SystemExit(f'sql error: {error}' if holder is None else f'sql error: {error} ({holder})') from e
    if result.columns:
        table = Table(*result.columns, show_lines=False)
        for row in result.rows:
            # Text, not str: a cell is data, so '[voting]' from snippet() or a '[wip]' title is not rich markup
            table.add_row(*(Text(_cell(v)) for v in row))
        common.console.print(table)
        if result.truncated:
            common.console.print(f'[dim]truncated to {sql.ROW_LIMIT} rows[/]')
    else:
        common.console.print(f'{result.changes} row(s) changed, last insert id {result.last_insert_rowid}')


def schema() -> None:
    """Print the guide to the database: what `piloop sql` accepts, every procedure, and every table.

    The procedures say what the default vcs provider writes in a brief: no campaign chooses one here.
    """
    vcs = VCS.selected({})
    procedures = '\n\n'.join(
        prompted((PROMPTS / name).read_text().strip(), vcs) for name in (PROCEDURES, ORCHESTRATOR_PROCEDURES)
    )
    text = f'{procedures}\n\n## Schema\n\n```sql\n{sql.schema_ddl()}\n```'
    common.console.print(text, markup=False, highlight=False, soft_wrap=True)


def _cell(value: object) -> str:
    return '' if value is None else json.dumps(value) if isinstance(value, dict | list) else str(value)


def migrate_cmd() -> None:
    """Migrate the database to the schema of this piloop, and print what changed and where its backup is.

    Before it migrates, it copies the database next to it (`migration_backups` in config.toml). Then it stores the
    archived manifests of piloop 0.2 again in the new format. It exits non-zero when it cannot read one, and the next
    run tries again. Only this command, `piloop serve` when it starts and `piloop upgrade` migrate. Every other command
    refuses a database at another schema revision, and says what to run. It refuses to migrate under the running
    daemon of an older piloop, and refuses an agent that the daemon launched.
    """
    common.refuse_agent('migrate')
    db = Database(common.settings.database_url, actor=common.settings.actor)
    try:
        old = revision(common.settings.home / 'piloop.db')
        backup = common.migrate(db)
        _print_schema(old, revision(common.settings.home / 'piloop.db'), backup)
        try:
            manifests = migrate_manifests(db, common.cas())
        except ManifestNotMigrated as e:
            raise SystemExit(str(e)) from e
    finally:
        db.close()
    if manifests:
        common.console.print(f'manifests: {manifests} stored again in the new format')


def _print_schema(old: str | None, new: str | None, backup: Path | None) -> None:
    if old == new:
        newest = backups(common.settings.home / 'piloop.db')
        common.console.print(
            f'schema: {new}, current' + (f'; the newest backup: {newest[-1]}' if newest else ''),
            markup=False,
            highlight=False,
            soft_wrap=True,
        )
    elif old is None:
        common.console.print(f'schema: none → {new}')
    else:
        done = f'backup of {old}: {backup}' if backup is not None else 'no backup (migration_backups = 0)'
        common.console.print(f'schema: {old} → {new}; {done}', markup=False, highlight=False, soft_wrap=True)
