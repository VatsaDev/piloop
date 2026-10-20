"""The commands on the database: ``sql``, ``schema`` and ``migrate``."""

import json
import sqlite3
from contextlib import closing
from typing import Annotated

import cyclopts
from rich.table import Table
from rich.text import Text
from sqlalchemy import create_engine

from piloop.agents.roles import PROCEDURES, PROMPTS
from piloop.cli import common, doctor
from piloop.daemon.serve import OlderDaemon, refuse_older_daemon
from piloop.db import sql
from piloop.db.session import Database, behind, head, revision
from piloop.version import installed


def sql_cmd(statement: Annotated[str, cyclopts.Parameter(help='One SQL statement.')], /) -> None:
    """Run one SQL statement on the piloop database.

    These are denied: schema changes, ATTACH, transactions, and writes to `alembic_version` and `session_actor`. Only
    the PRAGMAs that describe the schema are allowed. A query prints at most 500 rows.
    """
    database = common.settings.home / 'piloop.db'
    if (newer := doctor.schema_ahead(database)) is not None:
        raise SystemExit(str(newer))
    plain = create_engine(f'sqlite:///{database}')
    try:
        refuse_older_daemon(common.settings, plain)
        at = behind(plain)
    except OlderDaemon as e:
        raise SystemExit(str(e)) from e
    finally:
        plain.dispose()
    if at is not None:  # it does not migrate: its statements and the actor triggers expect this piloop's schema
        raise SystemExit(
            f'the database is at schema revision {at}, and this {installed()} reads revision {head()}:'
            ' run `piloop migrate` first'
        )
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
    """Print the guide to the database: what `piloop sql` accepts, the conventions, and every table."""
    text = f'{(PROMPTS / PROCEDURES).read_text().strip()}\n\n## Schema\n\n```sql\n{sql.schema_ddl()}\n```'
    common.console.print(text, markup=False, highlight=False, soft_wrap=True)


def _cell(value: object) -> str:
    return '' if value is None else json.dumps(value) if isinstance(value, dict | list) else str(value)


def migrate_cmd() -> None:
    """Migrate the database to the schema of this piloop, and print what changed.

    The daemon and the commands that read or change campaigns also migrate, without a message, when they start.
    `piloop sql` and `piloop doctor` do not migrate.
    """
    db = Database(common.settings.database_url, actor=common.settings.actor)
    try:
        old = revision(db.engine)
        common.migrate(db)
        new = revision(db.engine)
    finally:
        db.close()
    common.console.print(f'schema: {new}, current' if old == new else f'schema: {old or "none"} → {new}')
