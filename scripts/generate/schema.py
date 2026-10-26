"""Generate ``docs/reference/schema.mdx``: the tables that ``piloop sql`` reads and writes."""

from piloop.db.schema import schemas
from piloop.db.sql import schema_ddl
from scripts.generate import ROOT, header

PATH = ROOT / 'docs' / 'reference' / 'schema.mdx'
REPO = 'https://github.com/hiloopai/piloop/blob/main'


def generate() -> str:
    """Return the schema reference: a header, then the DDL of core's tables and indexes, and of each plugin's."""
    sections = '\n\n'.join(f'## {name}\n\n```sql\n{schema_ddl(name)}\n```' for name in schemas())
    return f"""---
title: Database schema
description: The tables that piloop sql reads and writes.
---

{header('the tables of core and of each plugin (`piloop.db.schema`)')}
Every client reads and writes one SQLite database, `$PILOOP_HOME/piloop.db`. Run one statement with `piloop sql`.
`piloop schema` prints the same tables with the conventions that agents follow. The agents' procedures are in
[sql_procedures.md]({REPO}/piloop/agents/prompts/sql_procedures.md).

The indexes are part of the contract: they show which filters are cheap. The [data model](/architecture/data-model)
shows the same tables as a diagram. Each section is core's tables, or a plugin's that adds them
([tables and migrations](/architecture/extension-points#tables-and-migrations)).

{sections}
"""
