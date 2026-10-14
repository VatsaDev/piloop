"""Generate ``docs/reference/schema.mdx``: the tables that ``piloop sql`` reads and writes."""

from piloop.db.sql import schema_ddl
from scripts.generate import ROOT, header

PATH = ROOT / 'docs' / 'reference' / 'schema.mdx'
REPO = 'https://github.com/hiloopai/piloop/blob/main'


def generate() -> str:
    """Return the schema reference: a header, then the DDL of every table and index."""
    return f"""---
title: Database schema
description: The tables that piloop sql reads and writes.
---

{header('the models in `piloop/db/models.py`')}
Every client reads and writes one SQLite database, `$PILOOP_HOME/piloop.db`. Run one statement with `piloop sql`.
`piloop schema` prints the same tables with the conventions that agents follow. The agents' procedures are in
[sql_procedures.md]({REPO}/piloop/agents/prompts/sql_procedures.md).

The indexes are part of the contract: they show which filters are cheap. The [data model](/architecture/data-model)
shows the same tables as a diagram.

```sql
{schema_ddl()}
```
"""
