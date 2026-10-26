"""Generate ``docs/snippets/generated/data-model.mdx``: the entity-relationship diagram of the tables."""

from sqlalchemy import Column, Enum
from sqlalchemy.dialects import sqlite

from piloop.db.schema import tables as owned
from piloop.db.triggers.fts import FTS_COLUMNS
from scripts.generate import SNIPPETS, header

PATH = SNIPPETS / 'data-model.mdx'


def _column_type(column: Column[object]) -> str:
    if isinstance(column.type, Enum) and column.type.enum_class is not None:
        return column.type.enum_class.__name__
    return column.type.compile(dialect=sqlite.dialect()).split('(')[0].lower()


def generate() -> str:
    """Return a Mermaid entity-relationship diagram of every table, with its columns, and one line per foreign key."""
    lines = ['```mermaid', 'erDiagram']
    tables = owned()
    for table in tables:
        lines.append(f'    {table.name} {{')
        for column in table.columns:
            keys = [
                k
                for k, on in (
                    ('PK', column.primary_key),
                    ('FK', bool(column.foreign_keys)),
                    ('UK', bool(column.unique)),
                )
                if on
            ]
            lines.append(f'        {_column_type(column)} {column.name} {",".join(keys)}'.rstrip())
        lines.append('    }')
    for table in tables:
        for column in table.columns:
            for fk in sorted(column.foreign_keys, key=lambda fk: fk.target_fullname):
                parent = '|o' if column.nullable else '||'
                lines.append(f'    {fk.column.table.name} {parent}--o{{ {table.name} : "{column.name}"')
    indexes = ', '.join(f'`{t}_fts` ({", ".join(f"`{c}`" for c in cols)})' for t, cols in FTS_COLUMNS.items())
    diagram = '\n'.join([*lines, '```', '', f'The full-text indexes are not in the diagram: {indexes}.', ''])
    return header('the models in `piloop/db/models.py`') + '\n' + diagram
