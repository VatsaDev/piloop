"""Full-text search over ideas and comments: FTS5 external-content indexes and the sync triggers that update them.

``ideas_fts`` and ``comments_fts`` index every row, so "has anyone tried soft voting" is one ``MATCH`` query, however
long the campaign is. An external-content table (``content='ideas'``) stores only the index, never a second copy of
the text. So three triggers on each table must tell the index about every insert, delete and change of the indexed
columns.

The authorizer (``piloop.db.sql``) lets only these triggers write the indexes.
"""

from piloop.db.triggers.ddl import Trigger

FTS_COLUMNS: dict[str, tuple[str, ...]] = {'ideas': ('title', 'hypothesis', 'plan'), 'comments': ('body',)}
"""The indexed text columns of each searchable table. ``<table>_fts`` is the index. Its rowid is the row's ``id``."""
FTS_TOKENIZER = 'porter unicode61 remove_diacritics 2'
"""Hypotheses are English text. Porter stemming makes vote, votes and voting one term (and encoder and encoding), so a
search matches every form of a word. unicode61 with remove_diacritics 2 makes naïve match naive, and the same for any
other accented spelling. The cost is that a prefix query (``ensem*``) matches stems, not words."""


def fts_table(table: str) -> str:
    """The ``CREATE VIRTUAL TABLE`` statement for the index of ``table``, ending with ``;`` like ``Trigger.sql()``."""
    columns = ', '.join(FTS_COLUMNS[table])
    return (
        f"CREATE VIRTUAL TABLE {table}_fts USING fts5({columns}, content='{table}', content_rowid='id', "
        f"tokenize='{FTS_TOKENIZER}');"
    )


def fts_sync(table: str) -> tuple[Trigger, ...]:
    """The three persistent sync triggers for the index of ``table``.

    Each one ``requires`` the index, so ``recreate`` skips them at a point of the migration chain where the index does
    not exist yet. This is the standard pattern for external content. The index never reads the table on a write, so
    a change is an explicit ``'delete'`` of the old text, then an insert of the new text. The old text must be exactly
    what was indexed, so it comes from ``OLD.*``. ``OF id, <columns>`` limits the update trigger to the indexed
    columns and the rowid. Without it, a change of status or priority, and the ``updated_at`` and ``updated_by``
    updates that follow every UPDATE, would each index the row again three times.
    """
    fts = f'{table}_fts'
    columns = FTS_COLUMNS[table]
    names = ', '.join(columns)
    new = ', '.join(f'NEW.{c}' for c in columns)
    old = ', '.join(f'OLD.{c}' for c in columns)
    insert = f'INSERT INTO {fts} (rowid, {names}) VALUES (NEW.id, {new})'
    delete = f"INSERT INTO {fts} ({fts}, rowid, {names}) VALUES ('delete', OLD.id, {old})"
    return (
        Trigger(f'{fts}_insert', table, 'AFTER INSERT', body=insert, requires=fts),
        Trigger(f'{fts}_delete', table, 'AFTER DELETE', body=delete, requires=fts),
        Trigger(f'{fts}_update', table, 'AFTER UPDATE', of=('id', *columns), body=f'{delete};\n{insert}', requires=fts),
    )


SYNC: tuple[Trigger, ...] = tuple(t for table in FTS_COLUMNS for t in fts_sync(table))
"""Every sync trigger, in the order of ``PERSISTENT``."""
FTS_TABLES = frozenset(f'{table}_fts' for table in FTS_COLUMNS)
FTS_TRIGGER_NAMES = frozenset(t.name for t in SYNC)
"""The only triggers that may write an index. The authorizer lets their bodies through and denies all else."""


def is_fts_object(name: str) -> bool:
    """Whether ``name`` is an index, one of its shadow tables (for example ``ideas_fts_data``) or a sync trigger.

    None of them is a SQLAlchemy model, so autogenerate must not see them. Agents must not write any of them.
    """
    return name in FTS_TABLES or any(name.startswith(f'{fts}_') for fts in FTS_TABLES)
