from importlib.metadata import EntryPoint
from pathlib import Path

import pytest
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from piloop.db import schema as schema_module
from piloop.db.models import Base, Idea
from piloop.db.schema import CORE, SCHEMAS, Schema, schemas, tables
from piloop.db.session import Database
from piloop.db.triggers import Trigger
from piloop.plugins.host import PluginError
from tests.daemon.test_roles import installed

NO_REVISIONS = Path(__file__).parent / 'no_revisions'
"""A test schema's revisions, which no test runs."""


class Note(Base):
    """A plugin's table that references a core table's id."""

    __tablename__ = 'test_schema_notes'

    id: Mapped[int] = mapped_column(primary_key=True)
    campaign_id: Mapped[int] = mapped_column(ForeignKey('campaigns.id', ondelete='CASCADE'))


class OnIdea(Base):
    """A package's table that references another package's table."""

    __tablename__ = 'test_schema_on_ideas'

    id: Mapped[int] = mapped_column(primary_key=True)
    idea_id: Mapped[int] = mapped_column(ForeignKey('ideas.id'))


class OnName(Base):
    """A plugin's table that references a core column other than the id."""

    __tablename__ = 'test_schema_on_names'

    id: Mapped[int] = mapped_column(primary_key=True)
    campaign: Mapped[str] = mapped_column(ForeignKey('campaigns.name'))


class Loose(Base):
    """A class of ``Base`` that no schema has."""

    __tablename__ = 'test_schema_loose'

    id: Mapped[int] = mapped_column(primary_key=True)


class OnLoose(Base):
    """A package's table that references a table that no schema has."""

    __tablename__ = 'test_schema_on_loose'

    id: Mapped[int] = mapped_column(primary_key=True)
    loose_id: Mapped[int] = mapped_column(ForeignKey('test_schema_loose.id'))


NOTES = Schema(tables=(Note,), versions=NO_REVISIONS)
ON_LOOSE = Schema(tables=(OnLoose,), versions=NO_REVISIONS)
STRAY = Schema(
    tables=(Note,),
    versions=NO_REVISIONS,
    triggers=(Trigger('campaigns_noted', 'campaigns', 'AFTER INSERT', body='SELECT 1'),),
)
ON_IDEAS = Schema(tables=(OnIdea,), versions=NO_REVISIONS)
ON_NAMES = Schema(tables=(OnName,), versions=NO_REVISIONS)
TWICE = Schema(tables=(Idea,), versions=NO_REVISIONS)


def test_every_table_of_a_migrated_database_is_in_one_schema(db: Database) -> None:
    with db.engine.connect() as con:
        made = {
            row[0]
            for row in con.exec_driver_sql("SELECT name FROM sqlite_master WHERE type = 'table'")
            if not row[0].startswith(('sqlite_', 'alembic_')) and '_fts' not in row[0]
        }
    assert {t.name for t in tables()} == made
    assert list(schemas()) == [CORE, 'archive/local', 'memory/local', 'optimization']


def schema_of(monkeypatch: pytest.MonkeyPatch, name: str, attribute: str) -> None:
    installed(monkeypatch, [EntryPoint(name, f'{__name__}:{attribute}', SCHEMAS.group)])


def test_a_plugin_s_table_references_a_core_table_s_id(monkeypatch: pytest.MonkeyPatch) -> None:
    schema_of(monkeypatch, 'notes', 'NOTES')
    assert schemas()['notes'] == NOTES
    assert 'test_schema_notes' in {t.name for t in tables()}


@pytest.mark.parametrize(
    ('name', 'attribute', 'refused'),
    [
        ('core', 'NOTES', "'core' is piloop's own schema"),
        ('Notes', 'NOTES', "'Notes' is no schema name"),
        ('a__b', 'NOTES', "'a__b' is no schema name"),
        ('ideas', 'TWICE', "the schema 'ideas' has the table 'ideas', which 'optimization' has"),
        ('on_ideas', 'ON_IDEAS', "test_schema_on_ideas.idea_id references ideas.id of 'optimization'"),
        ('on_names', 'ON_NAMES', "test_schema_on_names.campaign references campaigns.name of 'core'"),
        ('notes', 'Note', 'piloop.schemas is not a schema'),
        ('on_loose', 'ON_LOOSE', 'test_schema_on_loose.loose_id references test_schema_loose.id, which no schema has'),
        ('stray', 'STRAY', "the schema 'stray' has triggers on tables that are not its own: campaigns_noted"),
    ],
)
def test_a_schema_that_takes_a_table_or_crosses_schemas_is_refused(
    monkeypatch: pytest.MonkeyPatch, name: str, attribute: str, refused: str
) -> None:
    schema_of(monkeypatch, name, attribute)
    with pytest.raises(PluginError, match=refused):
        schemas()


def test_core_references_a_plugin_s_table_only_where_it_is_allowed() -> None:
    crossing = {
        (t.name, k.parent.name)
        for t in Base.metadata.tables.values()
        if t.name in {table.__tablename__ for table in schemas()[CORE].tables}
        for k in t.foreign_keys
        if k.column.table.name not in {table.__tablename__ for table in schemas()[CORE].tables}
    }
    assert crossing == {('agent_sessions', 'run_id')}


def test_core_s_tables_reference_only_core_s_but_where_it_is_allowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(schema_module, 'CORE_REFERENCES', frozenset())
    with pytest.raises(PluginError, match="agent_sessions.run_id references runs.id of 'optimization': core's tables"):
        schemas()
