"""piloop.db.triggers: one Trigger type with one rendering, the generated SQL files, and what migrate() installs."""

import re
import sqlite3
from contextlib import closing
from dataclasses import replace
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from inline_snapshot import snapshot
from sqlalchemy import create_engine

from piloop.db import sql, triggers
from piloop.db.models import Base, TargetKind
from piloop.db.session import migrate
from piloop.db.triggers import PERSISTENT, TEMP, Trigger, changelog, provenance


def trigger_names(con: sqlite3.Connection, schema: str = 'main') -> list[str]:
    master = 'sqlite_temp_master' if schema == 'temp' else 'sqlite_master'
    return [n for (n,) in con.execute(f"SELECT name FROM {master} WHERE type = 'trigger' ORDER BY name")]


def test_the_generated_file_is_the_package_rendered() -> None:
    """``make docs`` writes the TEMP triggers that every connection installs; the tuples in the package are the source."""
    assert triggers.temp_sql() == sql.ACTOR_TRIGGERS, 'stale piloop/db/actor_triggers.sql: run `make docs`'
    assert all(t.temp and t.sql() in sql.ACTOR_TRIGGERS for t in TEMP)
    assert all(not t.temp and 'CREATE TEMP' not in t.sql() for t in PERSISTENT)
    assert 'CREATE TRIGGER' not in sql.ACTOR_TRIGGERS


def test_every_trigger_is_one_statement_named_table_what_and_references_real_columns(tmp_path: Path) -> None:
    """sqlite3 executes one statement at a time, so each render must be exactly one; the names are the convention."""
    path = tmp_path / 'piloop.db'
    engine = create_engine(f'sqlite:///{path}')
    migrate(engine, backups=0)
    engine.dispose()
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute(provenance.SESSION_ACTOR_TABLE)
        for t in TEMP:
            assert sqlite3.complete_statement(t.sql())
            con.execute(t.sql())
        assert trigger_names(con, 'temp') == sorted(t.name for t in TEMP)
        assert trigger_names(con) == sorted(t.name for t in PERSISTENT), 'what migrate() installs is PERSISTENT'
    assert len({t.name for t in (*PERSISTENT, *TEMP)}) == len(PERSISTENT) + len(TEMP), 'names are unique'
    for t in (*PERSISTENT, *TEMP):
        assert t.name.startswith(f'{t.table}_'), t.name
        columns = Base.metadata.tables[t.table].columns
        referenced = set(re.findall(r'\b(?:NEW|OLD)\.(\w+)', f'{t.when or ""} {t.body}'))
        assert referenced <= set(columns.keys()) and set(t.of) <= set(columns.keys()), t.name
    assert [t.name for t in PERSISTENT] == snapshot(
        [
            'campaigns_updated_at',
            'ideas_updated_at',
            'runs_updated_at',
            'candidates_updated_at',
            'comments_updated_at',
            'workers_updated_at',
            'documents_updated_at',
            'comments_escalation_event',
            'idea_links_campaign',
            'workers_kind',
            'campaigns_parent_insert',
            'campaigns_parent_update',
            'journal_no_replace',
            'lessons_no_replace',
            'ideas_fts_insert',
            'ideas_fts_delete',
            'ideas_fts_update',
            'comments_fts_insert',
            'comments_fts_delete',
            'comments_fts_update',
            'documents_fts_insert',
            'documents_fts_delete',
            'documents_fts_update',
        ]
    )
    assert [t.name for t in TEMP] == snapshot(
        [
            'campaigns_updated_by',
            'ideas_updated_by',
            'runs_updated_by',
            'candidates_updated_by',
            'comments_updated_by',
            'workers_updated_by',
            'documents_updated_by',
            'ideas_status_event',
            'ideas_priority_event',
            'ideas_track_event',
            'ideas_base_candidate_id_event',
            'idea_links_linked_event',
            'idea_links_unlinked_event',
            'candidates_verdict_event',
            'runs_brief_event',
            'campaigns_status_event',
            'campaigns_type_event',
            'campaigns_parent_id_event',
            'campaigns_paused_event',
            'comments_acked_at_event',
            'ideas_deleted_event',
            'comments_deleted_event',
            'candidates_deleted_event',
            'comments_inserted_event',
            'events_checked',
            'events_next_id',
        ]
    )


def test_the_change_log_is_the_event_kinds_the_feed_documents() -> None:
    events = [t for t in (*TEMP, *PERSISTENT) if t.body.startswith('INSERT INTO events')]
    kinds = [m[1] for t in events if (m := re.search(r'VALUES \((.+?), ', t.body))]
    assert kinds == snapshot(
        [
            "'idea.status'",
            "'idea.priority'",
            "'idea.track'",
            "'idea.base_candidate_id'",
            "'idea.linked'",
            "'idea.unlinked'",
            "'candidate.verdict'",
            "'run.brief'",
            "'campaign.status'",
            "'campaign.type'",
            "'campaign.parent_id'",
            "'campaign.paused'",
            "'comment.acked_at'",
            "'idea.deleted'",
            "'comment.deleted'",
            "'candidate.deleted'",
            "'comment.' || NEW.kind",
            "'comment.escalation'",
        ]
    )
    targets = {m[1] for t in events if (m := re.search(r"VALUES \(.+?, .+?, '(\w+)', ", t.body))}
    assert len(targets) > 1 and targets <= {k.value for k in TargetKind}, (
        'the event target_kind (the table singular, or the idea of a link) must pass the CHECK constraint'
    )
    assert changelog.RUN_BRIEF in changelog.CHANGES and f"= '{provenance.DAEMON_ACTOR}'" in (
        changelog.RUN_BRIEF.when or ''
    )
    assert changelog.singular('ideas') == 'idea'


def test_recreate_is_a_filter_over_persistent_that_honours_requires(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """After a batch rebuild dropped a table's triggers, ``recreate`` puts back exactly its PERSISTENT ones.

    A trigger that ``requires`` a table (an FTS sync trigger) is skipped while that table does not exist.
    """
    path = tmp_path / 'piloop.db'
    engine = create_engine(f'sqlite:///{path}')
    migrate(engine, backups=0)
    with engine.begin() as conn:
        op = Operations(MigrationContext.configure(conn))
        for t in PERSISTENT:
            if t.table == 'comments':  # updated_at, the escalation event and the three FTS sync triggers
                conn.exec_driver_sql(f'DROP TRIGGER {t.name}')
        triggers.recreate(op, 'comments')
        names = [n for (n,) in conn.exec_driver_sql("SELECT name FROM sqlite_master WHERE type = 'trigger'")]
        assert sorted(names) == sorted(t.name for t in PERSISTENT)
        probe = Trigger('events_probe', 'events', 'AFTER INSERT', body='SELECT 1', requires='events_fts')
        monkeypatch.setattr(triggers, 'PERSISTENT', (probe, replace(probe, name='events_probe2', requires='events')))
        triggers.recreate(op, 'events')
        names = [n for (n,) in conn.exec_driver_sql("SELECT name FROM sqlite_master WHERE name LIKE 'events_probe%'")]
        assert names == ['events_probe2'], 'the index does not exist yet: only the trigger without a requirement'
    engine.dispose()


def test_a_trigger_renders_uniformly_and_of_needs_an_update() -> None:
    plain = Trigger('t_x', 't', 'AFTER DELETE', body='DELETE FROM u WHERE id = OLD.id;\nDELETE FROM v')
    assert plain.sql() == snapshot("""\
CREATE TRIGGER t_x AFTER DELETE ON t
BEGIN
  DELETE FROM u WHERE id = OLD.id;
  DELETE FROM v;
END;""")
    narrowed = Trigger(
        't_a_event', 't', 'AFTER UPDATE', of=('a', 'b'), when='NEW.a IS NOT OLD.a', body='SELECT 1', temp=True
    )
    assert narrowed.sql() == snapshot("""\
CREATE TEMP TRIGGER t_a_event AFTER UPDATE OF a, b ON t
WHEN NEW.a IS NOT OLD.a
BEGIN
  SELECT 1;
END;""")
    with pytest.raises(ValueError, match='t_bad: OF a needs AFTER UPDATE, not AFTER INSERT'):
        Trigger('t_bad', 't', 'AFTER INSERT', of=('a',), body='SELECT 1')
