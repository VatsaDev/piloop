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
from sqlalchemy import create_engine, text

from piloop.actor import System
from piloop.daemon.main import ACTOR
from piloop.db import sql, triggers
from piloop.db.events import SubjectSettled, in_order
from piloop.db.models import Campaign, Idea, Run, RunStatus
from piloop.db.schema import schemas, tables
from piloop.db.session import Database, migrate
from piloop.db.triggers import PERSISTENT, TEMP, Trigger, changelog, provenance
from piloop.ids import TargetKind
from tests.conftest import disk, end_of


def trigger_names(con: sqlite3.Connection, schema: str = 'main') -> list[str]:
    master = 'sqlite_temp_master' if schema == 'temp' else 'sqlite_master'
    return [n for (n,) in con.execute(f"SELECT name FROM {master} WHERE type = 'trigger' ORDER BY name")]


def test_piloop_s_schemas_share_its_triggers_by_table_and_every_connection_creates_the_temp_ones() -> None:
    declared = [t for schema in schemas().values() for t in schema.triggers]
    assert sorted(t.name for t in declared) == sorted(t.name for t in (*PERSISTENT, *TEMP)), 'each on one schema'
    assert all(t.temp and t.sql() in sql.declared().actor_sql for t in TEMP)
    assert all(not t.temp and 'CREATE TEMP' not in t.sql() for t in PERSISTENT)
    assert 'CREATE TRIGGER' not in sql.declared().actor_sql


def test_every_trigger_is_one_statement_named_table_what_and_references_real_columns(tmp_path: Path) -> None:
    """sqlite3 executes one statement at a time, so each render must be exactly one; the names are the convention."""
    path = tmp_path / 'piloop.db'
    engine = create_engine(f'sqlite:///{path}')
    migrate(engine, cas=disk(tmp_path), backups=0, daemon=None)
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
        columns = {table.name: table for table in tables()}[t.table].columns
        referenced = set(re.findall(r'\b(?:NEW|OLD)\.(\w+)', f'{t.when or ""} {t.body}'))
        assert referenced <= set(columns.keys()) and set(t.of) <= set(columns.keys()), t.name
    assert [t.name for t in PERSISTENT] == snapshot(
        [
            'campaigns_updated_at',
            'ideas_updated_at',
            'runs_updated_at',
            'candidates_updated_at',
            'comments_updated_at',
            'agent_sessions_updated_at',
            'documents_updated_at',
            'comments_escalation_event',
            'idea_links_campaign',
            'agent_sessions_kind',
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
            'agent_sessions_updated_by',
            'documents_updated_by',
            'ideas_status_event',
            'ideas_priority_event',
            'ideas_track_event',
            'ideas_base_candidate_id_event',
            'idea_links_linked_event',
            'idea_links_unlinked_event',
            'candidates_verdict_event',
            'candidates_origin_event',
            'runs_brief_cid_event',
            'campaigns_status_event',
            'campaigns_type_event',
            'campaigns_parent_id_event',
            'campaigns_paused_event',
            'comments_acked_at_event',
            'ideas_deleted_event',
            'comments_deleted_event',
            'candidates_deleted_event',
            'comments_inserted_event',
            'agent_sessions_requested_event',
            'ideas_created_event',
            'campaigns_config_event',
            'evaluations_cancelled_event',
            'limits_set_event',
            'limits_updated_event',
            'limits_unset_event',
            'ideas_settled_event',
            'runs_settled_event',
            'comments_no_late_finding',
            'events_checked',
            'events_next_id',
        ]
    )


def test_the_change_log_is_the_event_kinds_the_feed_documents() -> None:
    events = [t for t in (*TEMP, *PERSISTENT) if t.body.startswith('INSERT INTO events')]
    kinds = [m[1] for t in events if (m := re.search(r"json_object\('kind', (.+?), ", t.body))]
    assert kinds == snapshot(
        [
            "'idea.status'",
            "'idea.priority'",
            "'idea.track'",
            "'idea.base_candidate_id'",
            "'idea.linked'",
            "'idea.unlinked'",
            "'candidate.verdict'",
            "'candidate.origin'",
            "'run.brief_cid'",
            "'campaign.status'",
            "'campaign.type'",
            "'campaign.parent_id'",
            "'campaign.paused'",
            "'comment.acked_at'",
            "'idea.deleted'",
            "'comment.deleted'",
            "'candidate.deleted'",
            "'comment.' || NEW.kind",
            "'agent_session.requested'",
            "'evaluation.cancelled'",
            "'limit.set'",
            "'limit.set'",
            "'limit.unset'",
            "'subject.settled'",
            "'subject.settled'",
            "'comment.' || NEW.kind",
        ]
    )
    targets = {m[1] for t in events if (m := re.search(r"VALUES \(.+?, '(\w+)', ", t.body))}
    assert len(targets) > 1 and targets <= {k.value for k in TargetKind}, (
        'the event target_kind (the table singular, or the idea of a link) must pass the CHECK constraint'
    )
    assert changelog.RUN_BRIEF in changelog.CHANGES and f"= '{provenance.DAEMON_ACTOR}'" in (
        changelog.RUN_BRIEF.when or ''
    )


def test_recreate_is_a_filter_over_persistent_that_honours_requires(tmp_path: Path) -> None:
    """After a batch rebuild dropped a table's triggers, ``recreate`` puts back exactly its PERSISTENT ones.

    A trigger that ``requires`` a table (an FTS sync trigger) is skipped while that table does not exist.
    """
    path = tmp_path / 'piloop.db'
    engine = create_engine(f'sqlite:///{path}')
    migrate(engine, cas=disk(tmp_path), backups=0, daemon=None)
    with engine.begin() as conn:
        op = Operations(MigrationContext.configure(conn))
        for t in PERSISTENT:
            if t.table == 'comments':  # updated_at, the escalation event and the three FTS sync triggers
                conn.exec_driver_sql(f'DROP TRIGGER {t.name}')
        triggers.recreate(op, 'comments')
        names = [n for (n,) in conn.exec_driver_sql("SELECT name FROM sqlite_master WHERE type = 'trigger'")]
        assert sorted(names) == sorted(t.name for t in PERSISTENT)
        probe = Trigger('events_probe', 'events', 'AFTER INSERT', body='SELECT 1', requires='events_fts')
        triggers.recreate(op, 'events', (probe, replace(probe, name='events_probe2', requires='events')))
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


def settled(db: Database) -> list[tuple[str, int, int]]:
    """Each ``subject.settled`` event: who, on which idea, of which campaign."""
    with db.session() as s:
        return [(e.actor, e.target_id, p.campaign) for e, p in in_order(s, SubjectSettled)]


def test_an_idea_settles_when_it_is_newly_done_with_no_run_open_whoever_writes_it(
    db: Database, campaign: Campaign
) -> None:
    sql = Database(str(db.engine.url), actor=System.HUMAN)

    def raw(statement: str) -> None:
        with sql.engine.begin() as conn:
            conn.execute(text(statement))

    with db.session() as s:
        quiet, rerun, done = (
            Idea(campaign_id=campaign.id, track='score', title=t, hypothesis='h', created_by='t')
            for t in ('quiet', 'rerun', 'done')
        )
        s.add_all([quiet, rerun, done])
        s.flush()
        runs = [Run(idea_id=rerun.id, attempt=n, base_candidate_id=1, harness='h', model='m') for n in (1, 2)]
        s.add_all(runs)
        s.commit()
        ids, first, second = (quiet.id, rerun.id, done.id), runs[0].id, runs[1].id
    try:
        raw(f"UPDATE ideas SET status = 'done' WHERE id = {ids[0]}")
        raw(f"UPDATE ideas SET status = 'done' WHERE id = {ids[0]}")
        assert settled(db) == [(System.HUMAN, ids[0], campaign.id)], 'a raw UPDATE settles it, once'
        raw(f"UPDATE ideas SET status = 'done' WHERE id = {ids[1]}")
        raw(f"UPDATE runs SET status = 'failed', \"end\" = 'failed' WHERE id = {first}")
        assert len(settled(db)) == 1, 'done, but a run is still open'
        with db.session() as s:
            run = s.get_one(Run, second)
            run.status, run.end = RunStatus.SUCCEEDED, end_of(RunStatus.SUCCEEDED)
            s.commit()
        assert settled(db)[1:] == [(ACTOR, ids[1], campaign.id)], 'its last open run ended'
        raw(f"UPDATE ideas SET status = 'ready' WHERE id = {ids[0]}")
        raw(f"UPDATE ideas SET status = 'parked' WHERE id = {ids[2]}")
        assert len(settled(db)) == 2, 'an idea that leaves done, or ends another way, does not settle'
        raw(f"UPDATE ideas SET status = 'done' WHERE id = {ids[0]}")
        assert settled(db)[2:] == [(System.HUMAN, ids[0], campaign.id)], 'done again: it settles again'
    finally:
        sql.close()
    assert [t.sql() for t in changelog.SETTLED] == snapshot(
        [
            """\
CREATE TEMP TRIGGER ideas_settled_event AFTER UPDATE OF status ON ideas
WHEN OLD.status IS NOT 'done' AND NEW.status = 'done' AND NOT EXISTS (SELECT 1 FROM runs WHERE idea_id = NEW.id AND status IN ('queued', 'running'))
BEGIN
  INSERT INTO events (at, actor, target_kind, target_id, payload)
  VALUES (strftime('%Y-%m-%d %H:%M:%f', 'now'), (SELECT actor FROM temp.session_actor), 'idea', NEW.id, json_object('kind', 'subject.settled', 'campaign', NEW.campaign_id));
END;\
""",
            """\
CREATE TEMP TRIGGER runs_settled_event AFTER UPDATE OF status ON runs
WHEN OLD.status IN ('queued', 'running') AND NEW.status NOT IN ('queued', 'running') AND (SELECT status FROM ideas WHERE id = NEW.idea_id) = 'done' AND NOT EXISTS (SELECT 1 FROM runs WHERE idea_id = NEW.idea_id AND id != NEW.id AND status IN ('queued', 'running'))
BEGIN
  INSERT INTO events (at, actor, target_kind, target_id, payload)
  VALUES (strftime('%Y-%m-%d %H:%M:%f', 'now'), (SELECT actor FROM temp.session_actor), 'idea', NEW.idea_id, json_object('kind', 'subject.settled', 'campaign', (SELECT campaign_id FROM ideas WHERE id = NEW.idea_id)));
END;\
""",
        ]
    )
