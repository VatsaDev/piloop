import gc
import os
import re
import sqlite3
import subprocess
import sys
import warnings
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path

import pytest
from inline_snapshot import snapshot
from sqlalchemy import select
from sqlalchemy.exc import OperationalError

from piloop.daemon.main import ACTOR
from piloop.db import sql
from piloop.db.models import Base, CommentKind, Project
from piloop.db.session import Database
from tests.conftest import WRITER


@pytest.fixture
def con(db: Database, tmp_path: Path) -> Iterator[sqlite3.Connection]:
    connection = sql.connect(tmp_path / 'piloop.db', actor='human')
    yield connection
    connection.close()


def test_anything_but_schema_changes(con: sqlite3.Connection) -> None:
    r = sql.run(con, "INSERT INTO projects (name, path, config, created_at) VALUES ('p', '/p', '{}', '2026-01-01')")
    assert (r.changes, r.last_insert_rowid, r.columns) == (1, 1, [])
    r = sql.run(con, 'SELECT id, name FROM projects')
    assert (r.columns, r.rows, r.truncated) == (['id', 'name'], [(1, 'p')], False)
    assert sql.run(con, 'DELETE FROM projects').changes == 1
    for statement in (
        'CREATE TABLE t (x)',
        'DROP TABLE projects',
        'ALTER TABLE projects ADD COLUMN x',
        'CREATE INDEX i ON projects(name)',
        'CREATE VIRTUAL TABLE v USING fts5(body)',
        "ATTACH DATABASE ':memory:' AS m",
        'PRAGMA journal_mode=DELETE',
        'BEGIN',
        'SAVEPOINT s',
        'DELETE FROM alembic_version',
        'CREATE TEMP TABLE t (x)',
        'CREATE TEMP TRIGGER t AFTER UPDATE ON projects BEGIN SELECT 1; END',
        "INSERT INTO session_actor (actor) VALUES ('me')",
        "UPDATE session_actor SET actor = 'me'",
        'DELETE FROM session_actor',
    ):
        with pytest.raises(sqlite3.DatabaseError, match='not authorized'):
            sql.run(con, statement)
    assert 'name' in sql.run(con, 'PRAGMA table_info(projects)').columns


def test_row_limit(con: sqlite3.Connection) -> None:
    con.executemany(
        "INSERT INTO comments (target_kind, target_id, author, kind, body, created_at) VALUES ('idea', 1, 'a', 'note', ?, '2026-01-01')",
        [(str(i),) for i in range(5)],
    )
    r = sql.run(con, 'SELECT body FROM comments', limit=3)
    assert (len(r.rows), r.truncated) == (3, True)


def test_schema_ddl_lists_every_table_with_its_indexes_and_runs(tmp_path: Path) -> None:
    ddl = sql.schema_ddl()
    assert sorted(line.split()[2] for line in ddl.splitlines() if line.startswith('CREATE TABLE')) == snapshot(
        [
            'candidates',
            'comments',
            'evaluators',
            'events',
            'idea_links',
            'ideas',
            'measurements',
            'projects',
            'runs',
            'techniques',
            'workers',
        ]
    )
    assert 'CREATE TRIGGER' not in ddl
    with closing(sqlite3.connect(tmp_path / 'from_ddl.db')) as con:
        con.executescript(ddl)
        indexes = [i for (i,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'index' AND sql IS NOT NULL")]
    assert sorted(indexes) == sorted(str(i.name) for t in Base.metadata.tables.values() for i in t.indexes)
    assert ddl.count('CREATE VIRTUAL TABLE') == 2, (
        'the indexes are schema the agents query, listed without triggers too'
    )


def test_documented_procedures_work_verbatim(con: sqlite3.Connection) -> None:
    """The INSERTs in sql_procedures.md rely on server defaults for timestamps, statuses and priorities."""
    sql.run(con, "INSERT INTO projects (name, path, config) VALUES ('p', '/p', '{}')")
    idea = sql.run(
        con,
        "INSERT INTO ideas (track, project_id, title, hypothesis, plan, priority, created_by, status) VALUES ('score', 1, 't', 'h', NULL, 1, 'me', 'ready')",
    )
    assert idea.last_insert_rowid == 1
    row = sql.run(con, 'SELECT status, max_runs, created_at IS NOT NULL FROM ideas').rows[0]
    assert row == ('ready', 2, 1)
    insert = "INSERT INTO comments (target_kind, target_id, author, kind, body{}) VALUES ('idea', 1, 'me', 'escalation', 'help'{})"
    with pytest.raises(sqlite3.IntegrityError, match='CHECK constraint failed: escalation_needs_priority'):
        sql.run(con, insert.format('', ''))
    sql.run(con, insert.format(', priority', ', 1'))
    events = sql.run(
        con, "SELECT kind, actor, target_kind, target_id, payload FROM events WHERE kind = 'comment.escalation'"
    )
    assert events.rows == snapshot([('comment.escalation', 'me', 'comment', 1, '{"target_kind":"idea","target_id":1}')])
    with pytest.raises(sqlite3.IntegrityError, match='CHECK constraint failed'):
        sql.run(con, "UPDATE ideas SET status = 'urgent'")


def test_every_update_is_stamped_with_the_connection_actor(db: Database, tmp_path: Path) -> None:
    with closing(sql.connect(tmp_path / 'piloop.db', actor='research#3')) as con:
        sql.run(con, "INSERT INTO projects (name, path, config) VALUES ('p', '/p', '{}')")
        assert sql.run(con, 'SELECT updated_at = created_at, updated_by FROM projects').rows == [(1, None)]
        sql.run(con, "UPDATE projects SET name = 'q'")
        assert sql.run(con, 'SELECT updated_by FROM projects').rows == [('research#3',)]
        sql.run(con, "UPDATE projects SET name = 'r', updated_by = 'someone else'")
        assert sql.run(con, 'SELECT updated_by FROM projects').rows == [('research#3',)], 'callers cannot pick'
    with closing(sql.connect(tmp_path / 'piloop.db', actor='human')) as con:
        sql.run(con, "UPDATE projects SET name = 's'")
        assert sql.run(con, 'SELECT updated_by FROM projects').rows == [('human',)]


def test_actor_triggers_cover_every_table_with_updated_by() -> None:
    tracked = {t.name for t in Base.metadata.sorted_tables if 'updated_by' in t.columns}
    triggered = set(re.findall(r'CREATE TEMP TRIGGER (\w+)_updated_by AFTER UPDATE ON \1', sql.ACTOR_TRIGGERS))
    assert triggered == tracked == snapshot({'candidates', 'comments', 'ideas', 'projects', 'runs', 'workers'})


def test_every_client_connection_has_the_same_pragmas_and_optimizes_on_close(db: Database, tmp_path: Path) -> None:
    """Agent connections and the engine's apply CONNECTION_PRAGMAS on connect and run PRAGMA optimize before closing.

    The agent connection's authorizer denies PRAGMAs to the agent's statements; the settings go in before it is armed
    and the optimize lifts it for that one statement.
    """
    expected = {'foreign_keys': 1, 'busy_timeout': 5000, 'synchronous': 1, 'temp_store': 2, 'journal_mode': 'wal'}

    def pragmas(con: sqlite3.Connection) -> dict[str, object]:
        return {name: con.execute(f'PRAGMA {name}').fetchone()[0] for name in expected}

    agent = sql.connect(tmp_path / 'piloop.db', actor='human')
    with pytest.raises(sqlite3.DatabaseError, match='not authorized'):
        sql.run(agent, 'PRAGMA synchronous=FULL')
    agent.set_authorizer(None)
    assert pragmas(agent) == expected
    agent.set_authorizer(sql._authorize)
    traced: list[str] = []  # the optimize first, then whatever ANALYZE it decided on, traced as ``-- `` comments
    agent.set_trace_callback(traced.append)
    agent.close()
    assert traced[:2] == ['PRAGMA busy_timeout=0', sql.OPTIMIZE]

    pooled = db.engine.raw_connection()
    engine_con = pooled.driver_connection
    assert isinstance(engine_con, sqlite3.Connection)
    assert pragmas(engine_con) == expected
    traced.clear()
    engine_con.set_trace_callback(traced.append)
    pooled.close()  # back to the pool: not closed yet
    assert traced == []
    db.close()
    assert traced[:2] == ['PRAGMA busy_timeout=0', sql.OPTIMIZE]


@pytest.mark.skipif(
    sys.platform != 'linux', reason='piloop reads the lock holder from /proc/locks, which only Linux has'
)
def test_a_write_the_lock_refuses_names_the_holder(db: Database, tmp_path: Path, writer: subprocess.Popen[str]) -> None:
    """A 'database is locked' error names the process that holds the write lock and its command line."""
    holder = f'pid {writer.pid} holds the write lock: {sys.executable} -c {WRITER} {tmp_path / "piloop.db"}'
    insert = "INSERT INTO projects (name, path, config) VALUES ('p', '/p', '{}')"
    with db.session() as s:
        s.connection().exec_driver_sql('PRAGMA busy_timeout=0')
        with pytest.raises(OperationalError) as engine_error:
            s.connection().exec_driver_sql(insert)
    assert str(engine_error.value).splitlines()[:2] == [f'({holder})', '(sqlite3.OperationalError) database is locked']
    with db.session() as s, pytest.raises(OperationalError) as other:  # any other error is left as it was
        s.connection().exec_driver_sql('SELECT * FROM no_such_table')
    assert str(other.value).splitlines()[0] == '(sqlite3.OperationalError) no such table: no_such_table'

    agent = sql.connect(tmp_path / 'piloop.db', actor='human')
    agent.set_authorizer(None)
    agent.execute('PRAGMA busy_timeout=0')
    with pytest.raises(sqlite3.OperationalError) as agent_error:
        sql.run(agent, insert)
    assert sql.lock_holder(agent_error.value, tmp_path / 'piloop.db') == holder
    agent.close()


def test_reads_close_while_another_client_holds_the_write_lock(db: Database, tmp_path: Path, writer) -> None:
    """A close's ``PRAGMA optimize`` may ANALYZE, which writes: it is skipped, not waited for or failed with."""
    with db.session() as s:
        assert s.scalars(select(Project)).all() == []
    db.close()
    agent = sql.connect(tmp_path / 'piloop.db', actor='human')
    assert sql.run(agent, 'SELECT count(*) FROM projects').rows == [(0,)]
    agent.close()


def test_optimize_raises_what_is_not_the_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sql, 'OPTIMIZE', 'SELECT * FROM no_such_table')
    with closing(sqlite3.connect(':memory:')) as con, pytest.raises(sqlite3.OperationalError, match='no such table'):
        sql.optimize(con)


def test_the_holder_is_the_write_lock_line_in_proc_locks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Only a WRITE lock on byte 120 of this database's -shm file; a waiter (``->``) is not the holder."""
    database = tmp_path / 'piloop.db'
    holding = sqlite3.connect(database, isolation_level=None)
    holding.execute('PRAGMA journal_mode=WAL')
    holding.execute('BEGIN IMMEDIATE')
    with closing(sqlite3.connect(database, timeout=0)) as other, pytest.raises(sqlite3.OperationalError) as busy:
        other.execute('BEGIN IMMEDIATE')
    st = Path(f'{database}-shm').stat()
    file = f'{os.major(st.st_dev):02x}:{os.minor(st.st_dev):02x}:{st.st_ino}'
    locks = tmp_path / 'locks'
    monkeypatch.setattr(sql, 'PROC_LOCKS', locks)
    gone = 2**22 + 1  # above Linux's pid_max default: no such process
    locks.write_text(
        f'1: POSIX  ADVISORY  READ {gone} {file} 124 124\n'
        f'2: POSIX  ADVISORY  WRITE {gone} 00:00:1 120 120\n'
        f'3: -> POSIX  ADVISORY  WRITE {gone} {file} 120 120\n'
    )
    assert sql.lock_holder(busy.value, database) is None
    locks.write_text(f'4: POSIX  ADVISORY  WRITE {gone} {file} 120 120\n')
    assert sql.lock_holder(busy.value, database) == f'pid {gone} holds the write lock: (exited)'
    locks.unlink()  # no /proc: not Linux
    assert sql.lock_holder(busy.value, database) is None
    assert sql.lock_holder(busy.value, tmp_path / 'other.db') is None, 'no -shm file'
    holding.close()


def test_connect_requires_an_existing_database(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match='run `piloop serve` first'):
        sql.connect(tmp_path / 'missing.db', actor='human')


def events(con: sqlite3.Connection, since: int = 0) -> list[tuple[object, ...]]:
    return sql.run(
        con, f'SELECT kind, actor, target_kind, target_id, payload FROM events WHERE id > {since} ORDER BY id'
    ).rows


def test_one_update_of_a_tracked_column_is_one_event_and_the_provenance_stamps_still_land(
    con: sqlite3.Connection,
) -> None:
    """The change-log trigger, the updated_at trigger and the updated_by trigger each do their own UPDATE: one event."""
    sql.run(con, "INSERT INTO projects (name, path, config) VALUES ('p', '/p', '{}')")
    sql.run(
        con, "INSERT INTO ideas (track, project_id, title, hypothesis, created_by) VALUES ('score', 1, 't', 'h', 'me')"
    )
    (created,) = sql.run(con, 'SELECT updated_at FROM ideas').rows[0]
    sql.run(con, "UPDATE ideas SET status = 'parked' WHERE id = 1")
    assert events(con) == snapshot([('idea.status', 'human', 'idea', 1, '{"old":"ready","new":"parked"}')])
    updated_at, updated_by = sql.run(con, 'SELECT updated_at, updated_by FROM ideas').rows[0]
    assert updated_at != created and updated_by == 'human'
    sql.run(con, "UPDATE ideas SET status = 'parked' WHERE id = 1")
    sql.run(con, "UPDATE ideas SET title = 'renamed' WHERE id = 1")
    assert len(events(con)) == 1, 'the same value again, or an untracked column, is not a change of the tracked one'
    sql.run(con, "UPDATE ideas SET priority = 0, status = 'ready' WHERE id = 1")
    assert events(con, since=1) == snapshot(
        [
            ('idea.priority', 'human', 'idea', 1, '{"old":2,"new":0}'),
            ('idea.status', 'human', 'idea', 1, '{"old":"parked","new":"ready"}'),
        ]
    )


def test_verdict_brief_and_project_status_changes_carry_what_the_feed_needs(
    db: Database, tmp_path: Path, con: sqlite3.Connection
) -> None:
    sql.run(con, "INSERT INTO projects (name, path, config) VALUES ('p', '/p', '{}')")
    sql.run(
        con, "INSERT INTO ideas (track, project_id, title, hypothesis, created_by) VALUES ('score', 1, 't', 'h', 'me')"
    )
    sql.run(con, "INSERT INTO candidates (project_id, tree_sha, commit_sha) VALUES (1, 'tree', 'sha')")
    sql.run(con, "INSERT INTO runs (idea_id, attempt, base_candidate_id, harness, model) VALUES (1, 1, 1, 'pi', 'm')")
    sql.run(
        con,
        "UPDATE candidates SET verdict = 'interesting', verdict_by = 'human', verdict_reason = 'not a win, a lead' WHERE id = 1",
    )
    sql.run(con, "UPDATE runs SET brief = 'do it my way' WHERE id = 1")
    sql.run(con, "UPDATE projects SET status = 'stopped', stopped_reason = 'enough' WHERE id = 1")
    sql.run(con, "UPDATE projects SET status = 'active', stopped_reason = NULL WHERE id = 1")
    assert events(con) == snapshot(
        [
            (
                'candidate.verdict',
                'human',
                'candidate',
                1,
                '{"old":"pending","new":"interesting","verdict_by":"human","verdict_reason":"not a win, a lead"}',
            ),
            ('run.brief', 'human', 'run', 1, '{"old_length":null,"new_length":12}'),
            ('project.status', 'human', 'project', 1, '{"old":"active","new":"stopped","stopped_reason":"enough"}'),
            ('project.status', 'human', 'project', 1, '{"old":"stopped","new":"active","stopped_reason":null}'),
        ]
    )
    with closing(sql.connect(tmp_path / 'piloop.db', actor=ACTOR)) as daemon:
        sql.run(daemon, 'UPDATE runs SET brief = NULL WHERE id = 1')
        sql.run(daemon, "UPDATE runs SET brief = 'the composed brief' WHERE id = 1")
        assert events(daemon, since=4) == snapshot(
            [('run.brief', 'daemon', 'run', 1, '{"old_length":12,"new_length":null}')]
        ), 'the daemon composing a brief where there was none is the run starting, not a steer; replacing one is'
        sql.run(daemon, "UPDATE runs SET brief = 'the brief, revised' WHERE id = 1")
        assert events(daemon, since=5) == snapshot(
            [('run.brief', 'daemon', 'run', 1, '{"old_length":18,"new_length":18}')]
        )
    assert f"= '{ACTOR}'" in sql.ACTOR_TRIGGERS, 'the SQL names the daemon by its actor string'


def test_deletes_leave_tombstones_and_cascades_too(con: sqlite3.Connection) -> None:
    sql.run(con, "INSERT INTO projects (name, path, config) VALUES ('p', '/p', '{}')")
    sql.run(
        con, "INSERT INTO ideas (track, project_id, title, hypothesis, created_by) VALUES ('score', 1, 't', 'h', 'me')"
    )
    sql.run(con, "INSERT INTO candidates (project_id, tree_sha, commit_sha, verdict) VALUES (1, 'tree', 'sha', 'keep')")
    sql.run(
        con, "INSERT INTO comments (target_kind, target_id, author, kind, body) VALUES ('idea', 1, 'me', 'note', 'hi')"
    )
    sql.run(con, 'DELETE FROM comments WHERE id = 1')
    sql.run(con, 'DELETE FROM projects WHERE id = 1')
    assert events(con) == snapshot(
        [
            ('comment.note', 'me', 'comment', 1, '{"target_kind":"idea","target_id":1}'),
            (
                'comment.deleted',
                'human',
                'comment',
                1,
                '{"target_kind":"idea","target_id":1,"author":"me","kind":"note","body":"hi"}',
            ),
            ('candidate.deleted', 'human', 'candidate', 1, '{"tree_sha":"tree","commit_sha":"sha","verdict":"keep"}'),
            ('idea.deleted', 'human', 'idea', 1, '{"title":"t","status":"ready","hypothesis":"h"}'),
        ]
    ), 'a cascade deletes the child rows too, and each one leaves a tombstone'


def test_every_comment_kind_is_an_event_shaped_like_the_escalation_one(con: sqlite3.Connection) -> None:
    for kind in CommentKind:
        sql.run(
            con,
            'INSERT INTO comments (target_kind, target_id, author, kind, body, priority)'
            f" VALUES ('run', 7, 'pi', '{kind}', 'x', 2)",
        )
    assert events(con) == snapshot(
        [
            ('comment.note', 'pi', 'comment', 1, '{"target_kind":"run","target_id":7}'),
            ('comment.finding', 'pi', 'comment', 2, '{"target_kind":"run","target_id":7}'),
            ('comment.decision', 'pi', 'comment', 3, '{"target_kind":"run","target_id":7}'),
            ('comment.escalation', 'pi', 'comment', 4, '{"target_kind":"run","target_id":7}'),
        ]
    ), 'one event per comment: the persistent escalation trigger and the per-connection one never both fire'


def test_acknowledging_an_inbox_item_is_logged_with_the_connection_actor(con: sqlite3.Connection) -> None:
    sql.run(
        con,
        'INSERT INTO comments (target_kind, target_id, author, kind, body, priority)'
        " VALUES ('run', 7, 'pi', 'decision', 'lowered the floor', 0)",
    )
    sql.run(con, "UPDATE comments SET acked_at = '2026-10-01 12:00:00', acked_by = 'human' WHERE id = 1")
    sql.run(con, "UPDATE comments SET acked_by = 'human' WHERE id = 1")
    assert events(con)[1:] == snapshot(
        [('comment.acked_at', 'human', 'comment', 1, '{"old":null,"new":"2026-10-01 12:00:00","acked_by":"human"}')]
    )


def test_a_connection_whose_setup_fails_is_closed(
    db: Database, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A locked database can fail the pragmas or the actor's triggers. The half-made connection must not leak."""

    def locked(_con: sqlite3.Connection, _actor: str) -> None:
        raise sqlite3.OperationalError('database is locked')

    monkeypatch.setattr(sql, 'install_actor', locked)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        with pytest.raises(sqlite3.OperationalError, match='database is locked'):
            sql.connect(tmp_path / 'piloop.db', actor='human')
        gc.collect()
    assert [str(w.message) for w in caught if issubclass(w.category, ResourceWarning)] == []
