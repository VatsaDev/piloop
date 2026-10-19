"""The control operations: each validates, changes the state and leaves its event, in the caller's transaction."""

import sqlite3
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from piloop.campaign import ops
from piloop.db.models import Comment, Event, Pause, Project, TargetKind
from piloop.db.session import Database

ACTOR = 'orchestrator'


@pytest.fixture
def me(db: Database, project: Project, tmp_path: Path) -> Iterator[Database]:
    """The project's database, opened as the orchestrator: the change-log triggers sign its writes with that actor."""
    database = Database(f'sqlite:///{tmp_path / "piloop.db"}', actor=ACTOR)
    yield database
    database.close()


def kinds(db: Database, target_kind: TargetKind, target_id: int) -> list[tuple[str, str, dict[str, object]]]:
    with db.session() as s:
        rows = s.scalars(
            select(Event).where(Event.target_kind == target_kind, Event.target_id == target_id).order_by(Event.id)
        )
        return [(e.kind, e.actor, e.payload) for e in rows]


def test_a_pause_holds_launches_or_everything_with_its_reason_and_a_resume_clears_it(me: Database) -> None:
    with me.session() as s:
        project = ops.pause(s, 1, evals=False, reason=' wait for evaluator v2 ')
        s.commit()
        assert (project.paused, project.paused_reason) == (Pause.LAUNCHES, 'wait for evaluator v2')
    with me.session() as s, pytest.raises(ops.Refused, match=r'paused already \(launches\): wait for evaluator v2'):
        ops.pause(s, 1, evals=False, reason='again')
    with me.session() as s:
        ops.pause(s, 1, evals=True, reason='and the evaluations')
        s.commit()
    with me.session() as s:
        assert ops.resume(s, 1) is Pause.ALL
        s.commit()
        assert (s.get_one(Project, 1).paused, s.get_one(Project, 1).paused_reason) == (None, None)
    assert [e for e in kinds(me, TargetKind.PROJECT, 1) if e[0] == 'project.paused'] == [
        ('project.paused', ACTOR, {'old': None, 'new': 'launches', 'paused_reason': 'wait for evaluator v2'}),
        ('project.paused', ACTOR, {'old': 'launches', 'new': 'all', 'paused_reason': 'and the evaluations'}),
        ('project.paused', ACTOR, {'old': 'all', 'new': None, 'paused_reason': None}),
    ], 'the trigger logs each change, signed by the connection, and nothing else does'
    with me.session() as s:
        assert s.scalars(select(Comment)).all() == [], 'a pause is not an escalation'
        with pytest.raises(ops.Refused, match='project demo is not paused'):
            ops.resume(s, 1)
        with pytest.raises(ops.Refused, match='a reason is required'):
            ops.pause(s, 1, evals=False, reason='  ')
        with pytest.raises(ops.Refused, match='no project#9'):
            ops.pause(s, 9, evals=False, reason='why')


def test_a_raw_pause_needs_a_reason_and_a_known_scope(me: Database, tmp_path: Path) -> None:
    with closing(sqlite3.connect(tmp_path / 'piloop.db')) as con:
        with pytest.raises(sqlite3.IntegrityError, match='projects.paused and projects.paused_reason go together'):
            con.execute("UPDATE projects SET paused = 'launches' WHERE id = 1")
        with pytest.raises(sqlite3.IntegrityError, match='CHECK constraint failed'):
            con.execute("UPDATE projects SET paused = 'forever', paused_reason = 'r' WHERE id = 1")
    with me.session() as s:
        s.get_one(Project, 1).paused_reason = 'no pause'
        with pytest.raises(IntegrityError, match='go together'):
            s.commit()


def test_only_a_sweeping_agent_files_its_ideas_for_review_while_the_project_is_paused(me: Database) -> None:
    with me.session() as s:
        project = s.get_one(Project, 1)
        assert not ops.holds_ideas(project, 'research#4'), 'not paused'
        ops.pause(s, 1, evals=False, reason='why')
        assert ops.holds_ideas(project, 'research#4') and ops.holds_ideas(project, 'watchdog#5')
        assert not ops.holds_ideas(project, ACTOR) and not ops.holds_ideas(project, 'human')
        assert not ops.holds_ideas(project, 'worker#6'), 'a worker files no ideas anyway'
