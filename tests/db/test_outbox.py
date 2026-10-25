"""The outbox triggers: each write that the daemon reacts to emits its event, from the ORM and from ``piloop sql``."""

import sqlite3
from contextlib import closing
from pathlib import Path

import pytest
from sqlalchemy import select

from piloop.actor import System
from piloop.db import sql
from piloop.db.models import Campaign, Evaluation, Event, EventModel, Idea, Limit
from piloop.db.session import Database
from piloop.db.triggers.changelog import (
    AgentSessionRequested,
    CampaignConfigChanged,
    EvaluationCancelled,
    IdeaCreated,
    LimitSet,
    LimitUnset,
)
from piloop.ids import HOME, CampaignId, RoleName, TargetKind, TargetRef
from piloop.scoring import queue
from tests.conftest import baseline


def emitted(db: Database, after: int = 0) -> list[tuple[str, TargetRef, EventModel]]:
    with db.session() as s:
        rows = s.scalars(select(Event).where(Event.id > after).order_by(Event.id)).all()
        return [(row.actor, row.target, row.payload) for row in rows]


def newest(db: Database) -> int:
    with db.session() as s:
        return s.scalar(select(Event.id).order_by(Event.id.desc()).limit(1)) or 0


def test_a_requested_agent_emits_its_request_from_piloop_sql_and_no_other_insert_does(
    db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    start = newest(db)
    with closing(sql.connect(tmp_path / 'piloop.db', actor=System.HUMAN)) as con:
        sql.run(
            con,
            f"INSERT INTO agent_sessions (kind, campaign_id, state, reason) VALUES ('research', {campaign.id},"
            " 'requested', 'by hand')",
        )
        sql.run(
            con, f"INSERT INTO agent_sessions (kind, campaign_id, state) VALUES ('research', {campaign.id}, 'exited')"
        )
    [(actor, target, event)] = emitted(db, start)
    assert actor == 'human' and target.kind is TargetKind.AGENT_SESSION
    assert event == AgentSessionRequested(role=RoleName('research'), campaign=CampaignId(campaign.id), reason='by hand')


def test_a_new_idea_emits_its_creation_signed_by_who_created_it(db: Database, campaign: Campaign) -> None:
    start = newest(db)
    with db.session() as s:
        idea = Idea(campaign_id=campaign.id, track='score', title='t', hypothesis='h', created_by='research#4')
        s.add(idea)
        s.commit()
    assert emitted(db, start) == [('research#4', TargetRef(TargetKind.IDEA, idea.id), IdeaCreated())]


def test_a_changed_config_emits_a_config_event_and_the_same_config_none(db: Database, campaign: Campaign) -> None:
    start = newest(db)
    with db.session() as s:
        row = s.get_one(Campaign, campaign.id)
        row.config = row.config.model_copy()
        s.commit()
        assert emitted(db, start) == [], 'the same config'
        row.config = row.config.model_copy(update={'evaluate_timeout': 7})
        s.commit()
    [(_, target, event)] = emitted(db, start)
    assert (target, event) == (TargetRef(TargetKind.CAMPAIGN, campaign.id), CampaignConfigChanged())


def test_a_cancel_of_a_running_evaluation_emits_it_on_the_candidate(db: Database, campaign: Campaign) -> None:
    candidate = baseline(db, campaign.id)
    with db.session() as s:
        evaluator_id = s.get_one(Campaign, campaign.id).evaluator_id
        assert evaluator_id is not None
        evaluation, _ = queue.request(s, candidate.id, evaluator_id)
        s.get_one(Evaluation, evaluation.id).started_at = evaluation.created_at
        s.commit()
    start = newest(db)
    with db.session() as s:
        queue.cancel(s, candidate.id, System.HUMAN)
        s.commit()
    cancelled = [(t, e) for _, t, e in emitted(db, start) if isinstance(e, EvaluationCancelled)]
    assert cancelled == [
        (TargetRef(TargetKind.CANDIDATE, candidate.id), EvaluationCancelled(evaluation=evaluation.id, by='human'))
    ]


def test_a_limit_set_changed_and_unset_emits_its_events_on_the_home(db: Database, tmp_path: Path) -> None:
    start = newest(db)
    with db.session() as s:
        s.add(Limit(key='executor.agents', value=2, reason='r', actor='human'))
        s.commit()
        limit = s.get_one(Limit, 'executor.agents')
        limit.reason = 'only the reason'
        s.commit()
        limit.value = {'cap': None}
        s.commit()
        s.delete(limit)
        s.commit()
    assert [(target, event) for _, target, event in emitted(db, start)] == [
        (HOME, LimitSet(key='executor.agents', value=2)),
        (HOME, LimitSet(key='executor.agents', value={'cap': None})),
        (HOME, LimitUnset(key='executor.agents')),
    ]


def test_no_comment_is_on_the_home(db: Database, tmp_path: Path) -> None:
    with (
        closing(sql.connect(tmp_path / 'piloop.db', actor=System.HUMAN)) as con,
        pytest.raises(sqlite3.IntegrityError, match='targetkind'),
    ):
        sql.run(con, "INSERT INTO comments (target_kind, target_id, author, body) VALUES ('home', 1, 'human', 'x')")
