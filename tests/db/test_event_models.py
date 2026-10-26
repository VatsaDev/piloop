"""Events are typed models: the open registry of kinds, how a payload loads, and the triggers that render them."""

from contextlib import closing
from pathlib import Path
from typing import Literal

import pytest
from pydantic import TypeAdapter, ValidationError
from sqlalchemy import func, select

from piloop.actor import System
from piloop.db import sql
from piloop.db.events import emit, has, in_order, latest, payload_of
from piloop.db.models import EVENT, Campaign, Event, EventModel, UnknownEvent
from piloop.db.session import Database
from piloop.db.triggers import changelog
from piloop.db.triggers.changelog import IdeaCreated
from piloop.ids import CommentKind, Ref, TargetKind, TargetRef


class Probed(EventModel):
    kind: Literal['test.probed'] = 'test.probed'
    level: int
    note: str | None = None


def test_a_kind_is_registered_once_and_loads_as_its_model() -> None:
    assert Probed.kinds() == ('test.probed',)
    assert EventModel.kinds() == () and changelog.Changed.kinds() == (), 'a base registers no kind'
    assert EVENT.validate_python({'kind': 'test.probed', 'level': 3}) == Probed(level=3)
    assert EVENT.dump_python(Probed(level=3), mode='json') == {'kind': 'test.probed', 'level': 3, 'note': None}
    with pytest.raises(TypeError, match=r'Again: event kind .test.probed. is .*test_event_models\.Probed'):

        class Again(EventModel):
            kind: Literal['test.probed']


def test_a_payload_of_a_registered_kind_that_does_not_fit_its_model_raises() -> None:
    with pytest.raises(ValidationError, match='level'):
        EVENT.validate_python({'kind': 'test.probed'})
    with pytest.raises(ValidationError, match='Extra inputs are not permitted'):
        EVENT.validate_python({'kind': 'test.probed', 'level': 1, 'extra': True})
    for value in ({'level': 1}, {'kind': 7}, [1], 'test.probed'):
        with pytest.raises(ValidationError, match='an event is a JSON object with its kind in "kind"'):
            EVENT.validate_python(value)


def test_a_kind_that_nothing_registers_loads_as_an_unknown_event_and_dumps_as_stored() -> None:
    stored = {'kind': 'acme.observed', 'latency_ms': 12.5, 'tags': ['a']}
    event = EVENT.validate_python(stored)
    assert event == UnknownEvent(kind='acme.observed', payload={'latency_ms': 12.5, 'tags': ['a']})
    assert EVENT.dump_python(event, mode='json') == stored


def test_a_reference_is_its_text() -> None:
    refs: TypeAdapter[Ref] = TypeAdapter(Ref)
    assert refs.validate_python('agent_session#12') == TargetRef(TargetKind.AGENT_SESSION, 12)
    assert refs.dump_python(TargetRef(TargetKind.RUN, 3), mode='json') == 'run#3'
    for text in ('run#', 'run 3', 'worker#3'):
        with pytest.raises(ValidationError):
            refs.validate_python(text)


def test_has_finds_the_events_of_a_model_by_their_fields_and_the_readers_type_them(db: Database) -> None:
    on = TargetRef(TargetKind.CAMPAIGN, 1)
    with db.session() as s:
        for level in (1, 2, 3):
            emit(s, Probed(level=level), on, System.DAEMON)
        emit(s, IdeaCreated(), TargetRef(TargetKind.IDEA, 1), System.HUMAN)
        s.commit()
        assert [p.level for _, p in in_order(s, Probed, has(Probed, level=(1, 3)))] == [1, 3]
        found = latest(s, Probed, Event.target == on)
        assert found is not None and found[1] == Probed(level=3)
        assert latest(s, Probed, has(Probed, level=9)) is None
        with pytest.raises(TypeError, match="Probed has no field 'levels'"):
            has(Probed, levels=1)
        created = s.scalars(select(Event).where(has(IdeaCreated))).one()
        with pytest.raises(TypeError, match=rf'event#{created.id} is IdeaCreated, not Probed'):
            payload_of(created, Probed)


def test_every_trigger_writes_an_event_that_loads_as_the_model_that_renders_it(
    db: Database, tmp_path: Path, campaign: Campaign
) -> None:
    """The change log's SQL comes from its models, so a raw statement of an agent writes what the models read."""
    with db.session() as s:
        start = s.scalar(select(func.max(Event.id))) or 0
    with closing(sql.connect(tmp_path / 'piloop.db', actor=System.HUMAN)) as con:
        statements = [
            "INSERT INTO ideas (track, campaign_id, title, hypothesis, created_by) VALUES ('score', 1, 'a', 'h', 'me')",
            "INSERT INTO ideas (track, campaign_id, title, hypothesis, created_by) VALUES ('score', 1, 'b', 'h', 'me')",
            "UPDATE ideas SET status = 'parked', priority = 0, track = 'size', base_candidate_id = 1 WHERE id = 1",
            "INSERT INTO idea_links (idea_id, target_id, kind) VALUES (1, 2, 'after')",
            'DELETE FROM idea_links',
            "INSERT INTO candidates (campaign_id, tree_sha, revision) VALUES (1, 'tree', 'sha')",
            "UPDATE candidates SET origin = 'skipped' WHERE id = 2",
            "UPDATE candidates SET origin = 'run', verdict = 'interesting', verdict_by = 'human' WHERE id = 2",
            "INSERT INTO runs (idea_id, attempt, base_candidate_id, harness, model) VALUES (1, 1, 1, 'pi', 'm')",
            "UPDATE runs SET brief_cid = 'bafkr4idjif4mbxlvnejqvi73n325nycsdssgrqrgzlxeijc4onmshlzc7e' WHERE id = 1",
            "UPDATE ideas SET status = 'done' WHERE id = 1",
            "UPDATE runs SET status = 'failed', \"end\" = 'failed' WHERE id = 1",
            "INSERT INTO campaigns (name, path, config) VALUES ('other', '/other', '{}')",
            "UPDATE campaigns SET status = 'stopped', stopped_reason = 'enough', type = 'research' WHERE id = 2",
            "UPDATE campaigns SET parent_id = 1, paused = 'all', paused_reason = 'night' WHERE id = 2",
            *(
                'INSERT INTO comments (target_kind, target_id, author, kind, body, priority)'
                f" VALUES ('idea', 1, 'me', '{kind}', 'x', 2)"
                for kind in CommentKind
            ),
            "UPDATE comments SET acked_at = '2026-10-01 12:00:00', acked_by = 'human' WHERE id = 1",
            'DELETE FROM comments WHERE id = 1',
            'DELETE FROM candidates WHERE id = 2',
            'DELETE FROM ideas WHERE id = 2',
            "INSERT INTO agent_sessions (kind, campaign_id, state, reason) VALUES ('research', 1, 'requested', 'r')",
            "UPDATE campaigns SET config = json_set(config, '$.confidence', 0.9) WHERE id = 1",
            "INSERT INTO evaluations (candidate_id, evaluator_id, started_at) VALUES (1, 1, '2026-10-01 12:00:00')",
            "UPDATE evaluations SET cancelled_by = 'human'",
            "INSERT INTO limits (key, value, reason, actor) VALUES ('executor.agents', '2', 'r', 'human')",
            "UPDATE limits SET value = '3'",
            'DELETE FROM limits',
        ]
        for statement in statements:
            sql.run(con, statement)
    with db.session() as s:
        models = {e.kind: type(e.payload) for e in s.scalars(select(Event).where(Event.id > start))}
    rendered = [m for m in vars(changelog).values() if isinstance(m, type) and issubclass(m, EventModel) and m.kinds()]
    assert models == {kind: model for model in rendered for kind in model.kinds()}, (
        'each model of the change log has its trigger, and the row that it writes loads as the model'
    )


def test_a_retired_kind_loads_as_an_unknown_event_and_no_model_takes_it() -> None:
    EventModel.retire('test.gone')
    assert 'test.gone' in EventModel.retired_kinds()
    assert EVENT.validate_python({'kind': 'test.gone', 'a': 1}) == UnknownEvent(kind='test.gone', payload={'a': 1})
    with pytest.raises(TypeError, match="Back: event kind 'test.gone' is retired"):

        class Back(EventModel):
            kind: Literal['test.gone'] = 'test.gone'

    with pytest.raises(TypeError, match="event kind 'test.probed' is .*Probed: it cannot be retired"):
        EventModel.retire('test.probed')
