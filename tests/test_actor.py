from contextlib import closing
from pathlib import Path

import pytest
from pydantic import TypeAdapter, ValidationError

from piloop.actor import Actor, Agent, System
from piloop.db import sql
from piloop.db.events import emit
from piloop.db.models import Event
from piloop.db.session import Database
from piloop.ids import TargetKind, TargetRef
from piloop.scoring.ledger import IdeaCreated
from tests.conftest import at_head

ACTORS: TypeAdapter[Actor] = TypeAdapter(Actor)
"""Validates a signature into its actor, as ``Settings.actor`` does, and dumps an actor back."""


@pytest.mark.parametrize(
    ('signed', 'actor'),
    [
        ('implementer#12', Agent(role='implementer', session=12)),
        ('my-role#7', Agent(role='my-role', session=7)),
        ('human', System.HUMAN),
        ('orchestrator', System.ORCHESTRATOR),
        ('daemon', System.DAEMON),
    ],
)
def test_a_signature_parses_into_its_actor_and_dumps_back(signed: str, actor: Actor) -> None:
    assert ACTORS.validate_python(signed) == actor
    assert ACTORS.dump_python(actor) == actor.signature == str(actor) == signed
    assert ACTORS.validate_json(ACTORS.dump_json(actor)) == actor


@pytest.mark.parametrize(
    'signed', ['', '#', 'implementer#', '#12', 'implementer#0', 'Scout#3', 'a#b', 'a#1#2', 'controller', 'Human']
)
def test_a_malformed_signature_or_an_unknown_name_is_refused(signed: str) -> None:
    with pytest.raises(ValidationError):
        ACTORS.validate_python(signed)


def test_an_actor_is_written_as_its_signature(tmp_path: Path, schema: Path) -> None:
    """``emit`` signs ``events.actor``, and the connection's actor signs ``updated_by``, with the signature."""
    at_head(schema, tmp_path / 'piloop.db')
    scout = Agent(role='scout', session=3)
    with closing(Database(f'sqlite:///{tmp_path / "piloop.db"}', actor=scout)) as db, db.session() as s:
        emit(s, IdeaCreated(), TargetRef(TargetKind.IDEA, 1), scout)
        s.commit()
        assert [ACTORS.validate_python(e.actor) for e in s.query(Event)] == [scout]
    with closing(sql.connect(tmp_path / 'piloop.db', actor=scout)) as con:
        assert tuple(con.execute('SELECT actor FROM temp.session_actor').fetchone()) == ('scout#3',)
