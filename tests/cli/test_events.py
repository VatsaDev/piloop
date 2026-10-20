"""The machine-readable read side of the CLI: ``piloop events``, and ``--json`` on ``status`` and ``inbox``.

The acceptance test drives a real second process, a controller that knows piloop only by its CLI, against a temp home
while this process writes: snapshot with a cursor, follow, disconnect, resume, and an event kind it does not know.
"""

import os
import subprocess
import sys
import threading
from pathlib import Path
from typing import Literal, TypedDict

import pytest
from pydantic import TypeAdapter
from rich.console import Console
from sqlalchemy import select, text

from piloop.campaign import ops, project as project_module
from piloop.cli import common, main as cli
from piloop.db.models import Comment, CommentKind, Event, Idea, IdeaStatus, Project, TargetKind
from piloop.db.session import Database
from piloop.reports import feed
from piloop.settings import Settings

UNKNOWN = (
    "INSERT INTO events (kind, actor, target_kind, target_id, payload) VALUES ('acme.observed', 'acme', 'project', 1, "
    '\'{"latency_ms": 12.5, "tags": ["a"]}\')'
)
BROKEN = "INSERT INTO events (kind, actor, target_kind, target_id, payload) VALUES ('acme.raw', 'acme', 'project', 1, 'oops')"
HUGE = (
    "INSERT INTO events (kind, actor, target_kind, target_id, payload) VALUES ('acme.huge', 'acme', 'project', 1, "
    '\'{"v": 1e400}\')'
)


def run_cli(tokens: list[str]) -> None:
    cli.app(tokens, result_action='return_value', exit_on_error=False)


@pytest.fixture(autouse=True)
def isolated_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Console:
    monkeypatch.setattr(common, 'settings', Settings(home=tmp_path))
    monkeypatch.setattr(project_module, 'settings', common.settings)
    console = Console(record=True, width=80, force_terminal=False)
    monkeypatch.setattr(common, 'console', console)
    return console


@pytest.fixture
def out(isolated_home: Console):
    def read() -> str:
        return isolated_home.export_text(clear=True)

    return read


class Daemon(TypedDict):
    pid: int | None


class StatusDocument(TypedDict):
    protocol: Literal[1]
    cursor: int
    daemon: Daemon
    projects: list[feed.ProjectState]


class InboxDocument(feed.Inbox):
    protocol: Literal[1]
    cursor: int


class EventsDocument(feed.Batch):
    protocol: Literal[1]


class Error(TypedDict):
    code: str
    message: str


class ErrorDocument(TypedDict):
    protocol: Literal[1]
    error: Error


def document[T](shape: type[T], text_out: str) -> T:
    """The one JSON document a ``--json`` call printed, checked against its shape: one line, never wrapped."""
    [line] = text_out.splitlines()
    return TypeAdapter(shape).validate_json(line, strict=True)


def write(db: Database, statement: str) -> None:
    with db.engine.begin() as conn:
        conn.execute(text(statement))


def add_idea(db: Database, project_id: int, title: str) -> int:
    with db.session() as s:
        idea = Idea(project_id=project_id, title=title, hypothesis='it helps', track='score', created_by='human')
        s.add(idea)
        s.commit()
        return idea.id


def event_ids(db: Database, after: int) -> list[int]:
    with db.session() as s:
        return list(s.scalars(select(Event.id).where(Event.id > after).order_by(Event.id)))


def test_status_and_inbox_print_one_json_document_with_the_cursor_they_were_read_at(
    db: Database, project: Project, out
) -> None:
    with db.session() as s:
        s.add(
            Comment(
                target_kind=TargetKind.PROJECT,
                target_id=project.id,
                author='watchdog#2',
                kind=CommentKind.ESCALATION,
                priority=0,
                body='a long body ' * 20,
            )
        )
        ops.pause(s, project.id, evals=True, reason='budget review')
        s.commit()
    end = event_ids(db, 0)[-1]

    run_cli(['status', '--json'])
    status = document(StatusDocument, out())
    assert status['cursor'] == end and status['daemon'] == {'pid': None}
    [state] = status['projects']
    assert state['name'] == 'demo' and state['paused'] == 'all' and state['paused_reason'] == 'budget review'
    assert state['inbox'] == 1 and state['head'] is not None

    run_cli(['inbox', 'demo', '--json', '--kind', 'escalation'])
    inbox = document(InboxDocument, out())
    assert inbox['cursor'] == end and [i['body'] for i in inbox['items']] == ['a long body ' * 20]
    assert inbox['seeds'] == []
    run_cli(['inbox', '--json', '--all', '--kind', 'note'])
    assert document(InboxDocument, out())['items'] == []


def test_events_reads_after_a_cursor_with_filters_and_says_how_to_read_on(
    db: Database, project: Project, out, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = add_idea(db, project.id, 'one')
    write(db, UNKNOWN)
    write(db, BROKEN)
    run_cli(['events', '--json'])
    batch = document(EventsDocument, out())
    kinds = [e['kind'] for e in batch['events']]
    assert kinds == ['acme.observed', 'acme.raw'], 'an idea insert logs no event; unknown kinds pass through'
    assert batch['events'][0]['payload'] == {'latency_ms': 12.5, 'tags': ['a']}
    assert batch['events'][1]['payload'] is None and batch['events'][1]['invalid'] == {'payload': 'oops'}
    end = batch['next']

    run_cli(['events', '--after', '0', '--limit', '1'])
    lines = out().splitlines()
    assert lines[0].startswith('event#1 ') and ' acme.observed project#1 by acme: {"latency_ms": 12.5' in lines[0]
    assert lines[1] == 'next: piloop events --after 1 (more now)'
    run_cli(['events', '--after', '1', '--project', 'demo', '--kind', 'acme.raw', '--kind', 'idea.status'])
    lines = out().splitlines()
    assert lines[0].endswith('acme.raw project#1 by acme: "oops"'), 'a payload that is not JSON prints as stored'
    assert lines[1] == f'next: piloop events --after {end} --project demo --kind acme.raw --kind idea.status'

    monkeypatch.setattr(feed, 'POLL_SECONDS', 0.01)
    threading.Timer(0.3, write, [db, f"UPDATE ideas SET status = 'parked' WHERE id = {first}"]).start()
    run_cli(['events', '--after', str(end), '--follow', '--timeout', '30', '--json', '--kind', 'idea.status'])
    [parked] = document(EventsDocument, out())['events']
    assert parked['payload'] == {'old': 'ready', 'new': 'parked'} and parked['subject']['project'] == project.id
    run_cli(['events', '--after', str(parked['id']), '--follow', '--timeout', '0'])
    assert out().splitlines() == [f'next: piloop events --after {parked["id"]}']


def test_a_payload_that_json_cannot_say_prints_as_stored_text_never_as_infinity(
    db: Database, project: Project, out
) -> None:
    write(db, HUGE)
    run_cli(['events', '--json'])
    text_out = out()
    assert 'Infinity' not in text_out
    [event] = document(EventsDocument, text_out)['events']
    assert event['payload'] is None and event['invalid'] == {'payload': '{"v": 1e400}'}
    run_cli(['events'])
    assert out().splitlines()[0].endswith('acme.huge project#1 by acme: "{\\"v\\": 1e400}"')


def test_a_cursor_ahead_of_the_feed_is_an_error_a_program_can_act_on(db: Database, project: Project, out) -> None:
    with pytest.raises(SystemExit, match='cursor 99 is ahead of the feed, which ends at 0: read a new snapshot'):
        run_cli(['events', '--after', '99', '--json'])
    error = document(ErrorDocument, out())['error']
    assert error['code'] == 'cursor_ahead' and error['message'].startswith('cursor 99 is ahead')
    with pytest.raises(SystemExit, match='cursor 99 is ahead'):
        run_cli(['events', '--after', '99'])
    assert out() == '', 'without --json, the message goes to stderr only'


def controller(home: Path, *args: str) -> subprocess.CompletedProcess[str]:
    """Run ``piloop`` as a separate process on ``home``, as an external controller would."""
    return subprocess.run(
        [sys.executable, '-m', 'piloop', *args],
        env={**os.environ, 'PILOOP_HOME': str(home), 'PILOOP_ACTOR': 'controller'},
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


def one_document[T](shape: type[T], proc: subprocess.CompletedProcess[str]) -> T:
    assert proc.returncode == 0, proc.stderr
    return document(shape, proc.stdout)


def test_a_separate_process_joins_a_snapshot_to_the_feed_and_resumes_after_a_disconnect_without_loss(
    db: Database, project: Project, tmp_path: Path
) -> None:
    before = add_idea(db, project.id, 'before the snapshot')
    with db.session() as s:
        s.get_one(Idea, before).status = IdeaStatus.PARKED
        s.commit()

    snap = one_document(StatusDocument, controller(tmp_path, 'status', 'demo', '--json'))
    cursor = snap['cursor']
    assert cursor == event_ids(db, 0)[-1]
    [state] = snap['projects']
    assert state['ideas']['parked'] == 1, 'the snapshot holds what the events up to its cursor did'

    args = ['events', '--follow', '--timeout', '30', '--json', '--limit', '2']
    with subprocess.Popen(
        [sys.executable, '-m', 'piloop', *args, '--after', str(cursor)],
        env={**os.environ, 'PILOOP_HOME': str(tmp_path)},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as waiting:
        idea = add_idea(db, project.id, 'while it waits')
        with db.session() as s:
            s.get_one(Idea, idea).priority = 0
            s.commit()
        stdout, stderr = waiting.communicate(timeout=60)
    assert waiting.returncode == 0 and stderr == ''
    first = document(EventsDocument, stdout)
    seen = [e['id'] for e in first['events']]
    checkpoint = first['next']
    assert seen and checkpoint == seen[-1]

    # the controller disconnects while it waits; the campaign goes on, with a kind the controller does not know
    with subprocess.Popen(
        [sys.executable, '-m', 'piloop', *args, '--after', str(checkpoint), '--kind', 'never.happens'],
        env={**os.environ, 'PILOOP_HOME': str(tmp_path)},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ) as dropped:
        dropped.kill()
    with db.session() as s:
        ops.pause(s, project.id, evals=False, reason='night')
        s.get_one(Idea, idea).status = IdeaStatus.PARKED
        s.commit()
    write(db, UNKNOWN)
    write(db, BROKEN)

    # it resumes from its checkpoint and reads until it has caught up
    while True:
        batch = one_document(
            EventsDocument, controller(tmp_path, 'events', '--after', str(checkpoint), '--json', '--limit', '2')
        )
        seen += [e['id'] for e in batch['events']]
        checkpoint = batch['next']
        if not batch['more']:
            break
    assert seen == event_ids(db, cursor), 'every event after the snapshot, once each, in order'
    last = one_document(
        EventsDocument, controller(tmp_path, 'events', '--after', str(cursor), '--json', '--limit', '1000')
    )
    kinds = {e['kind']: e for e in last['events']}
    assert {'idea.priority', 'idea.status', 'project.paused', 'acme.observed', 'acme.raw'} <= kinds.keys()
    assert kinds['acme.observed']['payload'] == {'latency_ms': 12.5, 'tags': ['a']}
    assert kinds['acme.raw']['invalid'] == {'payload': 'oops'}

    ahead = controller(tmp_path, 'events', '--after', str(checkpoint + 100), '--json')
    assert ahead.returncode == 1 and 'read a new snapshot' in ahead.stderr
    assert document(ErrorDocument, ahead.stdout)['error']['code'] == 'cursor_ahead'
