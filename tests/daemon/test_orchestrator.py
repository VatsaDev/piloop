"""The orchestrator role: piloop launches it on news, and each agent resumes the last one's session."""

import sys
import threading
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import pytest
from sqlalchemy import select

from piloop.actor import Agent, Person, System
from piloop.agents.results import PromptOutcome, SessionNotResumed, Unresumed
from piloop.api.views import CommentNews, CommentView, StoppedNews, VerdictNews
from piloop.campaign import inbox, ops
from piloop.config import load_campaign_config
from piloop.daemon.scheduler import Scheduler
from piloop.db.events import emit, in_order
from piloop.db.models import AgentSession, AgentSessionState, Campaign, Comment, Event
from piloop.db.session import Database
from piloop.ids import AgentSessionId, CommentId, CommentKind, Ending, RoleName, TargetKind, TargetRef
from piloop.plugins.trigger.news import NewsTaken, Urgency, urgency
from piloop.plugins.verdict_rule.contract import Outcome
from piloop.scoring.ledger import CandidateJudged
from tests.agents.test_resume import HARNESS, SESSION
from tests.conftest import SETTLE, ScriptRuntime, run_all, wait_until, write_toml
from tests.daemon.test_roles import configure, recording, scheduler
from tests.daemon.test_triggers import agent_sessions

ORCHESTRATOR = 'orchestrator'
ANA = Person(name='ana')


def harness(tmp_path: Path, turn: str = '') -> str:
    """Keeps each agent's brief by its actor, and starts or resumes its session as ``test_resume`` does.

    ``turn`` runs in the first turn, after the session starts. Each resumed turn appends its prompt to
    ``prompts-<actor>``.
    """
    lines = SESSION.read_bytes().splitlines(keepends=True)
    (tmp_path / 'head.jsonl').write_bytes(b''.join(lines[:5]))
    (tmp_path / 'rest.jsonl').write_bytes(b''.join(lines[5:]))
    return (
        '#!/bin/sh\n'
        'D="$HOME/claude-home/projects/$(pwd | sed \'s/[^a-zA-Z0-9]/-/g\')"\n'
        'mkdir -p "$D"\n'
        f'cp ../brief.md "{tmp_path}/brief-$PILOOP_ACTOR.md"\n'
        'case "$1" in\n'
        f'  --session-id) cp {tmp_path / "head.jsonl"} "$D/$2.jsonl"; {turn} ;;\n'
        f'  --resume) cat {tmp_path / "rest.jsonl"} >> "$D/$2.jsonl"; printf \'%s\\n\' "$3" >> "{tmp_path}/prompts-$PILOOP_ACTOR" ;;\n'
        'esac\n'
    )


@pytest.fixture
def on(db: Database, campaign: Campaign, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The orchestrator role on, woken at once by urgent news and at most hourly by the rest."""
    monkeypatch.setenv('HOME', str(tmp_path / 'home'))
    turn_on(db, campaign, harness(tmp_path))


def turn_on(db: Database, campaign: Campaign, body: str) -> None:
    write_toml(Path(campaign.path), body)
    path = Path(campaign.path)
    script = {**load_campaign_config(path).harness['script'].model_dump(mode='json', exclude_unset=True), **HARNESS}
    news = {'triggers': [{'provider': 'news', 'quiet_seconds': 0, 'routine_every': 3600}]}
    with db.session() as s:
        cfg = load_campaign_config(path, plugins={'role': {'orchestrator': news}, 'harness': {'script': script}})
        s.get_one(Campaign, campaign.id).config = cfg
        s.commit()


def message(db: Database, campaign: Campaign, body: str) -> CommentId:
    with db.session() as s:
        return inbox.message(s, campaign.id, body, author=ANA, priority=1).id


def brief(tmp_path: Path, agent: AgentSessionId) -> str:
    return (tmp_path / f'brief-orchestrator#{agent}.md').read_text()


def wake(db: Database, runtime: ScriptRuntime, tmp_path: Path) -> list[AgentSessionId]:
    sched = scheduler(db, recording(runtime, []), tmp_path)
    launched = [a for a in run_all(sched) if kind(db, a) == ORCHESTRATOR]
    sched.wait(SETTLE)
    sched.stop(timeout=5)
    return launched


def kind(db: Database, agent: AgentSessionId) -> str:
    with db.session() as s:
        return s.get_one(AgentSession, agent).kind


def taken(db: Database, agent: AgentSessionId) -> NewsTaken:
    """The news that the one note on ``agent`` took."""
    with db.session() as s:
        on_it = select(Comment.id).where(Comment.target == TargetRef(TargetKind.AGENT_SESSION, agent))
        [(_, event)] = in_order(s, NewsTaken, Event.target_kind == TargetKind.COMMENT, Event.target_id.in_(on_it))
        return event


def test_news_launches_the_orchestrator_which_resumes_its_session_and_never_wakes_on_its_own_writes(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, on: None
) -> None:
    assert wake(db, runtime, tmp_path) == [], 'no news yet'
    first_message = message(db, campaign, 'how is it going?')
    [first] = wake(db, runtime, tmp_path)
    assert brief(tmp_path, first).count('by human:ana: how is it going?') == 1, 'in its notes, and only there'
    assert '\n# Notes\nNews for you:\n- ' in brief(tmp_path, first)
    assert brief(tmp_path, first).startswith('# Orchestrator'), 'the whole brief: nothing to resume yet'
    assert taken(db, first).after == 0
    assert outcomes(db, first) == ['comment.delivered'], 'its brief delivered the note'

    with db.session() as s:
        s.add(
            Comment(
                target=TargetRef(TargetKind.CAMPAIGN, campaign.id),
                author=Agent(role=ORCHESTRATOR, session=first).signature,
                kind=CommentKind.DECISION,
                priority=3,
                body='[process, reversible] nothing to change',
            )
        )
        inbox.ack(s, [first_message], Agent(role=ORCHESTRATOR, session=first))
    assert wake(db, runtime, tmp_path) == [], 'its own decision is for the human, not news for the next orchestrator'

    message(db, campaign, 'and now?')
    [second] = wake(db, runtime, tmp_path)
    text = brief(tmp_path, second)
    assert text.startswith(f'You continue this session as a new agent, orchestrator#{second}.')
    assert 'and now?' in text and 'how is it going?' not in text, 'only the news after what the last one took'
    assert taken(db, second).after == taken(db, first).upto
    with db.session() as s:
        row = s.get_one(AgentSession, second)
        assert (row.state, row.session_id) == (AgentSessionState.EXITED, s.get_one(AgentSession, first).session_id)
        on_second = Event.target == TargetRef(TargetKind.AGENT_SESSION, second)
        assert in_order(s, SessionNotResumed, on_second) == []


HOOK = (
    'import os, pathlib, sys\n'
    'from piloop.plugins.runtime.contract import take\n'
    "with open(sys.argv[1], 'a') as out:\n"
    "    out.writelines(f'{m}\\n' for m in take(pathlib.Path(os.environ['PILOOP_MESSAGES'])))\n"
)
"""Claude Code's ``PostToolUse`` hook, as ``piloop agent messages`` runs it: it takes the queue into the turn."""


@contextmanager
def serving(db: Database, runtime: ScriptRuntime, tmp_path: Path) -> Generator[Scheduler]:
    """The daemon's loop, in a thread: it reacts to the feed as ``piloop serve`` does."""
    sched = scheduler(db, recording(runtime, []), tmp_path)
    thread = threading.Thread(target=sched.serve)
    thread.start()
    yield sched
    sched.stop(timeout=10)
    thread.join(timeout=10)


def outcomes(db: Database, agent: AgentSessionId) -> list[str]:
    with db.session() as s:
        return [
            o.kind for _, o in in_order(s, PromptOutcome, Event.target == TargetRef(TargetKind.AGENT_SESSION, agent))
        ]


def notes(db: Database, agent: AgentSessionId) -> list[str]:
    with db.session() as s:
        on_it = Comment.target == TargetRef(TargetKind.AGENT_SESSION, agent)
        return list(s.scalars(select(Comment.body).where(on_it, Comment.kind == CommentKind.NOTE)))


@pytest.mark.parametrize('hook', [True, False], ids=['in its turn, through the hook', 'when its turn ends'])
def test_news_that_comes_while_a_wake_runs_reaches_that_wake_once(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, on: None, hook: bool
) -> None:
    hold, started, prompts = tmp_path / 'hold', tmp_path / 'started', tmp_path / 'prompts-orchestrator#1'
    hold.touch()
    (tmp_path / 'hook.py').write_text(HOOK)
    take = f'[ -s "$PILOOP_MESSAGES" ] && "{sys.executable}" {tmp_path / "hook.py"} "{prompts}"; ' if hook else ''
    turn_on(db, campaign, harness(tmp_path, f'touch {started}; while [ -e {hold} ]; do {take}sleep 0.05; done'))
    message(db, campaign, 'how is it going?')
    with serving(db, runtime, tmp_path):
        wait_until(started.exists, 'the wake')
        first = AgentSessionId(agent_sessions(db, ORCHESTRATOR)[0][0])
        message(db, campaign, 'stop the lzma runs')
        wait_until(lambda: outcomes(db, first)[1:] == ['comment.queued'], 'the second note in its queue')
        if hook:
            wait_until(lambda: prompts.is_file() and 'stop the lzma runs' in prompts.read_text(), 'the hook')
        hold.unlink()
        wait_until(lambda: agent_sessions(db, ORCHESTRATOR)[0][1] is AgentSessionState.EXITED, 'its end')
    [_, note] = notes(db, first)
    assert note.startswith('News for you:\n- ') and 'by human:ana: stop the lzma runs' in note
    assert prompts.read_text().count('stop the lzma runs') == 1
    assert outcomes(db, first) == ['comment.delivered', 'comment.queued', 'comment.delivered']
    assert wake(db, runtime, tmp_path) == [], 'the wake took it: the next one does not take it again'


def test_news_that_comes_while_a_wake_is_requested_goes_on_that_wake_and_its_brief_lists_it_once(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, on: None
) -> None:
    with db.session() as s:
        requested = ops.request_agent(s, campaign.id, RoleName(ORCHESTRATOR), harness=None, model=None, reason='a look')
        s.commit()
        requested_id = requested.id
    message(db, campaign, 'stop the lzma runs')
    assert wake(db, runtime, tmp_path) == [requested_id], 'the note goes on the waiting row: no second one'
    assert brief(tmp_path, requested_id).count('stop the lzma runs') == 1
    assert outcomes(db, requested_id) == ['comment.delivered'], 'its brief delivered it: it is not sent again'
    assert wake(db, runtime, tmp_path) == []


def test_routine_news_waits_for_its_interval_and_only_urgent_news_wakes_a_paused_campaign(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, on: None
) -> None:
    def judged() -> None:
        with db.session() as s:
            verdict = CandidateJudged(verdict=Outcome.KEEP, score=2.0, reason='score +1')
            emit(s, verdict, TargetRef(TargetKind.CANDIDATE, 1), System.DAEMON)
            s.commit()

    judged()
    [first] = wake(db, runtime, tmp_path)
    assert 'candidate#1 keep: score +1' in brief(tmp_path, first), 'the first routine news wakes it'
    judged()
    assert wake(db, runtime, tmp_path) == [], 'the next routine news waits for routine_every'
    with db.session() as s:
        ops.pause(s, campaign.id, evals=False, reason='a person reads the results')
        s.commit()
    assert wake(db, runtime, tmp_path) == []
    message(db, campaign, 'resume it when the head is measured')
    [paused] = wake(db, runtime, tmp_path)
    assert 'resume it when the head is measured' in brief(tmp_path, paused)
    assert [state for _, state, _ in agent_sessions(db, ORCHESTRATOR)] == [AgentSessionState.EXITED] * 2


def test_a_failed_orchestrator_leaves_its_news_to_the_next_and_off_is_the_default(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, on: None
) -> None:
    message(db, campaign, 'are you there?')
    with db.session() as s:
        failed = AgentSession(kind=ORCHESTRATOR, campaign_id=campaign.id, state=AgentSessionState.KILLED)
        s.add(failed)
        s.flush()
        on_failed = TargetRef(TargetKind.AGENT_SESSION, failed.id)
        note = Comment(target=on_failed, author='daemon', kind=CommentKind.NOTE, body='News for you:\n- are you there?')
        s.add(note)
        s.flush()
        emit(s, NewsTaken(after=0, upto=10_000), TargetRef(TargetKind.COMMENT, note.id), System.DAEMON)
        emit(s, PromptOutcome(kind='comment.delivered', comment=note.id), on_failed, System.DAEMON)
        s.commit()
    [next_one] = wake(db, runtime, tmp_path)
    assert 'are you there?' in brief(tmp_path, next_one) and taken(db, next_one).after == 0
    with db.session() as s:
        on_it = Event.target == TargetRef(TargetKind.AGENT_SESSION, next_one)
        assert [e.reason for _, e in in_order(s, SessionNotResumed, on_it)] == [Unresumed.NO_SESSION]

    configure(db, campaign, harness(tmp_path))
    message(db, campaign, 'anyone?')
    assert wake(db, runtime, tmp_path) == [], 'without triggers, piloop launches no orchestrator'


def test_one_launched_by_hand_gets_no_notes_and_a_live_one_that_ends_before_it_reads_its_note_leaves_the_news_to_the_next(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, on: None
) -> None:
    with db.session() as s:
        by_hand = ops.request_agent(s, campaign.id, RoleName(ORCHESTRATOR), harness=None, model=None, reason='a look')
        s.commit()
        by_hand_id = by_hand.id
    assert wake(db, runtime, tmp_path) == [by_hand_id]
    assert '\n# Notes\n' not in brief(tmp_path, by_hand_id), 'no news: it reads the inbox, as its prompt says'

    with db.session() as s:
        live = AgentSession(kind=ORCHESTRATOR, campaign_id=campaign.id, state=AgentSessionState.RUNNING)
        s.add(live)
        s.commit()
        live_id = live.id
    message(db, campaign, 'one at a time')
    assert wake(db, runtime, tmp_path) == [], 'not a second one: the live one gets a note'
    [note] = notes(db, live_id)
    assert 'one at a time' in note
    with db.session() as s:
        s.get_one(AgentSession, live_id).state = AgentSessionState.EXITED
        s.commit()
    [after] = wake(db, runtime, tmp_path)
    assert 'one at a time' in brief(tmp_path, after), 'it ended before it read the note'
    with db.session() as s:
        ops.stop(s, campaign.id, System.HUMAN, reason='done', ending=Ending.WRAP_UP)
        s.commit()
    woken = wake(db, runtime, tmp_path) + wake(db, runtime, tmp_path)
    assert any('campaign demo stopped' in brief(tmp_path, a) for a in woken), 'the stop wakes it to report'


def test_messages_escalations_and_the_stop_are_urgent_and_the_rest_is_routine() -> None:
    def comment(kind: str) -> CommentNews:
        view = CommentView(
            id=CommentId(1), kind=kind, target='campaign#1', in_reply_to=None, author='human', created='now',
            priority=1, acked_at=None, acked_by=None, body='b',
        )  # fmt: skip
        return CommentNews(comment=view, campaign_name='demo')

    assert [urgency(comment(k)) for k in ('message', 'escalation', 'finding')] == [
        Urgency.NOW,
        Urgency.NOW,
        Urgency.ROUTINE,
    ]
    assert urgency(StoppedNews(campaign_name='demo', reason='done')) is Urgency.NOW
    assert urgency(VerdictNews(candidate=1, verdict='keep', reason='r')) is Urgency.ROUTINE
