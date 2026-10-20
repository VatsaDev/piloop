"""The public read side: the event feed after a cursor, snapshots with their cursor, and the event envelope."""

import json
import sqlite3
import threading
from pathlib import Path

import pytest
from sqlalchemy import func, select, text, update

from piloop.campaign import ops
from piloop.db.events import emit
from piloop.db.models import (
    Campaign,
    Candidate,
    Comment,
    CommentKind,
    Event,
    Idea,
    IdeaStatus,
    Run,
    TargetKind,
    Verdict,
    Worker,
    WorkerState,
)
from piloop.db.session import Database
from piloop.reports import feed

RAW = "INSERT INTO events (kind, actor, target_kind, target_id, payload) VALUES ('{kind}', 'plugin', '{target}', {id}, '{payload}')"


def raw(db: Database, kind: str, payload: str, target: str = 'campaign', target_id: int = 1) -> int:
    """Insert an event the way an agent's ``piloop sql`` can: any kind, any payload text."""
    with db.engine.begin() as conn:
        return conn.execute(text(RAW.format(kind=kind, target=target, id=target_id, payload=payload))).lastrowid or 0


def make_idea(campaign_id: int, title: str, **fields: object) -> Idea:
    return Idea(
        campaign_id=campaign_id,
        title=title,
        hypothesis=f'{title} helps',
        track='score',
        **{'created_by': 'human', **fields},
    )


def newest(db: Database) -> int:
    with db.session() as s:
        return s.scalar(select(func.max(Event.id))) or 0


def test_a_read_transaction_sees_one_state_and_refuses_unflushed_changes(db: Database, campaign: Campaign) -> None:
    with db.session() as s, db.session() as other:
        with feed.snapshot(s) as cursor:
            emit(other, 'run.started', TargetKind.CAMPAIGN, campaign.id)
            other.commit()
            assert feed.newest(s) == cursor, 'a commit after the first read is not seen'
        assert feed.newest(s) == cursor + 1, 'the transaction ended: the commit is seen'
        s.add(make_idea(campaign.id, 'unsaved'))
        with pytest.raises(RuntimeError, match='uncommitted changes'), feed.snapshot(s):
            pass


def test_the_envelope_normalizes_history_without_inventing_and_keeps_unknown_kinds_opaque(
    db: Database, campaign: Campaign
) -> None:
    with db.session() as s:
        idea = make_idea(campaign.id, 'try it')
        s.add(idea)
        s.flush()
        s.add(Comment(target_kind=TargetKind.COMMENT, target_id=999, author='human', kind=CommentKind.NOTE, body='x'))
        s.commit()
        idea.status = IdeaStatus.PARKED
        s.commit()
        ops.pause(s, campaign.id, evals=False, reason='a quiet night')
        s.commit()
        idea_id = idea.id
    unknown = raw(db, 'myplugin.observed', '{"nested": {"a": [1, 2]}}')
    scalar = raw(db, 'myplugin.count', '7')
    broken = raw(db, 'myplugin.broken', 'not json {')
    with db.session() as s:
        batch = feed.read(s, 0)
    by_id = {e['id']: e for e in batch['events']}
    status = next(e for e in batch['events'] if e['kind'] == 'idea.status')
    assert status == {
        'id': status['id'],
        'kind': 'idea.status',
        'subject': {'kind': 'idea', 'id': idea_id, 'campaign': campaign.id},
        'actor': 'daemon',
        'time': status['time'],
        'payload': {'old': 'ready', 'new': 'parked'},
    }, 'its payload as stored, its campaign resolved from its subject'
    assert (at := status['time']) is not None and at.endswith('Z') and 'T' in at
    paused = next(e for e in batch['events'] if e['kind'] == 'campaign.paused')
    assert paused['subject'] == {'kind': 'campaign', 'id': campaign.id, 'campaign': campaign.id}
    assert paused['payload'] == {'old': None, 'new': 'launches', 'paused_reason': 'a quiet night'}
    reply = next(e for e in batch['events'] if e['kind'] == 'comment.note')
    assert reply['subject'] == {'kind': 'comment', 'id': reply['subject']['id'], 'campaign': None}, (
        'a subject with no campaign says so; nothing is guessed'
    )
    assert by_id[unknown]['payload'] == {'nested': {'a': [1, 2]}}
    assert by_id[scalar]['payload'] == 7, 'any JSON value passes through'
    assert by_id[broken]['payload'] is None and by_id[broken]['invalid'] == {'payload': 'not json {'}, (
        'a payload that is not JSON is shown as stored, not dropped and not a failure'
    )
    assert 'invalid' not in by_id[unknown]
    json.dumps(batch, allow_nan=False)


def test_a_row_that_an_older_piloop_let_through_never_stops_the_reader(db: Database, campaign: Campaign) -> None:
    start = newest(db)
    with sqlite3.connect(db.engine.url.database or '', isolation_level=None) as con:  # no checks on this connection
        for row in (
            "('a.time', 'x', 'campaign', 1, 'not a date', '{}')",
            "('a.number', 'x', 'campaign', 1, 2460000, '{}')",
            "(x'622e626c6f62', x'78', 'campaign', 1, '2026-10-20 08:00:00', '{}')",
            "('a.huge', 'x', 'campaign', 1, '2026-10-20 08:00:00+02:00', '{\"v\": 1e400}')",
            "('a.nan', 'x', 'campaign', 1, '2026-10-20 08:00:00', '{\"v\": NaN}')",
            "('a.blob', 'x', 'campaign', 1, '2026-10-20 08:00:00', x'00ff')",
        ):
            con.execute(f'INSERT INTO events (kind, actor, target_kind, target_id, at, payload) VALUES {row}')
    con.close()
    with db.session() as s:
        batch = feed.read(s, start)
    assert [(e['kind'], e['time'], e['payload'], e.get('invalid')) for e in batch['events']] == [
        ('a.time', None, {}, {'time': 'not a date'}),
        ('a.number', None, {}, {'time': '2460000'}),
        ('b.blob', '2026-10-20T08:00:00Z', {}, None),
        ('a.huge', '2026-10-20T06:00:00Z', None, {'payload': '{"v": 1e400}'}),
        ('a.nan', '2026-10-20T08:00:00Z', None, {'payload': '{"v": NaN}'}),
        ('a.blob', '2026-10-20T08:00:00Z', None, {'payload': "X'00FF'"}),
    ], 'each field that does not read is null, with its stored text under invalid; the next rows still read'
    assert batch['events'][2]['actor'] == 'x'
    json.dumps(batch, allow_nan=False)


def test_a_bounded_read_advances_its_cursor_over_what_it_examined(db: Database, campaign: Campaign) -> None:
    start = newest(db)
    ids = [raw(db, f'k.{n}', '{}') for n in range(5)]
    with db.session() as s:
        first = feed.read(s, start, limit=2)
        assert [e['id'] for e in first['events']] == ids[:2] and first['next'] == ids[1] and first['more']
        rest = feed.read(s, first['next'], limit=10)
        assert [e['id'] for e in rest['events']] == ids[2:] and rest['next'] == ids[-1] and not rest['more']
        none = feed.read(s, start, kinds=['no.such.kind'])
        assert none == {'events': [], 'next': ids[-1], 'more': False}, 'a filter that matches nothing still advances'
        some = feed.read(s, start, kinds=['k.1', 'k.3'], limit=1)
        assert [e['id'] for e in some['events']] == [ids[1]] and some['next'] == ids[1] and some['more']
        again = feed.read(s, some['next'], kinds=['k.1', 'k.3'], limit=1)
        assert [e['id'] for e in again['events']] == [ids[3]]
        assert feed.read(s, ids[-1]) == {'events': [], 'next': ids[-1], 'more': False}


def test_a_campaign_filter_follows_each_subject_to_its_campaign(
    db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    with db.session() as s:
        other = Campaign(name='other', path=str(tmp_path / 'other'), config=campaign.config)
        s.add(other)
        s.flush()
        mine = make_idea(campaign.id, 'mine')
        theirs = make_idea(other.id, 'theirs')
        s.add_all([mine, theirs])
        s.flush()
        candidate = s.scalars(select(Candidate).where(Candidate.campaign_id == campaign.id)).one()
        run = Run(idea_id=mine.id, attempt=1, base_candidate_id=candidate.id, harness='script', model='m')
        s.add(run)
        s.flush()
        worker = Worker(kind='worker', campaign_id=campaign.id, run_id=run.id)
        s.add(worker)
        s.flush()
        start = s.scalar(select(func.max(Event.id))) or 0
        for kind, target in (
            (TargetKind.IDEA, mine.id),
            (TargetKind.IDEA, theirs.id),
            (TargetKind.RUN, run.id),
            (TargetKind.WORKER, worker.id),
            (TargetKind.CANDIDATE, candidate.id),
            (TargetKind.CAMPAIGN, other.id),
        ):
            emit(s, 'x.y', kind, target)
        s.commit()
        batch = feed.read(s, start, campaign_id=campaign.id)
        assert [(e['subject']['kind'], e['subject']['campaign']) for e in batch['events']] == [
            ('idea', campaign.id),
            ('run', campaign.id),
            ('worker', campaign.id),
            ('candidate', campaign.id),
        ]
        assert batch['next'] == start + 6, 'the events of the other campaign were examined, and passed over'

        gone = make_idea(campaign.id, 'gone')
        s.add(gone)
        s.commit()
        gone.status = IdeaStatus.PARKED
        s.commit()
        s.delete(gone)
        s.commit()
        late = feed.read(s, batch['next'], campaign_id=other.id)
    assert [(e['kind'], e['subject']['campaign']) for e in late['events']] == [
        ('idea.status', None),
        ('idea.deleted', None),
    ], 'once its row is gone, an event has no campaign, and every campaign filter keeps it: nothing is lost'


def test_a_cursor_ahead_of_the_feed_asks_for_a_new_snapshot_and_bad_bounds_are_refused(
    db: Database, campaign: Campaign
) -> None:
    end = newest(db)
    with db.session() as s:
        with pytest.raises(feed.CursorAhead, match=f'cursor {end + 5} is ahead of the feed, which ends at {end}'):
            feed.read(s, end + 5)
        with pytest.raises(ValueError, match='cursor must be 0 or more'):
            feed.read(s, -1)
        for limit in (0, feed.MAX_LIMIT + 1):
            with pytest.raises(ValueError, match=f'limit must be from 1 to {feed.MAX_LIMIT}'):
                feed.read(s, 0, limit=limit)


def test_follow_wakes_on_a_matching_event_and_otherwise_returns_at_its_timeout(
    db: Database, campaign: Campaign
) -> None:
    start = newest(db)
    noise = raw(db, 'noise', '{}')
    with db.session() as s:
        quiet = feed.follow(s, start, kinds=['wanted'], timeout=0, poll=0.01)
        assert quiet == {'events': [], 'next': noise, 'more': False}, 'the timeout still hands back the advanced cursor'
        threading.Timer(0.2, raw, [db, 'wanted', '{}']).start()
        woke = feed.follow(s, quiet['next'], kinds=['wanted'], timeout=30, poll=0.01)
        assert [e['kind'] for e in woke['events']] == ['wanted']


def escalation(campaign_id: int, body: str) -> Comment:
    return Comment(
        target_kind=TargetKind.CAMPAIGN,
        target_id=campaign_id,
        author='watchdog#2',
        kind=CommentKind.ESCALATION,
        priority=1,
        body=body,
    )


def test_snapshots_carry_the_cursor_they_were_read_at(db: Database, campaign: Campaign) -> None:
    with db.session() as s, db.session() as other:
        idea = make_idea(campaign.id, 'queued')
        seed = make_idea(campaign.id, 'a hunch', created_by='scout#3', status=IdeaStatus.SEED, provenance='a paper')
        s.add_all([idea, seed])
        s.flush()
        candidate = s.scalars(select(Candidate).where(Candidate.campaign_id == campaign.id)).one()
        run = Run(idea_id=idea.id, attempt=1, base_candidate_id=candidate.id, harness='script', model='m')
        s.add(run)
        s.flush()
        worker = Worker(
            kind='worker', campaign_id=campaign.id, run_id=run.id, state=WorkerState.RUNNING, harness='script'
        )
        s.add_all([worker, Worker(kind='watchdog', campaign_id=campaign.id, state=WorkerState.EXITED)])
        s.add(escalation(campaign.id, 'look'))
        s.commit()
        ids = idea.id, seed.id, candidate.id, run.id, worker.id
        with feed.snapshot(s) as cursor:
            other.add(escalation(campaign.id, 'after the cursor'))
            other.commit()
            [state] = feed.status_view(s, [s.get_one(Campaign, campaign.id)])
            inbox = feed.inbox_view(s)
        assert feed.read(s, cursor)['events'][0]['kind'] == 'comment.escalation', 'what the snapshot missed follows it'
    idea_id, seed_id, candidate_id, run_id, worker_id = ids
    assert state == {
        'id': campaign.id,
        'name': 'demo',
        'path': campaign.path,
        'status': 'active',
        'stopped_reason': None,
        'paused': None,
        'paused_reason': None,
        'evaluator': campaign.evaluator_id,
        'head': {
            'candidate': candidate_id,
            'tracks': [{'track': 'score', 'metric': 'score', 'value': 1.0, 'delta': None}],
        },
        'kept': 1,
        'candidates': 1,
        'ideas': {**{str(v): 0 for v in IdeaStatus}, 'seed': 1, 'ready': 1},
        'runnable': 1,
        'runs': [
            {'id': run_id, 'idea': idea_id, 'title': 'queued', 'harness': 'script', 'model': 'm', 'status': 'queued'}
        ],
        'agents': [
            {'id': worker_id, 'kind': 'worker', 'state': 'running', 'harness': 'script', 'model': None, 'run': run_id}
        ],
        'inbox': 1,
        'tokens': 0,
    }, 'the escalation written after the cursor is not in the snapshot'
    [item] = inbox['items']
    assert item['kind'] == 'escalation' and item['body'] == 'look' and item['campaign'] == campaign.id
    assert item['target'] == {'kind': 'campaign', 'id': campaign.id} and item['acked_at'] is None
    assert item['priority'] == 1 and item['created'].endswith('Z')
    assert inbox['seeds'] == [
        {'idea': seed_id, 'campaign': campaign.id, 'created_by': 'scout#3', 'title': 'a hunch', 'provenance': 'a paper'}
    ]
    json.dumps([state, inbox])


def test_a_campaign_without_a_keep_has_no_head(db: Database, campaign: Campaign) -> None:
    with db.session() as s:
        s.execute(update(Candidate).values(verdict=Verdict.DISCARD))
        s.commit()
        [state] = feed.status_view(s, [s.get_one(Campaign, campaign.id)])
    assert state['head'] is None and state['kept'] == 0
