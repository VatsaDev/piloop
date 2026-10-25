"""The events that each release wrote load as the models of today, after the migrations, and none is lost.

Each fixture is a database at the head of a release with one row of every shape that the release wrote: by ``emit``,
by its triggers, and by its migrations. The shapes come from that release's code. After ``alembic upgrade head``
every row validates strictly as the model that registers its kind. Only a kind that no piloop module registers any
more loads as an ``UnknownEvent``.
"""

import importlib
import json
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest
from alembic import command
from inline_snapshot import snapshot
from sqlalchemy import select

from piloop.actor import System
from piloop.db.models import EVENT, Event, EventModel, UnknownEvent
from piloop.db.requests import RequestAnswered
from piloop.db.session import Database
from tests.db.test_migrations import alembic

type Row = tuple[str, str, int, str, dict[str, object]]
"""``kind``, ``target_kind``, ``target_id``, ``actor`` and ``payload``, as the release stored them."""


LINE = '- [incident] {key}, 4m ago: failed on claude (idea#12): agent exit 1'
V010_SETUP = (
    "INSERT INTO projects (name, path, config) VALUES ('p', '/p', '{}')",
    "INSERT INTO workers (kind, project_id) VALUES ('worker', 1), ('watchdog', 1), ('research', 1)",
)
V010: list[Row] = [
    ('worker.started', 'worker', 1, 'daemon', {'harness': 'claude', 'model': 'opus', 'reason': 'idea#12 is ready'}),
    ('worker.finished', 'worker', 1, 'daemon', {'reason': 'idea#12 is ready', 'status': 'idle', 'error': None}),
    ('watchdog.finished', 'worker', 2, 'daemon', {'reason': 'anomaly', 'status': 'blocked', 'error': 'dialog'}),
    ('scout.requested', 'worker', 3, 'cli', {}),
    ('research.started', 'worker', 3, 'daemon', {'harness': 'pi', 'model': 'm', 'reason': None}),
    (
        'anomaly.startup',
        'worker',
        2,
        'daemon',
        {'kind': 'worker', 'harness': 'codex', 'run': 36, 'seconds': 41.3, 'error': None},
    ),
    ('project.added', 'project', 1, 'cli', {'baseline': 1}),
    ('idea.created', 'idea', 1, 'human', {}),
    ('run.started', 'run', 1, 'daemon', {'harness': 'claude', 'model': 'opus'}),
    ('run.relaunched', 'run', 1, 'daemon', {'worker': 14, 'launches': 1}),
    (
        'measurement.recorded',
        'measurement',
        1,
        'daemon',
        {'candidate': 7, 'metrics': {'accuracy': 0.85}, 'error': None, 'run': 4},
    ),
    ('candidate.judged', 'candidate', 7, 'daemon', {'verdict': 'keep', 'score': 0.85, 'reason': 'beats candidate#3'}),
    ('run.finished', 'run', 1, 'daemon', {'status': 'succeeded', 'error': None}),
    ('run.finished', 'run', 2, 'daemon', {'status': 'failed', 'error': 'campaign stopped'}),
    ('run.finished', 'run', 3, 'daemon', {'status': 'failed', 'error': 'blocked on escalation comment#7'}),
    ('run.finished', 'run', 4, 'daemon', {'status': 'failed', 'error': 'agent exit 1'}),
    ('run.finished', 'run', 5, 'daemon', {'status': 'timed_out', 'error': None}),
    ('comment.undelivered', 'worker', 1, 'daemon', {'comment': 88, 'error': 'NotSteerable: headless'}),
    ('comment.delivered', 'worker', 1, 'daemon', {'comment': 89}),
    ('worker.wrap_up', 'worker', 1, 'daemon', {'reason': 'target reached'}),
    ('worker.wrap_up', 'worker', 1, 'daemon', {'reason': 'target reached', 'error': 'pane closed'}),
    (
        'anomaly.reported',
        'worker',
        2,
        'daemon',
        {'key': 'worker#5 dead at startup', 'anomaly': LINE.format(key='worker#5 dead at startup')},
    ),
    ('anomaly.reported', 'worker', 2, 'daemon', {'key': 'run#36 failed', 'anomaly': LINE.format(key='run#36 failed')}),
    (
        'run.queued',
        'run',
        1,
        'daemon',
        {'idea': 12, 'attempt': 2, 'harness': 'claude', 'track': 'accuracy', 'pinned_base': 9, 'computed_base': 11},
    ),
    ('run.queued', 'run', 2, 'daemon', {'idea': 13, 'attempt': 1, 'harness': 'pi', 'track': 'a', 'priority_pick': 4}),
    ('run.rerouted', 'run', 1, 'daemon', {'from': 'pi', 'to': 'claude'}),
    ('launch.paused', 'project', 1, 'daemon', {'comment': 90, 'deaths': 3, 'load': '7.9 / 6.1 / 4.0'}),
    ('launch.resumed', 'project', 1, 'daemon', {'comment': 90}),
    ('candidate.rebased', 'candidate', 7, 'daemon', {'head': 11, 'rebased': 42}),
    ('candidate.rebased', 'candidate', 8, 'daemon', {'head': 11, 'conflicts': ['a.py']}),
    ('candidate.imported', 'candidate', 9, 'human', {'ref': 'main', 'revision': '3f2a' * 10}),
    ('idea.status', 'idea', 1, 'orchestrator', {'old': 'ready', 'new': 'parked'}),
    ('idea.priority', 'idea', 1, 'human', {'old': 2, 'new': 0}),
    ('idea.track', 'idea', 1, 'human', {'old': 'a', 'new': 'b'}),
    (
        'candidate.verdict',
        'candidate',
        7,
        'human',
        {'old': 'pending', 'new': 'reference', 'verdict_by': 'human', 'verdict_reason': None},
    ),
    ('run.brief', 'run', 1, 'human', {'old_length': None, 'new_length': 12}),
    ('project.status', 'project', 1, 'daemon', {'old': 'active', 'new': 'stopped', 'stopped_reason': 'target'}),
    ('comment.acked_at', 'comment', 1, 'human', {'old': None, 'new': '2026-10-01 12:00:00', 'acked_by': 'human'}),
    ('idea.deleted', 'idea', 2, 'human', {'title': 't', 'status': 'seed', 'hypothesis': 'h'}),
    (
        'comment.deleted',
        'comment',
        2,
        'human',
        {'target_kind': 'worker', 'target_id': 1, 'author': 'me', 'kind': 'note', 'body': 'hi'},
    ),
    ('candidate.deleted', 'candidate', 3, 'human', {'tree_sha': 't', 'revision': 'c', 'verdict': 'crash'}),
    ('comment.note', 'comment', 3, 'worker#1', {'target_kind': 'project', 'target_id': 1}),
    ('comment.finding', 'comment', 4, 'worker#1', {'target_kind': 'run', 'target_id': 1}),
    ('comment.decision', 'comment', 5, 'orchestrator', {'target_kind': 'idea', 'target_id': 1}),
    ('comment.escalation', 'comment', 6, 'worker#1', {'target_kind': 'worker', 'target_id': 1}),
]
"""Every shape that 0.1.0 wrote."""

V020_SETUP = (
    "INSERT INTO campaigns (name, path, config) VALUES ('p', '/p', '{}')",
    "INSERT INTO agent_sessions (kind, campaign_id) VALUES ('implementer', 1), ('watchdog', 1), ('scout', 1)",
)
V020: list[Row] = [
    ('implementer.started', 'agent_session', 1, 'daemon', {'harness': 'claude', 'model': 'opus', 'reason': None}),
    ('implementer.finished', 'agent_session', 1, 'daemon', {'reason': None, 'status': 'exited', 'error': None}),
    ('scout.finished', 'agent_session', 3, 'daemon', {'reason': 'cron', 'status': 'timed_out', 'error': 'timeout'}),
    (
        'anomaly.startup',
        'agent_session',
        2,
        'daemon',
        {'kind': 'watchdog', 'harness': 'codex', 'run': None, 'seconds': 3.1, 'error': 'exit 1'},
    ),
    ('campaign.added', 'campaign', 1, 'cli', {'baseline': 1}),
    (
        'measurement.recorded',
        'measurement',
        2,
        'human',
        {'candidate': 7, 'metrics': {'accuracy': 0.9}, 'error': None, 'run': None},
    ),
    ('candidate.rejudge_requested', 'candidate', 7, 'human', {'measurement': 2}),
    ('campaign.stop_requested', 'campaign', 1, 'human', {'reason': '', 'kill': True}),
    ('campaign.stop_requested', 'campaign', 1, 'human', {'reason': 'enough', 'kill': False}),
    ('run.cancel_requested', 'run', 1, 'human', {'wrap_up': True}),
    ('agent_session.cancel_requested', 'agent_session', 2, 'human', {'wrap_up': False}),
    (
        'anomaly.disk',
        'campaign',
        1,
        'daemon',
        {'needed': 12000, 'free': 100, 'reserve': 1000, 'idea': None, 'retry_seconds': 600.0},
    ),
    ('run.relaunched', 'run', 1, 'daemon', {'agent_session': 4, 'launches': 2}),
    ('candidate.rejudge_failed', 'candidate', 7, 'daemon', {'error': 'GitError: gone'}),
    ('candidate.judged', 'candidate', 8, 'daemon', {'verdict': 'unmeasured', 'score': None, 'reason': 'skipped'}),
    ('run.finished', 'run', 1, 'daemon', {'status': 'failed', 'error': 'cancelled'}),
    ('idea.recovery_requeued', 'idea', 1, 'daemon', {'run': 1, 'candidate': 9}),
    ('agent_session.jobs_left', 'agent_session', 1, 'daemon', {'jobs': ['12 python train.py']}),
    ('agent_session.jobs_left', 'agent_session', 1, 'daemon', {'jobs': ['12 python train.py'], 'error': 'closed'}),
    ('agent_session.wrap_up', 'agent_session', 1, 'daemon', {'reason': 'cancelled by human'}),
    ('agent_session.stop_ended', 'agent_session', 1, 'daemon', {'reason': 'cancelled by human', 'error': 'cancelled'}),
    ('agent_session.stop_ended', 'agent_session', 1, 'daemon', {'reason': 'target', 'error': 'campaign stopped'}),
    (
        'anomaly.reported',
        'agent_session',
        2,
        'daemon',
        {
            'key': 'agent_session#1 silent while its jobs run',
            'anomaly': LINE.format(key='agent_session#1 silent while its jobs run'),
        },
    ),
    ('candidate.remeasure_requested', 'candidate', 7, 'human', {}),
    ('idea.base_candidate_id', 'idea', 1, 'human', {'old': None, 'new': 7}),
    ('idea.linked', 'idea', 1, 'human', {'target': 2, 'kind': 'after'}),
    ('idea.unlinked', 'idea', 1, 'human', {'target': 2, 'kind': 'supersedes'}),
    ('campaign.type', 'campaign', 1, 'human', {'old': 'optimization', 'new': 'research'}),
    ('campaign.parent_id', 'campaign', 1, 'human', {'old': None, 'new': 2}),
    ('campaign.paused', 'campaign', 1, 'human', {'old': None, 'new': 'launches', 'paused_reason': 'gpu'}),
    (
        'candidate.verdict',
        'candidate',
        8,
        'daemon',
        {'old': 'pending', 'new': 'recovered', 'verdict_by': 'daemon', 'verdict_reason': 'recovered'},
    ),
]
"""Every shape that 0.2.0 wrote and 0.1.0 did not, or wrote differently."""

V030_DEV_SETUP = V020_SETUP
V030_DEV: list[Row] = [
    ('implementer.finished', 'agent_session', 1, 'daemon', {'reason': None, 'status': None, 'error': 'worker lost'}),
    (
        'agent_session.identified',
        'agent_session',
        1,
        'daemon',
        {
            'session_id': 's-1',
            'harness': 'claude',
            'executable': 'claude',
            'role': 'implementer',
            'model': 'opus',
            'campaign': 'p',
            'run': 1,
            'idea': 1,
            'proposed_by': {'actor': 'research#3', 'harness': 'pi', 'executable': None, 'session_id': None},
        },
    ),
    (
        'agent_session.job_ended',
        'agent_session',
        1,
        'daemon',
        {
            'kind': 'ran',
            'process': {'status': 'exited', 'seconds': 12.5, 'exit_code': 0, 'error': None},
        },
    ),
    ('agent_session.job_ended', 'agent_session', 2, 'daemon', {'kind': 'not_started', 'reason': 'no pane'}),
    ('run.finished', 'run', 1, 'daemon', {'status': 'failed', 'end': 'worker_lost', 'error': 'worker lost'}),
    ('agent_session.stop_ended', 'agent_session', 1, 'daemon', {'reason': 'target', 'end': 'campaign_stopped'}),
    (
        'lease.stale_report',
        'agent_session',
        1,
        'daemon',
        {
            'lease': 3,
            'worker': 'serve',
            'reason': 'stale_lease',
            'report': {'kind': 'agent_not_started', 'reason': 'gone', 'seconds': 1.0},
        },
    ),
    ('lease.expired', 'candidate', 7, 'daemon', {'lease': 4, 'worker': 'w1', 'reason': 'worker lost'}),
    ('lease.unreachable', 'agent_session', 1, 'daemon', {'lease': 5, 'worker': 'w1'}),
    (
        'measurement.recorded',
        'measurement',
        2,
        'daemon',
        {'candidate': 7, 'metrics': {'accuracy': 0.9}, 'error': None, 'run': 1, 'pair': 1},
    ),
    ('candidate.remeasure_requested', 'candidate', 7, 'human', {'repeats': 5}),
]
"""Every shape that main wrote before events were typed, and no release did."""


@pytest.fixture(autouse=True)
def registered() -> None:
    """Load every module that registers events: the daemon imports each layer."""
    importlib.import_module('piloop.daemon.scheduler')


def release(tmp_path: Path, revision: str, setup: tuple[str, ...], rows: list[Row]) -> list[tuple[Event, str]]:
    """Write ``rows`` at ``revision`` as that release did, migrate to the head, and load every event with its JSON."""
    path = tmp_path / 'piloop.db'
    cfg = alembic(path)
    command.upgrade(cfg, revision)
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        for statement in setup:
            con.execute(statement)
        con.executemany(
            'INSERT INTO events (kind, target_kind, target_id, actor, payload) VALUES (?, ?, ?, ?, ?)',
            [(*row[:4], json.dumps(row[4])) for row in rows],
        )
    command.upgrade(cfg, 'head')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        stored = dict(con.execute('SELECT id, payload FROM events'))
    db = Database(f'sqlite:///{path}', actor=System.DAEMON)
    try:
        with db.session() as s:
            return [(e, stored[e.id]) for e in s.scalars(select(Event).order_by(Event.id))]
    finally:
        db.close()


def loaded(events: list[tuple[Event, str]]) -> list[tuple[str, str]]:
    """Each event as its model and its stored payload, in the order stored."""
    return [(type(e.payload).__name__, payload) for e, payload in events]


@pytest.mark.parametrize(
    ('revision', 'setup', 'rows'),
    [('0001', V010_SETUP, V010), ('0018', V020_SETUP, V020), ('0027', V030_DEV_SETUP, V030_DEV)],
    ids=['0.1.0', '0.2.0', 'main before typed events'],
)
def test_every_event_that_a_release_wrote_loads_as_its_model(
    tmp_path: Path, revision: str, setup: tuple[str, ...], rows: list[Row]
) -> None:
    events = [e for e, _ in release(tmp_path, revision, setup, rows)]
    answers = [e for e in events if isinstance(e.payload, RequestAnswered)]
    assert len(events) - len(answers) == len(rows), 'no event is lost, and the upgrade adds only answers of spent asks'
    assert {e.kind for e in events if isinstance(e.payload, UnknownEvent)} <= EventModel.retired_kinds()
    assert [e.payload for e in events] == [EVENT.validate_json(EVENT.dump_json(e.payload)) for e in events]


def test_the_shapes_of_0_1_0_are_rewritten_to_the_models(tmp_path: Path) -> None:
    assert loaded(release(tmp_path, '0001', V010_SETUP, V010)) == snapshot(
        [
            (
                'AgentSessionStarted',
                '{"harness":"claude","model":"opus","reason":"idea#12 is ready","role":"implementer","kind":"agent_session.started"}',
            ),
            (
                'AgentSessionFinished',
                '{"reason":"idea#12 is ready","error":null,"role":"implementer","kind":"agent_session.finished","state":"exited"}',
            ),
            (
                'AgentSessionFinished',
                '{"reason":"anomaly","error":"dialog","role":"watchdog","kind":"agent_session.finished","state":"exited"}',
            ),
            ('UnknownEvent', '{"kind":"scout.requested"}'),
            (
                'AgentSessionStarted',
                '{"harness":"pi","model":"m","reason":null,"role":"scout","kind":"agent_session.started"}',
            ),
            (
                'StartupAnomaly',
                '{"harness":"codex","run":36,"seconds":41.3,"error":null,"role":"implementer","kind":"anomaly.startup"}',
            ),
            ('CampaignAdded', '{"baseline":1,"kind":"campaign.added"}'),
            ('IdeaCreated', '{"kind":"idea.created"}'),
            ('RunStarted', '{"harness":"claude","model":"opus","kind":"run.started"}'),
            ('RunRelaunched', '{"launches":1,"agent_session":14,"kind":"run.relaunched"}'),
            (
                'MeasurementRecorded',
                '{"candidate":7,"metrics":{"accuracy":0.85},"error":null,"run":4,"kind":"measurement.recorded"}',
            ),
            (
                'CandidateJudged',
                '{"verdict":"keep","score":0.85,"reason":"beats candidate#3","kind":"candidate.judged"}',
            ),
            ('RunFinished', '{"error":null,"end":"finished","kind":"run.finished"}'),
            ('RunFinished', '{"error":"campaign stopped","end":"campaign_stopped","kind":"run.finished"}'),
            (
                'RunFinished',
                '{"error":"blocked on escalation comment#7","end":"awaiting_answer","kind":"run.finished"}',
            ),
            ('RunFinished', '{"error":"agent exit 1","end":"failed","kind":"run.finished"}'),
            ('RunFinished', '{"error":null,"end":"timed_out","kind":"run.finished"}'),
            ('PromptOutcome', '{"comment":88,"error":"NotSteerable: headless","kind":"comment.undelivered"}'),
            ('PromptOutcome', '{"comment":89,"kind":"comment.queued"}'),
            ('WrapUp', '{"reason":"target reached","kind":"agent_session.wrap_up"}'),
            ('WrapUp', '{"reason":"target reached","kind":"agent_session.wrap_up"}'),
            (
                'AnomalyReported',
                '{"subject":"agent_session#5","anomaly":"dead at startup","line":"- [incident] worker#5 dead at startup, 4m ago: failed on claude (idea#12): agent exit 1","kind":"anomaly.reported"}',
            ),
            (
                'AnomalyReported',
                '{"subject":"run#36","anomaly":"failed","line":"- [incident] run#36 failed, 4m ago: failed on claude (idea#12): agent exit 1","kind":"anomaly.reported"}',
            ),
            (
                'RunQueued',
                '{"idea":12,"attempt":2,"harness":"claude","track":"accuracy","pinned_base":9,"computed_base":11,"kind":"run.queued"}',
            ),
            ('RunQueued', '{"idea":13,"attempt":1,"harness":"pi","track":"a","priority_pick":4,"kind":"run.queued"}'),
            ('RunRerouted', '{"from":"pi","to":"claude","kind":"run.rerouted"}'),
            ('LaunchPaused', '{"comment":90,"deaths":3,"load":"7.9 / 6.1 / 4.0","kind":"launch.paused"}'),
            ('LaunchResumed', '{"comment":90,"kind":"launch.resumed"}'),
            ('Rebased', '{"head":11,"rebased":42,"kind":"candidate.rebased"}'),
            ('Rebased', '{"head":11,"conflicts":["a.py"],"kind":"candidate.rebased"}'),
            (
                'CandidateImported',
                '{"ref":"main","revision":"3f2a3f2a3f2a3f2a3f2a3f2a3f2a3f2a3f2a3f2a","kind":"candidate.imported"}',
            ),
            ('IdeaStatusChanged', '{"old":"ready","new":"parked","kind":"idea.status"}'),
            ('IdeaPriorityChanged', '{"old":2,"new":0,"kind":"idea.priority"}'),
            ('IdeaTrackChanged', '{"old":"a","new":"b","kind":"idea.track"}'),
            (
                'OriginChanged',
                '{"kind":"candidate.origin","old":"run","new":"reference"}',
            ),
            ('UnknownEvent', '{"old_length":null,"new_length":12,"kind":"run.brief"}'),
            (
                'CampaignStatusChanged',
                '{"old":"active","new":"stopped","stopped_reason":"target","kind":"campaign.status"}',
            ),
            ('CommentAcked', '{"old":null,"new":"2026-10-01 12:00:00","acked_by":"human","kind":"comment.acked_at"}'),
            ('IdeaDeleted', '{"title":"t","status":"seed","hypothesis":"h","kind":"idea.deleted"}'),
            (
                'CommentDeleted',
                '{"target_kind":"agent_session","target_id":1,"author":"me","body":"hi","comment_kind":"note","kind":"comment.deleted"}',
            ),
            (
                'CandidateDeleted',
                '{"tree_sha":"t","revision":"c","verdict":"crash","kind":"candidate.deleted","origin":"run"}',
            ),
            ('Commented', '{"target_kind":"campaign","target_id":1,"kind":"comment.note"}'),
            ('Commented', '{"target_kind":"run","target_id":1,"kind":"comment.finding"}'),
            ('Commented', '{"target_kind":"idea","target_id":1,"kind":"comment.decision"}'),
            ('Commented', '{"target_kind":"agent_session","target_id":1,"kind":"comment.escalation"}'),
        ]
    )


def test_the_shapes_of_0_2_0_are_rewritten_to_the_models(tmp_path: Path) -> None:
    assert loaded(release(tmp_path, '0018', V020_SETUP, V020)) == snapshot(
        [
            (
                'AgentSessionStarted',
                '{"harness":"claude","model":"opus","reason":null,"role":"implementer","kind":"agent_session.started"}',
            ),
            (
                'AgentSessionFinished',
                '{"reason":null,"error":null,"role":"implementer","kind":"agent_session.finished","state":"exited"}',
            ),
            (
                'AgentSessionFinished',
                '{"reason":"cron","error":"timeout","role":"scout","kind":"agent_session.finished","state":"killed"}',
            ),
            (
                'StartupAnomaly',
                '{"harness":"codex","run":null,"seconds":3.1,"error":"exit 1","role":"watchdog","kind":"anomaly.startup"}',
            ),
            ('CampaignAdded', '{"baseline":1,"kind":"campaign.added"}'),
            (
                'MeasurementRecorded',
                '{"candidate":7,"metrics":{"accuracy":0.9},"error":null,"run":null,"kind":"measurement.recorded"}',
            ),
            ('RejudgeRequested', '{"measurement":2,"kind":"candidate.rejudge_requested"}'),
            ('StopRequested', '{"reason":"","ending":"now","kind":"campaign.stop_requested"}'),
            ('StopRequested', '{"reason":"enough","ending":"wrap_up","kind":"campaign.stop_requested"}'),
            ('CancelRequested', '{"ending":"wrap_up","kind":"run.cancel_requested"}'),
            ('CancelRequested', '{"ending":"now","kind":"agent_session.cancel_requested"}'),
            (
                'DiskAnomaly',
                '{"needed":12000,"free":100,"reserve":1000,"idea":null,"retry_seconds":600.0,"kind":"anomaly.disk"}',
            ),
            ('RunRelaunched', '{"agent_session":4,"launches":2,"kind":"run.relaunched"}'),
            ('RejudgeFailed', '{"error":"GitError: gone","kind":"candidate.rejudge_failed"}'),
            ('OriginChanged', '{"kind":"candidate.origin","old":"run","new":"skipped"}'),
            ('RunFinished', '{"error":"cancelled","end":"cancelled","kind":"run.finished"}'),
            ('RecoveryRequeued', '{"run":1,"candidate":9,"kind":"idea.recovery_requeued"}'),
            ('UnknownEvent', '{"jobs":["12 python train.py"],"kind":"agent_session.jobs_left"}'),
            ('UnknownEvent', '{"jobs":["12 python train.py"],"error":"closed","kind":"agent_session.jobs_left"}'),
            ('WrapUp', '{"reason":"cancelled by human","kind":"agent_session.wrap_up"}'),
            ('AgentEnded', '{"reason":"cancelled by human","end":"cancelled","kind":"agent_session.stop_ended"}'),
            ('AgentEnded', '{"reason":"target","end":"campaign_stopped","kind":"agent_session.stop_ended"}'),
            (
                'AnomalyReported',
                '{"subject":"agent_session#1","anomaly":"silent while its jobs run","line":"- [incident] agent_session#1 silent while its jobs run, 4m ago: failed on claude (idea#12): agent exit 1","kind":"anomaly.reported"}',
            ),
            ('RemeasureRequested', '{"kind":"candidate.remeasure_requested"}'),
            ('IdeaBaseChanged', '{"old":null,"new":7,"kind":"idea.base_candidate_id"}'),
            ('IdeaLinked', '{"target":2,"link_kind":"after","kind":"idea.linked"}'),
            ('IdeaLinked', '{"target":2,"link_kind":"supersedes","kind":"idea.unlinked"}'),
            ('CampaignTypeChanged', '{"old":"optimization","new":"research","kind":"campaign.type"}'),
            ('CampaignParentChanged', '{"old":null,"new":2,"kind":"campaign.parent_id"}'),
            ('CampaignPausedChanged', '{"old":null,"new":"launches","paused_reason":"gpu","kind":"campaign.paused"}'),
            (
                'OriginChanged',
                '{"kind":"candidate.origin","old":"run","new":"recovered"}',
            ),
            ('RequestAnswered', '{"kind":"request.answered","ask":7}'),
        ]
    )


def test_the_shapes_of_main_before_typed_events_are_rewritten_to_the_models(tmp_path: Path) -> None:
    assert loaded(release(tmp_path, '0027', V030_DEV_SETUP, V030_DEV)) == snapshot(
        [
            (
                'AgentSessionFinished',
                '{"reason":null,"error":"worker lost","role":"implementer","kind":"agent_session.finished","state":"killed"}',
            ),
            (
                'Identified',
                '{"session_id":"s-1","harness":"claude","executable":"claude","role":"implementer","model":"opus","campaign":"p","run":1,"idea":1,"proposed_by":{"actor":"research#3","harness":"pi","executable":null,"session_id":null},"kind":"agent_session.identified"}',
            ),
            (
                'JobEnded',
                '{"ended":{"kind":"ran","process":{"status":"exited","seconds":12.5,"exit_code":0,"error":null}},"kind":"agent_session.job_ended"}',
            ),
            ('JobEnded', '{"ended":{"kind":"not_started","reason":"no pane"},"kind":"agent_session.job_ended"}'),
            ('RunFinished', '{"end":"worker_lost","error":"worker lost","kind":"run.finished"}'),
            ('AgentEnded', '{"reason":"target","end":"campaign_stopped","kind":"agent_session.stop_ended"}'),
            (
                'StaleReport',
                '{"lease":3,"worker":"serve","reason":"stale_lease","report":{"kind":"agent_not_started","reason":"gone","seconds":1.0},"kind":"lease.stale_report"}',
            ),
            ('LeaseExpired', '{"lease":4,"worker":"w1","reason":"worker lost","kind":"lease.expired"}'),
            ('LeaseUnreachable', '{"lease":5,"worker":"w1","kind":"lease.unreachable"}'),
            (
                'MeasurementRecorded',
                '{"candidate":7,"metrics":{"accuracy":0.9},"error":null,"run":1,"pair":1,"kind":"measurement.recorded"}',
            ),
            ('RemeasureRequested', '{"repeats":5,"kind":"candidate.remeasure_requested"}'),
        ]
    )


def test_an_event_of_a_kind_that_nothing_registers_keeps_its_payload_and_text_that_is_no_object_is_kept_as_such(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'piloop.db'
    cfg = alembic(path)
    command.upgrade(cfg, '0027')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.executemany(
            "INSERT INTO events (kind, target_kind, target_id, actor, payload) VALUES (?, 'campaign', 1, 'acme', ?)",
            [
                ('acme.observed', '{"latency_ms": 12.5}'),
                ('acme.raw', 'oops'),
                ('acme.count', '7'),
                ('acme.nan', '{"v": NaN}'),
            ],
        )
    command.upgrade(cfg, 'head')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute('SELECT kind, payload FROM events ORDER BY id').fetchall() == [
            ('acme.observed', '{"latency_ms":12.5,"kind":"acme.observed"}'),
            ('acme.raw', '{"invalid":"oops","kind":"acme.raw"}'),
            ('acme.count', '{"invalid":"7","kind":"acme.count"}'),
            ('acme.nan', '{"v":null,"kind":"acme.nan"}'),
        ], 'text that is no JSON object is kept under invalid; JSON5 is written as JSON'
    command.downgrade(cfg, '0027')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute('SELECT kind, payload FROM events ORDER BY id').fetchall()[0] == (
            'acme.observed',
            '{"latency_ms":12.5}',
        )


def test_a_payload_with_a_kind_of_its_own_that_no_rewrite_renames_stops_the_upgrade(tmp_path: Path) -> None:
    path = tmp_path / 'piloop.db'
    cfg = alembic(path)
    command.upgrade(cfg, '0027')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute(
            'INSERT INTO events (kind, target_kind, target_id, actor, payload)'
            " VALUES ('acme.queued', 'campaign', 1, 'daemon', '{\"kind\": \"agent\"}')"
        )
    with pytest.raises(RuntimeError, match='a field "kind" that no rewrite of revision .* renames: acme.queued'):
        command.upgrade(cfg, 'head')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute('SELECT kind, payload FROM events').fetchall() == [('acme.queued', '{"kind": "agent"}')]
