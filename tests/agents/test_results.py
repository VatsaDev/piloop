"""The core operations behind every executor (``CampaignResults``): a result for a settled row is refused."""

import threading
import time
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.agents.results import AgentSessionFinished, CampaignResults, Identified, learn
from piloop.db.events import in_order
from piloop.db.models import (
    AgentSession,
    AgentSessionState,
    Campaign,
    Evaluation,
    Event,
    Idea,
    Measurement,
    Run,
    utcnow,
)
from piloop.db.session import Database
from piloop.ids import IMPLEMENTER, RESEARCH, WATCHDOG, AgentSessionId, IdeaId, RunId
from piloop.otel import Tracer
from piloop.plugins.executor.contract import (
    AgentFinished,
    AgentJob,
    AgentKey,
    AgentNotStarted,
    AgentReport,
    Bundle,
    Checkout,
    EvaluationJob,
    EvaluationKey,
    EvaluationReport,
    GitSource,
    MissingBlobs,
    RepoId,
    Sample,
    Settled,
)
from piloop.plugins.runtime.contract import AgentExit, ExitStatus
from tests.agents.test_launcher import MINTED_HARNESS, agent_session, launch, request, use_harness
from tests.conftest import HARNESS_PROMPT_TO_FILE, ScriptRuntime, baseline, disk

RESEARCH_SESSION = '0c5b9f9e-3a43-4c39-9a51-6f0cfa1a3b0e'


def test_an_evaluation_that_left_the_queue_or_never_started_is_settled(
    db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    results = CampaignResults(db, Tracer(None), tmp_path, disk(tmp_path), lambda: False)
    base = baseline(db, campaign.id)
    with db.session() as s:
        evaluation = Evaluation(candidate_id=base.id, evaluator_id=s.get_one(Campaign, campaign.id).evaluator_id)
        s.add(evaluation)
        s.commit()
        waiting = evaluation.id
    job = EvaluationJob(
        evaluation=waiting,
        candidate=base.id,
        runner='process',
        artifact=GitSource(repo=RepoId(campaign.id), commit=base.commit_sha),
        place='eval',
        command=['true'],
        timeout=60,
        required=['score'],
    )
    report = EvaluationReport(samples=[Sample(metrics={'score': 1.0}, seconds=1)])
    with pytest.raises(Settled, match='is not running'):
        results.finished(job, report)  # it waits: no executor started it
    with pytest.raises(Settled, match='left the queue'):
        results.lost(EvaluationKey(waiting + 1), 'gone')


def test_a_start_report_moves_a_starting_agent_to_running_and_leaves_an_ending_one_ending(
    db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    """A start that the executor reports after the daemon ended the agent does not undo the end: its finding stays
    refused. The report still updates the handle and the session id."""
    results = CampaignResults(db, Tracer(None), tmp_path, disk(tmp_path), lambda: False)
    with db.session() as s:
        rows = [
            AgentSession(kind=WATCHDOG, campaign_id=campaign.id, harness='script', state=state)
            for state in (AgentSessionState.STARTING, AgentSessionState.ENDING)
        ]
        s.add_all(rows)
        s.commit()
        keys = [AgentKey(w.id) for w in rows]
    for key in keys:
        results.started(key, {'pid': key.agent_session}, f's{key.agent_session}')
    with db.session() as s:
        assert [
            (w.state, w.handle, w.session_id) for w in s.scalars(select(AgentSession).order_by(AgentSession.id))
        ] == [
            (AgentSessionState.RUNNING, {'pid': 1}, 's1'),
            (AgentSessionState.ENDING, {'pid': 2}, 's2'),
        ]


def test_two_results_for_one_job_in_plain_sessions_close_its_row_once_and_measure_it_once(
    db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    """A second result comes while the first is between its check and its commit, as two calls of an executor without
    a fence of its own make them: the second waits for the first's commit, then finds the row settled."""
    results = CampaignResults(db, Tracer(None), tmp_path, disk(tmp_path), lambda: False)
    base = baseline(db, campaign.id)
    with db.session() as s:
        w = AgentSession(kind=WATCHDOG, campaign_id=campaign.id, state=AgentSessionState.RUNNING, reason='t')
        evaluation = Evaluation(
            candidate_id=base.id, evaluator_id=s.get_one(Campaign, campaign.id).evaluator_id, started_at=utcnow()
        )
        s.add_all([w, evaluation])
        s.commit()
        agent_session_id, evaluation_id = w.id, evaluation.id
    agent = AgentJob(
        agent_session=agent_session_id,
        runtime='subprocess',
        campaign=campaign.name,
        artifact=Checkout(repo=RepoId(campaign.id)),
        place='agent',
        timeout=60,
        brief=None,
        spec=None,
    )
    measure = EvaluationJob(
        evaluation=evaluation_id,
        candidate=base.id,
        runner='process',
        artifact=GitSource(repo=RepoId(campaign.id), commit=base.commit_sha),
        place='eval',
        command=['true'],
        timeout=60,
        required=['score'],
    )
    reports: list[tuple[AgentJob | EvaluationJob, AgentReport | EvaluationReport]] = [
        (agent, AgentFinished(process=AgentExit(ExitStatus.EXITED, 200, 0))),
        (measure, EvaluationReport(samples=[Sample(metrics={'score': 1.0}, seconds=1)])),
    ]
    for job, report in reports:
        second: list[str] = []
        checked = threading.Event()

        def slow(_s: Session, checked: threading.Event = checked) -> None:
            checked.set()
            time.sleep(0.3)  # the second call comes now, between this one's check and its commit

        first = threading.Thread(target=results.finished, args=(job, report), kwargs={'guard': slow})
        first.start()
        assert checked.wait(10)
        try:
            results.finished(job, report)
            second.append('applied')
        except Settled:
            second.append('settled')
        first.join(30)
        assert second == ['settled']
    with db.session() as s:
        finished = in_order(s, AgentSessionFinished)
        measured = s.scalars(select(Measurement).where(Measurement.candidate_id == base.id)).all()
    assert (len(finished), len(measured)) == (1, 2), 'one close, and one measurement besides the baseline'


def identified(db: Database, agent_session_id: AgentSessionId) -> list[dict[str, object]]:
    """The ``Identified`` events of the row, as they are stored, without their kind."""
    with db.session() as s:
        found = in_order(s, Identified, Event.target_id == agent_session_id)
        return [e.model_dump(mode='json', exclude={'kind'}) for _, e in found]


def implementer_of(
    db: Database, campaign: Campaign, created_by: str, **fields: object
) -> tuple[AgentSessionId, RunId, IdeaId]:
    """Request an implementer for a run of a new idea that ``created_by`` proposed. Return its row, run and idea."""
    with db.session() as s:
        idea = Idea(campaign_id=campaign.id, track='score', title='stack', hypothesis='h', created_by=created_by)
        s.add(idea)
        s.flush()
        run = Run(idea_id=idea.id, attempt=1, base_candidate_id=1, harness='script', model='test-model')
        s.add(run)
        s.commit()
        run_id, idea_id = run.id, idea.id
    return request(db, campaign, IMPLEMENTER, run_id=run_id, **fields), run_id, idea_id


def agent_job(campaign: Campaign, agent_session_id: AgentSessionId, tmp_path: Path) -> AgentJob:
    return AgentJob(
        agent_session=agent_session_id,
        runtime='subprocess',
        campaign=campaign.name,
        artifact=Checkout(repo=RepoId(campaign.id)),
        place='agent',
        timeout=60,
        brief=None,
        spec=None,
    )


def test_an_implementer_with_a_minted_session_id_is_identified_at_launch_with_the_agent_that_proposed_its_idea(
    campaign: Campaign, db: Database, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    use_harness(db, campaign, HARNESS_PROMPT_TO_FILE, **MINTED_HARNESS)
    research = request(db, campaign, RESEARCH, harness='claude', session_id=RESEARCH_SESSION)
    implementer, run, idea = implementer_of(db, campaign, f'research#{research}')
    launch(db, runtime, tmp_path, implementer)
    w = agent_session(db, implementer)
    assert identified(db, implementer) == [
        {
            'session_id': w.session_id,
            'harness': 'script',
            'executable': str(Path(campaign.path).parent / 'harness.sh'),
            'role': 'implementer',
            'model': 'test-model',
            'campaign': 'demo',
            'run': run,
            'idea': idea,
            'proposed_by': {
                'actor': f'research#{research}',
                'harness': 'claude',
                'executable': 'claude',
                'session_id': RESEARCH_SESSION,
            },
        }
    ]


def test_a_reported_session_id_is_identified_once_whatever_repeats_it(
    campaign: Campaign, db: Database, tmp_path: Path
) -> None:
    """Heartbeats and the final report repeat the id: only an id that is new to the row emits the event."""
    results = CampaignResults(db, Tracer(None), tmp_path, disk(tmp_path), lambda: False)
    research = request(db, campaign, RESEARCH, harness='script', state=AgentSessionState.STARTING)
    key = AgentKey(research)
    results.started(key, {}, None)
    assert identified(db, research) == []
    results.session(key, 'first')
    results.session(key, 'first')
    results.session(key, 'second')
    report = AgentFinished(process=AgentExit(ExitStatus.EXITED, 200, 0), session_id='second')
    results.finished(agent_job(campaign, research, tmp_path), report)
    found = identified(db, research)
    assert [e['session_id'] for e in found] == ['first', 'second']
    assert found[0] == {
        'session_id': 'first',
        'harness': 'script',
        'executable': str(Path(campaign.path).parent / 'harness.sh'),
        'role': 'research',
        'model': None,
        'campaign': 'demo',
        'run': None,
        'idea': None,
        'proposed_by': None,
    }


def test_an_id_that_only_the_final_report_carries_is_identified(
    campaign: Campaign, db: Database, tmp_path: Path
) -> None:
    results = CampaignResults(db, Tracer(None), tmp_path, disk(tmp_path), lambda: False)
    watchdog = request(db, campaign, harness='script', state=AgentSessionState.RUNNING)
    report = AgentFinished(process=AgentExit(ExitStatus.EXITED, 200, 0), session_id='at-the-end')
    results.finished(agent_job(campaign, watchdog, tmp_path), report)
    assert [e['session_id'] for e in identified(db, watchdog)] == ['at-the-end']


@pytest.mark.parametrize(
    ('created_by', 'proposed_by'),
    [
        ('human', None),
        ('orchestrator', None),
        (
            'research#{research}',
            {'actor': 'research#{research}', 'harness': 'claude', 'executable': 'claude', 'session_id': None},
        ),
        ('research#9999', {'actor': 'research#9999', 'harness': None, 'executable': None, 'session_id': None}),
    ],
    ids=['human', 'orchestrator', 'agent-without-a-session-yet', 'agent-whose-row-is-gone'],
)
def test_the_proposer_is_an_agent_only(
    campaign: Campaign,
    db: Database,
    tmp_path: Path,
    created_by: str,
    proposed_by: dict[str, object] | None,
) -> None:
    research = request(db, campaign, RESEARCH, harness='claude')
    implementer, _, _ = implementer_of(
        db, campaign, created_by.format(research=research), harness='script', state=AgentSessionState.RUNNING
    )
    CampaignResults(db, Tracer(None), tmp_path, disk(tmp_path), lambda: False).session(AgentKey(implementer), 'x')
    [event] = identified(db, implementer)
    expected = (
        None if proposed_by is None else {**proposed_by, 'actor': str(proposed_by['actor']).format(research=research)}
    )
    assert event['proposed_by'] == expected


def test_the_event_commits_with_the_session_id_or_not_at_all(campaign: Campaign, db: Database) -> None:
    watchdog = request(db, campaign, harness='script', state=AgentSessionState.RUNNING)
    with db.session() as s:
        learn(s, s.get_one(AgentSession, watchdog), 'x')
        s.rollback()
    assert agent_session(db, watchdog).session_id is None
    assert identified(db, watchdog) == []


def test_a_report_naming_a_blob_the_cas_lacks_is_refused_until_it_is_there_then_its_files_land_in_its_place(
    db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    """REAPI's completeness check: nothing applies while a named blob is missing, and the same report applies after."""
    cas = disk(tmp_path)
    results = CampaignResults(db, Tracer(None), tmp_path, cas, lambda: False)
    base = baseline(db, campaign.id)
    with db.session() as s:
        evaluation = Evaluation(
            candidate_id=base.id, evaluator_id=s.get_one(Campaign, campaign.id).evaluator_id, started_at=utcnow()
        )
        s.add(evaluation)
        s.commit()
        evaluation_id = evaluation.id
    job = EvaluationJob(
        evaluation=evaluation_id,
        candidate=base.id,
        runner='process',
        artifact=GitSource(repo=RepoId(campaign.id), commit=base.commit_sha),
        place='runs/1/eval-0',
        command=['true'],
        timeout=60,
        required=['score'],
    )
    stdout = disk(tmp_path / 'worker').put(b'METRIC score=1\n')
    report = EvaluationReport(samples=[Sample(metrics={'score': 1.0}, seconds=1)], files={'eval.stdout': stdout})
    with pytest.raises(MissingBlobs, match=str(stdout)):
        results.finished(job, report)
    with db.session() as s:
        assert s.get(Evaluation, evaluation_id) is not None, 'nothing applied'
    assert cas.put(b'METRIC score=1\n') == stdout  # the worker puts it in the CAS
    results.finished(job, report)
    assert (tmp_path / 'runs' / '1' / 'eval-0' / 'eval.stdout').read_bytes() == b'METRIC score=1\n'
    with db.session() as s:
        assert s.get(Evaluation, evaluation_id) is None, 'measured'


def test_the_blobs_of_a_report_are_its_files_and_its_bundle(tmp_path: Path) -> None:
    cas = disk(tmp_path)
    brief, bundle = cas.put(b'brief'), cas.put(b'bundle')
    finished = AgentFinished(
        process=AgentExit(ExitStatus.EXITED, 1), commits=Bundle(blob=bundle, head='h'), files={'brief.md': brief}
    )
    assert finished.blobs() == [brief, bundle]
    assert AgentNotStarted(reason='r', files={'brief.md': brief}).blobs() == [brief]
