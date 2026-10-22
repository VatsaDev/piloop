"""The core operations behind every executor (``CampaignResults``): a result for a settled row is refused."""

import threading
import time
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.agents.results import CampaignResults
from piloop.db.models import AgentSession, AgentSessionState, Campaign, Evaluation, Event, Measurement, RoleName, utcnow
from piloop.db.session import Database
from piloop.otel import Tracer
from piloop.plugins.contracts.executor import (
    AgentJob,
    AgentReport,
    Checkout,
    EvaluationJob,
    EvaluationKey,
    EvaluationReport,
    GitTree,
    Settled,
)
from piloop.runtime import AgentStatus
from tests.conftest import baseline


def test_an_evaluation_that_left_the_queue_or_never_started_is_settled(
    db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    results = CampaignResults(db, Tracer(None), tmp_path / 'units', lambda: False)
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
        artifact=GitTree(repo=Path(campaign.path), commit=base.commit_sha),
        cwd=tmp_path / 'wt',
        log_dir=tmp_path,
        command=['true'],
        timeout=60,
        required=['score'],
        units_file=tmp_path / 'units.pending',
    )
    report = EvaluationReport(metrics={'score': 1.0}, seconds=1)
    with pytest.raises(Settled, match='is not running'):
        results.finished(job, report)  # it waits: no executor started it
    with pytest.raises(Settled, match='left the queue'):
        results.lost(EvaluationKey(waiting + 1), 'gone')


def test_two_results_for_one_job_in_plain_sessions_close_its_row_once_and_measure_it_once(
    db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    """A second result comes while the first is between its check and its commit, as two calls of an executor without
    a fence of its own make them: the second waits for the first's commit, then finds the row settled."""
    results = CampaignResults(db, Tracer(None), tmp_path / 'units', lambda: False)
    base = baseline(db, campaign.id)
    with db.session() as s:
        w = AgentSession(kind=RoleName.WATCHDOG, campaign_id=campaign.id, state=AgentSessionState.RUNNING, reason='t')
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
        campaign_path=Path(campaign.path),
        artifact=Checkout(path=tmp_path),
        cwd=tmp_path,
        log_dir=tmp_path,
        timeout=60,
        brief=None,
        spec=None,
    )
    measure = EvaluationJob(
        evaluation=evaluation_id,
        candidate=base.id,
        runner='process',
        artifact=GitTree(repo=Path(campaign.path), commit=base.commit_sha),
        cwd=tmp_path / 'wt',
        log_dir=tmp_path,
        command=['true'],
        timeout=60,
        required=['score'],
        units_file=tmp_path / 'units.pending',
    )
    reports: list[tuple[AgentJob | EvaluationJob, AgentReport | EvaluationReport]] = [
        (agent, AgentReport(status=AgentStatus.EXITED, seconds=200, exit_code=0)),
        (measure, EvaluationReport(metrics={'score': 1.0}, seconds=1)),
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
        finished = s.scalars(select(Event).where(Event.kind == 'watchdog.finished')).all()
        measured = s.scalars(select(Measurement).where(Measurement.candidate_id == base.id)).all()
    assert (len(finished), len(measured)) == (1, 2), 'one close, and one measurement besides the baseline'
