"""Requests: an ask is an event, pending until the daemon answers it or a newer ask of its kind replaces it."""

import pytest

from piloop.actor import Agent, System
from piloop.campaign.stop import (
    CancelRequested,
    Ending,
    StopRequested,
    agent_cancel,
    answer_done,
    request_cancel,
    request_stop,
    run_cancel,
)
from piloop.db.events import emit, in_order
from piloop.db.models import (
    AgentSession,
    AgentSessionState,
    Campaign,
    CampaignStatus,
    Candidate,
    Evaluator,
    Idea,
    IdeaStatus,
    Run,
)
from piloop.db.requests import RequestAnswered, answer, pending
from piloop.db.session import Database
from piloop.ids import IMPLEMENTER, CandidateId, RunId, TargetKind, TargetRef
from piloop.scoring.ledger import record, request_remeasure, requested
from tests.conftest import WATCHDOG


def test_an_ask_is_pending_while_it_is_the_latest_of_its_kind_on_its_target_and_unanswered(
    db: Database, campaign: Campaign
) -> None:
    on = TargetRef(TargetKind.CAMPAIGN, campaign.id)
    with db.session() as s:
        emit(s, StopRequested(reason='a', ending=Ending.WRAP_UP), on, System.HUMAN)
        emit(s, StopRequested(reason='b', ending=Ending.NOW), on, System.ORCHESTRATOR)
        s.commit()
        [(asked, stop)] = pending(s, StopRequested)
        assert (asked.actor, stop) == ('orchestrator', StopRequested(reason='b', ending=Ending.NOW)), 'the latest wins'
        answer(s, asked)
        s.commit()
        assert pending(s, StopRequested) == []
        emit(s, StopRequested(reason='c', ending=Ending.NOW), on, System.HUMAN)
        s.commit()
        assert [stop.reason for _, stop in pending(s, StopRequested)] == ['c'], 'a new ask after an answer'
        assert pending(s, CancelRequested) == []
        [(answered, payload)] = in_order(s, RequestAnswered)
        assert (answered.target, answered.actor, payload) == (on, 'daemon', RequestAnswered(ask=asked.id))


def queued_run(db: Database, campaign: Campaign, state: AgentSessionState) -> tuple[RunId, AgentSession]:
    with db.session() as s:
        idea = Idea(campaign_id=campaign.id, title='t', hypothesis='h', track='score', created_by='human')
        idea.status = IdeaStatus.SCHEDULED
        s.add(idea)
        s.flush()
        run = Run(idea_id=idea.id, attempt=1, base_candidate_id=1, harness='script', model='m')
        s.add(run)
        s.flush()
        agent = AgentSession(kind=IMPLEMENTER, campaign_id=campaign.id, run_id=run.id, state=state)
        s.add(agent)
        s.commit()
        return run.id, agent


def test_a_cancel_is_answered_once_none_of_its_agents_is_requested_starting_or_running(
    db: Database, campaign: Campaign
) -> None:
    run_id, agent = queued_run(db, campaign, AgentSessionState.STARTING)
    with db.session() as s:
        request_cancel(s, s.get_one(Run, run_id), System.HUMAN, ending=Ending.NOW)
        sweep = AgentSession(kind=WATCHDOG, campaign_id=campaign.id, state=AgentSessionState.RUNNING)
        s.add(sweep)
        s.flush()
        request_cancel(s, sweep, System.HUMAN, ending=Ending.WRAP_UP)
        answer_done(s)
        s.commit()
        assert len(pending(s, CancelRequested)) == 2, 'a starting agent and a running sweep: not yet'
        s.get_one(AgentSession, agent.id).state = AgentSessionState.ENDING
        s.get_one(AgentSession, sweep.id).state = AgentSessionState.EXITED
        answer_done(s)
        s.commit()
        assert pending(s, CancelRequested) == [], 'the daemon ended one, and the other ended on its own'
        found = run_cancel(s, run_id)
        assert found is not None, 'an answered cancel is still the fact'
        assert agent_cancel(s, s.get_one(AgentSession, agent.id)) == found, "an implementer's is its run's"


@pytest.mark.parametrize('ending', list(Ending))
def test_a_stop_is_answered_once_its_campaign_is_stopped_and_with_now_its_implementers_are_ended(
    db: Database, campaign: Campaign, ending: Ending
) -> None:
    _, agent = queued_run(db, campaign, AgentSessionState.RUNNING)
    with db.session() as s:
        request_stop(
            s, s.get_one(Campaign, campaign.id), Agent(role=IMPLEMENTER, session=agent.id), reason='', ending=ending
        )
        answer_done(s)
        s.commit()
        assert len(pending(s, StopRequested)) == 1, 'the campaign is still active'
        s.get_one(Campaign, campaign.id).status = CampaignStatus.STOPPED
        answer_done(s)
        s.commit()
        assert len(pending(s, StopRequested)) == (ending is Ending.NOW), 'now: its implementer still runs'
        s.get_one(AgentSession, agent.id).state = AgentSessionState.ENDING
        answer_done(s)
        s.commit()
        assert pending(s, StopRequested) == []


def test_only_a_measurement_under_the_current_evaluator_answers_a_remeasure(db: Database, campaign: Campaign) -> None:
    with db.session() as s:
        baseline = s.get_one(Candidate, CandidateId(1))
        request_remeasure(s, baseline, System.HUMAN)
        old = Evaluator(campaign_id=campaign.id, key='old', definition={})
        s.add(old)
        s.flush()
        record(s, baseline, old.id, System.DAEMON, metrics={'score': 1.0}, error=None, seconds=1.0)
        s.commit()
        assert [c.id for c, _ in requested(s, s.get_one(Campaign, campaign.id))] == [1], 'an old evaluator'
        current = s.get_one(Campaign, campaign.id).evaluator_id
        assert current is not None
        record(s, baseline, current, System.DAEMON, metrics={'score': 1.0}, error=None, seconds=1.0)
        s.commit()
        assert requested(s, s.get_one(Campaign, campaign.id)) == []
