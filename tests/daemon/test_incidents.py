"""The incident triggers (a failed run, an agent dead at startup, a leaking funnel) find what wakes an agent; the cost sections show what the progress review weighs."""

from pathlib import Path
from typing import Never

import pytest
from inline_snapshot import snapshot
from sqlalchemy.orm import Session

from piloop.agents.results import StartupAnomaly
from piloop.campaign.anomaly import AnomalyReported
from piloop.config import load_campaign_config
from piloop.db.models import (
    AgentSessionState,
    Campaign,
    CampaignStatus,
    Candidate,
    Event,
    IdeaStatus,
    Measurement,
    Pause,
    Run,
    RunEnd,
    RunStatus,
)
from piloop.db.session import Database
from piloop.ids import TargetKind, TargetRef
from piloop.plugins.campaign_type.consumers import RolePass
from piloop.plugins.section.contract import SECTIONS, Rendering, markdown
from piloop.plugins.section.reported import REPORTED_SHOWN
from piloop.plugins.trigger.contract import Fire
from piloop.plugins.trigger.dead_at_startup import DeadAtStartup
from piloop.plugins.trigger.leaking_funnel import LeakingFunnel
from piloop.plugins.trigger.run_failed import RunFailed
from piloop.plugins.verdict_rule.contract import Outcome
from tests.conftest import RESEARCH, WATCHDOG, cas_of
from tests.reports.test_overview import NOW, agent, at, candidate, idea, live_campaign, run


def fired(s: Session, campaign: Campaign, trigger: RunFailed | DeadAtStartup | LeakingFunnel) -> Fire | None:
    """What ``trigger`` of the watchdog fires on a pass now."""

    def unread(_: object) -> Never:
        raise AssertionError('an incident trigger reads no backlog and no stall')

    p = RolePass(
        s=s,
        now=NOW,
        campaign=s.get_one(Campaign, campaign.id),
        role=WATCHDOG,
        last=None,
        quiet=None,
        max_concurrent=1,
        interval=0,
        backlog=unread,
        without_keep=unread,
    )
    return trigger.fire(p)


def subjects(fire: Fire | None) -> list[str]:
    return [] if fire is None else [str(e.subject) for e in fire.events if isinstance(e, AnomalyReported)]


def shown(s: Session, campaign: Campaign, *names: str) -> str:
    sections = [SECTIONS.validated({'provider': name}) for name in names]
    return markdown(sections, Rendering(s, NOW, s.get_one(Campaign, campaign.id), Path('/no/workspace'), cas_of(s)))


def test_a_failed_or_timed_out_run_is_an_incident_and_a_stopped_or_blocked_one_is_not(
    db: Database, campaign: Campaign
) -> None:
    """A run that the daemon stopped did nothing wrong, and a blocked run's escalation is in the inbox already."""
    live_campaign(db, campaign)
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        for status, end in ((RunStatus.TIMED_OUT, None), (RunStatus.FAILED, RunEnd.DAEMON_STOPPED)):
            row = run(s, idea(s, p, f'{status}', IdeaStatus.DONE), status, 1, 60, 70)
            row.end = end or row.end
        blocked = run(s, idea(s, p, 'blocked', IdeaStatus.DONE), RunStatus.FAILED, 1, 60, 70, error='comment#9')
        blocked.end = RunEnd.AWAITING_ANSWER
        s.commit()
        fire = fired(s, campaign, RunFailed())
    assert fire is not None and fire.alone
    assert subjects(fire) == ['run#2', 'run#4'], 'oldest first'
    assert fire.reason == 'run_failed: run#2 failed, run#4 failed'
    [first, _] = fire.events
    assert isinstance(first, AnomalyReported) and first.line == snapshot(
        '- run#2 failed, 1h ago: failed on script (idea#2): boom'
    )


def test_an_incident_wakes_an_agent_once(db: Database, campaign: Campaign) -> None:
    live_campaign(db, campaign)
    with db.session() as s:
        fire = fired(s, campaign, RunFailed())
        assert fire is not None
        for event in fire.events:
            s.add(Event(actor='daemon', target=TargetRef(TargetKind.AGENT_SESSION, 1), payload=event))
        s.commit()
        assert fired(s, campaign, RunFailed()) is None, 'an agent of the campaign got it'
        assert subjects(fired(s, campaign, DeadAtStartup())) == ['agent_session#2'], (
            'another trigger, its own incidents'
        )


def test_an_agent_dead_at_startup_is_an_incident_unless_its_role_investigates(db: Database, campaign: Campaign) -> None:
    """A watchdog that died at startup would request a watchdog that could die the same way, and so on.

    The startup breaker pauses the launches instead. Research does not investigate, so its death is an incident.
    """
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        agent(s, p, WATCHDOG, 0, 5)  # a short life, without the launcher's event: no incident
        for kind in (WATCHDOG, RESEARCH):
            dead = agent(s, p, kind, 0, 35, state=AgentSessionState.KILLED)
            payload = StartupAnomaly(role=kind, harness='script', run=None, seconds=35.0, error='prompt_lost')
            s.add(Event(actor='daemon', target=TargetRef(TargetKind.AGENT_SESSION, dead.id), payload=payload))
        s.commit()
        assert subjects(fired(s, campaign, DeadAtStartup())) == ['agent_session#3']


def test_the_incidents_given_to_agents_are_listed_newest_first(db: Database, campaign: Campaign) -> None:
    with db.session() as s:
        assert shown(s, campaign, 'reported') == '## Incidents given to agents\n\n- None.'
        watchdog = agent(s, s.get_one(Campaign, campaign.id), WATCHDOG, 0, 5)
        for run_id in range(3, 3 + REPORTED_SHOWN + 1):
            subject = TargetRef(TargetKind.RUN, run_id)
            reported = AnomalyReported(subject=subject, trigger='run_failed', line=f'- {subject}')
            s.add(
                Event(actor='daemon', target=TargetRef(TargetKind.AGENT_SESSION, watchdog.id), payload=reported, at=NOW)
            )
        s.commit()
        text = shown(s, campaign, 'reported')
    lines = text.splitlines()[2:]
    assert lines[:2] == [
        f'- run#{2 + REPORTED_SHOWN + 1} (run_failed), given to agent_session#1 0s ago',
        f'- run#{2 + REPORTED_SHOWN} (run_failed), given to agent_session#1 0s ago',
    ], 'newest first'
    assert (len(lines), lines[-1]) == (REPORTED_SHOWN + 1, '- … and 1 older incidents.')


def test_the_idle_queue_counts_from_the_last_run_and_names_pending_research(db: Database, campaign: Campaign) -> None:
    live_campaign(db, campaign)
    with db.session() as s:
        s.get_one(Run, 3).status, s.get_one(Run, 3).end = RunStatus.SUCCEEDED, RunEnd.FINISHED
        s.get_one(Run, 3).finished_at = at(70)
        agent(s, s.get_one(Campaign, campaign.id), RESEARCH, 100, 0, state=AgentSessionState.REQUESTED)
        s.commit()
        assert shown(s, campaign, 'idle_queue') == snapshot("""\
## Idle queue

- 1 free implementer slots and no runnable idea since 50m ago; research#5 requested\
""")


@pytest.mark.parametrize('held', ['max_runs', 'stopped', 'paused'])
def test_no_idle_queue_while_nothing_may_start(db: Database, campaign: Campaign, held: str) -> None:
    """Two runs count toward ``max_runs``; the run that failed with no candidate does not count."""
    live_campaign(db, campaign)
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        p.config = load_campaign_config(
            Path(campaign.path),
            plugins={
                'role': {'implementer': {'max_concurrent': 4}},
                **({'stop_rule': {'max_runs': 2}} if held == 'max_runs' else {}),
            },
        )
        p.status = CampaignStatus.STOPPED if held == 'stopped' else p.status
        p.paused = Pause.LAUNCHES if held == 'paused' else None
        assert shown(s, campaign, 'idle_queue') == ''


def test_candidates_that_scored_exactly_their_parent_share_a_line(db: Database, campaign: Campaign) -> None:
    live_campaign(db, campaign)
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        for n in range(9):
            candidate(s, p, 2, Outcome.DISCARD, 3, 30 + n, run_id=1)
        orphan = Candidate(campaign_id=p.id, parent_id=1, tree_sha='x', revision='x', verdict=Outcome.CRASH)
        s.add(orphan)
        s.flush()
        s.add(Measurement(candidate_id=orphan.id, evaluator_id=p.evaluator_id, metrics={}, error='died'))
        s.commit()
        text = shown(s, campaign, 'same_as_parent')
    assert text.splitlines()[2] == snapshot(
        '- 10 candidates (candidate#12, candidate#11, candidate#10, candidate#9, candidate#8, candidate#7, candidate#6, candidate#5, …); the latest, 1h ago: scored what candidate#2 did on score and on 1 of 1 other metrics; 3.0 min of evaluation'
    ), 'a crashed measurement is no score'


def test_one_candidate_that_scored_exactly_its_parent_has_a_line_of_its_own(db: Database, campaign: Campaign) -> None:
    live_campaign(db, campaign)
    with db.session() as s:
        assert shown(s, campaign, 'same_as_parent').splitlines()[2].startswith('- candidate#3, 1h ago: scored what')
        s.get_one(Measurement, 3).metrics = {'score': 4.0, 'size': 10.0}
        s.commit()
        assert shown(s, campaign, 'same_as_parent') == '', 'nothing to show'


def test_a_leaking_funnel_is_an_incident_once_for_each_evaluator(db: Database, campaign: Campaign) -> None:
    """Fewer than 5 of the last 10 attempts measured: a gate that most runs cannot pass."""
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        evaluator = p.evaluator
        assert evaluator is not None
        evaluator.created_at = at(0)
        for n in range(9):
            row = run(s, idea(s, p, f'idea {n}', IdeaStatus.DONE), RunStatus.FAILED, 1, n, n + 1)
            if n < 4:
                candidate(s, p, 1, Outcome.DISCARD, 1, n + 1, run_id=row.id)
        s.commit()
        assert fired(s, campaign, LeakingFunnel()) is None, 'nine attempts: not yet a window'
        run(s, idea(s, p, 'the tenth', IdeaStatus.DONE), RunStatus.TIMED_OUT, 1, 9, 10)
        s.commit()
        fire = fired(s, campaign, LeakingFunnel())
        assert fire is not None and subjects(fire) == ['campaign#1']
        [event] = fire.events
        assert isinstance(event, AnomalyReported) and event.line == snapshot(
            '- campaign#1 leaks, 1h ago: 4 of the last 10 attempts were measured under evaluator#1; the others ended: 1 timed_out, 5 failed'
        )
        assert fired(s, campaign, LeakingFunnel(measured=4)) is None, '4 of 10 is enough for a lower bar'
        watchdog = agent(s, p, WATCHDOG, 0, 5)
        s.add(Event(actor='daemon', target=TargetRef(TargetKind.AGENT_SESSION, watchdog.id), payload=event, at=NOW))
        s.commit()
        assert fired(s, campaign, LeakingFunnel()) is None, 'once for each evaluator'
