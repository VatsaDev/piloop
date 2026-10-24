"""Tests for the overview that ``piloop status`` prints and that each sweep reads in its brief."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from inline_snapshot import snapshot
from sqlalchemy.orm import Session

from piloop.agents.results import StartupAnomaly
from piloop.campaign.anomaly import AnomalyKind
from piloop.config import CampaignConfig, load_campaign_config
from piloop.db.models import (
    AgentSession,
    AgentSessionState,
    Campaign,
    CampaignStatus,
    Candidate,
    Comment,
    CommentKind,
    Event,
    Idea,
    IdeaLink,
    IdeaStatus,
    LinkKind,
    Measurement,
    Run,
    RunEnd,
    RunStatus,
    Verdict,
)
from piloop.db.session import Database
from piloop.ids import IMPLEMENTER, RoleName, RunId, TargetKind, TargetRef
from piloop.reports import overview as overview_module
from piloop.reports.overview import LINKED_CHARS, anomalies, block, overview
from piloop.scoring.ledger import IdeaCreated
from tests.conftest import RESEARCH, WATCHDOG, end_of, spent

T = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
NOW = T + timedelta(hours=2)


def at(minutes: float) -> datetime:
    return T + timedelta(minutes=minutes)


def idea(
    s: Session, campaign: Campaign, title: str, status: IdeaStatus, *, after: tuple[int, ...] = (), **kw: object
) -> Idea:
    row = Idea(
        track='score',
        campaign_id=campaign.id,
        title=title,
        hypothesis=title,
        status=status,
        created_by='research#4',
        links=[IdeaLink(target_id=t, kind=LinkKind.AFTER) for t in after],
        **kw,
    )
    s.add(row)
    s.flush()
    return row


def run(s: Session, i: Idea, status: RunStatus, base: int, start: float, end: float | None, **kw: object) -> Run:
    row = Run(
        idea_id=i.id,
        attempt=1,
        base_candidate_id=base,
        harness='script',
        model='m',
        status=status,
        end=end_of(status),
        created_at=at(start),
        started_at=at(start),
        finished_at=at(end) if end is not None else None,
        **kw,
    )
    s.add(row)
    s.flush()
    return row


def agent(
    s: Session,
    campaign: Campaign,
    kind: RoleName,
    start: float,
    lived: float,
    *,
    run_id: RunId | None = None,
    **kw: object,
) -> AgentSession:
    row = AgentSession(
        kind=kind,
        campaign_id=campaign.id,
        run_id=run_id,
        harness='script',
        runtime='subprocess',
        started_at=at(start),
        ended_at=at(start) + timedelta(seconds=lived),
        **{'state': AgentSessionState.EXITED, **kw},
    )
    s.add(row)
    s.flush()
    return row


def candidate(
    s: Session, campaign: Campaign, parent: int, verdict: Verdict, value: float, when: float, **kw: object
) -> Candidate:
    row = Candidate(
        campaign_id=campaign.id,
        parent_id=parent,
        tree_sha=f'tree{when}',
        commit_sha=f'{when:07.0f}abcdef',
        summary=f'score {value:g}',
        verdict=verdict,
        **kw,
    )
    s.add(row)
    s.flush()
    s.add(
        Measurement(
            candidate_id=row.id,
            evaluator_id=campaign.evaluator_id,
            metrics={'score': value, 'size': 10.0},
            seconds=60 * value,
            created_at=at(when),
        )
    )
    return row


def live_campaign(db: Database, campaign: Campaign) -> None:
    """Add two hours of a small campaign that has one example of each anomaly."""
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        p.created_at = T
        baseline = s.get_one(Candidate, 1)
        baseline.measurements[0].created_at = T
        good = idea(s, p, 'longer lines', IdeaStatus.DONE)
        first = run(s, good, RunStatus.SUCCEEDED, 1, 5, 20)
        agent(s, p, IMPLEMENTER, 5, 600, run_id=first.id, usage=spent(1000))
        kept = candidate(s, p, 1, Verdict.KEEP, 3, 18, run_id=first.id)
        candidate(s, p, kept.id, Verdict.DISCARD, 3, 19, run_id=first.id)
        bad = idea(s, p, 'crash it', IdeaStatus.REJECTED)
        failed = run(s, bad, RunStatus.FAILED, 1, 30, 40, error='boom\nand the rest')
        dead = agent(s, p, IMPLEMENTER, 30, 5, run_id=failed.id, state=AgentSessionState.KILLED, error='dialog')
        startup = StartupAnomaly(role=IMPLEMENTER, harness='script', run=failed.id, seconds=5.0, error='dialog')
        s.add(
            Event(
                actor='daemon',
                target=TargetRef(TargetKind.AGENT_SESSION, dead.id),
                payload=startup,
                at=at(30) + timedelta(seconds=5),
            )
        )
        pinned = idea(s, p, 'build on longer lines', IdeaStatus.SCHEDULED, after=(good.id,), base_candidate_id=1)
        stuck = run(s, pinned, RunStatus.RUNNING, 1, 50, None)
        agent(s, p, IMPLEMENTER, 50, 300, run_id=stuck.id, usage=spent(2000))
        idea(s, p, 'after the crash', IdeaStatus.READY, after=(bad.id,))
        agent(s, p, RESEARCH, 60, 120, usage=spent(500))
        s.add_all(
            [
                Comment(
                    target=TargetRef(TargetKind.RUN, failed.id),
                    author='watchdog#9',
                    kind=CommentKind.ESCALATION,
                    priority=1,
                    body='run#2 died in a dialog\nevidence below',
                    created_at=at(45),
                ),
                Comment(
                    target=TargetRef(TargetKind.CAMPAIGN, p.id),
                    author='pi',
                    kind=CommentKind.DECISION,
                    body='keep going',
                    created_at=at(46),
                ),
                Comment(
                    target=TargetRef(TargetKind.IDEA, good.id),
                    author='implementer#1',
                    body='a note',
                    created_at=at(47),
                ),
            ]
        )
        s.commit()


def render(db: Database, campaign: Campaign, cfg: CampaignConfig | None = None, now: datetime = NOW) -> str:
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        return overview(s, p, cfg or CampaignConfig.model_validate(p.config), now)


def test_the_overview_of_a_campaign(db: Database, campaign: Campaign) -> None:
    live_campaign(db, campaign)
    text = render(db, campaign)
    lay, tools = text.split('### Tools\n')
    assert lay == snapshot("""\
## Campaign overview (computed at 2026-10-01 14:00 UTC)
demo · head score 3 (candidate#2, +2 vs baseline) · 2 kept / 3 candidates · 4k tok · evaluator #1, 0s old
### Goal
- Primary metric: score, higher is better; also measured: size (lower, watch)
- Campaign: active. Progress: run 2 · 1.9h (no limit stops it: open-ended).
- The goal in words is the campaign's brief.md: weigh the metric against it.
### Trajectory
- Start candidate#1, head candidate#2: score 1 → 3 (+2, +200.0%); 1 trunk keeps.
- Last trunk keep 1h ago; 2 runs finished since.
- Trunk keeps (latest 8; when first measured, the gain on its track over the head before it):
  - 10-01 12:00 candidate#1 1 (start): baseline
  - 10-01 12:18 candidate#2 3 (+2) idea#1: score 3
### Now
- Implementer slots: 1 of 1 busy.
  - run#3 idea#3 "build on longer lines" (script): running 1h, agent_session#3 ended 1h ago
- Ideas: 0 runnable, 1 blocked (idea#4 on idea#2 (rejected)); 0 seed, 0 proposed, 1 scheduled, 1 done, 0 parked, 1 rejected.
- Agents live: none
- Agents so far: 3 implementer (1 killed), 1 research, 0 scout, 0 watchdog; 0 of 1 ended research sweeps defined no idea.
- Ideas by source: research 4 ideas, 3 ran → 1 keep, 0 interesting.
- Tokens: 4k (implementer 3k, research 500, scout 0, watchdog 0); 500 by agents that ended in the last hour.
- Evaluations: 3 (0 crashed); median 3.0 min, first 5 3.0 min, latest 5 3.0 min; 6.0 min in all; 0 candidates waiting for a verdict.
### Recent candidates (3 of 3; score, vs parent)
- candidate#1 keep 1 (—), no run, 0s ago: baseline
- candidate#2 keep 3 (+2), run#1, 0s ago: score 3
- candidate#3 discard 3 (+0), run#1, 0s ago: score 3
### Anomalies the daemon detected
- [incident] run#2 failed, 1h ago: failed on script (idea#2): boom
- [incident] agent_session#2 dead at startup, 1h ago: implementer on script failed to start (5.0s): dialog
- candidate#3 scored exactly its parent, 1h ago: scored what candidate#2 did on score and on 1 of 1 other metrics; 3.0 min of evaluation
### Inbox (1 unacknowledged), decisions and escalations
- comment#2 decision on campaign#1 by pi, 1h ago: keep going
- comment#1 escalation (p1, unacknowledged) on run#2 by watchdog#9, 1h ago: run#2 died in a dialog
### Recent events
- comment.note comment#3 0s ago · comment.decision comment#2 0s ago · comment.escalation comment#1 0s ago · idea.linked idea#4 0s ago · idea.linked idea#3 0s ago
""")
    assert tools.startswith('- `piloop sql "<one statement>"`')
    assert '  - agent_sessions(id, kind, campaign_id, run_id,' in tools, (
        'the schema is listed, so an agent does not need PRAGMA table_info'
    )


def test_stop_conditions_say_how_far_each_one_is(db: Database, campaign: Campaign) -> None:
    live_campaign(db, campaign)
    stop = {'target': 5, 'max_runs': 10, 'max_hours': 4, 'max_tokens': 1_000_000, 'plateau': 3}
    cfg = load_campaign_config(Path(campaign.path), plugins={'stop_rule': stop})
    [line] = [x for x in render(db, campaign, cfg).splitlines() if x.startswith('- Campaign:')]
    assert line == snapshot(
        '- Campaign: active. Progress: run 2/10 · 1.9h/4h · 4k/1.0M tok · 0/3 runs without a keep · target ≥ 5 (head 3).'
    ), 'run#2 failed before it committed: it counts toward neither limit'
    with db.session() as s:
        s.get_one(Campaign, campaign.id).status = CampaignStatus.STOPPED
        s.get_one(Campaign, campaign.id).stopped_reason = 'plateau: 3 runs without a keep'
        s.commit()
    assert 'Campaign: stopped — plateau: 3 runs without a keep.' in render(db, campaign, cfg)


def test_a_fresh_campaign_has_little_to_say(db: Database, campaign: Campaign) -> None:
    """A campaign with no run, no comment and no trunk keep shows no trajectory and reports an idle queue."""
    stop = {'max_hours': 4, 'plateau': 2}
    cfg = load_campaign_config(Path(campaign.path), plugins={'stop_rule': stop})
    with db.session() as s:
        s.get_one(Campaign, campaign.id).created_at = T
        s.get_one(Candidate, 1).verdict = Verdict.PENDING
        s.commit()
    text = render(db, campaign, cfg)
    assert '- Campaign: active. Progress: run 0 · 0.0h/4h · 0/2 runs without a keep.' in text
    assert "- No trunk keep: the baseline's verdict was overridden and nothing was kept since." in text
    assert '- Evaluations: 1 (0 crashed); median' not in text, 'the baseline has no recorded seconds'
    assert '- Evaluations: 1 (0 crashed); 1 candidates waiting for a verdict.' in text
    assert (
        '- campaign#1 idle, 2h ago: 1 free implementer slots and no runnable idea; no research sweep pending' in text
    ), 'not tagged: an idle queue is a cost for the progress review, not an incident'
    assert '### Inbox (0 unacknowledged), decisions and escalations\n- None.\n' in text


def test_no_anomaly_reads_none(db: Database, campaign: Campaign) -> None:
    with db.session() as s:
        s.add(Idea(track='score', campaign_id=campaign.id, title='t', hypothesis='h', created_by='human'))
        s.commit()
    assert '### Anomalies the daemon detected\n- None.\n' in render(db, campaign)


def test_the_idle_queue_counts_from_the_last_run_and_names_pending_research(db: Database, campaign: Campaign) -> None:
    live_campaign(db, campaign)
    with db.session() as s:
        s.get_one(Run, 3).status, s.get_one(Run, 3).end = RunStatus.SUCCEEDED, RunEnd.FINISHED
        s.get_one(Run, 3).finished_at = at(70)
        s.add(AgentSession(kind=RESEARCH, campaign_id=campaign.id, started_at=at(100)))
        s.commit()
        [idle] = [a for a in found(s, campaign) if a.kind is AnomalyKind.IDLE_QUEUE]
    assert (idle.at, idle.detail) == (at(70), '1 free implementer slots and no runnable idea; research#5 requested')


def test_max_runs_reached_is_not_idle(db: Database, campaign: Campaign) -> None:
    """Two runs count toward ``max_runs``. The run that failed with no candidate does not count."""
    live_campaign(db, campaign)
    cfg = load_campaign_config(
        Path(campaign.path), plugins={'stop_rule': {'max_runs': 2}}, roles={'implementer': {'max_concurrent': 4}}
    )
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        assert AnomalyKind.IDLE_QUEUE not in {a.kind for a in anomalies(s, p, cfg)}
        p.status = CampaignStatus.STOPPED
        uncapped = load_campaign_config(Path(campaign.path), roles={'implementer': {'max_concurrent': 4}})
        assert AnomalyKind.IDLE_QUEUE not in {a.kind for a in anomalies(s, p, uncapped)}, 'a stopped campaign'


def test_a_run_that_the_daemon_stopped_is_not_a_failed_run(db: Database, campaign: Campaign) -> None:
    live_campaign(db, campaign)
    with db.session() as s:
        run = s.get_one(Run, 3)
        run.status, run.end, run.finished_at = RunStatus.FAILED, RunEnd.DAEMON_STOPPED, at(70)
        s.commit()
        assert TargetRef(TargetKind.RUN, run.id) not in {
            a.ref for a in found(s, campaign) if a.kind is AnomalyKind.RUN_FAILED
        }


def test_a_run_blocked_on_its_implementers_escalation_is_not_a_failed_run(db: Database, campaign: Campaign) -> None:
    """The escalation is in the inbox already: an incident sweep would only find it again."""
    live_campaign(db, campaign)
    with db.session() as s:
        run = s.get_one(Run, 3)
        run.status, run.end, run.error, run.finished_at = RunStatus.FAILED, RunEnd.AWAITING_ANSWER, 'comment#9', at(70)
        s.commit()
        assert TargetRef(TargetKind.RUN, run.id) not in {
            a.ref for a in found(s, campaign) if a.kind is AnomalyKind.RUN_FAILED
        }


def found(s: Session, campaign: Campaign, now: datetime = NOW) -> list[overview_module.Anomaly]:
    p = s.get_one(Campaign, campaign.id)
    return anomalies(s, p, CampaignConfig.model_validate(p.config))


def test_a_short_life_without_the_launcher_event_is_not_a_startup_death(db: Database, campaign: Campaign) -> None:
    """The launcher decides that an agent died at startup (``StartupAnomaly``). The overview only reports it."""
    with db.session() as s:
        agent(s, s.get_one(Campaign, campaign.id), WATCHDOG, 0, 5)
        s.commit()
        assert [a.key for a in found(s, campaign) if a.kind is AnomalyKind.DEAD_AT_STARTUP] == []


def test_an_agent_that_investigates_incidents_dead_at_startup_is_no_incident(db: Database, campaign: Campaign) -> None:
    """An agent of a role with the ``anomaly`` trigger that died at startup is reported, but it requests no agent.

    Otherwise each such agent that dies at startup requests a new one, which can die the same way. The watchdog has the
    trigger by default; research has not, so a research agent that died at startup is an incident.
    """
    with db.session() as s:
        for kind in (WATCHDOG, RESEARCH):
            dead = agent(s, s.get_one(Campaign, campaign.id), kind, 0, 35, state=AgentSessionState.KILLED)
            payload = StartupAnomaly(role=kind, harness='script', run=None, seconds=35.0, error='prompt_lost')
            s.add(
                Event(
                    actor='daemon',
                    target=TargetRef(TargetKind.AGENT_SESSION, dead.id),
                    payload=payload,
                )
            )
        s.commit()
        dead_at_startup = {a.name: a.incident for a in found(s, campaign) if a.kind is AnomalyKind.DEAD_AT_STARTUP}
        assert dead_at_startup == {'agent_session#1 dead at startup': False, 'agent_session#2 dead at startup': True}


def test_a_crashed_measurement_or_an_unmeasured_parent_is_not_same_as_parent(db: Database, campaign: Campaign) -> None:
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        orphan = Candidate(campaign_id=p.id, parent_id=1, tree_sha='x', commit_sha='x', verdict=Verdict.CRASH)
        s.add(orphan)
        s.flush()
        s.add(Measurement(candidate_id=orphan.id, evaluator_id=p.evaluator_id, metrics={}, error='died'))
        s.commit()
        assert found(s, campaign) == [a for a in found(s, campaign) if a.kind is not AnomalyKind.SAME_AS_PARENT]


def test_the_anomaly_list_shows_the_latest_and_counts_the_rest(db: Database, campaign: Campaign) -> None:
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        for n in range(overview_module.ANOMALIES_SHOWN + 2):
            run(s, idea(s, p, f'idea {n}', IdeaStatus.DONE), RunStatus.TIMED_OUT, 1, n, n + 1)
        idea(s, p, 'next', IdeaStatus.READY)
        s.commit()
    text = render(db, campaign)
    assert 'run#14 failed' in text and 'run#3 failed' in text and 'run#2 failed' not in text
    assert '- … and 2 older incidents.\n' in text
    assert 'timed_out on script (idea#14): no error recorded' in text


def test_costs_of_one_kind_share_a_line(db: Database, campaign: Campaign) -> None:
    live_campaign(db, campaign)
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        for n in range(overview_module.REFS_SHOWN):
            candidate(s, p, 2, Verdict.DISCARD, 3, 30 + n, run_id=1)
        s.commit()
    [line] = [x for x in render(db, campaign).splitlines() if 'scored exactly its parent' in x]
    assert line == snapshot(
        '- 9 × scored exactly its parent (candidate#11, candidate#10, candidate#9, candidate#8, candidate#7, candidate#6, candidate#5, candidate#4, …); the latest, 1h ago: scored what candidate#2 did on score and on 1 of 1 other metrics; 3.0 min of evaluation'
    )


def test_blocked_ideas_beyond_the_shown_few_are_counted(db: Database, campaign: Campaign) -> None:
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        gate = idea(s, p, 'gate', IdeaStatus.PARKED)
        for n in range(overview_module.BLOCKED_SHOWN + 1):
            idea(s, p, f'waits {n}', IdeaStatus.READY, after=(gate.id,))
        s.commit()
    assert ', 1 more); 0 seed, 0 proposed' in render(db, campaign)


def test_open_runs_say_how_far_they_got(db: Database, campaign: Campaign) -> None:
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        waiting = run(s, idea(s, p, 'waiting', IdeaStatus.SCHEDULED), RunStatus.QUEUED, 1, 100, None)
        waiting.started_at = None
        going = run(s, idea(s, p, 'going', IdeaStatus.SCHEDULED), RunStatus.RUNNING, 1, 110, None)
        s.add(AgentSession(kind=IMPLEMENTER, campaign_id=p.id, run_id=going.id, state=AgentSessionState.RUNNING))
        s.commit()
    cfg = load_campaign_config(Path(campaign.path), roles={'implementer': {'max_concurrent': 2}})
    lines = render(db, campaign, cfg).splitlines()
    start = lines.index('- Implementer slots: 2 of 2 busy.')
    assert lines[start + 1 : start + 3] == [
        '  - run#1 idea#1 "waiting" (script): queued',
        '  - run#2 idea#2 "going" (script): running 10m',
    ]


def test_branches_and_notes_say_where_else_a_run_may_start(db: Database, campaign: Campaign) -> None:
    """A branch keep starts its own lineage, and a note shows a candidate that regressed.

    A keep that the current evaluator has not measured yet is in no lineage.
    """
    live_campaign(db, campaign)
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        explore = idea(s, p, 'from the baseline', IdeaStatus.DONE, base_candidate_id=1)
        branch = run(s, explore, RunStatus.SUCCEEDED, 1, 60, 70, branch=True)
        candidate(s, p, 1, Verdict.KEEP, 2, 65, run_id=branch.id)
        noted = candidate(s, p, 1, Verdict.INTERESTING, 0.5, 66, run_id=branch.id, verdict_by=f'run#{branch.id}')
        noted.verdict_reason = 'note: halves the eval time; against candidate#1: score 0.5 vs 1'
        s.add(
            Candidate(campaign_id=p.id, tree_sha='unmeasured', commit_sha='u', verdict=Verdict.KEEP, run_id=branch.id)
        )
        s.commit()
    trajectory = render(db, campaign).split('### Trajectory\n')[1].split('### Now')[0]
    assert trajectory == snapshot("""\
- Start candidate#1, head candidate#2: score 1 → 3 (+2, +200.0%); 1 trunk keeps.
- Last trunk keep 1h ago; 3 runs finished since.
- Trunk keeps (latest 8; when first measured, the gain on its track over the head before it):
  - 10-01 12:00 candidate#1 1 (start): baseline
  - 10-01 12:18 candidate#2 3 (+2) idea#1: score 3
- Lineages (latest keep of each; a branch keep never moves the head):
  - trunk from candidate#1 (keep): latest candidate#2 3, 2 kept
  - branch from candidate#1 (keep): latest candidate#6 —, 2 kept
- Notes (regressed, kept in view as leads; pin one with base_candidate_id):
  - candidate#5 0.5 by run#4: halves the eval time; against candidate#1: score 0.5 vs 1
""")


def test_the_queue_the_live_agents_the_inbox_and_the_events(db: Database, campaign: Campaign, tmp_path: Path) -> None:
    """The Now, Inbox and Recent events sections show every row with its age."""
    live_campaign(db, campaign)
    log_dir = tmp_path / 'research-6'
    log_dir.mkdir()
    (log_dir / 'transcript.txt').write_text('thinking\n\nran evaluate.py: METRIC score=3\n\n')
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        s.get_one(Idea, 2).status = IdeaStatus.DONE  # unblocks idea#4
        idea(s, p, 'a' * 120, IdeaStatus.READY, priority=0, created_at=at(100))
        newer = idea(s, p, 'even longer lines', IdeaStatus.READY, priority=1, created_at=at(110))
        s.add(IdeaLink(idea_id=newer.id, target_id=4, kind=LinkKind.SUPERSEDES))
        s.add_all(
            [
                AgentSession(
                    kind=RESEARCH,
                    campaign_id=p.id,
                    harness='script',
                    model='m',
                    runtime='subprocess',
                    state=AgentSessionState.RUNNING,
                    started_at=at(115),
                    reason='low_water: 2 ready < 10',
                    log_dir=str(log_dir),
                ),
                AgentSession(kind=WATCHDOG, campaign_id=p.id, state=AgentSessionState.REQUESTED, reason='cron'),
            ]
        )
        s.add(Event(actor='cli', target=TargetRef(TargetKind.IDEA, newer.id), payload=IdeaCreated(), at=at(110)))
        s.commit()
    text = render(db, campaign)
    now = text[text.index('### Now') : text.index('### Anomalies')]
    assert now == snapshot("""\
### Now
- Implementer slots: 1 of 1 busy.
  - run#3 idea#3 "build on longer lines" (script): running 1h, agent_session#3 ended 1h ago
- Ideas: 3 runnable, 0 blocked; 0 seed, 0 proposed, 1 scheduled, 2 done, 0 parked, 0 rejected.
  - idea#5 p0 "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa…" by research#4, 20m ago
  - idea#6 p1 "even longer lines" by research#4, 10m ago; supersedes idea#4
  - idea#4 p2 "after the crash" by research#4, 0s ago; after idea#2; superseded by idea#6
- Agents live: 1; requested: watchdog#6
  - research#5 script/m 5m, on low_water: 2 ready < 10: ran evaluate.py: METRIC score=3
- Agents so far: 3 implementer (1 killed), 2 research, 0 scout, 1 watchdog; 0 of 1 ended research sweeps defined no idea.
- Ideas by source: research 6 ideas, 3 ran → 1 keep, 0 interesting.
- Tokens: 4k (implementer 3k, research 500, scout 0, watchdog 0); 500 by agents that ended in the last hour.
- Evaluations: 3 (0 crashed); median 3.0 min, first 5 3.0 min, latest 5 3.0 min; 6.0 min in all; 0 candidates waiting for a verdict.
### Recent candidates (3 of 3; score, vs parent)
- candidate#1 keep 1 (—), no run, 0s ago: baseline
- candidate#2 keep 3 (+2), run#1, 0s ago: score 3
- candidate#3 discard 3 (+0), run#1, 0s ago: score 3
""")
    assert text[text.index('### Inbox') :].split('### Tools')[0] == snapshot("""\
### Inbox (1 unacknowledged), decisions and escalations
- comment#2 decision on campaign#1 by pi, 1h ago: keep going
- comment#1 escalation (p1, unacknowledged) on run#2 by watchdog#9, 1h ago: run#2 died in a dialog
### Recent events
- idea.linked idea#6 0s ago · idea.created idea#6 10m ago · idea.status idea#2 0s ago · comment.note comment#3 0s ago · comment.decision comment#2 0s ago
""")


def test_age_floors_to_the_largest_unit() -> None:
    assert overview_module.age(NOW, NOW) == '0s'
    assert overview_module.age(NOW - timedelta(seconds=59), NOW) == '59s'
    assert overview_module.age(NOW - timedelta(minutes=3, seconds=59), NOW) == '3m'
    assert overview_module.age(NOW - timedelta(hours=23, minutes=59), NOW) == '23h'
    assert overview_module.age(NOW - timedelta(days=40), NOW) == '40d'
    assert overview_module.age(NOW + timedelta(hours=1), NOW) == '0s', 'a clock skew is not a negative age'


SESSION_ID = 'd96a2121-355c-4608-b2f7-8bfc85bba1b5'


def test_live_agents_say_their_last_words_from_the_session_file_else_their_output_else_their_transcript(
    db: Database, campaign: Campaign, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setenv('CLAUDE_CONFIG_DIR', str(tmp_path / '.claude'))
    session = tmp_path / '.claude' / 'projects' / '-somewhere' / f'{SESSION_ID}.jsonl'
    session.parent.mkdir(parents=True)
    session.write_text((Path(__file__).parent.parent / 'fixtures' / 'said' / 'claude.jsonl').read_text())
    writing, saved, silent = tmp_path / 'writing', tmp_path / 'saved', tmp_path / 'silent'
    for log_dir in (writing, saved, silent):
        log_dir.mkdir()
    (writing / 'agent_session.stdout').write_text('{"type":"turn.started"}\n\n{"type":"item.completed"}\n')
    (saved / 'transcript.txt').write_text('Ran the evaluation; measurement pending.\n')
    with db.session() as s:
        for harness, session_id, log_dir in (
            ('claude', SESSION_ID, silent),
            ('claude', 'no-file-yet', writing),
            ('codex', None, saved),
            ('script', None, silent),
        ):
            s.add(
                AgentSession(
                    kind=RESEARCH,
                    campaign_id=campaign.id,
                    state=AgentSessionState.RUNNING,
                    harness=harness,
                    model='m',
                    session_id=session_id,
                    runtime='subprocess',
                    handle={'pid': 1, 'start_time': 0, 'log_dir': str(log_dir)},
                    log_dir=str(log_dir),
                    reason='cron',
                    started_at=at(100),
                )
            )
        s.commit()
    assert [line for line in render(db, campaign).splitlines() if line.startswith('  - research#')] == snapshot(
        [
            '  - research#1 claude/m 20m, on cron: Starting both training runs (16 processes each).',
            '  - research#2 claude/m 20m, on cron: {"type":"item.completed"}',
            '  - research#3 codex/m 20m, on cron: Ran the evaluation; measurement pending.',
            '  - research#4 script/m 20m, on cron: —',
        ]
    )


def test_a_row_holding_a_blob_renders_as_a_placeholder_with_one_warning_naming_it(
    db: Database, campaign: Campaign, capsys: pytest.CaptureFixture[str]
) -> None:
    """A comment body stored as bytes renders as a placeholder and prints one warning. It does not crash."""
    with db.session() as s:
        s.add(Comment(target=TargetRef(TargetKind.CAMPAIGN, campaign.id), author='w', body='x', priority=1))
        s.commit()
    with db.engine.begin() as con:  # The CHECK constraints reject a blob, so turn them off to insert one.
        con.exec_driver_sql('PRAGMA ignore_check_constraints = ON')
        con.exec_driver_sql("UPDATE comments SET body = x'00ff' WHERE id = 1")
        con.exec_driver_sql('PRAGMA ignore_check_constraints = OFF')
    assert '- comment#1 note (p1, unacknowledged) on campaign#1 by w, 0s ago: <not text: a 2-byte blob>' in render(
        db, campaign
    )
    render(db, campaign)
    assert capsys.readouterr().err == (
        'piloop: warning: comments#1 body is a 2-byte blob, not text; shown as a placeholder. Repair it:'
        ' UPDATE comments SET body = CAST(body AS TEXT) WHERE id = 1\n'
    ), 'the warning prints once per process'
    with db.session() as s:
        s.get_one(Comment, 1).priority = 2
        s.commit()
    with db.engine.connect() as con:
        assert con.exec_driver_sql('SELECT typeof(body) FROM comments').scalar() == 'blob', 'never written back'


def test_progress_lists_only_the_configured_conditions(db: Database, campaign: Campaign) -> None:
    def config(stop: dict[str, object], direction: str = 'higher') -> CampaignConfig:
        metrics = [{'name': 'score', 'direction': direction, 'role': 'primary'}, {'name': 'size', 'direction': 'lower'}]
        return CampaignConfig.model_validate({**campaign.config, 'plugins': {'stop_rule': stop}, 'metrics': metrics})

    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        assert (
            overview_module.progress(s, p, config({'target': 0.5}, 'lower'), NOW)
            == 'run 0 · 0.0h · target ≤ 0.5 (head 1)'
        )
        assert overview_module.progress(s, p, config({}), NOW) == 'run 0 · 0.0h'
        s.get_one(Candidate, 1).verdict = Verdict.PENDING
        assert overview_module.progress(s, p, config({'target': 0.5}), NOW) == 'run 0 · 0.0h · target ≥ 0.5 (head —)'


def test_block_keeps_whole_lines_and_counts_the_rest() -> None:
    assert block('  \n') == '—'
    assert block('a\nb\n\nc') == 'a\nb\n\nc'
    assert block('a\n' + 'b' * 10 + '\nc\nd', 8) == 'a\n… (3 more lines)'
    assert block('x' * 500 + '\ny', 8) == 'xxxxxxx…\n… (1 more lines)'
    assert block('\n'.join(['r' * 79] * 30)) == '\n'.join(['r' * 79] * 10) + '\n… (20 more lines)'
    assert len(block('x' * 2000)) == LINKED_CHARS
