"""Each limit of the stop rule ends the campaign exactly as the configuration reference says. A stopped campaign requests nothing."""

import ast
import re
import sys
from datetime import timedelta
from importlib.metadata import EntryPoint
from pathlib import Path

import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from piloop_keep_cost import KeepCost
from piloop_stop_after_keeps import AfterKeeps
from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from piloop.actor import Agent, System
from piloop.campaign.memory import Synced
from piloop.campaign.ops import pause
from piloop.campaign.stop import (
    StopRequested,
    check as check_campaign,
    gather,
    hours,
    hours_due,
    request_stop,
    requested,
    runs,
    runs_without_keep,
    stall,
    timeout as agent_timeout,
)
from piloop.config import CampaignConfig, ConfigError, load_campaign_config
from piloop.daemon.run_controller import NO_CHANGES, SKIPPED_REASON, RunFinished
from piloop.db.events import has, in_order
from piloop.db.models import (
    AgentSession,
    AgentSessionState,
    Campaign,
    Candidate,
    Comment,
    Document,
    Event,
    Idea,
    Measurement,
    Origin,
    Pause,
    Run,
    RunEnd,
    RunStatus,
    utcnow,
)
from piloop.db.requests import RequestAnswered, pending
from piloop.db.session import Database
from piloop.db.triggers.changelog import CampaignPausedChanged, CampaignStatusChanged
from piloop.ids import (
    IMPLEMENTER,
    CampaignStatus,
    CandidateId,
    CommentKind,
    Ending,
    IdeaStatus,
    RunId,
    TargetKind,
    TargetRef,
)
from piloop.otel import Tracer
from piloop.plugins import host
from piloop.plugins.fact_source.contract import FactReadError
from piloop.plugins.fact_source.facts import (
    ATTEMPTS,
    CAMPAIGN,
    EVALUATIONS,
    FUNNEL,
    HOURS,
    KEEPS,
    METRIC_SUMS,
    OPTIMIZATION,
    RUNS,
    TOKENS,
    UNSETTLED,
    Facts,
    FactValue,
    Funnel,
    per_evaluation,
    per_keep,
    per_run,
    reading,
    since_keep,
    worker_spend,
)
from piloop.plugins.fact_source.kit.readings import FactRead, Readings
from piloop.plugins.host import data_errors
from piloop.plugins.metric import Direction
from piloop.plugins.stop_rule import limits as limits_module
from piloop.plugins.stop_rule.contract import GO, STOP_RULES, Escalate, Hold, Stop, StopCase, StopRule
from piloop.plugins.stop_rule.limits import Limits
from piloop.plugins.verdict_rule.contract import Outcome
from piloop.reports.overview import progress
from tests.conftest import (
    HARNESS_APPEND,
    HARNESS_NOOP,
    HARNESS_WORSE,
    RESEARCH,
    WATCHDOG,
    ScriptRuntime,
    end_of,
    example,
    span_named,
    spent,
    write_toml,
)
from tests.daemon.test_roles import PLUGIN, installed
from tests.daemon.test_scheduler import add_ideas, make_scheduler, set_config, stop_campaign

LOWER_PRIMARY = [{'name': 'score', 'direction': 'higher'}, {'name': 'size', 'direction': 'lower', 'role': 'primary'}]


def configure(
    db: Database,
    campaign: Campaign,
    max_implementers: int = 1,
    harness: str = HARNESS_NOOP,
    roles: dict[str, object] | None = None,
    **stop: float,
) -> CampaignConfig:
    write_toml(Path(campaign.path), harness, max_implementers=max_implementers)
    with db.session() as s:
        cfg = load_campaign_config(Path(campaign.path), plugins={'stop_rule': stop}, roles=roles)
        s.get_one(Campaign, campaign.id).config = cfg
        s.commit()
    return cfg


def add_run(
    db: Database,
    campaign: Campaign,
    *,
    status: RunStatus = RunStatus.SUCCEEDED,
    started_at: timedelta = timedelta(0),
    finished_at: timedelta = timedelta(0),
    keep_score: float | None = None,
    end: RunEnd | None = None,
    error: str | None = None,
    measured: bool = True,
) -> RunId:
    """Add a run, finished by default, with a measured candidate: a discard, or a keep that scores ``keep_score``.

    With ``measured=False`` the run has no candidate. Add one with ``add_candidate``. A run in flight has none.
    ``started_at`` and ``finished_at`` are how long ago.
    """
    with db.session() as s:
        n = s.scalar(select(func.count(Idea.id))) or 0
        idea = Idea(
            track='score',
            campaign_id=campaign.id,
            title=f'idea {n}',
            hypothesis='t',
            created_by='t',
            status=IdeaStatus.DONE,
        )
        s.add(idea)
        s.flush()
        in_flight = status in (RunStatus.QUEUED, RunStatus.RUNNING)
        run = Run(
            idea_id=idea.id,
            attempt=1,
            base_candidate_id=1,
            harness='h',
            model='m',
            status=status,
            end=end or end_of(status),
            error=error,
            started_at=utcnow() - started_at,
            finished_at=None if in_flight else utcnow() - finished_at,
        )
        s.add(run)
        s.commit()
    if keep_score is not None or (measured and not in_flight):
        verdict = Outcome.DISCARD if keep_score is None else Outcome.KEEP
        candidate_id = add_candidate(db, campaign, run.id, verdict)
        with db.session() as s:
            evaluator_id = s.get_one(Campaign, campaign.id).evaluator_id or 0
            s.add(Measurement(candidate_id=candidate_id, evaluator_id=evaluator_id, metrics={'score': keep_score or 0}))
            s.commit()
    return run.id


def add_candidate(
    db: Database, campaign: Campaign, run_id: RunId, state: Outcome | Origin, why: str | None = None
) -> CandidateId:
    """Add an unmeasured candidate of run ``run_id``, a child of the baseline: with a verdict, or of an origin."""
    match state:
        case Outcome():
            origin, verdict = Origin.RUN, state
        case Origin():
            origin, verdict = state, None
    with db.session() as s:
        candidate = Candidate(
            campaign_id=campaign.id,
            run_id=run_id,
            parent_id=1,
            tree_sha=f'tree{run_id}',
            revision=f'commit{run_id}',
            origin=origin,
            verdict=verdict,
            verdict_reason=why,
        )
        s.add(candidate)
        s.commit()
        return candidate.id


def reason(db: Database, campaign: Campaign) -> str | None:
    """Return why the stop rule stops the campaign now, or None when it goes on. These campaigns have no ``[spend]``."""
    with db.session() as s:
        row = s.get_one(Campaign, campaign.id)
        answer = check_campaign(s, row, CampaignConfig.model_validate(row.config))
    assert not isinstance(answer, Hold), 'nothing here holds launches'
    return answer.reason if isinstance(answer, Stop) else None


def progress_line(db: Database, campaign: Campaign, cfg: CampaignConfig) -> str:
    """Return the first part of the progress line: the runs against ``max_runs``."""
    with db.session() as s:
        return progress(s, s.get_one(Campaign, campaign.id), cfg, utcnow()).split(' · ')[0]


def campaign_state(db: Database, campaign: Campaign) -> tuple[CampaignStatus, str | None]:
    with db.session() as s:
        row = s.get_one(Campaign, campaign.id)
        return row.status, row.stopped_reason


def test_no_stop_table_means_open_ended(db: Database, campaign: Campaign) -> None:
    configure(db, campaign)
    add_run(db, campaign, keep_score=100.0, started_at=timedelta(days=30))
    for _ in range(3):
        add_run(db, campaign, status=RunStatus.FAILED)
    assert reason(db, campaign) is None


def test_target_on_the_best_kept_candidate_in_the_metric_direction(db: Database, campaign: Campaign) -> None:
    configure(db, campaign, target=3.0)
    with db.session() as s:
        s.get_one(Candidate, 1).verdict = Outcome.DISCARD
        s.commit()
    assert reason(db, campaign) is None, 'nothing kept'
    with db.session() as s:
        s.get_one(Candidate, 1).verdict = Outcome.KEEP
        s.commit()
    assert reason(db, campaign) is None, 'the baseline scores 1'
    add_run(db, campaign, keep_score=2.9)
    assert reason(db, campaign) is None
    with db.session() as s:
        s.get_one(Candidate, 2).verdict = Outcome.DISCARD
        s.commit()
    add_run(db, campaign, keep_score=3.0)
    assert reason(db, campaign) == 'target: score 3 ≥ 3'
    with db.session() as s:
        s.get_one(Candidate, 3).verdict = Outcome.INTERESTING
        s.commit()
    assert reason(db, campaign) is None, 'only a keep counts'
    with db.session() as s:
        c = s.get_one(Candidate, 3)
        c.verdict, c.origin = None, Origin.REFERENCE
        s.commit()
    assert reason(db, campaign) is None, 'a reference is never the best'
    with db.session() as s:
        c = s.get_one(Candidate, 3)
        c.verdict, c.origin = Outcome.KEEP, Origin.RUN
        for m in s.scalars(select(Measurement).where(Measurement.candidate_id == 3)):
            m.metrics = {'score': 3.4}
        s.commit()
    assert reason(db, campaign) == 'target: score 3.4 ≥ 3'


def test_target_lower_is_better(db: Database, campaign: Campaign) -> None:
    with db.session() as s:
        cfg = load_campaign_config(Path(campaign.path), metrics=LOWER_PRIMARY, plugins={'stop_rule': {'target': 5}})
        s.get_one(Campaign, campaign.id).config = cfg
        s.commit()
    assert reason(db, campaign) == 'target: size 5 ≤ 5', "the baseline's solve.txt is five bytes"


def test_a_campaign_that_max_runs_stopped_shows_all_its_runs_in_the_progress_line(
    db: Database, campaign: Campaign
) -> None:
    """The progress line counts the runs as ``max_runs`` does, and a stop never ends a run that ``max_runs`` counts."""
    cfg = configure(db, campaign, max_runs=2)
    add_run(db, campaign)
    in_flight = add_run(db, campaign, status=RunStatus.RUNNING)
    assert (reason(db, campaign), progress_line(db, campaign, cfg)) == (None, 'run 2/2'), (
        'the run in flight counts, but it can still end without a scored candidate, so the campaign goes on'
    )
    with db.session() as s:
        run = s.get_one(Run, in_flight)
        run.status, run.end, run.error = RunStatus.FAILED, RunEnd.FAILED, NO_CHANGES
        run.finished_at = utcnow()
        s.commit()
    assert (reason(db, campaign), progress_line(db, campaign, cfg)) == (None, 'run 1/2'), 'a run without a commit'
    add_run(db, campaign)
    assert (reason(db, campaign), progress_line(db, campaign, cfg)) == ('max_runs: 2 runs', 'run 2/2')


def test_max_runs_waits_for_a_finished_run_whose_commits_are_scored(db: Database, campaign: Campaign) -> None:
    """While the commits of a finished run are scored (``runs.scoring``), the run counts but cannot stop the campaign.

    Its idea's status does not matter: a human may park or reject it meanwhile.
    """
    configure(db, campaign, max_runs=1)
    run_id = add_run(db, campaign, measured=False)

    def scoring(on: bool, idea: IdeaStatus) -> tuple[str | None, int]:
        with db.session() as s:
            run = s.get_one(Run, run_id)
            run.scoring, run.idea.status = on, idea
            s.commit()
            return reason(db, campaign), runs(s, s.get_one(Campaign, campaign.id))

    assert scoring(True, IdeaStatus.SCHEDULED) == (None, 1), 'its candidates do not exist yet, but it keeps its place'
    assert scoring(True, IdeaStatus.PARKED) == (None, 1), 'parked meanwhile: it still keeps its place'
    add_candidate(db, campaign, run_id, Origin.SKIPPED, SKIPPED_REASON)
    assert scoring(False, IdeaStatus.DONE) == (None, 0), 'only skip commits: it does not count, and another run starts'
    assert scoring(False, IdeaStatus.SCHEDULED) == (None, 0), 'scored: the status of its idea does not matter'


def test_max_runs_caps_the_runs_queued_in_one_tick(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    configure(db, campaign, max_implementers=4, max_runs=1)
    add_ideas(db, campaign, 'a', 'b', 'c', 'd')
    sched = make_scheduler(db, runtime, tmp_path)
    assert sched.run_all() == [1], 'four free slots, but max_runs leaves room for one run'
    sched.wait()
    with db.session() as s:
        assert s.scalars(select(Run.id)).all() == [1]
    sched.stop(timeout=5)


def test_max_runs_leaves_room_for_the_remaining_runs_only(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    configure(db, campaign, max_implementers=4, max_runs=3)
    add_run(db, campaign)
    add_run(db, campaign)
    add_ideas(db, campaign, 'a', 'b')
    sched = make_scheduler(db, runtime, tmp_path)
    assert sched.run_all() == [1], 'two runs already: max_runs 3 leaves room for one more'
    sched.wait()
    with db.session() as s:
        assert s.scalar(select(func.count(Run.id))) == 3
    sched.stop(timeout=5)


def test_max_hours_since_the_first_run_started(db: Database, campaign: Campaign) -> None:
    configure(db, campaign, max_hours=12)
    assert reason(db, campaign) is None, 'the clock starts with the first run'
    add_run(db, campaign, started_at=timedelta(hours=11, minutes=59))
    assert reason(db, campaign) is None
    add_run(db, campaign, started_at=timedelta(hours=12, minutes=30))
    assert reason(db, campaign) == 'max_hours: 12.5h since the first run'


def test_the_pass_comes_back_when_max_hours_is_reached(db: Database, campaign: Campaign) -> None:
    """``max_hours`` is the one limit that time alone reaches: the pass returns when it will."""
    configure(db, campaign, max_hours=12)
    now = utcnow()
    with db.session() as s:
        row = s.get_one(Campaign, campaign.id)
        assert hours_due(s, row, row.config, now) is None, 'no run yet: no clock'
    add_run(db, campaign, started_at=timedelta(hours=11))
    with db.session() as s:
        row = s.get_one(Campaign, campaign.id)
        due = hours_due(s, row, row.config, now)
        assert due is not None and abs(due - (now + timedelta(hours=1))) < timedelta(minutes=1)
        row.paused, row.paused_reason = Pause.ALL, 'night'
        assert hours_due(s, row, row.config, now) is None, 'paused time does not count'


def test_the_time_a_campaign_was_paused_does_not_count_toward_max_hours(db: Database, campaign: Campaign) -> None:
    configure(db, campaign, max_hours=12)
    add_run(db, campaign, started_at=timedelta(hours=12, minutes=30))
    assert reason(db, campaign) == 'max_hours: 12.5h since the first run'
    now = utcnow()

    def paused(hours_ago: float, new: Pause | None) -> None:
        with db.session() as s:
            payload = CampaignPausedChanged(old=None, new=new, paused_reason=None)
            s.add(
                Event(
                    actor='t',
                    target=TargetRef(TargetKind.CAMPAIGN, campaign.id),
                    payload=payload,
                    at=now - timedelta(hours=hours_ago),
                )
            )
            s.commit()

    paused(14, Pause.LAUNCHES)  # before the first run: only its last half hour counts
    paused(12, None)
    paused(9, Pause.ALL)
    paused(8.9, Pause.LAUNCHES)  # a change of what it holds goes on with the same pause
    paused(8.75, None)

    def elapsed() -> float | None:
        with db.session() as s:
            value = hours(s, s.get_one(Campaign, campaign.id), now)
            return None if value is None else round(value, 2)

    assert (elapsed(), reason(db, campaign)) == (11.75, None), '12.5h less half an hour and a quarter paused'
    paused(1, Pause.LAUNCHES)  # still paused: up to now
    assert elapsed() == 10.75


def test_max_tokens_sums_the_usage_of_every_ended_agent_of_the_campaign(db: Database, campaign: Campaign) -> None:
    configure(db, campaign, max_tokens=100_000)
    assert reason(db, campaign) is None, 'nothing spent yet'
    with db.session() as s:
        other = Campaign(name='other', path='/other', config=campaign.config)
        s.add(other)
        s.flush()
        ended = dict(state=AgentSessionState.EXITED, ended_at=utcnow())
        s.add_all(
            [
                AgentSession(kind=IMPLEMENTER, campaign_id=campaign.id, usage=spent(60_000, 9), **ended),
                AgentSession(kind=RESEARCH, campaign_id=campaign.id, usage=spent(30_000, 2), **ended),
                AgentSession(kind=WATCHDOG, campaign_id=campaign.id, usage=None, **ended),
                AgentSession(kind=IMPLEMENTER, campaign_id=other.id, usage=spent(50_000, 1), **ended),
            ]
        )
        s.commit()
    assert reason(db, campaign) is None, "90k: every role counts, another campaign's agents do not, unknown usage is 0"
    with db.session() as s:
        s.add(AgentSession(kind=WATCHDOG, campaign_id=campaign.id, usage=spent(10_000, 1), **ended))
        s.commit()
    assert reason(db, campaign) == 'max_tokens: 100000 tokens spent'


def test_plateau_counts_finished_runs_since_the_last_keep(db: Database, campaign: Campaign) -> None:
    configure(db, campaign, plateau=2)
    add_run(db, campaign, status=RunStatus.FAILED)
    add_run(db, campaign, status=RunStatus.RUNNING)
    assert reason(db, campaign) is None, 'a run in flight has not failed to keep yet'
    add_run(db, campaign, status=RunStatus.TIMED_OUT)
    assert reason(db, campaign) == 'plateau: 2 runs without a keep'
    add_run(db, campaign, keep_score=5.0)
    assert reason(db, campaign) is None, 'a keep resets the count'
    add_run(db, campaign, status=RunStatus.SUCCEEDED)
    assert reason(db, campaign) is None
    add_run(db, campaign, status=RunStatus.SUCCEEDED)
    assert reason(db, campaign) == 'plateau: 2 runs without a keep'
    with db.session() as s:
        s.add(Candidate(campaign_id=campaign.id, tree_sha='imported', revision='c', origin=Origin.REFERENCE))
        s.commit()
    assert reason(db, campaign) == 'plateau: 2 runs without a keep', 'an imported reference resets nothing'


def test_only_runs_that_produced_a_measured_candidate_count_toward_max_runs(db: Database, campaign: Campaign) -> None:
    """A run with no commit, or with only ``Piloop-Eval: skip`` commits, counts toward neither ``max_runs`` nor
    ``plateau``.

    A run in flight counts toward ``max_runs``, so the queue never starts more runs than ``max_runs`` allows. A run
    whose candidate waits for its measurement also counts.
    """
    configure(db, campaign, max_runs=3)
    add_run(db, campaign, status=RunStatus.FAILED, error=NO_CHANGES, measured=False)
    add_candidate(db, campaign, add_run(db, campaign, measured=False), Origin.SKIPPED, SKIPPED_REASON)
    with db.session() as s:
        row = s.get_one(Campaign, campaign.id)
        assert (runs(s, row), runs_without_keep(s, row)) == (0, 0)
    add_run(db, campaign)
    assert reason(db, campaign) is None, 'one measured run'
    add_candidate(db, campaign, add_run(db, campaign, measured=False), Origin.RUN)
    assert reason(db, campaign) is None, 'two'
    add_run(db, campaign, status=RunStatus.RUNNING)
    with db.session() as s:
        assert runs(s, s.get_one(Campaign, campaign.id)) == 3, 'a run in flight counts toward max_runs'


def test_plateau_counts_the_runs_that_max_runs_counts(db: Database, campaign: Campaign) -> None:
    """Only a run that reached the evaluator counts toward a run limit, ``plateau`` as ``max_runs``.

    A run that stopped at a cheap check with no commit, or with only ``Piloop-Eval: skip`` commits, or that a harness
    fault ended before it committed, says nothing about the metric. ``[spend]``, ``max_tokens`` and ``max_hours``
    limit what such runs cost.
    """
    configure(db, campaign, plateau=2)
    add_run(db, campaign, keep_score=5.0)
    add_run(db, campaign, status=RunStatus.FAILED, error=NO_CHANGES, measured=False)
    add_candidate(db, campaign, add_run(db, campaign, measured=False), Origin.SKIPPED, SKIPPED_REASON)
    add_run(db, campaign, status=RunStatus.SUCCEEDED, measured=False)
    add_run(db, campaign, status=RunStatus.FAILED, error='agent exit 1', measured=False)
    assert reason(db, campaign) is None, 'four runs without a keep, none of them measured'
    add_candidate(db, campaign, add_run(db, campaign, measured=False), Origin.RUN)
    assert reason(db, campaign) is None, 'one waits for its measurement'
    add_run(db, campaign, status=RunStatus.FAILED, error='agent exit 1')
    assert reason(db, campaign) == 'plateau: 2 runs without a keep', 'a failed run whose commit was measured counts'


def test_reactivating_a_stopped_campaign_starts_a_fresh_plateau_window(db: Database, campaign: Campaign) -> None:
    """A campaign reactivated after a plateau stop must not stop again on the next tick.

    ``plateau`` counts the runs that finished after the later of the last keep and the last reactivation (a
    ``campaign.status`` event to ``active``, by ``piloop init`` or by SQL). An evaluator change alone resets nothing:
    the campaign still has made no progress.
    """
    configure(db, campaign, plateau=2)
    add_run(db, campaign, keep_score=5.0, finished_at=timedelta(minutes=11))
    add_run(db, campaign, finished_at=timedelta(minutes=10))
    add_run(db, campaign, finished_at=timedelta(minutes=9))
    assert reason(db, campaign) == 'plateau: 2 runs without a keep'
    stop_campaign(db, campaign, 'plateau: 2 runs without a keep')
    with db.session() as s:
        row = s.get_one(Campaign, campaign.id)
        row.status, row.stopped_reason = CampaignStatus.ACTIVE, None
        s.commit()
    assert reason(db, campaign) is None, 'the runs before the reactivation are not counted'
    add_run(db, campaign)
    assert reason(db, campaign) is None
    add_run(db, campaign)
    assert reason(db, campaign) == 'plateau: 2 runs without a keep'


def test_the_stall_mark_is_half_the_plateau_else_two_rounds_of_the_implementer_slots() -> None:
    bare = {
        'name': 'bare',
        'metrics': [{'name': 'a', 'direction': 'higher', 'role': 'primary'}],
        'evaluate': ['true'],
        'model': {'pi': 'm'},
    }
    plateau = {**bare, 'plugins': {'stop_rule': {'plateau': 15}}}
    assert stall(CampaignConfig.model_validate(plateau)) == (
        8,
        "half of the stop rule's limit of 15 runs without a keep",
    )
    agent_sessions = CampaignConfig.model_validate({**bare, 'roles': {'implementer': {'max_concurrent': 3}}})
    assert stall(agent_sessions) == (6, 'two rounds of [roles.implementer] max_concurrent = 3')
    quick = {'implementer': {'max_concurrent': 3}, 'quick': {'runs': True, 'max_concurrent': 2}}
    assert stall(CampaignConfig.model_validate({**bare, 'roles': quick})) == (
        10,
        'two rounds of [roles.quick] max_concurrent + [roles.implementer] max_concurrent = 5',
    )


def test_runs_that_the_daemon_stopped_count_toward_no_budget(db: Database, campaign: Campaign) -> None:
    configure(db, campaign, plateau=1, max_runs=2)
    for end in (RunEnd.DAEMON_STOPPED, RunEnd.STALE, RunEnd.CAMPAIGN_STOPPED):
        add_run(db, campaign, status=RunStatus.FAILED, end=end)
    assert reason(db, campaign) is None, 'a stop, a crash or a campaign stop is not an attempt'
    add_run(db, campaign, status=RunStatus.FAILED, error='evaluate timed out')
    assert reason(db, campaign) == 'plateau: 1 runs without a keep'
    add_run(db, campaign)
    assert reason(db, campaign) == 'max_runs: 2 runs'


def test_plateau_counts_in_completion_order(db: Database, campaign: Campaign) -> None:
    configure(db, campaign, plateau=1)
    add_run(db, campaign)
    add_run(db, campaign, keep_score=5.0, finished_at=timedelta(hours=1))
    assert reason(db, campaign) == 'plateau: 1 runs without a keep', (
        'the earlier-created run finished after the keep, so it counts'
    )


def test_plateau_ignores_runs_that_finished_before_a_later_keep(db: Database, campaign: Campaign) -> None:
    configure(db, campaign, plateau=1)
    add_run(db, campaign, keep_score=5.0)
    add_run(db, campaign, finished_at=timedelta(hours=1))
    assert reason(db, campaign) is None, 'the later-created run finished before the keep, so it does not count'


def test_stopping_drains_the_requests_and_queued_runs_pending_at_that_moment(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    configure(db, campaign, plateau=1)
    add_run(db, campaign)
    (idea,) = add_ideas(db, campaign, 'queued before the stop')
    with db.session() as s:
        s.get_one(Idea, idea).status = IdeaStatus.SCHEDULED
        run = Run(idea_id=idea, attempt=1, base_candidate_id=1, harness='script', model='m')
        s.add(run)
        s.flush()
        s.add(AgentSession(kind=IMPLEMENTER, campaign_id=campaign.id, run_id=run.id, harness='script', model='m'))
        s.add(AgentSession(kind=RESEARCH, campaign_id=campaign.id, reason='low_water before the stop'))
        s.commit()
    sched = make_scheduler(db, runtime, tmp_path)

    assert sched.run_all() == [3], 'only the closing watchdog launches'
    sched.wait()
    with db.session() as s:
        assert [(w.kind, w.state, w.error, w.ended_at is not None) for w in s.scalars(select(AgentSession))] == [
            (IMPLEMENTER, AgentSessionState.KILLED, RunEnd.CAMPAIGN_STOPPED, True),
            (RESEARCH, AgentSessionState.KILLED, RunEnd.CAMPAIGN_STOPPED, True),
            (WATCHDOG, AgentSessionState.EXITED, None, True),
        ]
        run = s.get_one(Run, 2)
        assert (run.status, run.end, run.finished_at is not None) == (RunStatus.FAILED, RunEnd.CAMPAIGN_STOPPED, True)
        assert s.get_one(Idea, idea).status is IdeaStatus.READY
        [(event, finished)] = in_order(s, RunFinished)
        assert (event.target_id, finished) == (2, RunFinished(end=RunEnd.CAMPAIGN_STOPPED, error=None))
        s.add(AgentSession(kind=RESEARCH, campaign_id=campaign.id, reason='by hand, after the stop'))
        s.commit()
    assert sched.run_all() == [4], 'a request made after the stop is explicit and launches'
    sched.wait()
    sched.stop(timeout=5)
    with db.session() as s:
        assert s.get_one(AgentSession, 4).state is AgentSessionState.EXITED


def test_a_requested_stop_ends_the_campaign_as_a_stop_condition_does(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """An orchestrator that set ``campaigns.status`` by hand got no closing review and no drain.
    ``piloop campaign stop`` records the request, and the daemon stops the campaign on the [stop] path."""
    configure(db, campaign)
    add_run(db, campaign)
    (idea,) = add_ideas(db, campaign, 'queued before the stop')
    with db.session() as s:
        s.get_one(Idea, idea).status = IdeaStatus.SCHEDULED
        run = Run(idea_id=idea, attempt=1, base_candidate_id=1, harness='script', model='m')
        s.add(run)
        s.flush()
        s.add(AgentSession(kind=IMPLEMENTER, campaign_id=campaign.id, run_id=run.id, harness='script', model='m'))
        request_stop(s, s.get_one(Campaign, campaign.id), System.HUMAN, reason='machine needed', ending=Ending.WRAP_UP)
        s.commit()
    sched = make_scheduler(db, runtime, tmp_path)
    assert sched.run_all() == [2], 'only the closing watchdog launches'
    sched.wait()
    assert campaign_state(db, campaign) == (CampaignStatus.STOPPED, 'stopped by human: machine needed')
    with db.session() as s:
        assert (s.get_one(Run, 2).end, s.get_one(Idea, idea).status) == (RunEnd.CAMPAIGN_STOPPED, IdeaStatus.READY)
        assert s.get_one(AgentSession, 2).reason == 'campaign stopped: stopped by human: machine needed'
        s.get_one(Campaign, campaign.id).status = CampaignStatus.ACTIVE
        s.commit()
    assert sched.run_all() == [3], 'reactivated: the old request stops nothing, and the queued idea runs'
    sched.wait()
    sched.stop(timeout=5)
    assert campaign_state(db, campaign)[0] is CampaignStatus.ACTIVE
    with db.session() as s:
        request_stop(
            s, s.get_one(Campaign, campaign.id), Agent(role='implementer', session=3), reason='', ending=Ending.NOW
        )
        s.commit()
        assert requested(s, s.get_one(Campaign, campaign.id)) == (Stop('stopped by implementer#3'))
        assert [(e.actor, stop) for e, stop in pending(s, StopRequested)] == [
            ('implementer#3', StopRequested(reason='', ending=Ending.NOW))
        ], 'the one pending ask: the daemon answered the first when it stopped the campaign'
        (first, _), _ = in_order(s, StopRequested)
        assert [answered.ask for _, answered in in_order(s, RequestAnswered)] == [first.id]


def stop_while_held(db: Database, campaign: Campaign, hold: str, review: str | None = None) -> int:
    """Queue a run, hold the campaign's launches (an unknown spend, or a pause), and request a stop. Return the idea.

    The spend command reads ``bill`` next to the campaign, which does not exist yet.
    """
    configure(db, campaign)
    if hold == 'spend':
        bill = str(Path(campaign.path).parent / 'bill')
        source = {'provider': 'command', 'command': ['cat', bill], 'metrics': ['spend']}
        rule = {'hold_at': {'spend': 10}} | ({'while_held': review} if review else {})
        set_config(
            db,
            campaign,
            facts=[{'provider': 'campaign'}, {'provider': 'optimization'}, source],
            plugins={'stop_rule': rule},
        )
    (idea,) = add_ideas(db, campaign, 'queued before the stop')
    with db.session() as s:
        s.get_one(Idea, idea).status = IdeaStatus.SCHEDULED
        s.add(Run(idea_id=idea, attempt=1, base_candidate_id=1, harness='script', model='m'))
        if hold == 'pause':
            pause(s, campaign.id, evals=False, reason='too expensive')
        request_stop(s, s.get_one(Campaign, campaign.id), System.HUMAN, reason='over budget', ending=Ending.WRAP_UP)
        s.commit()
    return idea


def due_now(db: Database, campaign: Campaign) -> None:
    """Make the next spend reading of ``campaign`` due: its readings are an hour old."""
    with db.session() as s:
        readings = select(Event.id).where(has(FactRead), Event.target == TargetRef(TargetKind.CAMPAIGN, campaign.id))
        s.execute(update(Event).where(Event.id.in_(readings)).values(at=utcnow() - timedelta(hours=1)))
        s.commit()


def closing_reviews(db: Database, campaign: Campaign) -> list[AgentSessionState]:
    with db.session() as s:
        rows = s.scalars(
            select(AgentSession).where(AgentSession.campaign_id == campaign.id, AgentSession.kind == WATCHDOG)
        )
        return [w.state for w in rows]


@pytest.mark.parametrize('hold', ['spend', 'pause'])
def test_a_requested_stop_ends_a_held_campaign_and_by_default_its_closing_review_runs(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, hold: str
) -> None:
    """Someone stops a campaign because it spends, so a stop works while the spend holds launches, or a pause does.

    The queued run is dropped. The closing review runs: a pause ends with the stop, and the stop rule's
    ``while_held`` is ``run`` by default.
    """
    idea = stop_while_held(db, campaign, hold)
    sched = make_scheduler(db, runtime, tmp_path)
    assert sched.run_all() == [1], 'only the closing review launches'
    assert campaign_state(db, campaign) == (CampaignStatus.STOPPED, 'stopped by human: over budget')
    with db.session() as s:
        assert (s.get_one(Run, 1).end, s.get_one(Idea, idea).status) == (RunEnd.CAMPAIGN_STOPPED, IdeaStatus.READY)
    sched.wait()
    sched.stop(timeout=5)


def test_closing_review_skip_ends_the_stop_at_once_with_an_escalation(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stop_while_held(db, campaign, 'spend', 'skip')
    synced: list[str] = []

    def sync(_s: Session, c: Campaign, *_: object) -> Synced:
        synced.append(c.name)
        return Synced()

    monkeypatch.setattr('piloop.daemon.scheduler.sync_documents', sync)
    sched = make_scheduler(db, runtime, tmp_path)
    assert sched.run_all() == []
    assert campaign_state(db, campaign) == (CampaignStatus.STOPPED, 'stopped by human: over budget')
    assert closing_reviews(db, campaign) == [], 'no closing review is requested'
    assert synced == ['demo'], 'the [memory] documents are synced, as at every close'
    with db.session() as s:
        last = s.scalars(select(Comment).order_by(Comment.id.desc()).limit(1)).one()
        assert (last.kind, last.priority, last.author) == (CommentKind.ESCALATION, 0, 'daemon')
        assert last.body.startswith(
            'Closing review skipped: the campaign stopped while the stop rule held its launches (spend not known)'
        )
        assert last.body.endswith('(while_held = "skip").')
        reads = len(in_order(s, FactRead))
    due_now(db, campaign)
    sched.run_all()
    with db.session() as s:
        assert len(in_order(s, FactRead)) == reads, 'no review waits: the spend of a stopped campaign is not read'
    sched.stop(timeout=5)


def test_closing_review_wait_starts_the_review_once_the_spend_hold_lifts(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """The daemon goes on reading the spend of a stopped campaign whose closing review waits."""
    stop_while_held(db, campaign, 'spend', 'wait')
    sched = make_scheduler(db, runtime, tmp_path)
    assert sched.run_all() == []
    assert campaign_state(db, campaign)[0] is CampaignStatus.STOPPED
    assert closing_reviews(db, campaign) == [AgentSessionState.REQUESTED], 'requested, waiting for the hold'
    (Path(campaign.path).parent / 'bill').write_text('METRIC spend=3')
    due_now(db, campaign)
    assert sched.run_all() == [1], 'a spend below the limit: the review starts'
    sched.wait()
    sched.stop(timeout=5)


def test_the_closing_review_waits_until_the_stopped_campaigns_runs_are_judged_and_fires_once_per_stop(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """The wrap-up's last commits are judged first, so the closing review sees the final head.

    The stop is on the feed, so a daemon that restarts in between still requests the review, once.
    """
    configure(db, campaign)
    running = add_run(db, campaign, status=RunStatus.RUNNING, measured=False)
    with db.session() as s:
        request_stop(s, s.get_one(Campaign, campaign.id), System.HUMAN, reason='done', ending=Ending.WRAP_UP)
        s.commit()
    sched = make_scheduler(db, runtime, tmp_path)
    assert sched.run_all() == []
    assert campaign_state(db, campaign) == (CampaignStatus.STOPPED, 'stopped by human: done')
    assert closing_reviews(db, campaign) == [], 'its run is still open'
    with db.session() as s:
        run = s.get_one(Run, running)
        run.status, run.end, run.finished_at, run.scoring = RunStatus.SUCCEEDED, RunEnd.FINISHED, utcnow(), True
        s.commit()
    assert sched.run_all() == [], 'its commits are still judged'
    sched.stop(timeout=5)
    with db.session() as s:
        s.get_one(Run, running).scoring = False
        s.commit()
    restarted = make_scheduler(db, runtime, tmp_path)
    assert restarted.run_all() == [1], 'settled: the closing review launches'
    restarted.wait()
    assert restarted.run_all() == [], 'once'
    with db.session() as s:
        s.get_one(Campaign, campaign.id).status = CampaignStatus.ACTIVE
        request_stop(s, s.get_one(Campaign, campaign.id), System.HUMAN, reason='again', ending=Ending.WRAP_UP)
        s.commit()
    assert restarted.run_all() == [2], 'stopped again: a closing review again'
    restarted.wait()
    restarted.stop(timeout=5)


def test_the_closing_sweep_is_each_stopped_trigger_and_none_without_one(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """``[roles.watchdog] triggers = []`` drops the closing review; a ``stopped`` trigger of another role fires once."""
    (Path(campaign.path) / 'closer.md').write_text('# Close\n')
    closer = {'prompt': 'closer.md', 'triggers': [{'provider': 'stopped'}]}
    configure(db, campaign, harness=HARNESS_WORSE, roles={'watchdog': {'triggers': []}, 'closer': closer}, max_runs=1)
    add_ideas(db, campaign, 'a')
    sched = make_scheduler(db, runtime, tmp_path)
    assert sched.run_all() == [1]
    sched.wait()
    assert sched.run_all() == [2], 'the closer, and no watchdog'
    sched.wait()
    assert sched.run_all() == [], 'it fires once'
    sched.stop(timeout=5)
    with db.session() as s:
        rows = [(w.kind, w.reason) for w in s.scalars(select(AgentSession).where(AgentSession.id > 1))]
    assert rows == [('closer', 'campaign stopped: max_runs: 1 runs')]


def test_a_stopped_campaign_gets_one_closing_watchdog_and_no_trigger_requests(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
) -> None:
    configure(db, campaign, harness=HARNESS_WORSE, max_runs=1)
    add_ideas(db, campaign, 'a', 'b')
    sched = make_scheduler(db, runtime, tmp_path, tracer)
    assert sched.run_all() == [1], 'below max_runs: the queue trigger creates run 1, which commits a measured discard'
    sched.wait()
    assert campaign_state(db, campaign) == (CampaignStatus.ACTIVE, None)

    assert sched.run_all() == [2], 'the closing watchdog launches on the tick that stops the campaign'
    assert campaign_state(db, campaign) == (CampaignStatus.STOPPED, 'max_runs: 1 runs')
    sched.wait()
    assert sched.run_all() == []
    sched.run_all()
    with db.session() as s:
        assert [
            (w.kind, w.reason)
            for w in s.scalars(select(AgentSession).where(AgentSession.id > 1).order_by(AgentSession.id))
        ] == [(WATCHDOG, 'campaign stopped: max_runs: 1 runs')], (
            'requested exactly once, and no implementer or research request follows'
        )
        assert s.scalars(select(Run)).all() and s.get_one(Idea, 2).status is IdeaStatus.READY, 'idea b never ran'
        [(event, change)] = in_order(s, CampaignStatusChanged)
        assert (event.actor, event.target_id, change) == (
            'daemon',
            campaign.id,
            CampaignStatusChanged(
                old=CampaignStatus.ACTIVE, new=CampaignStatus.STOPPED, stopped_reason='max_runs: 1 runs'
            ),
        ), 'the change log trigger records the stop, reason included; the daemon emits nothing by hand'
    assert dict(span_named(spans, 'campaign.stopped').attributes or {}) == {
        'campaign': 'demo',
        'reason': 'max_runs: 1 runs',
    }
    assert (
        'Why this agent started (context, not a scope): campaign stopped: max_runs: 1 runs'
        in (tmp_path / 'agent_sessions' / '2' / 'brief.md').read_text()
    )

    with db.session() as s:
        s.add(AgentSession(kind=RESEARCH, campaign_id=campaign.id, reason='by hand'))
        s.commit()
    assert sched.run_all() == [3], 'a hand-inserted request is explicit and launches'
    sched.wait()
    sched.stop(timeout=5)
    with db.session() as s:
        assert s.get_one(AgentSession, 3).state is AgentSessionState.EXITED


def test_a_stop_syncs_the_memory_documents_before_the_closing_watchdog_whose_brief_asks_for_lessons(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
) -> None:
    write_toml(Path(campaign.path), HARNESS_NOOP)
    with db.session() as s:
        cfg = load_campaign_config(
            Path(campaign.path), plugins={'stop_rule': {'max_runs': 1}}, memory={'documents': ['docs/*.md']}
        )
        s.get_one(Campaign, campaign.id).config = cfg
        s.commit()
    add_run(db, campaign)
    Path(campaign.path, 'docs').mkdir()
    Path(campaign.path, 'docs', 'report.md').write_text('# Closing report\n')
    sched = make_scheduler(db, runtime, tmp_path, tracer)
    [watchdog] = sched.run_all()
    sched.wait()
    sched.stop(timeout=5)
    with db.session() as s:
        assert s.get_one(AgentSession, watchdog).reason == 'campaign stopped: max_runs: 1 runs'
        [document] = s.scalars(select(Document)).all()
        assert (document.path, document.title, document.created_by) == ('docs/report.md', 'Closing report', 'daemon')
    assert dict(span_named(spans, 'documents.synced').attributes or {}) == {
        'campaign': 'demo',
        'result': 'documents: 1 added, 0 unchanged',
    }
    brief = (tmp_path / 'agent_sessions' / str(watchdog) / 'brief.md').read_text()
    assert 'one row of `lessons` on the campaign for each mechanism family that this' in brief
    assert 'INSERT INTO lessons (campaign_id, family, status, effect, metric, body, created_by)' in brief, (
        'the INSERT, from the procedures'
    )


def test_a_stop_whose_document_sync_fails_still_stops_and_launches_the_closing_watchdog(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Greptile on hiloopai/piloop#7: a sync that raised rolled the stop back and ended the daemon."""
    write_toml(Path(campaign.path), HARNESS_NOOP)
    with db.session() as s:
        cfg = load_campaign_config(
            Path(campaign.path), plugins={'stop_rule': {'max_runs': 1}}, memory={'documents': ['docs/*.md']}
        )
        s.get_one(Campaign, campaign.id).config = cfg
        s.commit()
    add_run(db, campaign)

    def broken(*_args: object) -> None:
        raise PermissionError(13, 'Permission denied', 'docs')

    monkeypatch.setattr('piloop.daemon.scheduler.sync_documents', broken)
    sched = make_scheduler(db, runtime, tmp_path, tracer)
    [watchdog] = sched.run_all()
    sched.wait()
    sched.stop(timeout=5)
    with db.session() as s:
        assert s.get_one(Campaign, campaign.id).status is CampaignStatus.STOPPED
        assert s.get_one(AgentSession, watchdog).reason == 'campaign stopped: max_runs: 1 runs'
    error = dict(span_named(spans, 'documents.sync_failed').attributes or {})['error']
    assert isinstance(error, str) and error.startswith('PermissionError: [Errno 13]')


def test_the_contract_of_a_stop_rule_is_data_in_data_out() -> None:
    """A rule can move out of process or to a Wasm component: nothing live crosses its contract."""
    assert data_errors(StopRule) == []


def imports_outside_the_standard_library(module: object) -> set[str]:
    tree = ast.parse(Path(str(getattr(module, '__file__', ''))).read_text())
    imported = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module is not None}
    return {m for m in imported if m.split('.')[0] not in sys.stdlib_module_names}


def test_the_built_in_stop_rule_imports_only_the_standard_library_pydantic_its_contract_and_the_metric_types() -> None:
    """``limits`` is a pure provider: it reads nothing of piloop's state. pydantic declares its parameters.
    test_capabilities checks the example."""
    contracts = {
        'piloop.plugins.stop_rule.contract',
        'piloop.plugins.fact_source.facts',
        'piloop.plugins.metric',
        'pydantic',
    }
    assert imports_outside_the_standard_library(limits_module) == contracts


def test_limits_holds_while_a_fact_is_unknown_or_at_its_hold_at_and_a_hold_comes_before_a_stop() -> None:
    """``hold_at`` holds, on any fact that a source reads. A hold releases itself: the next check that sees the value
    below it goes on.

    A held campaign does not stop: a stop requests the closing review, and nothing may launch while the value is
    unknown or at its limit.
    """
    spend = reading('spend')
    rule = Limits(max_runs=3, hold_at={'spend': 300})
    assert rule.limits() == {RUNS: 3, spend: 300}
    assert rule.reads() == [RUNS, HOURS, spend, UNSETTLED], 'what it shows, its limits, and what decides each'
    assert spend not in Limits(max_runs=3).reads(), 'no hold on the spend: no spend to read'

    def check(value: float | None, *, runs: int = 1) -> object:
        return rule.check(StopCase('score', Direction.HIGHER, facts(runs=runs, unsettled=0, spend=value)))

    assert check(299.99) == GO
    assert check(None) == Hold('spend not known'), 'not read yet, or the latest reading failed'
    at_limit = Escalate(
        'spend reached its hold_at of 300: nothing launches until you raise it and run piloop init',
        then=Hold('spend at its limit'),
    )
    assert check(300.0) == at_limit, 'it tells a human itself, once for each limit'
    assert check(300.0, runs=3) == at_limit, 'held, so max_runs waits'
    assert check(12.0, runs=3) == Stop('max_runs: 3 runs')
    gpu = Limits(hold_at={'gpu_hours': 8})
    assert gpu.check(StopCase('score', Direction.HIGHER, facts(gpu_hours=9))) == Escalate(
        'gpu_hours reached its hold_at of 8: nothing launches until you raise it and run piloop init',
        then=Hold('gpu_hours at its limit'),
    ), 'no fact is special'


def test_the_example_stop_rule_ends_after_its_keeps_or_at_a_limit_of_limits() -> None:
    """``examples/plugins/stop-after-keeps``: ``limits`` with one more limit, so it reads the facts that it limits and
    that ``limits`` shows, and every limit of ``limits`` still applies."""
    rule = AfterKeeps(keeps=3, max_hours=12, max_tokens=1000, hold_at={'spend': 300})
    assert AfterKeeps(keeps=3).reads() == [RUNS, HOURS, KEEPS]
    assert rule.limits() == {HOURS: 12, TOKENS: 1000, reading('spend'): 300, KEEPS: 3}

    def check(known: Facts) -> object:
        return rule.check(StopCase('score', Direction.HIGHER, known))

    assert check(facts(keeps=2, hours=1, tokens=10, spend=12)) == GO
    assert check(facts(keeps=3, hours=1, tokens=10, spend=12)) == Stop('after_keeps: 3 keeps')
    assert check(facts(keeps=0, hours=12.5, tokens=10, spend=12)) == Stop('max_hours: 12.5h since the first run')
    assert check(facts(keeps=0, hours=1, tokens=1000, spend=12)) == Stop('max_tokens: 1000 tokens spent')
    assert isinstance(answer := check(facts(keeps=3, hours=1, tokens=10, spend=300)), Escalate)
    assert answer.then == Hold('spend at its limit')
    assert check(facts(keeps=0, hours=1, tokens=10)) == Hold('spend not known')


def test_a_rule_that_reads_a_fact_needs_a_source_of_it_and_a_source_alone_only_posts_notices(
    campaign: Campaign, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A rule that reads a fact without a source of it would hold forever."""
    installed(monkeypatch, [EntryPoint('goes_on', f'{PLUGIN}:GoesOn', STOP_RULES.group)])
    try:
        source = {**SOURCE, 'notices': {'spend': [10]}}
        load_campaign_config(Path(campaign.path), facts=[*OWN, source], plugins={'stop_rule': {'provider': 'goes_on'}})
        refused = "the stop rule 'limits' reads spend, which no fact source of this campaign supplies"
        with pytest.raises(ConfigError, match=re.escape(refused)):
            load_campaign_config(Path(campaign.path), plugins={'stop_rule': {'hold_at': {'spend': 10}}})
    finally:
        host.close()


def test_a_stop_rule_from_a_plugin_replaces_the_built_in_one_through_piloop_toml(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The example plugin ``examples/plugins/stop-after-keeps``, installed, is selected by ``[plugins.stop_rule]``.

    Without a limit, ``limits`` never ends the campaign; ``after_keeps`` ends it once its first run keeps.
    """
    example(tmp_path / 'site', 'stop-after-keeps')
    monkeypatch.syspath_prepend(tmp_path / 'site')
    host.close()
    try:
        write_toml(Path(campaign.path), HARNESS_APPEND)
        for table, error in [
            ({'provider': 'never'}, "plugins.stop_rule: provider = 'never': unknown stop rule 'never'"),
            ({'provider': 'after_keeps'}, 'keeps: Field required'),
            (
                {'provider': 'limits', 'keeps': 1},
                'plugins.stop_rule.keeps = 1: Extra inputs are not permitted',
            ),
        ]:
            with pytest.raises(ConfigError, match=re.escape(error)):
                load_campaign_config(Path(campaign.path), plugins={'stop_rule': table})
        with db.session() as s:
            cfg = load_campaign_config(
                Path(campaign.path), plugins={'stop_rule': {'provider': 'after_keeps', 'keeps': 1}}
            )
            s.get_one(Campaign, campaign.id).config = cfg
            s.commit()
        add_ideas(db, campaign, 'a', 'b')
        sched = make_scheduler(db, runtime, tmp_path)
        assert sched.run_all() == [1]
        sched.wait()
        sched.run_all()
        assert campaign_state(db, campaign) == (CampaignStatus.STOPPED, 'after_keeps: 1 keeps')
        sched.stop(timeout=5)
        assert STOP_RULES.sources()['after_keeps'].release == 'piloop-stop-after-keeps 0.1.0'
    finally:
        host.close()


def test_an_agent_may_run_its_role_timeout_cut_at_the_time_that_the_campaign_stops(
    db: Database, campaign: Campaign
) -> None:
    """A run that started minutes before a max_hours stop got its whole role timeout and outlived the stop."""
    configure(db, campaign, max_runs=4)
    with db.session() as s:
        row = s.get_one(Campaign, campaign.id)
        no_time = CampaignConfig.model_validate(row.config)
        assert agent_timeout(s, row, no_time, 1800, utcnow()) == 1800, 'the rule limits no hours'
    configure(db, campaign, max_hours=3)
    with db.session() as s:
        row = s.get_one(Campaign, campaign.id)
        cfg = CampaignConfig.model_validate(row.config)
        assert agent_timeout(s, row, cfg, 1800, utcnow()) == 1800, 'no run yet: three hours left'
    add_run(db, campaign, started_at=timedelta(hours=2, minutes=55))
    with db.session() as s:
        row = s.get_one(Campaign, campaign.id)
        now = utcnow()
        assert 290 < agent_timeout(s, row, cfg, 1800, now) <= 300, 'five minutes left'
        started = now - timedelta(minutes=2)
        assert 410 < agent_timeout(s, row, cfg, 1800, started) <= 420, 'an agent that started 2 minutes ago'
        row.status = CampaignStatus.STOPPED
        assert agent_timeout(s, row, cfg, 1800, now) == 1800, 'a stopped campaign cuts nothing: its closing review'
        row.status = CampaignStatus.ACTIVE
    add_run(db, campaign, started_at=timedelta(hours=3, minutes=5))
    with db.session() as s:
        row = s.get_one(Campaign, campaign.id)
        assert agent_timeout(s, row, cfg, 1800, utcnow()) == 0, 'past the stop: no time, never a negative one'


def facts(**values: FactValue) -> Facts:
    """The facts of a case, by name."""
    return Facts(dict(values))


def test_the_ratios_are_defined_once_and_take_the_facts_a_rule_names() -> None:
    """piloop knows no cost: a rule names the fact of what the campaign spent and the evaluator's cost metric."""
    known = facts(spend=400, keeps=2, evaluations=10, metric_sums={'usd': 100, 'score': 7}, attempts=6)
    ratios = (
        per_evaluation(known, 'usd'),
        worker_spend(known, 'spend', 'usd'),
        per_run(known, 'spend', 'usd'),
        per_keep(known, 'spend'),
    )
    assert ratios == (10, 300, 50, 200)
    assert (per_evaluation(known, 'cost'), worker_spend(known, 'spend', 'cost')) == (None, 400), 'no row printed it'
    assert per_keep(known, 'gpu_hours') is None, 'a fact that the case does not have'
    unknown = facts(keeps=0, evaluations=0, attempts=0)
    ratios = (
        per_evaluation(unknown, 'usd'),
        worker_spend(unknown, 'spend', 'usd'),
        per_run(unknown, 'spend', 'usd'),
        per_keep(unknown, 'spend'),
    )
    assert ratios == (None,) * 4
    assert per_run(facts(spend=10, metric_sums={'usd': 4}, attempts=0), 'spend', 'usd') is None, 'no attempt yet'


COST = {'name': 'eval_cost_usd', 'direction': 'lower'}
OWN = [{'provider': 'campaign'}, {'provider': 'optimization'}]
SOURCE = {'provider': 'command', 'command': ['true'], 'metrics': ['spend']}


def test_the_campaign_facts_sum_every_evaluation_count_the_attempts_and_a_reading_grows_since_the_latest_keep(
    db: Database, campaign: Campaign
) -> None:
    metrics = [{'name': 'score', 'direction': 'higher', 'role': 'primary'}, {'name': 'size', 'direction': 'lower'}]
    set_config(db, campaign, metrics=[*metrics, COST], facts=[*OWN, SOURCE])
    measured = add_run(db, campaign)
    add_run(db, campaign, status=RunStatus.FAILED, end=RunEnd.CANCELLED, measured=False)
    add_run(db, campaign, status=RunStatus.RUNNING)
    with db.session() as s:
        evaluator = s.get_one(Campaign, campaign.id).evaluator_id or 0
        candidate = s.scalars(select(Candidate).where(Candidate.run_id == measured)).one()
        s.add(Measurement(candidate_id=candidate.id, evaluator_id=evaluator, metrics={'eval_cost_usd': 2}))
        failed = {'eval_cost_usd': 1.5}
        s.add(Measurement(candidate_id=candidate.id, evaluator_id=evaluator, metrics=failed, error='evaluate exit 1'))
        s.commit()
    spend, since = reading('spend'), since_keep('spend')
    every = [*CAMPAIGN, *OPTIMIZATION, spend, since]

    def known() -> Facts:
        with db.session() as s:
            row = s.get_one(Campaign, campaign.id)
            return gather(s, row, row.config, every, utcnow())

    def read(values: dict[str, float] | FactReadError, ago: timedelta = timedelta(0)) -> None:
        with db.session() as s:
            row = s.get_one(Campaign, campaign.id)
            [_, _, source] = row.config.fact_sources()
            assert isinstance(source, Readings)
            source.record(s, row, values)
            s.flush()
            s.execute(update(Event).where(Event.id == s.scalar(func.max(Event.id))).values(at=utcnow() - ago))
            s.commit()

    assert known().get(spend) is None, 'no reading yet'
    read({'spend': 100}, ago=timedelta(minutes=10))
    before = known()
    assert (before.get(EVALUATIONS), before.get(METRIC_SUMS), before.get(ATTEMPTS)) == (
        4,
        {'score': 1, 'size': 5, COST['name']: 3.5},
        2,
    ), 'every row, a failed one and the baseline among them; a row adds only what it printed; the open attempt too'
    assert before.get(FUNNEL) == Funnel(ideas=3, attempts=1, reached=1, measured=1, keeps=0, unmeasured={}), (
        'the funnel waits for the open run, and a cancelled run is no attempt'
    )
    assert [before.get(f) for f in (spend, KEEPS, since)] == [100, 0, 100], 'no keep yet: all of the spend'
    add_run(db, campaign, keep_score=1, finished_at=timedelta(minutes=5))
    read({'spend': 150})
    after = known()
    assert [after.get(f) for f in (KEEPS, ATTEMPTS, since)] == [1, 3, 50], 'the spend read before the keep'
    assert (per_evaluation(after, COST['name']), per_keep(after, 'spend')) == (3.5 / 5, 150)
    read(FactReadError('billing down'))
    assert (known().get(spend), known().get(since)) == (None, None), 'a failed reading: the value is not known'
    add_run(db, campaign, measured=False)
    assert known().get(FUNNEL) == Funnel(
        ideas=5, attempts=3, reached=2, measured=2, keeps=1, unmeasured={'finished': 1}
    ), 'an attempt that left no candidate never reached the evaluator'
    with db.session() as s:
        row = s.get_one(Campaign, campaign.id)
        assert gather(s, row, row.config, [KEEPS], utcnow()) == facts(keeps=1), 'only the facts a rule reads'


def test_a_rule_that_escalates_lets_the_campaign_go_on_and_tells_a_human_once_per_reason(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    installed(monkeypatch, [EntryPoint('escalates', f'{PLUGIN}:Escalates', STOP_RULES.group)])
    try:
        set_config(db, campaign, plugins={'stop_rule': {'provider': 'escalates', 'reason': '$900 since the last keep'}})
        add_ideas(db, campaign, 'a', 'b')
        sched = make_scheduler(db, runtime, tmp_path)
        assert sched.run_all() == [1], 'the campaign goes on'
        sched.wait()
        sched.run_all()
        set_config(db, campaign, plugins={'stop_rule': {'provider': 'escalates', 'reason': '$1,800 since the keep'}})
        sched.run_all()
        sched.wait()
        sched.stop(timeout=5)
        with db.session() as s:
            said = s.scalars(select(Comment).where(Comment.kind == CommentKind.ESCALATION).order_by(Comment.id))
            assert [(c.target, c.author, c.priority, c.body) for c in said] == [
                (TargetRef(TargetKind.CAMPAIGN, campaign.id), 'daemon', 0, f'Stop rule escalates: {reason}')
                for reason in ('$900 since the last keep', '$1,800 since the keep')
            ], 'one escalation for each distinct reason: the comment is the record'
    finally:
        host.close()


def test_the_example_keep_cost_rule_escalates_once_per_keep_then_stops_at_its_harder_line() -> None:
    """``examples/plugins/keep-cost``: the spend since the latest keep against the spend per keep, over ``limits``."""
    rule = KeepCost(escalate_at=2, stop_at=4, max_hours=12)
    assert rule.reads() == [RUNS, HOURS, reading('spend'), KEEPS, since_keep('spend')]

    def check(since: float, *, keeps: int = 2, spend: float = 400, hours: float = 1) -> object:
        known = facts(hours=hours, spend=spend, keeps=keeps, spend_since_keep=since)
        return rule.check(StopCase('score', Direction.HIGHER, known))

    assert check(399) == GO, 'below 2 × $200 per keep'
    escalation = 'keep_cost: since keep 2, spend passed 2 × its value per keep; it stops at 4 ×'
    assert check(400) == check(700) == Escalate(escalation), 'one reason for the keep, whatever the amount'
    assert check(800) == Stop('keep_cost: spend 800 since the latest keep, 4 × the 200 per keep')
    assert check(5000, keeps=0) == GO, 'no keep yet: no spend per keep'
    other = KeepCost(spend='gpu_hours')
    assert since_keep('gpu_hours') in other.reads(), 'the rule reads the fact that it names'
    assert check(800, hours=13) == Stop('max_hours: 13.0h since the first run'), 'the limits of limits come first'


def test_a_rule_that_reads_the_spend_since_a_keep_needs_a_source_of_it(
    campaign: Campaign, monkeypatch: pytest.MonkeyPatch
) -> None:
    installed(monkeypatch, [EntryPoint('keep_cost', 'piloop_keep_cost:KeepCost', STOP_RULES.group)])
    try:
        refused = "the stop rule 'keep_cost' reads spend, spend_since_keep, which no fact source of this campaign"
        with pytest.raises(ConfigError, match=re.escape(refused)):
            load_campaign_config(Path(campaign.path), plugins={'stop_rule': {'provider': 'keep_cost'}})
        rule = {'stop_rule': {'provider': 'keep_cost'}}
        load_campaign_config(Path(campaign.path), facts=[*OWN, SOURCE], plugins=rule)
    finally:
        host.close()
