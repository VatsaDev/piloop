import os
import re
import shutil
import signal
import sqlite3
import subprocess
import threading
import time
import traceback
from collections import Counter
from contextlib import closing
from dataclasses import dataclass, field
from datetime import timedelta
from importlib.metadata import EntryPoints
from pathlib import Path

import pytest
from dirty_equals import IsStr
from git import Repo
from inline_snapshot import snapshot
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode
from pytest_mock import MockerFixture
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError

from piloop import proc
from piloop.actor import System
from piloop.agents.results import (
    AgentEnded,
    AgentSessionFinished,
    CampaignResults,
    PromptOutcome,
    StartupAnomaly,
    WrapUp,
)
from piloop.agents.roles import SQL_HINT, compose_brief
from piloop.campaign.campaigns import sync
from piloop.campaign.stop import Ending, request_cancel, request_stop, runs
from piloop.config import CampaignConfig, config_of, load_campaign_config
from piloop.daemon.run_controller import UNCOMMITTED_SUMMARY, RunController, wrap_up_lead
from piloop.daemon.scheduler import STARTUP_BREAKER, STARTUP_WINDOW, RunQueued, RunRerouted, Scheduler
from piloop.db import sql
from piloop.db.events import has, in_order, payload_of
from piloop.db.models import (
    AgentSession,
    AgentSessionState,
    Campaign,
    CampaignStatus,
    Candidate,
    Comment,
    CommentKind,
    Evaluation,
    Event,
    EventModel,
    Idea,
    IdeaLink,
    IdeaStatus,
    Lease,
    LeaseKind,
    LeaseState,
    LinkKind,
    Measurement,
    Run,
    RunEnd,
    RunStatus,
    Verdict,
    utcnow,
)
from piloop.db.session import Database
from piloop.db.triggers import DAEMON_ACTOR
from piloop.db.triggers.changelog import IdeaStatusChanged
from piloop.ids import IMPLEMENTER, AgentSessionId, CandidateId, CommentId, RoleName, RunId, TargetKind, TargetRef
from piloop.otel import Tracer
from piloop.plugins import host as plugins
from piloop.plugins.executor.contract import AgentKey, Capacity, Prompt
from piloop.plugins.executor.kit.capacity import Pool
from piloop.plugins.executor.kit.leases import SERVE
from piloop.plugins.executor.workers import Workers
from piloop.plugins.role.research import ResearchRole
from piloop.plugins.runtime.contract import Handle
from piloop.plugins.trigger.low_water import LowWaterTrigger
from piloop.plugins.vcs.git import GitRepository, commands as git
from piloop.scoring.ledger import current_evaluator, due, head, import_reference, request_remeasure
from tests.conftest import (
    HARNESS_APPEND,
    HARNESS_SLEEPS,
    HARNESS_TWO_COMMITS,
    RESEARCH,
    WATCHDOG,
    Gate,
    ScriptRuntime,
    disk,
    events,
    factory,
    register,
    role_of,
    serving,
    span_named,
    wait_for_agent_session_handle,
    wait_until,
    workers_of,
    write_toml,
)
from tests.daemon.test_run_controller import reload_campaign
from tests.daemon.test_triggers import low_water
from tests.test_actor import ACTORS
from tests.usage.test_usage import PI_ID, store


def make_scheduler(db: Database, runtime: ScriptRuntime, tmp_path: Path, tracer: Tracer | None = None) -> Scheduler:
    tracer = tracer or Tracer(None)
    controller = serving(RunController(db, tracer, tmp_path, factory(runtime), executor=Workers()))
    return Scheduler(db, controller, tracer, tmp_path, factory(runtime))


def request_watchdog(db: Database, campaign: Campaign) -> AgentSessionId:
    with db.session() as s:
        agent_session = AgentSession(kind=WATCHDOG, campaign_id=campaign.id, reason='by hand')
        s.add(agent_session)
        s.commit()
        return agent_session.id


def add_ideas(
    db: Database,
    campaign: Campaign,
    *titles: str,
    priority: int = 2,
    harness: str | None = None,
    status: IdeaStatus = IdeaStatus.READY,
    after: tuple[int, ...] = (),
    supersedes: tuple[int, ...] = (),
    track: str = 'score',
) -> list[int]:
    with db.session() as s:
        ideas = [
            Idea(
                track=track,
                campaign_id=campaign.id,
                title=t,
                hypothesis=t,
                priority=priority,
                harness=harness,
                status=status,
                created_by='test',
                links=[
                    *(IdeaLink(target_id=a, kind=LinkKind.AFTER) for a in after),
                    *(IdeaLink(target_id=o, kind=LinkKind.SUPERSEDES) for o in supersedes),
                ],
            )
            for t in titles
        ]
        s.add_all(ideas)
        s.commit()
        return [i.id for i in ideas]


def statuses(db: Database) -> list[IdeaStatus]:
    with db.session() as s:
        return [i.status for i in s.scalars(select(Idea).order_by(Idea.id))]


def set_config(db: Database, campaign: Campaign, **overrides: object) -> None:
    with db.session() as s:
        s.get_one(Campaign, campaign.id).config = load_campaign_config(Path(campaign.path), **overrides).model_dump(
            mode='json'
        )
        s.commit()


def test_agents_launch_across_campaigns_within_the_executors_agent_slots_fewest_live_first(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """Two campaigns of four implementers each, and three agent slots: two go to one campaign, one to the other.

    The slots are free slots: core subtracts only what it took and has not submitted yet, never what the executor runs.
    """
    other_path = tmp_path / 'other' / 'campaign'
    shutil.copytree(campaign.path, other_path)
    for path in (Path(campaign.path), other_path):
        write_toml(path, HARNESS_SLEEPS, max_implementers=4)
    toml = other_path / 'piloop.toml'
    toml.write_text(toml.read_text().replace('name = "demo"', 'name = "other"'))
    Repo(other_path).git.commit('-qam', 'other')
    other = register(db, other_path)
    reload_campaign(db, campaign)
    scheduler = make_scheduler(db, runtime, tmp_path)
    workers_of(scheduler._controller).leases.pools = {SERVE: Pool(agents=3)}
    add_ideas(db, campaign, 'a1', 'a2', 'a3')
    add_ideas(db, other, 'b1', 'b2', 'b3')

    launched = scheduler.tick()
    with db.session() as s:
        by = Counter(s.get_one(AgentSession, i).campaign_id for i in launched)
    assert sorted(by.values()) == [1, 2], by
    assert scheduler.tick() == [], 'every slot is taken, by agents not submitted yet or by their leases'
    wait_until(lambda: not scheduler._controller.launcher.unsubmitted)
    assert scheduler.tick() == [], 'the executor counts them once submitted'
    mocker.patch.object(scheduler._controller.executor, 'capacity', return_value=Capacity(agents=1, evaluations=None))
    assert len(scheduler.tick()) == 1, 'one free slot: core subtracts nothing that the executor counts'
    scheduler.stop(timeout=5)


def test_a_job_submitted_while_the_scheduler_counts_its_slots_still_takes_one(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """The job leaves core's count as the executor counts it: between the two reads, it must not be in neither."""
    scheduler = make_scheduler(db, runtime, tmp_path)
    unsubmitted = scheduler._controller.launcher.unsubmitted
    unsubmitted.add(AgentSessionId(99))  # an agent that a launch thread is about to submit
    add_ideas(db, campaign, 'a1')

    def submitted_meanwhile() -> Capacity:
        if any(frame.name == '_launch' for frame in traceback.extract_stack()):  # the agents' count, not evaluations'
            unsubmitted.discard(AgentSessionId(99))  # its submit lands after the executor's read: no lease of it there
        return Capacity(agents=1, evaluations=None)

    mocker.patch.object(scheduler._controller.executor, 'capacity', side_effect=submitted_meanwhile)
    assert scheduler.tick() == [], 'its one slot is taken'
    scheduler.stop(timeout=5)


def test_ready_ideas_become_runs_in_priority_order(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    scheduler = make_scheduler(db, runtime, tmp_path)
    (low,) = add_ideas(db, campaign, 'low', priority=3)
    (high,) = add_ideas(db, campaign, 'high', priority=0)

    assert scheduler.tick() == [1]
    with db.session() as s:
        run = s.get_one(Run, 1)
        assert (run.idea_id, run.attempt, run.harness, run.model) == (high, 1, 'script', 'test-model')
        assert s.get_one(Idea, high).status is IdeaStatus.SCHEDULED
        agent_session = s.get_one(AgentSession, 1)
        assert (
            agent_session.kind,
            agent_session.run_id,
            agent_session.harness,
            agent_session.model,
            agent_session.reason,
        ) == (
            IMPLEMENTER,
            1,
            'script',
            'test-model',
            f'idea#{high} is ready',
        )
    assert scheduler.tick() == [], 'max_concurrent=1: the second idea waits'
    scheduler.wait()

    assert scheduler.tick() == [2]
    scheduler.wait()
    scheduler.stop(timeout=1)
    with db.session() as s:
        assert [(i.id, i.status) for i in s.scalars(select(Idea).order_by(Idea.id))] == [
            (low, IdeaStatus.DONE),
            (high, IdeaStatus.DONE),
        ]
        assert [m.metrics['score'] for m in s.scalars(select(Measurement).order_by(Measurement.id))] == [1.0, 2.0, 3.0]
        second = s.get_one(Run, 2)
        assert second.base_candidate_id == 2, 'the second run builds on the first kept result'
    assert scheduler.active() == []


def test_a_sweep_checks_out_the_head(db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path) -> None:
    scheduler = make_scheduler(db, runtime, tmp_path)
    add_ideas(db, campaign, 'more')
    assert scheduler.tick() == [1]
    scheduler.wait()
    with db.session() as s:
        best = s.get_one(Candidate, 2)
        assert best.verdict is Verdict.KEEP and best.commit_sha != s.get_one(Candidate, 1).commit_sha
        best_sha = best.commit_sha
    write_toml(Path(campaign.path), '#!/bin/sh\ngit rev-parse HEAD > workspace/head.txt\n')
    set_config(db, campaign)
    agent_session_id = request_watchdog(db, campaign)
    assert scheduler.tick() == [agent_session_id]
    scheduler.wait()
    head = tmp_path / 'projects' / str(campaign.id) / 'workspace' / 'head.txt'
    assert head.read_text().strip() == best_sha
    assert (
        'detached checkout of candidate#2, the head (score=2.0)'
        in (tmp_path / 'agent_sessions' / str(agent_session_id) / 'brief.md').read_text()
    )
    # the executor removes it once the sweep's result is in, after what waits for the sweep returned
    wait_until(lambda: git.worktrees_under(Repo(campaign.path), tmp_path / 'agent_sessions') == [])
    hold = tmp_path / 'hold'
    hold.touch()  # The tick measures the head again under the new evaluator: held until the sweep has its brief.
    evaluate = f'while [ -f {hold} ]; do sleep 0.05; done; echo METRIC score=9; echo METRIC size=9'
    with db.session() as s:
        cfg = load_campaign_config(Path(campaign.path), evaluate=['sh', '-c', evaluate])
        s.get_one(Campaign, campaign.id).config = cfg.model_dump(mode='json')
        current_evaluator(s, s.get_one(Campaign, campaign.id), cfg)
        s.commit()
    again = request_watchdog(db, campaign)
    assert scheduler.tick() == [again]
    brief = tmp_path / 'agent_sessions' / str(again) / 'brief.md'
    wait_until(brief.exists)
    hold.unlink()
    scheduler.wait()
    scheduler.stop(timeout=1)
    assert head.read_text().strip() == best_sha, (
        'a new evaluator: nothing is measured under it yet, and the head stays the latest trunk keep'
    )
    assert 'detached checkout of candidate#2, the head (score=unmeasured)' in brief.read_text()
    with db.session() as s:
        measured = s.scalars(select(Measurement).where(Measurement.candidate_id == 2).order_by(Measurement.id))
        assert [m.metrics['score'] for m in measured] == [2.0, 9.0], 'then the head is measured under the new evaluator'


def test_harness_rotation_and_pinning(db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path) -> None:
    write_toml(Path(campaign.path), '#!/bin/sh\ntrue\n')
    cfg = load_campaign_config(
        Path(campaign.path),
        harnesses=['script', 'pi'],
        model={'script': 'm', 'pi': 'p'},
        roles={'implementer': {'max_concurrent': 3}},
    )
    with db.session() as s:
        s.get_one(Campaign, campaign.id).config = cfg.model_dump(mode='json')
        s.commit()
    add_ideas(db, campaign, 'a', 'b')
    add_ideas(db, campaign, 'c', harness='script')
    scheduler = make_scheduler(db, runtime, tmp_path)
    assert scheduler.tick() == [1, 2, 3]
    scheduler.wait()
    scheduler.stop(timeout=1)
    with db.session() as s:
        assert [(r.harness, r.model) for r in s.scalars(select(Run).order_by(Run.id))] == snapshot(
            [('script', 'm'), ('pi', 'p'), ('script', 'm')]
        )


def test_a_harness_at_its_cap_passes_its_turn_and_an_idea_pinned_to_it_waits(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A harness at its ``max_concurrent`` cap passes its turn, and an idea pinned to it waits.

    ``max_concurrent = {claude = N}`` limits a campaign to N Claude agents while other harnesses fill the free slots.
    """
    set_config(
        db,
        campaign,
        harnesses=['script', 'pi'],
        model={'script': 'm', 'pi': 'p'},
        max_concurrent={'script': 1, 'pi': 2},
        roles={'implementer': {'max_concurrent': 5}},
    )
    a, b, c, d = add_ideas(db, campaign, 'a', 'b', 'c', 'd')
    (pinned,) = add_ideas(db, campaign, 'pinned', harness='script', priority=0)
    with db.session() as s:
        s.add(
            AgentSession(kind=IMPLEMENTER, campaign_id=campaign.id, harness='script', state=AgentSessionState.RUNNING)
        )
        s.commit()
    scheduler = make_scheduler(db, runtime, tmp_path)
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        scheduler._queue(s, p, CampaignConfig.model_validate(p.config))
        s.commit()
    with db.session() as s:
        assert [(r.idea_id, r.harness) for r in s.scalars(select(Run).order_by(Run.id))] == [(a, 'pi'), (b, 'pi')], (
            "script's turn went to pi; once pi was full too nothing more was queued"
        )
        assert [s.get_one(Idea, i).status for i in (c, d, pinned)] == [IdeaStatus.READY] * 3, 'the pin waits'


def test_a_sweep_passes_over_a_harness_at_its_cap_and_a_pinned_one_stays_requested(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    set_config(
        db,
        campaign,
        harnesses=['script', 'pi'],
        model={'script': 'm', 'pi': 'p'},
        max_concurrent={'script': 1},
        roles={'watchdog': {'harnesses': ['script', 'pi'], 'max_concurrent': 4}},
    )
    with db.session() as s:
        rows = [AgentSession(kind=WATCHDOG, campaign_id=campaign.id, reason='by hand') for _ in range(3)]
        rows.append(AgentSession(kind=WATCHDOG, campaign_id=campaign.id, harness='script', reason='pinned'))
        s.add_all(rows)
        s.commit()
    scheduler = make_scheduler(db, runtime, tmp_path)
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        taken = scheduler._take(s, p, CampaignConfig.model_validate(p.config))
        s.commit()
    assert [w for w, _ in taken] == [1, 2, 3]
    with db.session() as s:
        assert [(w.harness, w.state) for w in s.scalars(select(AgentSession).order_by(AgentSession.id))] == [
            ('script', AgentSessionState.STARTING),
            ('pi', AgentSessionState.STARTING),
            ('pi', AgentSessionState.STARTING),
            ('script', AgentSessionState.REQUESTED),
        ], "the third sweep's turn was script's, at its cap"


def test_a_queued_run_whose_harness_left_the_rotation_launches_on_the_new_one_unless_its_idea_pins_it(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A queued run whose harness left the rotation starts on a harness that is still in it, unless its idea pins it."""
    set_config(
        db,
        campaign,
        harnesses=['script'],
        model={'script': 'm', 'pi': 'p'},
        roles={'implementer': {'max_concurrent': 3}},
    )
    a, b = add_ideas(db, campaign, 'a', 'b')
    (pinned,) = add_ideas(db, campaign, 'pinned', harness='script')
    scheduler = make_scheduler(db, runtime, tmp_path)
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        scheduler._queue(s, p, CampaignConfig.model_validate(p.config))
        s.commit()
    set_config(
        db,
        campaign,
        harnesses=['pi'],
        model={'script': 'm', 'pi': 'p'},
        max_concurrent={'pi': 1},
        roles={'implementer': {'max_concurrent': 3}},
    )
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        scheduler._take(s, p, CampaignConfig.model_validate(p.config))
        s.commit()
    with db.session() as s:
        runs = s.scalars(select(Run).order_by(Run.id)).all()
        assert [(r.idea_id, r.harness, r.model) for r in runs] == [
            (a, 'pi', 'p'),
            (b, 'script', 'm'),
            (pinned, 'script', 'm'),
        ]
        assert [(w.harness, w.model, w.state) for w in s.scalars(select(AgentSession).order_by(AgentSession.id))] == [
            ('pi', 'p', AgentSessionState.STARTING),
            ('script', 'm', AgentSessionState.REQUESTED),
            ('script', 'm', AgentSessionState.STARTING),
        ], 'b waits for a pi slot; the pin is launched where it was pinned'
        [rerouted] = s.scalars(select(Event).where(Event.kind == 'run.rerouted')).all()
        assert (rerouted.target_id, rerouted.payload) == (runs[0].id, RunRerouted(from_='script', to='pi'))


def test_idea_without_a_base_is_parked(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    with db.session() as s:
        for c in s.scalars(select(Candidate)):
            c.verdict = Verdict.DISCARD
        s.commit()
    (idea,) = add_ideas(db, campaign, 'orphan')
    scheduler = make_scheduler(db, runtime, tmp_path)
    assert scheduler.tick() == []
    with db.session() as s:
        assert s.get_one(Idea, idea).status is IdeaStatus.PARKED


def test_a_run_starts_from_the_best_as_of_its_start_not_as_of_the_idea(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A run starts from the head at the time the run starts, not at the time its idea was written."""
    (idea,) = add_ideas(db, campaign, 'written while the baseline was best')
    with db.session() as s:
        s.add(
            Candidate(campaign_id=campaign.id, tree_sha='b', commit_sha='b', verdict=Verdict.KEEP, verdict_by='human')
        )
        s.flush()
        s.add(Measurement(candidate_id=2, evaluator_id=1, metrics={'score': 9, 'size': 9}))
        s.commit()
    scheduler = make_scheduler(db, runtime, tmp_path)
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        scheduler._queue(s, p, CampaignConfig.model_validate(p.config))
        s.commit()
    with db.session() as s:
        assert (s.get_one(Run, 1).idea_id, s.get_one(Run, 1).base_candidate_id) == (idea, 2)


def test_a_pin_is_honored_over_the_after_link_and_the_run_records_both_bases(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A pin to an older candidate wins over the ``after`` link, and ``run.queued`` records both bases."""
    set_config(db, campaign, roles={'implementer': {'max_concurrent': 2}})
    (done,) = add_ideas(db, campaign, 'prerequisite', status=IdeaStatus.DONE)
    (branch,) = add_ideas(db, campaign, 'branch from the baseline', after=(done,))
    (missing,) = add_ideas(db, campaign, 'pinned to nothing', priority=3)
    commit_candidate(db, campaign, 1, {'other.txt': 'x\n'}, Verdict.KEEP)
    with db.session() as s:
        s.add(Measurement(candidate_id=2, evaluator_id=1, metrics={'score': 9, 'size': 9}))
        s.get_one(Idea, branch).base_candidate_id = CandidateId(1)
        s.get_one(Idea, missing).base_candidate_id = CandidateId(99)
        s.commit()
    scheduler = make_scheduler(db, runtime, tmp_path)
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        scheduler._queue(s, p, CampaignConfig.model_validate(p.config))
        s.commit()
    with db.session() as s:
        run = s.scalars(select(Run).where(Run.idea_id == branch)).one()
        assert run.base_candidate_id == 1, 'the pin, not the head candidate#2 that the run would otherwise start from'
        queued = s.scalars(select(Event).where(Event.kind == 'run.queued', Event.target_id == run.id)).one()
        pinned = payload_of(queued, RunQueued)
        assert (pinned.pinned_base, pinned.computed_base) == (1, 2)
        assert run.branch, 'a pin other than the computed base: a branch run, judged against its own base'
        assert s.get_one(Idea, missing).status is IdeaStatus.PARKED, 'a pin to a candidate that does not exist'
        assert s.scalars(select(Run).where(Run.idea_id == missing)).all() == []


def commit_candidate(
    db: Database, campaign: Campaign, parent: int, files: dict[str, str], verdict: Verdict
) -> CandidateId:
    """Commit ``files`` on candidate ``parent``'s commit, and add the commit as a candidate with ``verdict``."""
    repo = Repo(campaign.path)
    with db.session() as s:
        repo.git.checkout('-q', '--detach', s.get_one(Candidate, parent).commit_sha)
        for name, body in files.items():
            (Path(campaign.path) / name).write_text(body)
        repo.git.add('-A')
        repo.git.commit('-qm', ', '.join(files))
        commit = repo.head.commit
        repo.git.checkout('-q', 'main')
        c = Candidate(
            campaign_id=campaign.id,
            parent_id=parent,
            tree_sha=commit.tree.hexsha,
            commit_sha=commit.hexsha,
            verdict=verdict,
        )
        s.add(c)
        s.commit()
        return c.id


def test_a_run_pinned_to_a_non_keep_is_a_branch_run_only_when_its_nearest_keep_is_not_the_head(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A run pinned to a candidate that is not a keep is a branch run only when its nearest keep is not the head.

    A candidate that is not a keep is not a bar, so the run is judged against its nearest kept ancestor
    (``ledger.bar_of``). When the pin is a noted candidate on top of the head, that ancestor is the head. The run is
    then a trunk run, and its keep moves the head.

    A pin that has the head's changes builds on the head too, whatever its nearest keep. Here
    a compose off the baseline repeats the head's change. Its run is a trunk run, so its keep can move the head.
    """
    set_config(db, campaign, roles={'implementer': {'max_concurrent': 3}})
    on_head, on_older, on_compose = add_ideas(
        db, campaign, 'on a note of the head', 'on a discard of the baseline', 'on a compose of the head'
    )
    k = commit_candidate(db, campaign, 1, {'other.txt': 'x\n'}, Verdict.KEEP)
    n = commit_candidate(db, campaign, k, {'notes.txt': 'n\n'}, Verdict.INTERESTING)
    d = commit_candidate(db, campaign, 1, {'solve.txt': 'base\nd\n'}, Verdict.DISCARD)
    c = commit_candidate(db, campaign, 1, {'solve.txt': 'base\nc\n', 'other.txt': 'x\n'}, Verdict.INTERESTING)
    with db.session() as s:
        s.add(Measurement(candidate_id=k, evaluator_id=1, metrics={'score': 9, 'size': 9}))
        s.get_one(Idea, on_head).base_candidate_id = n
        s.get_one(Idea, on_older).base_candidate_id = d
        s.get_one(Idea, on_compose).base_candidate_id = c
        s.commit()
    scheduler = make_scheduler(db, runtime, tmp_path)
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        scheduler._queue(s, p, CampaignConfig.model_validate(p.config))
        s.commit()
    with db.session() as s:
        runs = {r.idea_id: (r.base_candidate_id, r.branch) for r in s.scalars(select(Run))}
    assert runs == {on_head: (n, False), on_older: (d, True), on_compose: (c, False)}, 'the pin is the base'


def test_a_blocked_idea_waits_for_every_prerequisite_then_starts_from_the_head(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    (first,) = add_ideas(db, campaign, 'first', priority=3)
    (second,) = add_ideas(db, campaign, 'second', priority=4)
    (third,) = add_ideas(db, campaign, 'third', priority=0, after=(first, second))
    scheduler = make_scheduler(db, runtime, tmp_path)

    assert scheduler.tick() == [1]
    with db.session() as s:
        assert s.get_one(Run, 1).idea_id == first, 'the urgent idea is blocked, so the lower priorities run first'
        assert s.get_one(Idea, third).status is IdeaStatus.READY, 'skipped, not touched'
    scheduler.wait()
    assert scheduler.tick() == [2]
    with db.session() as s:
        assert s.get_one(Run, 2).idea_id == second, 'one prerequisite done is not enough'
        assert s.get_one(Idea, third).status is IdeaStatus.READY
    scheduler.wait()
    with db.session() as s:
        assert [s.get_one(Candidate, c).verdict for c in (2, 3)] == [Verdict.KEEP] * 2, 'scores 2 and 3'
        # a human keeps a commit outside any run: the head is now candidate#4, which no prerequisite made
        sha = s.get_one(Candidate, 3).commit_sha
        s.add(
            Candidate(campaign_id=campaign.id, tree_sha='h', commit_sha=sha, verdict=Verdict.KEEP, verdict_by='human')
        )
        s.flush()
        s.add(Measurement(candidate_id=4, evaluator_id=1, metrics={'score': 9, 'size': 9}))
        s.commit()

    assert scheduler.tick() == [3]
    scheduler.wait()
    scheduler.stop(timeout=1)
    with db.session() as s:
        run = s.get_one(Run, 3)
        assert (run.idea_id, run.base_candidate_id) == (third, 4), 'the head, which holds what the prerequisites kept'
        assert s.get_one(Idea, third).status is IdeaStatus.DONE


def test_a_prerequisite_that_ended_without_a_keep_parks_the_ideas_after_it_with_an_escalation(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, tracer: Tracer
) -> None:
    """A cancelled (parked), rejected, or done-but-kept-nothing prerequisite can never release its dependents.

    Left ``ready``, they block forever and the implementer slots sit idle. The daemon parks them, and the ideas after
    them, and asks for a decision. A prerequisite that kept something, never ran, or is still ``proposed`` does not.
    """
    (parked, rejected, nothing, kept, never, proposed) = add_ideas(
        db, campaign, 'cancelled', 'rejected', 'kept nothing', 'kept', 'never ran', 'proposed'
    )
    with db.session() as s:
        for idea_id, status in (
            (parked, IdeaStatus.PARKED),
            (rejected, IdeaStatus.REJECTED),
            (nothing, IdeaStatus.DONE),
            (kept, IdeaStatus.DONE),
            (never, IdeaStatus.DONE),
            (proposed, IdeaStatus.PROPOSED),
        ):
            s.get_one(Idea, idea_id).status = status
        for idea_id, verdict in ((parked, None), (nothing, Verdict.DISCARD), (kept, Verdict.KEEP)):
            run = Run(
                idea_id=idea_id,
                attempt=1,
                base_candidate_id=1,
                harness='h',
                model='m',
                status=RunStatus.SUCCEEDED,
                end=RunEnd.FINISHED,
            )
            s.add(run)
            s.flush()
            if verdict is not None:
                s.add(
                    Candidate(
                        campaign_id=campaign.id,
                        run_id=run.id,
                        tree_sha=f't{run.id}',
                        commit_sha=f'c{run.id}',
                        verdict=verdict,
                    )
                )
        s.commit()
    stop_by_hand(tmp_path, campaign)  # bookkeeping happens, nothing launches
    stranded = add_ideas(db, campaign, 'after cancelled', after=(parked,))
    stranded += add_ideas(db, campaign, 'after rejected', after=(rejected,))
    stranded += add_ideas(db, campaign, 'after kept nothing', after=(nothing, kept))
    (chained,) = add_ideas(db, campaign, 'after after cancelled', after=(stranded[0],))
    released = add_ideas(db, campaign, 'after kept', 'after never ran', 'after proposed')
    with db.session() as s:
        for idea_id, target in zip(released, (kept, never, proposed), strict=True):
            s.add(IdeaLink(idea_id=idea_id, target_id=target, kind=LinkKind.AFTER))
        s.commit()
    scheduler = make_scheduler(db, runtime, tmp_path, tracer)

    assert scheduler.tick() == []
    with db.session() as s:
        assert [s.get_one(Idea, i).status for i in (*stranded, chained)] == [IdeaStatus.PARKED] * 4, 'chains too'
        assert [s.get_one(Idea, i).status for i in released] == [IdeaStatus.READY] * 3
        asked = s.scalars(select(Comment).where(Comment.author == DAEMON_ACTOR).order_by(Comment.id)).all()
        assert [(c.target_id, c.kind, c.priority, c.body) for c in asked] == snapshot(
            [
                (
                    7,
                    CommentKind.ESCALATION,
                    2,
                    'parked: it runs after idea#1, which is parked. Unlink it (piloop idea unlink 7 after 1) to run it without that change, or set it ready once idea#1 is ready again',
                ),
                (
                    8,
                    CommentKind.ESCALATION,
                    2,
                    'parked: it runs after idea#2, which is rejected. Unlink it (piloop idea unlink 8 after 2) to run it without that change, or set it ready once idea#2 is ready again',
                ),
                (
                    9,
                    CommentKind.ESCALATION,
                    2,
                    'parked: it runs after idea#3, which kept nothing. Unlink it (piloop idea unlink 9 after 3) to run it without that change, or set it ready once idea#3 is ready again',
                ),
                (
                    10,
                    CommentKind.ESCALATION,
                    2,
                    'parked: it runs after idea#7, which is parked. Unlink it (piloop idea unlink 10 after 7) to run it without that change, or set it ready once idea#7 is ready again',
                ),
            ]
        )
    assert scheduler.tick() == []
    with db.session() as s:
        assert s.scalar(select(func.count()).select_from(Comment)) == 4, 'once'


def test_low_water_does_not_count_blocked_ideas_as_backlog(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    set_config(db, campaign, roles={'research': {'max_ideas': 1, 'max_concurrent': 1, 'timeout': 20}})
    (gate,) = add_ideas(db, campaign, 'gate', status=IdeaStatus.PROPOSED)
    add_ideas(db, campaign, 'behind the gate', after=(gate,))
    scheduler = make_scheduler(db, runtime, tmp_path)
    assert scheduler.tick() == [1]
    scheduler.wait()
    scheduler.stop(timeout=1)
    with db.session() as s:
        agent_session = s.get_one(AgentSession, 1)
        assert (agent_session.kind, agent_session.reason) == (
            RESEARCH,
            '1 free implementer slots without a runnable idea; 0 of 3 runnable ideas',
        ), 'one ready idea exists, but it cannot run: the backlog is empty for the research role'
        assert s.scalars(select(Run)).all() == []


def test_a_superseded_idea_that_never_ran_is_rejected_on_tick_and_one_that_ran_is_left_alone(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
) -> None:
    (ready,) = add_ideas(db, campaign, 'ready')
    (proposed,) = add_ideas(db, campaign, 'proposed', status=IdeaStatus.PROPOSED)
    (done,) = add_ideas(db, campaign, 'done', status=IdeaStatus.DONE)
    (parked,) = add_ideas(db, campaign, 'parked', status=IdeaStatus.PARKED)
    (retrying,) = add_ideas(db, campaign, 'failed once, one attempt left')
    with db.session() as s:
        s.add(
            Run(
                idea_id=retrying,
                attempt=1,
                base_candidate_id=1,
                harness='h',
                model='m',
                status=RunStatus.FAILED,
                end=RunEnd.FAILED,
            )
        )
        s.commit()
    stop_by_hand(tmp_path, campaign)  # bookkeeping happens, nothing launches
    add_ideas(db, campaign, 'sharper than both', supersedes=(ready, proposed))
    add_ideas(db, campaign, 'sharper done', supersedes=(done,))
    add_ideas(db, campaign, 'sharper parked', supersedes=(parked,))
    add_ideas(db, campaign, 'sharper still', supersedes=(ready,))
    add_ideas(db, campaign, 'sharper retry', supersedes=(retrying,))
    scheduler = make_scheduler(db, runtime, tmp_path, tracer)

    assert scheduler.tick() == []
    assert statuses(db)[:5] == [
        IdeaStatus.REJECTED,
        IdeaStatus.REJECTED,
        IdeaStatus.DONE,
        IdeaStatus.PARKED,
        IdeaStatus.READY,
    ], 'never ran → rejected; ran, parked by a human, or ready again after a failed attempt → untouched'
    assert statuses(db)[5:] == [IdeaStatus.READY] * 5
    with db.session() as s:
        logged = s.scalars(select(Event).where(Event.kind == 'idea.status').order_by(Event.id)).all()
        assert [(e.target_id, e.actor, e.payload) for e in logged] == [
            (ready, 'daemon', IdeaStatusChanged(old=IdeaStatus.READY, new=IdeaStatus.REJECTED)),
            (proposed, 'daemon', IdeaStatusChanged(old=IdeaStatus.PROPOSED, new=IdeaStatus.REJECTED)),
        ], 'the change-log trigger is the only writer of idea.status; the daemon emits nothing by hand'
        notes = s.scalars(select(Comment).where(Comment.author == 'daemon').order_by(Comment.id)).all()
        superseded = 'rejected: superseded by idea#{0}. It does not run, even if idea#{0} has not run yet.'
        assert [(c.target_id, str(c.kind), c.body) for c in notes] == [
            (ready, 'note', superseded.format(9)),
            (proposed, 'note', superseded.format(6)),
        ], 'the newest superseding idea is named where the event cannot say it'
    traced = [dict(sp.attributes or {}) for sp in spans.get_finished_spans() if sp.name == 'idea.superseded']
    assert traced == [
        {'campaign': 'demo', 'idea': ready, 'by': 9},
        {'campaign': 'demo', 'idea': proposed, 'by': 6},
    ], 'no open span: own spans'
    assert scheduler.tick() == []
    with db.session() as s:
        assert s.scalar(select(func.count()).select_from(Event).where(Event.kind == 'idea.status')) == 2, 'once'


def test_reap(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store(tmp_path, 'pi', PI_ID, cwd=tmp_path / 'agent_sessions' / '2' / 'worktree')
    (idea,) = add_ideas(db, campaign, 'stale')
    repo = Repo(campaign.path)
    with db.session() as s:
        s.add(Run(idea_id=idea, attempt=1, base_candidate_id=1, harness='script', model='m', status=RunStatus.RUNNING))
        s.flush()  # the run first: the unit of work orders unrelated mappers by name
        s.add(
            AgentSession(
                kind='implementer',
                campaign_id=campaign.id,
                run_id=1,
                harness='script',
                model='m',
                runtime='subprocess',
                handle={},
                state=AgentSessionState.STARTING,  # mid-start: nothing to re-attach to
                log_dir='x',
                session_id='no-spec-to-read-it-with',
            )
        )
        s.get_one(Idea, idea).status = IdeaStatus.SCHEDULED
        s.get_one(Idea, idea).max_runs = 1
        evaluator = s.get_one(Campaign, campaign.id).evaluator_id
        assert evaluator is not None
        s.add(Evaluation(candidate_id=1, evaluator_id=evaluator, started_at=utcnow()))
        s.commit()
        s.add(
            AgentSession(
                kind='research',
                campaign_id=campaign.id,
                harness='pi',
                model='m',
                runtime='subprocess',
                handle={},
                state=AgentSessionState.STARTING,
                log_dir='y',
                session_id=PI_ID,
            )
        )
        s.add(
            AgentSession(
                kind='watchdog',
                campaign_id=campaign.id,
                harness='pi',
                model='m',
                runtime='subprocess',
                handle={},
                state=AgentSessionState.RUNNING,
                log_dir='z',  # running, but its worktree is gone: nothing to re-attach to either
                session_id='a-session-whose-file-is-gone',
            )
        )
        s.commit()
    wt = tmp_path / 'runs' / '1' / 'worktree'
    sweep_wt = tmp_path / 'agent_sessions' / '2' / 'worktree'
    for path in (wt, sweep_wt):
        path.parent.mkdir(parents=True)
        repo.git.worktree('add', '--detach', str(path), 'HEAD')

    make_scheduler(db, runtime, tmp_path).reap()

    with db.session() as s:
        run = s.get_one(Run, 1)
        assert (run.status, run.end) == (RunStatus.FAILED, RunEnd.STALE)
        assert s.get_one(Idea, idea).status is IdeaStatus.READY, 'a crash is not an attempt: its only one remains'
        assert s.get_one(AgentSession, 1).state is AgentSessionState.KILLED
        sweep = s.get_one(AgentSession, 2)
        assert sweep.state is AgentSessionState.KILLED
        assert sweep.usage is not None and sweep.usage.total == 16478, (
            'a reaped agent spent tokens too: read from its session file as the row closes, so max_tokens sees them'
        )
        gone = s.get_one(AgentSession, 3)
        assert (gone.state, gone.usage) == (AgentSessionState.KILLED, None), (
            'a session file not found leaves usage NULL'
        )
        assert s.scalars(select(Evaluation)).all() == [], (
            'its evaluator died with the daemon: its measurement is asked again'
        )
    assert not wt.exists() and not sweep_wt.exists(), "the run's and the sweep's worktrees are both removed"
    assert len(runtime.killed) == 3, 'the stale implementer processes were asked to stop'


def test_reap_tolerates_a_runtime_that_cannot_kill(
    db: Database,
    runtime: ScriptRuntime,
    tmp_path: Path,
    mocker: MockerFixture,
    tracer: Tracer,
    spans: InMemorySpanExporter,
) -> None:
    with db.session() as s:
        s.add(
            AgentSession(
                kind='watchdog',
                harness='pi',
                model='m',
                runtime='subprocess',
                handle={},
                state=AgentSessionState.RUNNING,
                log_dir='x',
            )
        )
        s.commit()
    mocker.patch.object(runtime, 'kill', side_effect=RuntimeError('gone'))
    make_scheduler(db, runtime, tmp_path, tracer).reap()
    assert events(spans, 'agent_session.kill_failed') == [{'agent_session': 1, 'error': 'gone'}]
    assert span_named(spans, 'piloop.reap').events[0].name == 'agent_session.kill_failed', 'attached to the reap span'
    with db.session() as s:
        assert s.get_one(AgentSession, 1).state is AgentSessionState.KILLED


def test_reap_survives_a_deleted_campaign(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
) -> None:
    (idea,) = add_ideas(db, campaign, 'stale')
    with db.session() as s:
        s.get_one(Campaign, campaign.id).path = str(tmp_path / 'gone')
        s.add(Run(idea_id=idea, attempt=1, base_candidate_id=1, harness='script', model='m', status=RunStatus.RUNNING))
        s.commit()
    make_scheduler(db, runtime, tmp_path, tracer).reap()
    with db.session() as s:
        assert s.get_one(Run, 1).status is RunStatus.FAILED
    assert events(spans, 'worktrees.orphaned') == [
        {'agent_dir': str(tmp_path / 'runs' / '1'), 'campaign_path': str(tmp_path / 'gone')}
    ]


def test_serve_forever_stops(db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path) -> None:
    scheduler = make_scheduler(db, runtime, tmp_path)
    t = threading.Thread(target=scheduler.serve_forever, args=(0.05,))
    t.start()
    scheduler.stop(timeout=5)
    t.join(timeout=5)
    assert not t.is_alive()


class HeldEvent(threading.Event):
    """A stop event whose lock its loop holds, as inside ``wait``: setting it would wait for that lock forever."""

    def set(self) -> None:
        raise AssertionError('the lock of the event is held by the thread that the signal interrupted')


def test_a_signal_during_the_wait_between_ticks_ends_the_loop_at_once(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A SIGTERM handler runs on the main thread, also while the loop holds its stop event's lock inside ``wait``.

    ``Event.set`` in the handler then waited for that lock forever, and ``piloop stop`` gave up after 30 s.
    """
    scheduler = make_scheduler(db, runtime, tmp_path)
    monkeypatch.setattr(scheduler, '_stop', HeldEvent())
    previous = signal.signal(signal.SIGUSR2, lambda *_: scheduler.interrupt())
    started = time.monotonic()
    try:
        threading.Timer(0.5, os.kill, (os.getpid(), signal.SIGUSR2)).start()
        scheduler.serve_forever(30)
    finally:
        signal.signal(signal.SIGUSR2, previous)
    assert time.monotonic() - started < 10, 'the wait ends at the signal, not at the next tick'


def test_a_signal_during_start_up_ends_the_loop_before_its_first_tick(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    scheduler = make_scheduler(db, runtime, tmp_path)

    def tick() -> list[AgentSessionId]:
        raise AssertionError('no tick after the signal')

    monkeypatch.setattr(scheduler, 'tick', tick)
    scheduler.interrupt()
    scheduler.serve_forever(30)


def test_a_dead_thread_is_traced_and_its_row_closed(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
    mocker: MockerFixture,
) -> None:
    scheduler = make_scheduler(db, runtime, tmp_path, tracer)
    mocker.patch.object(scheduler._controller, 'execute', side_effect=RuntimeError('thread broke'))
    add_ideas(db, campaign, 'x')
    scheduler.tick()
    scheduler.wait()
    assert scheduler.active() == []
    died = span_named(spans, 'agent_session.thread_died')
    assert dict(died.attributes or {}) == {'agent_session': 1, 'error': 'thread broke'}, 'no open span: it gets its own'
    with db.session() as s:
        agent_session = s.get_one(AgentSession, 1)
    assert (agent_session.state, agent_session.error) == (AgentSessionState.KILLED, 'RuntimeError: thread broke')


def test_a_tick_reads_no_entry_points(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The installed packages do not change while a process runs: a tick reads each campaign's config without a scan."""
    scheduler = make_scheduler(db, runtime, tmp_path)
    scheduler.tick()
    scans: list[str] = []
    found = plugins.entry_points

    def entry_points(*, group: str) -> EntryPoints:
        scans.append(group)
        return found(group=group)

    monkeypatch.setattr(plugins, 'entry_points', entry_points)
    scheduler.tick()
    scheduler.tick()
    assert scans == []


def test_invalid_campaign_config_is_skipped_not_fatal(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
) -> None:
    with db.session() as s:
        s.get_one(Campaign, campaign.id).config = {}
        s.commit()
    assert make_scheduler(db, runtime, tmp_path, tracer).tick() == []
    assert dict(span_named(spans, 'campaign.invalid_config').attributes or {})['campaign'] == 'demo'


def test_reap_leaves_a_human_decision_alone(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    (idea,) = add_ideas(db, campaign, 'parked meanwhile')
    with db.session() as s:
        s.add(Run(idea_id=idea, attempt=1, base_candidate_id=1, harness='script', model='m', status=RunStatus.RUNNING))
        s.get_one(Idea, idea).status = IdeaStatus.PARKED
        s.commit()
    make_scheduler(db, runtime, tmp_path).reap()
    with db.session() as s:
        assert s.get_one(Idea, idea).status is IdeaStatus.PARKED


def test_stop_kills_in_flight_implementers_and_fails_their_runs(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
) -> None:
    write_toml(Path(campaign.path), HARNESS_SLEEPS, implementer_timeout=60)
    set_config(db, campaign)
    (idea,) = add_ideas(db, campaign, 'long')
    scheduler = make_scheduler(db, runtime, tmp_path, tracer)
    assert scheduler.tick() == [1]
    handle = wait_for_agent_session_handle(db)
    pid_file = tmp_path / 'runs' / '1' / 'child.pid'
    wait_until(lambda: pid_file.is_file() and pid_file.read_text().strip())  # the agent writes it after it starts
    child_pid = int(pid_file.read_text())

    started = time.monotonic()
    scheduler.stop(timeout=10)
    assert time.monotonic() - started < 3, 'stop() does not wait for implementer_timeout'
    assert proc.started(int(str(handle['pid']))) is None and proc.started(child_pid) is None
    assert scheduler.active() == []
    with db.session() as s:
        run = s.get_one(Run, 1)
        assert (run.status, run.end) == (RunStatus.FAILED, RunEnd.DAEMON_STOPPED)
        assert s.get_one(Idea, idea).status is IdeaStatus.READY
        assert (s.get_one(AgentSession, 1).state, s.get_one(AgentSession, 1).error) == (
            AgentSessionState.KILLED,
            RunEnd.DAEMON_STOPPED,
        )
        [(_, finished)] = in_order(s, AgentSessionFinished, Event.target_id == 1)
        assert finished.state is AgentSessionState.KILLED, 'its event says what its row says'
        assert s.scalars(select(Candidate).where(Candidate.run_id == 1)).all() == [], 'no scoring after stop'
    assert not (tmp_path / 'runs' / '1' / 'worktree').exists()
    assert dict(span_named(spans, 'run.aborted').attributes or {}) == {'run': 1, 'end': RunEnd.DAEMON_STOPPED}
    traced = dict(span_named(spans, 'piloop.run').attributes or {})
    assert (traced['status'], traced['end']) == (RunStatus.FAILED, RunEnd.DAEMON_STOPPED), "the row's values"


def test_stop_keeps_the_work_of_the_runs_it_cuts_and_the_next_run_of_the_idea_starts_from_it(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A stop pins the commits and the uncommitted work of the runs it ends, as ``recovered`` candidates.

    While the head has not moved past them, the idea's next run starts from the last one, as after a timed-out run.
    """
    harness = (
        '#!/bin/sh\necho step >> solve.txt\ngit commit -qam "step"\necho draft >> solve.txt\n'
        'sleep 30 &\necho $! > ../child.pid\nwait\n'
    )
    write_toml(Path(campaign.path), harness, implementer_timeout=60)
    set_config(db, campaign)
    (idea,) = add_ideas(db, campaign, 'long')
    scheduler = make_scheduler(db, runtime, tmp_path)
    assert scheduler.tick() == [1]
    pid_file = tmp_path / 'runs' / '1' / 'child.pid'
    wait_until(pid_file.exists)
    scheduler.stop(timeout=10)

    repo = Repo(campaign.path)
    step, head = git.pinned(repo, 'refs/piloop/demo/r1/')
    assert repo.commit(step).summary == 'step'
    recovered = repo.commit(head)
    assert recovered.summary == UNCOMMITTED_SUMMARY and recovered.tree['solve.txt'].data_stream.read().endswith(
        b'step\ndraft\n'
    ), 'the uncommitted line is its own recovered commit, as for any run'
    with db.session() as s:
        cut = s.scalars(select(Candidate).where(Candidate.run_id == 1).order_by(Candidate.id)).all()
        assert [(c.commit_sha, c.verdict) for c in cut] == [(step, Verdict.RECOVERED), (head, Verdict.RECOVERED)]
        assert all(c.measurements == [] for c in cut), 'never measured'
        assert (s.get_one(Run, 1).end, s.get_one(Idea, idea).status) == (RunEnd.DAEMON_STOPPED, IdeaStatus.READY)
        assert s.get_one(Idea, idea).base_candidate_id == cut[-1].id
        (note,) = s.scalars(select(Comment).where(Comment.author == DAEMON_ACTOR)).all()
        assert note.body.startswith(f'requeued from candidate#{cut[-1].id}: run#1 ended daemon_stopped')

    assert make_scheduler(db, runtime, tmp_path).tick() == [2]
    with db.session() as s:
        assert s.get_one(Run, 2).base_candidate_id == cut[-1].id


def test_a_cut_run_whose_role_allows_no_requeue_keeps_its_base_and_a_known_tree_keeps_its_candidate(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """``recovery_requeues = 0`` holds for a stop too. A cut commit of a known tree, here the base's, is that candidate."""
    harness = '#!/bin/sh\ngit commit -q --allow-empty -m "same tree"\nsleep 30 &\necho $! > ../child.pid\nwait\n'
    write_toml(Path(campaign.path), harness, implementer_timeout=60)
    with db.session() as s:
        s.get_one(Campaign, campaign.id).config = load_campaign_config(
            Path(campaign.path), roles={'implementer': {'recovery_requeues': 0}}
        ).model_dump(mode='json')
        s.commit()
    (idea,) = add_ideas(db, campaign, 'long')
    scheduler = make_scheduler(db, runtime, tmp_path)
    assert scheduler.tick() == [1]
    wait_until((tmp_path / 'runs' / '1' / 'child.pid').exists)
    scheduler.stop(timeout=10)
    with db.session() as s:
        assert s.scalars(select(Candidate).where(Candidate.run_id == 1)).all() == [], 'the base, not a new candidate'
        assert s.get_one(Candidate, 1).verdict is Verdict.KEEP
        assert (s.get_one(Idea, idea).status, s.get_one(Idea, idea).base_candidate_id) == (IdeaStatus.READY, None)


@pytest.mark.parametrize('escalated', [False, True])
def test_a_stop_after_the_implementer_left_its_finding_scores_its_run_on_the_next_start_and_never_requeues_its_idea(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, escalated: bool
) -> None:
    """An implementer that left its finding has handed its run off, even when a job of its own keeps the run open.

    A stop then closes the run as succeeded instead of returning the idea to ``ready``, which would start a second run
    of the same work. The commits are pinned and the next daemon scores them and settles the idea, as for any run whose
    scoring a stop cut.
    """
    finding = (
        "INSERT INTO comments (target_kind, target_id, author, kind, body) VALUES ('idea', 1, 'implementer#1', 'finding',"
        " 'the handoff is in workspace/HANDOFF.md')"
    )
    ask = (
        'INSERT INTO comments (target_kind, target_id, author, kind, priority, body) VALUES'
        " ('idea', 1, 'implementer#1', 'escalation', 1, 'run the audit')"
    )
    harness = (
        f'#!/bin/sh\necho step >> solve.txt\ngit commit -qam "step"\n{SQL_HINT} "{finding}"\n'
        + (f'{SQL_HINT} "{ask}"\n' if escalated else '')
        + 'sleep 30 &\necho $! > ../child.pid\nwait\n'
    )
    write_toml(Path(campaign.path), harness, implementer_timeout=60)
    set_config(db, campaign)
    (idea,) = add_ideas(db, campaign, 'long')
    scheduler = make_scheduler(db, runtime, tmp_path)
    assert scheduler.tick() == [1]
    pid_file = tmp_path / 'runs' / '1' / 'child.pid'
    wait_until(pid_file.exists)
    scheduler.stop(timeout=10)

    with db.session() as s:
        run = s.get_one(Run, 1)
        ended = (RunEnd.AWAITING_ANSWER, 'comment#2') if escalated else (RunEnd.FINISHED, None)
        assert (run.end, run.error, run.scoring) == (*ended, True)
        assert s.get_one(Idea, idea).status is IdeaStatus.SCHEDULED, 'not ready: a second run would redo the work'
        notes = s.scalars(select(Comment).where(Comment.author == DAEMON_ACTOR)).all()
        assert notes == [], 'its commits are scored, so no note sends the next run to them'
    [step] = git.pinned(Repo(campaign.path), 'refs/piloop/demo/r1/')
    assert Repo(campaign.path).commit(step).summary == 'step'

    restarted = make_scheduler(db, runtime, tmp_path)
    restarted.reap()
    restarted.wait()
    with db.session() as s:
        [candidate] = s.scalars(select(Candidate).where(Candidate.run_id == 1)).all()
        assert (candidate.summary, candidate.verdict) == ('step', Verdict.KEEP)
        assert s.get_one(Idea, idea).status is (IdeaStatus.PARKED if escalated else IdeaStatus.DONE)
        assert s.get_one(Run, 1).scoring is False


def test_reap_kills_an_agent_that_no_executor_adopts(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A running agent that a previous daemon left with no job (no lease, no worktree) outlives it: the reap kills it."""
    agent = subprocess.Popen(['sleep', '60'], start_new_session=True)
    with db.session() as s:
        s.add(
            AgentSession(
                kind='implementer',
                campaign_id=campaign.id,
                harness='pi',
                model='m',
                runtime='subprocess',
                handle={
                    'pid': agent.pid,
                    'start_time': proc.started(agent.pid),
                    'log_dir': str(tmp_path),
                    'token': 't',
                },
                state=AgentSessionState.RUNNING,
                log_dir='x',
            )
        )
        s.commit()
    controller = RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers())
    scheduler = Scheduler(db, controller, Tracer(None), tmp_path)
    scheduler.reap()
    scheduler.stop(timeout=5)
    assert agent.wait(timeout=5) == -signal.SIGKILL
    with db.session() as s:
        assert s.get_one(AgentSession, 1).state is AgentSessionState.KILLED


def test_a_hand_over_leaves_a_sweep_running_and_the_next_scheduler_re_attaches_to_it(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """An in-place restart during a sweep keeps the sweep's agent and worktree, and the next daemon closes its row."""
    hold = tmp_path / 'hold'
    hold.touch()
    write_toml(Path(campaign.path), f'#!/bin/sh\necho $$ > ../agent.pid\nwhile [ -f {hold} ]; do sleep 0.05; done\n')
    set_config(db, campaign)
    agent_session_id = request_watchdog(db, campaign)
    first = make_scheduler(db, runtime, tmp_path)
    assert agent_session_id in first.tick()
    sweep_dir = tmp_path / 'agent_sessions' / str(agent_session_id)
    wait_until((sweep_dir / 'agent.pid').exists)
    first.hand_over(timeout=5)
    with db.session() as s:
        assert s.get_one(AgentSession, agent_session_id).state is AgentSessionState.RUNNING
    assert (sweep_dir / 'worktree').is_dir()

    second = make_scheduler(db, ScriptRuntime(), tmp_path)
    second.reap()
    hold.unlink()
    second.wait()
    first.wait()
    with db.session() as s:
        sweep = s.get_one(AgentSession, agent_session_id)
        assert (sweep.state, sweep.error) == (AgentSessionState.EXITED, None)
    wait_until(lambda: not (sweep_dir / 'worktree').exists())  # once its result is in
    second.stop(timeout=5)


def test_a_wait_that_runs_out_names_what_is_still_in_flight(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate = Gate()
    monkeypatch.setattr('piloop.plugins.vcs.git.commands.add_worktree', gate.wrap(git.add_worktree))
    scheduler = make_scheduler(db, runtime, tmp_path)
    add_ideas(db, campaign, 'x')
    assert scheduler.tick() == [1]
    assert gate.entered.wait(10)
    with pytest.raises(TimeoutError, match=r'^still in flight after 0.1 s: agent_session#1$'):
        scheduler.wait(0.1)
    gate.release.set()
    scheduler.wait()
    scheduler.stop(timeout=5)


def test_stop_before_the_implementer_starts_never_starts_it(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = Gate()
    monkeypatch.setattr('piloop.plugins.vcs.git.commands.add_worktree', gate.wrap(git.add_worktree))
    scheduler = make_scheduler(db, runtime, tmp_path, tracer)
    (idea,) = add_ideas(db, campaign, 'x')
    scheduler.tick()
    assert gate.entered.wait(10)
    started = time.monotonic()
    scheduler.stop(timeout=0.5)  # the worker makes the worktree, before it starts the agent
    assert time.monotonic() - started < 3
    gate.release.set()
    scheduler.wait()
    with db.session() as s:
        run = s.get_one(Run, 1)
        assert (run.status, run.end) == (RunStatus.FAILED, RunEnd.DAEMON_STOPPED)
        assert s.get_one(Idea, idea).status is IdeaStatus.READY
        agent_session = s.get_one(AgentSession, 1)
        assert (agent_session.state, agent_session.error, agent_session.handle) == (
            AgentSessionState.KILLED,
            RunEnd.DAEMON_STOPPED,
            {},
        ), 'closed by stop(); nothing was started'
        assert s.scalars(select(Lease.state)).all() == [LeaseState.CANCELLED], 'no worker reports it'

    wait_until(lambda: not (tmp_path / 'runs' / '1' / 'worktree').exists())  # the worker's, once its start returns
    assert events(spans, 'agent_session.kill_failed') == [], 'a row without a process is not asked to die'
    traced = dict(span_named(spans, 'piloop.run').attributes or {})
    assert (traced['status'], traced['end']) == (RunStatus.FAILED, RunEnd.DAEMON_STOPPED)
    assert dict(span_named(spans, 'piloop.agent').attributes or {})['state'] == 'killed'


def test_stop_waits_for_a_start_in_flight_then_kills_the_implementer(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_toml(Path(campaign.path), HARNESS_SLEEPS, implementer_timeout=60)
    set_config(db, campaign)
    gate = Gate()
    monkeypatch.setattr(runtime, 'start', gate.wrap(runtime.start))
    scheduler = make_scheduler(db, runtime, tmp_path)
    add_ideas(db, campaign, 'long')
    scheduler.tick()
    assert gate.entered.wait(10)
    stopper = threading.Thread(target=scheduler.stop, args=(10,))
    stopper.start()
    stopper.join(0.3)
    assert stopper.is_alive(), 'stop() waits for the handle to be published'
    with db.session() as s:
        assert (s.get_one(AgentSession, 1).state, s.get_one(AgentSession, 1).handle) == (AgentSessionState.STARTING, {})
    gate.release.set()
    stopper.join(10)
    assert not stopper.is_alive()
    with db.session() as s:
        agent_session = s.get_one(AgentSession, 1)
        assert (agent_session.state, agent_session.error) == (AgentSessionState.KILLED, RunEnd.DAEMON_STOPPED)
        assert proc.started(int(str(agent_session.handle['pid']))) is None
        run = s.get_one(Run, 1)
        assert (run.status, run.end) == (RunStatus.FAILED, RunEnd.DAEMON_STOPPED)
    assert scheduler.active() == []


def test_stop_kills_the_evaluator_and_scores_nothing_more(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
) -> None:
    pid_file = tmp_path / 'evaluator.pid'
    write_toml(Path(campaign.path), HARNESS_TWO_COMMITS)
    # by a rename: `>` creates the file before echo writes it, and the test would read it empty
    set_config(
        db, campaign, evaluate=['sh', '-c', f'echo $$ > {pid_file}.part; mv {pid_file}.part {pid_file}; exec sleep 30']
    )
    scheduler = make_scheduler(db, runtime, tmp_path, tracer)
    add_ideas(db, campaign, 'two commits')
    scheduler.tick()
    wait_until(pid_file.exists)
    pid = int(pid_file.read_text())

    started = time.monotonic()
    scheduler.stop(timeout=10)
    assert time.monotonic() - started < 3, 'stop() does not wait for evaluate_timeout'
    assert proc.started(pid) is None
    assert scheduler.active() == []
    with db.session() as s:
        run = s.get_one(Run, 1)
        assert (run.status, run.error) == (RunStatus.SUCCEEDED, None), 'closed when its implementer ended'
        assert s.get_one(Idea, 1).status is IdeaStatus.SCHEDULED, (
            'its run finished and only its scoring was cut: the next daemon scores it, the idea is not run again'
        )
        assert s.get_one(AgentSession, 1).state is AgentSessionState.EXITED, (
            'the implementer had finished; only scoring was cut'
        )
        made = s.scalars(select(Candidate).where(Candidate.run_id == 1).order_by(Candidate.id)).all()
        assert [(c.summary, c.verdict, c.measurements) for c in made] == [
            ('first', Verdict.PENDING, []),
            ('second', Verdict.PENDING, []),
        ], 'both trees are candidates; the measurement of the first was killed and nothing judged them'
    measured = [sp for sp in spans.get_finished_spans() if sp.name == 'piloop.measure']
    assert len(measured) == 1, 'the second commit is never measured'
    assert dict(measured[0].attributes or {})['status'] == 'cancelled'
    traced = dict(span_named(spans, 'piloop.run').attributes or {})
    assert (traced['status'], traced['end']) == (RunStatus.SUCCEEDED, RunEnd.FINISHED), 'only its scoring was cut'
    assert not (tmp_path / 'runs' / '1' / 'worktree').exists()
    assert list((tmp_path / 'runs' / '1' / 'eval-0').glob('lease-*/worktree')) == []


def test_a_restart_during_an_evaluation_scores_the_run_on_start_and_never_runs_its_idea_again(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """After a restart during an evaluation, the next daemon scores the run's commits and does not run its idea again.

    The next daemon scores and judges every commit of the run, and settles the idea once.
    """
    hold = tmp_path / 'hold'
    hold.touch()
    write_toml(Path(campaign.path), HARNESS_TWO_COMMITS)
    wait_then_score = f'while [ -f {hold} ]; do sleep 0.05; done; echo METRIC score=$(wc -l < solve.txt)'
    set_config(db, campaign, evaluate=['sh', '-c', f'{wait_then_score}; echo METRIC size=1'])
    first = make_scheduler(db, runtime, tmp_path)
    add_ideas(db, campaign, 'two commits')
    first.tick()
    wait_until(lambda: list((tmp_path / 'runs' / '1' / 'eval-0').glob('lease-*/candidate-0/eval.stdout')))
    first.stop(timeout=10)
    with db.session() as s:
        assert s.get_one(Idea, 1).status is IdeaStatus.SCHEDULED
        assert [c.verdict for c in s.scalars(select(Candidate).where(Candidate.run_id == 1))] == [Verdict.PENDING] * 2

    hold.unlink()
    second = make_scheduler(db, runtime, tmp_path)
    second.reap()
    second.wait()
    with db.session() as s:
        made = s.scalars(select(Candidate).where(Candidate.run_id == 1).order_by(Candidate.id)).all()
        assert [(c.summary, c.verdict, len(c.measurements)) for c in made] == [
            ('first', Verdict.KEEP, 1),
            ('second', Verdict.KEEP, 1),
        ], 'the cut commit and the one never reached are both measured and judged'
        assert s.get_one(Idea, 1).status is IdeaStatus.DONE, 'settled by its one finished run'
    second.tick()
    second.wait()
    with db.session() as s:
        assert s.scalar(select(func.count()).select_from(Run)) == 1, 'the idea was never queued again'
    second.stop(timeout=5)


@pytest.mark.parametrize(('before', 'after'), [(IdeaStatus.SCHEDULED, IdeaStatus.READY), (IdeaStatus.PARKED, None)])
def test_a_run_whose_scoring_cannot_resume_returns_its_idea_to_the_queue(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
    before: IdeaStatus,
    after: IdeaStatus | None,
) -> None:
    """``reap`` resumes every run that a stopped daemon was scoring, whatever its idea's status, and the run settles.

    A human may have parked the idea meanwhile: it stays parked, and the run gives back its place under ``max_runs``.
    """
    (idea,) = add_ideas(db, campaign, 'orphan')
    with db.session() as s:
        s.add(
            Run(
                idea_id=idea,
                attempt=1,
                base_candidate_id=1,
                harness='script',
                model='m',
                status=RunStatus.SUCCEEDED,
                end=RunEnd.FINISHED,
                scoring=True,
            )
        )
        s.get_one(Idea, idea).status = before
        s.get_one(Campaign, campaign.id).path = str(tmp_path / 'gone')
        s.commit()
    sched = make_scheduler(db, runtime, tmp_path, tracer)
    sched.reap()
    sched.wait()
    with db.session() as s:
        assert s.get_one(Idea, idea).status is (after or before), 'attempts remain; a parked idea stays parked'
        assert s.scalar(select(Run.scoring)) is False
    assert span_named(spans, 'piloop.resume').status.status_code is StatusCode.ERROR


def measured_by(db: Database, candidate_id: CandidateId) -> list[tuple[int, float | None, str | None]]:
    with db.session() as s:
        rows = s.get_one(Candidate, candidate_id).measurements
        return [(m.evaluator_id, m.metrics.get('score'), m.error) for m in rows]


def test_a_new_evaluator_measures_the_head_runs_wait_for_it_and_older_keeps_only_on_request(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A new evaluator measures the head again and unpinned runs wait for it. An older keep waits for someone to ask.

    Without the head's score, new runs cannot be compared with the kept lineage. Every older keep measured again
    unasked held the evaluations for hours after each change of the evaluator.
    """
    write_toml(Path(campaign.path), HARNESS_APPEND)
    set_config(db, campaign)
    sched = make_scheduler(db, runtime, tmp_path)
    add_ideas(db, campaign, 'one more line')
    sched.tick()
    sched.wait()
    with db.session() as s:
        old = s.get_one(Campaign, campaign.id).evaluator_id
        kept = s.scalars(select(Candidate).where(Candidate.run_id == 1)).one()
        assert kept.verdict is Verdict.KEEP
    doubled = ['sh', '-c', 'echo METRIC score=$(( 2 * $(wc -l < solve.txt) )); echo METRIC size=1']
    write_toml(Path(campaign.path), HARNESS_APPEND, evaluate=doubled)
    with db.session() as s:
        proj = s.get_one(Campaign, campaign.id)
        sync(s, proj)
        s.commit()
        new = proj.evaluator_id
    assert new != old
    add_ideas(db, campaign, 'another line')

    assert sched.tick() == [], 'no keep has a score under the new evaluator: the run waits instead of being parked'
    assert statuses(db) == [IdeaStatus.DONE, IdeaStatus.READY]
    sched.wait()
    assert measured_by(db, kept.id) == [(old, 2.0, None), (new, 4.0, None)], 'the head first'
    assert measured_by(db, CandidateId(1)) == [(old, 1.0, None)], 'one at a time'

    assert sched.tick() == [2], 'the head has a score: the run starts from it'
    sched.wait()
    assert measured_by(db, CandidateId(1)) == [(old, 1.0, None)], (
        'the baseline is no base or bar of any run: nobody asked'
    )
    with db.session() as s:
        assert s.get_one(Run, 2).base_candidate_id == kept.id
        assert s.get_one(Candidate, kept.id).verdict is Verdict.KEEP, 'measuring again judges nothing again'
        request_remeasure(s, s.get_one(Candidate, 1), System.HUMAN, repeats=1)
        s.commit()
    sched.tick()
    sched.wait()
    assert measured_by(db, CandidateId(1)) == [(old, 1.0, None), (new, 2.0, None)], 'measured when asked'
    with db.session() as s:
        assert s.get_one(Candidate, 1).verdict is Verdict.KEEP


def test_after_a_new_evaluator_runs_start_from_the_head_once_the_head_alone_is_measured(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """Implementer slots sat idle while old keeps, neither base nor bar of any idea, were measured again one at a
    time. An unpinned run starts from the head and is judged against it."""
    write_toml(Path(campaign.path), HARNESS_APPEND, max_implementers=2)
    set_config(db, campaign)
    sched = make_scheduler(db, runtime, tmp_path)
    add_ideas(db, campaign, 'first line')
    sched.tick()
    sched.wait()
    add_ideas(db, campaign, 'second line')
    sched.tick()
    sched.wait()
    with db.session() as s:
        older, newer = (s.scalars(select(Candidate).where(Candidate.run_id == r)).one().id for r in (1, 2))
    doubled = ['sh', '-c', 'echo METRIC score=$(( 2 * $(wc -l < solve.txt) )); echo METRIC size=1']
    write_toml(Path(campaign.path), HARNESS_APPEND, max_implementers=2, evaluate=doubled)
    with db.session() as s:
        proj = s.get_one(Campaign, campaign.id)
        sync(s, proj)
        new = proj.evaluator_id
        assert new is not None
        s.add(Measurement(candidate_id=newer, evaluator_id=new, metrics={'score': 6.0, 'size': 1.0}))
        s.commit()
        assert due(s, proj) == [], 'the head has a score: the older keeps wait for an ask'
    (free,) = add_ideas(db, campaign, 'free')
    assert sched.tick() == [3], 'the head has a score'
    with db.session() as s:
        assert (s.get_one(Run, 3).idea_id, s.get_one(Run, 3).base_candidate_id) == (free, newer)
    sched.wait()
    sched.stop(timeout=5)


def test_after_a_new_evaluator_no_run_starts_from_the_best_until_every_keep_is_measured(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """After a new evaluator, no run starts until every keep is measured again.

    A keep can land while the new evaluator measures an older one. If the queue opened when the older keep had a
    score, runs would start from it, because the newer keep has no score yet.
    """
    write_toml(Path(campaign.path), HARNESS_APPEND, max_implementers=2)
    set_config(db, campaign)
    sched = make_scheduler(db, runtime, tmp_path)
    add_ideas(db, campaign, 'first line')
    sched.tick()
    sched.wait()
    (second,) = add_ideas(db, campaign, 'second line')
    sched.tick()
    sched.wait()
    with db.session() as s:
        older, newer = (s.scalars(select(Candidate).where(Candidate.run_id == r)).one().id for r in (1, 2))
        assert [s.get_one(Candidate, c).verdict for c in (older, newer)] == [Verdict.KEEP, Verdict.KEEP]
    doubled = ['sh', '-c', 'echo METRIC score=$(( 2 * $(wc -l < solve.txt) )); echo METRIC size=1']
    write_toml(Path(campaign.path), HARNESS_APPEND, max_implementers=2, evaluate=doubled)
    with db.session() as s:
        proj = s.get_one(Campaign, campaign.id)
        sync(s, proj)
        new = proj.evaluator_id
        assert new is not None
        for candidate, value in ((1, 2.0), (older, 4.0)):  # measured before the newer keep landed
            s.add(Measurement(candidate_id=candidate, evaluator_id=new, metrics={'score': value, 'size': 1.0}))
        s.commit()
    (chained,) = add_ideas(db, campaign, 'chained', after=(second,))
    (free,) = add_ideas(db, campaign, 'free')

    assert sched.tick() == [], 'the newer keep has no score: no base is known to be the best'
    sched.wait()
    assert measured_by(db, newer)[-1] == (new, 6.0, None)
    assert sorted(sched.tick()) == [3, 4]
    with db.session() as s:
        assert {s.get_one(Run, r).idea_id: s.get_one(Run, r).base_candidate_id for r in (3, 4)} == {
            chained: newer,
            free: newer,
        }
    sched.stop(timeout=5)
    sched.stop(timeout=5)


def test_an_imported_reference_is_measured_by_the_daemon_and_stays_a_reference(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    repo = Repo(campaign.path)
    repo.git.checkout('-qb', 'elsewhere')
    (Path(campaign.path) / 'solve.txt').write_text('a\nb\nc\n')
    repo.git.commit('-qam', 'three lines')
    repo.git.checkout('-q', 'main')
    with db.session() as s:
        proj = s.get_one(Campaign, campaign.id)
        cfg = load_campaign_config(Path(campaign.path))
        reference = import_reference(
            s, proj, cfg, GitRepository(repo), 'elsewhere', summary=None, actor=System.ORCHESTRATOR
        ).id
        evaluator = proj.evaluator_id
        s.commit()
    sched = make_scheduler(db, runtime, tmp_path)
    sched.tick()
    sched.wait()
    assert measured_by(db, reference) == [(evaluator, 3.0, None)]
    with db.session() as s:
        assert s.get_one(Candidate, reference).verdict is Verdict.REFERENCE, 'measured, never judged'
        trunk = head(s, s.get_one(Campaign, campaign.id))
        assert trunk is not None and trunk.id == 1, "it scores 3 to the baseline's 1 and is still not the head"
    sched.stop(timeout=5)


def test_a_keep_that_cannot_be_checked_out_is_measured_as_a_crash_once(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    with db.session() as s:
        s.add(
            Candidate(
                campaign_id=campaign.id,
                tree_sha='0' * 40,
                commit_sha='0' * 40,
                verdict=Verdict.KEEP,
                verdict_by='human',
            )
        )
        s.commit()
    sched = make_scheduler(db, runtime, tmp_path)
    sched.tick()
    sched.wait()
    [(_, score, error)] = measured_by(db, CandidateId(2))
    assert score is None and error is not None and error.startswith('GitCommandError'), error
    sched.tick()
    sched.wait()
    assert len(measured_by(db, CandidateId(2))) == 1, 'a crash is an answer: not measured again'
    sched.stop(timeout=5)


def test_stop_before_a_sweep_starts_never_starts_it(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate = Gate()
    monkeypatch.setattr('piloop.daemon.scheduler.compose_brief', gate.wrap(compose_brief))
    scheduler = make_scheduler(db, runtime, tmp_path)
    agent_session_id = request_watchdog(db, campaign)
    assert scheduler.tick() == [agent_session_id]
    assert gate.entered.wait(10)
    started = time.monotonic()
    scheduler.stop(timeout=0.5)  # the sweep thread is composing its brief, before its start window
    assert time.monotonic() - started < 3
    gate.release.set()
    scheduler.wait()
    with db.session() as s:
        agent_session = s.get_one(AgentSession, agent_session_id)
        assert (agent_session.state, agent_session.error, agent_session.harness, agent_session.handle) == (
            AgentSessionState.KILLED,
            RunEnd.DAEMON_STOPPED,
            'script',
            {},
        )
        assert s.scalars(select(Event).where(Event.kind == 'agent_session.started')).all() == []
    assert not (tmp_path / 'agent_sessions' / str(agent_session_id)).exists(), 'nothing was started'


def test_stop_waits_for_a_sweep_start_in_flight_then_kills_it(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_toml(Path(campaign.path), HARNESS_SLEEPS)
    set_config(db, campaign)
    gate = Gate()
    monkeypatch.setattr(runtime, 'start', gate.wrap(runtime.start))
    scheduler = make_scheduler(db, runtime, tmp_path)
    agent_session_id = request_watchdog(db, campaign)
    assert scheduler.tick() == [agent_session_id]
    assert gate.entered.wait(10)
    stopper = threading.Thread(target=scheduler.stop, args=(10,))
    stopper.start()
    stopper.join(0.3)
    assert stopper.is_alive(), 'stop() waits for the handle to be published'
    with db.session() as s:
        agent_session = s.get_one(AgentSession, agent_session_id)
        assert (agent_session.state, agent_session.harness, agent_session.handle) == (
            AgentSessionState.STARTING,
            'script',
            {},
        )
    gate.release.set()
    stopper.join(10)
    assert not stopper.is_alive()
    with db.session() as s:
        agent_session = s.get_one(AgentSession, agent_session_id)
        assert (agent_session.state, agent_session.error) == (AgentSessionState.KILLED, RunEnd.DAEMON_STOPPED)
        assert proc.started(int(str(agent_session.handle['pid']))) is None
    assert scheduler.active() == []


class PromptingRuntime(ScriptRuntime):
    """A subprocess runtime with a prompt channel that records what the daemon sends."""

    def __init__(self) -> None:
        super().__init__()
        self.prompts: list[tuple[str, str]] = []

    def prompt(self, handle: Handle, text: str) -> None:
        self.prompts.append((str(handle.data['agent']), text))


def running_agent_session(
    db: Database, campaign: Campaign, title: str, *, state: AgentSessionState = AgentSessionState.RUNNING
) -> int:
    """Add an idea with one run that started now and its agent session row in ``state``.

    The idea, the run and the implementer have the same id.
    """
    (idea,) = add_ideas(db, campaign, title)
    with db.session() as s:
        s.get_one(Idea, idea).status = IdeaStatus.SCHEDULED
        run = Run(
            idea_id=idea,
            attempt=1,
            base_candidate_id=1,
            harness='script',
            model='m',
            status=RunStatus.RUNNING,
            started_at=utcnow(),
        )
        s.add(run)
        s.flush()
        s.add(
            AgentSession(
                kind=IMPLEMENTER,
                campaign_id=campaign.id,
                run_id=run.id,
                harness='script',
                model='m',
                runtime='subprocess',
                handle={'agent': f'implementer-{run.id}', 'pid': 4242},
                state=state,
                log_dir='x',
            )
        )
        s.commit()
    return idea


def lease_running(db: Database) -> None:
    """Put each running agent session on a live lease of a worker elsewhere, as the daemon's are."""
    with db.session() as s:
        leased = select(Lease.agent_session_id).where(Lease.agent_session_id.is_not(None))
        rows = s.scalars(
            select(AgentSession).where(AgentSession.state == AgentSessionState.RUNNING, AgentSession.id.not_in(leased))
        ).all()
        for w in rows:
            now = utcnow()
            job = {'kind': 'agent', 'agent_session': w.id}
            handle: dict[str, object] = {'runtime': w.runtime, 'data': w.handle}
            s.add(
                Lease(
                    kind=LeaseKind.AGENT,
                    agent_session_id=w.id,
                    state=LeaseState.RUNNING,
                    worker='elsewhere',
                    job=job,
                    handle=handle,
                    renewed_at=now,
                    expires_at=now + timedelta(hours=1),
                )
            )
        s.commit()


def revoked(db: Database) -> list[object]:
    """The agents whose leases the daemon revoked: what it asked their workers to kill."""
    with db.session() as s:
        leases = s.scalars(select(Lease).where(Lease.revoked_at.is_not(None)).order_by(Lease.id)).all()
        handles = [lease.handle['data'] for lease in leases]
        return [data['agent'] for data in handles if isinstance(data, dict)]


def comment(
    db: Database, kind: str, target_kind: str, target_id: int, body: str, *, author: str = 'human', priority: int = 2
) -> CommentId:
    """Add a comment through the raw SQL path that agents use. ``created_at`` is CURRENT_TIMESTAMP, to the second.

    Each comment is addressed to the human at ``priority``. An escalation must have this, and other kinds may. The
    push to the implementer ignores it.
    """
    with closing(sql.connect(Path(db.engine.url.database or ''), actor=ACTORS.validate_python(author))) as con:
        return CommentId(
            sql.run(
                con,
                f"INSERT INTO comments (target_kind, target_id, author, kind, body, priority) VALUES ('{target_kind}',"
                f" {target_id}, '{author}', '{kind}', '{body}', {priority})",
            ).last_insert_rowid
        )


def settled(db: Database, kind: str) -> list[tuple[int, EventModel]]:
    with db.session() as s:
        return [(e.target_id, e.payload) for e in s.scalars(select(Event).where(Event.kind == kind))]


@dataclass
class Prompted:
    """``Executor.prompt`` that records each prompt and, when ``answers``, records it queued at once."""

    results: CampaignResults
    answers: bool = True
    prompts: list[tuple[int, Prompt]] = field(default_factory=list)

    def __call__(self, key: AgentKey, prompt: Prompt) -> None:
        self.prompts.append((key.agent_session, prompt))
        if self.answers:
            self.results.queued(key, prompt.id, None)


def prompted(db: Database, scheduler: Scheduler, mocker: MockerFixture, *, answers: bool = True) -> Prompted:
    """Replace the executor's ``prompt`` of ``scheduler`` with a ``Prompted``."""
    fake = Prompted(CampaignResults(db, Tracer(None), scheduler._home, disk(scheduler._home), lambda: False), answers)
    mocker.patch.object(scheduler._controller.executor, 'prompt', side_effect=fake)
    return fake


def test_comments_for_a_running_agent_go_to_its_executor_once_each_until_they_have_an_outcome(
    db: Database, campaign: Campaign, tmp_path: Path, tracer: Tracer, spans: InMemorySpanExporter, mocker: MockerFixture
) -> None:
    runtime = PromptingRuntime()
    idea = running_agent_session(db, campaign, 'live')
    coming_up = running_agent_session(db, campaign, 'coming up', state=AgentSessionState.STARTING)
    with db.session() as s:
        before = utcnow() - timedelta(minutes=5)
        s.add(Comment(target=TargetRef(TargetKind.IDEA, idea), author='human', body='in the brief', created_at=before))
        s.commit()
    scheduler = make_scheduler(db, runtime, tmp_path, tracer)
    executor = prompted(db, scheduler, mocker)
    assert scheduler.tick() == [] and executor.prompts == [], 'a comment from before the run started is in its brief'

    decision = comment(db, 'decision', 'idea', idea, 'keep solve.py under 120 lines\nthe rest is detail')
    note = comment(db, 'note', 'run', 1, 'x' * 250, author='orchestrator')
    comment(db, 'finding', 'campaign', campaign.id, 'not for any implementer')
    waiting = comment(db, 'note', 'idea', coming_up, 'its implementer is starting: waits for its pane')
    comment(db, 'escalation', 'idea', idea, 'for the human, not the pane')
    direct = comment(db, 'note', 'agent_session', 1, 'piloop prompt: the text as written\nall of it')
    scheduler.tick()
    assert executor.prompts == [
        (
            1,
            Prompt(
                decision,
                '[piloop] decision on idea#1 by human: keep solve.py under 120 lines — full text: SELECT body FROM'
                f' comments WHERE id={decision}',
            ),
        ),
        (
            1,
            Prompt(
                note,
                f'[piloop] note on run#1 by orchestrator: {"x" * 199}… — full text: SELECT body FROM comments'
                f' WHERE id={note}',
            ),
        ),
        (1, Prompt(direct, 'piloop prompt: the text as written\nall of it')),
    ]
    assert settled(db, 'comment.queued') == [
        (1, PromptOutcome(kind='comment.queued', comment=c)) for c in (decision, note, direct)
    ]
    assert events(spans, 'comment.queued') == [{'comment': c, 'agent_session': 1} for c in (decision, note, direct)]
    scheduler.tick()
    assert len(executor.prompts) == 3, 'settled by the events: nothing is sent twice'

    with db.session() as s:
        s.get_one(AgentSession, 2).state = AgentSessionState.RUNNING
        s.commit()
    scheduler.tick()
    assert executor.prompts[-1][0] == 2 and executor.prompts[-1][1].id == waiting, (
        'a comment that landed while the pane was coming up is sent once the implementer runs'
    )

    restarted = make_scheduler(db, runtime, tmp_path)
    executor = prompted(db, restarted, mocker, answers=False)
    restarted.tick()
    assert executor.prompts == [], 'a new daemon replays nothing that has an outcome: the record is in the database'
    later = comment(db, 'decision', 'run', 1, 'and stop after the next commit')
    restarted.tick()
    restarted.tick()
    assert [p.id for _, p in executor.prompts] == [later], 'once per daemon process, also without an outcome yet'
    again = make_scheduler(db, runtime, tmp_path, tracer)
    executor = prompted(db, again, mocker)
    again.tick()
    assert [p.id for _, p in executor.prompts] == [later], 'the next daemon sends what has no outcome again'
    refused = comment(db, 'note', 'run', 1, 'the executor fails to take it once')
    locked = OperationalError('SELECT', {}, Exception('locked'))
    taking = mocker.patch.object(again._controller.executor, 'prompt', side_effect=[locked, None])
    again.tick()
    again.tick()
    assert [c.args[1].id for c in taking.call_args_list] == [refused, refused], 'the next tick sends it again'


def test_an_implementers_own_comments_are_not_echoed_back(
    db: Database, campaign: Campaign, tmp_path: Path, mocker: MockerFixture
) -> None:
    idea = running_agent_session(db, campaign, 'live')
    scheduler = make_scheduler(db, PromptingRuntime(), tmp_path)
    executor = prompted(db, scheduler, mocker)
    comment(db, 'finding', 'idea', idea, 'what I tried', author='implementer#1')
    comment(db, 'note', 'run', 1, 'signed the other documented way', author='run#1')
    comment(db, 'note', 'idea', idea, 'a 0.1.0 agent signs as before the upgrade', author='worker#1')
    scheduler.tick()
    assert executor.prompts == [] and settled(db, 'comment.queued') == []
    comment(db, 'note', 'idea', idea, 'a sibling implementer is not the author', author='implementer#2')
    scheduler.tick()
    assert [p.text.split(' — ')[0] for _, p in executor.prompts] == [
        '[piloop] note on idea#1 by implementer#2: a sibling implementer is not the author'
    ]


def test_a_comment_that_cannot_be_delivered_is_settled_as_undelivered_and_never_fails_the_tick(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
) -> None:
    """A comment that cannot reach the implementer is settled as undelivered, and the tick does not fail.

    Here the row runs without a lease, so ``workers`` says why at once. The tick does not try again.
    """
    idea = running_agent_session(db, campaign, 'headless')
    scheduler = make_scheduler(db, runtime, tmp_path, tracer)
    stuck = comment(db, 'decision', 'idea', idea, 'a headless implementer has no input channel')
    assert scheduler.tick() == []
    error = 'the agent has no live lease'
    assert settled(db, 'comment.queued') == []
    assert settled(db, 'comment.undelivered') == [
        (1, PromptOutcome(kind='comment.undelivered', comment=stuck, error=error))
    ]
    assert events(spans, 'comment.undelivered') == [{'comment': stuck, 'agent_session': 1, 'error': error}]
    scheduler.tick()
    assert len(settled(db, 'comment.undelivered')) == 1 and len(events(spans, 'comment.undelivered')) == 1, (
        'settled: the next brief carries the comment, retrying would fail the same way'
    )


def test_a_run_frees_its_slot_when_its_implementer_ends_and_its_idea_settles_once_scored(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A run frees its implementer slot when its implementer ends, and its idea settles only when the commits are scored."""
    go = tmp_path / 'go'
    gated = f'while [ ! -f {go} ]; do sleep 0.05; done; echo METRIC score=$(wc -l < solve.txt); echo METRIC size=1'
    set_config(db, campaign, evaluate=['sh', '-c', gated])
    first, second = add_ideas(db, campaign, 'first', 'second')
    with db.session() as s:  # the new evaluator never measured the baseline, so only a pin makes it a base
        s.get_one(Idea, second).base_candidate_id = CandidateId(1)
        s.commit()
    scheduler = make_scheduler(db, runtime, tmp_path)
    assert scheduler.tick() == [1]
    # the run closes, then its commit becomes a candidate: wait for both, the evaluation is gated
    wait_until(lambda: candidate_rows(db, RunId(1)))
    with db.session() as s:
        run = s.get_one(Run, 1)
        assert (run.status, run.finished_at is not None) == (RunStatus.SUCCEEDED, True), 'closed: its implementer ended'
        assert s.get_one(Idea, first).status is IdeaStatus.SCHEDULED, 'its commit is not judged yet'
        assert s.scalars(select(Candidate).where(Candidate.run_id == 1)).one().verdict is Verdict.PENDING
    assert scheduler.tick() == [2], 'the one slot is free while run#1 is scored'
    go.touch()
    scheduler.wait()
    assert statuses(db) == [IdeaStatus.DONE, IdeaStatus.DONE]
    with db.session() as s:
        assert s.scalars(select(Candidate).where(Candidate.run_id == 1)).one().verdict is Verdict.KEEP
        assert s.get_one(Idea, second).status is IdeaStatus.DONE


def candidate_rows(db: Database, run_id: RunId) -> int:
    with db.session() as s:
        return len(s.scalars(select(Candidate).where(Candidate.run_id == run_id)).all())


def stop_by_hand(tmp_path: Path, campaign: Campaign) -> None:
    """Stop the campaign as a person's UPDATE does: not the daemon's stop, so it gets no closing sweep."""
    with closing(sql.connect(tmp_path / 'piloop.db', actor=System.HUMAN)) as con:
        sql.run(
            con, f"UPDATE campaigns SET status = 'stopped', stopped_reason = 'by the test' WHERE id = {campaign.id}"
        )


def stop_campaign(db: Database, campaign: Campaign, reason: str) -> None:
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        p.status, p.stopped_reason = CampaignStatus.STOPPED, reason
        s.commit()


def test_implementers_of_a_stopped_campaign_are_told_to_wrap_up_then_ended_after_the_grace(
    db: Database, campaign: Campaign, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    """When a campaign stops, its implementers are told to wrap up, and they are ended after the grace period."""
    runtime = PromptingRuntime()
    running_agent_session(db, campaign, 'in flight')
    with db.session() as s:
        s.add(
            AgentSession(
                kind=RESEARCH,
                campaign_id=campaign.id,
                runtime='subprocess',
                handle={'agent': 'research-2', 'pid': 4243},
                state=AgentSessionState.RUNNING,
            )
        )
        s.commit()
    lease_running(db)
    scheduler = make_scheduler(db, runtime, tmp_path)
    fake = prompted(db, scheduler, mocker)
    scheduler.tick()
    assert fake.prompts == [], 'an active campaign is left alone'

    stop_campaign(db, campaign, 'max_runs: 2 runs')
    scheduler.tick()
    scheduler.tick()
    assert [(agent, p.text) for agent, p in fake.prompts] == snapshot(
        [
            (
                1,
                '[piloop] The campaign stopped (max_runs: 2 runs). Commit what you have now, with a message that says where it stands. Leave your finding and stop. This session ends in 10 minutes.',
            )
        ]
    ), 'a sweep is bounded by its timeout and the closing watchdog is one: only implementers are wound down'
    assert settled(db, 'agent_session.wrap_up') == [(1, WrapUp(reason='max_runs: 2 runs', comment=1))]
    assert runtime.prompts == [], 'a note that its executor delivers, never straight to the runtime'
    scheduler.tick()
    assert (len(fake.prompts), revoked(db)) == (1, []), 'told once; ended only when the grace is over'

    monkeypatch.setattr('piloop.daemon.scheduler.STOP_GRACE_SECONDS', 0)
    scheduler.tick()
    assert revoked(db) == ['implementer-1']


def test_an_implementer_that_the_stop_ends_mid_turn_closes_its_run_as_cut_by_the_campaign_stop(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An implementer in the middle of a turn takes a message only when the turn ends, so the wrap-up prompt
    may never reach it before the grace period ends. The run then closed as "implementer produced no commits" or with the
    kill's exit status, and nothing named the stop.

    The run closes with ``campaign stopped``: not an attempt at its idea, and not counted toward a run limit. What
    the implementer committed, and what it left uncommitted, is still scored.
    """
    harness = '#!/bin/sh\necho step >> solve.txt\ngit commit -qam "step"\necho draft >> solve.txt\nexec sleep 30\n'
    write_toml(Path(campaign.path), harness, implementer_timeout=60)
    set_config(db, campaign)
    (idea,) = add_ideas(db, campaign, 'long')
    scheduler = make_scheduler(db, runtime, tmp_path)
    assert scheduler.tick() == [1]
    wait_for_agent_session_handle(db)
    deadline = time.monotonic() + 10
    while 'draft' not in (tmp_path / 'runs' / '1' / 'worktree' / 'solve.txt').read_text():
        assert time.monotonic() < deadline
        time.sleep(0.05)

    stop_campaign(db, campaign, 'plateau: 5 runs without a keep')
    scheduler.tick()
    assert [kind for kind, _ in settled(db, 'agent_session.wrap_up')] == [1], 'told, but the turn goes on'
    monkeypatch.setattr('piloop.daemon.scheduler.STOP_GRACE_SECONDS', 0)
    scheduler.tick()
    scheduler.wait()
    scheduler.stop(timeout=5)
    with db.session() as s:
        run = s.get_one(Run, 1)
        assert (run.status, run.end) == (RunStatus.FAILED, RunEnd.CAMPAIGN_STOPPED)
        [ended] = s.scalars(select(Event).where(Event.kind == 'agent_session.stop_ended')).all()
        assert (ended.target_id, ended.payload) == (
            1,
            AgentEnded(reason='plateau: 5 runs without a keep', end=RunEnd.CAMPAIGN_STOPPED),
        )
        verdicts = [c.verdict for c in s.scalars(select(Candidate).where(Candidate.run_id == 1).order_by(Candidate.id))]
        assert verdicts == [Verdict.KEEP, Verdict.RECOVERED], 'its commit is scored, its draft recovered'
        assert s.get_one(Idea, idea).status is IdeaStatus.READY, 'requeued from the recovered draft'
        assert runs(s, s.get_one(Campaign, campaign.id)) == 0, 'cut by the stop: no run limit counts it'


def test_a_stop_with_kill_ends_the_implementers_at_once_and_a_graceful_one_can_be_made_one(
    db: Database, campaign: Campaign, tmp_path: Path, mocker: MockerFixture
) -> None:
    """``piloop campaign stop --kill``: no wrap-up prompt and no grace. A sweep keeps its timeout."""
    runtime = PromptingRuntime()
    running_agent_session(db, campaign, 'in flight')
    lease_running(db)
    scheduler = make_scheduler(db, runtime, tmp_path)
    fake = prompted(db, scheduler, mocker)
    with db.session() as s:
        request_stop(s, s.get_one(Campaign, campaign.id), System.HUMAN, reason='machine needed', ending=Ending.WRAP_UP)
        s.commit()
    scheduler.tick()  # stops the campaign; its implementers wind down from the next tick, as for a [stop] condition
    scheduler.tick()
    scheduler.tick()
    assert (len(fake.prompts), revoked(db)) == (1, []), 'graceful: told, and the grace runs'
    with db.session() as s:
        request_stop(s, s.get_one(Campaign, campaign.id), System.HUMAN, reason='now', ending=Ending.NOW)
        s.commit()
    scheduler.tick()
    assert revoked(db) == ['implementer-1'], 'a stopped campaign can still be killed'
    scheduler.tick()
    assert settled(db, 'agent_session.stop_ended') == [
        (1, AgentEnded(reason='stopped by human: machine needed', end=RunEnd.CAMPAIGN_STOPPED))
    ], 'one event, however many ticks the kill takes to land'
    scheduler.stop(timeout=5)


def test_a_cancelled_run_is_told_to_wrap_up_and_ended_after_the_grace(
    db: Database, campaign: Campaign, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    runtime = PromptingRuntime()
    running_agent_session(db, campaign, 'redundant')
    running_agent_session(db, campaign, 'useful')
    with db.session() as s:
        sweep = AgentSession(
            kind=RESEARCH,
            campaign_id=campaign.id,
            runtime='subprocess',
            handle={'agent': 'research-3', 'pid': 4243},
            state=AgentSessionState.RUNNING,
            started_at=utcnow(),
        )
        s.add(sweep)
        s.flush()
        request_cancel(s, s.get_one(Run, 1), System.HUMAN, ending=Ending.WRAP_UP)
        request_cancel(s, sweep, System.ORCHESTRATOR, ending=Ending.WRAP_UP)
        s.commit()
    lease_running(db)
    scheduler = make_scheduler(db, runtime, tmp_path)
    fake = prompted(db, scheduler, mocker)
    scheduler.tick()
    scheduler.tick()
    assert [(agent, p.text) for agent, p in fake.prompts] == snapshot(
        [
            (
                1,
                '[piloop] Your run was cancelled (by human). Commit what you have now, with a message that says where it stands. Leave your finding and stop. This session ends in 10 minutes.',
            ),
            (
                3,
                '[piloop] This sweep was cancelled (by orchestrator). Commit what you have now, with a message that says where it stands. Leave your finding and stop. This session ends in 10 minutes.',
            ),
        ]
    ), 'only the cancelled run and the cancelled sweep are told'
    scheduler.tick()
    assert revoked(db) == []
    monkeypatch.setattr('piloop.daemon.scheduler.STOP_GRACE_SECONDS', 0)
    scheduler.tick()
    assert revoked(db) == ['implementer-1', 'research-3']
    assert settled(db, 'agent_session.stop_ended') == [
        (1, AgentEnded(reason='cancelled by human', end=RunEnd.CANCELLED)),
        (3, AgentEnded(reason='cancelled by orchestrator', end=RunEnd.CANCELLED)),
    ]
    scheduler.stop(timeout=5)


def test_a_cancelled_implementer_closes_its_run_as_cancelled_scores_its_commits_and_parks_its_idea(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """``piloop agent cancel``: the run became redundant, so its idea does not run again until it is set ready."""
    harness = '#!/bin/sh\necho step >> solve.txt\ngit commit -qam "step"\nexec sleep 30\n'
    write_toml(Path(campaign.path), harness, implementer_timeout=60)
    set_config(db, campaign)
    (idea,) = add_ideas(db, campaign, 'redundant')
    scheduler = make_scheduler(db, runtime, tmp_path)
    assert scheduler.tick() == [1]
    wait_for_agent_session_handle(db)
    deadline = time.monotonic() + 10
    while not Repo(tmp_path / 'runs' / '1' / 'worktree').git.log('--oneline').count('step'):
        assert time.monotonic() < deadline
        time.sleep(0.05)
    with db.session() as s:
        request_cancel(s, s.get_one(AgentSession, 1), System.HUMAN, ending=Ending.NOW)
        s.commit()
    assert scheduler.tick() == []
    scheduler.wait()
    scheduler.stop(timeout=5)
    with db.session() as s:
        run = s.get_one(Run, 1)
        assert (run.status, run.end) == (RunStatus.FAILED, RunEnd.CANCELLED)
        verdicts = [c.verdict for c in s.scalars(select(Candidate).where(Candidate.run_id == 1))]
        assert verdicts == [Verdict.KEEP], 'what it committed is scored'
        assert s.get_one(Idea, idea).status is IdeaStatus.PARKED
        [note] = s.scalars(select(Comment).where(Comment.target_kind == TargetKind.IDEA)).all()
        assert note.body == 'parked: run#1 was cancelled by human; set the idea ready to run it again'
        assert runs(s, s.get_one(Campaign, campaign.id)) == 0, 'not an attempt: no run limit counts it'


def test_a_finding_that_lands_after_a_cancel_ended_the_implementer_is_refused_and_the_run_is_cancelled(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A ``piloop sql`` that was in flight when the daemon ended a cancelled implementer wrote its finding after the end,
    and the finding settled the run as ``succeeded``. The row is ``ending`` in a commit before the kill, so the late
    finding is refused and the run closes as cancelled."""
    write_toml(Path(campaign.path), '#!/bin/sh\nexec sleep 30\n', implementer_timeout=60)
    set_config(db, campaign)
    (idea,) = add_ideas(db, campaign, 'redundant')
    scheduler = make_scheduler(db, runtime, tmp_path)
    assert scheduler.tick() == [1]
    wait_for_agent_session_handle(db)
    with db.session() as s:
        request_cancel(s, s.get_one(Run, 1), System.HUMAN, ending=Ending.NOW)
        s.commit()
    scheduler.tick()
    with pytest.raises(sqlite3.IntegrityError, match='the agent session of this connection has ended'):
        comment(db, 'finding', 'idea', idea, 'done after all', author='implementer#1')
    scheduler.wait()
    scheduler.stop(timeout=5)
    with db.session() as s:
        run = s.get_one(Run, 1)
        assert (run.status, run.end) == (RunStatus.FAILED, RunEnd.CANCELLED)
        assert s.get_one(AgentSession, 1).state is AgentSessionState.KILLED


def test_an_implementer_cancelled_before_it_did_anything_is_not_a_failed_start(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """An agent that the daemon ends on request within ``STARTUP_SECONDS``, with no commit and no comment, did not
    fail to start: no ``anomaly.startup`` and no count toward the startup breaker. Its row is ``killed``."""
    write_toml(Path(campaign.path), '#!/bin/sh\nexec sleep 30\n', implementer_timeout=60)
    set_config(db, campaign)
    add_ideas(db, campaign, 'unwanted')
    scheduler = make_scheduler(db, runtime, tmp_path)
    assert scheduler.tick() == [1]
    wait_for_agent_session_handle(db)
    with db.session() as s:
        request_cancel(s, s.get_one(Run, 1), System.HUMAN, ending=Ending.NOW)
        s.commit()
    assert scheduler.tick() == []
    scheduler.wait()
    scheduler.stop(timeout=5)
    with db.session() as s:
        assert s.scalars(select(Event).where(has(StartupAnomaly))).all() == []
        assert s.get_one(AgentSession, 1).state is AgentSessionState.KILLED
        run = s.get_one(Run, 1)
        assert (run.status, run.end) == (RunStatus.FAILED, RunEnd.CANCELLED)
        assert s.scalar(select(func.count()).select_from(AgentSession)) == 1, 'not launched again'


def test_a_cancelled_request_never_launches_and_a_cancelled_sweep_is_ended(
    db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    runtime = PromptingRuntime()
    (idea,) = add_ideas(db, campaign, 'queued')
    with db.session() as s:
        s.get_one(Idea, idea).status = IdeaStatus.SCHEDULED
        run = Run(idea_id=idea, attempt=1, base_candidate_id=1, harness='script', model='m')
        s.add(run)
        s.flush()
        queued = AgentSession(kind=IMPLEMENTER, campaign_id=campaign.id, run_id=run.id, harness='script', model='m')
        sweep = AgentSession(
            kind=RESEARCH,
            campaign_id=campaign.id,
            runtime='subprocess',
            handle={'agent': 'research-2', 'pid': 4243},
            state=AgentSessionState.RUNNING,
        )
        requested = AgentSession(kind=RESEARCH, campaign_id=campaign.id, reason='low_water')
        s.add_all([queued, sweep, requested])
        s.flush()
        request_cancel(s, queued, System.HUMAN, ending=Ending.WRAP_UP)
        request_cancel(s, sweep, System.HUMAN, ending=Ending.NOW)
        request_cancel(s, requested, System.HUMAN, ending=Ending.NOW)
        s.commit()
    lease_running(db)
    scheduler = make_scheduler(db, runtime, tmp_path)
    assert scheduler.tick() == [], 'the queued run is cancelled before it launches'
    assert revoked(db) == ['research-2']
    with db.session() as s:
        w = s.get_one(AgentSession, 1)
        assert (w.state, w.error, s.get_one(Run, 1).end) == (
            AgentSessionState.KILLED,
            'cancelled by human',
            RunEnd.CANCELLED,
        )
        assert (s.get_one(AgentSession, 3).state, s.get_one(AgentSession, 3).error) == (
            AgentSessionState.KILLED,
            'cancelled by human',
        )
        assert s.get_one(Idea, idea).status is IdeaStatus.PARKED
    scheduler.stop(timeout=5)


def died_at_startup(db: Database, campaign: Campaign, kind: RoleName, minutes_ago: float = 0) -> AgentSessionId:
    """Add a killed agent row and the launcher's ``anomaly.startup`` event, ``minutes_ago`` minutes ago."""
    with db.session() as s:
        agent_session = AgentSession(
            kind=kind, campaign_id=campaign.id, harness='script', state=AgentSessionState.KILLED
        )
        s.add(agent_session)
        s.flush()
        payload = StartupAnomaly(
            role=kind,
            harness='script',
            run=None,
            seconds=35.0,
            error=f'FileNotFoundError: {kind}-{agent_session.id}: no such harness',
        )
        s.add(
            Event(
                actor='daemon',
                target=TargetRef(TargetKind.AGENT_SESSION, agent_session.id),
                payload=payload,
                at=utcnow() - timedelta(minutes=minutes_ago),
            )
        )
        s.commit()
        return agent_session.id


def pause_escalations(db: Database) -> list[Comment]:
    with db.session() as s:
        return list(s.scalars(select(Comment).where(Comment.kind == 'escalation').order_by(Comment.id)))


def test_startup_deaths_pause_launches_and_escalate_until_they_age_out(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """Three startup deaths within the window pause launches, with one escalation that says why.

    Without the pause, each death launches another agent on a host that is already overloaded. Launches resume when
    the deaths age out of the window.
    """
    died_at_startup(db, campaign, IMPLEMENTER, minutes_ago=11)
    for kind in (WATCHDOG, RESEARCH):
        died_at_startup(db, campaign, kind)
    wanted = request_watchdog(db, campaign)
    scheduler = make_scheduler(db, runtime, tmp_path)
    assert wanted in scheduler.tick(), 'two recent deaths: launches go on'
    scheduler.wait()

    died = [died_at_startup(db, campaign, WATCHDOG) for _ in range(2)]
    request_watchdog(db, campaign)
    assert scheduler.tick() == []
    [escalation] = pause_escalations(db)
    assert (escalation.target_kind, escalation.author, escalation.priority) == (TargetKind.CAMPAIGN, 'daemon', 1)
    assert escalation.body == IsStr(
        regex=r'Launches paused: 4 agents died at startup in the last 10 minutes\. Load average [\d.]+ / [\d.]+ / [\d.]+'
        rf' on \d+ CPUs when launches paused\..*\n- agent_session#\d+ \(research on script\): .*\n- agent_session#{died[0]} \(watchdog on script\):'
        rf' FileNotFoundError: watchdog-{died[0]}: no such harness\n'
        rf'- agent_session#{died[1]} .*',
        regex_flags=re.DOTALL,
    )
    assert scheduler.tick() == [] and len(pause_escalations(db)) == 1, 'one escalation while the pause holds'

    with db.session() as s:
        s.get_one(Comment, escalation.id).acked_at = utcnow()
        s.commit()
    assert scheduler.tick() == [] and len(pause_escalations(db)) == 1, 'acknowledged means seen: not posted again'

    latest = died_at_startup(db, campaign, RESEARCH)
    assert scheduler.tick() == []
    [extended] = pause_escalations(db)
    assert extended.id == escalation.id and extended.acked_at is not None, 'the same pause: updated in place'
    assert (
        extended.body.startswith('Launches paused: 5 agents') and f'- agent_session#{latest} (research' in extended.body
    )

    with db.session() as s:
        for event in s.scalars(select(Event).where(has(StartupAnomaly))):
            event.at -= STARTUP_WINDOW
        s.commit()
    assert scheduler.tick() != [], 'the deaths aged out of the window: launches resume'
    scheduler.wait()

    for _ in range(STARTUP_BREAKER):
        died_at_startup(db, campaign, WATCHDOG)
    request_watchdog(db, campaign)
    assert scheduler.tick() == [] and len(pause_escalations(db)) == 2, 'a new pause: a new escalation'
    with db.session() as s:
        kinds = s.scalars(select(Event.kind).where(Event.kind.like('launch.%')).order_by(Event.id)).all()
    assert kinds == ['launch.paused', 'launch.resumed', 'launch.paused']


def explore_setup(db: Database, campaign: Campaign, explore_every: int) -> tuple[int, int, int]:
    """Set up two finished trunk runs, a better keep (candidate 2) and three ready ideas.

    The ideas are an urgent one, one pinned to the head (a trunk run), and a someday one pinned to the baseline (a
    branch run).
    """
    set_config(db, campaign, roles={'implementer': {'max_concurrent': 1, 'explore_every': explore_every}})
    (done,) = add_ideas(db, campaign, 'history', status=IdeaStatus.DONE)
    urgent, level, explore = (
        add_ideas(db, campaign, t, priority=p)[0] for t, p in (('urgent', 0), ('level', 1), ('explore', 4))
    )
    commit_candidate(db, campaign, 1, {'other.txt': 'x\n'}, Verdict.KEEP)
    with db.session() as s:
        s.add(Measurement(candidate_id=2, evaluator_id=1, metrics={'score': 9, 'size': 9}))
        s.get_one(Idea, level).base_candidate_id = CandidateId(2)
        s.get_one(Idea, explore).base_candidate_id = CandidateId(1)
        for attempt in (1, 2):
            s.add(
                Run(
                    idea_id=done,
                    attempt=attempt,
                    base_candidate_id=1,
                    harness='h',
                    model='m',
                    status=RunStatus.SUCCEEDED,
                    end=RunEnd.FINISHED,
                )
            )
        s.commit()
    return urgent, level, explore


def queue_one(db: Database, scheduler: Scheduler) -> tuple[int, bool]:
    """Queue the next run and finish it at once to free the slot.

    Returns:
        The run's idea id and whether the run is a branch run.
    """
    with db.session() as s:
        p = s.scalars(select(Campaign)).one()
        scheduler._queue(s, p, CampaignConfig.model_validate(p.config))
        run = s.scalars(select(Run).order_by(Run.id.desc())).first()
        assert run is not None
        run.status, run.end = RunStatus.SUCCEEDED, RunEnd.FINISHED
        s.commit()
        return run.idea_id, run.branch


def test_every_nth_run_starts_from_an_alternative_base_whatever_its_priority(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """Every nth run starts from an alternative base, whatever its priority."""
    urgent, level, explore = explore_setup(db, campaign, explore_every=3)
    scheduler = make_scheduler(db, runtime, tmp_path)
    assert queue_one(db, scheduler) == (explore, True), 'the last two runs were no branch: this one explores'
    assert queue_one(db, scheduler) == (urgent, False), 'a branch run among the last two: priority order'
    assert queue_one(db, scheduler) == (level, False), 'a pin to the computed base is no alternative'
    (later,) = add_ideas(db, campaign, 'later')
    assert queue_one(db, scheduler) == (later, False), 'due, but no idea pinned to an alternative base is ready'


def test_explore_every_0_keeps_the_priority_order(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    urgent, _, _ = explore_setup(db, campaign, explore_every=0)
    assert queue_one(db, make_scheduler(db, runtime, tmp_path)) == (urgent, False)


TWO_TRACKS: dict[str, object] = {
    'metrics': [
        {'name': 'score', 'direction': 'higher', 'role': 'primary', 'track': 'accuracy', 'margin': 1},
        {'name': 'size', 'direction': 'lower', 'role': 'primary', 'track': 'cost', 'margin': 5},
    ],
    'tracks': {'accuracy': {'share': 0.75}, 'cost': {'share': 0.25}},
}
"""The test campaign's two metrics as two tracks: accuracy (``score``) and cost (``size``), three slots to one."""


def queue(db: Database, scheduler: Scheduler, campaign: Campaign) -> list[tuple[int, RunQueued]]:
    """Run one ``_queue`` pass.

    Returns:
        The idea and the ``run.queued`` payload of every queued run, in order.
    """
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        scheduler._queue(s, p, CampaignConfig.model_validate(p.config))
        s.commit()
    with db.session() as s:
        rows = s.scalars(select(Event).where(has(RunQueued)).order_by(Event.id)).all()
        return [(p.idea, p) for p in (payload_of(e, RunQueued) for e in rows)]


def test_a_free_slot_goes_to_the_track_furthest_below_its_share_whatever_the_priorities(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    set_config(db, campaign, roles={'implementer': {'max_concurrent': 4}}, **TWO_TRACKS)
    accuracy = add_ideas(db, campaign, 'a1', 'a2', 'a3', 'a4', priority=0, track='accuracy')
    (cost,) = add_ideas(db, campaign, 'c1', priority=3, track='cost')
    queued = queue(db, make_scheduler(db, runtime, tmp_path), campaign)
    assert [idea for idea, _ in queued] == [*accuracy[:3], cost], (
        'three of four slots for the 0.75 share, the fourth for cost although its idea is the least urgent'
    )
    assert [(p.track, p.priority_pick) for _, p in queued] == [
        ('accuracy', None),
        ('accuracy', None),
        ('accuracy', None),
        ('cost', accuracy[3]),
    ], 'the share rule bound once: plain priority order would have taken a4'


def test_a_track_with_nothing_runnable_gives_its_slot_away(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    set_config(db, campaign, roles={'implementer': {'max_concurrent': 4}}, **TWO_TRACKS)
    accuracy = add_ideas(db, campaign, 'a1', 'a2', 'a3', 'a4', track='accuracy')
    queued = queue(db, make_scheduler(db, runtime, tmp_path), campaign)
    assert [idea for idea, _ in queued] == accuracy, 'no cost idea: its slot goes to accuracy'
    assert all(p.priority_pick is None for _, p in queued)


def test_an_idea_on_a_track_the_config_does_not_name_is_parked_and_a_track_at_share_zero_waits(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    wound_down = {**TWO_TRACKS, 'tracks': {'accuracy': {'share': 1}, 'cost': {'share': 0}}}
    set_config(db, campaign, **wound_down)
    (stray,) = add_ideas(db, campaign, 'on a renamed track', track='speed')
    (waiting,) = add_ideas(db, campaign, 'cost work', track='cost')
    assert queue(db, make_scheduler(db, runtime, tmp_path), campaign) == []
    with db.session() as s:
        assert (s.get_one(Idea, stray).status, s.get_one(Idea, waiting).status) == (IdeaStatus.PARKED, IdeaStatus.READY)
        [note] = s.scalars(select(Comment).where(Comment.target_id == stray)).all()
        assert note.body == (
            "parked: its track 'speed' is not in piloop.toml (tracks: accuracy, cost); set ideas.track to one of"
            ' them, then set it ready'
        )


def test_low_water_counts_each_track_short_of_its_share_whatever_the_others_hold(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    research = {'triggers': low_water(0), 'max_ideas': 1}
    set_config(db, campaign, roles={'implementer': {'max_concurrent': 4}, 'research': research}, **TWO_TRACKS)
    add_ideas(db, campaign, *(f'a{i}' for i in range(6)), track='accuracy')
    scheduler = make_scheduler(db, runtime, tmp_path)
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        cfg = CampaignConfig.model_validate(p.config)
        backlog = scheduler._backlog(s, p, cfg, RESEARCH, 0)
        assert (backlog.runnable, backlog.target, backlog.short) == (6, 4, 1), 'cost is one short; accuracy has 3 spare'
        assert str(backlog) == '6 of 4 runnable ideas (accuracy 6 of 3, cost 0 of 1)'
        fire = LowWaterTrigger(buffer=0).fire(scheduler._tick(s, p, cfg, RESEARCH, role_of(cfg, ResearchRole)))
        assert fire is not None and fire.reason == str(backlog)
    add_ideas(db, campaign, 'c0', track='cost')
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        assert scheduler._backlog(s, p, cfg, RESEARCH, 0).short == -3, 'no track short: the surplus, as with one track'


def test_an_implementer_is_told_once_to_wrap_up_before_its_timeout(
    db: Database, campaign: Campaign, tmp_path: Path, mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without a warning, the role timeout kills an implementer mid-probe, with its last step uncommitted.

    The wrap-up is a note on the agent, which its executor delivers like any other. The role timeout still ends it.
    """
    runtime = PromptingRuntime()
    running_agent_session(db, campaign, 'long')
    lease_running(db)
    scheduler = make_scheduler(db, runtime, tmp_path)
    fake = prompted(db, scheduler, mocker)
    with db.session() as s:
        timeout = config_of(s.get_one(Campaign, campaign.id)).role(IMPLEMENTER).timeout
    lead = wrap_up_lead(timeout)

    def started(ago: float) -> None:
        with db.session() as s:
            s.get_one(AgentSession, 1).started_at = utcnow() - timedelta(seconds=ago)
            s.commit()

    started(timeout - lead - 5)
    scheduler.tick()
    scheduler.tick()
    assert fake.prompts == [], 'not yet'
    started(timeout - lead)
    monkeypatch.setattr('piloop.daemon.scheduler.STOP_GRACE_SECONDS', 0)
    for _ in range(3):
        scheduler.tick()
    [(agent, prompt)] = fake.prompts  # once
    assert agent == 1 and prompt.text == IsStr(
        regex=re.escape(
            f'[piloop] Your time is almost up (the role timeout of {timeout:g}s). Commit what you have now, with a'
            ' message that says where it stands. Leave your finding and stop. This session ends in'
        )
        + r' [\d.]+ minutes\.'  # the time left when the tick ran: a loaded host takes seconds of it
    )
    assert revoked(db) == [], 'the role timeout ends it, not the daemon'
    assert [kind for kind, _ in settled(db, 'agent_session.wrap_up')] == [1]
    scheduler.stop(timeout=5)


def test_an_implementer_is_ended_a_minute_after_its_finding(db: Database, campaign: Campaign, tmp_path: Path) -> None:
    """The finding is the implementer's report that it is done. A job that it left running must not hold its slot.

    The daemon ends it ``FINDING_GRACE_SECONDS`` after the finding, with no prompt: a commit after the finding takes
    seconds. The run closes as finished (``run_outcome``).
    """
    runtime = PromptingRuntime()
    idea = running_agent_session(db, campaign, 'build')
    lease_running(db)
    scheduler = make_scheduler(db, runtime, tmp_path)
    found = comment(db, 'finding', 'idea', idea, 'done', author='implementer#1')
    scheduler.tick()
    assert revoked(db) == [], 'within the grace: it may still commit'
    with db.session() as s:
        s.get_one(Comment, found).created_at = utcnow() - timedelta(seconds=61)
        s.commit()
    scheduler.tick()
    scheduler.tick()
    assert (revoked(db), runtime.prompts) == (['implementer-1'], [])
    assert settled(db, 'agent_session.stop_ended') == [(1, AgentEnded(reason='left its finding', end=RunEnd.FINISHED))]
    scheduler.stop(timeout=5)


def test_an_implementer_that_left_a_job_running_after_its_finding_succeeds_and_the_job_is_killed(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A headless agent whose background job outlives its turn does not exit: it held the slot to the role timeout."""
    finding = (
        "INSERT INTO comments (target_kind, target_id, author, kind, body) VALUES ('idea', 1, 'implementer#1', 'finding',"
        " 'done')"
    )
    harness = (
        f'#!/bin/sh\necho step >> solve.txt\ngit commit -qam "step"\n{SQL_HINT} "{finding}"\n'
        'sleep 300 &\necho $! > ../child.pid\nwait\n'
    )
    write_toml(Path(campaign.path), harness, implementer_timeout=300)
    set_config(db, campaign)
    (idea,) = add_ideas(db, campaign, 'long')
    monkeypatch.setattr('piloop.daemon.scheduler.FINDING_GRACE_SECONDS', 0)
    scheduler = make_scheduler(db, runtime, tmp_path)
    assert scheduler.tick() == [1]
    pid_file = tmp_path / 'runs' / '1' / 'child.pid'
    wait_until(lambda: pid_file.exists() and pid_file.read_text().strip())
    child = int(pid_file.read_text())
    start = proc.started(child)
    assert start is not None
    scheduler.tick()
    scheduler.wait(20)
    with db.session() as s:
        run = s.get_one(Run, 1)
        assert (run.status, run.end) == (RunStatus.SUCCEEDED, RunEnd.FINISHED)
        assert s.get_one(Idea, idea).status is IdeaStatus.DONE
    assert not proc.alive(child, start), 'the job it left is killed with it'
    scheduler.stop(timeout=5)


def test_a_stopped_campaign_is_measured_again_only_on_request(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """Measuring a suspicious result again is part of the review after a campaign stops. The head and the bars that
    runs need are moot then: a new evaluator measures nothing unasked."""
    doubled = ['sh', '-c', 'echo METRIC score=$(( 2 * $(wc -l < solve.txt) )); echo METRIC size=1']
    write_toml(Path(campaign.path), HARNESS_APPEND, evaluate=doubled)
    with db.session() as s:
        sync(s, s.get_one(Campaign, campaign.id))
        s.commit()
    stop_campaign(db, campaign, 'plateau: 5 runs without a keep')
    scheduler = make_scheduler(db, runtime, tmp_path)
    measured = mocker.patch.object(scheduler._controller, 'remeasure')
    scheduler.tick()
    scheduler.wait()
    measured.assert_not_called()
    with db.session() as s:
        request_remeasure(s, s.get_one(Candidate, 1), System.HUMAN)
        s.commit()
    scheduler.tick()
    scheduler.wait()
    measured.assert_called_once_with(1)


def test_the_daemon_measures_a_candidate_again_when_someone_asks(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """A requested re-measure goes through the queue of re-measures, after the head and the bars that runs need."""
    scheduler = make_scheduler(db, runtime, tmp_path)
    measured = mocker.patch.object(scheduler._controller, 'remeasure')
    with db.session() as s:
        request_remeasure(s, s.get_one(Candidate, 1), System.ORCHESTRATOR)
        s.commit()
    scheduler.tick()
    scheduler.wait()
    measured.assert_called_once_with(1)
