import json
import re
import threading
import time
from contextlib import closing
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

from piloop import git, proc
from piloop.agents.launcher import STARTUP_ANOMALY
from piloop.agents.roles import SQL_HINT, compose_brief
from piloop.campaign.campaigns import sync
from piloop.campaign.stop import (
    BLOCKED,
    CAMPAIGN_STOPPED_ERROR,
    CANCELLED_ERROR,
    STALE_ERROR,
    request_cancel,
    request_stop,
    runs,
)
from piloop.config import CampaignConfig, load_campaign_config
from piloop.daemon.run_executor import UNCOMMITTED_SUMMARY, RunExecutor
from piloop.daemon.scheduler import STARTUP_BREAKER, STARTUP_WINDOW, STOPPED_ERROR, Scheduler
from piloop.db import sql
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
    Idea,
    IdeaLink,
    IdeaStatus,
    LinkKind,
    Measurement,
    RoleName,
    Run,
    RunStatus,
    TargetKind,
    Verdict,
    utcnow,
)
from piloop.db.session import Database
from piloop.db.triggers import DAEMON_ACTOR
from piloop.otel import Tracer
from piloop.plugins import host as plugins
from piloop.reports.overview import SILENT_SECONDS
from piloop.runtime import AgentStatus, Handle, NotSteerable
from piloop.scoring.ledger import current_evaluator, due, head, import_reference, request_remeasure
from piloop.triggers import LowWaterTrigger
from tests.conftest import (
    HARNESS_APPEND,
    HARNESS_SLEEPS,
    HARNESS_TWO_COMMITS,
    Gate,
    ScriptRuntime,
    events,
    factory,
    span_named,
    wait_for_agent_session_handle,
    wait_until,
    write_toml,
)
from tests.daemon.test_triggers import low_water
from tests.usage.test_usage import PI_ID, store


def make_scheduler(db: Database, runtime: ScriptRuntime, tmp_path: Path, tracer: Tracer | None = None) -> Scheduler:
    tracer = tracer or Tracer(None)
    executor = RunExecutor(db, tracer, tmp_path, factory(runtime))
    return Scheduler(db, executor, tracer, tmp_path, factory(runtime))


def request_watchdog(db: Database, campaign: Campaign) -> int:
    with db.session() as s:
        agent_session = AgentSession(kind=RoleName.WATCHDOG, campaign_id=campaign.id, reason='by hand')
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
            RoleName.IMPLEMENTER,
            1,
            'script',
            'test-model',
            f'idea#{high} is ready',
        )
    assert scheduler.tick() == [], 'max_concurrent=1: the second idea waits'
    scheduler.wait(timeout=30)

    assert scheduler.tick() == [2]
    scheduler.wait(timeout=30)
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
    scheduler.wait(timeout=30)
    with db.session() as s:
        best = s.get_one(Candidate, 2)
        assert best.verdict is Verdict.KEEP and best.commit_sha != s.get_one(Candidate, 1).commit_sha
        best_sha = best.commit_sha
    write_toml(Path(campaign.path), '#!/bin/sh\ngit rev-parse HEAD > workspace/head.txt\n')
    set_config(db, campaign)
    agent_session_id = request_watchdog(db, campaign)
    assert scheduler.tick() == [agent_session_id]
    scheduler.wait(timeout=30)
    head = tmp_path / 'projects' / str(campaign.id) / 'workspace' / 'head.txt'
    assert head.read_text().strip() == best_sha
    assert (
        'detached checkout of candidate#2, the head (score=2.0)'
        in (tmp_path / 'agent_sessions' / str(agent_session_id) / 'brief.md').read_text()
    )
    assert git.worktrees_under(Repo(campaign.path), tmp_path / 'agent_sessions') == [], 'removed with the sweep'
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
    scheduler.wait(timeout=30)
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
    scheduler.wait(timeout=30)
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
            AgentSession(
                kind=RoleName.IMPLEMENTER, campaign_id=campaign.id, harness='script', state=AgentSessionState.RUNNING
            )
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
        rows = [AgentSession(kind=RoleName.WATCHDOG, campaign_id=campaign.id, reason='by hand') for _ in range(3)]
        rows.append(AgentSession(kind=RoleName.WATCHDOG, campaign_id=campaign.id, harness='script', reason='pinned'))
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
        assert (rerouted.target_id, rerouted.payload) == (runs[0].id, {'from': 'script', 'to': 'pi'})


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
        s.get_one(Idea, branch).base_candidate_id = 1
        s.get_one(Idea, missing).base_candidate_id = 99
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
        assert (queued.payload['pinned_base'], queued.payload['computed_base']) == (1, 2)
        assert run.branch, 'a pin other than the computed base: a branch run, judged against its own base'
        assert s.get_one(Idea, missing).status is IdeaStatus.PARKED, 'a pin to a candidate that does not exist'
        assert s.scalars(select(Run).where(Run.idea_id == missing)).all() == []


def commit_candidate(db: Database, campaign: Campaign, parent: int, files: dict[str, str], verdict: Verdict) -> int:
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

    hiloopai/piloop-dev#60: a pin that has the head's changes builds on the head too, whatever its nearest keep. Here
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
    scheduler.wait(timeout=30)
    assert scheduler.tick() == [2]
    with db.session() as s:
        assert s.get_one(Run, 2).idea_id == second, 'one prerequisite done is not enough'
        assert s.get_one(Idea, third).status is IdeaStatus.READY
    scheduler.wait(timeout=30)
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
    scheduler.wait(timeout=30)
    scheduler.stop(timeout=1)
    with db.session() as s:
        run = s.get_one(Run, 3)
        assert (run.idea_id, run.base_candidate_id) == (third, 4), 'the head, which holds what the prerequisites kept'
        assert s.get_one(Idea, third).status is IdeaStatus.DONE


def test_a_prerequisite_that_is_not_done_keeps_blocking(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    (rejected,) = add_ideas(db, campaign, 'rejected', status=IdeaStatus.REJECTED)
    (parked,) = add_ideas(db, campaign, 'parked', status=IdeaStatus.PARKED)
    (proposed,) = add_ideas(db, campaign, 'proposed', status=IdeaStatus.PROPOSED)
    (after_rejected,) = add_ideas(db, campaign, 'after rejected', after=(rejected,))
    (after_parked,) = add_ideas(db, campaign, 'after parked', after=(parked,))
    (after_proposed,) = add_ideas(db, campaign, 'after proposed', after=(proposed,))
    (chained,) = add_ideas(db, campaign, 'after a blocked one', after=(after_rejected,))
    scheduler = make_scheduler(db, runtime, tmp_path)

    assert scheduler.tick() == [], 'rejected, parked and proposed prerequisites all block; so does a blocked one'
    assert statuses(db)[3:] == [IdeaStatus.READY] * 4, 'blocked ideas stay ready for the watchdog or a human'

    with db.session() as s:
        s.get_one(Idea, rejected).status = IdeaStatus.DONE  # a human reconsidered; it never ran, so it kept nothing
        s.commit()
    assert scheduler.tick() == [1]
    scheduler.wait(timeout=30)
    scheduler.stop(timeout=1)
    with db.session() as s:
        run = s.get_one(Run, 1)
        assert (run.idea_id, run.base_candidate_id) == (after_rejected, 1), 'the head: the baseline'
        assert s.get_one(Idea, after_rejected).status is IdeaStatus.DONE
        assert [s.get_one(Idea, i).status for i in (after_parked, after_proposed, chained)] == [IdeaStatus.READY] * 3


def test_low_water_does_not_count_blocked_ideas_as_backlog(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    set_config(db, campaign, roles={'research': {'max_ideas': 1, 'max_concurrent': 1, 'timeout': 20}})
    (gate,) = add_ideas(db, campaign, 'gate', status=IdeaStatus.PROPOSED)
    add_ideas(db, campaign, 'behind the gate', after=(gate,))
    scheduler = make_scheduler(db, runtime, tmp_path)
    assert scheduler.tick() == [1]
    scheduler.wait(timeout=30)
    scheduler.stop(timeout=1)
    with db.session() as s:
        agent_session = s.get_one(AgentSession, 1)
        assert (agent_session.kind, agent_session.reason) == (
            RoleName.RESEARCH,
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
        s.add(Run(idea_id=retrying, attempt=1, base_candidate_id=1, harness='h', model='m', status=RunStatus.FAILED))
        p = s.get_one(Campaign, campaign.id)
        p.status, p.stopped_reason = CampaignStatus.STOPPED, 'by the test'  # bookkeeping happens, nothing launches
        s.commit()
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
            (ready, 'daemon', {'old': 'ready', 'new': 'rejected'}),
            (proposed, 'daemon', {'old': 'proposed', 'new': 'rejected'}),
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
        assert (run.status, run.error) == (RunStatus.FAILED, STALE_ERROR)
        assert s.get_one(Idea, idea).status is IdeaStatus.READY, 'a crash is not an attempt: its only one remains'
        assert s.get_one(AgentSession, 1).state is AgentSessionState.KILLED
        sweep = s.get_one(AgentSession, 2)
        assert sweep.state is AgentSessionState.KILLED
        assert sweep.usage is not None and sweep.usage['total'] == 16478, (
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
    mocker.patch.object(scheduler._executor, 'execute', side_effect=RuntimeError('thread broke'))
    add_ideas(db, campaign, 'x')
    scheduler.tick()
    scheduler.wait(30)
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
        assert (run.status, run.error) == (RunStatus.FAILED, STOPPED_ERROR)
        assert s.get_one(Idea, idea).status is IdeaStatus.READY
        assert (s.get_one(AgentSession, 1).state, s.get_one(AgentSession, 1).error) == (
            AgentSessionState.KILLED,
            STOPPED_ERROR,
        )
        assert s.scalars(select(Candidate).where(Candidate.run_id == 1)).all() == [], 'no scoring after stop'
    assert not (tmp_path / 'runs' / '1' / 'worktree').exists()
    assert dict(span_named(spans, 'run.aborted').attributes or {}) == {'run': 1, 'error': STOPPED_ERROR}
    assert dict(span_named(spans, 'piloop.run').attributes or {})['status'] == 'cancelled'


def test_stop_keeps_the_work_of_the_runs_it_cuts_pinned_and_noted_for_the_next_attempt(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A stop pins the commits and the uncommitted work of the runs it ends, and notes them for the next attempt.

    The note reaches the brief of the next run on the same idea.
    """
    harness = (
        '#!/bin/sh\necho step >> solve.txt\ngit commit -qam "step"\necho draft >> solve.txt\n'
        'sleep 30 &\necho $! > ../child.pid\nwait\n'
    )
    write_toml(Path(campaign.path), harness, implementer_timeout=60)
    set_config(db, campaign)
    add_ideas(db, campaign, 'long')
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
        (note,) = s.scalars(select(Comment).where(Comment.target_kind == TargetKind.RUN, Comment.target_id == 1)).all()
        assert (note.author, note.kind) == (DAEMON_ACTOR, CommentKind.NOTE)
        assert f'its 2 commits are pinned, unscored, at refs/piloop/demo/r1/<n> (head {head})' in note.body
        assert s.scalars(select(Candidate).where(Candidate.run_id == 1)).all() == [], 'pinned, not scored'


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
        ended = (RunStatus.FAILED, f'{BLOCKED} comment#2') if escalated else (RunStatus.SUCCEEDED, None)
        assert (run.status, run.error, run.scoring) == (*ended, True)
        assert s.get_one(Idea, idea).status is IdeaStatus.SCHEDULED, 'not ready: a second run would redo the work'
        notes = s.scalars(select(Comment).where(Comment.author == DAEMON_ACTOR)).all()
        assert notes == [], 'its commits are scored, so no note sends the next run to them'
    [step] = git.pinned(Repo(campaign.path), 'refs/piloop/demo/r1/')
    assert Repo(campaign.path).commit(step).summary == 'step'

    restarted = make_scheduler(db, runtime, tmp_path)
    restarted.reap()
    restarted.wait(30)
    with db.session() as s:
        [candidate] = s.scalars(select(Candidate).where(Candidate.run_id == 1)).all()
        assert (candidate.summary, candidate.verdict) == ('step', Verdict.KEEP)
        assert s.get_one(Idea, idea).status is (IdeaStatus.PARKED if escalated else IdeaStatus.DONE)
        assert s.get_one(Run, 1).scoring is False


def test_stop_closes_herdr_tabs(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, herdr_log: Path
) -> None:
    with db.session() as s:
        s.add(
            AgentSession(
                kind='implementer',
                campaign_id=campaign.id,
                harness='pi',
                model='m',
                runtime='herdr',
                handle={'workspace_id': 'w7', 'tab_id': 'w7:t2', 'pane_id': 'w7:p2', 'agent': 'implementer-1'},
                state=AgentSessionState.RUNNING,
                log_dir='x',
            )
        )
        s.commit()
    executor = RunExecutor(db, Tracer(None), tmp_path, factory(runtime))
    Scheduler(db, executor, Tracer(None), tmp_path).stop(timeout=5)
    assert [json.loads(line) for line in herdr_log.read_text().splitlines()] == [
        ['--session', 'piloop', 'tab', 'close', 'w7:t2']
    ]
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
    second.wait(30)
    first.wait(10)
    with db.session() as s:
        sweep = s.get_one(AgentSession, agent_session_id)
        assert (sweep.state, sweep.error) == (AgentSessionState.EXITED, None)
    assert not (sweep_dir / 'worktree').exists()
    second.stop(timeout=5)


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
    monkeypatch.setattr('piloop.daemon.run_executor.git.add_worktree', gate.wrap(git.add_worktree))
    scheduler = make_scheduler(db, runtime, tmp_path, tracer)
    (idea,) = add_ideas(db, campaign, 'x')
    scheduler.tick()
    assert gate.entered.wait(10)
    started = time.monotonic()
    scheduler.stop(timeout=0.5)  # the run thread is past its run-row write, before its start window
    assert time.monotonic() - started < 3
    gate.release.set()
    scheduler.wait(10)
    with db.session() as s:
        run = s.get_one(Run, 1)
        assert (run.status, run.error) == (RunStatus.FAILED, STOPPED_ERROR)
        assert s.get_one(Idea, idea).status is IdeaStatus.READY
        agent_session = s.get_one(AgentSession, 1)
        assert (agent_session.state, agent_session.error, agent_session.runtime, agent_session.handle) == (
            AgentSessionState.KILLED,
            STOPPED_ERROR,
            None,
            {},
        ), 'closed by stop(); nothing was started'
    assert not (tmp_path / 'runs' / '1' / 'worktree').exists()
    assert events(spans, 'agent_session.kill_failed') == [], 'a row without a process is not asked to die'
    assert dict(span_named(spans, 'piloop.run').attributes or {})['status'] == 'cancelled'
    assert dict(span_named(spans, 'piloop.agent').attributes or {}) == {
        'agent_session': 1,
        'state': 'killed',
        'error': STOPPED_ERROR,
    }


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
        assert (agent_session.state, agent_session.error) == (AgentSessionState.KILLED, STOPPED_ERROR)
        assert proc.started(int(str(agent_session.handle['pid']))) is None
        run = s.get_one(Run, 1)
        assert (run.status, run.error) == (RunStatus.FAILED, STOPPED_ERROR)
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
    set_config(db, campaign, evaluate=['sh', '-c', f'echo $$ > {pid_file}; exec sleep 30'])
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
    assert dict(span_named(spans, 'piloop.run').attributes or {})['status'] == 'cancelled'
    assert not (tmp_path / 'runs' / '1' / 'worktree').exists()
    assert not (tmp_path / 'runs' / '1' / 'eval-0' / 'worktree').exists()


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
    wait_until((tmp_path / 'runs' / '1' / 'eval-0' / 'eval.stdout').exists)
    first.stop(timeout=10)
    with db.session() as s:
        assert s.get_one(Idea, 1).status is IdeaStatus.SCHEDULED
        assert [c.verdict for c in s.scalars(select(Candidate).where(Candidate.run_id == 1))] == [Verdict.PENDING] * 2

    hold.unlink()
    second = make_scheduler(db, runtime, tmp_path)
    second.reap()
    second.wait(30)
    with db.session() as s:
        made = s.scalars(select(Candidate).where(Candidate.run_id == 1).order_by(Candidate.id)).all()
        assert [(c.summary, c.verdict, len(c.measurements)) for c in made] == [
            ('first', Verdict.KEEP, 1),
            ('second', Verdict.KEEP, 1),
        ], 'the cut commit and the one never reached are both measured and judged'
        assert s.get_one(Idea, 1).status is IdeaStatus.DONE, 'settled by its one finished run'
    second.tick()
    second.wait(30)
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
                scoring=True,
            )
        )
        s.get_one(Idea, idea).status = before
        s.get_one(Campaign, campaign.id).path = str(tmp_path / 'gone')
        s.commit()
    sched = make_scheduler(db, runtime, tmp_path, tracer)
    sched.reap()
    sched.wait(10)
    with db.session() as s:
        assert s.get_one(Idea, idea).status is (after or before), 'attempts remain; a parked idea stays parked'
        assert s.scalar(select(Run.scoring)) is False
    assert span_named(spans, 'piloop.resume').status.status_code is StatusCode.ERROR


def measured_by(db: Database, candidate_id: int) -> list[tuple[int, float | None, str | None]]:
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
    sched.wait(30)
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
    sched.wait(30)
    assert measured_by(db, kept.id) == [(old, 2.0, None), (new, 4.0, None)], 'the head first'
    assert measured_by(db, 1) == [(old, 1.0, None)], 'one at a time'

    assert sched.tick() == [2], 'the head has a score: the run starts from it'
    sched.wait(30)
    assert measured_by(db, 1) == [(old, 1.0, None)], 'the baseline is no base or bar of any run: nobody asked'
    with db.session() as s:
        assert s.get_one(Run, 2).base_candidate_id == kept.id
        assert s.get_one(Candidate, kept.id).verdict is Verdict.KEEP, 'measuring again judges nothing again'
        request_remeasure(s, s.get_one(Candidate, 1), 'human')
        s.commit()
    sched.tick()
    sched.wait(30)
    assert measured_by(db, 1) == [(old, 1.0, None), (new, 2.0, None)], 'measured when asked'
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
    sched.wait(30)
    add_ideas(db, campaign, 'second line')
    sched.tick()
    sched.wait(30)
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
    sched.wait(30)
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
    sched.wait(30)
    (second,) = add_ideas(db, campaign, 'second line')
    sched.tick()
    sched.wait(30)
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
    sched.wait(30)
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
        reference = import_reference(s, proj, cfg, repo, 'elsewhere', summary=None, actor='orchestrator').id
        evaluator = proj.evaluator_id
        s.commit()
    sched = make_scheduler(db, runtime, tmp_path)
    sched.tick()
    sched.wait(30)
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
    sched.wait(10)
    [(_, score, error)] = measured_by(db, 2)
    assert score is None and error is not None and error.startswith('GitCommandError'), error
    sched.tick()
    sched.wait(10)
    assert len(measured_by(db, 2)) == 1, 'a crash is an answer: not measured again'
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
    scheduler.wait(10)
    with db.session() as s:
        agent_session = s.get_one(AgentSession, agent_session_id)
        assert (agent_session.state, agent_session.error, agent_session.harness, agent_session.handle) == (
            AgentSessionState.KILLED,
            STOPPED_ERROR,
            'script',
            {},
        )
        assert s.scalars(select(Event).where(Event.kind == 'watchdog.started')).all() == []
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
        assert (agent_session.state, agent_session.error) == (AgentSessionState.KILLED, STOPPED_ERROR)
        assert proc.started(int(str(agent_session.handle['pid']))) is None
    assert scheduler.active() == []


class PromptingRuntime(ScriptRuntime):
    """A subprocess runtime with a prompt channel that records what the daemon sends, in place of a Herdr pane."""

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
                kind=RoleName.IMPLEMENTER,
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


def comment(
    db: Database, kind: str, target_kind: str, target_id: int, body: str, *, author: str = 'human', priority: int = 2
) -> int:
    """Add a comment through the raw SQL path that agents use. ``created_at`` is CURRENT_TIMESTAMP, to the second.

    Each comment is addressed to the human at ``priority``. An escalation must have this, and other kinds may. The
    push to the implementer ignores it.
    """
    with closing(sql.connect(Path(db.engine.url.database or ''), actor=author)) as con:
        return sql.run(
            con,
            f"INSERT INTO comments (target_kind, target_id, author, kind, body, priority) VALUES ('{target_kind}',"
            f" {target_id}, '{author}', '{kind}', '{body}', {priority})",
        ).last_insert_rowid


def settled(db: Database, kind: str) -> list[tuple[int, dict[str, object]]]:
    with db.session() as s:
        return [(e.target_id, e.payload) for e in s.scalars(select(Event).where(Event.kind == kind))]


def test_comments_on_a_running_run_or_its_idea_reach_the_implementer_once_as_one_line(
    db: Database, campaign: Campaign, tmp_path: Path, tracer: Tracer, spans: InMemorySpanExporter
) -> None:
    runtime = PromptingRuntime()
    idea = running_agent_session(db, campaign, 'live')
    coming_up = running_agent_session(db, campaign, 'coming up', state=AgentSessionState.STARTING)
    with db.session() as s:
        before = utcnow() - timedelta(minutes=5)
        s.add(
            Comment(target_kind=TargetKind.IDEA, target_id=idea, author='human', body='in the brief', created_at=before)
        )
        s.commit()
    scheduler = make_scheduler(db, runtime, tmp_path, tracer)
    assert scheduler.tick() == [] and runtime.prompts == [], 'a comment from before the run started is in its brief'

    decision = comment(db, 'decision', 'idea', idea, 'keep solve.py under 120 lines\nthe rest is detail')
    note = comment(db, 'note', 'run', 1, 'x' * 250, author='pi')
    comment(db, 'finding', 'campaign', campaign.id, 'not for any implementer')
    waiting = comment(db, 'note', 'idea', coming_up, 'its implementer is starting: waits for its pane')
    comment(db, 'escalation', 'idea', idea, 'for the human, not the pane')
    scheduler.tick()
    assert runtime.prompts == [
        (
            'implementer-1',
            '[piloop] decision on idea#1 by human: keep solve.py under 120 lines — full text: SELECT body FROM comments'
            f' WHERE id=2\n[piloop] note on run#1 by pi: {"x" * 199}… — full text: SELECT body FROM comments WHERE id=3',
        )
    ], 'one prompt with one line per comment: two prompts close together run into one another in the queue'
    assert settled(db, 'comment.delivered') == [(1, {'comment': decision}), (1, {'comment': note})]
    assert events(spans, 'comment.delivered') == [
        {'comment': decision, 'agent_session': 1},
        {'comment': note, 'agent_session': 1},
    ]
    scheduler.tick()
    assert len(runtime.prompts) == 1, 'settled by the events: nothing is delivered twice'

    with db.session() as s:
        s.get_one(AgentSession, 2).state = AgentSessionState.RUNNING
        s.commit()
    scheduler.tick()
    assert runtime.prompts[-1] == (
        'implementer-2',
        '[piloop] note on idea#2 by human: its implementer is starting: waits for its pane — full text: SELECT body FROM'
        ' comments WHERE id=5',
    ), 'a comment that landed while the pane was coming up is delivered once the implementer runs'
    assert settled(db, 'comment.delivered')[-1] == (2, {'comment': waiting})

    restarted = make_scheduler(db, runtime, tmp_path)
    restarted.tick()
    assert len(runtime.prompts) == 2, 'a new daemon replays nothing: the record is in the database'
    later = comment(db, 'decision', 'run', 1, 'and stop after the next commit')
    restarted.tick()
    assert runtime.prompts[-1][0] == 'implementer-1' and settled(db, 'comment.delivered')[-1] == (1, {'comment': later})


def test_an_implementers_own_comments_are_not_echoed_back(db: Database, campaign: Campaign, tmp_path: Path) -> None:
    runtime = PromptingRuntime()
    idea = running_agent_session(db, campaign, 'live')
    scheduler = make_scheduler(db, runtime, tmp_path)
    comment(db, 'finding', 'idea', idea, 'what I tried', author='implementer#1')
    comment(db, 'note', 'run', 1, 'signed the other documented way', author='run#1')
    comment(db, 'note', 'idea', idea, 'a 0.1.0 agent signs as before the upgrade', author='worker#1')
    scheduler.tick()
    assert runtime.prompts == [] and settled(db, 'comment.delivered') == []
    comment(db, 'note', 'idea', idea, 'a sibling implementer is not the author', author='implementer#2')
    scheduler.tick()
    assert [text.split(' — ')[0] for _, text in runtime.prompts] == [
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

    The subprocess runtime raises NotSteerable. The tick records this on the implementer and does not try again.
    """
    idea = running_agent_session(db, campaign, 'headless')
    scheduler = make_scheduler(db, runtime, tmp_path, tracer)
    stuck = comment(db, 'decision', 'idea', idea, 'a headless implementer has no input channel')
    assert scheduler.tick() == []
    error = IsStr(regex='subprocess agent pid 4242 cannot take a message.*')
    assert settled(db, 'comment.delivered') == []
    assert settled(db, 'comment.undelivered') == [(1, {'comment': stuck, 'error': error})]
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
        s.get_one(Idea, second).base_candidate_id = 1
        s.commit()
    scheduler = make_scheduler(db, runtime, tmp_path)
    assert scheduler.tick() == [1]
    # the run closes, then its commit becomes a candidate: wait for both, the evaluation is gated
    wait_until(lambda: candidate_rows(db, 1))
    with db.session() as s:
        run = s.get_one(Run, 1)
        assert (run.status, run.finished_at is not None) == (RunStatus.SUCCEEDED, True), 'closed: its implementer ended'
        assert s.get_one(Idea, first).status is IdeaStatus.SCHEDULED, 'its commit is not judged yet'
        assert s.scalars(select(Candidate).where(Candidate.run_id == 1)).one().verdict is Verdict.PENDING
    assert scheduler.tick() == [2], 'the one slot is free while run#1 is scored'
    go.touch()
    scheduler.wait(20)
    assert statuses(db) == [IdeaStatus.DONE, IdeaStatus.DONE]
    with db.session() as s:
        assert s.scalars(select(Candidate).where(Candidate.run_id == 1)).one().verdict is Verdict.KEEP
        assert s.get_one(Idea, second).status is IdeaStatus.DONE


def candidate_rows(db: Database, run_id: int) -> int:
    with db.session() as s:
        return len(s.scalars(select(Candidate).where(Candidate.run_id == run_id)).all())


def stop_campaign(db: Database, campaign: Campaign, reason: str) -> None:
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        p.status, p.stopped_reason = CampaignStatus.STOPPED, reason
        s.commit()


def test_implementers_of_a_stopped_campaign_are_told_to_wrap_up_then_ended_after_the_grace(
    db: Database, campaign: Campaign, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When a campaign stops, its implementers are told to wrap up, and they are ended after the grace period."""
    runtime = PromptingRuntime()
    running_agent_session(db, campaign, 'in flight')
    with db.session() as s:
        s.add(
            AgentSession(
                kind=RoleName.RESEARCH,
                campaign_id=campaign.id,
                runtime='subprocess',
                handle={'agent': 'research-2', 'pid': 4243},
                state=AgentSessionState.RUNNING,
            )
        )
        s.commit()
    scheduler = make_scheduler(db, runtime, tmp_path)
    scheduler.tick()
    assert runtime.prompts == [], 'an active campaign is left alone'

    stop_campaign(db, campaign, 'max_runs: 2 runs')
    scheduler.tick()
    assert runtime.prompts == snapshot(
        [
            (
                'implementer-1',
                '[piloop] The campaign stopped (max_runs: 2 runs). Commit what you have now, with a message that says where it stands. Leave your finding and stop. This session ends in 10 minutes.',
            )
        ]
    ), 'a sweep is bounded by its timeout and the closing watchdog is one: only implementers are wound down'
    assert settled(db, 'agent_session.wrap_up') == [(1, {'reason': 'max_runs: 2 runs'})]
    scheduler.tick()
    assert (len(runtime.prompts), runtime.killed) == (1, []), 'told once; ended only when the grace is over'

    monkeypatch.setattr('piloop.daemon.scheduler.STOP_GRACE_SECONDS', 0)
    scheduler.tick()
    assert [h.data['agent'] for h in runtime.killed] == ['implementer-1']


def test_a_headless_implementer_of_a_stopped_campaign_cannot_be_told_and_is_just_ended(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    running_agent_session(db, campaign, 'headless')
    stop_campaign(db, campaign, 'plateau: 5 runs without a keep')
    monkeypatch.setattr('piloop.daemon.scheduler.STOP_GRACE_SECONDS', 0)
    scheduler = make_scheduler(db, runtime, tmp_path)
    scheduler.tick()
    assert settled(db, 'agent_session.wrap_up') == [
        (1, {'reason': 'plateau: 5 runs without a keep', 'error': IsStr(regex='subprocess agent pid 4242 cannot.*')})
    ]
    scheduler.tick()
    assert [h.data['agent'] for h in runtime.killed] == ['implementer-1']


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
    scheduler.wait(20)
    scheduler.stop(timeout=5)
    with db.session() as s:
        run = s.get_one(Run, 1)
        assert (run.status, run.error) == (RunStatus.FAILED, CAMPAIGN_STOPPED_ERROR)
        [ended] = s.scalars(select(Event).where(Event.kind == 'agent_session.stop_ended')).all()
        assert (ended.target_id, ended.payload) == (
            1,
            {'reason': 'plateau: 5 runs without a keep', 'error': CAMPAIGN_STOPPED_ERROR},
        )
        verdicts = [c.verdict for c in s.scalars(select(Candidate).where(Candidate.run_id == 1).order_by(Candidate.id))]
        assert verdicts == [Verdict.KEEP, Verdict.RECOVERED], 'its commit is scored, its draft recovered'
        assert s.get_one(Idea, idea).status is IdeaStatus.READY, 'requeued from the recovered draft'
        assert runs(s, s.get_one(Campaign, campaign.id)) == 0, 'cut by the stop: no run limit counts it'


def test_a_stop_with_kill_ends_the_implementers_at_once_and_a_graceful_one_can_be_made_one(
    db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    """``piloop campaign stop --kill``: no wrap-up prompt and no grace. A sweep keeps its timeout."""
    runtime = PromptingRuntime()
    running_agent_session(db, campaign, 'in flight')
    scheduler = make_scheduler(db, runtime, tmp_path)
    with db.session() as s:
        request_stop(s, s.get_one(Campaign, campaign.id), 'human', reason='machine needed', kill=False)
        s.commit()
    scheduler.tick()  # stops the campaign; its implementers wind down from the next tick, as for a [stop] condition
    scheduler.tick()
    assert (len(runtime.prompts), runtime.killed) == (1, []), 'graceful: told, and the grace runs'
    with db.session() as s:
        request_stop(s, s.get_one(Campaign, campaign.id), 'human', reason='now', kill=True)
        s.commit()
    scheduler.tick()
    assert [h.data['agent'] for h in runtime.killed] == ['implementer-1'], 'a stopped campaign can still be killed'
    scheduler.tick()
    assert settled(db, 'agent_session.stop_ended') == [
        (1, {'reason': 'stopped by human: machine needed', 'error': CAMPAIGN_STOPPED_ERROR})
    ], 'one event, however many ticks the kill takes to land'
    scheduler.stop(timeout=5)


def test_a_cancelled_run_is_told_to_wrap_up_and_ended_after_the_grace(
    db: Database, campaign: Campaign, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = PromptingRuntime()
    running_agent_session(db, campaign, 'redundant')
    running_agent_session(db, campaign, 'useful')
    with db.session() as s:
        sweep = AgentSession(
            kind=RoleName.RESEARCH,
            campaign_id=campaign.id,
            runtime='subprocess',
            handle={'agent': 'research-3', 'pid': 4243},
            state=AgentSessionState.RUNNING,
        )
        s.add(sweep)
        s.flush()
        request_cancel(s, s.get_one(Run, 1), 'human', wrap_up=True)
        request_cancel(s, sweep, 'orchestrator', wrap_up=True)
        s.commit()
    scheduler = make_scheduler(db, runtime, tmp_path)
    scheduler.tick()
    assert runtime.prompts == snapshot(
        [
            (
                'implementer-1',
                '[piloop] Your run was cancelled (by human). Commit what you have now, with a message that says where it stands. Leave your finding and stop. This session ends in 10 minutes.',
            ),
            (
                'research-3',
                '[piloop] This sweep was cancelled (by orchestrator). Commit what you have now, with a message that says where it stands. Leave your finding and stop. This session ends in 10 minutes.',
            ),
        ]
    ), 'only the cancelled run and the cancelled sweep are told'
    scheduler.tick()
    assert runtime.killed == []
    monkeypatch.setattr('piloop.daemon.scheduler.STOP_GRACE_SECONDS', 0)
    scheduler.tick()
    assert [h.data['agent'] for h in runtime.killed] == ['implementer-1', 'research-3']
    assert settled(db, 'agent_session.stop_ended') == [
        (1, {'reason': 'cancelled by human', 'error': CANCELLED_ERROR}),
        (3, {'reason': 'cancelled by orchestrator', 'error': CANCELLED_ERROR}),
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
        request_cancel(s, s.get_one(AgentSession, 1), 'human', wrap_up=False)
        s.commit()
    assert scheduler.tick() == []
    scheduler.wait(20)
    scheduler.stop(timeout=5)
    with db.session() as s:
        run = s.get_one(Run, 1)
        assert (run.status, run.error) == (RunStatus.FAILED, CANCELLED_ERROR)
        verdicts = [c.verdict for c in s.scalars(select(Candidate).where(Candidate.run_id == 1))]
        assert verdicts == [Verdict.KEEP], 'what it committed is scored'
        assert s.get_one(Idea, idea).status is IdeaStatus.PARKED
        [note] = s.scalars(select(Comment).where(Comment.target_kind == TargetKind.IDEA)).all()
        assert note.body == 'parked: run#1 was cancelled by human; set the idea ready to run it again'
        assert runs(s, s.get_one(Campaign, campaign.id)) == 0, 'not an attempt: no run limit counts it'


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
        request_cancel(s, s.get_one(Run, 1), 'human', wrap_up=False)
        s.commit()
    assert scheduler.tick() == []
    scheduler.wait(20)
    scheduler.stop(timeout=5)
    with db.session() as s:
        assert s.scalars(select(Event).where(Event.kind == STARTUP_ANOMALY)).all() == []
        assert s.get_one(AgentSession, 1).state is AgentSessionState.KILLED
        run = s.get_one(Run, 1)
        assert (run.status, run.error) == (RunStatus.FAILED, CANCELLED_ERROR)
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
        queued = AgentSession(
            kind=RoleName.IMPLEMENTER, campaign_id=campaign.id, run_id=run.id, harness='script', model='m'
        )
        sweep = AgentSession(
            kind=RoleName.RESEARCH,
            campaign_id=campaign.id,
            runtime='subprocess',
            handle={'agent': 'research-2', 'pid': 4243},
            state=AgentSessionState.RUNNING,
        )
        requested = AgentSession(kind=RoleName.RESEARCH, campaign_id=campaign.id, reason='low_water')
        s.add_all([queued, sweep, requested])
        s.flush()
        request_cancel(s, queued, 'human', wrap_up=True)
        request_cancel(s, sweep, 'human', wrap_up=False)
        request_cancel(s, requested, 'human', wrap_up=False)
        s.commit()
    scheduler = make_scheduler(db, runtime, tmp_path)
    assert scheduler.tick() == [], 'the queued run is cancelled before it launches'
    assert [h.data['agent'] for h in runtime.killed] == ['research-2']
    with db.session() as s:
        w = s.get_one(AgentSession, 1)
        assert (w.state, w.error, s.get_one(Run, 1).error) == (
            AgentSessionState.KILLED,
            CANCELLED_ERROR,
            CANCELLED_ERROR,
        )
        assert (s.get_one(AgentSession, 3).state, s.get_one(AgentSession, 3).error) == (
            AgentSessionState.KILLED,
            CANCELLED_ERROR,
        )
        assert s.get_one(Idea, idea).status is IdeaStatus.PARKED
    scheduler.stop(timeout=5)


class BusyRuntime(ScriptRuntime):
    """A subprocess runtime whose implementer has a job in flight, or whose jobs it cannot report (``error``)."""

    def __init__(self, error: str | None = None) -> None:
        super().__init__()
        self.error = error

    def jobs(self, handle: Handle) -> list[str]:
        if self.error is not None:
            raise RuntimeError(self.error)
        return ['4242 python train.py']


def quiet_for_over_an_hour(db: Database, campaign: Campaign) -> None:
    running_agent_session(db, campaign, 'train')
    with db.session() as s:
        s.get_one(Run, 1).started_at = utcnow() - timedelta(seconds=SILENT_SECONDS + 60)
        s.commit()


def test_an_implementer_silent_while_its_jobs_run_requests_an_incident_sweep(
    db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    quiet_for_over_an_hour(db, campaign)
    scheduler = make_scheduler(db, BusyRuntime(), tmp_path)
    scheduler.tick()
    with db.session() as s:
        [watchdog] = s.scalars(select(AgentSession).where(AgentSession.kind == RoleName.WATCHDOG)).all()
        assert watchdog.reason == 'anomaly: agent_session#1 silent while its jobs run'
    scheduler.stop(timeout=5)


def test_jobs_the_runtime_cannot_report_are_traced_and_page_nobody(
    db: Database, campaign: Campaign, tmp_path: Path, tracer: Tracer, spans: InMemorySpanExporter
) -> None:
    quiet_for_over_an_hour(db, campaign)
    scheduler = make_scheduler(db, BusyRuntime('server_not_running: herdr is down'), tmp_path, tracer)
    assert scheduler.tick() == []
    failed = span_named(spans, 'agent_session.jobs_failed').attributes
    assert dict(failed or {}) == {'agent_session': 1, 'error': 'server_not_running: herdr is down'}
    with db.session() as s:
        assert s.scalars(select(AgentSession).where(AgentSession.kind == RoleName.WATCHDOG)).all() == []


def died_at_startup(db: Database, campaign: Campaign, kind: RoleName, minutes_ago: float = 0) -> int:
    """Add a killed agent row and the launcher's ``anomaly.startup`` event, ``minutes_ago`` minutes ago."""
    with db.session() as s:
        agent_session = AgentSession(
            kind=kind, campaign_id=campaign.id, harness='script', state=AgentSessionState.KILLED
        )
        s.add(agent_session)
        s.flush()
        payload: dict[str, object] = {
            'kind': kind,
            'harness': 'script',
            'run': None,
            'seconds': 35.0,
            'error': f'HerdrError: prompt_lost: {kind}-{agent_session.id} did not start working\nIts screen ended with:\n> brief',
        }
        s.add(
            Event(
                kind=STARTUP_ANOMALY,
                actor='daemon',
                target_kind=TargetKind.AGENT_SESSION,
                target_id=agent_session.id,
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
    died_at_startup(db, campaign, RoleName.IMPLEMENTER, minutes_ago=11)
    for kind in (RoleName.WATCHDOG, RoleName.RESEARCH):
        died_at_startup(db, campaign, kind)
    wanted = request_watchdog(db, campaign)
    scheduler = make_scheduler(db, runtime, tmp_path)
    assert wanted in scheduler.tick(), 'two recent deaths: launches go on'
    scheduler.wait(10)

    died = [died_at_startup(db, campaign, RoleName.WATCHDOG) for _ in range(2)]
    request_watchdog(db, campaign)
    assert scheduler.tick() == []
    [escalation] = pause_escalations(db)
    assert (escalation.target_kind, escalation.author, escalation.priority) == (TargetKind.CAMPAIGN, 'daemon', 1)
    assert escalation.body == IsStr(
        regex=r'Launches paused: 4 agents died at startup in the last 10 minutes\. Load average [\d.]+ / [\d.]+ / [\d.]+'
        rf' on \d+ CPUs when launches paused\..*\n- agent_session#\d+ \(research on script\): .*\n- agent_session#{died[0]} \(watchdog on script\):'
        rf' HerdrError: prompt_lost: watchdog-{died[0]} did not start working\nIts screen ended with:\n> brief\n'
        rf'- agent_session#{died[1]} .*',
        regex_flags=re.DOTALL,
    )
    assert scheduler.tick() == [] and len(pause_escalations(db)) == 1, 'one escalation while the pause holds'

    with db.session() as s:
        s.get_one(Comment, escalation.id).acked_at = utcnow()
        s.commit()
    assert scheduler.tick() == [] and len(pause_escalations(db)) == 1, 'acknowledged means seen: not posted again'

    latest = died_at_startup(db, campaign, RoleName.RESEARCH)
    assert scheduler.tick() == []
    [extended] = pause_escalations(db)
    assert extended.id == escalation.id and extended.acked_at is not None, 'the same pause: updated in place'
    assert (
        extended.body.startswith('Launches paused: 5 agents') and f'- agent_session#{latest} (research' in extended.body
    )

    with db.session() as s:
        for event in s.scalars(select(Event).where(Event.kind == STARTUP_ANOMALY)):
            event.at -= STARTUP_WINDOW
        s.commit()
    assert scheduler.tick() != [], 'the deaths aged out of the window: launches resume'
    scheduler.wait(10)

    for _ in range(STARTUP_BREAKER):
        died_at_startup(db, campaign, RoleName.WATCHDOG)
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
        s.get_one(Idea, level).base_candidate_id = 2
        s.get_one(Idea, explore).base_candidate_id = 1
        for attempt in (1, 2):
            s.add(
                Run(
                    idea_id=done,
                    attempt=attempt,
                    base_candidate_id=1,
                    harness='h',
                    model='m',
                    status=RunStatus.SUCCEEDED,
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
        run.status = RunStatus.SUCCEEDED
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


def queue(db: Database, scheduler: Scheduler, campaign: Campaign) -> list[tuple[int, dict[str, object]]]:
    """Run one ``_queue`` pass.

    Returns:
        The idea and the ``run.queued`` payload of every queued run, in order.
    """
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        scheduler._queue(s, p, CampaignConfig.model_validate(p.config))
        s.commit()
    with db.session() as s:
        rows = s.scalars(select(Event).where(Event.kind == 'run.queued').order_by(Event.id)).all()
        return [(int(str(e.payload['idea'])), e.payload) for e in rows]


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
    assert [(p['track'], p.get('priority_pick')) for _, p in queued] == [
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
    assert all('priority_pick' not in p for _, p in queued)


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
        backlog = scheduler._backlog(s, p, cfg, RoleName.RESEARCH, 0)
        assert (backlog.runnable, backlog.target, backlog.short) == (6, 4, 1), 'cost is one short; accuracy has 3 spare'
        assert str(backlog) == '6 of 4 runnable ideas (accuracy 6 of 3, cost 0 of 1)'
        fire = LowWaterTrigger(buffer=0).fire(scheduler._tick(s, p, cfg, RoleName.RESEARCH, cfg.roles.research))
        assert fire is not None and fire.reason == str(backlog)
    add_ideas(db, campaign, 'c0', track='cost')
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        assert scheduler._backlog(s, p, cfg, RoleName.RESEARCH, 0).short == -3, (
            'no track short: the surplus, as with one track'
        )


class WaitingRuntime(PromptingRuntime):
    """An implementer whose agent is idle or working, with one job left that never ends: a waiter that finds itself."""

    def __init__(self, *, refuse: bool = False) -> None:
        super().__init__()
        self.state = AgentStatus.WORKING
        self.refuse = refuse

    def jobs(self, handle: Handle) -> list[str]:
        return ['4243 zsh -c until ! pgrep -f "train.py --plan"; do sleep 5; done']

    def status(self, handle: Handle) -> AgentStatus:
        return self.state

    def prompt(self, handle: Handle, text: str) -> None:
        if self.refuse:
            raise NotSteerable('agent implementer-1 is blocked on an approval or question')
        super().prompt(handle, text)


def test_an_idle_implementer_that_left_its_finding_is_told_once_which_of_its_jobs_keep_its_run_open(
    db: Database, campaign: Campaign, tmp_path: Path, tracer: Tracer, spans: InMemorySpanExporter
) -> None:
    """An idle agent with a job in flight is not finished, so its run stays open, as it should for a training job.

    An implementer that left its finding thinks it is done, but cannot see what keeps its run open: a forgotten job, or a
    waiter whose own command line matches its pattern. The daemon tells it once, when it is idle.
    """
    runtime = WaitingRuntime()
    idea = running_agent_session(db, campaign, 'build')
    scheduler = make_scheduler(db, runtime, tmp_path, tracer)
    scheduler.tick()
    assert runtime.prompts == [], 'no finding yet: the job is part of the work'
    comment(db, 'finding', 'idea', idea, 'done; the handoff is written', author='implementer#1')
    scheduler.tick()
    assert runtime.prompts == [], 'its turn is not over: it may still stop the job itself'
    runtime.state = AgentStatus.IDLE
    scheduler.tick()
    scheduler.tick()
    assert runtime.prompts == [
        (
            'implementer-1',
            '[piloop] You left your finding, but your run stays open while these jobs that you started still run:'
            ' 4243 zsh -c until ! pgrep -f "train.py --plan"; do sleep 5; done. Kill the ones that you no longer'
            ' need: your run ends when you stop with nothing left running.',
        )
    ], 'once'
    jobs = ['4243 zsh -c until ! pgrep -f "train.py --plan"; do sleep 5; done']
    assert settled(db, 'agent_session.jobs_left') == [(1, {'jobs': jobs})]
    assert dict(span_named(spans, 'agent_session.jobs_left').attributes or {}) == {'agent_session': 1, 'jobs': 1}


def test_an_implementer_that_cannot_take_the_list_of_its_jobs_is_not_asked_again(
    db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    runtime = WaitingRuntime(refuse=True)
    runtime.state = AgentStatus.IDLE
    idea = running_agent_session(db, campaign, 'build')
    comment(db, 'finding', 'run', 1, 'done', author=f'run#{idea}')
    scheduler = make_scheduler(db, runtime, tmp_path)
    scheduler.tick()
    scheduler.tick()
    jobs = ['4243 zsh -c until ! pgrep -f "train.py --plan"; do sleep 5; done']
    assert settled(db, 'agent_session.jobs_left') == [
        (1, {'jobs': jobs, 'error': 'agent implementer-1 is blocked on an approval or question'})
    ]


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
    measured = mocker.patch.object(scheduler._executor, 'remeasure')
    scheduler.tick()
    scheduler.wait(5)
    measured.assert_not_called()
    with db.session() as s:
        request_remeasure(s, s.get_one(Candidate, 1), 'human')
        s.commit()
    scheduler.tick()
    scheduler.wait(5)
    measured.assert_called_once_with(1)


def test_the_daemon_measures_a_candidate_again_when_someone_asks(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """A requested re-measure goes through the queue of re-measures, after the head and the bars that runs need."""
    scheduler = make_scheduler(db, runtime, tmp_path)
    measured = mocker.patch.object(scheduler._executor, 'remeasure')
    with db.session() as s:
        request_remeasure(s, s.get_one(Candidate, 1), 'orchestrator')
        s.commit()
    scheduler.tick()
    scheduler.wait(5)
    measured.assert_called_once_with(1)
