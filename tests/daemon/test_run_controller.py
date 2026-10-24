import fcntl
import json
import math
import os
import random
import re
import sqlite3
import subprocess
import threading
from contextlib import closing
from pathlib import Path
from typing import Any
from unittest.mock import DEFAULT

import pytest
from dirty_equals import IsPositive, IsStr
from git import GitCommandError, Repo
from inline_snapshot import snapshot
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode
from pytest_mock import MockerFixture
from sqlalchemy import func, insert, select

from piloop.actor import System
from piloop.agents.results import NEVER_WORKED, Ended, Lost, NotStarted, StartupAnomaly
from piloop.agents.roles import SQL_HINT
from piloop.campaign import stop
from piloop.campaign.campaigns import sync
from piloop.campaign.ideas import supersede
from piloop.campaign.ops import Report, add_measurement, set_verdict
from piloop.cli.plugins import selections
from piloop.config import CampaignConfig, ConfigError, config_of, load_campaign_config
from piloop.daemon.run_controller import (
    INTERRUPTED,
    NO_CHANGES,
    RECOVERED_REASON,
    UNCOMMITTED_SUMMARY,
    RecoveryRequeued,
    RunController,
    RunFacts,
    RunRelaunched,
    run_outcome,
)
from piloop.db.events import has, in_order
from piloop.db.models import (
    AgentSession,
    AgentSessionState,
    Campaign,
    CampaignStatus,
    Candidate,
    Comment,
    Evaluator,
    Event,
    Idea,
    IdeaStatus,
    Lesson,
    LessonStatus,
    Measurement,
    Run,
    RunEnd,
    RunStatus,
    Verdict,
    utcnow,
)
from piloop.db.session import Database
from piloop.harnesses import SESSION_ENV
from piloop.ids import (
    IMPLEMENTER,
    CampaignId,
    CandidateId,
    CommentId,
    EvaluatorId,
    RoleName,
    RunId,
    TargetKind,
    TargetRef,
)
from piloop.otel import Span, Tracer
from piloop.plugins import host
from piloop.plugins.executor.contract import Source
from piloop.plugins.executor.kit import jobs
from piloop.plugins.executor.kit.jobs import CHANGED_INPUTS, INPUTS_CHANGED, INPUTS_REBUILT
from piloop.plugins.executor.workers import Workers
from piloop.plugins.executor.workers.worker import _Control
from piloop.plugins.runtime.contract import AgentExit, Handle
from piloop.plugins.vcs.contract import Revision
from piloop.plugins.vcs.git import GitRepository, commands as git
from piloop.plugins.vcs.git.commands import pin as git_pin
from piloop.plugins.verdict_rule.contract import VERDICT_RULES
from piloop.scoring.ledger import (
    DAEMON,
    SKIPPED,
    CandidateJudged,
    MeasurementRecorded,
    Rebased,
    branches,
    current_evaluator,
    due,
    head,
    import_reference,
    pairs as ledger_pairs,
    request_remeasure,
    requested,
    resolve,
    save_units,
    supersede_composed,
    units,
)
from piloop.settings import Settings
from piloop.store.tree import FICLONE
from piloop.usage.jsonl import CWD_ENCODERS
from tests.conftest import (
    HARNESS_APPEND,
    HARNESS_COMMITS_AND_LEAVES_DIRT,
    HARNESS_FAILS,
    HARNESS_FORGETS_COMMIT,
    HARNESS_NOOP,
    HARNESS_PROMPT_TO_FILE,
    HARNESS_SLEEPS,
    HARNESS_TRY_AND_REVERT,
    HARNESS_TWO_COMMITS,
    HARNESS_WORSE,
    Gate,
    ScriptRuntime,
    baseline,
    events,
    example,
    factory,
    serving,
    span_named,
    wait_until,
    workers_of,
    write_script,
    write_toml,
)
from tests.usage.test_usage import PI_SPEC, SESSIONS


def make_run(
    db: Database,
    campaign: Campaign,
    *,
    title: str = 'more lines',
    plan: str | None = None,
    max_runs: int = 2,
    provenance: str | None = None,
    track: str = 'score',
) -> RunId:
    with db.session() as s:
        idea = Idea(
            track=track,
            campaign_id=campaign.id,
            title=title,
            hypothesis='lines are good',
            plan=plan,
            provenance=provenance,
            created_by='test',
            max_runs=max_runs,
            status=IdeaStatus.SCHEDULED,
        )
        s.add(idea)
        s.flush()
        run = Run(
            idea_id=idea.id,
            attempt=1,
            base_candidate_id=baseline(db, campaign.id).id,
            harness='script',
            model='test-model',
        )
        s.add(run)
        s.flush()
        s.add(
            AgentSession(
                kind=IMPLEMENTER,
                campaign_id=campaign.id,
                run_id=run.id,
                harness='script',
                model='test-model',
                reason=f'idea#{idea.id} is ready',
            )
        )
        s.commit()
        return run.id


def execute(ex: RunController, db: Database, run_id: RunId) -> None:
    """Run ``run_id`` through its requested agent session row, the way the scheduler does: it takes the row, then executes."""
    with db.session() as s:
        requested = select(AgentSession).where(
            AgentSession.run_id == run_id, AgentSession.state == AgentSessionState.REQUESTED
        )
        agent_session = s.scalars(requested).one()
        agent_session.state = AgentSessionState.STARTING
        s.commit()
        agent_session_id = agent_session.id
    ex.execute(run_id, agent_session_id)


def controller(db: Database, runtime: ScriptRuntime, tmp_path: Path, tracer: Tracer | None = None) -> RunController:
    return serving(RunController(db, tracer or Tracer(None), tmp_path, factory(runtime), executor=Workers()))


def reload_campaign(db: Database, campaign: Campaign) -> None:

    with db.session() as s:
        s.get_one(Campaign, campaign.id).config = load_campaign_config(Path(campaign.path)).model_dump(mode='json')
        s.commit()


class Scored:
    """A candidate with its (single) measurement flattened on, for terse assertions."""

    def __init__(self, c: Candidate) -> None:
        self.id, self.parent_id, self.commit_sha, self.summary = c.id, c.parent_id, c.commit_sha, c.summary
        self.verdict, self.verdict_by = c.verdict, c.verdict_by
        [m] = c.measurements
        self.metrics, self.error = m.metrics, m.error


def candidates(db: Database, run_id: RunId) -> list[Scored]:
    """The candidates a run produced (its commits' trees), with their one measurement each."""
    with db.session() as s:
        rows = s.scalars(select(Candidate).where(Candidate.run_id == run_id).order_by(Candidate.id)).all()
        return [Scored(c) for c in rows]


def candidates_measured(db: Database, run_id: RunId) -> list[Scored]:
    """``candidates``, without the ones nothing measured (a skip commit, recovered work)."""
    with db.session() as s:
        rows = s.scalars(select(Candidate).where(Candidate.run_id == run_id).order_by(Candidate.id)).all()
        return [Scored(c) for c in rows if c.measurements]


def run_row(db: Database, run_id: RunId) -> Run:
    with db.session() as s:
        return s.get_one(Run, run_id)


def test_successful_run_scores_the_commit(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
) -> None:
    run_id = make_run(db, campaign, plan='append one line', provenance='finding comment#3: short files lose')
    execute(controller(db, runtime, tmp_path, tracer), db, run_id)
    agent, measure, candidate, run_span = [s for s in spans.get_finished_spans() if s.name != 'piloop.job']
    assert dict(agent.attributes or {}) == snapshot(
        {
            'agent_session': 1,
            'kind': 'implementer',
            'harness': 'script',
            'model': 'test-model',
            'reason': 'idea#1 is ready',
            'state': 'exited',
        }
    )
    assert dict(run_span.attributes or {}) == snapshot(
        {
            'run': 1,
            'idea': 1,
            'agent_session': 1,
            'harness': 'script',
            'model': 'test-model',
            'status': 'succeeded',
            'end': 'finished',
        }
    )
    assert candidate.parent is not None and candidate.parent.span_id == run_span.context.span_id
    assert measure.parent is not None and measure.parent.span_id == candidate.context.span_id
    assert dict(measure.attributes or {}) == snapshot(
        {
            'run': 1,
            'commit': IsStr(),
            'candidate': 2,
            'evaluation': 1,
            'joined': False,
            'measurement': 2,
            'metric.score': 2.0,
            'metric.size': IsPositive(),
        }
    )
    assert dict(candidate.attributes or {}) == snapshot(
        {'run': 1, 'commit': IsStr(), 'candidate': 2, 'verdict': 'keep', 'summary': 'add a line', 'score': 2.0}
    )

    run = run_row(db, run_id)
    assert (run.status, run.error) == (RunStatus.SUCCEEDED, None)
    assert run.brief is not None and run.brief.startswith('# Implementer') and '### Plan\nappend one line' in run.brief
    assert 'lines are good\n\nProposed by test, prompted by: finding comment#3: short files lose\n' in run.brief
    [e] = candidates(db, run_id)
    assert (e.verdict, e.summary, e.metrics) == (Verdict.KEEP, 'add a line', {'score': 2.0, 'size': IsPositive()})
    assert e.parent_id == baseline(db, campaign.id).id
    with db.session() as s:
        assert s.get_one(Idea, run.idea_id).status is IdeaStatus.DONE
        [agent_session] = s.scalars(select(AgentSession)).all()
        assert (agent_session.state, agent_session.run_id) == (AgentSessionState.EXITED, run_id)
        kinds = [ev.kind for ev in s.scalars(select(Event)).all()]
    assert kinds == snapshot(
        [
            'run.started',
            'agent_session.started',
            'lease.queued',
            'agent_session.finished',
            'agent_session.job_ended',
            'run.received',
            'run.finished',
            'lease.queued',
            'measurement.recorded',
            'candidate.judged',
            'candidate.verdict',
            'idea.status',
        ]
    ), 'the run closes when its implementer ends; the idea settles once the commit is judged'

    repo = Repo(campaign.path)
    assert [r.path for r in repo.refs if r.path.startswith('refs/piloop/')] == [f'refs/piloop/demo/r{run_id}/0']
    assert repo.head.commit.summary == 'baseline', 'the campaign checkout never moves'
    log_dir = tmp_path / 'runs' / str(run_id)
    assert (
        (log_dir / 'brief.md').is_file()
        and (log_dir / 'transcript.txt').is_file()
        and not (log_dir / 'worktree').exists()
    )
    assert runtime.killed, 'the runtime is told to release the implementer'


HARNESS_WRITES_THE_WORKSPACE = (
    '#!/bin/sh\nmkdir -p workspace/idea1\necho out > workspace/idea1/out.txt\n'
    'cat workspace/NOTES.md > workspace/idea1/seen.txt\nrm workspace/NOTES.md\necho mine > workspace/NOTES.md\n'
    'echo line >> solve.txt\ngit commit -qam "add a line"\n'
)


def test_what_an_implementer_writes_in_its_workspace_is_merged_into_the_campaigns_after_its_run(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """The implementer starts from a snapshot, and its writes land when its report is applied, not while it works."""
    live = tmp_path / 'projects' / str(campaign.id) / 'workspace'
    live.mkdir(parents=True)
    (live / 'NOTES.md').write_text('the notes\n')
    write_toml(Path(campaign.path), HARNESS_WRITES_THE_WORKSPACE)
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign)
    execute(controller(db, runtime, tmp_path), db, run_id)
    assert (live / 'idea1' / 'out.txt').read_text() == 'out\n'
    assert (live / 'idea1' / 'seen.txt').read_text() == 'the notes\n', 'it started from the workspace'
    assert (live / 'NOTES.md').read_text() == 'mine\n'
    assert run_row(db, run_id).status is RunStatus.SUCCEEDED
    [c] = candidates(db, run_id)
    assert c.summary == 'add a line', 'what it wrote in the workspace is not in its commit'


def test_every_commit_is_scored_and_lineage_follows_keeps(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    write_toml(Path(campaign.path), HARNESS_TWO_COMMITS)
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign)
    execute(controller(db, runtime, tmp_path), db, run_id)
    first, second = candidates(db, run_id)
    assert (first.summary, first.verdict, first.metrics['score']) == ('first', Verdict.KEEP, 2.0)
    assert (second.summary, second.verdict, second.metrics['score'], second.parent_id) == (
        'second',
        Verdict.KEEP,
        3.0,
        first.id,
    )


def test_every_commit_of_a_run_is_a_candidate_before_the_first_is_measured(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A commit became a candidate only when the evaluations of the commits before it were
    done, 33 minutes after its run, so it looked lost. Each commit is a pending candidate when its run closes."""
    write_toml(Path(campaign.path), HARNESS_TWO_COMMITS)
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign)
    ex = controller(db, runtime, tmp_path)
    measure = ex._measure
    seen: list[list[tuple[str | None, Verdict, int | None]]] = []

    def watched(
        run: RunId | None,
        campaign_id: CampaignId,
        cfg: CampaignConfig,
        sha: Revision,
        candidate_id: CandidateId,
        evaluator_id: EvaluatorId,
        eval_dir: Path,
        *,
        bar: CandidateId | None,
        repeats: int,
    ) -> bool:
        with db.session() as s:
            rows = s.scalars(select(Candidate).where(Candidate.run_id == run_id).order_by(Candidate.id)).all()
            seen.append([(c.summary, c.verdict, c.parent_id) for c in rows])
        return measure(run, campaign_id, cfg, sha, candidate_id, evaluator_id, eval_dir, bar=bar, repeats=repeats)

    monkeypatch.setattr(ex, '_measure', watched)
    execute(ex, db, run_id)
    first, second = candidates(db, run_id)
    assert seen[0] == [('first', Verdict.PENDING, 1), ('second', Verdict.PENDING, first.id)], 'all at once, in order'
    assert (first.verdict, second.verdict, second.metrics['score']) == (Verdict.KEEP, Verdict.KEEP, 3.0)


@pytest.mark.parametrize('moved_to', [None, IdeaStatus.PARKED, IdeaStatus.REJECTED])
def test_a_run_counts_toward_max_runs_from_its_close_to_its_verdict(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    mocker: MockerFixture,
    moved_to: IdeaStatus | None,
) -> None:
    """A run keeps its place under ``max_runs`` from its close until its commits are scored, before its candidates
    exist, and also when a human parks or rejects its idea meanwhile (``piloop watch`` allows it)."""
    ex = controller(db, runtime, tmp_path)
    close = ex._close
    counted: list[tuple[int, bool]] = []

    def close_and_count(span: Span, run_id: RunId, end: RunEnd, error: str | None) -> None:
        close(span, run_id, end, error)
        with db.session() as s:
            if moved_to is not None:
                s.get_one(Run, run_id).idea.status = moved_to
                s.commit()
            campaign_row = s.get_one(Campaign, campaign.id)
            counted.append((stop.runs(s, campaign_row), bool(s.scalar(select(Run.id).where(stop.unsettled())))))

    mocker.patch.object(ex, '_close', side_effect=close_and_count)
    run_id = make_run(db, campaign)
    execute(ex, db, run_id)
    assert counted == [(1, True)], 'counted and unsettled while it is scored'
    with db.session() as s:
        assert s.scalar(select(Run.id).where(stop.unsettled())) is None, f'run#{run_id} settled'


def test_worse_commit_is_discarded(db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path) -> None:
    write_toml(Path(campaign.path), HARNESS_WORSE)
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign)
    execute(controller(db, runtime, tmp_path), db, run_id)
    [e] = candidates(db, run_id)
    assert (e.verdict, e.verdict_by) == (Verdict.DISCARD, DAEMON)
    assert run_row(db, run_id).status is RunStatus.SUCCEEDED


def test_a_verdict_rule_from_a_plugin_replaces_the_built_in_one_through_piloop_toml(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The example plugin ``examples/plugins/min-gain``, installed, is selected by ``[plugins.verdict_rule]``.

    The daemon judges the run's commit with it: ``bootstrap`` would keep a gain of 1, ``min_gain = 2`` does not.
    """
    example(tmp_path / 'site', 'min-gain')
    monkeypatch.syspath_prepend(tmp_path / 'site')
    host.close()
    try:
        for table, error in [
            ({'provider': 'strict'}, "plugins.verdict_rule: provider = 'strict': unknown verdict rule 'strict'"),
            ({'provider': 'min_gain', 'min_gain': 'x'}, "min_gain = 'x': Input should be a valid number"),
            (
                {'provider': 'bootstrap', 'min_gain': 2},
                'plugins.verdict_rule.min_gain = 2: Extra inputs are not permitted',
            ),
        ]:
            with pytest.raises(ConfigError, match=re.escape(error)):
                load_campaign_config(Path(campaign.path), plugins={'verdict_rule': table})
        with db.session() as s:
            cfg = load_campaign_config(
                Path(campaign.path), plugins={'verdict_rule': {'provider': 'min_gain', 'min_gain': 2}}
            )
            s.get_one(Campaign, campaign.id).config = cfg.model_dump(mode='json')
            s.commit()
        run_id = make_run(db, campaign)
        execute(controller(db, runtime, tmp_path), db, run_id)
        [e] = candidates(db, run_id)
        with db.session() as s:
            reason = s.get_one(Candidate, e.id).verdict_reason
        assert (e.verdict, e.verdict_by, e.metrics['score']) == (Verdict.INTERESTING, DAEMON, 2.0)
        assert reason == snapshot(
            'against candidate#1: score 2 vs 1 (+1; aggregate: no per-unit results); the gain is below min_gain 2'
        )
        assert VERDICT_RULES.sources()['min_gain'].release == 'piloop-min-gain 0.1.0'
        stored = f'in the config of demo that the last `piloop init` stored from {campaign.path}/piloop.toml'
        selected = (
            f'demo: verdict rule min_gain, from piloop-min-gain 0.1.0 ([plugins.verdict_rule] provider, {stored})'
        )
        assert selected in selections(Settings(home=tmp_path))
    finally:
        host.close()


def test_work_a_stopped_implementer_left_uncommitted_is_recovered_and_a_new_run_finishes_it(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """comment#10936: a failed implementer's uncommitted tree was scored, and could become the head, never declared ready.

    It is now ``recovered``: committed, pinned, never measured, never a keep. The idea goes back to the queue with the
    recovered candidate as its base, attempts left or not, and the run that finishes it is judged as any other.
    """
    write_toml(Path(campaign.path), HARNESS_FORGETS_COMMIT)
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign, max_runs=1)
    execute(controller(db, runtime, tmp_path), db, run_id)
    assert (run_row(db, run_id).status, run_row(db, run_id).error) == (RunStatus.FAILED, INTERRUPTED)
    [(summary, verdict, reason, measurements, _)] = scored(db, run_id)
    assert (summary, verdict, reason, measurements) == (
        UNCOMMITTED_SUMMARY,
        Verdict.RECOVERED,
        RECOVERED_REASON.format(run=run_id),
        0,
    )
    with db.session() as s:
        recovered = s.scalars(select(Candidate).where(Candidate.run_id == run_id)).one()
        idea = s.get_one(Idea, run_row(db, run_id).idea_id)
        assert (idea.status, idea.base_candidate_id) == (IdeaStatus.READY, recovered.id), 'max_runs=1 is spent'
        [note] = s.scalars(select(Comment).where(Comment.target_kind == TargetKind.IDEA)).all()
        assert note.body.startswith(f'requeued from candidate#{recovered.id}: run#{run_id} ended failed')
        trunk = head(s, s.get_one(Campaign, campaign.id))
        assert trunk is not None and trunk.id == baseline(db, campaign.id).id, 'the head did not move'
        recovered_id, recovered_sha, idea_id = recovered.id, recovered.commit_sha, idea.id
    repo = Repo(campaign.path)
    assert 'data' not in repo.git.show('--stat', '--format=', recovered_sha), 'linked paths are never committed'
    assert repo.git.log('-1', '--format=%(trailers:key=Piloop-Recovered,valueonly)', recovered_sha).strip() == (
        f'run#{run_id}'
    )

    write_toml(Path(campaign.path), HARNESS_APPEND)
    reload_campaign(db, campaign)
    with db.session() as s:
        s.get_one(Idea, idea_id).status = IdeaStatus.SCHEDULED
        run = Run(idea_id=idea_id, attempt=2, base_candidate_id=recovered_id, harness='script', model='test-model')
        s.add(run)
        s.flush()
        s.add(
            AgentSession(kind=IMPLEMENTER, campaign_id=campaign.id, run_id=run.id, harness='script', model='test-model')
        )
        s.commit()
        finishing = run.id
    ex = controller(db, runtime, tmp_path)
    execute(ex, db, finishing)
    with db.session() as s:
        assert '- candidate#' in (s.get_one(Run, finishing).brief or '') and 'It was never measured.' in (
            s.get_one(Run, finishing).brief or ''
        )
    [done] = candidates(db, finishing)
    assert (done.summary, done.verdict, done.metrics['score'], done.parent_id) == (
        'add a line',
        Verdict.KEEP,
        3.0,
        recovered_id,
    ), 'judged against the nearest keep below the recovered work, the baseline'
    with db.session() as s:
        assert s.get_one(Idea, idea_id).status is IdeaStatus.DONE


TWO_DISCARDS = (
    '#!/bin/sh\n: > solve.txt\ngit commit -qam "wipe"\necho x > other.txt\ngit add other.txt\ngit commit -qm "second"\n'
)
"""Two commits that score below the baseline: neither moves the head."""


def test_a_run_that_timed_out_after_its_commits_is_retried_from_its_last_commit(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """A retry from the idea's base redid the work that the timed-out run had committed and that was scored."""
    write_toml(Path(campaign.path), f'{TWO_DISCARDS}touch ../committed\nsleep 30\n')
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign, max_runs=1)
    wait = runtime.wait

    def time_out_after_the_commits(handle: Handle, timeout: float) -> AgentExit:
        wait_until((tmp_path / 'runs' / str(run_id) / 'committed').exists)
        return wait(handle, 0.5)

    mocker.patch.object(runtime, 'wait', side_effect=time_out_after_the_commits)
    execute(controller(db, runtime, tmp_path), db, run_id)
    assert run_row(db, run_id).end is RunEnd.TIMED_OUT
    [_, second] = candidates(db, run_id)
    with db.session() as s:
        idea = s.get_one(Idea, run_row(db, run_id).idea_id)
        assert (idea.status, idea.base_candidate_id) == (IdeaStatus.READY, second.id), 'max_runs=1 is spent'
        [note] = s.scalars(select(Comment).where(Comment.target_kind == TargetKind.IDEA)).all()
        assert note.body == (
            f'requeued from candidate#{second.id}: run#{run_id} ended timed_out after its commits; the next run starts'
            ' from its last one to finish the work'
        )
        assert [e.payload for e in s.scalars(select(Event).where(has(RecoveryRequeued)))] == [
            RecoveryRequeued(run=run_id, candidate=second.id)
        ]


def test_a_failed_run_whose_commits_the_head_moved_past_is_retried_on_the_head_with_its_commits_named(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A retry from those commits would be a branch run, whose keeps never move the head."""
    harness = (
        f'#!/bin/sh\nif [ -p ../go ]; then\n{TWO_DISCARDS.removeprefix("#!/bin/sh\n")}read line < ../go\nexit 1\nfi\n'
        'echo line >> solve.txt\ngit commit -qam "add a line"\n'
    )
    write_toml(Path(campaign.path), harness, max_implementers=2)
    reload_campaign(db, campaign)
    ex = controller(db, runtime, tmp_path)
    slow = make_run(db, campaign, title='slow')
    (tmp_path / 'runs' / str(slow)).mkdir(parents=True)
    go = tmp_path / 'runs' / str(slow) / 'go'
    os.mkfifo(go)
    running = threading.Thread(target=execute, args=(ex, db, slow))
    running.start()
    wait_until(lambda: (tmp_path / 'runs' / str(slow) / 'worktree' / 'other.txt').exists())
    execute(ex, db, make_run(db, campaign, title='quick'))
    go.write_text('\n')
    running.join(30)
    [wipe, second] = candidates(db, slow)
    with db.session() as s:
        moved = head(s, s.get_one(Campaign, campaign.id))
        assert moved is not None and moved.summary == 'add a line'
        idea = s.get_one(Idea, run_row(db, slow).idea_id)
        assert (idea.status, idea.base_candidate_id) == (IdeaStatus.READY, None), 'on the head'
        [note] = s.scalars(select(Comment).where(Comment.target == TargetRef(TargetKind.IDEA, idea.id))).all()
        base = baseline(db, campaign.id).commit_sha
        assert note.body == (
            f'requeued on the head: run#{slow} ended failed after its commits, and the head moved past them. To start'
            f' from them, run git cherry-pick {base}..{second.commit_sha}'
        )
    assert wipe.verdict is Verdict.DISCARD


def test_dirt_after_a_commit_is_recovered_on_its_own_and_the_commit_is_scored_as_made(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    write_toml(Path(campaign.path), HARNESS_COMMITS_AND_LEAVES_DIRT)
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign)
    execute(controller(db, runtime, tmp_path), db, run_id)
    assert [(c[0], c[1], c[3]) for c in scored(db, run_id)] == [
        ('the idea', Verdict.KEEP, 1),
        (UNCOMMITTED_SUMMARY, Verdict.RECOVERED, 0),
    ], 'never amended: the implementer declared its commit, not the dirt after it'
    [e] = [c for c in candidates_measured(db, run_id)]
    assert e.metrics['score'] == 2.0
    with db.session() as s:
        idea = s.get_one(Idea, run_row(db, run_id).idea_id)
        assert (idea.status, idea.base_candidate_id) == (IdeaStatus.DONE, None), 'a run that succeeded is not requeued'


def test_an_idea_whose_implementers_keep_dying_is_requeued_from_its_recovered_work_only_so_often(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """``[roles.implementer] recovery_requeues`` caps the requeues, counted in events: implementers that leave the same two trees
    in turn reuse two candidates, and counting candidates would requeue them forever."""
    toggles = '#!/bin/sh\nif grep -q A solve.txt; then echo B > solve.txt; else echo A > solve.txt; fi\n'
    write_toml(Path(campaign.path), toggles)
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign, max_runs=1)
    ex = controller(db, runtime, tmp_path)
    execute(ex, db, run_id)
    for attempt in range(2, 10):
        with db.session() as s:
            idea = s.get_one(Idea, run_row(db, run_id).idea_id)
            if idea.status is not IdeaStatus.READY:
                break
            idea.status = IdeaStatus.SCHEDULED
            run = Run(
                idea_id=idea.id,
                attempt=attempt,
                base_candidate_id=idea.base_candidate_id or 0,
                harness='script',
                model='test-model',
            )
            s.add(run)
            s.flush()
            s.add(
                AgentSession(
                    kind=IMPLEMENTER,
                    campaign_id=campaign.id,
                    run_id=run.id,
                    harness='script',
                    model='test-model',
                )
            )
            s.commit()
            run_id = run.id
        execute(ex, db, run_id)
    with db.session() as s:
        idea = s.get_one(Idea, run_row(db, run_id).idea_id)
        recovered = s.scalars(select(Candidate).where(Candidate.verdict == Verdict.RECOVERED)).all()
        assert (idea.status, len(idea.runs), len(recovered)) == (IdeaStatus.DONE, 3, 2), (
            'two requeues (the default), three runs, two recovered trees'
        )


def test_recovery_requeues_0_leaves_recovered_work_to_the_orchestrator(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    write_toml(Path(campaign.path), HARNESS_FORGETS_COMMIT)
    with db.session() as s:
        s.get_one(Campaign, campaign.id).config = load_campaign_config(
            Path(campaign.path), roles={'implementer': {'recovery_requeues': 0}}
        ).model_dump(mode='json')
        s.commit()
    run_id = make_run(db, campaign, max_runs=1)
    execute(controller(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        idea = s.get_one(Idea, run_row(db, run_id).idea_id)
        assert (idea.status, idea.base_candidate_id) == (IdeaStatus.DONE, None)


def test_the_role_of_the_run_caps_its_recovery_requeues(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A role that does runs has its own ``recovery_requeues``: the implementer's value does not apply to its runs."""
    write_toml(Path(campaign.path), HARNESS_FORGETS_COMMIT)
    roles = {'implementer': {'recovery_requeues': 0}, 'quick': {'runs': True, 'recovery_requeues': 1}}
    with db.session() as s:
        s.get_one(Campaign, campaign.id).config = load_campaign_config(Path(campaign.path), roles=roles).model_dump(
            mode='json'
        )
        s.commit()
    run_id = make_run(db, campaign, max_runs=1)
    with db.session() as s:
        s.get_one(Run, run_id).role = RoleName('quick')
        s.commit()
    execute(controller(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        idea = s.get_one(Idea, run_row(db, run_id).idea_id)
        assert idea.status is IdeaStatus.READY and idea.base_candidate_id is not None, (
            'requeued from its recovered work'
        )


def test_no_changes_fails_the_run_and_retries_the_idea(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr('piloop.agents.results.STARTUP_SECONDS', 0)  # an implementer that worked, then left nothing
    write_toml(Path(campaign.path), HARNESS_NOOP)
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign, max_runs=2)
    execute(controller(db, runtime, tmp_path), db, run_id)
    run = run_row(db, run_id)
    assert (run.status, run.error) == (RunStatus.FAILED, NO_CHANGES)
    with db.session() as s:
        assert s.get_one(Idea, run.idea_id).status is IdeaStatus.READY, 'one attempt left'


def test_implementer_failure_after_max_runs_finishes_the_idea(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr('piloop.agents.results.STARTUP_SECONDS', 0)  # an implementer that worked, then left nothing
    write_toml(Path(campaign.path), HARNESS_FAILS)
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign, max_runs=1)
    execute(controller(db, runtime, tmp_path), db, run_id)
    run = run_row(db, run_id)
    assert (run.status, run.error) == (RunStatus.FAILED, 'agent exit 3')
    with db.session() as s:
        assert s.get_one(Idea, run.idea_id).status is IdeaStatus.DONE


def test_timeout(db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path) -> None:
    write_toml(Path(campaign.path), HARNESS_SLEEPS, implementer_timeout=0.5)
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign, max_runs=1)
    execute(controller(db, runtime, tmp_path), db, run_id)
    run = run_row(db, run_id)
    assert (run.status, run.error) == (RunStatus.TIMED_OUT, 'agent timeout after 0.5s')
    with db.session() as s:
        [agent_session] = s.scalars(select(AgentSession)).all()
        assert agent_session.state is AgentSessionState.KILLED


def test_evaluate_failure_is_a_crash_verdict(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:

    cfg = load_campaign_config(
        Path(campaign.path),
        evaluate=['sh', '-c', 'test $(wc -l < solve.txt) -eq 1 && echo METRIC score=1 && echo METRIC size=1 || exit 9'],
    )
    with db.session() as s:
        s.get_one(Campaign, campaign.id).config = cfg.model_dump(mode='json')
        s.commit()
    run_id = make_run(db, campaign)
    execute(controller(db, runtime, tmp_path), db, run_id)
    [e] = candidates(db, run_id)
    assert (e.verdict, e.metrics) == (Verdict.CRASH, {})
    assert e.error is not None and e.error.startswith('evaluate exit 9')
    assert run_row(db, run_id).status is RunStatus.SUCCEEDED, (
        'the implementer did its job; the candidate carries the verdict'
    )


def test_a_crash_from_a_fault_of_the_host_is_measured_again_on_request_and_judged(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A crash that the host caused (a full disk) says nothing about the code, and its tree cannot be committed again.

    ``piloop candidate remeasure`` asks the daemon to measure the candidate again with the current evaluator. A crash
    that the daemon judged is then judged as its run would have judged it: here it keeps against its own bar after the
    head moved on, so it is rebased onto the head, and the merge is judged there.
    """
    fault = tmp_path / 'disk-full'
    measure = (
        f'test -e {fault} && exit 28; echo METRIC score=$(cat solve.txt other.txt 2>/dev/null | wc -l);'
        ' echo METRIC size=$(wc -c < solve.txt)'
    )
    write_toml(Path(campaign.path), HARNESS_APPEND, evaluate=['sh', '-c', measure])
    reload_campaign(db, campaign)
    ex = controller(db, runtime, tmp_path)
    ex.remeasure(baseline(db, campaign.id).id)  # the new evaluator measures the head first, as the daemon does
    run_id = make_run(db, campaign)
    fault.touch()
    execute(ex, db, run_id)
    [crashed] = candidates(db, run_id)
    assert crashed.verdict is Verdict.CRASH
    with db.session() as s:
        request_remeasure(s, s.get_one(Candidate, crashed.id), System.ORCHESTRATOR)
        s.commit()
    ex.remeasure(crashed.id)
    with db.session() as s:
        c = s.get_one(Candidate, crashed.id)
        assert (c.verdict, len(c.measurements)) == (Verdict.CRASH, 2), 'the fault is still there: a crash again'
        assert len(s.get_one(Candidate, baseline(db, campaign.id).id).measurements) == 2, (
            'its first run crashed, so its bar never ran'
        )
    fault.unlink()
    write_script(
        Path(campaign.path).parent / 'harness.sh',
        '#!/bin/sh\necho x > other.txt\ngit add other.txt\ngit commit -qm other\n',
    )
    execute(ex, db, make_run(db, campaign, title='elsewhere'))
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        moved = head(s, p)
        assert moved is not None and moved.summary == 'other', 'the head moved on meanwhile'
        assert requested(s, p) == [], 'nobody asked'
        request_remeasure(s, s.get_one(Candidate, crashed.id), System.ORCHESTRATOR)
        s.commit()
        assert [c.id for c in requested(s, p)] == [crashed.id]

    ex.remeasure(crashed.id)
    with db.session() as s:
        c = s.get_one(Candidate, crashed.id)
        assert (c.verdict, c.verdict_by) == (Verdict.INTERESTING, DAEMON)
        assert c.verdict_reason is not None and c.verdict_reason.startswith(
            f'keeps against its own bar, but the head moved on to candidate#{moved.id}: against candidate#1: score 2 vs 1'
            ' (+1; 95% worst case +1 over 3 pairs)'
        )
        assert [m.error is None for m in c.measurements] == [False, False, True, True, True]
        p = s.get_one(Campaign, campaign.id)
        assert requested(s, p) == [], 'measured since the request'
        merged = head(s, p)
        assert merged is not None and (merged.parent_id, merged.run_id, merged.verdict) == (
            moved.id,
            run_id,
            Verdict.KEEP,
        )

        request_remeasure(s, merged, System.ORCHESTRATOR)
        s.commit()
        merged_id = merged.id
    ex.remeasure(merged_id)
    with db.session() as s:
        c = s.get_one(Candidate, merged_id)
        assert (len(c.measurements), c.verdict) == (4, Verdict.KEEP), 'a repeat of a keep keeps its verdict'
        assert {m.pair.candidate_id for m in c.measurements if m.pair} == {moved.id}, (
            'its bar is the head it merged onto'
        )


def test_a_crash_on_the_evaluate_timeout_is_measured_again_on_request_once_the_timeout_is_raised(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A load that slows the evaluator past ``evaluate_timeout`` crashes good code, and its tree cannot be committed
    again. The timeout is no part of the evaluator, so once it is raised, ``piloop candidate remeasure`` measures the
    candidate again under the same evaluator, with the new timeout, and judges it again."""
    measure = 'test $(wc -l < solve.txt) -gt 1 && sleep 1; echo METRIC score=$(wc -l < solve.txt); echo METRIC size=1'
    write_toml(Path(campaign.path), HARNESS_APPEND, evaluate=['sh', '-c', measure])
    toml = Path(campaign.path) / 'piloop.toml'
    toml.write_text(toml.read_text().replace('evaluate_timeout = 30', 'evaluate_timeout = 0.2'))
    reload_campaign(db, campaign)
    ex = controller(db, runtime, tmp_path)
    ex.remeasure(baseline(db, campaign.id).id)
    run_id = make_run(db, campaign)
    execute(ex, db, run_id)
    [crashed] = candidates(db, run_id)
    assert (crashed.verdict, crashed.error) == (Verdict.CRASH, 'evaluate timeout after 0.2s')
    with db.session() as s:
        evaluator_id = s.get_one(Campaign, campaign.id).evaluator_id
    toml.write_text(toml.read_text().replace('evaluate_timeout = 0.2', 'evaluate_timeout = 30'))
    reload_campaign(db, campaign)
    with db.session() as s:
        request_remeasure(s, s.get_one(Candidate, crashed.id), System.ORCHESTRATOR, repeats=1)
        s.commit()
    ex.remeasure(crashed.id)
    with db.session() as s:
        c = s.get_one(Candidate, crashed.id)
        assert [m.evaluator_id == evaluator_id for m in c.measurements] == [True, True], 'the same evaluator'
        assert (c.verdict, c.verdict_by, c.verdict_reason) == (
            Verdict.KEEP,
            DAEMON,
            'against candidate#1: score 2 vs 1 (+1; 95% worst case +1 over 1 pairs)',
        )
        assert head(s, s.get_one(Campaign, campaign.id)) == c


LOADED = (
    'lines=$(wc -l < solve.txt); echo $lines >> {order}; grep -qx $lines {fail} 2>/dev/null && exit 3;'
    ' echo METRIC score=$lines; echo METRIC seconds=$((10 * $(cat {load})))'
)
"""An evaluate whose time is 10 s times the host's load, whatever the tree. It logs the lines of each tree it runs, and
fails on a tree whose lines are in ``fail``."""


def loaded(db: Database, campaign: Campaign, tmp_path: Path) -> tuple[Path, Path, Path]:
    """Give the campaign the ``LOADED`` evaluate, with ``seconds`` as a guardrail of margin 5. Return its files."""
    load, order, fail = tmp_path / 'load', tmp_path / 'order', tmp_path / 'fail'
    load.write_text('1')
    metrics = [
        {'name': 'score', 'direction': 'higher', 'role': 'primary'},
        {'name': 'seconds', 'direction': 'lower', 'role': 'guardrail', 'margin': 5},
    ]
    evaluate = ['sh', '-c', LOADED.format(load=load, order=order, fail=fail)]
    set_config(db, campaign, metrics=metrics, evaluate=evaluate)
    return load, order, fail


def test_a_remeasure_runs_pairs_of_the_candidate_and_its_bar_and_judges_it_again_on_them(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """The bar was measured on a quiet host, the candidate under load, so a guardrail held the keep back. A remeasure
    runs the candidate and its bar one after the other, so the load hits both, and judges the candidate on the pairs."""
    load, order, _ = loaded(db, campaign, tmp_path)
    ex = controller(db, runtime, tmp_path)
    ex.remeasure(baseline(db, campaign.id).id)  # the new evaluator measures the head first, as the daemon does
    load.write_text('4')
    run_id = make_run(db, campaign)
    execute(ex, db, run_id)
    [c] = candidates(db, run_id)
    with db.session() as s:
        assert s.get_one(Candidate, c.id).verdict_reason == snapshot(
            'against candidate#1: score 2 vs 1 (+1; aggregate: no per-unit results); guardrails: seconds 40 vs 10 (+30; aggregate: no per-unit results) beyond its margin 5'
        )
    order.write_text('')
    with db.session() as s:
        request_remeasure(s, s.get_one(Candidate, c.id), System.ORCHESTRATOR, repeats=3)
        s.commit()
    ex.remeasure(c.id)
    assert order.read_text().split() == ['2', '1', '1', '2', '2', '1'], 'the candidate first, then in ABBA order'
    with db.session() as s:
        candidate = s.get_one(Candidate, c.id)
        assert (candidate.verdict, candidate.verdict_by, candidate.verdict_reason) == snapshot(
            (
                Verdict.KEEP,
                'daemon',
                'against candidate#1: score 2 vs 1 (+1; 95% worst case +1 over 3 pairs); guardrails: seconds 40 vs 40 (+0; 95% worst case +0 over 3 pairs) within its margin 5',
            )
        )
        paired = [(m.metrics, m.pair.candidate_id, m.pair.metrics) for m in candidate.measurements if m.pair]
        assert paired == [({'score': 2.0, 'seconds': 40.0}, 1, {'score': 1.0, 'seconds': 40.0})] * 3
        assert head(s, s.get_one(Campaign, campaign.id)) == candidate
        recorded = [p.pair for _, p in in_order(s, MeasurementRecorded)]
        bars = [m.pair_id for m in candidate.measurements if m.pair]
        assert recorded[-6:] == [None, None, None, *bars], 'the event names the pair too'


def test_a_paired_evaluation_stops_at_the_first_run_that_fails(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A pair needs both sides, so a bar that crashes ends the evaluation. The candidate ran first: it has a sample."""
    _, order, fail = loaded(db, campaign, tmp_path)
    ex = controller(db, runtime, tmp_path)
    ex.remeasure(baseline(db, campaign.id).id)
    run_id = make_run(db, campaign)
    execute(ex, db, run_id)
    [c] = candidates(db, run_id)
    fail.write_text('1\n')
    order.write_text('')
    with db.session() as s:
        request_remeasure(s, s.get_one(Candidate, c.id), System.ORCHESTRATOR)
        s.commit()
    ex.remeasure(c.id)
    assert order.read_text().split() == ['2', '1'], 'the bar failed: no pair can follow'
    with db.session() as s:
        crashed = s.get_one(Candidate, baseline(db, campaign.id).id).measurements[-1]
        assert crashed.error == IsStr(regex='evaluate exit 3.*')
        last = s.get_one(Candidate, c.id).measurements[-1]
        assert (last.error, last.pair) == (None, crashed), 'it names what ran next to it, which is no pair'


DRIFTING = (
    'load=$(head -n 1 {loads}); tail -n +2 {loads} > {loads}.next; mv {loads}.next {loads};'
    ' echo METRIC score=$(wc -l < solve.txt); echo METRIC seconds=$(awk -v l=$load "BEGIN {{ print 10 * l }}")'
)
"""An evaluate whose time is 10 s times the next load of ``loads``, whatever the tree: one line per run."""


def drifting_loads(seed: int) -> list[float]:
    """The load of each run: 1 for the bar on a quiet host, then a load between 2x and 5x that drifts a little on each
    run, as a host's load does over seconds."""
    rng = random.Random(seed)
    load, loads = rng.uniform(2, 5), [1.0]
    for _ in range(12):
        loads.append(round(load, 3))
        load = min(max(load * math.exp(rng.gauss(0, 0.03)), 1), 5)
    return loads


@pytest.mark.parametrize('seed', range(10))
@pytest.mark.parametrize('repeats', [None, 3])
def test_an_a_a_test_under_load_keeps_a_gain_of_the_same_speed_only_when_it_is_paired(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, seed: int, repeats: int | None
) -> None:
    """The candidate gains on the primary and has the speed of its bar, which was measured on a quiet host. Unpaired,
    the load decides its time guardrail. With ``repeats``, the bar runs next to it, so the load hits both alike."""
    loads = tmp_path / 'loads'
    loads.write_text(''.join(f'{x}\n' for x in drifting_loads(seed)))
    seconds = {'name': 'seconds', 'direction': 'lower', 'role': 'guardrail', 'margin': 5, 'repeats': repeats}
    metrics = [{'name': 'score', 'direction': 'higher', 'role': 'primary'}, seconds]
    set_config(db, campaign, metrics=metrics, evaluate=['sh', '-c', DRIFTING.format(loads=loads)])
    ex = controller(db, runtime, tmp_path)
    ex.remeasure(baseline(db, campaign.id).id)  # the new evaluator measures the head on a quiet host
    run_id = make_run(db, campaign)
    execute(ex, db, run_id)
    with db.session() as s:
        c = s.scalars(select(Candidate).where(Candidate.run_id == run_id)).one()
        assert (c.verdict, len(c.measurements)) == ((Verdict.KEEP, 3) if repeats else (Verdict.INTERESTING, 1))


def test_a_remeasure_that_a_stop_cancels_records_and_judges_nothing(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    ex = controller(db, runtime, tmp_path)
    ex.launcher.cancel()
    ex.remeasure(baseline(db, campaign.id).id)
    with db.session() as s:
        c = s.get_one(Candidate, baseline(db, campaign.id).id)
        assert (len(c.measurements), c.verdict) == (1, Verdict.KEEP), 'the stop owns the rows'


def test_brief_carries_history_and_comments(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    write_toml(Path(campaign.path), HARNESS_PROMPT_TO_FILE)
    reload_campaign(db, campaign)
    first = make_run(db, campaign, title='first idea')
    execute(controller(db, runtime, tmp_path), db, first)  # produces an uncommitted prompt.txt → scored as a commit
    second = make_run(db, campaign, title='second idea')
    with db.session() as s:
        idea_id = s.get_one(Run, second).idea_id
        s.add(Comment(target=TargetRef(TargetKind.IDEA, idea_id), author='human:t', body='try harder'))
        s.add(Comment(target=TargetRef(TargetKind.RUN, second), author='pi', kind='decision', body='and faster'))
        s.add(Comment(target=TargetRef(TargetKind.RUN, first), author='pi', body='about another idea'))
        s.commit()
    execute(controller(db, runtime, tmp_path), db, second)
    brief = run_row(db, second).brief or ''
    order = [
        '# Implementer',
        '# Demo',
        '# Working with the piloop database',
        '# Context',
        '## Your idea',
        '## Comments on this idea',
        '## History: ideas',
        '## History: kept lineage',
    ]
    assert [brief.index(s) for s in order] == sorted(brief.index(s) for s in order), 'the fixed brief order'
    assert '## Your idea (idea#2): second idea' in brief and 'candidate#2' in brief
    assert (
        '## Comments on this idea and its runs\n- human:t (note): try harder\n- pi (decision on run#2): and faster\n'
        in (brief)
    ), "the idea's comments and those on any of its runs, in order, each run comment saying which run"
    assert 'about another idea' not in brief
    assert '| 1 ' in brief and 'first idea' in brief, 'the same history block the research agent reads'
    assert 'Tried so far' not in brief
    assert '"$PILOOP_BIN" sql' in brief and "'implementer#2', 'finding'" in brief
    assert (
        '- Your session ends 0.3 minutes after it starts, at `$PILOOP_DEADLINE` (Unix time). The daemon tells you to'
        ' wrap up 0.1 minutes before, but a message reaches you only between two tool calls: start no command that'
        ' can run past the deadline. Commit each step that runs, and leave your finding before the end.\n' in brief
    ), 'the role timeout of 20s'
    assert "VALUES ('idea', 2, 'implementer#2', 'escalation', 1, '...')" in brief, (
        'a request is an escalation, never a note'
    )
    assert 'Never wait with `pgrep -f "<pattern>"`' in brief, 'a waiter that finds itself keeps the run open'
    assert '- A hunch worth a run of its own goes in your finding' in brief and 'idea seed demo' not in brief


def test_an_implementer_brief_names_its_worktree_as_the_place_to_work_and_the_campaign_repo_as_read_only(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """Given only the campaign repo's path, an implementer edited and committed there: never scored, and main moved.

    The brief names no path of the worktree: the worker makes it, on whichever machine runs the job."""
    run_id = make_run(db, campaign)
    execute(controller(db, runtime, tmp_path), db, run_id)
    brief = run_row(db, run_id).brief or ''
    assert (
        '- Your cwd is your worktree: make every edit, run every evaluation and make every commit there.'
        f' Only its commits are scored. The campaign repo, {campaign.path}, is read-only for you: read its history'
        ' and candidates, never edit, run or commit in it.'
    ) in brief


@pytest.mark.parametrize('budget', [400, 0])
def test_an_implementer_brief_opens_with_the_current_lessons_of_the_repo_within_its_budget(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, budget: int
) -> None:
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        p.repo = 'github.com/o/r'
        p.config = load_campaign_config(Path(campaign.path), roles={'implementer': {'wake_budget': budget}}).model_dump(
            mode='json'
        )
        s.add(
            Lesson(
                campaign_id=campaign.id,
                family='voting',
                status=LessonStatus.DEAD,
                effect=0,
                metric='score',
                body='idea#1 on evaluator#1',
                created_by='watchdog#9',
            )
        )
        s.commit()
    run_id = make_run(db, campaign)
    execute(controller(db, runtime, tmp_path), db, run_id)
    brief = run_row(db, run_id).brief or ''
    line = '- [demo] voting | dead | +0 score (lesson#1)'
    if budget:
        assert brief.index('# Implementer') < brief.index(line) < brief.index('# Demo'), (
            'after the role, before the campaign'
        )
    else:
        assert line not in brief and 'What past campaigns' not in brief, (
            '[roles.implementer] wake_budget = 0 turns it off'
        )


def test_pre_written_brief_is_used_verbatim(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    run_id = make_run(db, campaign)
    with db.session() as s:
        s.get_one(Run, run_id).brief = 'custom brief'
        s.commit()
    execute(controller(db, runtime, tmp_path), db, run_id)
    assert (tmp_path / 'runs' / str(run_id) / 'brief.md').read_text() == 'custom brief'


def test_exception_in_run_is_recorded(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
) -> None:
    run_id = make_run(db, campaign, max_runs=1)
    with db.session() as s:
        s.get_one(Run, run_id).base_candidate_id = CandidateId(999)
        s.commit()
    execute(controller(db, runtime, tmp_path, tracer), db, run_id)
    run = run_row(db, run_id)
    assert run.status is RunStatus.FAILED and run.error is not None and run.error.startswith('NoResultFound')
    with db.session() as s:
        assert s.get_one(Idea, run.idea_id).status is IdeaStatus.DONE
    [span] = spans.get_finished_spans()
    assert span.status.status_code is StatusCode.ERROR and [e.name for e in span.events] == ['exception']


def test_the_head_is_the_latest_trunk_keep_whatever_it_scores(db: Database, campaign: Campaign) -> None:
    with db.session() as s:
        proj = s.get_one(Campaign, campaign.id)
        evaluator_id = proj.evaluator_id
        assert evaluator_id is not None
        idea = Idea(track='score', campaign_id=campaign.id, title='t', hypothesis='h', created_by='t')
        s.add(idea)
        s.flush()
        branch = Run(idea_id=idea.id, attempt=1, base_candidate_id=1, harness='h', model='m', branch=True)
        branch.status, branch.end = RunStatus.SUCCEEDED, RunEnd.FINISHED
        s.add(branch)
        s.flush()
        rows = {}
        for tree, verdict, run_id, score in (
            ('a', Verdict.KEEP, None, 9.0),
            ('b', Verdict.KEEP, None, 1.0),
            ('c', Verdict.INTERESTING, None, 99.0),
            ('d', Verdict.KEEP, branch.id, 50.0),
            ('r', Verdict.REFERENCE, None, 5.0),
        ):
            rows[tree] = c = Candidate(
                campaign_id=campaign.id, tree_sha=tree, commit_sha=tree, verdict=verdict, run_id=run_id
            )
            s.add(c)
            s.flush()
            s.add(Measurement(candidate_id=c.id, evaluator_id=evaluator_id, metrics={'score': score}))
        s.commit()
        trunk = head(s, proj)
        assert trunk is not None and trunk.tree_sha == 'b', (
            'b is the latest trunk keep: a lower score than a changes nothing, an interesting is no keep, a branch'
            ' run keep (d) never moves the head and a reference is never one'
        )
        assert head(s, proj, excluding=rows['b']) == rows['a'], 'the candidate being judged is never its own bar'
        unscored = Candidate(campaign_id=campaign.id, tree_sha='u', commit_sha='u', verdict=Verdict.REFERENCE)
        s.add(unscored)
        s.flush()
        assert [c.tree_sha for c in due(s, proj)] == ['u'], 'a reference that no evaluator measured'
        proj.evaluator_id = s.scalar(
            insert(Evaluator).values(campaign_id=campaign.id, key='next', definition={}).returning(Evaluator.id)
        )
        assert [c.tree_sha for c in due(s, proj)] == ['b', 'u'], (
            'under a new evaluator the head, not the older keeps, the branch keep or the measured reference'
        )


def test_a_new_evaluator_measures_what_a_verdict_needs_then_what_someone_asked_for(
    db: Database, campaign: Campaign
) -> None:
    """After an evaluator change, the daemon measures the head, the bars of open runs, the asks and the bars of pinned
    ideas, in that order. An ask needs no measured bar: its paired evaluation measures the bar too.

    Every other keep keeps its old scores until someone asks: measuring the whole history again held every evaluation
    for hours after each change of the evaluator.
    """
    with db.session() as s:
        proj = s.get_one(Campaign, campaign.id)
        old = proj.evaluator_id
        assert old is not None
        idea = Idea(track='score', campaign_id=campaign.id, title='t', hypothesis='h', created_by='t')
        s.add(idea)
        s.flush()
        rows: dict[str, Candidate] = {}

        def add(tree: str, verdict: Verdict, parent: str | None = None, run: Run | None = None) -> Candidate:
            rows[tree] = c = Candidate(
                campaign_id=campaign.id,
                tree_sha=tree,
                commit_sha=tree,
                verdict=verdict,
                verdict_by=DAEMON,
                parent_id=rows[parent].id if parent else None,
                run_id=run.id if run else None,
            )
            s.add(c)
            s.flush()
            s.add(Measurement(candidate_id=c.id, evaluator_id=old, metrics={'score': 1.0}))
            return c

        for tree in ('k1', 'k2', 'k3', 'k4', 'k5'):
            add(tree, Verdict.KEEP, parent='k1' if tree != 'k1' else None)
        add('lead', Verdict.INTERESTING, parent='k1')
        add('ref', Verdict.REFERENCE)
        done = Run(idea_id=idea.id, attempt=1, base_candidate_id=rows['k2'].id, harness='h', model='m')
        done.status, done.end = RunStatus.SUCCEEDED, RunEnd.FINISHED
        s.add_all([done, Run(idea_id=idea.id, attempt=2, base_candidate_id=rows['lead'].id, harness='h', model='m')])
        s.flush()
        add('crash', Verdict.CRASH, parent='k2', run=done)
        add('repeat', Verdict.DISCARD, parent='k3', run=done)
        add('head', Verdict.KEEP, parent='k5')
        fresh = Candidate(campaign_id=campaign.id, tree_sha='fresh', commit_sha='fresh', verdict=Verdict.REFERENCE)
        s.add(fresh)
        s.add(
            Idea(
                track='score',
                campaign_id=campaign.id,
                title='pinned',
                hypothesis='h',
                created_by='t',
                base_candidate_id=rows['k4'].id,
            )
        )
        for asked in ('crash', 'repeat'):
            request_remeasure(s, rows[asked], System.ORCHESTRATOR)
        proj.evaluator_id = new = s.scalar(
            insert(Evaluator).values(campaign_id=campaign.id, key='next', definition={}).returning(Evaluator.id)
        )
        s.flush()
        assert [c.tree_sha for c in due(s, proj)] == ['head', 'k1', 'crash', 'repeat', 'k4', 'fresh'], (
            'the head; the bar of the open run (its base is a lead of k1); the asks; the bar of the pinned idea; the'
            ' new reference. Not k2, k3, k5, the baseline or ref'
        )
        for tree in ('head', 'k1'):
            s.add(Measurement(candidate_id=rows[tree].id, evaluator_id=new, metrics={'score': 1.0}))
        s.flush()
        s.expire_all()
        assert [c.tree_sha for c in due(s, proj)] == ['crash', 'repeat', 'k4', 'fresh'], 'measured: no longer due'


def test_transcript_failure_is_only_an_event(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    mocker: MockerFixture,
    tracer: Tracer,
    spans: InMemorySpanExporter,
) -> None:
    mocker.patch.object(runtime, 'read', side_effect=OSError('pane gone'))
    run_id = make_run(db, campaign)
    execute(controller(db, runtime, tmp_path, tracer), db, run_id)
    assert run_row(db, run_id).status is RunStatus.SUCCEEDED
    assert events(spans, 'agent_session.transcript_lost') == [{'error': 'pane gone'}]


def test_a_rejection_during_the_run_is_not_undone(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    run_id = make_run(db, campaign)
    with db.session() as s:
        s.get_one(Idea, s.get_one(Run, run_id).idea_id).status = IdeaStatus.REJECTED
        s.commit()
    execute(controller(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        assert s.get_one(Idea, 1).status is IdeaStatus.REJECTED


def test_implementer_env_points_at_the_daemon_home(
    db: Database, campaign: Campaign, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The implementer sees the daemon's home, its PATH after its own piloop, and no harness session of the daemon.

    A daemon started from a Claude Code session inherits its variables, and a Claude agent that has them acts as a
    child of that session, which saves no transcript. They are cleared for every agent, whatever its harness
    definition: ``[harness.claude]`` replaces the built-in one, and a custom harness can run the same CLI. This one is
    the ``script`` harness of the fixture, with a session variable of its own.
    """
    cleared = {*SESSION_ENV, 'SCRIPT_SESSION'}
    for name in cleared:
        monkeypatch.setenv(name, '1')
    write_toml(Path(campaign.path), '#!/bin/sh\nenv > env.txt\ngit add env.txt\ngit commit -qm env\n')
    with (Path(campaign.path) / 'piloop.toml').open('a') as toml:  # [harness.script] is the last table
        toml.write('session_env = ["SCRIPT_SESSION"]\n')
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign)
    execute(controller(db, ScriptRuntime(), tmp_path), db, run_id)
    [e] = candidates(db, run_id)
    lines = Repo(campaign.path).git.show(f'{e.commit_sha}:env.txt').splitlines()
    env = dict(line.split('=', 1) for line in lines if '=' in line)
    assert (env['PILOOP_HOME'], env['PATH']) == (str(tmp_path), f'{tmp_path / "bin"}{os.pathsep}{os.environ["PATH"]}')
    assert {name: env.get(name) for name in cleared} == dict.fromkeys(cleared, '')


def test_lineage_follows_the_actual_predecessor(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    write_toml(
        Path(campaign.path),
        '#!/bin/sh\n: > solve.txt\ngit commit -qam worse\necho a >> solve.txt\necho b >> solve.txt\ngit commit -qam better\n',
    )
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign)
    execute(controller(db, runtime, tmp_path), db, run_id)
    worse, better = candidates(db, run_id)
    assert (worse.verdict, better.verdict) == (Verdict.DISCARD, Verdict.KEEP)
    assert better.parent_id == worse.id, 'built on the discarded commit, judged against the last keep'


def test_an_implementer_that_never_got_going_is_relaunched_on_its_run_then_the_run_fails(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """An implementer that dies at startup is not an attempt: the same run is queued again for a new implementer.

    The dead implementer's transcript is kept aside. A run whose every launch died fails, but it is still no attempt: its
    idea is parked, whatever attempts it has left, so that a broken host or harness does not burn them.
    """
    run_id = make_run(db, campaign, max_runs=1)
    write_toml(Path(campaign.path), '#!/bin/sh\necho Welcome\n')
    reload_campaign(db, campaign)
    ex = controller(db, runtime, tmp_path)
    for launches in (1, 2):
        execute(ex, db, run_id)
        with db.session() as s:
            run = s.get_one(Run, run_id)
            assert (run.status, run.finished_at, run.idea.status) == (RunStatus.QUEUED, None, IdeaStatus.SCHEDULED)
            rows = s.scalars(select(AgentSession).where(AgentSession.run_id == run_id).order_by(AgentSession.id)).all()
            assert [w.state for w in rows] == [*[AgentSessionState.EXITED] * launches, AgentSessionState.REQUESTED]
            assert (rows[-1].harness, rows[-1].model) == ('script', 'test-model')
            assert rows[-1].reason == f'run#{run_id} relaunched: implementer#{rows[-2].id} failed to start', 'its role'
            assert stop.runs(s, run.idea.campaign) == 1
        assert (tmp_path / 'runs' / str(run_id) / f'transcript-implementer{rows[-2].id}.txt').read_text() == 'Welcome'
    execute(ex, db, run_id)
    with db.session() as s:
        relaunched = in_order(s, RunRelaunched)
        assert [p.launches for _, p in relaunched] == [1, 2]
        idea = s.get_one(Idea, run_row(db, run_id).idea_id)
        assert idea.status is IdeaStatus.PARKED, 'no agent of its run started to work'
        [note] = s.scalars(select(Comment).where(Comment.target_id == idea.id)).all()
        assert (note.target_kind, note.author, note.kind, note.body) == (
            TargetKind.IDEA,
            'daemon',
            'note',
            f'parked: no agent of run#{run_id} started to work; fix what stopped them (the errors of its implementers, their'
            ' anomaly.startup events), then set it ready',
        )
    run = run_row(db, run_id)
    assert (run.status, run.end) == (RunStatus.FAILED, RunEnd.NOT_STARTED)
    with db.session() as s:
        assert [a.error for _, a in in_order(s, StartupAnomaly)] == [NEVER_WORKED] * 3, 'each says why it failed'
        assert stop.attempts(s.get_one(Idea, run_row(db, run_id).idea_id)) == 0

    mocker.patch.object(runtime, 'start', side_effect=RuntimeError('no server'))
    retried = make_run(db, campaign, title='retried', max_runs=2)
    for _ in range(3):
        execute(ex, db, retried)
    with db.session() as s:
        assert s.get_one(Idea, run_row(db, retried).idea_id).status is IdeaStatus.PARKED, (
            'attempts left, but a new run would fail to start the same way'
        )


def test_a_worktree_that_cannot_be_made_is_a_failed_start_that_frees_the_slot(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """A run whose worktree cannot be made closes its implementer as a failed start, so the implementer holds no slot."""
    full = GitCommandError(['git', 'worktree', 'add'], 128, b'fatal: Could not write new index file')
    mocker.patch('piloop.plugins.vcs.git.commands.add_worktree', side_effect=full)
    run_id = make_run(db, campaign, max_runs=1)
    ex = controller(db, runtime, tmp_path)
    for _ in range(3):
        execute(ex, db, run_id)
    with db.session() as s:
        rows = s.scalars(select(AgentSession).where(AgentSession.run_id == run_id).order_by(AgentSession.id)).all()
        assert [w.state for w in rows] == [AgentSessionState.KILLED] * 3
        assert all('Could not write new index file' in (w.error or '') for w in rows)
        assert s.scalars(select(Event.target_id).where(has(StartupAnomaly))).all() == [w.id for w in rows]
        run = s.get_one(Run, run_id)
        assert (run.status, run.end, run.idea.status) == (RunStatus.FAILED, RunEnd.NOT_STARTED, IdeaStatus.PARKED)


def test_a_run_that_fails_before_its_agent_starts_closes_its_implementer(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """A fault of the daemon before the launch fails the run and closes its implementer, which then holds no slot."""
    mocker.patch('piloop.plugins.vcs.git.commands.open_repo', side_effect=RuntimeError('repo gone'))
    run_id = make_run(db, campaign)
    execute(controller(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        [w] = s.scalars(select(AgentSession).where(AgentSession.run_id == run_id)).all()
        assert (w.state, w.error, w.ended_at is not None) == (AgentSessionState.KILLED, 'RuntimeError: repo gone', True)
    assert (run_row(db, run_id).status, run_row(db, run_id).error) == (RunStatus.FAILED, 'RuntimeError: repo gone')


def test_an_implementer_that_spends_tokens_but_ends_at_once_with_nothing_did_not_start_to_work(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An implementer whose tools fail at launch still reads its brief and says it is blocked, which costs tokens.

    A run's work is a commit or a comment, so an implementer that ends within the startup window without either did not
    start to work, whatever it spent. Its run is queued again, not counted as an attempt.
    """
    home = tmp_path / 'home'
    monkeypatch.setenv('HOME', str(home))
    run_id = make_run(db, campaign, max_runs=1)
    folder = (
        home
        / '.pi'
        / 'agent'
        / 'sessions'
        / CWD_ENCODERS['dash_separators_wrapped'](tmp_path / 'runs' / str(run_id) / 'worktree')
    )
    blocked = f'#!/bin/sh\nmkdir -p {folder}\ncp {SESSIONS / "pi.jsonl"} "{folder}/2026-09-09T05-48-14-300Z_$2.jsonl"\n'
    write_toml(Path(campaign.path), blocked)
    script = load_campaign_config(Path(campaign.path)).harness['script'].model_dump(mode='json')
    counted = {'headless': ['--session-id', '{session_id}', '{prompt}'], 'usage': PI_SPEC}
    set_config(db, campaign, harness={'script': {**script, **counted}})
    execute(controller(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        dead, relaunched = s.scalars(
            select(AgentSession).where(AgentSession.run_id == run_id).order_by(AgentSession.id)
        ).all()
        assert (dead.state, dead.usage.total if dead.usage else None, relaunched.state) == (
            AgentSessionState.EXITED,
            16478,
            AgentSessionState.REQUESTED,
        )
        assert s.scalars(select(Event.target_id).where(has(StartupAnomaly))).all() == [dead.id]
        run = s.get_one(Run, run_id)
        assert (run.status, run.error, run.idea.status) == (RunStatus.QUEUED, None, IdeaStatus.SCHEDULED), (
            'the same run'
        )


@pytest.mark.parametrize('on', ['run', 'agent'])
def test_a_cancel_made_while_the_implementer_starts_survives_a_failed_start(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture, on: str
) -> None:
    """The daemon leaves a starting agent alone. When its start then failed, the run was queued again with a new
    implementer, which the cancel did not name, and the cancelled run ran after all."""
    run_id = make_run(db, campaign)

    def cancel_then_fail(spec: object) -> Handle:
        with db.session() as s:
            run = s.get_one(Run, run_id)
            [agent_session] = s.scalars(select(AgentSession).where(AgentSession.run_id == run_id)).all()
            if on == 'run':
                stop.request_cancel(s, run, System.HUMAN, ending=stop.Ending.NOW)
            else:
                stop.request_cancel(s, agent_session, System.HUMAN, ending=stop.Ending.NOW)
            s.commit()
        raise RuntimeError('no pickup')

    mocker.patch.object(runtime, 'start', side_effect=cancel_then_fail)
    execute(controller(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        run = s.get_one(Run, run_id)
        assert (run.status, run.end, run.idea.status) == (RunStatus.FAILED, RunEnd.CANCELLED, IdeaStatus.PARKED)
        assert [w.state for w in s.scalars(select(AgentSession).where(AgentSession.run_id == run_id))] == [
            AgentSessionState.KILLED
        ]
        assert s.scalars(select(Event).where(Event.kind == 'run.relaunched')).all() == []


def test_a_cancelled_run_whose_implementer_ends_by_itself_first_closes_as_cancelled(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """The daemon ends a cancelled implementer on its next tick; one that ends before that does not requeue its idea."""
    run_id = make_run(db, campaign)
    with db.session() as s:
        stop.request_cancel(s, s.get_one(Run, run_id), System.HUMAN, ending=stop.Ending.NOW)
        s.commit()
    execute(controller(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        run = s.get_one(Run, run_id)
        assert (run.status, run.end, run.idea.status) == (RunStatus.FAILED, RunEnd.CANCELLED, IdeaStatus.PARKED)
        assert [c.verdict for c in s.scalars(select(Candidate).where(Candidate.run_id == run_id))] == [Verdict.KEEP]


def test_a_relaunched_implementer_signs_as_itself_and_its_finding_without_a_commit_is_a_result(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """A relaunched implementer gets a brief that names it, not the dead implementer, and its finding without a commit counts."""
    run_id = make_run(db, campaign, max_runs=1)
    idea_id = run_row(db, run_id).idea_id
    finding = f"INSERT INTO comments (target_kind, target_id, author, kind, body) VALUES ('idea', {idea_id}, '$me', 'finding', 'no effect')"
    me = 'me=$(grep -o "Sign what you write as .implementer#[0-9]*" ../brief.md | grep -o "implementer#[0-9]*")'
    write_toml(Path(campaign.path), f'#!/bin/sh\n{me}\n{SQL_HINT} "{finding}"\n')
    reload_campaign(db, campaign)
    start = mocker.patch.object(runtime, 'start', wraps=runtime.start)
    start.side_effect = [RuntimeError('no pickup'), DEFAULT]
    ex = controller(db, runtime, tmp_path)
    execute(ex, db, run_id)
    execute(ex, db, run_id)
    run = run_row(db, run_id)
    assert (run.status, run.error) == (RunStatus.SUCCEEDED, None)
    with db.session() as s:
        dead, replacement = s.scalars(
            select(AgentSession.id).where(AgentSession.run_id == run_id).order_by(AgentSession.id)
        ).all()
        [author] = s.scalars(select(Comment.author).where(Comment.kind == 'finding')).all()
        assert author == f'implementer#{replacement}'
        assert f'implementer#{dead}' not in (run.brief or '') and f'You are implementer#{replacement},' in (
            run.brief or ''
        )
        assert s.get_one(Idea, idea_id).status is IdeaStatus.DONE


@pytest.mark.parametrize('work', ['commit', 'finding'])
def test_a_fast_implementer_whose_tokens_are_unknown_is_no_startup_death_when_it_committed_or_left_a_finding(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, work: str
) -> None:
    """An implementer whose tokens are unknown is not a startup death when it committed or left a finding.

    The runtime may report no session for an implementer, so its tokens cannot be read. Work done within the startup window
    proves that it started.
    """
    monkeypatch.setenv('HOME', str(tmp_path / 'home'))  # no session file: the tokens are unknown
    run_id = make_run(db, campaign, max_runs=1)
    idea_id = run_row(db, run_id).idea_id
    finding = f"INSERT INTO comments (target_kind, target_id, author, kind, body) VALUES ('idea', {idea_id}, '$me', 'finding', 'no effect')"
    me = 'me=$(grep -o "Sign what you write as .implementer#[0-9]*" ../brief.md | grep -o "implementer#[0-9]*")'
    body = HARNESS_APPEND if work == 'commit' else f'#!/bin/sh\n{me}\n{SQL_HINT} "{finding}"\n'
    write_toml(Path(campaign.path), body)
    script = load_campaign_config(Path(campaign.path)).harness['script'].model_dump(mode='json')
    counted = {'headless': ['--session-id', '{session_id}', '{prompt}'], 'usage': PI_SPEC}
    set_config(db, campaign, harness={'script': {**script, **counted}})
    execute(controller(db, runtime, tmp_path), db, run_id)
    run = run_row(db, run_id)
    assert (run.status, run.error) == (RunStatus.SUCCEEDED, None)
    with db.session() as s:
        assert s.scalar(select(func.count()).select_from(AgentSession).where(AgentSession.run_id == run_id)) == 1, (
            'no relaunch'
        )
        assert not s.scalars(select(Event).where(has(RunRelaunched) | has(StartupAnomaly))).all()


@pytest.mark.parametrize('edit', ['echo line >> solve.txt', 'echo note > new.txt'])
def test_a_fast_implementer_that_left_uncommitted_work_started_and_its_work_is_recovered(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, edit: str
) -> None:
    """An implementer that changed its worktree worked, however fast it ended without a commit or a comment.

    Its change is recovered as for any other implementer. A relaunch would remove the worktree, and the change with it.
    """
    monkeypatch.setenv('HOME', str(tmp_path / 'home'))
    write_toml(Path(campaign.path), f'#!/bin/sh\n{edit}\n')
    script = load_campaign_config(Path(campaign.path)).harness['script'].model_dump(mode='json')
    counted = {'headless': ['--session-id', '{session_id}', '{prompt}'], 'usage': PI_SPEC}
    set_config(db, campaign, harness={'script': {**script, **counted}})
    run_id = make_run(db, campaign, max_runs=1)
    execute(controller(db, runtime, tmp_path), db, run_id)
    assert (run_row(db, run_id).status, run_row(db, run_id).error) == (RunStatus.FAILED, INTERRUPTED)
    [(summary, verdict, *_)] = scored(db, run_id)
    assert (summary, verdict) == (UNCOMMITTED_SUMMARY, Verdict.RECOVERED)
    with db.session() as s:
        assert s.scalar(select(func.count()).select_from(AgentSession).where(AgentSession.run_id == run_id)) == 1, (
            'no relaunch'
        )
        assert not s.scalars(select(Event).where(has(StartupAnomaly))).all()


def test_a_startup_death_during_a_stop_leaves_the_run_to_the_stop(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """A startup death during a stop does not relaunch the run, because the stop fails the open runs itself."""
    ex = controller(db, runtime, tmp_path)

    def stopped_meanwhile(spec: object) -> Handle:
        ex.launcher.cancelled.set()  # what ``cancel`` sets; it takes the lock this start holds
        raise RuntimeError('no pickup')

    mocker.patch.object(runtime, 'start', side_effect=stopped_meanwhile)
    run_id = make_run(db, campaign)
    execute(ex, db, run_id)
    with db.session() as s:
        assert s.get_one(Run, run_id).status is RunStatus.RUNNING, 'left for the stop to fail'
        assert s.scalar(select(func.count()).select_from(AgentSession).where(AgentSession.run_id == run_id)) == 1


@pytest.mark.parametrize('role', ['implementer', 'worker'])
def test_a_finding_without_a_commit_is_a_result(
    role: str, db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A run that posts a finding and commits nothing succeeds, and its idea is not retried.

    Some plans, such as an audit, produce a finding and no code. A 0.1.0 agent that outlives the upgrade signs its
    finding ``worker#<id>``, as its brief told it, and the finding still counts.
    """
    run_id = make_run(db, campaign, max_runs=2)
    with db.session() as s:
        idea_id, agent_session_id = run_row(db, run_id).idea_id, s.scalars(select(AgentSession.id)).one()
    finding = f"INSERT INTO comments (target_kind, target_id, author, kind, body) VALUES ('idea', {idea_id}, '{role}#{agent_session_id}', 'finding', 'arm B: no effect')"
    write_toml(Path(campaign.path), f'#!/bin/sh\n{SQL_HINT} "{finding}"\n')
    reload_campaign(db, campaign)
    execute(controller(db, runtime, tmp_path), db, run_id)
    run = run_row(db, run_id)
    assert (run.status, run.error) == (RunStatus.SUCCEEDED, None)
    with db.session() as s:
        assert s.get_one(Idea, idea_id).status is IdeaStatus.DONE, 'not retried'


TIMEOUT = 'implementer timeout after 1s'


@pytest.mark.parametrize(
    ('facts', 'expected'),
    [
        # one case per case of run_outcome, in order
        pytest.param(
            RunFacts(finding=True, asked=CommentId(7)), (RunEnd.AWAITING_ANSWER, 'comment#7'), id='finding-asked'
        ),
        pytest.param(RunFacts(finding=True), (RunEnd.FINISHED, None), id='finding'),
        pytest.param(RunFacts(cancelled=True), (RunEnd.CANCELLED, None), id='cancel'),
        pytest.param(RunFacts(ended=True, commits=1), (RunEnd.CAMPAIGN_STOPPED, None), id='stop'),
        pytest.param(RunFacts(timed_out=True, error=TIMEOUT), (RunEnd.TIMED_OUT, TIMEOUT), id='timeout'),
        pytest.param(
            RunFacts(error='implementer exit 1', commits=1), (RunEnd.FAILED, 'implementer exit 1'), id='error'
        ),
        pytest.param(RunFacts(asked=CommentId(7)), (RunEnd.AWAITING_ANSWER, 'comment#7'), id='asked'),
        pytest.param(RunFacts(), (RunEnd.FAILED, NO_CHANGES), id='nothing'),
        pytest.param(RunFacts(commits=1, recovered_only=True), (RunEnd.FAILED, INTERRUPTED), id='recovered-only'),
        pytest.param(RunFacts(commits=2), (RunEnd.FINISHED, None), id='commits'),
        # the overlaps that fix the order: each pair of neighbouring rows, and the #116 cases
        pytest.param(
            RunFacts(finding=True, asked=CommentId(7), timed_out=True, error=TIMEOUT),
            (RunEnd.AWAITING_ANSWER, 'comment#7'),
            id='finding-asked-timeout',
        ),
        pytest.param(RunFacts(finding=True, ended=True), (RunEnd.FINISHED, None), id='finding-stop'),
        pytest.param(RunFacts(finding=True, cancelled=True), (RunEnd.FINISHED, None), id='finding-cancel'),
        pytest.param(
            RunFacts(finding=True, timed_out=True, error=TIMEOUT, commits=1),
            (RunEnd.FINISHED, None),
            id='finding-timeout',
        ),
        pytest.param(RunFacts(finding=True, error='implementer exit 1'), (RunEnd.FINISHED, None), id='finding-error'),
        pytest.param(
            RunFacts(finding=True, commits=1, recovered_only=True),
            (RunEnd.FINISHED, None),
            id='finding-recovered-only',
        ),
        pytest.param(
            RunFacts(ended=True, cancelled=True),
            (RunEnd.CANCELLED, None),
            id='cancel-stop',
        ),
        pytest.param(
            RunFacts(ended=True, timed_out=True, error=TIMEOUT),
            (RunEnd.CAMPAIGN_STOPPED, None),
            id='stop-timeout',
        ),
        pytest.param(
            RunFacts(cancelled=True, timed_out=True, error=TIMEOUT),
            (RunEnd.CANCELLED, None),
            id='cancel-timeout',
        ),
        pytest.param(RunFacts(cancelled=True, commits=1), (RunEnd.CANCELLED, None), id='cancel-commits'),
        pytest.param(
            RunFacts(timed_out=True, error=TIMEOUT, asked=CommentId(7), commits=1),
            (RunEnd.TIMED_OUT, TIMEOUT),
            id='timeout-asked',
        ),
        pytest.param(
            RunFacts(error='implementer exit 1', asked=CommentId(7)),
            (RunEnd.FAILED, 'implementer exit 1'),
            id='error-asked',
        ),
        pytest.param(
            RunFacts(asked=CommentId(7), commits=1), (RunEnd.AWAITING_ANSWER, 'comment#7'), id='asked-commits'
        ),
    ],
)
def test_how_a_run_ended_is_the_first_row_that_holds(facts: RunFacts, expected: tuple[RunEnd, str | None]) -> None:
    assert run_outcome(facts) == expected


@pytest.mark.parametrize('ended', [NotStarted('no server'), Lost('worker lost')], ids=['not-started', 'lost'])
def test_a_failed_start_or_a_lost_job_while_the_daemon_stops_leaves_the_run_to_the_stop(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture, ended: Ended
) -> None:
    """The stop fails the open runs itself, so the run is neither relaunched nor closed as lost."""
    ex = controller(db, runtime, tmp_path)

    def stopped_meanwhile(*args: object, **kwargs: object) -> Ended:
        ex.launcher.cancel()
        return ended

    mocker.patch.object(ex.launcher, 'launch', side_effect=stopped_meanwhile)
    run_id = make_run(db, campaign)
    execute(ex, db, run_id)
    assert run_row(db, run_id).status is RunStatus.RUNNING, 'stop() writes the row, not the run thread'


@pytest.mark.parametrize('timed_out', [True, False])
def test_an_implementer_that_left_its_finding_has_finished_its_run_however_its_process_then_ends(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture, timed_out: bool
) -> None:
    """An implementer that committed and left its finding, then timed out or exited non-zero, finished its idea.

    The finding is its report that it is done, as on a daemon stop (``Scheduler._hand_off``): the run succeeds and the
    idea is ``done``, so no second run redoes the work. A job that it forgot keeps the run open to the timeout. The
    agent session row keeps how its process ended.
    """
    run_id = make_run(db, campaign, max_runs=2)
    with db.session() as s:
        idea_id, agent_session_id = run_row(db, run_id).idea_id, s.scalars(select(AgentSession.id)).one()
    finding = (
        'INSERT INTO comments (target_kind, target_id, author, kind, body) VALUES'
        f" ('idea', {idea_id}, 'implementer#{agent_session_id}', 'finding', 'DONE: added a line')"
    )
    end = 'touch ../found\nsleep 30 &\nwait' if timed_out else 'exit 1'
    body = f'#!/bin/sh\necho line >> solve.txt\ngit commit -qam "a line"\n{SQL_HINT} "{finding}"\n{end}\n'
    write_toml(Path(campaign.path), body)
    reload_campaign(db, campaign)
    wait = runtime.wait

    def time_out_after_the_finding(handle: Handle, timeout: float) -> AgentExit:
        wait_until((tmp_path / 'runs' / str(run_id) / 'found').exists)
        return wait(handle, 0.5)

    if timed_out:  # a loaded host can take longer than any short timeout to write the finding
        mocker.patch.object(runtime, 'wait', side_effect=time_out_after_the_finding)
    execute(controller(db, runtime, tmp_path), db, run_id)
    run = run_row(db, run_id)
    assert (run.status, run.error) == (RunStatus.SUCCEEDED, None)
    with db.session() as s:
        assert s.get_one(Idea, idea_id).status is IdeaStatus.DONE, 'not retried'
        assert s.get_one(AgentSession, agent_session_id).error == (
            'agent timeout after 0.5s' if timed_out else 'agent exit 1'
        )
        assert [c.verdict for c in s.scalars(select(Candidate).where(Candidate.run_id == run_id))] == [Verdict.KEEP]


@pytest.mark.parametrize('role', ['implementer', 'worker'])
@pytest.mark.parametrize('reported', [True, False])
def test_an_implementer_that_stops_on_its_open_escalation_is_blocked_and_its_idea_waits_for_the_answer(
    role: str, db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, reported: bool
) -> None:
    """An implementer that asks for a decision and stops before it comes did not succeed, and its idea is not done.

    The run fails as blocked, which no incident sweep investigates, and the idea is parked with a note that names the
    escalation. A retry would stop on the same question, and ``done`` would release the ideas that wait for it. A
    finding or a commit does not make it a success.
    """
    run_id = make_run(db, campaign, max_runs=2)
    with db.session() as s:
        idea_id, agent_session_id = run_row(db, run_id).idea_id, s.scalars(select(AgentSession.id)).one()
    ask = (
        'INSERT INTO comments (target_kind, target_id, author, kind, priority, body) VALUES'
        f" ('idea', {idea_id}, '{role}#{agent_session_id}', 'escalation', 1, 'run the audit that I cannot run')"
    )
    report = (
        'INSERT INTO comments (target_kind, target_id, author, kind, body) VALUES'
        f" ('idea', {idea_id}, '{role}#{agent_session_id}', 'finding', 'blocked on the audit')"
    )
    write_toml(
        Path(campaign.path),
        (
            f'#!/bin/sh\n{SQL_HINT} "{ask}"\n'
            + (f'{SQL_HINT} "{report}"\n' if reported else '')
            + 'echo line >> solve.txt\ngit commit -qam "a line"\n'
        ),
    )
    reload_campaign(db, campaign)
    execute(controller(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        escalation = s.scalars(select(Comment.id).where(Comment.kind == 'escalation')).one()
        run = s.get_one(Run, run_id)
        assert (run.status, run.end, run.error) == (RunStatus.FAILED, RunEnd.AWAITING_ANSWER, f'comment#{escalation}')
        assert s.get_one(Idea, idea_id).status is IdeaStatus.PARKED
        note = s.scalars(select(Comment).where(Comment.author == 'daemon', Comment.target_id == idea_id)).one()
        assert f'escalation comment#{escalation}' in note.body and 'set the idea ready' in note.body
        assert [c.verdict for c in s.scalars(select(Candidate).where(Candidate.run_id == run_id))] == [Verdict.KEEP], (
            'its commits are scored as any run'
        )


def test_an_escalation_answered_before_the_implementer_stops_does_not_block_its_run(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    run_id = make_run(db, campaign, max_runs=2)
    with db.session() as s:
        idea_id, agent_session_id = run_row(db, run_id).idea_id, s.scalars(select(AgentSession.id)).one()
        s.add(
            Comment(
                target=TargetRef(TargetKind.RUN, run_id),
                author=f'implementer#{agent_session_id}',
                kind='escalation',
                priority=1,
                body='may I use the cache?',
                acked_at=utcnow(),
                acked_by='orchestrator',
            )
        )
        s.add(
            Comment(
                target=TargetRef(TargetKind.IDEA, idea_id),
                author='watchdog#9',
                kind='escalation',
                priority=1,
                body='not the implementer',
            )
        )
        s.commit()
    execute(controller(db, runtime, tmp_path), db, run_id)
    assert (run_row(db, run_id).status, run_row(db, run_id).error) == (RunStatus.SUCCEEDED, None)
    with db.session() as s:
        assert s.get_one(Idea, idea_id).status is IdeaStatus.DONE


def test_uncommitted_changes_without_a_finding_are_scored_but_not_an_attempt_that_finished(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """An implementer that leaves uncommitted changes and no finding did not finish its idea.

    The daemon commits and scores the changes, but the run fails and the idea is ready again. A finding on the run
    makes the same run a success.
    """
    write_toml(Path(campaign.path), HARNESS_FORGETS_COMMIT)
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign, max_runs=2)
    execute(controller(db, runtime, tmp_path), db, run_id)
    run = run_row(db, run_id)
    assert (run.status, run.error) == (RunStatus.FAILED, INTERRUPTED)
    [(summary, verdict, *_)] = scored(db, run_id)
    assert (summary, verdict) == (UNCOMMITTED_SUMMARY, Verdict.RECOVERED), 'what it left is kept, not judged'
    with db.session() as s:
        assert s.get_one(Idea, run.idea_id).status is IdeaStatus.READY

    run_id = make_run(db, campaign, title='reported', max_runs=2)
    with db.session() as s:
        idea_id, agent_session_id = (
            run_row(db, run_id).idea_id,
            s.scalars(select(AgentSession.id).where(AgentSession.run_id == run_id)).one(),
        )
        s.add(
            Comment(
                target=TargetRef(TargetKind.RUN, run_id),
                author=f'implementer#{agent_session_id}',
                kind='finding',
                body='x',
            )
        )
        s.add(Comment(target=TargetRef(TargetKind.IDEA, idea_id), author='watchdog#9', kind='finding', body='y'))
        s.commit()
    write_toml(Path(campaign.path), '#!/bin/sh\necho other >> solve.txt\n')
    reload_campaign(db, campaign)
    execute(controller(db, runtime, tmp_path), db, run_id)
    assert run_row(db, run_id).status is RunStatus.SUCCEEDED, 'it reported where it stands'


def test_a_cancelled_executor_leaves_the_bookkeeping_to_the_scheduler(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
) -> None:
    ex = controller(db, runtime, tmp_path, tracer)
    queued = make_run(db, campaign)
    ex.launcher.cancel()
    execute(ex, db, queued)
    assert run_row(db, queued).status is RunStatus.QUEUED, 'not even started'
    assert dict(span_named(spans, 'piloop.run').attributes or {}) == {
        'run': queued,
        'status': 'failed',
        'end': 'daemon_stopped',
    }, 'what the stop writes on the row'


def test_an_implementer_dying_after_cancel_is_not_a_run_failure(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    ex = controller(db, runtime, tmp_path)

    def tab_closed(handle: Handle, timeout: float) -> AgentExit:
        ex.launcher.cancel()
        raise RuntimeError('agent_not_found')

    mocker.patch.object(runtime, 'wait', side_effect=tab_closed)
    run_id = make_run(db, campaign)
    execute(ex, db, run_id)
    run = run_row(db, run_id)
    assert (run.status, run.error) == (RunStatus.RUNNING, None), 'stop() writes the row, not the dying thread'
    workers_of(ex).worker.join(10)  # it removes the worktree once its report is out
    assert not (tmp_path / 'runs' / str(run_id) / 'worktree').exists()


def test_a_stop_between_the_implementers_end_and_the_runs_close_leaves_the_row_to_the_scheduler(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    ex = controller(db, runtime, tmp_path)

    def stopped_meanwhile(*args: object) -> list[str]:
        ex.launcher.cancel()
        raise RuntimeError('worktree pulled away')

    mocker.patch.object(RunController, '_commits', side_effect=stopped_meanwhile)
    run_id = make_run(db, campaign)
    execute(ex, db, run_id)
    run = run_row(db, run_id)
    assert (run.status, run.error) == (RunStatus.RUNNING, None), 'stop() writes the row, not the dying thread'
    with db.session() as s:
        assert s.get_one(Idea, run.idea_id).status is IdeaStatus.SCHEDULED, 'and returns the idea'


def test_an_evaluator_spawned_after_cancel_is_killed_at_once(
    db: Database, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    ex = controller(db, runtime, tmp_path)
    ex.stop(0)
    proc = subprocess.Popen(['sleep', '30'], start_new_session=True)
    worker = workers_of(ex).worker
    worker._register(1, None, evaluation=True)  # the evaluation's job, before its evaluator starts
    jobs._track(_Control(worker, 1), proc)  # what run_evaluate calls right after Popen: the stop may have won meanwhile
    assert proc.wait(timeout=2) == -9


def test_a_commit_that_reverts_to_a_known_tree_is_that_candidate_and_is_not_measured_again(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
) -> None:
    write_toml(Path(campaign.path), HARNESS_TRY_AND_REVERT)
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign)
    execute(controller(db, runtime, tmp_path, tracer), db, run_id)
    [tried] = candidates(db, run_id)
    assert (tried.summary, tried.verdict, tried.metrics['score']) == ('try', Verdict.KEEP, 2.0)
    base = baseline(db, campaign.id)
    with db.session() as s:
        assert s.scalar(select(func.count()).select_from(Candidate)) == 2, 'the revert is the baseline, not a third row'
        assert len(s.get_one(Candidate, base.id).measurements) == 1, 'and the baseline was not measured again'
        kinds = [ev.kind for ev in s.scalars(select(Event)).all()]
    assert kinds.count('measurement.recorded') == 1 and 'candidate.judged' in kinds
    known = [sp for sp in spans.get_finished_spans() if sp.name == 'piloop.candidate' and sp.attributes]
    assert [sp.attributes.get('status') for sp in known if sp.attributes] == [None, 'known']
    repo = Repo(campaign.path)
    assert len([r for r in repo.refs if r.path.startswith('refs/piloop/')]) == 2, 'both commits are still pinned'


def test_a_changed_evaluator_measures_again_and_old_numbers_do_not_count(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    run_id = make_run(db, campaign)
    execute(controller(db, runtime, tmp_path), db, run_id)
    [added] = candidates(db, run_id)
    assert added.metrics['score'] == 2.0
    doubled = ['sh', '-c', 'echo METRIC score=$(( 2 * $(wc -l < solve.txt) )); echo METRIC size=1']
    write_toml(Path(campaign.path), HARNESS_TRY_AND_REVERT, evaluate=doubled)
    with db.session() as s:
        proj = s.get_one(Campaign, campaign.id)
        before = proj.evaluator_id
        sync(s, proj)
        s.commit()
        assert proj.evaluator_id != before, 'a new evaluate command is a new evaluator'
        trunk = head(s, proj)
        assert trunk is not None and trunk.id == added.id, (
            'nothing is measured under the new evaluator yet, and the head stays the latest trunk keep'
        )
    second = make_run(db, campaign, title='again')
    with db.session() as s:
        s.get_one(Run, second).base_candidate_id = added.id
        s.commit()
    execute(controller(db, runtime, tmp_path), db, second)
    with db.session() as s:
        proj = s.get_one(Campaign, campaign.id)
        tried = s.scalars(select(Candidate).where(Candidate.run_id == second)).one()
        assert [m.metrics['score'] for m in tried.measurements] == [6.0], 'measured under the doubled evaluator'
        base = s.get_one(Candidate, added.id)
        assert [m.metrics['score'] for m in base.measurements] == [2.0, 4.0], (
            'the revert resolved to the base, which the new evaluator had not seen: measured once more, under it'
        )
        assert base.verdict is Verdict.KEEP, 'a known candidate keeps its verdict; the revert says nothing new about it'
        assert (tried.verdict, tried.verdict_by) == (Verdict.KEEP, DAEMON), 'an unmeasured bar is beaten by any score'
        trunk = head(s, proj)
        assert trunk is not None and trunk.id == tried.id


def test_a_config_change_during_a_run_scores_it_under_the_new_evaluator(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``piloop init`` during a long run: the run finishing must not point the campaign back at the old evaluator."""
    gate = Gate()
    monkeypatch.setattr(runtime, 'start', gate.wrap(runtime.start))
    run_id = make_run(db, campaign)
    thread = threading.Thread(target=execute, args=(controller(db, runtime, tmp_path), db, run_id))
    thread.start()
    assert gate.entered.wait(10), 'the implementer is about to start; the run holds the old config'
    doubled = ['sh', '-c', 'echo METRIC score=$(( 2 * $(wc -l < solve.txt) )); echo METRIC size=1']
    write_toml(Path(campaign.path), HARNESS_APPEND, evaluate=doubled)
    with db.session() as s:
        proj = s.get_one(Campaign, campaign.id)
        sync(s, proj)
        s.commit()
        new_evaluator = proj.evaluator_id
    gate.release.set()
    thread.join(30)
    with db.session() as s:
        assert s.get_one(Campaign, campaign.id).evaluator_id == new_evaluator
        [added] = candidates(db, run_id)
        assert [m.evaluator_id for m in s.get_one(Candidate, added.id).measurements] == [new_evaluator]
        assert added.metrics['score'] == 4.0, 'measured under the doubled evaluator'


SPLIT_UNITS = [
    'sh',
    '-c',
    'L=$(wc -l < solve.txt); echo METRIC score=$L; echo METRIC size=$(wc -c < solve.txt);'
    ' [ -z "$PILOOP_UNITS_FILE" ] || printf \'{"score": {"a": %d, "b": %d}}\' $((2*L-1)) $((1-L)) > "$PILOOP_UNITS_FILE"',
]
"""score = lines, split over two units so one more line is +2 on ``a`` and -1 on ``b``: up, but not significantly."""


def set_config(db: Database, campaign: Campaign, **overrides: object) -> None:
    with db.session() as s:
        s.get_one(Campaign, campaign.id).config = load_campaign_config(Path(campaign.path), **overrides).model_dump(
            mode='json'
        )
        s.commit()


def test_per_unit_results_decide_the_verdict_and_live_outside_the_database(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    units_dir = tmp_path / 'units'
    with db.session() as s:
        proj = s.get_one(Campaign, campaign.id)
        cfg = load_campaign_config(Path(campaign.path), evaluate=SPLIT_UNITS)
        proj.config = cfg.model_dump(mode='json')
        bar = Measurement(
            candidate_id=baseline(db, campaign.id).id,
            evaluator_id=current_evaluator(s, proj, cfg).id,
            metrics={'score': 1.0, 'size': 5.0},
        )
        s.add(bar)
        s.commit()
        base_measurement = bar.id
    save_units(units_dir, base_measurement, {'score': {'a': 1.0, 'b': 0.0}})
    run_id = make_run(db, campaign)
    execute(controller(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        c = s.scalars(select(Candidate).where(Candidate.run_id == run_id)).one()
        [m] = c.measurements
        assert (c.verdict, c.verdict_by) == (Verdict.INTERESTING, DAEMON)
        assert c.verdict_reason == snapshot(
            'against candidate#1: score 2 vs 1 (+1; 95% worst case -2 over 2 units): the gain is not significant'
        )
        [(_, judged)] = in_order(s, CandidateJudged)
        assert judged.reason == c.verdict_reason
        assert m.metrics.keys() == {'score', 'size'}, 'per-unit results are not a column any agent can SELECT'
    assert json.loads((units_dir / f'{m.id}.json').read_text()) == {'score': {'a': 3, 'b': -1}}
    assert sorted(p.name for p in units_dir.iterdir()) == [f'{base_measurement}.json', f'{m.id}.json'], (
        'no pending file is left behind'
    )


def test_without_per_unit_results_the_aggregate_decides_and_the_reason_says_so(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    run_id = make_run(db, campaign)
    execute(controller(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        c = s.scalars(select(Candidate).where(Candidate.run_id == run_id)).one()
        assert (c.verdict, c.verdict_reason) == snapshot(
            (Verdict.KEEP, 'against candidate#1: score 2 vs 1 (+1; aggregate: no per-unit results)')
        )
    assert list((tmp_path / 'units').glob('*')) == []


def test_guardrails_are_enforced_by_the_daemon_even_after_the_campaign_stopped(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """The daemon enforces a guardrail floor, also on a run still in flight after the campaign stopped."""
    metrics = [
        {'name': 'score', 'direction': 'higher', 'role': 'primary'},
        {'name': 'size', 'direction': 'lower', 'role': 'guardrail', 'margin': 100, 'floor': 8},
    ]
    set_config(db, campaign, metrics=metrics)
    run_id = make_run(db, campaign)
    with db.session() as s:
        proj = s.get_one(Campaign, campaign.id)
        proj.status, proj.stopped_reason = CampaignStatus.STOPPED, 'max_runs: 1'
        s.commit()
    execute(controller(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        c = s.scalars(select(Candidate).where(Candidate.run_id == run_id)).one()
        assert (c.verdict, c.verdict_reason) == snapshot((Verdict.DISCARD, 'floor: size 10 past its floor 8'))


def test_units_of_a_candidate_are_the_mean_per_unit_over_its_measurements(
    db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    with db.session() as s:
        evaluator_id = s.get_one(Campaign, campaign.id).evaluator_id
        assert evaluator_id is not None
        c = Candidate(campaign_id=campaign.id, tree_sha='t', commit_sha='t')
        s.add(c)
        s.flush()
        rows = [Measurement(candidate_id=c.id, evaluator_id=evaluator_id, metrics={'score': 1.0}) for _ in range(4)]
        rows[3].error = 'crashed'
        s.add_all(rows)
        s.flush()
        save_units(tmp_path, rows[0].id, {'score': {'a': 1.0, 'b': 0.0}})
        save_units(tmp_path, rows[1].id, {'score': {'a': 3.0, 'b': 0.0}})
        save_units(tmp_path, rows[3].id, {'score': {'a': 100.0, 'b': 0.0}})
        (tmp_path / f'{rows[2].id}.json').write_text('{"score": {"a": 50}}')
        save_units(tmp_path, rows[2].id, {})
        assert units(c, evaluator_id, tmp_path) == {'score': {'a': 2.0, 'b': 0.0}}, (
            'a crashed measurement does not count, and one without units leaves no stale file behind'
        )


def test_pairs_are_the_samples_of_a_candidate_next_to_the_successful_samples_of_that_bar(
    db: Database, campaign: Campaign
) -> None:
    with db.session() as s:
        evaluator_id = s.get_one(Campaign, campaign.id).evaluator_id
        assert evaluator_id is not None
        bar, other, c = (Candidate(campaign_id=campaign.id, tree_sha=t, commit_sha=t) for t in 'boc')
        s.add_all([bar, other, c])
        s.flush()

        def sample(
            on: Candidate, score: float, pair: Measurement | None = None, error: str | None = None
        ) -> Measurement:
            m = Measurement(candidate=on, evaluator_id=evaluator_id, metrics={'score': score}, pair=pair, error=error)
            s.add(m)
            return m

        sample(c, 5.0, sample(bar, 1.0))
        sample(c, 6.0, sample(bar, 2.0, error='crashed after its METRIC line'))
        sample(c, 7.0, sample(other, 3.0))
        sample(c, 8.0)
        sample(c, 9.0, sample(bar, 4.0))
        s.flush()
        assert ledger_pairs(c, bar, evaluator_id) == ({'score': [5.0, 9.0]}, {'score': [1.0, 4.0]})


HARNESS_SCAFFOLD_THEN_WORK = (
    '#!/bin/sh\necho a >> solve.txt\ngit commit -qam scaffold -m "piloop-eval: SKIP"\n'
    'echo b >> solve.txt\ngit commit -qam work\n'
)


def scored(db: Database, run_id: RunId) -> list[tuple[str | None, Verdict, str | None, int, int | None]]:
    """``(summary, verdict, reason, measurements, parent)`` of each candidate the run produced, in order."""
    with db.session() as s:
        rows = s.scalars(select(Candidate).where(Candidate.run_id == run_id).order_by(Candidate.id)).all()
        return [(c.summary, c.verdict, c.verdict_reason, len(c.measurements), c.parent_id) for c in rows]


def test_a_commit_marked_skip_is_a_candidate_without_an_evaluation_and_moves_no_bar(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    write_toml(Path(campaign.path), HARNESS_SCAFFOLD_THEN_WORK)
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign)
    execute(controller(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        first = s.scalars(select(Candidate).where(Candidate.run_id == run_id).order_by(Candidate.id)).first()
        assert first is not None
        skip_id = first.id
    assert scored(db, run_id) == [
        ('scaffold', Verdict.UNMEASURED, SKIPPED, 0, 1),
        ('work', Verdict.KEEP, IsStr(regex=r'against candidate#1: score 3 vs 1 .*'), 1, skip_id),
    ], 'the next commit descends from the scaffold and is judged against the bar, the baseline'
    with db.session() as s:
        judged = in_order(s, CandidateJudged)[0][1]
        assert judged == CandidateJudged(verdict=Verdict.UNMEASURED, score=None, reason=SKIPPED)


def test_a_skipped_tree_reached_again_without_the_trailer_is_measured_and_judged(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A revert to a skipped tree is measured and judged against the keep before it.

    The run makes a scaffold commit marked skip, then a work commit that keeps, then reverts to the scaffold's tree.
    """
    write_toml(Path(campaign.path), HARNESS_SCAFFOLD_THEN_WORK + 'git revert --no-edit HEAD >/dev/null\n')
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign)
    execute(controller(db, runtime, tmp_path), db, run_id)
    [scaffold, work] = scored(db, run_id)
    assert scaffold[1:4] == (Verdict.DISCARD, IsStr(regex=r'against candidate#\d+: score 2 vs 3 .*'), 1)
    assert work[1] is Verdict.KEEP


def test_a_skip_commit_on_a_known_tree_keeps_its_verdict_unmeasured(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    harness = HARNESS_TRY_AND_REVERT + 'git commit -q --amend -m "back" -m "Piloop-Eval: skip"\n'
    write_toml(Path(campaign.path), harness)
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign)
    execute(controller(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        base = s.get_one(Candidate, baseline(db, campaign.id).id)
        assert (base.verdict, base.verdict_reason, len(base.measurements)) == (Verdict.KEEP, None, 1)


def test_dirt_after_a_skip_commit_is_committed_on_its_own_and_evaluated(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    harness = '#!/bin/sh\necho a >> solve.txt\ngit commit -qam scaffold -m "Piloop-Eval: skip"\necho b >> solve.txt\n'
    write_toml(Path(campaign.path), harness)
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign)
    execute(controller(db, runtime, tmp_path), db, run_id)
    assert [(c[0], c[1], c[3]) for c in scored(db, run_id)] == [
        ('scaffold', Verdict.UNMEASURED, 0),
        (UNCOMMITTED_SUMMARY, Verdict.RECOVERED, 0),
    ], 'the work after a skip commit is recovered on its own: never amended into the commit'


def test_a_commit_that_moves_no_deciding_metric_says_no_op(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    write_toml(Path(campaign.path), '#!/bin/sh\necho x > notes.txt\ngit add notes.txt\ngit commit -qm notes\n')
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign)
    execute(controller(db, runtime, tmp_path), db, run_id)
    [(_, verdict, reason, _, _)] = scored(db, run_id)
    assert (verdict, reason) == (
        Verdict.DISCARD,
        snapshot(
            'no-op: score identical to parent candidate#1; against candidate#1: score 1 vs 1 (+0; aggregate: no per-unit results)'
        ),
    )


def import_empty_solution(db: Database, campaign: Campaign) -> CandidateId:
    """Import an empty solution from another branch as a reference that scores 0, below the baseline's 1."""
    repo = Repo(campaign.path)
    repo.git.checkout('-qb', 'elsewhere')
    (Path(campaign.path) / 'solve.txt').write_text('')
    repo.git.commit('-qam', 'an empty solution')
    repo.git.checkout('-q', 'main')
    with db.session() as s:
        proj = s.get_one(Campaign, campaign.id)
        cfg = load_campaign_config(Path(campaign.path))
        reference = import_reference(
            s, proj, cfg, GitRepository(repo), 'elsewhere', summary=None, actor=System.ORCHESTRATOR
        ).id
        s.commit()
    return reference


def test_a_run_whose_history_holds_the_head_is_judged_against_the_head_itself(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A run whose base descends from the head has nothing to merge, so a keep must beat the head."""
    reference = import_empty_solution(db, campaign)
    write_toml(Path(campaign.path), HARNESS_TWO_COMMITS)
    reload_campaign(db, campaign)
    ex = controller(db, runtime, tmp_path)
    ex.remeasure(reference)
    run_id = make_run(db, campaign)
    with db.session() as s:
        s.get_one(Run, run_id).base_candidate_id = reference
        s.commit()
    execute(ex, db, run_id)
    [(_, first, first_reason, _, parent), (_, second, second_reason, _, _)] = scored(db, run_id)
    assert parent == reference
    assert (first, first_reason) == (
        Verdict.DISCARD,
        snapshot(
            'the head candidate#1 is in its history: against candidate#1: score 1 vs 1 (+0; aggregate: no per-unit results)'
        ),
    ), 'level with the head: a gain over its own base only'
    assert (second, second_reason) == (
        Verdict.KEEP,
        snapshot(
            'the head candidate#1 is in its history: against candidate#1: score 2 vs 1 (+1; aggregate: no per-unit results)'
        ),
    ), 'above the head: kept, the new head'
    with db.session() as s:
        assert s.get_one(Candidate, reference).verdict is Verdict.REFERENCE, 'the starting point is never judged'
        trunk = head(s, s.get_one(Campaign, campaign.id))
        assert trunk is not None and trunk.summary == 'second'
        assert not s.scalars(select(Event).where(has(Rebased))).all(), 'nothing was merged'


def test_a_branch_run_is_judged_against_its_own_base_and_its_keep_never_moves_the_head(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A branch run is judged against its own base, not the head, and its keeps never move the head."""
    reference = import_empty_solution(db, campaign)
    write_toml(Path(campaign.path), HARNESS_TWO_COMMITS)
    reload_campaign(db, campaign)
    ex = controller(db, runtime, tmp_path)
    ex.remeasure(reference)
    run_id = make_run(db, campaign)
    with db.session() as s:
        run = s.get_one(Run, run_id)
        run.base_candidate_id, run.branch = reference, True
        s.commit()
    execute(ex, db, run_id)
    [(_, first, first_reason, _, _), (_, second, second_reason, _, _)] = scored(db, run_id)
    assert (first, first_reason) == (
        Verdict.KEEP,
        snapshot(
            'against candidate#2: score 1 vs 0 (+1; aggregate: no per-unit results); a branch keep, not above the head candidate#1 (score 1)'
        ),
    ), 'a gain over its own base keeps on the branch'
    assert (second, second_reason) == (
        Verdict.KEEP,
        snapshot(
            'against candidate#3: score 2 vs 1 (+1; aggregate: no per-unit results); a branch keep, above the head candidate#1 (score 1)'
        ),
    ), 'the bar moved to the branch keep; above the head'
    with db.session() as s:
        proj = s.get_one(Campaign, campaign.id)
        trunk = head(s, proj)
        assert trunk is not None and trunk.id == 1, 'a branch keep never moves the head, however it scores'
        assert [(b.root.id, b.branched, b.latest.summary, b.keeps) for b in branches(s, proj)] == [
            (1, False, 'baseline', 1),
            (reference, True, 'second', 2),
        ], "two lineages: the trunk first, then the reference's branch"


def keep_elsewhere(
    db: Database,
    campaign: Campaign,
    summary: str,
    files: dict[str, str],
    metrics: dict[str, float],
    *,
    on: int = 1,
    verdict: Verdict = Verdict.KEEP,
    run_id: RunId | None = None,
) -> CandidateId:
    """Add a trunk keep made outside any run.

    The keep writes ``files`` on a commit off candidate ``on`` and is measured as ``metrics``. To a run that started
    before it, it looks like another run's keep: the head moved on. With another ``verdict``, it is a candidate that
    the orchestrator judged. With ``run_id``, it is that run's candidate.
    """
    repo = Repo(campaign.path)
    with db.session() as s:
        parent = s.get_one(Candidate, on)
        repo.git.checkout('-q', '--detach', parent.commit_sha)
        for name, text in files.items():
            (Path(campaign.path) / name).write_text(text)
        repo.git.add('-A')
        repo.git.commit('-qm', summary)
        sha = repo.head.commit.hexsha
        repo.git.checkout('-q', 'main')
        git_pin(repo, f'refs/piloop/demo/elsewhere/{summary.replace(" ", "-")}', sha)
        candidate, _ = resolve(s, campaign.id, GitRepository(repo).version(sha), parent=parent, run_id=run_id)
        candidate.verdict, candidate.verdict_by = verdict, 'orchestrator'
        evaluator_id = s.get_one(Campaign, campaign.id).evaluator_id
        assert evaluator_id is not None
        s.add(Measurement(candidate_id=candidate.id, evaluator_id=evaluator_id, metrics=metrics))
        s.commit()
        return candidate.id


def test_a_trunk_run_that_merges_a_branch_keep_is_judged_against_the_head_not_the_branch(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A trunk run that merges a branch keep onto the head must beat the head.

    A branch's numbers are never a trunk run's bar. Judged against the branch keep, the merge was a no-op discard,
    though it beats the head. The merge's parent is the head it merged onto, not the branch.
    """
    with db.session() as s:
        idea = Idea(track='score', campaign_id=campaign.id, title='a branch', hypothesis='h', created_by='t')
        s.add(idea)
        s.flush()
        branch_run = Run(idea_id=idea.id, attempt=1, base_candidate_id=1, harness='h', model='m', branch=True)
        s.add(branch_run)
        s.commit()
        branch_run_id = branch_run.id
    kept = keep_elsewhere(
        db, campaign, 'branch keep', {'solve.txt': 'base\nb\nc\n'}, {'score': 3.0, 'size': 9.0}, run_id=branch_run_id
    )
    trunk = keep_elsewhere(db, campaign, 'trunk keep', {'other.txt': 'x\n'}, {'score': 1.0, 'size': 5.0})
    with db.session() as s:
        kept_sha = s.get_one(Candidate, kept).commit_sha
    write_toml(Path(campaign.path), f'#!/bin/sh\ngit merge -q --no-edit {kept_sha}\n')
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign)
    with db.session() as s:
        s.get_one(Run, run_id).base_candidate_id = trunk
        s.commit()
    execute(controller(db, runtime, tmp_path), db, run_id)
    [(_, verdict, reason, _, parent)] = scored(db, run_id)
    assert (verdict, reason, parent) == (
        Verdict.KEEP,
        f'against candidate#{trunk}: score 3 vs 1 (+2; aggregate: no per-unit results)',
        trunk,
    ), 'judged against the head it merged onto'
    with db.session() as s:
        new_head = head(s, s.get_one(Campaign, campaign.id))
        assert new_head is not None and new_head.run_id == run_id, 'the merge is the new head'
        assert s.get_one(Candidate, kept).verdict is Verdict.KEEP, 'the branch keep keeps its verdict'


def test_a_run_pinned_to_a_non_keep_is_judged_against_its_nearest_keep_on_every_metric(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A run pinned to a candidate that is not a keep is judged against that candidate's nearest keep.

    A candidate that is not a keep is not a bar. The run starts from its tree and is judged as a trunk run against
    the head it grew from, so its keep moves the head.
    """
    lead = keep_elsewhere(
        db, campaign, 'lead', {'solve.txt': 'base\nx\n'}, {'score': 9.0, 'size': 1.0}, verdict=Verdict.INTERESTING
    )
    run_id = make_run(db, campaign)
    with db.session() as s:
        s.get_one(Run, run_id).base_candidate_id = lead
        s.commit()
    ex = controller(db, runtime, tmp_path)
    execute(ex, db, run_id)
    [(_, verdict, reason, _, parent)] = scored(db, run_id)
    assert (verdict, reason, parent) == (
        Verdict.KEEP,
        'against candidate#1: score 3 vs 1 (+2; aggregate: no per-unit results)',
        lead,
    ), "the lead's 9 is no bar: the head's 1 is"
    with db.session() as s:
        trunk = head(s, s.get_one(Campaign, campaign.id))
        assert trunk is not None and trunk.parent_id == lead, 'a trunk run: its keep is the new head'
        brief = s.get_one(Run, run_id).brief or ''
    assert f'- candidate#{lead} is no keep: your commits are judged against candidate#1, its nearest keep' in brief


def test_a_keep_that_lost_the_race_to_the_head_is_merged_onto_it_and_judged_there_again_and_again(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A keep whose head moved on is merged onto the new head and judged again, as often as the head moves.

    Run X starts from head H1, and H2 is kept meanwhile. X keeps against H1, so it is merged onto H2. While that merge
    is measured, H3 is kept. The merge keeps against H2, so it is merged onto H3 in turn.
    """
    run_id = make_run(db, campaign)
    h2 = keep_elsewhere(db, campaign, 'notes', {'notes.txt': 'n\n'}, {'score': 1.0, 'size': 5.0})
    ex = controller(db, runtime, tmp_path)
    ended: list[None] = []
    track = jobs._track

    def h3_kept_while_the_merge_is_measured(control: jobs.Control, proc: subprocess.Popen[bytes] | None) -> None:
        track(control, proc)
        if proc is None:
            ended.append(None)
            if len(ended) == 2:
                keep_elsewhere(db, campaign, 'docs', {'docs.txt': 'd\n'}, {'score': 1.0, 'size': 5.0}, on=h2)

    monkeypatch.setattr(jobs, '_track', h3_kept_while_the_merge_is_measured)
    execute(ex, db, run_id)
    repo = Repo(campaign.path)
    with db.session() as s:
        rows = s.scalars(select(Candidate).order_by(Candidate.id)).all()
        assert [(c.id, c.summary, c.verdict, c.parent_id, c.run_id) for c in rows] == snapshot(
            [
                (1, 'baseline', Verdict.KEEP, None, None),
                (2, 'notes', Verdict.KEEP, 1, None),
                (3, 'add a line', Verdict.INTERESTING, 1, 1),
                (4, 'add a line (rebased onto candidate#2)', Verdict.INTERESTING, 2, 1),
                (5, 'docs', Verdict.KEEP, 2, None),
                (6, 'add a line (rebased onto candidate#5)', Verdict.KEEP, 5, 1),
            ]
        )
        x, merged, h3, final = rows[2], rows[3], rows[4], rows[5]
        assert [x.verdict_reason, merged.verdict_reason, final.verdict_reason] == snapshot(
            [
                'keeps against its own bar, but the head moved on to candidate#2: against candidate#1: score 2 vs 1 (+1; aggregate: no per-unit results); rebased onto the head as candidate#4',
                'keeps against its own bar, but the head moved on to candidate#5: against candidate#2: score 2 vs 1 (+1; aggregate: no per-unit results); rebased onto the head as candidate#6',
                'against candidate#5: score 2 vs 1 (+1; aggregate: no per-unit results)',
            ]
        )
        trunk = head(s, s.get_one(Campaign, campaign.id))
        assert trunk is not None and trunk.id == final.id, 'the last merge is the head'
        rebased = s.scalars(select(Event).where(has(Rebased)).order_by(Event.id)).all()
        assert [(e.target_id, e.payload) for e in rebased] == [
            (x.id, Rebased(head=h2, rebased=merged.id)),
            (merged.id, Rebased(head=h3.id, rebased=final.id)),
        ]
        assert [p.hexsha for p in repo.commit(final.commit_sha).parents] == [h3.commit_sha, merged.commit_sha]
        assert sorted((Path(campaign.path) / '.git' / 'refs' / 'piloop' / 'demo' / 'rebase').iterdir()) == [
            Path(campaign.path) / '.git' / 'refs' / 'piloop' / 'demo' / 'rebase' / str(c)
            for c in sorted((x.id, merged.id))
        ], 'every merge commit is pinned'
        tree = set(repo.git.ls_tree('--name-only', final.commit_sha).splitlines())
        assert {'notes.txt', 'docs.txt', 'solve.txt'} <= tree, "the head's changes and the run's, together"
        assert s.get_one(Idea, run_row(db, run_id).idea_id).status is IdeaStatus.DONE


def test_with_a_noisy_metric_a_run_pairs_its_commit_with_its_bar_and_a_merge_with_the_head_it_merged_onto(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    size = {'name': 'size', 'direction': 'lower', 'role': 'guardrail', 'margin': 100, 'repeats': 2}
    set_config(db, campaign, metrics=[{'name': 'score', 'direction': 'higher', 'role': 'primary'}, size])
    ex = controller(db, runtime, tmp_path)
    ex.remeasure(baseline(db, campaign.id).id)
    run_id = make_run(db, campaign)
    h2 = keep_elsewhere(db, campaign, 'notes', {'notes.txt': 'n\n'}, {'score': 1.0, 'size': 5.0})
    execute(ex, db, run_id)
    with db.session() as s:
        x, merged = s.scalars(select(Candidate).where(Candidate.run_id == run_id).order_by(Candidate.id)).all()
        assert [m.pair.candidate_id for m in x.measurements if m.pair] == [1, 1], 'paired with its bar'
        assert [m.pair.candidate_id for m in merged.measurements if m.pair] == [h2, h2], 'paired with the new head'
        assert merged.verdict_reason == snapshot(
            'against candidate#2: score 2 vs 1 (+1; 95% worst case +1 over 2 pairs); guardrails: size 10 vs 5 (+5; 95% worst case +5 over 2 pairs) within its margin 100'
        )


def test_a_keep_whose_merge_onto_the_head_conflicts_stays_interesting_and_files_a_seed_to_compose_it(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    run_id = make_run(db, campaign)
    h2 = keep_elsewhere(db, campaign, 'other line', {'solve.txt': 'base\nother\n'}, {'score': 2.0, 'size': 11.0})
    execute(controller(db, runtime, tmp_path), db, run_id)
    [(_, verdict, reason, _, _)] = scored(db, run_id)
    assert (verdict, reason) == (
        Verdict.INTERESTING,
        snapshot(
            'keeps against its own bar, but the head moved on to candidate#2: against candidate#1: score 2 vs 1 (+1; aggregate: no per-unit results); its merge onto the head conflicts in solve.txt: seed idea#2 composes it by hand'
        ),
    )
    with db.session() as s:
        [x] = s.scalars(select(Candidate).where(Candidate.run_id == run_id)).all()
        [seed] = s.scalars(select(Idea).where(Idea.status == IdeaStatus.SEED)).all()
        assert (seed.title, seed.track, seed.base_candidate_id, seed.created_by, seed.provenance) == (
            f'Compose candidate#{x.id} onto the head',
            'score',
            None,
            DAEMON,
            f'candidate#{x.id}',
        ), 'unpinned, so it runs from the head, on the run track'
        assert seed.hypothesis == (
            f'Follow-up of candidate#{x.id} (add a line): it kept against its own bar but its merge onto the head'
            f' candidate#{h2} conflicts in solve.txt: from the head, git merge {x.commit_sha} and resolve the conflicts'
        )
        [event] = s.scalars(select(Event).where(has(Rebased))).all()
        assert (event.target_id, event.payload) == (x.id, Rebased(head=h2, conflicts=['solve.txt']))
        trunk = head(s, s.get_one(Campaign, campaign.id))
        assert trunk is not None and trunk.id == h2


def test_a_run_whose_chained_commits_each_conflict_on_the_head_leaves_one_compose_seed(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """The seed of the run's later commit composes the earlier one too, so it supersedes the earlier one's seed."""
    write_toml(
        Path(campaign.path),
        '#!/bin/sh\necho a >> solve.txt\ngit commit -qam first\necho b >> solve.txt\ngit commit -qam second\n',
    )
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign)
    keep_elsewhere(db, campaign, 'other line', {'solve.txt': 'base\nother\n'}, {'score': 2.0, 'size': 11.0})
    execute(controller(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        first, second = candidates(db, run_id)
        seeds = s.scalars(select(Idea).where(Idea.provenance.is_not(None)).order_by(Idea.id)).all()
        assert [(i.provenance, [o.id for o in i.supersedes]) for i in seeds] == [
            (f'candidate#{first.id}', []),
            (f'candidate#{second.id}', [seeds[0].id]),
        ]
        supersede_composed(s, GitRepository(Repo(campaign.path)), s.get_one(Candidate, first.id), seeds[0])
        assert seeds[0].supersedes == [], 'the earlier commit does not compose the later one'
        assert list(supersede(s, campaign.id)) == [seeds[0].id]
        s.commit()
        live = s.scalars(select(Idea.provenance).where(Idea.status == IdeaStatus.SEED)).all()
    assert live == [f'candidate#{second.id}']


def test_a_conflicting_merge_in_a_stopped_campaign_files_no_seed(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """The final commits of a stopped campaign are still scored, but the daemon files no idea for it."""
    run_id = make_run(db, campaign)
    keep_elsewhere(db, campaign, 'other line', {'solve.txt': 'base\nother\n'}, {'score': 2.0, 'size': 11.0})
    with db.session() as s:
        s.get_one(Campaign, campaign.id).status = CampaignStatus.STOPPED
        s.commit()
    execute(controller(db, runtime, tmp_path), db, run_id)
    [(_, verdict, reason, _, _)] = scored(db, run_id)
    assert verdict is Verdict.INTERESTING
    assert reason is not None and reason.endswith('its merge onto the head conflicts in solve.txt')
    with db.session() as s:
        assert s.scalars(select(Idea).where(Idea.status == IdeaStatus.SEED)).all() == []


def test_a_rejudge_does_not_file_a_compose_seed_again_after_it_was_rejected(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A seed from this candidate with this title exists, whatever its status or name."""
    run_id = make_run(db, campaign)
    keep_elsewhere(db, campaign, 'other line', {'solve.txt': 'base\nother\n'}, {'score': 2.0, 'size': 11.0})
    ex = controller(db, runtime, tmp_path)
    execute(ex, db, run_id)
    with db.session() as s:
        [x] = s.scalars(select(Candidate).where(Candidate.run_id == run_id)).all()
        [seed] = s.scalars(select(Idea).where(Idea.status == IdeaStatus.SEED)).all()
        seed.status = IdeaStatus.REJECTED
        add_measurement(s, x.id, Report(metrics={'score': 2.0, 'size': 10.0}), actor=System.HUMAN)
        s.commit()
        x_id, seed_id = x.id, seed.id
    ex.rejudge(x_id)
    with db.session() as s:
        filed = s.scalars(select(Idea.id).where(Idea.provenance == f'candidate#{x_id}')).all()
        reason = s.get_one(Candidate, x_id).verdict_reason
    assert filed == [seed_id], 'the rejected compose seed is filed again under another title'
    assert reason is not None and reason.endswith('its merge onto the head conflicts in solve.txt')


def test_two_sessions_that_register_one_new_tree_share_one_candidate(
    db: Database, campaign: Campaign, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The second session looked before the first committed, so its insert conflicts."""
    repo = Repo(campaign.path)
    (Path(campaign.path) / 'solve.txt').write_text('base\nsame\n')
    repo.git.commit('-qam', 'same')
    sha = repo.head.commit.hexsha
    parent = baseline(db, campaign.id)
    with db.session() as a, db.session() as b:
        looked = b.scalar

        def stale(*args: object, **kwargs: object) -> object:
            monkeypatch.setattr(b, 'scalar', looked)
            return None

        monkeypatch.setattr(b, 'scalar', stale)
        first, _ = resolve(a, campaign.id, GitRepository(repo).version(sha), parent=parent, run_id=None)
        a.commit()
        found, created = resolve(b, campaign.id, GitRepository(repo).version(sha), parent=parent, run_id=None)
        b.commit()
        assert (found.id, created) == (first.id, False)
    with db.session() as s:
        assert s.scalar(select(func.count()).select_from(Candidate).where(Candidate.tree_sha == first.tree_sha)) == 1


def test_a_keep_whose_merge_onto_the_head_is_the_head_already_changes_nothing(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    run_id = make_run(db, campaign)
    files = {'solve.txt': 'base\nline\n', 'notes.txt': 'n\n'}
    h2 = keep_elsewhere(db, campaign, 'line and notes', files, {'score': 2.0, 'size': 10.0})
    execute(controller(db, runtime, tmp_path), db, run_id)
    [(_, verdict, reason, _, _)] = scored(db, run_id)
    assert (verdict, reason) == (
        Verdict.INTERESTING,
        snapshot(
            'keeps against its own bar, but the head moved on to candidate#2: against candidate#1: score 2 vs 1 (+1; aggregate: no per-unit results); its merge onto the head is candidate#2 already (keep)'
        ),
    )
    with db.session() as s:
        [event] = s.scalars(select(Event).where(has(Rebased))).all()
        assert event.payload == Rebased(head=h2, known=h2)


def test_a_keep_that_lands_while_another_run_measures_its_tree_is_not_judged_again(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A run that reaches a pending tree of another run judges it only while it is pending.

    The other run's keep lands while this run measures the same tree on another track. Judged again on this track's
    primary, the keep would become a discard and miss the head.
    """
    tracks = {
        'metrics': [
            {'name': 'score', 'direction': 'higher', 'role': 'primary', 'track': 'accuracy', 'margin': 1},
            {'name': 'size', 'direction': 'lower', 'role': 'primary', 'track': 'cost', 'margin': 100},
        ],
        'tracks': {'accuracy': {'share': 0.5}, 'cost': {'share': 0.5}},
    }
    set_config(db, campaign, **tracks)
    ex = controller(db, runtime, tmp_path)
    first = make_run(db, campaign, track='accuracy')
    execute(ex, db, first)
    [kept] = candidates(db, first)
    assert kept.verdict is Verdict.KEEP
    with db.session() as s:  # back to while the first run measures its commit
        candidate = s.get_one(Candidate, kept.id)
        [measurement] = candidate.measurements
        evaluator_id = measurement.evaluator_id
        s.delete(measurement)
        candidate.verdict = Verdict.PENDING
        s.commit()
    track = jobs._track

    def kept_meanwhile(control: jobs.Control, proc: subprocess.Popen[bytes] | None) -> None:
        track(control, proc)
        if proc is None:  # the first run's measurement and keep land while the second run measures the same tree
            with db.session() as s:
                s.add(Measurement(candidate_id=kept.id, evaluator_id=evaluator_id, metrics=kept.metrics))
                c = s.get_one(Candidate, kept.id)
                c.verdict, c.verdict_reason = Verdict.KEEP, 'kept on accuracy'
                s.commit()

    monkeypatch.setattr(jobs, '_track', kept_meanwhile)
    execute(ex, db, make_run(db, campaign, title='the same line, on the cost track', track='cost'))
    with db.session() as s:
        again = s.get_one(Candidate, kept.id)
        assert (again.verdict, again.verdict_reason) == (Verdict.KEEP, 'kept on accuracy')


def test_a_known_non_keep_that_another_run_reaches_again_is_judged_again_on_that_runs_bar(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A tree that another run proposes again is judged again, unless it is a keep.

    The daemon's old verdict judged the tree against another bar, or a merge that conflicted. A compose that reproduces
    the tree on the head must be able to land.
    """
    ex = controller(db, runtime, tmp_path)
    first = make_run(db, campaign)
    execute(ex, db, first)
    [known] = candidates(db, first)
    with db.session() as s:
        c = s.get_one(Candidate, known.id)
        c.verdict, c.verdict_reason = Verdict.INTERESTING, 'its merge onto the head conflicts in solve.txt'
        s.commit()
    execute(ex, db, make_run(db, campaign, title='a compose that reproduces the tree'))
    with db.session() as s:
        again = s.get_one(Candidate, known.id)
        assert (again.verdict, again.verdict_by, again.verdict_reason) == (
            Verdict.KEEP,
            DAEMON,
            'against candidate#1: score 2 vs 1 (+1; aggregate: no per-unit results)',
        )
        assert len(again.measurements) == 1, 'judged with the measurement it has'
        now = head(s, s.get_one(Campaign, campaign.id))
        assert now is not None and now.id == known.id


def test_a_merge_commit_descends_from_its_first_parent_the_runs_base(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A run's commits are its first-parent history, so a merge's parent is the run's base.

    The merged-in line is not the run's, and its commits are known candidates already. With the merged line's last
    candidate as its parent, the merge did not build on the head, and a keep by hand was refused.
    """
    metrics = {'score': 1.0, 'size': 5.0}
    side = keep_elsewhere(db, campaign, 'notes', {'notes.txt': 'n\n'}, metrics, verdict=Verdict.INTERESTING)
    trunk = keep_elsewhere(db, campaign, 'docs', {'docs.txt': 'd\n'}, metrics)
    with db.session() as s:
        side_sha = s.get_one(Candidate, side).commit_sha
    write_toml(Path(campaign.path), f'#!/bin/sh\ngit merge -q --no-ff --no-edit {side_sha}\n')
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign)
    with db.session() as s:
        s.get_one(Run, run_id).base_candidate_id = trunk
        s.commit()
    execute(controller(db, runtime, tmp_path), db, run_id)
    [(_, verdict, _, measurements, parent)] = scored(db, run_id)
    assert (verdict, measurements, parent) == (Verdict.DISCARD, 1, trunk)
    with db.session() as s:
        [merge] = s.scalars(select(Candidate).where(Candidate.run_id == run_id)).all()
        assert (
            set_verdict(s, merge.id, Verdict.KEEP, reason='the notes are worth it', actor=System.HUMAN)
            is Verdict.DISCARD
        )
        assert len(s.get_one(Candidate, side).measurements) == 1, 'the merged line is not measured again'


def test_a_keep_that_has_the_heads_changes_without_its_history_is_judged_against_the_head(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A commit that cherry-picked the head needs no merge onto it.

    The merge would give the candidate's own tree, a known candidate, so the keep stayed ``interesting`` for good.
    """
    run_id = make_run(db, campaign)
    h2 = keep_elsewhere(db, campaign, 'other', {'other.txt': 'x\n'}, {'score': 1.0, 'size': 5.0})
    with db.session() as s:
        h2_sha = s.get_one(Candidate, h2).commit_sha
    pick = f'#!/bin/sh\ngit cherry-pick -n {h2_sha}\necho line >> solve.txt\ngit commit -qam "the head and a line"\n'
    write_toml(Path(campaign.path), pick)
    reload_campaign(db, campaign)
    execute(controller(db, runtime, tmp_path), db, run_id)
    [(_, verdict, reason, _, _)] = scored(db, run_id)
    assert (verdict, reason) == (
        Verdict.KEEP,
        f'the head candidate#{h2} is in its history: against candidate#{h2}: score 2 vs 1'
        ' (+1; aggregate: no per-unit results)',
    )
    with db.session() as s:
        now = head(s, s.get_one(Campaign, campaign.id))
        assert now is not None and now.run_id == run_id


def test_a_merge_a_stop_left_unjudged_is_measured_and_judged_when_the_run_resumes(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_id = make_run(db, campaign)
    keep_elsewhere(db, campaign, 'notes', {'notes.txt': 'n\n'}, {'score': 1.0, 'size': 5.0})
    ex = controller(db, runtime, tmp_path)
    measure = ex._measure

    merged_id = 4

    def stopped_at_the_merge(
        run: RunId | None,
        campaign_id: CampaignId,
        cfg: CampaignConfig,
        sha: Revision,
        candidate_id: CandidateId,
        evaluator_id: EvaluatorId,
        eval_dir: Path,
        *,
        bar: CandidateId | None,
        repeats: int,
    ) -> bool:
        if candidate_id == merged_id:
            return False  # what a stop does to an evaluation in flight
        return measure(run, campaign_id, cfg, sha, candidate_id, evaluator_id, eval_dir, bar=bar, repeats=repeats)

    monkeypatch.setattr(ex, '_measure', stopped_at_the_merge)
    execute(ex, db, run_id)
    with db.session() as s:
        merged = s.get_one(Candidate, merged_id)
        assert (merged.verdict, merged.measurements, merged.parent_id) == (Verdict.PENDING, [], 2)
    ex.resume(run_id)  # stopped again on the merge
    with db.session() as s:
        assert s.get_one(Candidate, merged_id).verdict is Verdict.PENDING
    monkeypatch.setattr(ex, '_measure', measure)
    ex.resume(run_id)
    with db.session() as s:
        merged = s.get_one(Candidate, merged_id)
        assert (merged.verdict, len(merged.measurements)) == (Verdict.KEEP, 1)


def test_a_tree_two_runs_reach_is_never_judged_against_itself(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A tree that two runs reach is judged against the best candidate other than itself.

    The second run reaches the tree while the first run's evaluation is pending. The first keep then makes the tree the
    head, but the second run must not judge it level with itself.
    """
    ex = controller(db, runtime, tmp_path)
    first = make_run(db, campaign)
    execute(ex, db, first)
    [kept] = candidates(db, first)
    assert kept.verdict is Verdict.KEEP
    with db.session() as s:  # rewind to before the first run's measurement landed
        candidate = s.get_one(Candidate, kept.id)
        [measurement] = candidate.measurements
        evaluator_id = measurement.evaluator_id
        s.delete(measurement)
        candidate.verdict = Verdict.PENDING
        s.commit()
    track = jobs._track

    def kept_meanwhile(control: jobs.Control, proc: subprocess.Popen[bytes] | None) -> None:
        """Land the first run's measurement and keep while the second run evaluates the same tree."""
        track(control, proc)
        if proc is None:
            with db.session() as s:
                s.add(Measurement(candidate_id=kept.id, evaluator_id=evaluator_id, metrics=kept.metrics))
                s.get_one(Candidate, kept.id).verdict = Verdict.KEEP
                s.commit()

    monkeypatch.setattr(jobs, '_track', kept_meanwhile)
    execute(ex, db, make_run(db, campaign, title='same lines'))
    with db.session() as s:
        again = s.get_one(Candidate, kept.id)
        assert (len(again.measurements), again.verdict, again.verdict_reason) == (
            2,
            Verdict.KEEP,
            snapshot('against candidate#1: score 2 vs 1 (+1; aggregate: no per-unit results)'),
        ), 'judged against the bar it beat, the best but for itself'


HARNESS_NOTED = (
    '#!/bin/sh\n: > solve.txt\n'
    'git commit -qam wipe -m "Piloop-Note: faster, though it scores less\nPiloop-Follow-Up: wipe, then refill"\n'
    'echo a >> solve.txt\necho b >> solve.txt\n'
    'git commit -qam refill -m "Piloop-Note: redundant"\n'
)


def test_a_piloop_note_keeps_a_regressed_commit_in_view_files_no_follow_up_and_moves_no_bar(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    write_toml(Path(campaign.path), HARNESS_NOTED)
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign)
    execute(controller(db, runtime, tmp_path), db, run_id)
    [(_, wipe, wipe_reason, _, _), (_, refill, refill_reason, _, _)] = scored(db, run_id)
    assert (wipe, wipe_reason) == (
        Verdict.INTERESTING,
        snapshot(
            'note: faster, though it scores less; against candidate#1: score 0 vs 1 (-1; aggregate: no per-unit results)'
        ),
    )
    assert (refill, refill_reason) == (
        Verdict.KEEP,
        snapshot('against candidate#1: score 2 vs 1 (+1; aggregate: no per-unit results)'),
    ), 'judged against the baseline: the note moved no bar, and a keep keeps its verdict'
    with db.session() as s:
        noted = s.scalars(select(Candidate).where(Candidate.run_id == run_id).order_by(Candidate.id)).first()
        assert noted is not None and noted.verdict_by == 'implementer#1', (
            "the note is its implementer's: the daemon never judges a noted commit again"
        )
        follow_ups = s.scalars(select(Idea).where(Idea.base_candidate_id.is_not(None))).all()
        assert follow_ups == [], 'a Piloop-Follow-Up trailer files no seed: an implementer files none'


def test_a_keep_on_one_track_must_hold_the_other_track_primaries_and_its_brief_carries_the_track(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A keep on one track must stay within the floor of the other track's primary metric.

    An accuracy candidate that adds one line stays within the cost track's floor on size.
    """
    path = Path(campaign.path)
    (path / 'briefs').mkdir()
    (path / 'briefs' / 'accuracy.md').write_text('# Accuracy track\nOnly add lines that are right.\n')
    tracks = {
        'metrics': [
            {'name': 'score', 'direction': 'higher', 'role': 'primary', 'track': 'accuracy', 'margin': 1},
            {'name': 'size', 'direction': 'lower', 'role': 'primary', 'track': 'cost', 'margin': 2, 'floor': 8},
        ],
        'tracks': {'accuracy': {'share': 0.5, 'addendum': 'briefs/accuracy.md'}, 'cost': {'share': 0.5}},
    }
    with db.session() as s:
        s.get_one(Campaign, campaign.id).config = load_campaign_config(path, **tracks).model_dump(mode='json')
        s.commit()
    run_id = make_run(db, campaign, track='accuracy')
    execute(controller(db, runtime, tmp_path), db, run_id)
    [(_, verdict, reason, _, _)] = scored(db, run_id)
    assert (verdict, reason) == (Verdict.DISCARD, 'floor: size 10 past its floor 8'), 'score 2 > 1, but size 10 > 8'
    brief = run_row(db, run_id).brief or ''
    assert '# Accuracy track\nOnly add lines that are right.' in brief, "the track's addendum"
    assert (
        '- Your track is accuracy: a keep must improve score (higher is better) with confidence and hold size'
        ' (margin 2, floor 8).'
    ) in brief
    assert '- Starting from candidate#1: score=1.0, size=5.0 — baseline' in brief
    assert '- Tracks: accuracy (primary score, higher is better; share 0.5); cost (primary size' in brief
    assert 'Your run is on track accuracy.' in brief


def test_recovered_work_whose_tree_is_already_a_candidate_still_requeues_its_idea(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """The recovery commit is that candidate, which keeps its verdict; the idea still starts again from it."""
    write_toml(Path(campaign.path), HARNESS_APPEND)
    reload_campaign(db, campaign)
    first = make_run(db, campaign, title='commits the line')
    execute(controller(db, runtime, tmp_path), db, first)
    [known] = candidates(db, first)
    write_toml(Path(campaign.path), HARNESS_FORGETS_COMMIT)
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign, title='leaves the same line', max_runs=1)
    execute(controller(db, runtime, tmp_path), db, run_id)
    with db.session() as s:
        idea = s.get_one(Idea, run_row(db, run_id).idea_id)
        assert (idea.status, idea.base_candidate_id) == (IdeaStatus.READY, known.id)
        assert s.get_one(Candidate, known.id).verdict is Verdict.KEEP, 'a known tree keeps its verdict'


def test_every_evaluator_has_the_process_runner_and_an_uninstalled_runner_is_refused(
    db: Database, campaign: Campaign
) -> None:
    with db.session() as s:
        row = s.get_one(Campaign, campaign.id)
        evaluator = current_evaluator(s, row, config_of(row))
        assert evaluator.runner == 'process', 'what piloop init records'
        evaluator.runner = 'lean'
        s.flush()
        with pytest.raises(ValueError, match=r"has the evaluator runner 'lean', which is not installed: process$"):
            current_evaluator(s, row, config_of(row))


def test_a_stop_that_comes_just_after_the_report_leaves_the_run_to_the_stop(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """The report applied before the stop, but the run had not read it yet: the stop owns the rows, as for a cut run."""
    ex = controller(db, runtime, tmp_path)
    launch = ex.launcher.launch

    def stopped_just_after(*args: Any, **kwargs: Any) -> Ended:
        ended = launch(*args, **kwargs)
        ex.launcher.cancel()
        return ended

    mocker.patch.object(ex.launcher, 'launch', side_effect=stopped_just_after)
    run_id = make_run(db, campaign)
    execute(ex, db, run_id)
    assert run_row(db, run_id).status is RunStatus.RUNNING, 'stop() writes the row, not the run thread'
    assert git.pinned(git.open_repo(Path(campaign.path)), f'refs/piloop/{config_of(campaign).name}/r{run_id}/'), (
        'its commit is kept'
    )


def test_linked_inputs_go_into_the_store_while_no_transaction_holds_the_write_lock(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """The first snapshot of a large ``link`` takes minutes: the daemon's other writers must not wait on it."""
    ex = controller(db, runtime, tmp_path)
    real = ex.sources.source
    locked: list[bool] = []

    def git_source(c: Campaign, commit: Revision) -> Source:
        # another thread's write (the worker's heartbeat) ends within the wait; one of this thread's never would
        other = sqlite3.connect(db.engine.url.database or '', timeout=5)
        try:
            other.execute('BEGIN IMMEDIATE')
            locked.append(False)
        except sqlite3.OperationalError:
            locked.append(True)
        finally:
            other.close()
        return real(c, commit)

    mocker.patch.object(ex.sources, 'source', side_effect=git_source)
    ex.remeasure(baseline(db, campaign.id).id)
    assert locked == [False]


def test_an_evaluation_that_changes_a_linked_input_is_a_crash_and_the_shared_copy_is_made_again(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
) -> None:
    """Read-only is a mode that the evaluate command can change. What it measured on inputs it changed is no
    measurement, and the next job does not get the changed copy."""
    cooks = 'chmod u+w data/x.csv && echo 0 > data/x.csv && echo METRIC score=9 && echo METRIC size=1'
    set_config(db, campaign, link=['data'], evaluate=['sh', '-c', cooks])
    candidate = baseline(db, campaign.id).id
    ex = controller(db, runtime, tmp_path, tracer)
    ex.remeasure(candidate)
    with db.session() as s:
        measured = s.scalars(select(Measurement).where(Measurement.candidate_id == candidate)).all()[-1]
    assert (measured.metrics, measured.error) == ({}, f'{CHANGED_INPUTS}: data/x.csv')
    assert [e['paths'] for e in events(spans, INPUTS_CHANGED)] == ['data/x.csv']
    set_config(
        db,
        campaign,
        link=['data'],
        evaluate=['sh', '-c', 'echo METRIC score=$(cat data/x.csv); echo METRIC size=1'],
    )
    ex.remeasure(candidate)
    with db.session() as s:
        measured = s.scalars(select(Measurement).where(Measurement.candidate_id == candidate)).all()[-1]
    assert measured.metrics['score'] == 1.0, 'the input as it was put in the store'
    assert len(events(spans, INPUTS_REBUILT)) == (0 if reflinks(tmp_path) else 1), 'a hard link carried the change'


def reflinks(tmp_path: Path) -> bool:
    """Whether the filesystem of ``tmp_path`` makes copy-on-write clones."""
    (tmp_path / 'probe').write_text('x')
    try:
        with (tmp_path / 'probe').open('rb') as src, (tmp_path / 'clone').open('wb') as dst:
            fcntl.ioctl(dst.fileno(), FICLONE, src.fileno())
    except OSError:
        return False
    return True


@pytest.mark.parametrize('recorded', [True, False], ids=['received', 'before-0.3.0'])
def test_a_resumed_run_scores_the_versions_that_it_received(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    recorded: bool,
) -> None:
    """A stop during scoring leaves the run's versions recorded: the next daemon scores them, whatever refs say.

    A run that a daemon before 0.3.0 received has no ``RunReceived``: its versions are the ones that the vcs kept.
    """
    run_id = make_run(db, campaign)
    ex = controller(db, runtime, tmp_path)
    measure = ex._measure

    def stopped(*args: object, **kwargs: object) -> bool:
        return False  # what a stop does to the evaluation in flight

    monkeypatch.setattr(ex, '_measure', stopped)
    execute(ex, db, run_id)
    with db.session() as s:
        [pending] = s.scalars(select(Candidate.verdict).where(Candidate.run_id == run_id)).all()
    assert pending is Verdict.PENDING
    repo = Repo(campaign.path)
    if recorded:
        for ref in repo.git.for_each_ref('--format=%(refname)', f'refs/piloop/demo/r{run_id}/').split():
            repo.git.update_ref('-d', ref)
    else:
        with closing(sqlite3.connect(db.engine.url.database or '')) as con, con:
            con.execute("DELETE FROM events WHERE kind = 'run.received'")
    monkeypatch.setattr(ex, '_measure', measure)
    ex.resume(run_id)
    [scored] = candidates(db, run_id)
    assert scored.verdict is Verdict.KEEP
