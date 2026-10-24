"""The control operations: each validates, changes the state and leaves its event, in the caller's transaction."""

import sqlite3
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path

import pytest
from git import Repo
from pydantic import ValidationError
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from piloop.actor import Agent, System
from piloop.campaign import ops
from piloop.config import load_campaign_config
from piloop.db.events import emit, has, in_order
from piloop.db.models import (
    Campaign,
    CampaignStatus,
    Candidate,
    Comment,
    CommentKind,
    Event,
    EventModel,
    Idea,
    IdeaLink,
    IdeaStatus,
    LinkKind,
    Measurement,
    Origin,
    Pause,
    Run,
    RunStatus,
)
from piloop.db.session import Database
from piloop.db.triggers.changelog import (
    CampaignParentChanged,
    CampaignPausedChanged,
    IdeaBaseChanged,
    IdeaLinked,
    IdeaTrackChanged,
    VerdictChanged,
)
from piloop.ids import CampaignId, CandidateId, IdeaId, TargetKind, TargetRef, TrackName
from piloop.plugins.verdict_rule.contract import Outcome
from piloop.scoring.ledger import (
    CandidateJudged,
    MeasurementRecorded,
    RejudgeFailed,
    RejudgeRequested,
    head,
    rejudges,
)
from tests.conftest import end_of, reconfigured

ACTOR = System.ORCHESTRATOR


@pytest.fixture
def me(db: Database, campaign: Campaign, tmp_path: Path) -> Iterator[Database]:
    """The campaign's database, opened as the orchestrator: the change-log triggers sign its writes with that actor."""
    database = Database(f'sqlite:///{tmp_path / "piloop.db"}', actor=ACTOR)
    yield database
    database.close()


def kinds(db: Database, target_kind: TargetKind, target_id: int) -> list[tuple[str, str, EventModel]]:
    with db.session() as s:
        rows = s.scalars(
            select(Event).where(Event.target_kind == target_kind, Event.target_id == target_id).order_by(Event.id)
        )
        return [(e.kind, e.actor, e.payload) for e in rows]


def add_idea(db: Database, campaign_id: CampaignId, title: str, status: IdeaStatus = IdeaStatus.READY) -> IdeaId:
    with db.session() as s:
        idea = Idea(
            campaign_id=campaign_id, track='score', title=title, hypothesis=title, status=status, created_by='t'
        )
        s.add(idea)
        s.commit()
        return idea.id


def run_idea(db: Database, status: IdeaStatus, run: RunStatus, *, scoring: bool = False) -> IdeaId:
    """An idea in ``status`` with one run in ``run``: a status set by hand while the run is open, or scored."""
    idea_id = add_idea(db, CampaignId(1), f'{status} with a {run} run {scoring}', status)
    with db.session() as s:
        s.add(
            Run(
                idea_id=idea_id,
                attempt=1,
                base_candidate_id=1,
                harness='h',
                model='m',
                status=run,
                end=end_of(run),
                scoring=scoring,
            )
        )
        s.commit()
    return idea_id


def run_candidate(
    db: Database,
    campaign: Campaign,
    verdict: Outcome | None,
    *,
    by: str | None = System.DAEMON,
    parent: int = 1,
    branch: bool = False,
    commit: str | None = None,
) -> CandidateId:
    """A candidate of a run of a new idea from ``parent``, with the daemon's ``verdict`` and one measurement (score 0).

    Its commit is ``commit``, else a sha that the repo does not have.
    """
    idea_id = add_idea(db, campaign.id, f'tried {verdict} {parent} {branch} {by}')
    with db.session() as s:
        run = Run(idea_id=idea_id, attempt=1, base_candidate_id=parent, harness='script', model='m', branch=branch)
        s.add(run)
        s.flush()
        c = Candidate(
            campaign_id=campaign.id,
            run_id=run.id,
            parent_id=parent,
            tree_sha=Repo(campaign.path).commit(commit).tree.hexsha if commit else f'tree {idea_id}',
            commit_sha=commit or f'commit {idea_id}',
            verdict=verdict,
            verdict_by=by,
        )
        s.add(c)
        s.flush()
        s.add(Measurement(candidate_id=c.id, evaluator_id=1, metrics={'score': 0.0, 'size': 0.0}))
        s.commit()
        return c.id


def test_a_pause_holds_launches_or_everything_with_its_reason_and_a_resume_clears_it(me: Database) -> None:
    with me.session() as s:
        campaign = ops.pause(s, CampaignId(1), evals=False, reason=' wait for evaluator v2 ')
        s.commit()
        assert (campaign.paused, campaign.paused_reason) == (Pause.LAUNCHES, 'wait for evaluator v2')
    with me.session() as s, pytest.raises(ops.Refused, match=r'paused already \(launches\): wait for evaluator v2'):
        ops.pause(s, CampaignId(1), evals=False, reason='again')
    with me.session() as s:
        ops.pause(s, CampaignId(1), evals=True, reason='and the evaluations')
        s.commit()
    with me.session() as s:
        assert ops.resume(s, CampaignId(1)) is Pause.ALL
        s.commit()
        assert (s.get_one(Campaign, 1).paused, s.get_one(Campaign, 1).paused_reason) == (None, None)
    assert [e for e in kinds(me, TargetKind.CAMPAIGN, 1) if e[0] == 'campaign.paused'] == [
        (
            'campaign.paused',
            ACTOR.signature,
            CampaignPausedChanged(old=None, new=Pause.LAUNCHES, paused_reason='wait for evaluator v2'),
        ),
        (
            'campaign.paused',
            ACTOR.signature,
            CampaignPausedChanged(old=Pause.LAUNCHES, new=Pause.ALL, paused_reason='and the evaluations'),
        ),
        ('campaign.paused', ACTOR.signature, CampaignPausedChanged(old=Pause.ALL, new=None, paused_reason=None)),
    ], 'the trigger logs each change, signed by the connection, and nothing else does'
    with me.session() as s:
        assert s.scalars(select(Comment)).all() == [], 'a pause is not an escalation'
        with pytest.raises(ops.Refused, match='campaign demo is not paused'):
            ops.resume(s, CampaignId(1))
        with pytest.raises(ops.Refused, match='a reason is required'):
            ops.pause(s, CampaignId(1), evals=False, reason='  ')
        with pytest.raises(ops.Refused, match='no campaign#9'):
            ops.pause(s, CampaignId(9), evals=False, reason='why')


def test_a_raw_pause_needs_a_reason_and_a_known_scope(me: Database, tmp_path: Path) -> None:
    with closing(sqlite3.connect(tmp_path / 'piloop.db')) as con:
        with pytest.raises(sqlite3.IntegrityError, match='campaigns.paused and campaigns.paused_reason go together'):
            con.execute("UPDATE campaigns SET paused = 'launches' WHERE id = 1")
        with pytest.raises(sqlite3.IntegrityError, match='CHECK constraint failed'):
            con.execute("UPDATE campaigns SET paused = 'forever', paused_reason = 'r' WHERE id = 1")
    with me.session() as s:
        s.get_one(Campaign, 1).paused_reason = 'no pause'
        with pytest.raises(IntegrityError, match='go together'):
            s.commit()


def test_only_a_sweeping_agent_files_its_ideas_for_review_while_the_campaign_is_paused(me: Database) -> None:
    with me.session() as s:
        campaign = s.get_one(Campaign, 1)
        assert ops.review_first(campaign, Agent(role='research', session=4), new=False) is None, 'not paused'
        assert (
            ops.review_first(campaign, Agent(role='research', session=4), new=True) == '[roles.research] max_ideas = 0'
        ), 'the test file'
        ops.pause(s, CampaignId(1), evals=False, reason='why')
        assert ops.review_first(campaign, Agent(role='watchdog', session=5), new=False) == 'campaign demo is paused'
        assert (
            ops.review_first(campaign, ACTOR, new=True) is None
            and ops.review_first(campaign, System.HUMAN, new=True) is None
        )
        assert ops.review_first(campaign, Agent(role='implementer', session=6), new=True) is None, (
            'an implementer files no ideas anyway'
        )


def test_a_verdict_override_is_the_actors_with_the_reason_in_its_event(me: Database, campaign: Campaign) -> None:
    c = run_candidate(me, campaign, Outcome.DISCARD)
    with me.session() as s:
        assert ops.set_verdict(s, c, Outcome.KEEP, reason=' measured by hand: +3 ', actor=ACTOR) is Outcome.DISCARD
        s.commit()
        candidate = s.get_one(Candidate, c)
        assert (candidate.verdict, candidate.verdict_by, candidate.verdict_reason) == (
            Outcome.KEEP,
            ACTOR.signature,
            'measured by hand: +3',
        )
    payload = VerdictChanged(
        old=Outcome.DISCARD, new=Outcome.KEEP, verdict_by=ACTOR.signature, verdict_reason='measured by hand: +3'
    )
    assert kinds(me, TargetKind.CANDIDATE, c) == [('candidate.verdict', ACTOR.signature, payload)]
    pending = run_candidate(me, campaign, None, by=None)
    with me.session() as s:
        with pytest.raises(ops.Refused, match=r'candidate#\d+ is keep already \(measured by hand: \+3\)'):
            ops.set_verdict(s, c, Outcome.KEEP, reason='again', actor=ACTOR)
        with pytest.raises(ops.Refused, match=f'candidate#{pending} is pending: the daemon judges it now'):
            ops.set_verdict(s, pending, Outcome.KEEP, reason='why', actor=ACTOR)
        with pytest.raises(ops.Refused, match='a reason is required'):
            ops.set_verdict(s, c, Outcome.CRASH, reason='', actor=ACTOR)
        with pytest.raises(ops.Refused, match='no candidate#99'):
            ops.set_verdict(s, CandidateId(99), Outcome.CRASH, reason='why', actor=ACTOR)
        for origin in (Origin.REFERENCE, Origin.RECOVERED, Origin.SKIPPED):
            s.get_one(Candidate, pending).origin = origin
            with pytest.raises(ops.Refused, match=f'candidate#{pending} is {origin}, which is never judged'):
                ops.set_verdict(s, pending, Outcome.KEEP, reason='why', actor=ACTOR)


def commit_on(campaign: Campaign, on: str, files: dict[str, str], *, merge: str | None = None) -> str:
    """Commit ``files`` on commit ``on`` of the campaign repo, or a merge of ``merge`` onto it, and return the sha."""
    repo = Repo(campaign.path)
    repo.git.checkout('-q', '--detach', on)
    if merge is not None:
        repo.git.merge('-q', '--no-edit', merge)
    for name, body in files.items():
        (Path(campaign.path) / name).write_text(body)
    repo.git.add('-A')
    repo.git.commit('-qm', ', '.join(files) or 'merge', '--allow-empty')
    sha = repo.head.commit.hexsha
    repo.git.checkout('-q', 'main')
    return sha


def test_a_hand_keep_must_build_on_the_head_unless_its_run_is_a_branch(me: Database, campaign: Campaign) -> None:
    """The head is the newest trunk keep, so a keep from an older bar would drop what the head kept since."""
    base = Repo(campaign.path).head.commit.hexsha
    old_sha, trunk_sha = commit_on(campaign, base, {'old.txt': 'o\n'}), commit_on(campaign, base, {'t.txt': 't\n'})
    old = run_candidate(me, campaign, Outcome.DISCARD, commit=old_sha)
    trunk = run_candidate(me, campaign, Outcome.KEEP, commit=trunk_sha)
    on_head = run_candidate(me, campaign, Outcome.DISCARD, parent=trunk)
    after_it = run_candidate(me, campaign, Outcome.INTERESTING, parent=on_head)
    branch = run_candidate(me, campaign, Outcome.DISCARD, branch=True)
    with me.session() as s:
        with pytest.raises(ops.Refused, match=f'candidate#{old} does not build on the head candidate#{trunk}'):
            ops.set_verdict(s, old, Outcome.KEEP, reason='measured by hand', actor=ACTOR)
        ops.set_verdict(s, old, Outcome.INTERESTING, reason='a lead', actor=ACTOR)
        ops.set_verdict(s, after_it, Outcome.KEEP, reason='its parent builds on the head', actor=ACTOR)
        ops.set_verdict(s, branch, Outcome.KEEP, reason='a branch keep never moves the head', actor=ACTOR)
        s.commit()
        now = head(s, s.get_one(Campaign, 1))
        assert now is not None and now.id == after_it, 'the newest trunk keep'


def test_a_hand_keep_builds_on_the_head_by_git_where_the_parent_links_miss_it(me: Database, campaign: Campaign) -> None:
    """Git decides what a candidate builds on when piloop's parent links do not reach the head.

    The merge's parent is the merged-in line, as piloop recorded merges before, but its commit merges the head.
    """
    base = Repo(campaign.path).head.commit.hexsha
    side_sha, trunk_sha = commit_on(campaign, base, {'side.txt': 's\n'}), commit_on(campaign, base, {'t.txt': 't\n'})
    side = run_candidate(me, campaign, Outcome.INTERESTING, commit=side_sha)
    trunk = run_candidate(me, campaign, Outcome.KEEP, commit=trunk_sha)
    merge_sha = commit_on(campaign, trunk_sha, {}, merge=side_sha)
    merged = run_candidate(me, campaign, Outcome.DISCARD, parent=side, commit=merge_sha)
    with me.session() as s:
        ops.set_verdict(s, merged, Outcome.KEEP, reason='its commit merges the head', actor=ACTOR)
        s.commit()
        now = head(s, s.get_one(Campaign, 1))
        assert now is not None and now.id == merged, f'a trunk keep newer than candidate#{trunk}'
        lost = run_candidate(me, campaign, Outcome.DISCARD)
        with pytest.raises(
            ops.Refused, match=f'the vcs cannot tell if candidate#{lost} builds on the head candidate#{merged}'
        ):
            ops.set_verdict(s, lost, Outcome.KEEP, reason='its commit is gone', actor=ACTOR)


def test_a_measurement_by_hand_is_recorded_and_asks_the_daemon_to_judge_its_candidate_again(
    me: Database, campaign: Campaign
) -> None:
    c = run_candidate(me, campaign, Outcome.CRASH)
    report = ops.Report(metrics={'score': 3.0, 'size': 9.0}, seconds=12.5)
    with me.session() as s:
        added = ops.add_measurement(s, c, report, actor=ACTOR)
        s.commit()
        measurement = added.measurement
        assert (added.rejudged, added.effect) == (True, 'the daemon judges it again with all its measurements')
        assert (measurement.evaluator_id, measurement.metrics, measurement.seconds) == (1, report.metrics, 12.5)
        assert [x.id for x in rejudges(s, s.get_one(Campaign, 1))] == [c]
    assert kinds(me, TargetKind.MEASUREMENT, measurement.id) == [
        (
            'measurement.recorded',
            ACTOR.signature,
            MeasurementRecorded(candidate=c, metrics={'score': 3.0, 'size': 9.0}, error=None, run=None),
        )
    ], 'shaped like the daemon event'
    requested = RejudgeRequested(measurement=measurement.id)
    assert kinds(me, TargetKind.CANDIDATE, c) == [('candidate.rejudge_requested', ACTOR.signature, requested)]
    with me.session() as s:
        judged = CandidateJudged(verdict=Outcome.DISCARD, score=None, reason='judged again')
        emit(s, judged, TargetRef(TargetKind.CANDIDATE, c), System.DAEMON)
        s.commit()
        assert rejudges(s, s.get_one(Campaign, 1)) == [], 'a later judgement answers the request'
        ops.add_measurement(s, c, ops.Report(metrics={}, error='timeout'), actor=ACTOR)
        s.commit()
        assert [x.id for x in rejudges(s, s.get_one(Campaign, 1))] == [c], 'a failed one counts too'
        emit(s, RejudgeFailed(error='gone'), TargetRef(TargetKind.CANDIDATE, c), System.DAEMON)
        s.commit()
        assert rejudges(s, s.get_one(Campaign, 1)) == [], 'so does a failure'


def test_a_measurement_by_hand_of_a_keep_or_of_someones_verdict_leaves_the_verdict(
    me: Database, campaign: Campaign
) -> None:
    mine = run_candidate(me, campaign, Outcome.DISCARD, by='human')
    with me.session() as s:
        keep = ops.add_measurement(s, CandidateId(1), ops.Report(metrics={'score': 1.0, 'size': 5.0}), actor=ACTOR)
        assert (keep.rejudged, keep.effect) == (
            False,
            'its keep stays; the runs that start from it must beat its new mean',
        )
        human = ops.add_measurement(s, mine, ops.Report(metrics={'score': 1.0, 'size': 5.0}), actor=ACTOR)
        assert (human.rejudged, human.effect) == (False, 'its verdict stays: discard, set by human')
        s.commit()
        assert s.scalars(select(Event).where(has(RejudgeRequested))).all() == []
        emit(s, RejudgeRequested(measurement=keep.measurement.id), TargetRef(TargetKind.CANDIDATE, mine), ACTOR)
        s.commit()
        assert rejudges(s, s.get_one(Campaign, 1)) == [], 'a request for a verdict that someone set is dropped'
        with pytest.raises(ops.Refused, match='the report has no size: give every metric, or the error'):
            ops.add_measurement(s, CandidateId(1), ops.Report(metrics={'score': 1.0}), actor=ACTOR)
        with pytest.raises(ops.Refused, match='piloop.toml has no metric accuracy: its metrics are score, size'):
            ops.add_measurement(
                s, CandidateId(1), ops.Report(metrics={'score': 1.0, 'size': 5.0, 'accuracy': 1.0}), actor=ACTOR
            )
        with pytest.raises(ops.Refused, match='no candidate#99'):
            ops.add_measurement(s, CandidateId(99), ops.Report(metrics={}), actor=ACTOR)
        s.get_one(Campaign, 1).evaluator_id = None
        with pytest.raises(ops.Refused, match='campaign demo has no evaluator yet'):
            ops.add_measurement(s, CandidateId(1), ops.Report(metrics={'score': 1.0, 'size': 5.0}), actor=ACTOR)


@pytest.mark.parametrize(
    'text',
    [
        '{"metrics": {"score": "high"}}',
        '{"metrics": {"score": 1e999}}',
        '{"metrics": {}, "seconds": -1}',
        '{"metrics": {}, "extra": 1}',
        '{"seconds": 1}',
    ],
)
def test_a_report_is_validated_at_the_boundary(text: str) -> None:
    with pytest.raises(ValidationError):
        ops.Report.model_validate_json(text)


def test_a_measurement_by_hand_says_when_the_daemon_judges_again_and_waits_for_a_pending_judgement(
    me: Database, campaign: Campaign
) -> None:
    crashed = run_candidate(me, campaign, Outcome.CRASH)
    pending = run_candidate(me, campaign, None, by=None)
    report = ops.Report(metrics={'score': 2.0, 'size': 4.0})
    with me.session() as s:
        with pytest.raises(ops.Refused, match=f'candidate#{pending} is pending: the daemon judges it now'):
            ops.add_measurement(s, pending, report, actor=ACTOR)
        p = s.get_one(Campaign, 1)
        p.status, p.stopped_reason = CampaignStatus.STOPPED, 'max_runs: 3 runs'
        assert ops.add_measurement(s, crashed, report, actor=ACTOR).effect == (
            'the daemon judges it again with all its measurements'
        ), 'a stopped campaign is still measured and judged on request'
        p.status, p.stopped_reason = CampaignStatus.ACTIVE, None
        ops.pause(s, CampaignId(1), evals=True, reason='why')
        assert ops.add_measurement(s, crashed, report, actor=ACTOR).effect.endswith(
            'once campaign demo is resumed (its evaluations are paused)'
        )
        ops.resume(s, CampaignId(1))
        p.config = reconfigured(p.config, spend={'command': ['true']}, plugins={'stop_rule': {'max_spend': 10.0}})
        assert ops.add_measurement(s, crashed, report, actor=ACTOR).effect.endswith(
            'once the stop rule no longer holds it (spend not read yet)'
        )
        p.status = CampaignStatus.STOPPED
        assert ops.add_measurement(s, crashed, report, actor=ACTOR).effect == (
            'the daemon judges it again with all its measurements'
        ), 'the stop rule holds an active campaign only'


def test_an_idea_status_takes_a_reason_in_a_note_and_refuses_what_is_not_set_by_hand(
    me: Database, campaign: Campaign
) -> None:
    idea = add_idea(me, CampaignId(1), 'an idea')
    seed = add_idea(me, CampaignId(1), 'a hunch', IdeaStatus.SEED)
    running = add_idea(me, CampaignId(1), 'running', IdeaStatus.SCHEDULED)
    with me.session() as s:
        assert ops.set_idea_status(s, idea, IdeaStatus.PARKED, reason='after the v2 evaluator', actor=ACTOR) is (
            IdeaStatus.READY
        )
        ops.set_idea_status(s, seed, IdeaStatus.REJECTED, reason='dup of idea#1', actor=ACTOR)
        ops.set_idea_status(s, running, IdeaStatus.PARKED, reason='redundant', actor=ACTOR)
        s.commit()
        notes = s.scalars(select(Comment).order_by(Comment.id)).all()
        assert [(n.target_id, n.author, n.kind, n.body) for n in notes] == [
            (idea, ACTOR.signature, CommentKind.NOTE, 'ready → parked: after the v2 evaluator'),
            (seed, ACTOR.signature, CommentKind.NOTE, 'seed → rejected: dup of idea#1'),
            (running, ACTOR.signature, CommentKind.NOTE, 'scheduled → parked: redundant'),
        ]
    assert [e[:2] for e in kinds(me, TargetKind.IDEA, idea)] == [('idea.status', ACTOR.signature)], 'the trigger, once'
    other = add_idea(me, CampaignId(1), 'other', IdeaStatus.SEED)
    busy = add_idea(me, CampaignId(1), 'busy', IdeaStatus.SCHEDULED)
    parked = run_idea(me, IdeaStatus.PARKED, RunStatus.RUNNING)
    scoring = run_idea(me, IdeaStatus.PARKED, RunStatus.SUCCEEDED, scoring=True)
    with me.session() as s:
        for target, status, error in [
            (parked, IdeaStatus.READY, f'idea#{parked} has an open run: run#\\d+ is running; park or reject it'),
            (parked, IdeaStatus.DONE, f'idea#{parked} has an open run'),
            (scoring, IdeaStatus.READY, f'idea#{scoring} has an open run: run#\\d+ is being scored'),
            (idea, IdeaStatus.PARKED, f'idea#{idea} is parked already'),
            (idea, IdeaStatus.SCHEDULED, 'scheduled is not set by hand: give proposed, ready, parked, rejected, done'),
            (idea, IdeaStatus.SEED, 'seed is not set by hand'),
            (other, IdeaStatus.READY, f'idea#{other} is a seed: research defines it first'),
            (busy, IdeaStatus.DONE, f'idea#{busy} is scheduled: its run is open; park or reject it'),
            (IdeaId(99), IdeaStatus.DONE, 'no idea#99'),
        ]:
            with pytest.raises(ops.Refused, match=error):
                ops.set_idea_status(s, target, status, reason='why', actor=ACTOR)
        with pytest.raises(ops.Refused, match='a reason is required'):
            ops.set_idea_status(s, idea, IdeaStatus.READY, reason=' ', actor=ACTOR)


def test_links_are_validated_kept_acyclic_and_leave_events(me: Database, campaign: Campaign, db: Database) -> None:
    a, b, c = (add_idea(me, CampaignId(1), t) for t in 'abc')
    running = add_idea(me, CampaignId(1), 'running', IdeaStatus.SCHEDULED)
    with me.session() as s:
        ops.link(s, a, LinkKind.AFTER, b, reason='b lands the parser', actor=ACTOR)
        ops.link(s, b, LinkKind.AFTER, c, reason='c first', actor=ACTOR)
        ops.link(s, running, LinkKind.SUPERSEDES, c, reason='sharper', actor=ACTOR)
        s.commit()
        links = s.scalars(select(IdeaLink).order_by(IdeaLink.idea_id)).all()
        assert [(lk.idea_id, lk.kind, lk.target_id) for lk in links] == [
            (a, LinkKind.AFTER, b),
            (b, LinkKind.AFTER, c),
            (running, LinkKind.SUPERSEDES, c),
        ]
    assert kinds(me, TargetKind.IDEA, a) == [
        ('idea.linked', ACTOR.signature, IdeaLinked(kind='idea.linked', target=b, link_kind=LinkKind.AFTER))
    ], 'a trigger'
    with me.session() as s:
        assert [c.body for c in s.scalars(select(Comment).where(Comment.target_id == a))] == [
            f'after idea#{b}: b lands the parser'
        ]
    with db.session() as s:
        s.add(Campaign(name='other', path='/other', config=s.get_one(Campaign, 1).config))
        s.commit()
    elsewhere = add_idea(me, CampaignId(2), 'elsewhere')
    with me.session() as s:
        for idea, kind, target, error in [
            (c, LinkKind.AFTER, a, f'a cycle: idea#{a} already links after idea#{c}'),
            (c, LinkKind.AFTER, c, f'idea#{c} cannot link to itself'),
            (a, LinkKind.AFTER, b, f'idea#{a} links after idea#{b} already'),
            (a, LinkKind.AFTER, elsewhere, f'idea#{elsewhere} is in another campaign'),
            (running, LinkKind.AFTER, a, f'idea#{running} is scheduled: its run is open: it started without idea#{a}'),
            (a, LinkKind.AFTER, IdeaId(99), 'no idea#99'),
        ]:
            with pytest.raises(ops.Refused, match=error):
                ops.link(s, idea, kind, target, reason='why', actor=ACTOR)
        ops.link(s, c, LinkKind.SUPERSEDES, a, reason='another kind closes no cycle', actor=ACTOR)
        with pytest.raises(ops.Refused, match='a reason is required'):
            ops.link(s, b, LinkKind.SUPERSEDES, a, reason='', actor=ACTOR)

        ops.unlink(s, a, LinkKind.AFTER, b, reason='b was rejected', actor=ACTOR)
        s.commit()
        assert s.get_one(Idea, a).links == []
        with pytest.raises(ops.Refused, match=f'idea#{a} has no after link to idea#{b}'):
            ops.unlink(s, a, LinkKind.AFTER, b, reason='again', actor=ACTOR)
        with pytest.raises(ops.Refused, match='a reason is required'):
            ops.unlink(s, b, LinkKind.AFTER, c, reason='', actor=ACTOR)
    assert kinds(me, TargetKind.IDEA, a)[-1] == (
        'idea.unlinked',
        ACTOR.signature,
        IdeaLinked(kind='idea.unlinked', target=b, link_kind=LinkKind.AFTER),
    )


def test_a_raw_sql_link_or_base_change_is_logged_like_the_commands(me: Database, campaign: Campaign) -> None:
    a, b = add_idea(me, CampaignId(1), 'a'), add_idea(me, CampaignId(1), 'b')
    with me.session() as s:
        s.execute(text(f"INSERT INTO idea_links (idea_id, target_id, kind) VALUES ({a}, {b}, 'after')"))
        s.execute(text(f'DELETE FROM idea_links WHERE idea_id = {a}'))
        s.execute(text(f'UPDATE ideas SET base_candidate_id = 1 WHERE id = {a}'))
        s.commit()
    assert kinds(me, TargetKind.IDEA, a) == [
        ('idea.linked', ACTOR.signature, IdeaLinked(kind='idea.linked', target=b, link_kind=LinkKind.AFTER)),
        ('idea.unlinked', ACTOR.signature, IdeaLinked(kind='idea.unlinked', target=b, link_kind=LinkKind.AFTER)),
        ('idea.base_candidate_id', ACTOR.signature, IdeaBaseChanged(old=None, new=CandidateId(1))),
    ]


def test_a_base_pins_a_candidate_of_the_campaign_or_clears_the_pin(me: Database, campaign: Campaign) -> None:
    idea = add_idea(me, CampaignId(1), 'pinned')
    with me.session() as s:
        assert ops.set_base(s, idea, CandidateId(1), reason='from the baseline', actor=ACTOR) is None
        s.commit()
        assert ops.set_base(s, idea, None, reason='follow the head', actor=ACTOR) == 1
        s.commit()
        assert s.get_one(Idea, idea).base_candidate_id is None
        with pytest.raises(ops.Refused, match=f'idea#{idea} starts from the head already'):
            ops.set_base(s, idea, None, reason='again', actor=ACTOR)
        ops.set_base(s, idea, CandidateId(1), reason='pin', actor=ACTOR)
        with pytest.raises(ops.Refused, match=f'idea#{idea} starts from candidate#1 already'):
            ops.set_base(s, idea, CandidateId(1), reason='again', actor=ACTOR)
        with pytest.raises(ops.Refused, match='no candidate#99 in campaign demo'):
            ops.set_base(s, idea, CandidateId(99), reason='why', actor=ACTOR)
        with pytest.raises(ops.Refused, match='a reason is required'):
            ops.set_base(s, idea, None, reason='', actor=ACTOR)
    assert kinds(me, TargetKind.IDEA, idea) == [
        ('idea.base_candidate_id', ACTOR.signature, IdeaBaseChanged(old=None, new=CandidateId(1))),
        ('idea.base_candidate_id', ACTOR.signature, IdeaBaseChanged(old=CandidateId(1), new=None)),
    ]
    with me.session() as s:
        assert [c.body for c in s.scalars(select(Comment).order_by(Comment.id))][:2] == [
            'base the head → candidate#1: from the baseline',
            'base candidate#1 → the head: follow the head',
        ]


def test_a_track_must_be_in_the_config_and_its_move_says_why(me: Database, campaign: Campaign) -> None:
    with me.session() as s:
        two = load_campaign_config(
            Path(campaign.path),
            metrics=[
                {'name': 'score', 'direction': 'higher', 'role': 'primary', 'margin': 1},
                {'name': 'size', 'direction': 'lower', 'role': 'primary', 'margin': 1},
            ],
            tracks={'score': {'share': 0.5}, 'size': {'share': 0.5}},
        )
        s.get_one(Campaign, 1).config = two
        s.commit()
    idea = add_idea(me, CampaignId(1), 'smaller')
    running = add_idea(me, CampaignId(1), 'running', IdeaStatus.SCHEDULED)
    with me.session() as s:
        assert ops.set_track(s, idea, TrackName('size'), reason='it shrinks the output', actor=ACTOR) == 'score'
        s.commit()
        [note] = s.scalars(select(Comment)).all()
        assert note.body == 'track score → size: it shrinks the output'
        for target, track, error in [
            (idea, TrackName('size'), f'idea#{idea} is on track size already'),
            (idea, TrackName('speed'), "demo has tracks score, size: there is no track 'speed'"),
            (
                running,
                TrackName('size'),
                f'idea#{running} is scheduled: its run is open: its commits are judged on track score',
            ),
            (IdeaId(99), TrackName('size'), 'no idea#99'),
        ]:
            with pytest.raises(ops.Refused, match=error):
                ops.set_track(s, target, track, reason='why', actor=ACTOR)
        with pytest.raises(ops.Refused, match='a reason is required'):
            ops.set_track(s, idea, TrackName('score'), reason='', actor=ACTOR)
        with pytest.raises(ops.Refused, match='demo has tracks score, size: name one with --track'):
            ops.track_for(s.get_one(Campaign, 1), None)
    assert [(k, a, p) for k, a, p in kinds(me, TargetKind.IDEA, idea) if k == 'idea.track'] == [
        ('idea.track', ACTOR.signature, IdeaTrackChanged(old=TrackName('score'), new=TrackName('size')))
    ]


def parents(s: Session) -> dict[str, str | None]:
    """Each campaign's name, with the name of its parent."""
    names = {p.id: p.name for p in s.scalars(select(Campaign))}
    return {p.name: names.get(p.parent_id) if p.parent_id else None for p in s.scalars(select(Campaign))}


def test_a_campaign_has_one_parent_never_itself_or_a_descendant_and_each_change_is_an_event(me: Database) -> None:
    with me.session() as s:
        a = s.get_one(Campaign, 1)
        b, c, d = (Campaign(name=name, path=f'/{name}', config=a.config) for name in 'bcd')
        s.add_all([a, b, c, d])
        s.flush()
        assert (a.type, a.parent_id) == ('optimization', None), 'an optimization campaign, a root'
        assert ops.set_parent(s, b.id, a.id) is b
        ops.set_parent(s, c.id, b.id)
        ops.set_parent(s, d.id, a.id)
        assert parents(s) == {'demo': None, 'b': 'demo', 'c': 'b', 'd': 'demo'}
        for child, parent in ((a, c), (a, a)):
            with pytest.raises(ops.Refused, match=f'^{child.name} cannot be under {parent.name}: .*its descendants'):
                ops.set_parent(s, child.id, parent.id)
        assert a.parent_id is None and s.get(Campaign, c.id) is c, 'a refused change leaves the session usable'
        with pytest.raises(ops.Refused, match='^c is under b already'):
            ops.set_parent(s, c.id, b.id)
        with pytest.raises(ops.Refused, match='^no campaign#9'):
            ops.set_parent(s, c.id, CampaignId(9))
        ops.set_parent(s, c.id, None)
        with pytest.raises(ops.Refused, match='^c is a root already'):
            ops.set_parent(s, c.id, None)
        s.commit()
        assert parents(s) == {'demo': None, 'b': 'demo', 'c': None, 'd': 'demo'}
        changes = in_order(s, CampaignParentChanged)
        assert [(e.target_id, e.actor, change) for e, change in changes] == [
            (b.id, ACTOR.signature, CampaignParentChanged(old=None, new=a.id)),
            (c.id, ACTOR.signature, CampaignParentChanged(old=None, new=b.id)),
            (d.id, ACTOR.signature, CampaignParentChanged(old=None, new=a.id)),
            (c.id, ACTOR.signature, CampaignParentChanged(old=b.id, new=None)),
        ], 'the trigger logs each change, and a refused one leaves no event'
        with pytest.raises(IntegrityError, match='FOREIGN KEY'):
            s.execute(text('DELETE FROM campaigns WHERE id = :id'), {'id': a.id})


def test_the_database_refuses_a_cycle_of_parents(me: Database, tmp_path: Path) -> None:
    with closing(sqlite3.connect(tmp_path / 'piloop.db')) as con:
        con.execute('PRAGMA foreign_keys = ON')
        con.execute("INSERT INTO campaigns (name, path, config) VALUES ('b', '/b', '{}')")
        con.execute('UPDATE campaigns SET parent_id = 1 WHERE id = 2')
        for statement in (
            'UPDATE campaigns SET parent_id = 2 WHERE id = 1',
            'UPDATE campaigns SET parent_id = id WHERE id = 1',
            "INSERT INTO campaigns (id, name, path, config, parent_id) VALUES (9, 'c', '/c', '{}', 9)",
        ):
            with pytest.raises(sqlite3.IntegrityError, match='^campaigns.parent_id: a campaign cannot be under itself'):
                con.execute(statement)
        with pytest.raises(sqlite3.IntegrityError, match='FOREIGN KEY'):
            con.execute('UPDATE campaigns SET parent_id = 7 WHERE id = 1')
        assert con.execute('SELECT id, type, parent_id FROM campaigns').fetchall() == [
            (1, 'optimization', None),
            (2, 'optimization', 1),
        ]
