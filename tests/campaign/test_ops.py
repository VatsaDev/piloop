"""The control operations: each validates, changes the state and leaves its event, in the caller's transaction."""

import sqlite3
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path

import pytest
from pydantic import ValidationError
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

from piloop.campaign import ops
from piloop.config import load_project_config
from piloop.db.events import emit
from piloop.db.models import (
    Candidate,
    Comment,
    CommentKind,
    Event,
    Idea,
    IdeaLink,
    IdeaStatus,
    LinkKind,
    Measurement,
    Pause,
    Project,
    ProjectStatus,
    Run,
    RunStatus,
    TargetKind,
    Verdict,
)
from piloop.db.session import Database
from piloop.scoring.ledger import DAEMON, REJUDGE_FAILED, REJUDGE_REQUESTED, head, rejudges

ACTOR = 'orchestrator'


@pytest.fixture
def me(db: Database, project: Project, tmp_path: Path) -> Iterator[Database]:
    """The project's database, opened as the orchestrator: the change-log triggers sign its writes with that actor."""
    database = Database(f'sqlite:///{tmp_path / "piloop.db"}', actor=ACTOR)
    yield database
    database.close()


def kinds(db: Database, target_kind: TargetKind, target_id: int) -> list[tuple[str, str, dict[str, object]]]:
    with db.session() as s:
        rows = s.scalars(
            select(Event).where(Event.target_kind == target_kind, Event.target_id == target_id).order_by(Event.id)
        )
        return [(e.kind, e.actor, e.payload) for e in rows]


def add_idea(db: Database, project_id: int, title: str, status: IdeaStatus = IdeaStatus.READY) -> int:
    with db.session() as s:
        idea = Idea(project_id=project_id, track='score', title=title, hypothesis=title, status=status, created_by='t')
        s.add(idea)
        s.commit()
        return idea.id


def run_idea(db: Database, status: IdeaStatus, run: RunStatus, *, scoring: bool = False) -> int:
    """An idea in ``status`` with one run in ``run``: a status set by hand while the run is open, or scored."""
    idea_id = add_idea(db, 1, f'{status} with a {run} run {scoring}', status)
    with db.session() as s:
        s.add(Run(idea_id=idea_id, attempt=1, base_candidate_id=1, harness='h', model='m', status=run, scoring=scoring))
        s.commit()
    return idea_id


def run_candidate(
    db: Database,
    project: Project,
    verdict: Verdict,
    *,
    by: str | None = DAEMON,
    parent: int = 1,
    branch: bool = False,
) -> int:
    """A candidate of a run of a new idea from ``parent``, with the daemon's ``verdict`` and one measurement (score 0)."""
    idea_id = add_idea(db, project.id, f'tried {verdict} {parent} {branch} {by}')
    with db.session() as s:
        run = Run(idea_id=idea_id, attempt=1, base_candidate_id=parent, harness='script', model='m', branch=branch)
        s.add(run)
        s.flush()
        c = Candidate(
            project_id=project.id,
            run_id=run.id,
            parent_id=parent,
            tree_sha=f'tree {idea_id}',
            commit_sha=f'commit {idea_id}',
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
        project = ops.pause(s, 1, evals=False, reason=' wait for evaluator v2 ')
        s.commit()
        assert (project.paused, project.paused_reason) == (Pause.LAUNCHES, 'wait for evaluator v2')
    with me.session() as s, pytest.raises(ops.Refused, match=r'paused already \(launches\): wait for evaluator v2'):
        ops.pause(s, 1, evals=False, reason='again')
    with me.session() as s:
        ops.pause(s, 1, evals=True, reason='and the evaluations')
        s.commit()
    with me.session() as s:
        assert ops.resume(s, 1) is Pause.ALL
        s.commit()
        assert (s.get_one(Project, 1).paused, s.get_one(Project, 1).paused_reason) == (None, None)
    assert [e for e in kinds(me, TargetKind.PROJECT, 1) if e[0] == 'project.paused'] == [
        ('project.paused', ACTOR, {'old': None, 'new': 'launches', 'paused_reason': 'wait for evaluator v2'}),
        ('project.paused', ACTOR, {'old': 'launches', 'new': 'all', 'paused_reason': 'and the evaluations'}),
        ('project.paused', ACTOR, {'old': 'all', 'new': None, 'paused_reason': None}),
    ], 'the trigger logs each change, signed by the connection, and nothing else does'
    with me.session() as s:
        assert s.scalars(select(Comment)).all() == [], 'a pause is not an escalation'
        with pytest.raises(ops.Refused, match='project demo is not paused'):
            ops.resume(s, 1)
        with pytest.raises(ops.Refused, match='a reason is required'):
            ops.pause(s, 1, evals=False, reason='  ')
        with pytest.raises(ops.Refused, match='no project#9'):
            ops.pause(s, 9, evals=False, reason='why')


def test_a_raw_pause_needs_a_reason_and_a_known_scope(me: Database, tmp_path: Path) -> None:
    with closing(sqlite3.connect(tmp_path / 'piloop.db')) as con:
        with pytest.raises(sqlite3.IntegrityError, match='projects.paused and projects.paused_reason go together'):
            con.execute("UPDATE projects SET paused = 'launches' WHERE id = 1")
        with pytest.raises(sqlite3.IntegrityError, match='CHECK constraint failed'):
            con.execute("UPDATE projects SET paused = 'forever', paused_reason = 'r' WHERE id = 1")
    with me.session() as s:
        s.get_one(Project, 1).paused_reason = 'no pause'
        with pytest.raises(IntegrityError, match='go together'):
            s.commit()


def test_only_a_sweeping_agent_files_its_ideas_for_review_while_the_project_is_paused(me: Database) -> None:
    with me.session() as s:
        project = s.get_one(Project, 1)
        assert ops.review_first(project, 'research#4', new=False) is None, 'not paused'
        assert ops.review_first(project, 'research#4', new=True) == '[roles.research] max_ideas = 0', 'the test file'
        ops.pause(s, 1, evals=False, reason='why')
        assert ops.review_first(project, 'watchdog#5', new=False) == 'project demo is paused'
        assert (
            ops.review_first(project, ACTOR, new=True) is None and ops.review_first(project, 'human', new=True) is None
        )
        assert ops.review_first(project, 'worker#6', new=True) is None, 'a worker files no ideas anyway'


def test_a_verdict_override_is_the_actors_with_the_reason_in_its_event(me: Database, project: Project) -> None:
    c = run_candidate(me, project, Verdict.DISCARD)
    with me.session() as s:
        assert ops.set_verdict(s, c, Verdict.KEEP, reason=' measured by hand: +3 ', actor=ACTOR) is Verdict.DISCARD
        s.commit()
        candidate = s.get_one(Candidate, c)
        assert (candidate.verdict, candidate.verdict_by, candidate.verdict_reason) == (
            Verdict.KEEP,
            ACTOR,
            'measured by hand: +3',
        )
    payload = {'old': 'discard', 'new': 'keep', 'verdict_by': ACTOR, 'verdict_reason': 'measured by hand: +3'}
    assert kinds(me, TargetKind.CANDIDATE, c) == [('candidate.verdict', ACTOR, payload)]
    pending = run_candidate(me, project, Verdict.PENDING, by=None)
    with me.session() as s:
        with pytest.raises(ops.Refused, match=r'candidate#\d+ is keep already \(measured by hand: \+3\)'):
            ops.set_verdict(s, c, Verdict.KEEP, reason='again', actor=ACTOR)
        with pytest.raises(ops.Refused, match='reference is not a judgement: give keep, interesting, discard, crash'):
            ops.set_verdict(s, c, Verdict.REFERENCE, reason='why', actor=ACTOR)
        with pytest.raises(ops.Refused, match=f'candidate#{pending} is pending: the daemon judges it now'):
            ops.set_verdict(s, pending, Verdict.KEEP, reason='why', actor=ACTOR)
        with pytest.raises(ops.Refused, match='a reason is required'):
            ops.set_verdict(s, c, Verdict.CRASH, reason='', actor=ACTOR)
        with pytest.raises(ops.Refused, match='no candidate#99'):
            ops.set_verdict(s, 99, Verdict.CRASH, reason='why', actor=ACTOR)
        for state in (Verdict.REFERENCE, Verdict.UNMEASURED, Verdict.RECOVERED):
            s.get_one(Candidate, pending).verdict = state
            with pytest.raises(ops.Refused, match=f'candidate#{pending} is {state}, not a judgement'):
                ops.set_verdict(s, pending, Verdict.KEEP, reason='why', actor=ACTOR)


def test_a_hand_keep_must_build_on_the_head_unless_its_run_is_a_branch(me: Database, project: Project) -> None:
    """The head is the newest trunk keep, so a keep from an older bar would drop what the head kept since."""
    old = run_candidate(me, project, Verdict.DISCARD)
    trunk = run_candidate(me, project, Verdict.KEEP)
    on_head = run_candidate(me, project, Verdict.DISCARD, parent=trunk)
    after_it = run_candidate(me, project, Verdict.INTERESTING, parent=on_head)
    branch = run_candidate(me, project, Verdict.DISCARD, branch=True)
    with me.session() as s:
        with pytest.raises(ops.Refused, match=f'candidate#{old} does not build on the head candidate#{trunk}'):
            ops.set_verdict(s, old, Verdict.KEEP, reason='measured by hand', actor=ACTOR)
        ops.set_verdict(s, old, Verdict.INTERESTING, reason='a lead', actor=ACTOR)
        ops.set_verdict(s, after_it, Verdict.KEEP, reason='its parent builds on the head', actor=ACTOR)
        ops.set_verdict(s, branch, Verdict.KEEP, reason='a branch keep never moves the head', actor=ACTOR)
        s.commit()
        now = head(s, s.get_one(Project, 1))
        assert now is not None and now.id == after_it, 'the newest trunk keep'


def test_a_measurement_by_hand_is_recorded_and_asks_the_daemon_to_judge_its_candidate_again(
    me: Database, project: Project
) -> None:
    c = run_candidate(me, project, Verdict.CRASH)
    report = ops.Report(metrics={'score': 3.0, 'size': 9.0}, seconds=12.5)
    with me.session() as s:
        added = ops.add_measurement(s, c, report, actor=ACTOR)
        s.commit()
        measurement = added.measurement
        assert (added.rejudged, added.effect) == (True, 'the daemon judges it again with all its measurements')
        assert (measurement.evaluator_id, measurement.metrics, measurement.seconds) == (1, report.metrics, 12.5)
        assert [x.id for x in rejudges(s, s.get_one(Project, 1))] == [c]
    assert kinds(me, TargetKind.MEASUREMENT, measurement.id) == [
        (
            'measurement.recorded',
            ACTOR,
            {'candidate': c, 'metrics': {'score': 3.0, 'size': 9.0}, 'error': None, 'run': None},
        )
    ], 'shaped like the daemon event'
    assert kinds(me, TargetKind.CANDIDATE, c) == [(REJUDGE_REQUESTED, ACTOR, {'measurement': measurement.id})]
    with me.session() as s:
        emit(s, 'candidate.judged', TargetKind.CANDIDATE, c)
        s.commit()
        assert rejudges(s, s.get_one(Project, 1)) == [], 'a later judgement answers the request'
        ops.add_measurement(s, c, ops.Report(metrics={}, error='timeout'), actor=ACTOR)
        s.commit()
        assert [x.id for x in rejudges(s, s.get_one(Project, 1))] == [c], 'a failed one counts too'
        emit(s, REJUDGE_FAILED, TargetKind.CANDIDATE, c, payload={'error': 'gone'})
        s.commit()
        assert rejudges(s, s.get_one(Project, 1)) == [], 'so does a failure'


def test_a_measurement_by_hand_of_a_keep_or_of_someones_verdict_leaves_the_verdict(
    me: Database, project: Project
) -> None:
    mine = run_candidate(me, project, Verdict.DISCARD, by='human')
    with me.session() as s:
        keep = ops.add_measurement(s, 1, ops.Report(metrics={'score': 1.0, 'size': 5.0}), actor=ACTOR)
        assert (keep.rejudged, keep.effect) == (
            False,
            'its keep stays; the runs that start from it must beat its new mean',
        )
        human = ops.add_measurement(s, mine, ops.Report(metrics={'score': 1.0, 'size': 5.0}), actor=ACTOR)
        assert (human.rejudged, human.effect) == (False, 'its verdict stays: discard, set by human')
        s.commit()
        assert s.scalars(select(Event).where(Event.kind == REJUDGE_REQUESTED)).all() == []
        emit(s, REJUDGE_REQUESTED, TargetKind.CANDIDATE, mine)
        s.commit()
        assert rejudges(s, s.get_one(Project, 1)) == [], 'a request for a verdict that someone set is dropped'
        with pytest.raises(ops.Refused, match='the report has no size: give every metric, or the error'):
            ops.add_measurement(s, 1, ops.Report(metrics={'score': 1.0}), actor=ACTOR)
        with pytest.raises(ops.Refused, match='piloop.toml has no metric accuracy: its metrics are score, size'):
            ops.add_measurement(s, 1, ops.Report(metrics={'score': 1.0, 'size': 5.0, 'accuracy': 1.0}), actor=ACTOR)
        with pytest.raises(ops.Refused, match='no candidate#99'):
            ops.add_measurement(s, 99, ops.Report(metrics={}), actor=ACTOR)
        s.get_one(Project, 1).evaluator_id = None
        with pytest.raises(ops.Refused, match='project demo has no evaluator yet'):
            ops.add_measurement(s, 1, ops.Report(metrics={'score': 1.0, 'size': 5.0}), actor=ACTOR)


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
    me: Database, project: Project
) -> None:
    crashed = run_candidate(me, project, Verdict.CRASH)
    pending = run_candidate(me, project, Verdict.PENDING, by=None)
    report = ops.Report(metrics={'score': 2.0, 'size': 4.0})
    with me.session() as s:
        with pytest.raises(ops.Refused, match=f'candidate#{pending} is pending: the daemon judges it now'):
            ops.add_measurement(s, pending, report, actor=ACTOR)
        p = s.get_one(Project, 1)
        p.status, p.stopped_reason = ProjectStatus.STOPPED, 'max_runs: 3 runs'
        assert ops.add_measurement(s, crashed, report, actor=ACTOR).effect == (
            'the daemon judges it again with all its measurements once project demo is active again'
            ' (it stopped: max_runs: 3 runs)'
        )
        p.status, p.stopped_reason = ProjectStatus.ACTIVE, None
        ops.pause(s, 1, evals=True, reason='why')
        assert ops.add_measurement(s, crashed, report, actor=ACTOR).effect.endswith(
            'once project demo is resumed (its evaluations are paused)'
        )
        ops.resume(s, 1)
        p.config = {**p.config, 'spend': {'command': ['true'], 'limit': 10.0}}
        assert ops.add_measurement(s, crashed, report, actor=ACTOR).effect.endswith(
            'once the spend guard lets it (spend not read yet)'
        )


def test_an_idea_status_takes_a_reason_in_a_note_and_refuses_what_is_not_set_by_hand(
    me: Database, project: Project
) -> None:
    idea = add_idea(me, 1, 'an idea')
    seed = add_idea(me, 1, 'a hunch', IdeaStatus.SEED)
    running = add_idea(me, 1, 'running', IdeaStatus.SCHEDULED)
    with me.session() as s:
        assert ops.set_idea_status(s, idea, IdeaStatus.PARKED, reason='after the v2 evaluator', actor=ACTOR) is (
            IdeaStatus.READY
        )
        ops.set_idea_status(s, seed, IdeaStatus.REJECTED, reason='dup of idea#1', actor=ACTOR)
        ops.set_idea_status(s, running, IdeaStatus.PARKED, reason='redundant', actor=ACTOR)
        s.commit()
        notes = s.scalars(select(Comment).order_by(Comment.id)).all()
        assert [(n.target_id, n.author, n.kind, n.body) for n in notes] == [
            (idea, ACTOR, CommentKind.NOTE, 'ready → parked: after the v2 evaluator'),
            (seed, ACTOR, CommentKind.NOTE, 'seed → rejected: dup of idea#1'),
            (running, ACTOR, CommentKind.NOTE, 'scheduled → parked: redundant'),
        ]
    assert [e[:2] for e in kinds(me, TargetKind.IDEA, idea)] == [('idea.status', ACTOR)], 'the trigger, once'
    other = add_idea(me, 1, 'other', IdeaStatus.SEED)
    busy = add_idea(me, 1, 'busy', IdeaStatus.SCHEDULED)
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
            (99, IdeaStatus.DONE, 'no idea#99'),
        ]:
            with pytest.raises(ops.Refused, match=error):
                ops.set_idea_status(s, target, status, reason='why', actor=ACTOR)
        with pytest.raises(ops.Refused, match='a reason is required'):
            ops.set_idea_status(s, idea, IdeaStatus.READY, reason=' ', actor=ACTOR)


def test_links_are_validated_kept_acyclic_and_leave_events(me: Database, project: Project, db: Database) -> None:
    a, b, c = (add_idea(me, 1, t) for t in 'abc')
    running = add_idea(me, 1, 'running', IdeaStatus.SCHEDULED)
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
    assert kinds(me, TargetKind.IDEA, a) == [('idea.linked', ACTOR, {'target': b, 'kind': 'after'})], 'a trigger'
    with me.session() as s:
        assert [c.body for c in s.scalars(select(Comment).where(Comment.target_id == a))] == [
            f'after idea#{b}: b lands the parser'
        ]
    with db.session() as s:
        s.add(Project(name='other', path='/other', config=s.get_one(Project, 1).config))
        s.commit()
    elsewhere = add_idea(me, 2, 'elsewhere')
    with me.session() as s:
        for idea, kind, target, error in [
            (c, LinkKind.AFTER, a, f'a cycle: idea#{a} already links after idea#{c}'),
            (c, LinkKind.AFTER, c, f'idea#{c} cannot link to itself'),
            (a, LinkKind.AFTER, b, f'idea#{a} links after idea#{b} already'),
            (a, LinkKind.AFTER, elsewhere, f'idea#{elsewhere} is in another project'),
            (running, LinkKind.AFTER, a, f'idea#{running} is scheduled: its run is open: it started without idea#{a}'),
            (a, LinkKind.AFTER, 99, 'no idea#99'),
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
    assert kinds(me, TargetKind.IDEA, a)[-1] == ('idea.unlinked', ACTOR, {'target': b, 'kind': 'after'})


def test_a_raw_sql_link_or_base_change_is_logged_like_the_commands(me: Database, project: Project) -> None:
    a, b = add_idea(me, 1, 'a'), add_idea(me, 1, 'b')
    with me.session() as s:
        s.execute(text(f"INSERT INTO idea_links (idea_id, target_id, kind) VALUES ({a}, {b}, 'after')"))
        s.execute(text(f'DELETE FROM idea_links WHERE idea_id = {a}'))
        s.execute(text(f'UPDATE ideas SET base_candidate_id = 1 WHERE id = {a}'))
        s.commit()
    assert kinds(me, TargetKind.IDEA, a) == [
        ('idea.linked', ACTOR, {'target': b, 'kind': 'after'}),
        ('idea.unlinked', ACTOR, {'target': b, 'kind': 'after'}),
        ('idea.base_candidate_id', ACTOR, {'old': None, 'new': 1}),
    ]


def test_a_base_pins_a_candidate_of_the_project_or_clears_the_pin(me: Database, project: Project) -> None:
    idea = add_idea(me, 1, 'pinned')
    with me.session() as s:
        assert ops.set_base(s, idea, 1, reason='from the baseline', actor=ACTOR) is None
        s.commit()
        assert ops.set_base(s, idea, None, reason='follow the head', actor=ACTOR) == 1
        s.commit()
        assert s.get_one(Idea, idea).base_candidate_id is None
        with pytest.raises(ops.Refused, match=f'idea#{idea} starts from the head already'):
            ops.set_base(s, idea, None, reason='again', actor=ACTOR)
        ops.set_base(s, idea, 1, reason='pin', actor=ACTOR)
        with pytest.raises(ops.Refused, match=f'idea#{idea} starts from candidate#1 already'):
            ops.set_base(s, idea, 1, reason='again', actor=ACTOR)
        with pytest.raises(ops.Refused, match='no candidate#99 in project demo'):
            ops.set_base(s, idea, 99, reason='why', actor=ACTOR)
        with pytest.raises(ops.Refused, match='a reason is required'):
            ops.set_base(s, idea, None, reason='', actor=ACTOR)
    assert kinds(me, TargetKind.IDEA, idea) == [
        ('idea.base_candidate_id', ACTOR, {'old': None, 'new': 1}),
        ('idea.base_candidate_id', ACTOR, {'old': 1, 'new': None}),
    ]
    with me.session() as s:
        assert [c.body for c in s.scalars(select(Comment).order_by(Comment.id))][:2] == [
            'base the head → candidate#1: from the baseline',
            'base candidate#1 → the head: follow the head',
        ]


def test_a_track_must_be_in_the_config_and_its_move_says_why(me: Database, project: Project) -> None:
    with me.session() as s:
        two = load_project_config(
            Path(project.path),
            metrics=[
                {'name': 'score', 'direction': 'higher', 'role': 'primary', 'margin': 1},
                {'name': 'size', 'direction': 'lower', 'role': 'primary', 'margin': 1},
            ],
            tracks={'score': {'share': 0.5}, 'size': {'share': 0.5}},
        )
        s.get_one(Project, 1).config = two.model_dump(mode='json')
        s.commit()
    idea = add_idea(me, 1, 'smaller')
    running = add_idea(me, 1, 'running', IdeaStatus.SCHEDULED)
    with me.session() as s:
        assert ops.set_track(s, idea, 'size', reason='it shrinks the output', actor=ACTOR) == 'score'
        s.commit()
        [note] = s.scalars(select(Comment)).all()
        assert note.body == 'track score → size: it shrinks the output'
        for target, track, error in [
            (idea, 'size', f'idea#{idea} is on track size already'),
            (idea, 'speed', "demo has tracks score, size: there is no track 'speed'"),
            (running, 'size', f'idea#{running} is scheduled: its run is open: its commits are judged on track score'),
            (99, 'size', 'no idea#99'),
        ]:
            with pytest.raises(ops.Refused, match=error):
                ops.set_track(s, target, track, reason='why', actor=ACTOR)
        with pytest.raises(ops.Refused, match='a reason is required'):
            ops.set_track(s, idea, 'score', reason='', actor=ACTOR)
        with pytest.raises(ops.Refused, match='demo has tracks score, size: name one with --track'):
            ops.track_for(s.get_one(Project, 1), None)
    assert [(k, a, p) for k, a, p in kinds(me, TargetKind.IDEA, idea) if k == 'idea.track'] == [
        ('idea.track', ACTOR, {'old': 'score', 'new': 'size'})
    ]
