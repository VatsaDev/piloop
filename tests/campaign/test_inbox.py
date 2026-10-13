"""The inbox: comments with a priority, scoped to a project through every target, until someone acknowledges them."""

import re

from inline_snapshot import snapshot
from sqlalchemy import func, select

from piloop.campaign import inbox
from piloop.db.events import ANOMALY_EVENT, emit
from piloop.db.models import (
    Comment,
    CommentKind,
    Event,
    Idea,
    IdeaStatus,
    Measurement,
    Project,
    ProjectStatus,
    Run,
    TargetKind,
    Worker,
    WorkerKind,
    WorkerState,
    utcnow,
)
from piloop.db.session import Database
from piloop.scoring.ledger import SKIPPED
from tests.reports.test_history import idea
from tests.reports.test_overview import render


def bodies(rows: list[tuple[Comment, Project | None]]) -> list[str]:
    return [c.body for c, _ in rows]


def test_items_scope_to_the_project_through_every_target(db: Database, project: Project) -> None:
    with db.session() as s:
        other = Project(name='other', path='/other', config=project.config)
        s.add(other)
        s.flush()
        mine = idea(s, project, 'mine', IdeaStatus.READY)
        theirs = idea(s, other, 'theirs', IdeaStatus.READY)
        run = Run(idea=mine, attempt=1, base_candidate_id=1, harness='h', model='m')
        s.add(run)
        s.add(Worker(kind=WorkerKind.WATCHDOG, project_id=project.id, state=WorkerState.EXITED))
        s.flush()
        s.add(Measurement(candidate_id=1, evaluator_id=1, metrics={'score': 1}))
        s.flush()
        escalation = dict(author='watchdog', kind=CommentKind.ESCALATION, priority=1)
        s.add_all(
            [
                Comment(target_kind=TargetKind.PROJECT, target_id=project.id, body='on project', **escalation),
                Comment(target_kind=TargetKind.IDEA, target_id=mine.id, body='on idea', **escalation),
                Comment(target_kind=TargetKind.RUN, target_id=run.id, body='on run', **escalation),
                Comment(target_kind=TargetKind.CANDIDATE, target_id=1, body='on candidate', **escalation),
                Comment(target_kind=TargetKind.MEASUREMENT, target_id=1, body='on measurement', **escalation),
                Comment(target_kind=TargetKind.WORKER, target_id=1, body='on worker', **escalation),
                Comment(target_kind=TargetKind.IDEA, target_id=theirs.id, body='other project', **escalation),
                Comment(target_kind=TargetKind.PROJECT, target_id=other.id, body='other project too', **escalation),
                Comment(target_kind=TargetKind.RUN, target_id=run.id, author='w', kind=CommentKind.FINDING, body='no'),
                Comment(
                    target_kind=TargetKind.IDEA, target_id=mine.id, author='h', kind=CommentKind.DECISION, body='park'
                ),
                Comment(target_kind=TargetKind.COMMENT, target_id=1, body='on a comment of the project', **escalation),
                Comment(target_kind=TargetKind.COMMENT, target_id=7, body='on a comment of the other', **escalation),
                Comment(target_kind=TargetKind.COMMENT, target_id=11, body="two levels deep: nobody's", **escalation),
            ]
        )
        s.commit()
        assert bodies(inbox.items(s, project_id=project.id)) == [
            'on project',
            'on idea',
            'on run',
            'on candidate',
            'on measurement',
            'on worker',
            'on a comment of the project',
        ], 'the forum (no priority) is not in it'
        assert bodies(inbox.items(s, project_id=other.id)) == [
            'other project',
            'other project too',
            'on a comment of the other',
        ]
        everything = inbox.items(s)
        assert len(everything) == 11, 'unfiltered, the reply to a reply is in it too'
        assert [p.name if p else None for _, p in everything][-3:] == ['demo', 'other', None]
        lines = [render(db, p).split('### Recent events\n')[1].splitlines()[0] for p in (project, other)]
        recent = [re.sub(r' \d+[smhd] ago', '', line) for line in lines]
        assert recent == snapshot(
            [
                '- comment.decision comment#10 · comment.finding comment#9 · comment.escalation comment#6 · comment.escalation comment#5 · comment.escalation comment#4',
                '- comment.escalation comment#8 · comment.escalation comment#7',
            ]
        ), "each project's escalation events and none of the other's (project_of serves events too)"


def test_most_urgent_first_narrowed_by_kind_until_acknowledged(db: Database, project: Project) -> None:
    on_project = dict(target_kind=TargetKind.PROJECT, target_id=project.id, author='pi')
    with db.session() as s:
        s.add_all(
            [
                Comment(kind=CommentKind.DECISION, priority=3, body='requeued idea#4', **on_project),
                Comment(kind=CommentKind.ESCALATION, priority=1, body='SSO expired', **on_project),
                Comment(kind=CommentKind.DECISION, priority=0, body='lowered the floor', **on_project),
                Comment(kind=CommentKind.FINDING, priority=3, body='half the sweeps add nothing', **on_project),
            ]
        )
        s.commit()
        assert bodies(inbox.items(s)) == [
            'lowered the floor',
            'SSO expired',
            'requeued idea#4',
            'half the sweeps add nothing',
        ]
        assert bodies(inbox.items(s, kinds=[CommentKind.DECISION])) == ['lowered the floor', 'requeued idea#4']
        assert inbox.ack(s, [3, 1, 3, 99], 'human') == [
            'comment#3 acknowledged',
            'comment#1 acknowledged',
            'comment#3 was acknowledged already, by human',
            'comment#99 is not in the inbox',
        ]
        assert bodies(inbox.items(s)) == ['SSO expired', 'half the sweeps add nothing']
        assert bodies(inbox.items(s, acked=True)) == [
            'lowered the floor',
            'SSO expired',
            'requeued idea#4',
            'half the sweeps add nothing',
        ]
        acked = s.scalars(select(Event).where(Event.kind == 'comment.acked_at').order_by(Event.id)).all()
        assert [(e.target_id, e.payload['acked_by']) for e in acked] == [(3, 'human'), (1, 'human')]


def test_thread_follows_citations_both_ways_and_the_replies_not_the_rest_of_the_target(
    db: Database, project: Project
) -> None:
    with db.session() as s:
        on_project = dict(target_kind=TargetKind.PROJECT, target_id=project.id)
        s.add_all(
            [
                Comment(author='research#2', kind=CommentKind.FINDING, body='a sweep', **on_project),
                Comment(
                    author='research#2', kind=CommentKind.ESCALATION, priority=1, body='floor exact?', **on_project
                ),
                Comment(target_kind=TargetKind.COMMENT, target_id=2, author='pi', body='looking'),
                Comment(
                    author='pi', kind=CommentKind.DECISION, priority=0, body='floor 2687\nRe: comment#2', **on_project
                ),
                Comment(
                    author='pi', kind=CommentKind.DECISION, priority=3, body='see comment#20, not #2', **on_project
                ),
                Comment(target_kind=TargetKind.IDEA, target_id=4, author='pi', body='as comment#4 decided, comment#2.'),
            ]
        )
        s.commit()
        escalation, decision = s.get_one(Comment, 2), s.get_one(Comment, 4)
        assert [c.body for c in inbox.thread(s, escalation)] == [
            'looking',
            'floor 2687\nRe: comment#2',
            'as comment#4 decided, comment#2.',
        ]
        assert [c.id for c in inbox.thread(s, decision)] == [2, 6], 'what it answers, and who cites it'


def test_news_is_what_happened_since_the_cursor_that_the_orchestrator_did_not_write_or_see(
    db: Database, project: Project
) -> None:
    def on(p: Project) -> dict[str, object]:
        return dict(target_kind=TargetKind.PROJECT, target_id=p.id)

    with db.session() as s:
        other = Project(name='other', path='/other', config=project.config)
        s.add(other)
        s.add(Comment(author='watchdog#1', kind=CommentKind.ESCALATION, priority=1, body='before', **on(project)))
        s.commit()
        cursor = s.scalar(select(func.max(Event.id)))
        assert cursor is not None
        watchdog = Worker(kind=WorkerKind.WATCHDOG, project_id=project.id)
        s.add(watchdog)
        s.flush()
        s.add_all(
            [
                Comment(
                    author='watchdog#1', kind=CommentKind.ESCALATION, priority=1, body='SSO\nlog in', **on(project)
                ),
                Comment(author='orchestrator', kind=CommentKind.DECISION, priority=3, body='mine', **on(project)),
                Comment(author='research#2', kind=CommentKind.FINDING, body='the forum', **on(project)),
                Comment(
                    author='human', kind=CommentKind.DECISION, priority=2, body='seen', acked_at=utcnow(), **on(project)
                ),
                Comment(author='watchdog#5', kind=CommentKind.ESCALATION, priority=0, body='theirs', **on(other)),
            ]
        )
        seeds = [
            Idea(
                track='score',
                project_id=p.id,
                title=f'hunch {n}',
                hypothesis='h',
                status=status,
                created_by=by,
                provenance='run#1',
            )
            for n, (p, status, by) in enumerate(
                [
                    (project, IdeaStatus.SEED, 'worker#3'),
                    (project, IdeaStatus.SEED, 'orchestrator'),
                    (other, IdeaStatus.SEED, 'watchdog#1'),
                    (project, IdeaStatus.READY, 'human'),
                ]
            )
        ]
        s.add_all(seeds)
        s.flush()
        for idea in seeds:
            emit(s, 'idea.created', TargetKind.IDEA, idea.id, actor=idea.created_by)
        emit(s, ANOMALY_EVENT, TargetKind.WORKER, watchdog.id, payload={'key': 'k', 'anomaly': 'run#3 failed: boom'})
        judged: dict[str, object] = {'verdict': 'keep', 'score': 2.0, 'reason': 'score +1 over candidate#1\nmore'}
        emit(s, 'candidate.judged', TargetKind.CANDIDATE, 1, payload=judged)
        emit(s, 'candidate.judged', TargetKind.CANDIDATE, 1, payload={'verdict': 'unmeasured', 'reason': SKIPPED})
        s.get_one(Project, project.id).status = ProjectStatus.STOPPED
        s.get_one(Project, project.id).stopped_reason = 'max_tokens 100 reached'
        s.commit()
        last, lines = inbox.news(s, cursor, me='orchestrator')
        assert lines == snapshot(
            [
                'comment#6 escalation p0 on project#2 (other) by watchdog#5: theirs',
                'comment#2 escalation p1 on project#1 (demo) by watchdog#1: SSO',
                'seed idea#1 (demo) by worker#3: hunch 0 — run#1',
                'seed idea#3 (other) by watchdog#1: hunch 2 — run#1',
                'anomaly, watchdog worker#1 requested: run#3 failed: boom',
                'candidate#1 keep: score +1 over candidate#1',
                'project demo stopped: max_tokens 100 reached',
            ]
        )
        assert inbox.news(s, last, me='orchestrator') == (last, []), 'the cursor moved past all of it'
        assert inbox.news(s, cursor, project_id=other.id, me='orchestrator')[1] == snapshot(
            [
                'comment#6 escalation p0 on project#2 (other) by watchdog#5: theirs',
                'seed idea#3 (other) by watchdog#1: hunch 2 — run#1',
            ]
        )
        assert [i.id for i, _ in inbox.seeds(s)] == [1, 2, 3] and [
            i.id for i, _ in inbox.seeds(s, project_id=other.id)
        ] == [3]


def test_a_reply_answers_the_root_of_the_thread_and_wakes_the_orchestrator_but_not_its_author(
    db: Database, project: Project
) -> None:
    with db.session() as s:
        item = Comment(
            target_kind=TargetKind.CANDIDATE,
            target_id=1,
            author='orchestrator',
            kind=CommentKind.DECISION,
            priority=1,
            body='Keep #1.',
        )
        s.add(item)
        s.flush()
        follow_up = Comment(
            target_kind=TargetKind.COMMENT, target_id=item.id, author='orchestrator', priority=3, body='Details.'
        )
        s.add(follow_up)
        s.commit()
        cursor = s.scalar(select(func.max(Event.id))) or 0
        answer = inbox.reply(s, follow_up.id, '  Revert it: the floor moved.\n', author='human', priority=2)
        assert (answer.target_kind, answer.target_id, answer.kind, answer.priority, answer.body) == (
            TargetKind.COMMENT,
            item.id,
            CommentKind.DECISION,
            2,
            'Revert it: the floor moved.',
        ), 'a reply to a reply goes to the root: one level from a project'
        assert [c.id for c in inbox.thread(s, item)] == [follow_up.id, answer.id]
        assert [c.id for c, _ in inbox.items(s, project_id=project.id)] == [item.id, answer.id, follow_up.id]
        assert inbox.news(s, cursor, project_id=project.id, me='orchestrator')[1] == [
            'comment#3 decision p2 on comment#1 (demo) by human: Revert it: the floor moved.'
        ]
        assert inbox.news(s, cursor, me='human')[1] == [], 'not news to whoever wrote it'
