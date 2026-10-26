"""The inbox: comments with a priority, scoped to a campaign through every target, until someone acknowledges them."""

import re
from datetime import UTC, datetime

import pytest
from inline_snapshot import snapshot
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from piloop.actor import Actor, Agent, Orchestrating, Person, Service, System, party_of
from piloop.api.views import AckOutcome, Relation, thread_lines
from piloop.campaign import inbox
from piloop.campaign.anomaly import AnomalyReported
from piloop.db import feed as db_feed
from piloop.db.events import emit, in_order
from piloop.db.models import AgentSession, AgentSessionState, Campaign, Comment, Event, Idea, Measurement, Run, utcnow
from piloop.db.session import Database
from piloop.db.triggers.changelog import CommentAcked, Commented, IdeaCreated
from piloop.ids import CampaignId, CampaignStatus, CommentId, CommentKind, IdeaStatus, TargetKind, TargetRef
from piloop.plugins.verdict_rule.contract import Outcome
from piloop.scoring.ledger import CandidateJudged
from tests.conftest import WATCHDOG
from tests.reports.test_history import idea
from tests.reports.test_overview import render


def bodies(rows: list[tuple[Comment, Campaign | None]]) -> list[str]:
    return [c.body for c, _ in rows]


def test_items_scope_to_the_campaign_through_every_target(db: Database, campaign: Campaign) -> None:
    with db.session() as s:
        other = Campaign(name='other', path='/other', config=campaign.config)
        s.add(other)
        s.flush()
        mine = idea(s, campaign, 'mine', IdeaStatus.READY)
        theirs = idea(s, other, 'theirs', IdeaStatus.READY)
        run = Run(idea=mine, attempt=1, base_candidate_id=1, harness='h', model='m')
        s.add(run)
        s.add(AgentSession(kind=WATCHDOG, campaign_id=campaign.id, state=AgentSessionState.EXITED))
        s.flush()
        s.add(Measurement(candidate_id=1, evaluator_id=1, metrics={'score': 1}))
        s.flush()
        escalation = dict(author='watchdog', kind=CommentKind.ESCALATION, priority=1)
        s.add_all(
            [
                Comment(target=TargetRef(TargetKind.CAMPAIGN, campaign.id), body='on campaign', **escalation),
                Comment(target=TargetRef(TargetKind.IDEA, mine.id), body='on idea', **escalation),
                Comment(target=TargetRef(TargetKind.RUN, run.id), body='on run', **escalation),
                Comment(target=TargetRef(TargetKind.CANDIDATE, 1), body='on candidate', **escalation),
                Comment(target=TargetRef(TargetKind.MEASUREMENT, 1), body='on measurement', **escalation),
                Comment(target=TargetRef(TargetKind.AGENT_SESSION, 1), body='on implementer', **escalation),
                Comment(target=TargetRef(TargetKind.IDEA, theirs.id), body='other campaign', **escalation),
                Comment(target=TargetRef(TargetKind.CAMPAIGN, other.id), body='other campaign too', **escalation),
                Comment(target=TargetRef(TargetKind.RUN, run.id), author='w', kind=CommentKind.FINDING, body='no'),
                Comment(target=TargetRef(TargetKind.IDEA, mine.id), author='h', kind=CommentKind.DECISION, body='park'),
                Comment(target=TargetRef(TargetKind.COMMENT, 1), body='on a comment of the campaign', **escalation),
                Comment(target=TargetRef(TargetKind.COMMENT, 7), body='on a comment of the other', **escalation),
                Comment(target=TargetRef(TargetKind.COMMENT, 11), body='two levels deep', **escalation),
                Comment(target=TargetRef(TargetKind.COMMENT, 13), body="three levels deep: nobody's", **escalation),
            ]
        )
        s.commit()
        assert bodies(inbox.items(s, campaign_id=campaign.id)) == [
            'on campaign',
            'on idea',
            'on run',
            'on candidate',
            'on measurement',
            'on implementer',
            'on a comment of the campaign',
            'two levels deep',
        ], 'the forum (no priority) is not in it'
        assert bodies(inbox.items(s, campaign_id=other.id)) == [
            'other campaign',
            'other campaign too',
            'on a comment of the other',
        ]
        everything = inbox.items(s)
        assert len(everything) == 12, 'unfiltered, the comment three levels deep is in it too'
        assert [p.name if p else None for _, p in everything][-3:] == ['other', 'demo', None]
        lines = [render(db, p).split('## Recent events\n\n')[1].split('\n\n')[0] for p in (campaign, other)]
        recent = [re.sub(r' \d+[smhd] ago', '', line).splitlines() for line in lines]
        assert recent == snapshot(
            [
                [
                    '- comment.escalation comment#11',
                    '- comment.decision comment#10',
                    '- comment.finding comment#9',
                    '- comment.escalation comment#6',
                    '- comment.escalation comment#5',
                ],
                [
                    '- comment.escalation comment#12',
                    '- comment.escalation comment#8',
                    '- comment.escalation comment#7',
                    '- idea.created idea#2',
                ],
            ]
        ), "each campaign's escalation events and none of the other's (campaign_of serves events too)"


def test_most_urgent_first_narrowed_by_kind_until_acknowledged(db: Database, campaign: Campaign) -> None:
    on_campaign = dict(target=TargetRef(TargetKind.CAMPAIGN, campaign.id), author='pi')
    with db.session() as s:
        s.add_all(
            [
                Comment(kind=CommentKind.DECISION, priority=3, body='requeued idea#4', **on_campaign),
                Comment(kind=CommentKind.ESCALATION, priority=1, body='SSO expired', **on_campaign),
                Comment(kind=CommentKind.DECISION, priority=0, body='lowered the floor', **on_campaign),
                Comment(kind=CommentKind.FINDING, priority=3, body='half the sweeps add nothing', **on_campaign),
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
        assert [
            r.line() for r in inbox.ack(s, [CommentId(3), CommentId(1), CommentId(3), CommentId(99)], System.HUMAN)
        ] == [
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
        acked = in_order(s, CommentAcked)
        assert [(e.target_id, a.acked_by) for e, a in acked] == [(3, 'human'), (1, 'human')]


def test_an_author_never_has_its_own_comment_waiting_nor_acknowledges_it(db: Database, campaign: Campaign) -> None:
    ana = Person(name='ana')
    with db.session() as s:
        sent = inbox.message(s, campaign.id, 'what next?', author=ana, priority=1)
        assert [c.id for c, _ in inbox.waiting_for(s, ana.party)] == [], 'not for its author'
        assert [c.id for c, _ in inbox.waiting_for(s, System.ORCHESTRATOR.party)] == [sent.id]
        assert [r.line() for r in inbox.ack(s, [sent.id], ana)] == [
            'comment#1 is yours: only its recipients acknowledge it'
        ]
        assert s.get_one(Comment, sent.id).acked_at is None
        assert [r.outcome for r in inbox.ack(s, [sent.id], System.ORCHESTRATOR)] == [AckOutcome.ACKED]
        [own] = inbox.ack(s, [sent.id], ana)
        assert (own.outcome, own.acked_by) == (AckOutcome.OWN, 'orchestrator'), 'yours, whoever acknowledged it'


def test_every_agent_of_a_role_and_the_outside_orchestrator_are_one_party_for_what_waits_and_what_is_news(
    db: Database, campaign: Campaign
) -> None:
    earlier, later = Agent(role='orchestrator', session=41), Agent(role='orchestrator', session=42)
    with db.session() as s:
        cursor = db_feed.newest(s)
        decided = Comment(
            author=earlier.signature,
            kind=CommentKind.DECISION,
            priority=3,
            body='parked idea#1',
            target=TargetRef(TargetKind.CAMPAIGN, campaign.id),
        )
        s.add(decided)
        s.flush()
        emit(
            s,
            CandidateJudged(verdict=Outcome.INTERESTING, score=1.0, reason='mine'),
            TargetRef(TargetKind.CANDIDATE, 1),
            earlier,
        )
        emit(
            s,
            CandidateJudged(verdict=Outcome.KEEP, score=2.0, reason='the daemon'),
            TargetRef(TargetKind.CANDIDATE, 1),
            System.DAEMON,
        )
        s.commit()
        for me in (later, System.ORCHESTRATOR, Orchestrating(by=Person(name='ana'))):
            assert inbox.waiting_for(s, me.party) == [], f'{me} wrote it, as one party'
            assert news_lines(s, cursor, me=me)[1] == ['candidate#1 keep: the daemon'], 'its own verdict is no news'
            assert [r.outcome for r in inbox.ack(s, [decided.id], me)] == [AckOutcome.OWN]
        assert [c.id for c, _ in inbox.waiting_for(s, Agent(role='watchdog', session=41).party)] == [decided.id]
        assert party_of('not an actor') == 'not an actor'
        assert Service(name='ci').party == Service(name='ci').signature, 'a service writes for itself'
        assert party_of('orchestrator via human:ana') == party_of('orchestrator#7') == 'orchestrator'


def test_thread_follows_citations_both_ways_and_the_replies_not_the_rest_of_the_target(
    db: Database, campaign: Campaign
) -> None:
    with db.session() as s:
        on_campaign = dict(target=TargetRef(TargetKind.CAMPAIGN, campaign.id))
        s.add_all(
            [
                Comment(author='research#2', kind=CommentKind.FINDING, body='a sweep', **on_campaign),
                Comment(
                    author='research#2', kind=CommentKind.ESCALATION, priority=1, body='floor exact?', **on_campaign
                ),
                Comment(target=TargetRef(TargetKind.COMMENT, 2), author='pi', body='looking'),
                Comment(
                    author='pi', kind=CommentKind.DECISION, priority=0, body='floor 2687\nRe: comment#2', **on_campaign
                ),
                Comment(
                    author='pi', kind=CommentKind.DECISION, priority=3, body='see comment#20, not #2', **on_campaign
                ),
                Comment(target=TargetRef(TargetKind.IDEA, 4), author='pi', body='as comment#4 decided, comment#2.'),
            ]
        )
        s.commit()
        escalation, decision = s.get_one(Comment, 2), s.get_one(Comment, 4)
        assert [(c.body, r) for c, r in inbox.thread(s, escalation)] == [
            ('looking', Relation.REPLY),
            ('floor 2687\nRe: comment#2', Relation.CITES),
            ('as comment#4 decided, comment#2.', Relation.CITES),
        ]
        assert [(c.id, r) for c, r in inbox.thread(s, decision)] == [
            (2, Relation.CITED_BY),
            (6, Relation.CITES),
        ], 'what it answers, and who cites it'


def test_each_comment_of_a_thread_has_the_thread_of_its_root(db: Database, campaign: Campaign) -> None:
    on_campaign = TargetRef(TargetKind.CAMPAIGN, campaign.id)
    with db.session() as s:
        s.add(Comment(target=on_campaign, author='research#2', kind=CommentKind.FINDING, body='the login expires'))
        root = Comment(
            target=on_campaign, author='watchdog#1', kind=CommentKind.ESCALATION, priority=1, body='SSO, see comment#1'
        )
        s.add(root)
        s.commit()
        first = inbox.reply(s, root.id, 'logged in, Re: comment#2', author=System.ORCHESTRATOR, priority=3)
        second = inbox.reply(s, first.id, 'thanks', author=System.HUMAN, priority=3)
        threads = [inbox.thread_of(s, c) for c in (root, first, second)]
        assert threads[1:] == threads[:-1], 'every member answers the same thread'
        assert threads[0].comment.id == root.id
        assert [(c.id, c.relation, c.in_reply_to) for c in threads[0].linked] == [
            (first.id, Relation.REPLY, root.id),
            (second.id, Relation.REPLY, first.id),
            (1, Relation.CITED_BY, None),
        ], 'a reply that cites its root is a reply'
        s.add_all(
            [
                Comment(target=TargetRef(TargetKind.COMMENT, root.id), author='pi', body='an old reply\nsays nothing'),
                Comment(target=TargetRef(TargetKind.IDEA, 1), author='pi', body='as comment#2 said'),
            ]
        )
        s.commit()
        title, lines = thread_lines(inbox.thread_of(s, second), datetime.now(UTC))
        assert [title.split(', ')[0], *(re.sub(r'\d+s ago', '0s ago', line) for line in lines)] == snapshot(
            [
                'comment#2 escalation on campaign#1 by watchdog#1',
                'SSO, see comment#1',
                '',
                '- comment#3 decision by orchestrator, 0s ago, answers comment#2: logged in, Re: comment#2',
                '- comment#4 decision by human, 0s ago, answers comment#3: thanks',
                '- comment#5 note by pi, 0s ago, a reply: an old reply',
                '  says nothing',
                '- comment#1 finding by research#2, 0s ago, cited by it: the login expires',
                '- comment#6 note by pi, 0s ago, cites it: as comment#2 said',
            ]
        )


def test_news_is_what_happened_since_the_cursor_that_the_orchestrator_did_not_write_or_see(
    db: Database, campaign: Campaign
) -> None:
    def on(p: Campaign) -> dict[str, object]:
        return dict(target=TargetRef(TargetKind.CAMPAIGN, p.id))

    with db.session() as s:
        other = Campaign(name='other', path='/other', config=campaign.config)
        s.add(other)
        s.add(Comment(author='watchdog#1', kind=CommentKind.ESCALATION, priority=1, body='before', **on(campaign)))
        s.commit()
        cursor = s.scalar(select(func.max(Event.id)))
        assert cursor is not None
        watchdog = AgentSession(kind=WATCHDOG, campaign_id=campaign.id)
        s.add(watchdog)
        s.flush()
        s.add_all(
            [
                Comment(
                    author='watchdog#1', kind=CommentKind.ESCALATION, priority=1, body='SSO\nlog in', **on(campaign)
                ),
                Comment(author='orchestrator', kind=CommentKind.DECISION, priority=3, body='mine', **on(campaign)),
                Comment(author='research#2', kind=CommentKind.FINDING, body='the forum', **on(campaign)),
                Comment(
                    author='human',
                    kind=CommentKind.DECISION,
                    priority=2,
                    body='seen',
                    acked_at=utcnow(),
                    **on(campaign),
                ),
                Comment(author='watchdog#5', kind=CommentKind.ESCALATION, priority=0, body='theirs', **on(other)),
            ]
        )
        seeds = [
            Idea(
                track='score',
                campaign_id=p.id,
                title=f'hunch {n}',
                hypothesis='h',
                status=status,
                created_by=by,
                provenance='run#1',
            )
            for n, (p, status, by) in enumerate(
                [
                    (campaign, IdeaStatus.SEED, 'implementer#3'),
                    (campaign, IdeaStatus.SEED, 'orchestrator'),
                    (other, IdeaStatus.SEED, 'watchdog#1'),
                    (campaign, IdeaStatus.READY, 'human'),
                ]
            )
        ]
        s.add_all(seeds)  # each insert emits idea.created, signed by its creator
        s.flush()
        run3 = TargetRef(TargetKind.RUN, 3)
        reported = AnomalyReported(subject=run3, trigger='run_failed', line='run#3 failed: boom')
        emit(s, reported, TargetRef(TargetKind.AGENT_SESSION, watchdog.id), System.DAEMON)
        judged = CandidateJudged(verdict=Outcome.KEEP, score=2.0, reason='score +1 over candidate#1\nmore')
        candidate = TargetRef(TargetKind.CANDIDATE, 1)
        emit(s, judged, candidate, System.DAEMON)
        s.get_one(Campaign, campaign.id).status = CampaignStatus.STOPPED
        s.get_one(Campaign, campaign.id).stopped_reason = 'max_tokens 100 reached'
        s.commit()
        last, lines = news_lines(s, cursor, me=System.ORCHESTRATOR)
        assert lines == snapshot(
            [
                'comment#6 escalation p0 on campaign#2 (other) by watchdog#5: theirs',
                'comment#2 escalation p1 on campaign#1 (demo) by watchdog#1: SSO',
                'seed idea#1 (demo) by implementer#3: hunch 0 — run#1',
                'seed idea#3 (other) by watchdog#1: hunch 2 — run#1',
                'anomaly, watchdog#1 requested: run#3 failed: boom',
                'candidate#1 keep: score +1 over candidate#1',
                'campaign demo stopped: max_tokens 100 reached',
            ]
        )
        assert news_lines(s, last, me=System.ORCHESTRATOR) == (last, []), 'the cursor moved past all of it'
        assert news_lines(s, cursor, campaign_id=other.id, me=System.ORCHESTRATOR)[1] == snapshot(
            [
                'comment#6 escalation p0 on campaign#2 (other) by watchdog#5: theirs',
                'seed idea#3 (other) by watchdog#1: hunch 2 — run#1',
            ]
        )
        assert [i.id for i, _ in inbox.seeds(s)] == [1, 2, 3] and [
            i.id for i, _ in inbox.seeds(s, campaign_id=other.id)
        ] == [3]


def test_news_reads_only_what_wakes_a_page_at_a_time_until_a_page_has_a_line(
    db: Database, campaign: Campaign, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(inbox, 'PAGE', 1)
    with db.session() as s:
        cursor = s.scalar(select(func.max(Event.id))) or 0
        on_campaign = TargetRef(TargetKind.CAMPAIGN, campaign.id)
        for n in range(3):
            s.add(Comment(target=on_campaign, author='watchdog#1', priority=1, body=f'acked {n}', acked_at=utcnow()))
        emit(s, IdeaCreated(), TargetRef(TargetKind.IDEA, 99), System.HUMAN)  # a deleted idea: no line
        s.add(Comment(target=on_campaign, author='watchdog#1', priority=1, body='open'))
        s.add(Comment(target=on_campaign, author='watchdog#1', body='a note: an event, but not of the inbox'))
        s.commit()
        last, lines = news_lines(s, cursor, me=System.ORCHESTRATOR)
        assert lines == ['comment#4 note p1 on campaign#1 (demo) by watchdog#1: open']
        newest = db_feed.newest(s)
        assert last < newest and news_lines(s, last, me=System.ORCHESTRATOR) == (newest, []), 'it passes the note'


def test_a_reply_answers_the_root_of_the_thread_and_wakes_the_orchestrator_but_not_its_author(
    db: Database, campaign: Campaign
) -> None:
    with db.session() as s:
        item = Comment(
            target=TargetRef(TargetKind.CANDIDATE, 1),
            author='orchestrator',
            kind=CommentKind.DECISION,
            priority=1,
            body='Keep #1.',
        )
        s.add(item)
        s.flush()
        follow_up = Comment(
            target=TargetRef(TargetKind.COMMENT, item.id), author='orchestrator', priority=3, body='Details.'
        )
        s.add(follow_up)
        s.commit()
        cursor = s.scalar(select(func.max(Event.id))) or 0
        answer = inbox.reply(s, follow_up.id, '  Revert it: the floor moved.\n', author=System.HUMAN, priority=2)
        assert (
            answer.target_kind,
            answer.target_id,
            answer.in_reply_to,
            answer.kind,
            answer.priority,
            answer.body,
        ) == (
            TargetKind.COMMENT,
            item.id,
            follow_up.id,
            CommentKind.DECISION,
            2,
            'Revert it: the floor moved.',
        ), 'a reply to a reply goes to the root, one level from a campaign, and says what it answers'
        assert [p for e, p in in_order(s, Commented) if e.target_id == answer.id] == [
            Commented(
                kind='comment.decision', target_kind=TargetKind.COMMENT, target_id=item.id, in_reply_to=follow_up.id
            )
        ]
        assert [c.id for c, _ in inbox.thread(s, item)] == [follow_up.id, answer.id]
        assert [c.id for c, _ in inbox.items(s, campaign_id=campaign.id)] == [item.id, answer.id, follow_up.id]
        assert news_lines(s, cursor, campaign_id=campaign.id, me=System.ORCHESTRATOR)[1] == [
            'comment#3 decision p2 on comment#1 (demo) by human: Revert it: the floor moved.'
        ]
        assert news_lines(s, cursor, me=System.HUMAN)[1] == [], 'not news to whoever wrote it'


def news_lines(s: Session, after: int, *, campaign_id: CampaignId | None = None, me: Actor) -> tuple[int, list[str]]:
    """The news after ``after``, as ``piloop inbox wait`` prints it, and the next cursor."""
    found = inbox.news(s, after, campaign_id=campaign_id, party=me.party)
    return found.next, [item.text() for item in found.items]
