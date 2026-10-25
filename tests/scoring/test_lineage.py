"""The lineage follows the campaign's verdict rule: the bar of a run, the keeps that raise it, the head,
and what follows a keep that lacks the head.

The built-in rule's lineage is pinned by the run controller's and the scheduler's tests. These select a rule that
changes one method, and check that core asks it.
"""

from collections.abc import Iterator
from importlib.metadata import EntryPoint
from pathlib import Path

import pytest
from git import Repo
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.campaign import ops
from piloop.campaign.ops import missing_head
from piloop.config import load_campaign_config
from piloop.db.models import Campaign, Candidate, Cause, Idea, IdeaStatus, Origin, Run
from piloop.db.session import Database
from piloop.plugins.host import ContractBroken
from piloop.plugins.verdict_rule.contract import Outcome
from piloop.scoring.ledger import bar_of, baseline, head, trunk_keeps
from tests.campaign.test_ops import ACTOR, commit_on, run_candidate
from tests.conftest import ScriptRuntime
from tests.daemon.test_roles import PLUGIN, forget_loaded, installed
from tests.daemon.test_run_controller import controller, execute, keep_elsewhere, make_run, scored

RULES = {
    'base_bar': 'BaseBar',
    'every_keep': 'EveryKeep',
    'first_head': 'FirstHead',
    'leaves': 'Leaves',
    'no_compose': 'NoCompose',
    'astray': 'Astray',
}


@pytest.fixture
def rules(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Install a package that adds the verdict rules of ``RULES``."""
    installed(monkeypatch, [EntryPoint(n, f'{PLUGIN}:{c}', 'piloop.verdict_rules') for n, c in RULES.items()])
    yield
    forget_loaded()


def select_rule(db: Database, campaign: Campaign, name: str) -> None:
    with db.session() as s:
        cfg = load_campaign_config(Path(campaign.path), plugins={'verdict_rule': {'provider': name}})
        s.get_one(Campaign, campaign.id).config = cfg
        s.commit()


def child(s: Session, parent: Candidate, name: str, verdict: Outcome | None, run: Run | None = None) -> Candidate:
    """Add a candidate on top of ``parent``, made by ``run``."""
    found = Candidate(
        campaign_id=parent.campaign_id,
        parent_id=parent.id,
        run=run,
        tree_sha=name,
        revision=name,
        summary=name,
        verdict=verdict,
    )
    s.add(found)
    s.flush()
    return found


def test_the_bar_of_a_run_is_the_rules_choice_from_the_ancestry_of_its_base(
    db: Database, campaign: Campaign, rules: None
) -> None:
    with db.session() as s:
        root = baseline(s, s.get_one(Campaign, campaign.id))
        lead = child(s, child(s, root, 'lead', Outcome.INTERESTING), 'fix', Outcome.DISCARD)
        reference = Candidate(campaign_id=root.campaign_id, tree_sha='ref', revision='ref', origin=Origin.REFERENCE)
        s.add(reference)
        s.flush()
        on_reference = child(s, reference, 'on reference', None)
        assert (bar_of(s, lead), bar_of(s, on_reference), bar_of(s, root)) == (root, reference, root), (
            'bootstrap: the nearest keep or reference'
        )
        s.commit()
        lead_id = lead.id
    select_rule(db, campaign, 'base_bar')
    with db.session() as s:
        lead = s.get_one(Candidate, lead_id)
        assert bar_of(s, lead) is lead, 'base_bar: the base, a keep or not'
    select_rule(db, campaign, 'astray')
    with (
        db.session() as s,
        pytest.raises(
            ContractBroken,
            match=f'^the verdict rule chose candidate#1000000 as the bar of candidate#{lead_id}, which it was not given$',
        ),
    ):
        bar_of(s, s.get_one(Candidate, lead_id))


def test_the_trunk_and_its_head_are_the_keeps_that_raise_the_trunks_bar(
    db: Database, campaign: Campaign, rules: None
) -> None:
    """``every_keep`` lets a branch keep raise the trunk's bar: it is a trunk keep, and the head."""
    with db.session() as s:
        root = baseline(s, s.get_one(Campaign, campaign.id))
        idea = Idea(track='score', campaign_id=campaign.id, title='t', hypothesis='h', created_by='t')
        s.add(idea)
        s.flush()
        run = Run(idea_id=idea.id, attempt=1, base_candidate_id=root.id, branch=True, harness='h', model='m')
        kept = child(s, root, 'branch keep', Outcome.KEEP, run)
        lead = child(s, root, 'branch lead', Outcome.INTERESTING, run)
        s.commit()
        kept_id, lead_id = kept.id, lead.id
    with db.session() as s:
        proj = s.get_one(Campaign, campaign.id)
        root = baseline(s, proj)
        assert (trunk_keeps(s, proj), head(s, proj)) == ([root], root)
        assert missing_head(s, s.get_one(Candidate, lead_id)) is None, (
            'bootstrap: a branch keep never moves the head, nor would a keep of a branch lead'
        )
    select_rule(db, campaign, 'every_keep')
    with db.session() as s:
        proj = s.get_one(Campaign, campaign.id)
        root, kept = baseline(s, proj), s.get_one(Candidate, kept_id)
        assert (trunk_keeps(s, proj), head(s, proj)) == ([root, kept], kept)
        assert head(s, proj, excluding=kept) == root, 'the candidate judged is never its own bar'


def test_the_head_is_the_rules_choice_among_the_trunk_keeps(db: Database, campaign: Campaign, rules: None) -> None:
    with db.session() as s:
        root = baseline(s, s.get_one(Campaign, campaign.id))
        kept_id = child(s, root, 'trunk keep', Outcome.KEEP).id
        s.commit()
    with db.session() as s:
        proj = s.get_one(Campaign, campaign.id)
        assert head(s, proj) == s.get_one(Candidate, kept_id), 'bootstrap: the latest trunk keep'
    select_rule(db, campaign, 'first_head')
    with db.session() as s:
        proj = s.get_one(Campaign, campaign.id)
        root = baseline(s, proj)
        assert (head(s, proj), head(s, proj, excluding=root)) == (root, s.get_one(Candidate, kept_id))
    select_rule(db, campaign, 'astray')
    with (
        db.session() as s,
        pytest.raises(ContractBroken, match='^the verdict rule chose candidate#1000000 as the head'),
    ):
        head(s, s.get_one(Campaign, campaign.id))


def test_a_keep_that_lacks_the_head_is_left_or_merged_without_a_seed_as_the_rule_says(
    db: Database, campaign: Campaign, rules: None
) -> None:
    """The keeps by hand of ``ops.set_verdict``: ``leaves`` merges none, ``no_compose`` files no seed on a conflict."""
    base = Repo(campaign.path).head.commit.hexsha
    old_sha, trunk_sha = commit_on(campaign, base, {'x.txt': 'o\n'}), commit_on(campaign, base, {'x.txt': 't\n'})
    old = run_candidate(db, campaign, Outcome.DISCARD, commit=old_sha)
    trunk = run_candidate(db, campaign, Outcome.KEEP, commit=trunk_sha)
    left_sha = commit_on(campaign, base, {'y.txt': 'y\n'})
    left = run_candidate(db, campaign, Outcome.DISCARD, by=None, commit=left_sha)
    select_rule(db, campaign, 'leaves')
    with db.session() as s:
        kept = ops.set_verdict(s, left, Outcome.KEEP, reason='measured by hand', actor=ACTOR)
        c = s.get_one(Candidate, left)
        assert (kept.rebased, c.verdict, c.cause, c.verdict_reason) == (
            None,
            Outcome.INTERESTING,
            Cause.MOVED,
            f'keeps against its own bar, but the head moved on to candidate#{trunk}: kept by hand: measured by hand',
        ), 'no merge, nothing appended'
        s.commit()
    select_rule(db, campaign, 'no_compose')
    with db.session() as s:
        kept = ops.set_verdict(s, old, Outcome.KEEP, reason='measured by hand', actor=ACTOR)
        assert kept.rebased is not None and kept.rebased.conflicts == ['x.txt']
        assert s.scalars(select(Idea).where(Idea.status == IdeaStatus.SEED)).all() == []
        assert s.get_one(Candidate, old).verdict_reason == (
            f'keeps against its own bar, but the head moved on to candidate#{trunk}: kept by hand: measured by hand;'
            ' its merge onto the head conflicts in x.txt'
        )


def test_a_runs_keep_that_lost_the_race_to_the_head_is_left_off_the_trunk_as_the_rule_says(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, rules: None
) -> None:
    select_rule(db, campaign, 'leaves')
    run_id = make_run(db, campaign)
    h2 = keep_elsewhere(db, campaign, 'other line', {'other.txt': 'other\n'}, {'score': 2.0, 'size': 11.0})
    execute(controller(db, runtime, tmp_path), db, run_id)
    [(_, verdict, reason, _, _)] = scored(db, run_id)
    assert (verdict, reason) == (
        Outcome.INTERESTING,
        f'keeps against its own bar, but the head moved on to candidate#{h2}: against candidate#1: score 2 vs 1 (+1;'
        ' aggregate: no per-unit results)',
    ), 'one candidate: no merge onto the head'
    with db.session() as s:
        assert head(s, s.get_one(Campaign, campaign.id)) == s.get_one(Candidate, h2)
