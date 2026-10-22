"""A campaign with two tracks, run through the real daemon: share picks, rebase on keep, and a floor on another track.

The harness is a script that edits files by idea title, so the whole loop runs in CI without a real agent.
"""

import time
from pathlib import Path

from git import Repo
from sqlalchemy import select

from piloop.daemon.main import Daemon
from piloop.db.models import Campaign, Candidate, Event, Idea, Verdict
from piloop.scoring.ledger import REBASED, head
from piloop.settings import Settings
from tests.conftest import ScriptRuntime, factory, register, workers_of, write_script

TOML = """
name = "tracks"
evaluate = ["sh", "-c", "echo METRIC accuracy=$(wc -l < acc.txt); echo METRIC cost=$(wc -l < cost.txt)"]
runtime = "subprocess"
harnesses = ["script"]
evaluate_timeout = 30

[model]
script = "test-model"

[roles.implementer]
timeout = 30
max_concurrent = 2

[roles.research]
max_ideas = 0

[[metrics]]
name = "accuracy"
direction = "higher"
role = "primary"
track = "accuracy"
margin = 1
floor = 1

[[metrics]]
name = "cost"
direction = "lower"
role = "primary"
track = "cost"
margin = 1
floor = 5

[tracks.accuracy]
share = 0.5

[tracks.cost]
share = 0.5

[harness.script]
executable = "{script}"
headless = ["{{prompt}}"]
"""

HARNESS = """#!/bin/sh
case "$(cat ../brief.md)" in
  *"): trim cost"*) tail -n +2 cost.txt > cost.tmp && mv cost.tmp cost.txt; git commit -qam "trim cost" ;;
  *"): slow accuracy"*)
    while [ ! -e {go} ]; do sleep 0.05; done
    echo a >> acc.txt; git commit -qam "slow accuracy" ;;
  *"): heavy accuracy"*) echo a >> acc.txt; printf 'c\\nc\\nc\\nc\\n' >> cost.txt; git commit -qam "heavy accuracy" ;;
esac
"""
"""Edits by the title in the brief's ``## Your idea (idea#<id>): <title>`` line: ``slow accuracy`` waits for the test's go file, so the cost keep lands first."""


def test_two_tracks_rebase_a_keep_that_lost_the_race_and_refuse_one_past_the_other_tracks_floor(
    tmp_path: Path,
) -> None:
    path, go = tmp_path / 'tracks', tmp_path / 'go'
    path.mkdir()
    (path / 'acc.txt').write_text('a\n')
    (path / 'cost.txt').write_text('c\nc\nc\n')
    (path / 'brief.md').write_text('# Tracks\n')
    (path / 'piloop.toml').write_text(TOML.format(script=write_script(tmp_path / 'harness.sh', HARNESS.format(go=go))))
    repo = Repo.init(path, initial_branch='main')
    repo.git.add('-A')
    repo.git.commit('-qm', 'baseline')
    runtime = ScriptRuntime()
    daemon = Daemon(Settings(home=tmp_path / 'home', tick_seconds=0.05))
    workers_of(daemon.controller).worker.runtime_factory = factory(runtime)
    campaign = register(daemon.db, path)
    with daemon.db.session() as s:
        for title, track in (('slow accuracy', 'accuracy'), ('trim cost', 'cost')):
            s.add(Idea(campaign_id=campaign.id, track=track, title=title, hypothesis=title, created_by='t'))
        s.commit()

    daemon.scheduler.reap()
    assert len(daemon.scheduler.tick()) == 2, 'one slot per track'
    deadline = time.monotonic() + 120  # four evaluations and a rebase; a macOS runner is slow
    while _verdicts(daemon) != [Verdict.KEEP, Verdict.KEEP]:
        assert time.monotonic() < deadline, 'the cost keep never landed'
        time.sleep(0.05)
    go.touch()
    daemon.scheduler.wait(30)

    with daemon.db.session() as s:
        rows = s.scalars(select(Candidate).order_by(Candidate.id)).all()
        assert [(c.summary, c.verdict, c.parent_id) for c in rows] == [
            ('baseline', Verdict.KEEP, None),
            ('trim cost', Verdict.KEEP, 1),
            ('slow accuracy', Verdict.INTERESTING, 1),
            ('slow accuracy (rebased onto candidate#2)', Verdict.KEEP, 2),
        ]
        _, trimmed, raced, merged = rows
        assert raced.verdict_reason == (
            'keeps against its own bar, but the head moved on to candidate#2: against candidate#1: accuracy 2 vs 1'
            ' (+1; aggregate: no per-unit results); guardrails: cost 3 vs 3 (+0; aggregate: no per-unit results)'
            ' within its margin 1; rebased onto the head as candidate#4'
        )
        assert merged.verdict_reason == (
            'against candidate#2: accuracy 2 vs 1 (+1; aggregate: no per-unit results); guardrails: cost 2 vs 2'
            ' (+0; aggregate: no per-unit results) within its margin 1'
        ), 'judged on the head, where both tracks gained'
        assert [p.hexsha for p in repo.commit(merged.commit_sha).parents] == [trimmed.commit_sha, raced.commit_sha]
        [rebased] = s.scalars(select(Event).where(Event.kind == REBASED)).all()
        assert (rebased.target_id, rebased.payload) == (raced.id, {'head': trimmed.id, 'rebased': merged.id})
        queued = s.scalars(select(Event).where(Event.kind == 'run.queued').order_by(Event.id)).all()
        assert sorted(str(e.payload['track']) for e in queued) == ['accuracy', 'cost']
        trunk = head(s, s.get_one(Campaign, campaign.id))
        assert trunk is not None and trunk.id == merged.id
        s.add(Idea(campaign_id=campaign.id, track='accuracy', title='heavy accuracy', hypothesis='h', created_by='t'))
        s.commit()

    assert len(daemon.scheduler.tick()) == 1
    daemon.scheduler.wait(30)
    with daemon.db.session() as s:
        heavy = s.scalars(select(Candidate).order_by(Candidate.id.desc()).limit(1)).one()
        assert (heavy.summary, heavy.verdict, heavy.verdict_reason, heavy.parent_id) == (
            'heavy accuracy',
            Verdict.DISCARD,
            'floor: cost 6 past its floor 5',
            merged.id,
        ), 'accuracy 3 beats the head, but the cost track holds its floor'
        trunk = head(s, s.get_one(Campaign, campaign.id))
        assert trunk is not None and trunk.id == merged.id, 'the head stays'
    daemon.shutdown(timeout=5)


def _verdicts(daemon: Daemon) -> list[Verdict]:
    with daemon.db.session() as s:
        return list(s.scalars(select(Candidate.verdict).where(Candidate.verdict == Verdict.KEEP)))
