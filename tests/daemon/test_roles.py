"""Roles as data: a sweeping role and a role that does runs that piloop.toml defines, new triggers for a built-in role,
and the roles, triggers and runtimes that packages add through entry points."""

import os
import re
import time
from collections.abc import Iterator
from importlib.metadata import EntryPoint
from pathlib import Path

import pytest
from piloop_dir_store import Dir
from sqlalchemy import select

from piloop.campaign.stop import ROLE_REMOVED
from piloop.cli import common, main as cli
from piloop.cli.plugins import selections
from piloop.config import CampaignConfig, ConfigError, load_campaign_config
from piloop.daemon.main import Daemon
from piloop.daemon.run_controller import RunController
from piloop.daemon.scheduler import Scheduler
from piloop.db.models import (
    AgentSession,
    AgentSessionState,
    Campaign,
    Candidate,
    Comment,
    CommentKind,
    Idea,
    IdeaStatus,
    Measurement,
    RoleName,
    Run,
    RunStatus,
    TargetKind,
    Verdict,
    utcnow,
)
from piloop.db.session import Database
from piloop.otel import Tracer
from piloop.plugins import host as plugins_module
from piloop.plugins.host import PluginError, compose
from piloop.plugins.role.contract import ROLES
from piloop.plugins.runtime.contract import RUNTIMES, Runtime
from piloop.plugins.trigger.contract import TRIGGERS
from piloop.runtime.factory import RuntimeFactory
from piloop.settings import Settings
from piloop.store.config import open_store
from tests.conftest import (
    HARNESS_APPEND,
    ScriptRuntime,
    console_lines,
    example,
    package,
    piloop_entry_points,
    wait_until,
    write_toml,
)
from tests.daemon.test_scheduler import add_ideas
from tests.daemon.test_triggers import BLOCKING, agent_sessions, fail_a_run, reported_events, wait_for

REVIEW = "SELECT id FROM comments WHERE kind = 'decision' AND created_at > :since"
PLUGIN = 'tests.fixtures.plugin'
TRIGGER_GROUP, ROLE_GROUP, RUNTIME_GROUP = TRIGGERS.group, ROLES.group, RUNTIMES.group


def recording(runtime: ScriptRuntime, kinds: list[str]) -> RuntimeFactory:
    """A factory that runs every agent in ``runtime`` and records the kind of runtime that was asked for."""

    def make(kind: str, *, campaign: str, campaign_path: Path) -> Runtime:
        kinds.append(kind)
        return runtime

    return make


def scheduler(db: Database, make: RuntimeFactory, tmp_path: Path) -> Scheduler:
    tracer = Tracer(None)
    return Scheduler(db, RunController(db, tracer, tmp_path, make), tracer, tmp_path, make)


def configure(db: Database, campaign: Campaign, harness_body: str, **roles: dict[str, object]) -> None:
    write_toml(Path(campaign.path), harness_body)
    with db.session() as s:
        cfg = load_campaign_config(Path(campaign.path), roles=roles)
        s.get_one(Campaign, campaign.id).config = cfg.model_dump(mode='json')
        s.commit()


def decide(db: Database, campaign: Campaign, body: str) -> None:
    with db.session() as s:
        s.add(
            Comment(
                target_kind=TargetKind.CAMPAIGN,
                target_id=campaign.id,
                author='human',
                kind=CommentKind.DECISION,
                body=body,
            )
        )
        s.commit()
    time.sleep(0.01)


def test_a_role_that_piloop_toml_defines_runs_on_its_sql_trigger_with_its_prompt_workdir_and_runtime(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A ``[roles.reviewer]`` table with a ``sql`` trigger: each new decision requests one reviewer.

    The reviewer runs with its own prompt, in the campaign checkout, under its own runtime, one at a time.
    """
    release = tmp_path / 'release'
    (Path(campaign.path) / 'reviewer.md').write_text('# Review\nRead the latest decision and say if it holds.\n')
    harness = f'#!/bin/sh\npwd > {tmp_path}/cwd.txt\n' + BLOCKING.format(release=release).removeprefix('#!/bin/sh\n')
    reviewer: dict[str, object] = {
        'prompt': 'reviewer.md',
        'triggers': [{'kind': 'sql', 'when': REVIEW}],
        'workdir': 'repo',
        'runtime': 'herdr',
        'harnesses': ['script'],
        'max_concurrent': 1,
    }
    configure(db, campaign, harness, reviewer=reviewer)
    kinds: list[str] = []
    sched = scheduler(db, recording(runtime, kinds), tmp_path)
    assert sched.tick() == [], 'no decision yet: the query returns no row'

    decide(db, campaign, 'keep the cache')
    assert sched.tick() == [1]
    wait_for(db, 1, AgentSessionState.RUNNING)
    assert sched.tick() == [], 'the decision is older than the running reviewer'
    decide(db, campaign, 'drop the cache')
    assert sched.tick() == [], 'max_concurrent = 1: the second reviewer waits'
    assert sched.tick() == [], 'one pending request is enough'
    reason = f'sql: {REVIEW}'
    assert agent_sessions(db, 'reviewer') == [
        (1, AgentSessionState.RUNNING, reason),
        (2, AgentSessionState.REQUESTED, reason),
    ]
    release.touch()
    sched.wait(10)
    assert sched.tick() == [2], 'the slot is free'
    sched.wait(10)
    sched.stop(timeout=5)

    brief = (tmp_path / 'agent_sessions' / '1' / 'brief.md').read_text()
    assert brief.startswith('# Review\nRead the latest decision')
    assert '- You are reviewer#1, the reviewer for campaign#1 (demo).' in brief
    assert f'- Why this agent started (context, not a scope): {reason}' in brief
    assert '- Your cwd is the campaign checkout, shared with the human' in brief
    assert '- File a seed:' in brief and '- You may write:' not in brief, 'a new role files seeds and states no writes'
    assert Path((tmp_path / 'cwd.txt').read_text().strip()) == Path(campaign.path).resolve()
    assert not (tmp_path / 'agent_sessions' / '1' / 'worktree').exists()
    with db.session() as s:
        rows = s.scalars(select(AgentSession).where(AgentSession.kind == 'reviewer')).all()
        assert [(w.state, w.runtime, w.harness) for w in rows] == [(AgentSessionState.EXITED, 'herdr', 'script')] * 2
    assert 'herdr' in kinds


def test_a_role_adds_its_incident_prompt_to_an_incident_sweep_and_its_routine_prompt_to_any_other(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """The keys that give the watchdog its two kinds of sweep work for a role that ``piloop.toml`` defines."""
    repo = Path(campaign.path)
    for name, text in (('triage.md', '# Triage'), ('incident.md', '## An incident'), ('routine.md', '## A routine')):
        (repo / name).write_text(text + '\n')
    triage: dict[str, object] = {
        'prompt': 'triage.md',
        'triggers': [{'kind': 'anomaly'}],
        'incident_prompt': 'incident.md',
        'routine_prompt': 'routine.md',
    }
    configure(db, campaign, '#!/bin/sh\ntrue\n', triage=triage, watchdog={'triggers': []})
    sched = scheduler(db, recording(runtime, []), tmp_path)
    fail_a_run(db, campaign, 'broken')
    assert sched.tick() == [1]
    sched.wait(10)
    with db.session() as s:
        s.add(AgentSession(kind='triage', campaign_id=campaign.id, reason='by hand'))
        s.commit()
    assert sched.tick() == [2]
    sched.wait(10)
    sched.stop(timeout=5)
    incident, routine = ((tmp_path / 'agent_sessions' / str(w) / 'brief.md').read_text() for w in (1, 2))
    assert incident.startswith('# Triage\n\n## An incident\n\n# Demo') and '## Incidents to investigate' in incident
    assert routine.startswith('# Triage\n\n## A routine\n\n# Demo') and '## Research pack' in routine
    with pytest.raises(ConfigError, match=r"roles\.triage\.routine_prompt = 'gone\.md': no such file"):
        load_campaign_config(repo, roles={'triage': triage | {'routine_prompt': 'gone.md'}})


def test_a_role_that_defines_ideas_is_told_how_many_its_sweep_may_add(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """``low_water`` counts on ``max_ideas`` from each sweep of the role, so the brief of the sweep states it."""
    (Path(campaign.path) / 'ideator.md').write_text('# Ideate\n')
    ideator: dict[str, object] = {
        'prompt': 'ideator.md',
        'triggers': [{'kind': 'low_water', 'buffer': 1}],
        'files': 'ideas',
        'max_ideas': 2,
    }
    configure(db, campaign, '#!/bin/sh\ntrue\n', ideator=ideator, research={'triggers': []}, watchdog={'triggers': []})
    sched = scheduler(db, recording(runtime, []), tmp_path)
    assert sched.tick() == [1], 'no runnable idea: the backlog is short'
    sched.wait(10)
    sched.stop(timeout=5)
    brief = (tmp_path / 'agent_sessions' / '1' / 'brief.md').read_text()
    assert '- New ideas of your own in this sweep: at most 2 (`max_ideas`).' in brief
    [(_, _, reason)] = agent_sessions(db, 'ideator')
    assert reason is not None and reason.startswith('1 free implementer slots without a runnable idea')


HARNESS_SIGNS = '#!/bin/sh\necho "$PILOOP_ACTOR" >> solve.txt\ngit commit -qam "sign"\n'
"""Commit a tree of one's own: an agent of another role that runs at the same time makes another candidate."""
QUICK: dict[str, object] = {
    'runs': True,
    'prompt': 'quick.md',
    'takes': "priority >= 3 AND title NOT LIKE 'skip:%'",
    'timeout': 60,
    'max_concurrent': 1,
    'explore_every': 0,
}


def runs_by_role(db: Database) -> list[tuple[int, str, int, str]]:
    """Return each run as ``(id, role, idea, kind of its agent)``."""
    with db.session() as s:
        return [
            (
                run.id,
                run.role,
                run.idea_id,
                s.scalars(select(AgentSession.kind).where(AgentSession.run_id == run.id)).one(),
            )
            for run in s.scalars(select(Run).order_by(Run.id))
        ]


def queue(db: Database, sched: Scheduler, campaign: Campaign) -> None:
    """Queue runs for the free slots, as a tick does, without launching their agents."""
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        sched._queue(s, p, CampaignConfig.model_validate(p.config))
        s.commit()


def test_a_role_that_does_runs_takes_its_ideas_with_its_own_prompt_and_its_commits_are_scored(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """``[roles.quick]`` with ``runs = true`` takes the idea that ``takes`` selects, and the implementer takes the other one.

    The quick agent works with its own prompt in a new worktree at the run's base, and its commit is measured and
    judged.
    """
    (Path(campaign.path) / 'quick.md').write_text('# Quick\nTry the idea in a few minutes.\n')
    configure(db, campaign, HARNESS_SIGNS, quick=QUICK)
    (deep,) = add_ideas(db, campaign, 'deep', priority=1)
    (cheap,) = add_ideas(db, campaign, 'cheap', priority=3)
    sched = scheduler(db, recording(runtime, []), tmp_path)
    assert sorted(sched.tick()) == [1, 2]
    sched.wait(30)
    sched.stop(timeout=5)
    assert runs_by_role(db) == [(1, 'quick', cheap, 'quick'), (2, 'implementer', deep, 'implementer')]
    assert (tmp_path / 'runs' / '1' / 'brief.md').read_text().startswith('# Quick\nTry the idea')
    assert (tmp_path / 'runs' / '2' / 'brief.md').read_text().startswith('# Implementer\n')
    with db.session() as s:
        assert s.get_one(Run, 1).status is RunStatus.SUCCEEDED
        candidate = s.scalars(select(Candidate).where(Candidate.run_id == 1)).one()
        assert candidate.verdict is not Verdict.PENDING
        assert s.scalars(select(Measurement).where(Measurement.candidate_id == candidate.id)).all() != []


def test_each_role_that_does_runs_fills_its_own_slots_and_the_implementer_takes_what_waits(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """Quick has one slot, so its second idea goes to the implementer. The implementer takes what the roles before it leave."""
    configure(db, campaign, HARNESS_APPEND, quick={**QUICK, 'prompt': None})
    cheap, cheaper, later = (
        add_ideas(db, campaign, t, priority=p)[0] for t, p in (('cheap', 3), ('cheaper', 4), ('later', 4))
    )
    sched = scheduler(db, recording(runtime, []), tmp_path)
    queue(db, sched, campaign)
    assert runs_by_role(db) == [(1, 'quick', cheap, 'quick'), (2, 'implementer', cheaper, 'implementer')]
    with db.session() as s:
        assert s.get_one(Idea, later).status is IdeaStatus.READY, 'both slots are busy'
    sched.stop(timeout=5)


def test_a_queued_run_of_a_role_that_piloop_toml_no_longer_has_fails_and_its_idea_goes_back_to_the_queue(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    (Path(campaign.path) / 'quick.md').write_text('# Quick\n')
    configure(db, campaign, HARNESS_APPEND, quick=QUICK)
    (cheap,) = add_ideas(db, campaign, 'cheap', priority=3)
    sched = scheduler(db, recording(runtime, []), tmp_path)
    queue(db, sched, campaign)
    configure(db, campaign, HARNESS_APPEND)
    assert sched.tick() == []
    with db.session() as s:
        run, agent_session, idea = s.get_one(Run, 1), s.get_one(AgentSession, 1), s.get_one(Idea, cheap)
        assert (run.status, run.error) == (RunStatus.FAILED, ROLE_REMOVED)
        assert (agent_session.state, agent_session.error) == (
            AgentSessionState.KILLED,
            'no [roles.quick] in piloop.toml',
        )
        assert idea.status is IdeaStatus.READY, 'the removed role was not an attempt at the idea'
    assert sched.tick() == [2]
    sched.wait(30)
    sched.stop(timeout=5)
    assert runs_by_role(db)[1:] == [(2, 'implementer', cheap, 'implementer')]


def test_a_built_in_role_takes_other_triggers(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """The watchdog without its ``anomaly`` trigger ignores a failure; its timer still fires at its ``interval``."""
    configure(
        db,
        campaign,
        '#!/bin/sh\ntrue\n',
        watchdog={'triggers': [{'kind': 'cron'}], 'interval': 30},
        research={'triggers': [{'kind': 'sql', 'when': REVIEW}]},
    )
    sched = scheduler(db, recording(runtime, []), tmp_path)
    fail_a_run(db, campaign, 'broken')
    assert sched.tick() == [], 'no anomaly trigger, and the timer is not due'
    with db.session() as s:
        s.get_one(Campaign, campaign.id).created_at = utcnow().replace(year=2000)
        s.commit()
    decide(db, campaign, 'look again')
    assert sched.tick() == [1, 2]
    sched.wait(10)
    sched.stop(timeout=5)
    assert agent_sessions(db, RoleName.WATCHDOG) == [(1, AgentSessionState.EXITED, 'timer: 30s')]
    assert agent_sessions(db, RoleName.RESEARCH) == [(2, AgentSessionState.EXITED, f'sql: {REVIEW}')]
    assert reported_events(db) == [], 'the watchdog got no anomaly'


def test_a_request_for_a_role_that_piloop_toml_no_longer_has_is_refused(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    (Path(campaign.path) / 'reviewer.md').write_text('# Review\n')
    configure(db, campaign, '#!/bin/sh\ntrue\n', reviewer={'prompt': 'reviewer.md', 'triggers': []})
    with db.session() as s:
        s.add(AgentSession(kind='reviewer', campaign_id=campaign.id, reason='by hand'))
        s.commit()
    configure(db, campaign, '#!/bin/sh\ntrue\n')
    sched = scheduler(db, recording(runtime, []), tmp_path)
    assert sched.tick() == []
    sched.stop(timeout=5)
    with db.session() as s:
        row = s.get_one(AgentSession, 1)
    assert (row.state, row.error) == (AgentSessionState.KILLED, 'no [roles.reviewer] in piloop.toml')


def test_a_hand_over_leaves_a_sweep_in_the_campaign_checkout_running_and_the_next_scheduler_re_attaches_to_it(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    hold = tmp_path / 'hold'
    hold.touch()
    harness = f'#!/bin/sh\necho $$ > {tmp_path}/agent.pid\nwhile [ -f {hold} ]; do sleep 0.05; done\n'
    configure(db, campaign, harness, watchdog={'workdir': 'repo', 'triggers': []})
    with db.session() as s:
        s.add(AgentSession(kind=RoleName.WATCHDOG, campaign_id=campaign.id, reason='by hand'))
        s.commit()
    first = scheduler(db, recording(runtime, []), tmp_path)
    assert first.tick() == [1]
    wait_until((tmp_path / 'agent.pid').exists)
    first.hand_over(timeout=5)
    second = scheduler(db, recording(ScriptRuntime(), []), tmp_path)
    second.reap()
    hold.unlink()
    second.wait(30)
    first.wait(10)
    with db.session() as s:
        sweep = s.get_one(AgentSession, 1)
    assert (sweep.state, sweep.error) == (AgentSessionState.EXITED, None), 'the next daemon watched it to its end'
    assert Path(campaign.path).is_dir()
    second.stop(timeout=5)


ENTRY_POINTS = [
    EntryPoint('every_tick', f'{PLUGIN}:EveryTick', TRIGGER_GROUP),
    EntryPoint('broken', f'{PLUGIN}:Broken', TRIGGER_GROUP),
    EntryPoint('auditor', f'{PLUGIN}:AUDITOR', ROLE_GROUP),
    EntryPoint('quick', f'{PLUGIN}:QUICK', ROLE_GROUP),
    EntryPoint('typed', f'{PLUGIN}:Typed', RUNTIME_GROUP),
]


def forget_loaded() -> None:
    plugins_module.close()


def installed(monkeypatch: pytest.MonkeyPatch, entries: list[EntryPoint]) -> None:
    """Make piloop's own entry points and ``entries`` the installed ones, and forget the ones that were loaded."""

    def entry_points(*, group: str) -> list[EntryPoint]:
        return [*piloop_entry_points(group), *(ep for ep in entries if ep.group == group)]

    monkeypatch.setattr(plugins_module, 'entry_points', entry_points)
    forget_loaded()


@pytest.fixture
def plugins(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    installed(monkeypatch, ENTRY_POINTS)
    yield
    forget_loaded()


def test_a_package_adds_a_role_and_triggers_through_entry_points(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    plugins: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``piloop init`` stores the package's role with its defaults, under the table of ``piloop.toml``.

    The daemon runs it on the package's trigger. A trigger that raises is traced, and the role's other triggers fire.
    """
    cfg = load_campaign_config(Path(campaign.path))
    auditor = cfg.roles.sweeping()['auditor']
    assert (auditor.timeout, auditor.triggers[0].kind) == (5, 'every_tick')
    toml = Path(campaign.path) / 'piloop.toml'
    toml.write_text(toml.read_text() + '\n[roles.auditor]\ntimeout = 9\n')
    assert load_campaign_config(Path(campaign.path)).roles.sweeping()['auditor'].timeout == 9, 'the table goes over'
    configure(db, campaign, '#!/bin/sh\ntrue\n', auditor={'triggers': [{'kind': 'broken'}, {'kind': 'every_tick'}]})
    sched = scheduler(db, recording(runtime, []), tmp_path)
    assert sched.tick() == [1]
    sched.wait(10)
    sched.stop(timeout=5)
    assert agent_sessions(db, 'auditor') == [(1, AgentSessionState.EXITED, 'every tick for auditor')]
    brief = (tmp_path / 'agent_sessions' / '1' / 'brief.md').read_text()
    assert brief.startswith('# Audit\n')
    assert 'trigger.failed Ns campaign=demo role=auditor trigger=broken error=the probe is down' in console_lines(
        capsys.readouterr().err
    )


MINIMAL = """
name = "demo"
evaluate = ["true"]
model = {{ pi = "m" }}
{roles}

[[metrics]]
name = "score"
direction = "higher"
role = "primary"
"""


def test_a_package_role_takes_its_defaults_and_a_roles_value_that_is_not_a_table_fails(
    campaign_path: Path, plugins: None
) -> None:
    toml = campaign_path / 'piloop.toml'
    toml.write_text(MINIMAL.format(roles='[roles.auditor]'))
    assert load_campaign_config(campaign_path).roles.sweeping()['auditor'].timeout == 5, 'an empty table: the defaults'
    toml.write_text(MINIMAL.format(roles='roles = { auditor = 3 }'))
    with pytest.raises(ConfigError, match=r'roles\.auditor = 3: Input should be a valid dictionary'):
        load_campaign_config(campaign_path)
    toml.write_text(MINIMAL.format(roles='roles = 3'))
    with pytest.raises(ConfigError, match=r'\n  roles = 3: Input should be a valid dictionary'):
        load_campaign_config(campaign_path)


@pytest.mark.parametrize(
    ('entry', 'error'),
    [
        (EntryPoint('not_a_trigger', f'{PLUGIN}:NOT_A_TRIGGER', TRIGGER_GROUP), 'is not a piloop Trigger class'),
        (EntryPoint('cron', f'{PLUGIN}:EveryTick', TRIGGER_GROUP), "'cron' is a trigger already, from piloop "),
        (EntryPoint('auditor', f'{PLUGIN}:NOT_A_ROLE', ROLE_GROUP), 'is not a piloop SweepRole'),
        (EntryPoint('research', f'{PLUGIN}:AUDITOR', ROLE_GROUP), "'research' is a role already, from piloop "),
        (EntryPoint('typed', f'{PLUGIN}:NOT_A_RUNTIME', RUNTIME_GROUP), 'is not a piloop runtime class'),
        (EntryPoint('other', f'{PLUGIN}:Typed', RUNTIME_GROUP), "it needs kind = 'other'"),
        (EntryPoint('unsure', f'{PLUGIN}:Unsure', RUNTIME_GROUP), 'and an invocation'),
        (EntryPoint('herdr', f'{PLUGIN}:Typed', RUNTIME_GROUP), "'herdr' is a runtime already"),
    ],
)
def test_an_entry_point_of_the_wrong_kind_fails_with_its_name(
    monkeypatch: pytest.MonkeyPatch, entry: EntryPoint, error: str
) -> None:
    """The daemon's composition fails before it starts anything, and registers nothing."""
    installed(monkeypatch, [EntryPoint('every_tick', f'{PLUGIN}:EveryTick', TRIGGER_GROUP), entry])
    try:
        with pytest.raises(PluginError, match=f'entry point {entry.name} = {entry.value} in {entry.group}.*{error}'):
            compose(*plugins_module.declared())
        assert not any(c.active for c in plugins_module.declared())
    finally:
        forget_loaded()


def test_a_package_adds_a_role_that_does_runs(campaign_path: Path, plugins: None) -> None:
    cfg = load_campaign_config(campaign_path)
    assert list(cfg.roles.doing_runs()) == ['quick', 'implementer']
    quick = cfg.roles.doing_runs()['quick']
    assert (quick.takes, quick.timeout, quick.max_concurrent) == ('priority >= 3', 60, 1)


HARNESS_NICENESS = '#!/bin/sh\nps -o nice= -p $$ | tr -d " " > niceness.txt\n'
"""A harness that writes its own niceness, so a test sees which runtime started it."""


def test_example_plugins_replace_the_built_in_runtime_and_store_backend_through_the_configuration(
    db: Database, campaign: Campaign, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The example plugins ``piloop-nice`` and ``piloop-dir-store`` are installed, with a package role ``fast``.

    ``runtime = "nice"`` in piloop.toml and ``[storage.backend] kind = "dir"`` in config.toml bind them in place of
    ``subprocess`` and of no remote. The daemon composes them before it starts, its agents start through ``nice`` at
    the niceness of ``[runtimes.nice]``, and ``piloop plugins`` shows each binding with its package, its version and
    where the choice comes from.
    """
    example(tmp_path / 'site', 'nice-runtime')
    example(tmp_path / 'site', 'dir-store')
    package(tmp_path / 'site', 'piloop-fast-role', '1.0', f'[piloop.roles]\nfast = {PLUGIN}:FAST\n')
    monkeypatch.syspath_prepend(tmp_path / 'site')
    monkeypatch.setenv('PILOOP_HOME', str(tmp_path))  # where the runtime reads its [runtimes.nice] settings
    forget_loaded()
    write_toml(Path(campaign.path), HARNESS_NICENESS)
    with db.session() as s:
        cfg = load_campaign_config(
            Path(campaign.path), runtime='nice', roles={'watchdog': {'runtime': 'nice', 'workdir': 'repo'}}
        )
        s.get_one(Campaign, campaign.id).config = cfg.model_dump(mode='json')
        s.add(AgentSession(kind=RoleName.WATCHDOG, campaign_id=campaign.id, reason='by hand'))
        s.commit()
    remote = f'[storage.backend]\nkind = "dir"\nroot = "{tmp_path / "remote"}"\n'
    (tmp_path / 'config.toml').write_text(f'{remote}[runtimes.nice]\nniceness = 5\n')
    home = Settings(home=tmp_path, tick_seconds=0.05)
    assert isinstance(open_store(home).remote, Dir)
    daemon = Daemon(home)
    try:
        assert daemon.scheduler.tick() == [1]
        daemon.scheduler.wait(30)
    finally:
        daemon.shutdown(timeout=5)
    with db.session() as s:
        agent_session = s.get_one(AgentSession, 1)
        assert (agent_session.state, agent_session.runtime) == (AgentSessionState.EXITED, 'nice')
    assert (Path(campaign.path) / 'niceness.txt').read_text().strip() == str(min(os.nice(0) + 5, 19))
    monkeypatch.setattr(common, 'settings', home)
    printed: list[str] = []
    monkeypatch.setattr(common.console, 'print', lambda text, **_: printed.append(text))
    cli.app(['plugins'], result_action='return_value', exit_on_error=False)
    lines = printed[0].splitlines()
    assert [line for line in lines if not line.startswith(' ')] == [
        *(f'{c.noun}s ({c.tier} tier, {c.group}):' for c in plugins_module.declared()),
        'selected:',
    ]
    assert '  nice        piloop-nice 0.1.0, entry point nice = piloop_nice:NiceRuntime' in lines
    toml, config = Path(campaign.path) / 'piloop.toml', tmp_path / 'config.toml'
    store = f'storage backend dir, from piloop-dir-store 0.1.0 ([storage.backend] kind in {config})'
    stored = f'in the config of demo that the last `piloop init` stored from {toml}'
    settings = f'; its settings: [runtimes.nice] in {config}'
    keys = {
        'implementer': 'runtime',
        'watchdog': '[roles.watchdog] runtime',
        'research': 'runtime',
        'scout': 'runtime',
        'fast': 'the default of the fast role from piloop-fast-role 1.0',
    }
    assert lines[lines.index('selected:') + 1 :] == [
        *(f'  demo {k}: runtime nice, from piloop-nice 0.1.0 ({key}, {stored}{settings})' for k, key in keys.items()),
        f'  {store}',
    ]
    with db.session() as s:
        row = s.get_one(Campaign, campaign.id)
        row.config = {**row.config, 'runtime': 'subprocess'}
        s.commit()
    built_in = RUNTIMES.sources()['subprocess'].release
    assert (
        selections(home)[0] == f'demo implementer: runtime subprocess, from {built_in} (runtime, its default, {stored})'
    )
    with db.session() as s:
        s.get_one(Campaign, campaign.id).config = {}
        s.commit()
    assert selections(home) == [store], 'a stored config that does not load binds nothing; piloop doctor says why'
    forget_loaded()


def test_a_runtime_that_is_not_registered_fails_with_the_registered_ones(campaign_path: Path, plugins: None) -> None:
    with pytest.raises(
        ConfigError,
        match=re.escape(
            f"runtime = 'tmux': unknown runtime 'tmux'; registered runtimes: {', '.join(RUNTIMES.providers())}"
        ),
    ):
        load_campaign_config(campaign_path, runtime='tmux')
    with pytest.raises(ConfigError, match=r"roles.research.runtime = 'tmux': unknown runtime 'tmux'"):
        load_campaign_config(campaign_path, roles={'research': {'runtime': 'tmux'}})
