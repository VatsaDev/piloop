"""Roles as data: a sweeping role and a role that does runs that piloop.toml defines, new triggers for a built-in role,
and the roles, triggers and runtimes that packages add through entry points."""

import re
import time
from collections.abc import Iterator
from importlib.metadata import EntryPoint
from pathlib import Path

import pytest
from sqlalchemy import select

from piloop import config as config_module, triggers as triggers_module
from piloop.campaign.stop import ROLE_REMOVED
from piloop.config import ROLE_GROUP, ConfigError, ProjectConfig, load_project_config
from piloop.daemon.runner import RunExecutor
from piloop.daemon.scheduler import Scheduler
from piloop.db.models import (
    Candidate,
    Comment,
    CommentKind,
    Idea,
    IdeaStatus,
    Measurement,
    Project,
    Run,
    RunStatus,
    TargetKind,
    Verdict,
    Worker,
    WorkerKind,
    WorkerState,
    utcnow,
)
from piloop.db.session import Database
from piloop.otel import Tracer
from piloop.runtime import Runtime, factory as factory_module
from piloop.runtime.factory import RUNTIME_GROUP, RuntimeFactory, make_runtime, runtimes
from piloop.triggers import TRIGGER_GROUP
from tests.conftest import (
    HARNESS_APPEND,
    HARNESS_PROMPT_TO_FILE,
    ScriptRuntime,
    console_lines,
    wait_until,
    write_toml,
)
from tests.daemon.test_scheduler import add_ideas
from tests.daemon.test_triggers import BLOCKING, fail_a_run, reported_events, wait_for, workers

REVIEW = "SELECT id FROM comments WHERE kind = 'decision' AND created_at > :since"
PLUGIN = 'tests.fixtures.plugin'


def recording(runtime: ScriptRuntime, kinds: list[str]) -> RuntimeFactory:
    """A factory that runs every agent in ``runtime`` and records the kind of runtime that was asked for."""

    def make(kind: str, *, project: str, project_path: Path) -> Runtime:
        kinds.append(kind)
        return runtime

    return make


def scheduler(db: Database, make: RuntimeFactory, tmp_path: Path) -> Scheduler:
    tracer = Tracer(None)
    return Scheduler(db, RunExecutor(db, tracer, tmp_path, make), tracer, tmp_path, make)


def configure(db: Database, project: Project, harness_body: str, **roles: dict[str, object]) -> None:
    write_toml(Path(project.path), harness_body)
    with db.session() as s:
        cfg = load_project_config(Path(project.path), roles=roles)
        s.get_one(Project, project.id).config = cfg.model_dump(mode='json')
        s.commit()


def decide(db: Database, project: Project, body: str) -> None:
    with db.session() as s:
        s.add(
            Comment(
                target_kind=TargetKind.PROJECT,
                target_id=project.id,
                author='human',
                kind=CommentKind.DECISION,
                body=body,
            )
        )
        s.commit()
    time.sleep(0.01)


def test_a_role_that_piloop_toml_defines_runs_on_its_sql_trigger_with_its_prompt_workdir_and_runtime(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A ``[roles.reviewer]`` table with a ``sql`` trigger: each new decision requests one reviewer.

    The reviewer runs with its own prompt, in the project checkout, under its own runtime, one at a time.
    """
    release = tmp_path / 'release'
    (Path(project.path) / 'reviewer.md').write_text('# Review\nRead the latest decision and say if it holds.\n')
    harness = f'#!/bin/sh\npwd > {tmp_path}/cwd.txt\n' + BLOCKING.format(release=release).removeprefix('#!/bin/sh\n')
    reviewer: dict[str, object] = {
        'prompt': 'reviewer.md',
        'triggers': [{'kind': 'sql', 'when': REVIEW}],
        'workdir': 'project',
        'runtime': 'herdr',
        'harnesses': ['script'],
        'max_concurrent': 1,
    }
    configure(db, project, harness, reviewer=reviewer)
    kinds: list[str] = []
    sched = scheduler(db, recording(runtime, kinds), tmp_path)
    assert sched.tick() == [], 'no decision yet: the query returns no row'

    decide(db, project, 'keep the cache')
    assert sched.tick() == [1]
    wait_for(db, 1, WorkerState.RUNNING)
    assert sched.tick() == [], 'the decision is older than the running reviewer'
    decide(db, project, 'drop the cache')
    assert sched.tick() == [], 'max_concurrent = 1: the second reviewer waits'
    assert sched.tick() == [], 'one pending request is enough'
    reason = f'sql: {REVIEW}'
    assert workers(db, 'reviewer') == [(1, WorkerState.RUNNING, reason), (2, WorkerState.REQUESTED, reason)]
    release.touch()
    sched.wait(10)
    assert sched.tick() == [2], 'the slot is free'
    sched.wait(10)
    sched.stop(timeout=5)

    brief = (tmp_path / 'workers' / '1' / 'brief.md').read_text()
    assert brief.startswith('# Review\nRead the latest decision')
    assert '- You are worker#1, the reviewer for project#1 (demo).' in brief
    assert f'- Trigger: {reason}' in brief
    assert '- Your cwd is the project checkout, shared with the human' in brief
    assert '- File a seed:' in brief and '- You may write:' not in brief, 'a new role files seeds and states no writes'
    assert Path((tmp_path / 'cwd.txt').read_text().strip()) == Path(project.path).resolve()
    assert not (tmp_path / 'workers' / '1' / 'worktree').exists()
    with db.session() as s:
        rows = s.scalars(select(Worker).where(Worker.kind == 'reviewer')).all()
        assert [(w.state, w.runtime, w.harness) for w in rows] == [(WorkerState.EXITED, 'herdr', 'script')] * 2
    assert 'herdr' in kinds


def test_a_role_adds_its_incident_prompt_to_an_incident_sweep_and_its_routine_prompt_to_any_other(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """The keys that give the watchdog its two kinds of sweep work for a role that ``piloop.toml`` defines."""
    repo = Path(project.path)
    for name, text in (('triage.md', '# Triage'), ('incident.md', '## An incident'), ('routine.md', '## A routine')):
        (repo / name).write_text(text + '\n')
    triage: dict[str, object] = {
        'prompt': 'triage.md',
        'triggers': [{'kind': 'anomaly'}],
        'incident_prompt': 'incident.md',
        'routine_prompt': 'routine.md',
    }
    configure(db, project, '#!/bin/sh\ntrue\n', triage=triage, watchdog={'triggers': []})
    sched = scheduler(db, recording(runtime, []), tmp_path)
    fail_a_run(db, project, 'broken')
    assert sched.tick() == [1]
    sched.wait(10)
    with db.session() as s:
        s.add(Worker(kind='triage', project_id=project.id, reason='by hand'))
        s.commit()
    assert sched.tick() == [2]
    sched.wait(10)
    sched.stop(timeout=5)
    incident, routine = ((tmp_path / 'workers' / str(w) / 'brief.md').read_text() for w in (1, 2))
    assert incident.startswith('# Triage\n\n## An incident\n\n# Demo') and '## Incidents to investigate' in incident
    assert routine.startswith('# Triage\n\n## A routine\n\n# Demo') and '## Research pack' in routine
    with pytest.raises(ConfigError, match=r"roles\.triage\.routine_prompt = 'gone\.md': no such file"):
        load_project_config(repo, roles={'triage': triage | {'routine_prompt': 'gone.md'}})


def test_a_role_that_defines_ideas_is_told_how_many_its_sweep_may_add(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """``low_water`` counts on ``max_ideas`` from each sweep of the role, so the brief of the sweep states it."""
    (Path(project.path) / 'ideator.md').write_text('# Ideate\n')
    ideator: dict[str, object] = {
        'prompt': 'ideator.md',
        'triggers': [{'kind': 'low_water', 'buffer': 1}],
        'files': 'ideas',
        'max_ideas': 2,
    }
    configure(db, project, '#!/bin/sh\ntrue\n', ideator=ideator, research={'triggers': []}, watchdog={'triggers': []})
    sched = scheduler(db, recording(runtime, []), tmp_path)
    assert sched.tick() == [1], 'no runnable idea: the backlog is short'
    sched.wait(10)
    sched.stop(timeout=5)
    brief = (tmp_path / 'workers' / '1' / 'brief.md').read_text()
    assert '- New ideas of your own in this sweep: at most 2 (`max_ideas`).' in brief
    [(_, _, reason)] = workers(db, 'ideator')
    assert reason is not None and reason.startswith('1 free worker slots without a runnable idea')


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
            (run.id, run.role, run.idea_id, s.scalars(select(Worker.kind).where(Worker.run_id == run.id)).one())
            for run in s.scalars(select(Run).order_by(Run.id))
        ]


def queue(db: Database, sched: Scheduler, project: Project) -> None:
    """Queue runs for the free slots, as a tick does, without launching their agents."""
    with db.session() as s:
        p = s.get_one(Project, project.id)
        sched._queue(s, p, ProjectConfig.model_validate(p.config))
        s.commit()


def test_a_role_that_does_runs_takes_its_ideas_with_its_own_prompt_and_its_commits_are_scored(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """``[roles.quick]`` with ``runs = true`` takes the idea that ``takes`` selects, and the worker takes the other one.

    The quick agent works with its own prompt in a new worktree at the run's base, and its commit is measured and
    judged.
    """
    (Path(project.path) / 'quick.md').write_text('# Quick\nTry the idea in a few minutes.\n')
    configure(db, project, HARNESS_SIGNS, quick=QUICK)
    (deep,) = add_ideas(db, project, 'deep', priority=1)
    (cheap,) = add_ideas(db, project, 'cheap', priority=3)
    sched = scheduler(db, recording(runtime, []), tmp_path)
    assert sorted(sched.tick()) == [1, 2]
    sched.wait(30)
    sched.stop(timeout=5)
    assert runs_by_role(db) == [(1, 'quick', cheap, 'quick'), (2, 'worker', deep, 'worker')]
    assert (tmp_path / 'runs' / '1' / 'brief.md').read_text().startswith('# Quick\nTry the idea')
    assert (tmp_path / 'runs' / '2' / 'brief.md').read_text().startswith('# Worker\n')
    with db.session() as s:
        assert s.get_one(Run, 1).status is RunStatus.SUCCEEDED
        candidate = s.scalars(select(Candidate).where(Candidate.run_id == 1)).one()
        assert candidate.verdict is not Verdict.PENDING
        assert s.scalars(select(Measurement).where(Measurement.candidate_id == candidate.id)).all() != []


def test_each_role_that_does_runs_fills_its_own_slots_and_the_worker_takes_what_waits(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """Quick has one slot, so its second idea goes to the worker. The worker takes what the roles before it leave."""
    configure(db, project, HARNESS_APPEND, quick={**QUICK, 'prompt': None})
    cheap, cheaper, later = (
        add_ideas(db, project, t, priority=p)[0] for t, p in (('cheap', 3), ('cheaper', 4), ('later', 4))
    )
    sched = scheduler(db, recording(runtime, []), tmp_path)
    queue(db, sched, project)
    assert runs_by_role(db) == [(1, 'quick', cheap, 'quick'), (2, 'worker', cheaper, 'worker')]
    with db.session() as s:
        assert s.get_one(Idea, later).status is IdeaStatus.READY, 'both slots are busy'
    sched.stop(timeout=5)


def test_a_queued_run_of_a_role_that_piloop_toml_no_longer_has_fails_and_its_idea_goes_back_to_the_queue(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    (Path(project.path) / 'quick.md').write_text('# Quick\n')
    configure(db, project, HARNESS_APPEND, quick=QUICK)
    (cheap,) = add_ideas(db, project, 'cheap', priority=3)
    sched = scheduler(db, recording(runtime, []), tmp_path)
    queue(db, sched, project)
    configure(db, project, HARNESS_APPEND)
    assert sched.tick() == []
    with db.session() as s:
        run, worker, idea = s.get_one(Run, 1), s.get_one(Worker, 1), s.get_one(Idea, cheap)
        assert (run.status, run.error) == (RunStatus.FAILED, ROLE_REMOVED)
        assert (worker.state, worker.error) == (WorkerState.KILLED, 'no [roles.quick] in piloop.toml')
        assert idea.status is IdeaStatus.READY, 'the removed role was not an attempt at the idea'
    assert sched.tick() == [2]
    sched.wait(30)
    sched.stop(timeout=5)
    assert runs_by_role(db)[1:] == [(2, 'worker', cheap, 'worker')]


def test_a_built_in_role_takes_other_triggers(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """The watchdog without its ``anomaly`` trigger ignores a failure; its timer still fires at its ``interval``."""
    configure(
        db,
        project,
        '#!/bin/sh\ntrue\n',
        watchdog={'triggers': [{'kind': 'cron'}], 'interval': 30},
        research={'triggers': [{'kind': 'sql', 'when': REVIEW}]},
    )
    sched = scheduler(db, recording(runtime, []), tmp_path)
    fail_a_run(db, project, 'broken')
    assert sched.tick() == [], 'no anomaly trigger, and the timer is not due'
    with db.session() as s:
        s.get_one(Project, project.id).created_at = utcnow().replace(year=2000)
        s.commit()
    decide(db, project, 'look again')
    assert sched.tick() == [1, 2]
    sched.wait(10)
    sched.stop(timeout=5)
    assert workers(db, WorkerKind.WATCHDOG) == [(1, WorkerState.EXITED, 'timer: 30s')]
    assert workers(db, WorkerKind.RESEARCH) == [(2, WorkerState.EXITED, f'sql: {REVIEW}')]
    assert reported_events(db) == [], 'the watchdog got no anomaly'


def test_a_request_for_a_role_that_piloop_toml_no_longer_has_is_refused(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    (Path(project.path) / 'reviewer.md').write_text('# Review\n')
    configure(db, project, '#!/bin/sh\ntrue\n', reviewer={'prompt': 'reviewer.md', 'triggers': []})
    with db.session() as s:
        s.add(Worker(kind='reviewer', project_id=project.id, reason='by hand'))
        s.commit()
    configure(db, project, '#!/bin/sh\ntrue\n')
    sched = scheduler(db, recording(runtime, []), tmp_path)
    assert sched.tick() == []
    sched.stop(timeout=5)
    with db.session() as s:
        row = s.get_one(Worker, 1)
    assert (row.state, row.error) == (WorkerState.KILLED, 'no [roles.reviewer] in piloop.toml')


def test_a_hand_over_leaves_a_sweep_in_the_project_checkout_running_and_the_next_scheduler_re_attaches_to_it(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    hold = tmp_path / 'hold'
    hold.touch()
    harness = f'#!/bin/sh\necho $$ > {tmp_path}/agent.pid\nwhile [ -f {hold} ]; do sleep 0.05; done\n'
    configure(db, project, harness, watchdog={'workdir': 'project', 'triggers': []})
    with db.session() as s:
        s.add(Worker(kind=WorkerKind.WATCHDOG, project_id=project.id, reason='by hand'))
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
        sweep = s.get_one(Worker, 1)
    assert (sweep.state, sweep.error) == (WorkerState.EXITED, None), 'the next daemon watched it to its end'
    assert Path(project.path).is_dir()
    second.stop(timeout=5)


ENTRY_POINTS = [
    EntryPoint('every_tick', f'{PLUGIN}:EveryTick', TRIGGER_GROUP),
    EntryPoint('broken', f'{PLUGIN}:Broken', TRIGGER_GROUP),
    EntryPoint('auditor', f'{PLUGIN}:AUDITOR', ROLE_GROUP),
    EntryPoint('quick', f'{PLUGIN}:QUICK', ROLE_GROUP),
    EntryPoint('typed', f'{PLUGIN}:Typed', RUNTIME_GROUP),
]


def forget_loaded() -> None:
    triggers_module.triggers.cache_clear()
    config_module.registered_roles.cache_clear()
    factory_module.runtimes.cache_clear()


def installed(monkeypatch: pytest.MonkeyPatch, entries: list[EntryPoint]) -> None:
    """Make ``entries`` the installed entry points, and forget the ones that were loaded."""

    def entry_points(*, group: str) -> list[EntryPoint]:
        return [ep for ep in entries if ep.group == group]

    monkeypatch.setattr(triggers_module, 'entry_points', entry_points)
    monkeypatch.setattr(config_module, 'entry_points', entry_points)
    monkeypatch.setattr(factory_module, 'entry_points', entry_points)
    forget_loaded()


@pytest.fixture
def plugins(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    installed(monkeypatch, ENTRY_POINTS)
    yield
    forget_loaded()


def test_a_package_adds_a_role_and_triggers_through_entry_points(
    db: Database,
    project: Project,
    runtime: ScriptRuntime,
    tmp_path: Path,
    plugins: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``piloop init`` stores the package's role with its defaults, under the table of ``piloop.toml``.

    The daemon runs it on the package's trigger. A trigger that raises is traced, and the role's other triggers fire.
    """
    cfg = load_project_config(Path(project.path))
    auditor = cfg.roles.sweeping()['auditor']
    assert (auditor.timeout, auditor.triggers[0].kind) == (5, 'every_tick')
    toml = Path(project.path) / 'piloop.toml'
    toml.write_text(toml.read_text() + '\n[roles.auditor]\ntimeout = 9\n')
    assert load_project_config(Path(project.path)).roles.sweeping()['auditor'].timeout == 9, 'the table goes over'
    configure(db, project, '#!/bin/sh\ntrue\n', auditor={'triggers': [{'kind': 'broken'}, {'kind': 'every_tick'}]})
    sched = scheduler(db, recording(runtime, []), tmp_path)
    assert sched.tick() == [1]
    sched.wait(10)
    sched.stop(timeout=5)
    assert workers(db, 'auditor') == [(1, WorkerState.EXITED, 'every tick for auditor')]
    brief = (tmp_path / 'workers' / '1' / 'brief.md').read_text()
    assert brief.startswith('# Audit\n')
    assert 'trigger.failed Ns project=demo role=auditor trigger=broken error=the probe is down' in console_lines(
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
    project_path: Path, plugins: None
) -> None:
    toml = project_path / 'piloop.toml'
    toml.write_text(MINIMAL.format(roles='[roles.auditor]'))
    assert load_project_config(project_path).roles.sweeping()['auditor'].timeout == 5, 'an empty table: the defaults'
    toml.write_text(MINIMAL.format(roles='roles = { auditor = 3 }'))
    with pytest.raises(ConfigError, match=r'roles\.auditor = 3: Input should be a valid dictionary'):
        load_project_config(project_path)
    toml.write_text(MINIMAL.format(roles='roles = 3'))
    with pytest.raises(ConfigError, match=r'\n  roles = 3: Input should be a valid dictionary'):
        load_project_config(project_path)


@pytest.mark.parametrize(
    ('entry', 'error'),
    [
        (EntryPoint('every_tick', f'{PLUGIN}:NOT_A_TRIGGER', TRIGGER_GROUP), 'is not a piloop Trigger class'),
        (EntryPoint('cron', f'{PLUGIN}:EveryTick', TRIGGER_GROUP), "'cron' is a trigger already"),
        (EntryPoint('auditor', f'{PLUGIN}:NOT_A_ROLE', ROLE_GROUP), 'is not a piloop SweepRole'),
        (EntryPoint('research', f'{PLUGIN}:AUDITOR', ROLE_GROUP), "'research' is a built-in role"),
        (EntryPoint('typed', f'{PLUGIN}:NOT_A_RUNTIME', RUNTIME_GROUP), 'is not a piloop runtime class'),
        (EntryPoint('other', f'{PLUGIN}:Typed', RUNTIME_GROUP), "it needs kind = 'other'"),
        (EntryPoint('unsure', f'{PLUGIN}:Unsure', RUNTIME_GROUP), 'and an invocation'),
        (EntryPoint('herdr', f'{PLUGIN}:Typed', RUNTIME_GROUP), "'herdr' is a runtime already"),
    ],
)
def test_an_entry_point_of_the_wrong_kind_fails_with_its_name(
    monkeypatch: pytest.MonkeyPatch, entry: EntryPoint, error: str
) -> None:
    installed(monkeypatch, [EntryPoint('every_tick', f'{PLUGIN}:EveryTick', TRIGGER_GROUP), entry])
    try:
        with pytest.raises((TypeError, ValueError), match=error):
            triggers_module.triggers()
            config_module.registered_roles()
            factory_module.runtimes()
    finally:
        forget_loaded()


def test_a_package_adds_a_role_that_does_runs(project_path: Path, plugins: None) -> None:
    cfg = load_project_config(project_path)
    assert list(cfg.roles.doing_runs()) == ['quick', 'worker']
    quick = cfg.roles.doing_runs()['quick']
    assert (quick.takes, quick.timeout, quick.max_concurrent) == ('priority >= 3', 60, 1)


@pytest.fixture
def typed(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Install only the ``typed`` runtime, so that no package role or trigger requests an agent."""
    installed(monkeypatch, [EntryPoint('typed', f'{PLUGIN}:Typed', RUNTIME_GROUP)])
    yield
    forget_loaded()


def test_a_package_adds_a_runtime_and_its_agents_start_the_argv_of_its_invocation(
    db: Database, project: Project, tmp_path: Path, typed: None
) -> None:
    """``typed`` takes the interactive argv of the harness. Every lookup by name, after the start too, finds it."""
    write_toml(Path(project.path), HARNESS_PROMPT_TO_FILE)
    script = {'executable': str(tmp_path / 'harness.sh'), 'headless': ['{prompt}'], 'interactive': ['interactive']}
    watchdog = {'runtime': 'typed', 'workdir': 'project', 'triggers': []}
    with db.session() as s:
        cfg = load_project_config(Path(project.path), harness={'script': script}, roles={'watchdog': watchdog})
        s.get_one(Project, project.id).config = cfg.model_dump(mode='json')
        s.add(Worker(kind=WorkerKind.WATCHDOG, project_id=project.id, reason='by hand'))
        s.commit()
    sched = scheduler(db, make_runtime, tmp_path)
    assert sched.tick() == [1]
    sched.wait(30)
    sched.stop(timeout=5)
    with db.session() as s:
        worker = s.get_one(Worker, 1)
        assert (worker.state, worker.runtime) == (WorkerState.EXITED, 'typed')
    assert (Path(project.path) / 'prompt.txt').read_text() == 'interactive'


def test_a_runtime_that_is_not_registered_fails_with_the_registered_ones(project_path: Path, plugins: None) -> None:
    with pytest.raises(
        ConfigError,
        match=re.escape(f"runtime = 'tmux': unknown runtime 'tmux'; registered runtimes: {', '.join(runtimes())}"),
    ):
        load_project_config(project_path, runtime='tmux')
    with pytest.raises(ConfigError, match=r"roles.research.runtime = 'tmux': unknown runtime 'tmux'"):
        load_project_config(project_path, roles={'research': {'runtime': 'tmux'}})
