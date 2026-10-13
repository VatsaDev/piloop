"""Roles as data: a role that piloop.toml defines, new triggers for a built-in role, and roles and triggers that packages
add through entry points."""

import time
from collections.abc import Iterator
from importlib.metadata import EntryPoint
from pathlib import Path

import pytest
from sqlalchemy import select

from piloop import config as config_module, triggers as triggers_module
from piloop.config import ROLE_GROUP, ConfigError, RuntimeKind, load_project_config
from piloop.daemon.runner import RunExecutor
from piloop.daemon.scheduler import Scheduler
from piloop.db.models import Comment, CommentKind, Project, TargetKind, Worker, WorkerKind, WorkerState, utcnow
from piloop.db.session import Database
from piloop.otel import Tracer
from piloop.runtime import Runtime
from piloop.runtime.factory import RuntimeFactory
from piloop.triggers import TRIGGER_GROUP
from tests.conftest import ScriptRuntime, write_toml
from tests.daemon.test_triggers import BLOCKING, fail_a_run, reported_events, wait_for, workers

REVIEW = "SELECT id FROM comments WHERE kind = 'decision' AND created_at > :since"
PLUGIN = 'tests.fixtures.plugin'


def recording(runtime: ScriptRuntime, kinds: list[RuntimeKind]) -> RuntimeFactory:
    """A factory that runs every agent in ``runtime`` and records the kind of runtime that was asked for."""

    def make(kind: RuntimeKind, *, project: str, project_path: Path) -> Runtime:
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
    kinds: list[RuntimeKind] = []
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
    assert RuntimeKind.HERDR in kinds


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
    deadline = time.monotonic() + 10
    while not (tmp_path / 'agent.pid').exists() and time.monotonic() < deadline:
        time.sleep(0.05)
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
]


def installed(monkeypatch: pytest.MonkeyPatch, entries: list[EntryPoint]) -> None:
    """Make ``entries`` the installed entry points, and forget the ones that were loaded."""

    def entry_points(*, group: str) -> list[EntryPoint]:
        return [ep for ep in entries if ep.group == group]

    monkeypatch.setattr(triggers_module, 'entry_points', entry_points)
    monkeypatch.setattr(config_module, 'entry_points', entry_points)
    triggers_module.triggers.cache_clear()
    config_module.registered_roles.cache_clear()


@pytest.fixture
def plugins(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    installed(monkeypatch, ENTRY_POINTS)
    yield
    triggers_module.triggers.cache_clear()
    config_module.registered_roles.cache_clear()


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
    assert (
        'trigger.failed 0.0s project=demo role=auditor trigger=broken error=the probe is down'
        in capsys.readouterr().err
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
    finally:
        triggers_module.triggers.cache_clear()
        config_module.registered_roles.cache_clear()
