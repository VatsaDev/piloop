import json
import re
import tomllib
from pathlib import Path

import pytest
from dirty_equals import IsFloat
from inline_snapshot import snapshot
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode
from pytest_mock import MockerFixture
from sqlalchemy import select

from piloop.agents import capture
from piloop.agents.capture import ENTIRE_INSTALL, MIN_ENTIRE
from piloop.agents.launcher import PROMPT, STARTUP_ANOMALY, STOPPED_ERROR, Launcher, fill
from piloop.agents.roles import PILOOP_HINT, PROMPTS, ROLES, SQL_HINT, Role, compose_brief
from piloop.config import ProjectConfig, load_project_config
from piloop.db.models import Event, Project, Worker, WorkerKind, WorkerState
from piloop.db.session import Database
from piloop.harnesses import DEFAULT_HARNESSES
from piloop.mcp import MCP
from piloop.otel import Tracer
from piloop.runtime import StartFailed, WorkerSpec, WorkerStatus
from piloop.settings import Settings
from piloop.usage.jsonl import CWD_ENCODERS
from tests.conftest import (
    HARNESS_FAILS,
    HARNESS_PROMPT_TO_FILE,
    HARNESS_SLEEPS,
    ScriptRuntime,
    events,
    factory,
    write_toml,
)
from tests.fixtures.fake_entire import CHECKPOINT, TOKENS
from tests.usage.test_usage import PI_SPEC, SESSIONS

MINTED_HARNESS = {
    'entire_agent': 'pi',
    'headless': ['--session-id', '{session_id}', '{prompt}'],
    'interactive': ['--session-id', '{session_id}'],
}
CODEX_ID = '01a09262-d574-7e10-9f75-23b0ce9c0a2f'
CODEX_LIKE = f'#!/bin/sh\necho \'{{"type":"thread.started","thread_id":"{CODEX_ID}"}}\'\nsleep 0.3\necho more\n'
"""``codex exec --json``: the thread id is the first stdout line, then the run's events."""

UUID = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$')

WATCHDOG: Role = ROLES[WorkerKind.WATCHDOG]


def request(db: Database, project: Project, kind: WorkerKind = WorkerKind.WATCHDOG, **fields: object) -> int:
    with db.session() as s:
        worker = Worker(kind=kind, project_id=project.id, reason='test', **fields)
        s.add(worker)
        s.commit()
        return worker.id


def launcher(db: Database, runtime: ScriptRuntime, tmp_path: Path, tracer: Tracer | None = None) -> Launcher:
    return Launcher(db, tracer or Tracer(None), tmp_path, factory(runtime))


def launch(db: Database, runtime: ScriptRuntime, tmp_path: Path, worker_id: int, tracer: Tracer | None = None):
    with db.session() as s:
        project = s.get_one(Project, s.get_one(Worker, worker_id).project_id)
        brief = compose_brief(WATCHDOG, project, worker_id, reason='test', context='')
    cwd = tmp_path / 'cwd'
    cwd.mkdir(exist_ok=True)
    return launcher(db, runtime, tmp_path, tracer).launch(worker_id, brief=brief, cwd=cwd, log_dir=tmp_path / 'w')


def worker(db: Database, worker_id: int) -> Worker:
    with db.session() as s:
        return s.get_one(Worker, worker_id)


def use_harness(db: Database, project: Project, body: str, **definition: object) -> None:
    """Point the test harness at ``body``; ``definition`` overrides keys of its ``[harness.script]`` table."""
    write_toml(Path(project.path), body)
    script = {**load_project_config(Path(project.path)).harness['script'].model_dump(mode='json'), **definition}
    with db.session() as s:
        s.get_one(Project, project.id).config = load_project_config(
            Path(project.path), harness={'script': script}
        ).model_dump(mode='json')
        s.commit()


def test_brief_is_role_prompt_project_brief_procedures_then_context(project: Project, db: Database) -> None:
    with db.session() as s:
        brief = compose_brief(WATCHDOG, s.get_one(Project, project.id), 7, reason='timer', context='- extra line')
    assert brief.startswith((PROMPTS / 'watchdog.md').read_text()[:40])
    assert (
        brief.index('# Watchdog')
        < brief.index('# Demo')
        < brief.index('# Working with the piloop')
        < brief.index('# Context')
    )
    assert brief.endswith(
        f"""# Context
- You are worker#7, the watchdog for project#1 (demo).
- Project repo: {project.path} (every candidate commit and Entire checkpoint is in it).
- Sign what you write as 'watchdog#7' (comments.author, ideas.created_by).
- Trigger: timer
- SQL tool: `{SQL_HINT} "<sql>"`
- File a seed: `{PILOOP_HINT} idea seed demo "<hunch, one line>" --from "<pointers: ids, paths, numbers>"` (`--base\
 <candidate>` when it is about that tree): research checks it against the existing work and develops it into an idea,\
 or rejects it with the reason.
- You may write: comments(escalation, finding, note), seeds via idea seed.
- extra line
"""
    )


def test_a_roles_addendum_follows_the_project_brief_in_that_roles_briefs_only(project: Project, db: Database) -> None:
    (Path(project.path) / 'research.md').write_text('# Search wide first\n')
    with db.session() as s:
        p = s.get_one(Project, project.id)
        p.config = load_project_config(Path(project.path), roles={'research': {'addendum': 'research.md'}}).model_dump(
            mode='json'
        )
        research = compose_brief(ROLES[WorkerKind.RESEARCH], p, 7, reason='timer', context='')
        watchdog = compose_brief(WATCHDOG, p, 8, reason='timer', context='')
    assert (
        research.index('# Demo') < research.index('# Search wide first') < research.index('# Working with the piloop')
    )
    assert '# Search wide first' not in watchdog


def test_launch_runs_the_row_through_its_lifecycle(
    project: Project, db: Database, runtime: ScriptRuntime, tmp_path: Path, tracer: Tracer, spans: InMemorySpanExporter
) -> None:
    use_harness(db, project, HARNESS_PROMPT_TO_FILE)
    worker_id = request(db, project)
    outcome = launch(db, runtime, tmp_path, worker_id, tracer)
    assert (outcome.status, outcome.exit_code, outcome.error) == (WorkerStatus.EXITED, 0, None)
    w = worker(db, worker_id)
    assert (w.state, w.harness, w.model, w.runtime, w.log_dir, w.error) == (
        WorkerState.EXITED,
        'script',
        'watchdog-model',
        'subprocess',
        str(tmp_path / 'w'),
        None,
    )
    assert w.handle['pid'] and w.ended_at is not None and w.ended_at >= w.started_at
    assert (tmp_path / 'cwd' / 'prompt.txt').read_text() == PROMPT.format(path=tmp_path / 'w' / 'brief.md')
    assert (tmp_path / 'w' / 'brief.md').read_text().startswith('# Watchdog')
    assert (tmp_path / 'w' / 'transcript.txt').is_file()
    assert runtime.killed, 'the runtime is told to release the worker'
    with db.session() as s:
        events = [(e.kind, e.target_id, e.payload) for e in s.scalars(select(Event).order_by(Event.id))]
    assert events == snapshot(
        [
            ('watchdog.started', 1, {'harness': 'script', 'model': 'watchdog-model', 'reason': 'test'}),
            ('watchdog.finished', 1, {'reason': 'test', 'status': 'exited', 'error': None}),
        ]
    )
    [span] = spans.get_finished_spans()
    assert (span.name, dict(span.attributes or {})) == (
        'piloop.agent',
        {
            'worker': 1,
            'kind': 'watchdog',
            'harness': 'script',
            'model': 'watchdog-model',
            'reason': 'test',
            'state': 'exited',
        },
    )


def test_rotation_over_the_role_harnesses_unless_pinned(
    project: Project, db: Database, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    write_toml(Path(project.path), HARNESS_PROMPT_TO_FILE)
    cfg = load_project_config(
        Path(project.path),
        harnesses=['script', 'pi'],
        model={'script': 'm', 'pi': 'p'},
        roles={'watchdog': {'harnesses': ['script', 'pi'], 'model': {'pi': 'wp'}}},
    )
    with db.session() as s:
        s.get_one(Project, project.id).config = cfg.model_dump(mode='json')
        s.commit()
    ids = [request(db, project), request(db, project), request(db, project, harness='script', model='pinned')]
    request(db, project, kind=WorkerKind.RESEARCH)  # another kind: not in this rotation
    picks = []
    for worker_id in ids:
        launch(db, runtime, tmp_path, worker_id)
        w = worker(db, worker_id)
        picks.append((w.harness, w.model))
    assert picks == snapshot([('script', 'watchdog-model'), ('pi', 'wp'), ('script', 'pinned')])


def test_an_agent_writes_through_piloop_sql_as_its_own_actor(
    project: Project, db: Database, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """The brief's SQL command in a launched process records ``<kind>#<worker>``: PILOOP_ACTOR comes with PILOOP_HOME."""
    use_harness(
        db, project, f'#!/bin/sh\n{SQL_HINT} "UPDATE projects SET name = \'renamed\' WHERE id = {project.id}"\n'
    )
    worker_id = request(db, project, kind=WorkerKind.RESEARCH)
    outcome = launch(db, runtime, tmp_path, worker_id)
    assert outcome.error is None, (tmp_path / 'w' / 'worker.stderr').read_text()
    with db.session() as s:
        row = s.get_one(Project, project.id)
        assert (row.name, row.updated_by) == ('renamed', f'research#{worker_id}')


def test_harness_exit_code_is_recorded_not_raised(
    project: Project, db: Database, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    use_harness(db, project, HARNESS_FAILS)
    worker_id = request(db, project)
    outcome = launch(db, runtime, tmp_path, worker_id)
    w = worker(db, worker_id)
    assert (outcome.error, outcome.exit_code, w.state, w.error) == (
        'worker exit 3',
        3,
        WorkerState.EXITED,
        'worker exit 3',
    )


def test_timeout_kills_and_records(project: Project, db: Database, runtime: ScriptRuntime, tmp_path: Path) -> None:
    write_toml(Path(project.path), HARNESS_SLEEPS)
    cfg = load_project_config(Path(project.path), roles={'watchdog': {'timeout': 0.3}})
    with db.session() as s:
        s.get_one(Project, project.id).config = cfg.model_dump(mode='json')
        s.commit()
    worker_id = request(db, project)
    outcome = launch(db, runtime, tmp_path, worker_id)
    w = worker(db, worker_id)
    assert (outcome.status, w.state, w.error) == (
        WorkerStatus.TIMED_OUT,
        WorkerState.KILLED,
        'worker timeout after 0.3s',
    )


def test_runtime_failure_is_isolated(
    project: Project,
    db: Database,
    runtime: ScriptRuntime,
    tmp_path: Path,
    mocker: MockerFixture,
    tracer: Tracer,
    spans: InMemorySpanExporter,
) -> None:
    mocker.patch.object(runtime, 'start', side_effect=RuntimeError('no server'))
    worker_id = request(db, project)
    outcome = launch(db, runtime, tmp_path, worker_id, tracer)
    w = worker(db, worker_id)
    assert (outcome.error, w.state, w.handle, w.error) == (
        'RuntimeError: no server',
        WorkerState.KILLED,
        {},
        'RuntimeError: no server',
    )
    [span] = spans.get_finished_spans()
    assert span.status.status_code is StatusCode.ERROR and dict(span.attributes or {})['state'] == 'killed'
    with db.session() as s:
        [finished] = s.scalars(select(Event).where(Event.kind == 'watchdog.finished')).all()
    assert finished.payload['error'] == 'RuntimeError: no server'


def test_a_failed_start_leaves_its_screen_as_the_transcript_and_its_last_lines_in_the_error(
    project: Project, db: Database, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """A failed start keeps the screen the agent was stuck on, so the row's error shows the cause."""
    screen = 'Welcome to Claude Code\n\n' + ''.join(f'line {i}\n' for i in range(20)) + '> do the thing\n\n'
    error = 'HerdrError: prompt_lost: watchdog-1 did not start working after 2 prompt submissions'
    mocker.patch.object(runtime, 'start', side_effect=StartFailed(error, screen))
    worker_id = request(db, project)
    outcome = launch(db, runtime, tmp_path, worker_id)
    assert (tmp_path / 'w' / 'transcript.txt').read_text() == screen
    tail = '\n'.join([*(f'line {i}' for i in range(9, 20)), '> do the thing'])
    assert outcome.error == worker(db, worker_id).error == f'{error}\nIts screen ended with:\n{tail}'
    assert outcome.startup_failed

    mocker.patch.object(runtime, 'start', side_effect=StartFailed(error, '\n'))
    assert launch(db, runtime, tmp_path, request(db, project)).error == error, 'a blank screen adds nothing'


def anomalies(db: Database) -> list[tuple[int, dict[str, object]]]:
    with db.session() as s:
        return [(e.target_id, e.payload) for e in s.scalars(select(Event).where(Event.kind == STARTUP_ANOMALY))]


def test_an_agent_that_never_got_going_is_an_anomaly(
    project: Project,
    db: Database,
    runtime: ScriptRuntime,
    tmp_path: Path,
    mocker: MockerFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A start that fails, or an agent that ends at once without work, is a startup anomaly, not a finished agent.

    An unanswered first-launch dialog (Codex's "Trust this folder?") fails the start. A dialog that Herdr reports as
    idle ends the agent quickly, as if it had finished.
    """
    mocker.patch.object(runtime, 'start', side_effect=RuntimeError('agent_not_ready: Trust this folder?'))
    dialog = request(db, project)
    outcome = launch(db, runtime, tmp_path, dialog)
    assert (outcome.startup_failed, worker(db, dialog).state) == (True, WorkerState.KILLED)
    assert anomalies(db) == [
        (
            dialog,
            {
                'kind': 'watchdog',
                'harness': 'script',
                'run': None,
                'seconds': IsFloat(ge=0),
                'error': 'RuntimeError: agent_not_ready: Trust this folder?',
            },
        )
    ]
    mocker.stopall()

    monkeypatch.setenv('HOME', str(tmp_path / 'home'))
    counted = {'headless': ['--session-id', '{session_id}', '{prompt}'], 'usage': PI_SPEC}
    use_harness(db, project, HARNESS_PROMPT_TO_FILE, **counted)
    idle_at_once = request(db, project)
    outcome = launch(db, runtime, tmp_path, idle_at_once)
    assert (outcome.startup_failed, outcome.error, worker(db, idle_at_once).state) == (True, None, WorkerState.EXITED)
    assert [w for w, _ in anomalies(db)] == [dialog, idle_at_once], 'ended within a minute without a token'

    monkeypatch.setattr('piloop.agents.launcher.STARTUP_SECONDS', 0)
    assert not launch(db, runtime, tmp_path, request(db, project)).startup_failed, 'past the startup window'
    monkeypatch.undo()
    use_harness(db, project, HARNESS_PROMPT_TO_FILE)
    assert not launch(db, runtime, tmp_path, request(db, project)).startup_failed, (
        'a harness whose usage is not read cannot tell "spent nothing" from "unknown"'
    )
    assert len(anomalies(db)) == 2


def test_nothing_starts_after_cancel(project: Project, db: Database, runtime: ScriptRuntime, tmp_path: Path) -> None:
    worker_id = request(db, project)
    lc = launcher(db, runtime, tmp_path)
    lc.cancel()
    outcome = lc.launch(worker_id, brief='x', cwd=tmp_path, log_dir=tmp_path / 'w')
    w = worker(db, worker_id)
    assert (outcome.error, w.state, w.harness) == (STOPPED_ERROR, WorkerState.REQUESTED, None), 'stop() closes rows'
    assert not (tmp_path / 'w').exists()


def test_a_harness_with_an_entire_agent_gets_a_minted_session_id_in_its_argv_and_on_its_row(
    project: Project, db: Database, runtime: ScriptRuntime, tmp_path: Path, tracer: Tracer, spans: InMemorySpanExporter
) -> None:
    use_harness(db, project, HARNESS_PROMPT_TO_FILE)
    plain = request(db, project)
    launch(db, runtime, tmp_path, plain)
    assert worker(db, plain).session_id is None, 'no entire_agent: nothing to attach, no id minted'
    use_harness(db, project, '#!/bin/sh\nprintf %s "$2" > session.txt\n', **MINTED_HARNESS)
    worker_id = request(db, project)
    launch(db, runtime, tmp_path, worker_id, tracer)
    w = worker(db, worker_id)
    assert w.session_id is not None and UUID.match(w.session_id), 'a uuid4, as pi and claude --session-id take'
    assert (tmp_path / 'cwd' / 'session.txt').read_text() == w.session_id
    assert dict(span_attributes(spans, 'piloop.agent', worker_id))['session_id'] == w.session_id


def test_a_harness_that_prints_its_session_id_has_it_read_back_by_the_runtime(
    project: Project, db: Database, runtime: ScriptRuntime, tmp_path: Path, tracer: Tracer, spans: InMemorySpanExporter
) -> None:
    codex = DEFAULT_HARNESSES['codex']
    use_harness(db, project, CODEX_LIKE, entire_agent='codex', session_id_pattern=codex.session_id_pattern)
    worker_id = request(db, project)
    launch(db, runtime, tmp_path, worker_id, tracer)
    assert worker(db, worker_id).session_id == CODEX_ID
    assert span_attributes(spans, 'piloop.agent', worker_id)['session_id'] == CODEX_ID
    use_harness(db, project, HARNESS_PROMPT_TO_FILE, entire_agent='codex', session_id_pattern=codex.session_id_pattern)
    silent = request(db, project)
    launch(db, runtime, tmp_path, silent, tracer)
    assert worker(db, silent).session_id is None
    assert events(spans, 'worker.session_unknown') == [{'worker': silent, 'runtime': 'subprocess'}], (
        'nothing to attach, now or later'
    )


def test_the_session_is_attached_to_entire_from_the_cwd_when_the_agent_ends(
    project: Project,
    db: Database,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
    entire_log,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    use_harness(db, project, HARNESS_PROMPT_TO_FILE)
    plain = request(db, project)
    launch(db, runtime, tmp_path, plain, tracer)
    assert entire_log() == [] and worker(db, plain).checkpoint_id is None, 'no entire_agent: entire is never called'
    use_harness(db, project, HARNESS_PROMPT_TO_FILE, **MINTED_HARNESS)
    worker_id = request(db, project)
    launch(db, runtime, tmp_path, worker_id, tracer)
    w = worker(db, worker_id)
    assert w.checkpoint_id == CHECKPOINT
    assert entire_log() == [
        {'argv': ['session', 'attach', w.session_id, '--agent', 'pi'], 'cwd': str(tmp_path / 'cwd')},
        {'argv': ['session', 'info', w.session_id, '--json'], 'cwd': str(tmp_path / 'cwd')},
    ], "from the agent's own cwd: pi and claude key their session stores by it; the id comes from info's JSON"
    assert events(spans, 'worker.session_attached') == [
        {'worker': worker_id, 'session_id': w.session_id, 'checkpoint': CHECKPOINT, 'agent': 'Pi', 'tokens': TOKENS}
    ]
    assert span_attributes(spans, 'piloop.agent', worker_id)['checkpoint'] == CHECKPOINT
    with db.session() as s:
        [finished] = s.scalars(select(Event).where(Event.kind == 'watchdog.finished', Event.target_id == worker_id))
        assert finished.payload['status'] == 'exited'
    for mode in ('added', 'existing'):
        monkeypatch.setenv('FAKE_ENTIRE_MODE', mode)
        other = request(db, project)
        launch(db, runtime, tmp_path, other, tracer)
        assert worker(db, other).checkpoint_id == CHECKPOINT, (
            'HEAD carrying a trailer adds the session to that checkpoint; a session entire already knows is not an error'
        )
    monkeypatch.setenv('FAKE_ENTIRE_MODE', 'fail')
    failed = request(db, project)
    launch(db, runtime, tmp_path, failed, tracer)
    w = worker(db, failed)
    assert (w.state, w.error, w.checkpoint_id) == (WorkerState.EXITED, None, None), 'the row is fine'
    assert events(spans, 'worker.session_unattached') == [
        {
            'worker': failed,
            'session_id': w.session_id,
            'error': f'entire session attach exit 1: Error: invalid session ID: {w.session_id}',
        }
    ]
    assert entire_log()[-1]['argv'][:2] == ['session', 'attach'], 'no info call after a failed attach'


def test_the_daemons_entire_version_gates_attach_and_is_on_the_event(
    project: Project,
    db: Database,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
    entire_log,
) -> None:
    use_harness(db, project, HARNESS_PROMPT_TO_FILE, **MINTED_HARNESS)
    gated = launcher(db, runtime, tmp_path, tracer)
    gated.entire_version = '0.10.5'
    (tmp_path / 'cwd').mkdir()
    old = request(db, project)
    gated.launch(old, brief='b', cwd=tmp_path / 'cwd', log_dir=tmp_path / 'logs')
    assert entire_log() == [] and worker(db, old).checkpoint_id is None, 'an untested entire is not asked'
    w = worker(db, old)
    assert events(spans, 'worker.session_unattached') == [
        {
            'worker': old,
            'session_id': w.session_id,
            'error': f'entire 0.10.5 is older than {MIN_ENTIRE}; upgrade it: {ENTIRE_INSTALL}',
        }
    ], 'nothing is attached until entire is upgraded'
    gated.entire_version = '0.11.0'
    new = request(db, project)
    gated.launch(new, brief='b', cwd=tmp_path / 'cwd', log_dir=tmp_path / 'logs')
    assert worker(db, new).checkpoint_id == CHECKPOINT
    assert events(spans, 'worker.session_attached')[0]['entire'] == '0.11.0'


def test_attach_failures_are_events_and_never_touch_the_row(
    project: Project,
    db: Database,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
    entire_log,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    use_harness(db, project, HARNESS_PROMPT_TO_FILE, **MINTED_HARNESS)
    monkeypatch.setattr(capture, 'settings', Settings(home=tmp_path, entire_attach_timeout_seconds=0.3))
    monkeypatch.setenv('FAKE_ENTIRE_MODE', 'hang')
    slow = request(db, project)
    launch(db, runtime, tmp_path, slow, tracer)
    monkeypatch.setenv('FAKE_ENTIRE_MODE', 'silent')
    silent = request(db, project)
    launch(db, runtime, tmp_path, silent, tracer)
    monkeypatch.setenv('PATH', str(tmp_path))
    missing = request(db, project)
    launch(db, runtime, tmp_path, missing, tracer)
    assert [(e['worker'], e['error']) for e in events(spans, 'worker.session_unattached')] == [
        (slow, 'entire session attach timed out after 0.3s'),
        (silent, f'entire session info reports no checkpoint for session {worker(db, silent).session_id}'),
        (missing, 'entire is not on PATH; install it: curl -fsSL https://entire.io/install.sh | bash'),
    ]
    for worker_id in (slow, silent, missing):
        w = worker(db, worker_id)
        assert (w.state, w.checkpoint_id) == (WorkerState.EXITED, None) and w.session_id is not None, 'the row is fine'


def test_the_agents_token_usage_is_summed_from_its_session_file_when_it_ends(
    project: Project,
    db: Database,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
) -> None:
    home = tmp_path / 'home'
    monkeypatch.setenv('HOME', str(home))
    folder = home / '.pi' / 'agent' / 'sessions' / CWD_ENCODERS['dash_separators_wrapped'](tmp_path / 'cwd')
    writes_session = (
        f'#!/bin/sh\nmkdir -p {folder}\ncp {SESSIONS / "pi.jsonl"} "{folder}/2026-09-09T05-48-14-300Z_$2.jsonl"\n'
    )
    minted = {'headless': ['--session-id', '{session_id}', '{prompt}'], 'usage': PI_SPEC}
    use_harness(db, project, writes_session, **minted)
    worker_id = request(db, project)
    launch(db, runtime, tmp_path, worker_id, tracer)
    w = worker(db, worker_id)
    assert w.session_id is not None, 'a usage spec alone mints the id: no Entire needed to know what an agent cost'
    assert w.usage == {
        'input': 4,
        'output': 993,
        'cache_read': 6763,
        'cache_write': 8718,
        'total': 16478,
        'turns': 2,
        'source': str(folder / f'2026-09-09T05-48-14-300Z_{w.session_id}.jsonl'),
    }, "pi's store is keyed by the agent's cwd, the worktree; read before the caller removes it"
    assert span_attributes(spans, 'piloop.agent', worker_id)['tokens'] == 16478
    assert events(spans, 'worker.usage_unknown') == []
    assert anomalies(db) == [], 'it spent tokens: it started, however fast it ended'

    use_harness(db, project, HARNESS_PROMPT_TO_FILE, **minted)
    silent = request(db, project)
    launch(db, runtime, tmp_path, silent, tracer)
    w = worker(db, silent)
    assert (w.state, w.error, w.usage) == (WorkerState.EXITED, None, None), 'the row is fine'
    assert events(spans, 'worker.usage_unknown') == [
        {'worker': silent, 'error': f'no session file for {w.session_id} under {home}'}
    ]
    use_harness(db, project, HARNESS_PROMPT_TO_FILE)
    plain = request(db, project)
    launch(db, runtime, tmp_path, plain, tracer)
    assert worker(db, plain).usage is None and len(events(spans, 'worker.usage_unknown')) == 1, (
        'no usage spec: nothing to read and nothing to say, like no entire_agent'
    )
    mocker.patch.object(Launcher, '_prepare', side_effect=RuntimeError('no config'))
    broken = request(db, project)
    launch(db, runtime, tmp_path, broken, tracer)
    assert (worker(db, broken).state, worker(db, broken).usage) == (WorkerState.KILLED, None)
    assert len(events(spans, 'worker.usage_unknown')) == 1, 'an agent that never ran has no session to read'


def test_a_killed_agents_usage_is_read_too(
    project: Project, db: Database, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The usage of an agent killed at its timeout is read too: it spent its whole budget."""
    home = tmp_path / 'home'
    monkeypatch.setenv('HOME', str(home))
    folder = home / '.pi' / 'agent' / 'sessions' / CWD_ENCODERS['dash_separators_wrapped'](tmp_path / 'cwd')
    writes_then_hangs = (
        f'#!/bin/sh\nmkdir -p {folder}\ncp {SESSIONS / "pi.jsonl"} "{folder}/2026-09-09T05-48-14-300Z_$2.jsonl"\n'
        'sleep 30 &\necho $! > ../child.pid\nwait\n'
    )
    use_harness(db, project, writes_then_hangs, headless=['--session-id', '{session_id}', '{prompt}'], usage=PI_SPEC)
    with db.session() as s:
        p = s.get_one(Project, project.id)
        config = ProjectConfig.model_validate(p.config).model_dump(mode='json')
        config['roles']['watchdog']['timeout'] = 0.5
        p.config = config
        s.commit()
    worker_id = request(db, project)
    outcome = launch(db, runtime, tmp_path, worker_id)
    w = worker(db, worker_id)
    assert (outcome.status, w.state, w.error) == (
        WorkerStatus.TIMED_OUT,
        WorkerState.KILLED,
        'worker timeout after 0.5s',
    )
    assert w.usage is not None and (w.usage['total'], w.usage['turns']) == (16478, 2)


def span_attributes(spans: InMemorySpanExporter, name: str, worker_id: int) -> dict[str, object]:
    [span] = [
        s for s in spans.get_finished_spans() if s.name == name and (s.attributes or {}).get('worker') == worker_id
    ]
    return dict(span.attributes or {})


def test_fill_keeps_literal_braces() -> None:
    assert fill('{"k": "{model}"}', model='m') == '{"k": "m"}'


def test_the_argv_gets_the_agents_cwd(project: Project, db: Database, runtime: ScriptRuntime, tmp_path: Path) -> None:
    use_harness(db, project, RECORDS_ARGV_AND_ENV, headless=['--cd', '{cwd}', '{prompt}'])
    launch(db, runtime, tmp_path, request(db, project))
    assert (tmp_path / 'cwd' / 'argv.txt').read_text().splitlines()[:2] == ['--cd', str(tmp_path / 'cwd')]


def test_codex_trusts_the_agents_worktree_and_gives_its_tool_shells_the_agents_env() -> None:
    """The ``-c`` values parse the way Codex reads them: a dotted key split on every ``.``, and a TOML value."""
    cwd = '/home/me/.piloop/runs/7/worktree'
    codex = DEFAULT_HARNESSES['codex']
    for template in (codex.interactive, codex.headless):
        args = [fill(a, cwd=cwd, model='m', prompt='p', session_id='s') for a in template]
        flags = [value.partition('=') for flag, value in zip(args, args[1:], strict=False) if flag == '-c']
        parsed = {key: tomllib.loads(f'v = {value}')['v'] for key, _, value in flags}
        assert parsed['projects'] == {cwd: {'trust_level': 'trusted'}}, 'this launch only; no dialog, no config write'
        assert {k: v for k, v in parsed.items() if k.startswith('shell_environment_policy.')} == {
            'shell_environment_policy.inherit': 'all',
            'shell_environment_policy.ignore_default_excludes': True,
            'shell_environment_policy.include_only': [],
        }, 'PILOOP_WORKER_TOKEN and PILOOP_ACTOR reach every command; the MCP renderer excludes the keys'


def test_a_brief_past_the_limit_of_one_argument_reaches_the_agent_as_a_file(
    project: Project, db: Database, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A brief longer than Linux's limit on one argument (``MAX_ARG_STRLEN``, 128 KiB) reaches the agent as a file.

    The agent's first prompt names the file instead of holding the brief.
    """
    use_harness(db, project, RECORDS_ARGV_AND_ENV)
    brief = 'x' * 200 * 1024 + '\n'
    cwd = tmp_path / 'cwd'
    cwd.mkdir()
    outcome = launcher(db, runtime, tmp_path).launch(request(db, project), brief=brief, cwd=cwd, log_dir=tmp_path / 'w')
    assert (outcome.status, outcome.error, outcome.startup_failed) == (WorkerStatus.EXITED, None, False)
    assert (cwd / 'argv.txt').read_text().splitlines() == [PROMPT.format(path=tmp_path / 'w' / 'brief.md')]
    assert (tmp_path / 'w' / 'brief.md').read_text() == brief


def test_a_herdr_agent_is_prompted_with_where_its_brief_is(
    project: Project, db: Database, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Herdr submits ``WorkerSpec.prompt`` as one argument of ``agent prompt <name> <text>``, under the same limit."""
    with db.session() as s:
        p = s.get_one(Project, project.id)
        p.config = {**p.config, 'runtime': 'herdr'}
        s.commit()
    specs: list[WorkerSpec] = []
    start = runtime.start
    monkeypatch.setattr(runtime, 'start', lambda spec: specs.append(spec) or start(spec))
    launcher(db, runtime, tmp_path).launch(
        request(db, project), brief='y' * 200 * 1024, cwd=tmp_path, log_dir=tmp_path / 'w'
    )
    [spec] = specs
    assert (spec.prompt, spec.args) == (PROMPT.format(path=tmp_path / 'w' / 'brief.md'), [])


def give_watchdog_servers(db: Database, project: Project, servers: dict[str, object]) -> None:
    """``[mcp.*]`` tables in the stored config, all listed by ``[roles.watchdog] mcp``."""
    with db.session() as s:
        p = s.get_one(Project, project.id)
        cfg = ProjectConfig.model_validate(p.config)
        watchdog = cfg.roles.watchdog.model_copy(update={'mcp': list(servers)})
        roles = cfg.roles.model_copy(update={'watchdog': watchdog})
        dumped = cfg.model_copy(update={'roles': roles}).model_dump(mode='json') | {'mcp': servers}
        p.config = ProjectConfig.model_validate(dumped).model_dump(mode='json')
        s.commit()


RECORDS_ARGV_AND_ENV = '#!/bin/sh\nprintf "%s\\n" "$@" > argv.txt\nenv > env.txt\n'


def test_the_mcp_element_expands_to_the_roles_servers_and_their_env_reaches_the_agent(
    project: Project, db: Database, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    use_harness(db, project, RECORDS_ARGV_AND_ENV, mcp='pi', headless=['--no-extensions', MCP, '{prompt}'])
    give_watchdog_servers(db, project, {'probe': {'command': ['cat'], 'env': ['PROBE_KEY']}})
    monkeypatch.setenv('PROBE_KEY', 'probe-value')
    specs: list[WorkerSpec] = []
    start = runtime.start
    monkeypatch.setattr(runtime, 'start', lambda spec: specs.append(spec) or start(spec))
    worker_id = request(db, project)
    launch(db, runtime, tmp_path, worker_id)
    argv = (tmp_path / 'cwd' / 'argv.txt').read_text().splitlines()
    config = tmp_path / 'w' / 'mcp.json'
    assert argv[:5] == ['--no-extensions', '-e', 'npm:pi-mcp-adapter@4.0.0', '--mcp-config', str(config)]
    assert json.loads(config.read_text()) == {
        'mcpServers': {'probe': {'command': 'cat', 'args': [], 'env': {'PROBE_KEY': '${PROBE_KEY}'}}}
    }
    [spec] = specs
    assert spec.env['PROBE_KEY'] == 'probe-value', "in the spec's own env: a Herdr pane does not inherit the daemon's"
    assert 'probe-value' not in ' '.join(spec.args) and 'probe-value' not in config.read_text()
    assert 'PI_MCP_CONFIG_MODE=exclusive' in (tmp_path / 'cwd' / 'env.txt').read_text().splitlines()
    assert worker(db, worker_id).error is None


def test_a_variable_a_roles_server_names_but_the_daemon_lacks_fails_the_launch_naming_it(
    project: Project, db: Database, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    use_harness(db, project, RECORDS_ARGV_AND_ENV, mcp='pi', headless=['--no-extensions', MCP, '{prompt}'])
    give_watchdog_servers(db, project, {'probe': {'url': 'https://x', 'bearer_token_env': 'PROBE_TOKEN'}})
    monkeypatch.delenv('PROBE_TOKEN', raising=False)
    worker_id = request(db, project)
    launch(db, runtime, tmp_path, worker_id)
    w = worker(db, worker_id)
    assert w.state is WorkerState.KILLED and w.error is not None
    assert w.error.startswith(
        "McpError: MCP servers ['probe'] need PROBE_TOKEN in the environment piloop serve runs in"
    )
    assert not (tmp_path / 'cwd' / 'argv.txt').exists(), 'never started'


def test_servers_that_cannot_be_rendered_fail_the_launch_on_the_row(
    project: Project, db: Database, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    use_harness(db, project, RECORDS_ARGV_AND_ENV, mcp='codex', headless=[MCP, '{prompt}'])
    give_watchdog_servers(db, project, {'probe': {'command': ['cat']}})
    worker_id = request(db, project)
    launch(db, runtime, tmp_path, worker_id)
    w = worker(db, worker_id)
    assert w.state is WorkerState.KILLED and w.error is not None and w.error.startswith('McpError: ')
    assert 'printed no server list' in w.error, 'the stand-in is no codex: `mcp list --json` printed nothing'
