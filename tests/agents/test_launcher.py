import json
import os
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

from piloop.actor import Agent, System
from piloop.agents import capture
from piloop.agents.capture import ENTIRE_INSTALL, MIN_ENTIRE
from piloop.agents.launcher import PROMPT, Launcher, fill
from piloop.agents.results import (
    NEVER_WORKED,
    AgentSessionFinished,
    AgentSessionStarted,
    DaemonStopped,
    Ended,
    JobEnded,
    NotStarted,
    Ran,
    StartupAnomaly,
    WrapUp,
)
from piloop.agents.roles import PILOOP_HINT, PROMPTS, SQL_HINT, Role, compose_brief, role
from piloop.config import CampaignConfig, config_of, load_campaign_config
from piloop.daemon.run_controller import RunController
from piloop.db.events import emit, in_order
from piloop.db.models import AgentSession, AgentSessionState, Campaign, Event, LeaseKind, Usage
from piloop.db.session import Database
from piloop.harnesses import DEFAULT_HARNESSES
from piloop.ids import IMPLEMENTER, RESEARCH, WATCHDOG, AgentSessionId, RoleName, TargetKind, TargetRef
from piloop.mcp import MCP
from piloop.otel import Tracer
from piloop.plugins.executor.contract import Checkout, GitTree
from piloop.plugins.executor.kit.leases import LeaseQueued
from piloop.plugins.executor.workers import Workers
from piloop.plugins.runtime.contract import AgentExit, AgentSpec, ExitStatus, StartFailed
from piloop.settings import Settings
from piloop.usage.jsonl import CWD_ENCODERS
from tests.conftest import (
    HARNESS_FAILS,
    HARNESS_PROMPT_TO_FILE,
    HARNESS_SLEEPS,
    ScriptRuntime,
    events,
    factory,
    serving,
    workers_of,
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


def role_in(campaign: Campaign, kind: RoleName = WATCHDOG) -> Role:
    return role(config_of(campaign), kind, Path(campaign.path))


def request(db: Database, campaign: Campaign, kind: RoleName = WATCHDOG, **fields: object) -> AgentSessionId:
    with db.session() as s:
        agent_session = AgentSession(kind=kind, campaign_id=campaign.id, reason='test', **fields)
        s.add(agent_session)
        s.commit()
        return agent_session.id


def controller(db: Database, runtime: ScriptRuntime, tmp_path: Path, tracer: Tracer | None = None) -> RunController:
    """The launcher and the in-process worker that takes its leases, as the daemon makes them."""
    return serving(RunController(db, tracer or Tracer(None), tmp_path, factory(runtime), executor=Workers()))


def launcher(db: Database, runtime: ScriptRuntime, tmp_path: Path, tracer: Tracer | None = None) -> Launcher:
    return controller(db, runtime, tmp_path, tracer).launcher


def launch(
    db: Database, runtime: ScriptRuntime, tmp_path: Path, agent_session_id: AgentSessionId, tracer: Tracer | None = None
):
    with db.session() as s:
        campaign = s.get_one(Campaign, s.get_one(AgentSession, agent_session_id).campaign_id)
        brief = compose_brief(
            role_in(campaign), campaign, Agent(role='watchdog', session=agent_session_id), reason='test', context=''
        )
    cwd = tmp_path / 'cwd'
    cwd.mkdir(exist_ok=True)
    ctl = controller(db, runtime, tmp_path, tracer)
    ended = ctl.launcher.launch(
        agent_session_id, brief=brief, cwd=cwd, log_dir=tmp_path / 'w', artifact=Checkout(path=cwd)
    )
    # its worker heartbeats until it stops: left running, it takes the next launch's job, with this host's entire
    ctl.stop(5)
    return ended


def exit_of(ended: Ended) -> AgentExit:
    """How the agent's process ended: the agent ran."""
    assert isinstance(ended, Ran), ended
    return ended.process


def agent_session(db: Database, agent_session_id: AgentSessionId) -> AgentSession:
    with db.session() as s:
        return s.get_one(AgentSession, agent_session_id)


def use_harness(db: Database, campaign: Campaign, body: str, **definition: object) -> None:
    """Point the test harness at ``body``; ``definition`` overrides keys of its ``[harness.script]`` table."""
    write_toml(Path(campaign.path), body)
    script = {**load_campaign_config(Path(campaign.path)).harness['script'].model_dump(mode='json'), **definition}
    with db.session() as s:
        s.get_one(Campaign, campaign.id).config = load_campaign_config(
            Path(campaign.path), harness={'script': script}
        ).model_dump(mode='json')
        s.commit()


def test_brief_is_role_prompt_campaign_brief_procedures_then_context(campaign: Campaign, db: Database) -> None:
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        brief = compose_brief(role_in(p), p, Agent(role='watchdog', session=7), reason='timer', context='- extra line')
    assert brief.startswith((PROMPTS / 'watchdog.md').read_text()[:40])
    assert (
        brief.index('# Watchdog')
        < brief.index('# Demo')
        < brief.index('# Working with the piloop')
        < brief.index('# Context')
    )
    assert brief.endswith(
        f"""# Context
- You are watchdog#7, the watchdog for campaign#1 (demo).
- Campaign repo: {campaign.path} (every candidate commit and Entire checkpoint is in it).
- Sign what you write as 'watchdog#7' (comments.author, ideas.created_by).
- Why this agent started (context, not a scope): timer
- piloop: `{PILOOP_HINT}` is this daemon's piloop. Run every `piloop` command of this brief with it.
- SQL tool: `{SQL_HINT} -`, with the statement on stdin from a quoted heredoc (`<<'EOF'`).
- Sweeping roles (`agent_sessions.kind` of a request): watchdog, research, scout.
- File a seed: `{PILOOP_HINT} idea seed demo '<hunch, one line>' --from '<pointers: ids, paths, numbers>'` (`--base\
 <candidate>` when it is about that tree): research checks it against the existing work and develops it into an idea,\
 or rejects it with the reason.
- You may write: comments(escalation, finding, note), lessons on the campaign once it stopped, seeds via idea seed.
- extra line
"""
    )


def test_a_roles_addendum_follows_the_campaign_brief_in_that_roles_briefs_only(
    campaign: Campaign, db: Database
) -> None:
    (Path(campaign.path) / 'research.md').write_text('# Search wide first\n')
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        p.config = load_campaign_config(
            Path(campaign.path), roles={'research': {'addendum': 'research.md'}}
        ).model_dump(mode='json')
        research = compose_brief(role_in(p, RESEARCH), p, Agent(role='research', session=7), reason='timer', context='')
        watchdog = compose_brief(role_in(p), p, Agent(role='watchdog', session=8), reason='timer', context='')
    assert (
        research.index('# Demo') < research.index('# Search wide first') < research.index('# Working with the piloop')
    )
    assert '# Search wide first' not in watchdog


def test_launch_runs_the_row_through_its_lifecycle(
    campaign: Campaign,
    db: Database,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
) -> None:
    use_harness(db, campaign, HARNESS_PROMPT_TO_FILE)
    agent_session_id = request(db, campaign)
    outcome = launch(db, runtime, tmp_path, agent_session_id, tracer)
    process = exit_of(outcome)
    assert (process.status, process.exit_code, process.error) == (ExitStatus.EXITED, 0, None)
    w = agent_session(db, agent_session_id)
    assert (w.state, w.harness, w.model, w.runtime, w.log_dir, w.error) == (
        AgentSessionState.EXITED,
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
    assert runtime.killed, 'the runtime is told to release the implementer'
    with db.session() as s:
        events = [(e.target_id, e.payload) for e in s.scalars(select(Event).order_by(Event.id))]
    assert [e for e in events if not isinstance(e[1], JobEnded)] == [
        (1, AgentSessionStarted(role=WATCHDOG, harness='script', model='watchdog-model', reason='test')),
        (1, LeaseQueued(lease=1, lease_kind=LeaseKind.AGENT)),
        (1, AgentSessionFinished(role=WATCHDOG, reason='test', status=ExitStatus.EXITED, error=None)),
    ]
    [ended] = [payload.ended for _, payload in events if isinstance(payload, JobEnded)]
    assert isinstance(ended, Ran) and ended.head is None, 'how it ended, for what waits for it'
    [span] = [span for span in spans.get_finished_spans() if span.name == 'piloop.agent']
    assert dict(span.attributes or {}) == {
        'agent_session': 1,
        'kind': 'watchdog',
        'harness': 'script',
        'model': 'watchdog-model',
        'reason': 'test',
        'state': 'exited',
    }
    [job] = [span for span in spans.get_finished_spans() if span.name == 'piloop.job']
    assert dict(job.attributes or {}) == {'lease': 1, 'agent_session': 1, 'status': 'exited'}, "the worker's span"


def test_rotation_over_the_role_harnesses_unless_pinned(
    campaign: Campaign, db: Database, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    write_toml(Path(campaign.path), HARNESS_PROMPT_TO_FILE)
    cfg = load_campaign_config(
        Path(campaign.path),
        harnesses=['script', 'pi'],
        model={'script': 'm', 'pi': 'p'},
        roles={'watchdog': {'harnesses': ['script', 'pi'], 'model': {'pi': 'wp'}}},
    )
    with db.session() as s:
        s.get_one(Campaign, campaign.id).config = cfg.model_dump(mode='json')
        s.commit()
    ids = [request(db, campaign), request(db, campaign), request(db, campaign, harness='script', model='pinned')]
    request(db, campaign, kind=RESEARCH)  # another kind: not in this rotation
    picks = []
    for agent_session_id in ids:
        launch(db, runtime, tmp_path, agent_session_id)
        w = agent_session(db, agent_session_id)
        picks.append((w.harness, w.model))
    assert picks == snapshot([('script', 'watchdog-model'), ('pi', 'wp'), ('script', 'pinned')])


def test_an_agent_writes_through_piloop_sql_as_its_own_actor(
    campaign: Campaign, db: Database, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """The brief's SQL command in a launched process records ``<kind>#<agent-session>``: PILOOP_ACTOR comes with PILOOP_HOME."""
    use_harness(
        db, campaign, f'#!/bin/sh\n{SQL_HINT} "UPDATE campaigns SET name = \'renamed\' WHERE id = {campaign.id}"\n'
    )
    agent_session_id = request(db, campaign, kind=RESEARCH)
    outcome = launch(db, runtime, tmp_path, agent_session_id)
    assert exit_of(outcome).error is None, (tmp_path / 'w' / 'agent_session.stderr').read_text()
    with db.session() as s:
        row = s.get_one(Campaign, campaign.id)
        assert (row.name, row.updated_by) == ('renamed', f'research#{agent_session_id}')


@pytest.mark.parametrize('cli', ['piloop', '"$PILOOP_BIN"', PILOOP_HINT])
def test_an_agents_piloop_is_the_daemons_whatever_comes_first_on_path_or_sits_in_its_cwd(
    cli: str, campaign: Campaign, db: Database, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The prompts say `piloop`, the brief `"$PILOOP_BIN"`: neither another install nor the campaign's code answers.

    A second install (a release next to a build of main) comes first on the PATH. The agent's cwd is the campaign's
    worktree, and a campaign's own ``json.py`` or ``piloop/`` must not shadow piloop's imports (``python -P``).
    """
    other = tmp_path / 'other-bin'
    other.mkdir()
    (other / 'piloop').write_text('#!/bin/sh\necho "another piloop" >&2\nexit 9\n')
    (other / 'piloop').chmod(0o755)
    monkeypatch.setenv('PATH', f'{other}:{os.environ["PATH"]}')
    cwd = tmp_path / 'cwd'
    (cwd / 'piloop').mkdir(parents=True)
    for module in (cwd / 'json.py', cwd / 'piloop' / '__init__.py'):
        module.write_text("raise SystemExit('SHADOWED')\n")
    use_harness(
        db, campaign, f'#!/bin/sh\n{cli} sql "UPDATE campaigns SET name = \'renamed\' WHERE id = {campaign.id}"\n'
    )

    agent_session_id = request(db, campaign, kind=RESEARCH)
    outcome = launch(db, runtime, tmp_path, agent_session_id)
    assert exit_of(outcome).error is None, (tmp_path / 'w' / 'agent_session.stderr').read_text()
    with db.session() as s:
        assert s.get_one(Campaign, campaign.id).name == 'renamed'


def test_harness_exit_code_is_recorded_not_raised(
    campaign: Campaign, db: Database, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    use_harness(db, campaign, HARNESS_FAILS)
    agent_session_id = request(db, campaign)
    outcome = launch(db, runtime, tmp_path, agent_session_id)
    w = agent_session(db, agent_session_id)
    assert (exit_of(outcome).error, exit_of(outcome).exit_code, w.state, w.error) == (
        'agent exit 3',
        3,
        AgentSessionState.EXITED,
        'agent exit 3',
    )


def test_timeout_kills_and_records(campaign: Campaign, db: Database, runtime: ScriptRuntime, tmp_path: Path) -> None:
    write_toml(Path(campaign.path), HARNESS_SLEEPS)
    cfg = load_campaign_config(Path(campaign.path), roles={'watchdog': {'timeout': 0.3}})
    with db.session() as s:
        s.get_one(Campaign, campaign.id).config = cfg.model_dump(mode='json')
        s.commit()
    agent_session_id = request(db, campaign)
    outcome = launch(db, runtime, tmp_path, agent_session_id)
    w = agent_session(db, agent_session_id)
    assert (exit_of(outcome).status, w.state, w.error) == (
        ExitStatus.TIMED_OUT,
        AgentSessionState.KILLED,
        'agent timeout after 0.3s',
    )


def test_runtime_failure_is_isolated(
    campaign: Campaign,
    db: Database,
    runtime: ScriptRuntime,
    tmp_path: Path,
    mocker: MockerFixture,
    tracer: Tracer,
    spans: InMemorySpanExporter,
) -> None:
    mocker.patch.object(runtime, 'start', side_effect=RuntimeError('no server'))
    agent_session_id = request(db, campaign)
    outcome = launch(db, runtime, tmp_path, agent_session_id, tracer)
    w = agent_session(db, agent_session_id)
    assert (outcome, w.state, w.handle, w.error) == (
        NotStarted('RuntimeError: no server'),
        AgentSessionState.KILLED,
        {},
        'RuntimeError: no server',
    )
    [job] = [span for span in spans.get_finished_spans() if span.name == 'piloop.job']
    assert job.status.status_code is StatusCode.ERROR, "the worker's span has the error"
    assert span_attributes(spans, 'piloop.agent', agent_session_id)['state'] == 'killed'
    with db.session() as s:
        [(_, finished)] = in_order(s, AgentSessionFinished)
    assert (finished.role, finished.error) == (WATCHDOG, 'RuntimeError: no server')


def test_a_failed_start_leaves_its_screen_as_the_transcript_and_its_last_lines_in_the_error(
    campaign: Campaign, db: Database, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """A failed start keeps the screen the agent was stuck on, so the row's error shows the cause."""
    screen = 'Welcome to Claude Code\n\n' + ''.join(f'line {i}\n' for i in range(20)) + '> do the thing\n\n'
    error = 'HerdrError: prompt_lost: watchdog-1 did not start working after 2 prompt submissions'
    mocker.patch.object(runtime, 'start', side_effect=StartFailed(error, screen))
    agent_session_id = request(db, campaign)
    outcome = launch(db, runtime, tmp_path, agent_session_id)
    assert (tmp_path / 'w' / 'transcript.txt').read_text() == screen
    tail = '\n'.join([*(f'line {i}' for i in range(9, 20)), '> do the thing'])
    reason = f'{error}\nIts screen ended with:\n{tail}'
    assert (outcome, agent_session(db, agent_session_id).error) == (NotStarted(reason), reason)

    mocker.patch.object(runtime, 'start', side_effect=StartFailed(error, '\n'))
    assert launch(db, runtime, tmp_path, request(db, campaign)) == NotStarted(error), 'a blank screen adds nothing'


def anomalies(db: Database) -> list[tuple[int, StartupAnomaly]]:
    with db.session() as s:
        return [(e.target_id, anomaly) for e, anomaly in in_order(s, StartupAnomaly)]


def test_an_agent_that_never_got_going_is_an_anomaly(
    campaign: Campaign,
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
    dialog = request(db, campaign)
    outcome = launch(db, runtime, tmp_path, dialog)
    assert (isinstance(outcome, NotStarted), agent_session(db, dialog).state) == (True, AgentSessionState.KILLED)
    [(on, anomaly)] = anomalies(db)
    assert (on, anomaly.model_dump(exclude={'kind'})) == (
        dialog,
        {
            'role': 'watchdog',
            'harness': 'script',
            'run': None,
            'seconds': IsFloat(ge=0),
            'error': 'RuntimeError: agent_not_ready: Trust this folder?',
        },
    )
    mocker.stopall()

    monkeypatch.setenv('HOME', str(tmp_path / 'home'))
    counted = {'headless': ['--session-id', '{session_id}', '{prompt}'], 'usage': PI_SPEC}
    use_harness(db, campaign, HARNESS_PROMPT_TO_FILE, **counted)
    idle_at_once = request(db, campaign)
    outcome = launch(db, runtime, tmp_path, idle_at_once)
    assert (outcome, agent_session(db, idle_at_once).state) == (NotStarted(NEVER_WORKED), AgentSessionState.EXITED)
    assert [w for w, _ in anomalies(db)] == [dialog, idle_at_once], 'ended within a minute without a token'

    monkeypatch.setattr('piloop.agents.results.STARTUP_SECONDS', 0)
    assert not isinstance(launch(db, runtime, tmp_path, request(db, campaign)), NotStarted), 'past the startup window'
    monkeypatch.undo()
    use_harness(db, campaign, HARNESS_PROMPT_TO_FILE)
    assert not isinstance(launch(db, runtime, tmp_path, request(db, campaign)), NotStarted), (
        'a harness whose usage is not read cannot tell "spent nothing" from "unknown"'
    )
    assert len(anomalies(db)) == 2


def test_an_agent_told_to_wrap_up_that_stops_with_nothing_is_not_a_failed_start(
    campaign: Campaign, db: Database, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A cancel or a stop with the wrap-up prompt: the agent stopped because it was told to, not at startup."""
    monkeypatch.setenv('HOME', str(tmp_path / 'home'))
    counted = {'headless': ['--session-id', '{session_id}', '{prompt}'], 'usage': PI_SPEC}
    use_harness(db, campaign, HARNESS_PROMPT_TO_FILE, **counted)
    told = request(db, campaign)
    with db.session() as s:
        emit(s, WrapUp(reason='cancelled'), TargetRef(TargetKind.AGENT_SESSION, told), System.DAEMON)
        s.commit()
    outcome = launch(db, runtime, tmp_path, told)
    assert (isinstance(outcome, NotStarted), agent_session(db, told).state, anomalies(db)) == (
        False,
        AgentSessionState.EXITED,
        [],
    )


def test_a_cwd_that_cannot_be_made_is_a_failed_start(
    campaign: Campaign, db: Database, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """The worker makes the artifact (a worktree) as part of the start: when it fails, the agent did not start to work."""
    start = mocker.spy(runtime, 'start')
    agent_session_id = request(db, campaign)
    brief = compose_brief(
        role_in(campaign), campaign, Agent(role='watchdog', session=agent_session_id), reason='test', context=''
    )
    mocker.patch(
        'piloop.plugins.executor.kit.jobs.git.add_worktree', side_effect=OSError(28, 'No space left on device')
    )
    outcome = launcher(db, runtime, tmp_path).launch(
        agent_session_id,
        brief=brief,
        cwd=tmp_path / 'wt',
        log_dir=tmp_path / 'w',
        artifact=GitTree(repo=Path(campaign.path), commit='HEAD'),
    )
    w = agent_session(db, agent_session_id)
    assert (isinstance(outcome, NotStarted), w.state, w.error) == (
        True,
        AgentSessionState.KILLED,
        'OSError: [Errno 28] No space left on device',
    )
    assert [target for target, _ in anomalies(db)] == [agent_session_id] and not start.called


def test_nothing_starts_after_cancel(campaign: Campaign, db: Database, runtime: ScriptRuntime, tmp_path: Path) -> None:
    agent_session_id = request(db, campaign)
    lc = launcher(db, runtime, tmp_path)
    lc.cancel()
    outcome = lc.launch(
        agent_session_id, brief='x', cwd=tmp_path, log_dir=tmp_path / 'w', artifact=Checkout(path=tmp_path)
    )
    w = agent_session(db, agent_session_id)
    assert (outcome, w.state, w.harness) == (DaemonStopped(), AgentSessionState.REQUESTED, None), 'stop() closes rows'
    assert not (tmp_path / 'w').exists()


def test_a_harness_with_an_entire_agent_gets_a_minted_session_id_in_its_argv_and_on_its_row(
    campaign: Campaign,
    db: Database,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
) -> None:
    use_harness(db, campaign, HARNESS_PROMPT_TO_FILE)
    plain = request(db, campaign)
    launch(db, runtime, tmp_path, plain)
    assert agent_session(db, plain).session_id is None, 'no entire_agent: nothing to attach, no id minted'
    use_harness(db, campaign, '#!/bin/sh\nprintf %s "$2" > session.txt\n', **MINTED_HARNESS)
    agent_session_id = request(db, campaign)
    launch(db, runtime, tmp_path, agent_session_id, tracer)
    w = agent_session(db, agent_session_id)
    assert w.session_id is not None and UUID.match(w.session_id), 'a uuid4, as pi and claude --session-id take'
    assert (tmp_path / 'cwd' / 'session.txt').read_text() == w.session_id
    assert dict(span_attributes(spans, 'piloop.agent', agent_session_id))['session_id'] == w.session_id


def test_a_harness_that_prints_its_session_id_has_it_read_back_by_the_runtime(
    campaign: Campaign,
    db: Database,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
) -> None:
    codex = DEFAULT_HARNESSES['codex']
    use_harness(db, campaign, CODEX_LIKE, entire_agent='codex', session_id_pattern=codex.session_id_pattern)
    agent_session_id = request(db, campaign)
    launch(db, runtime, tmp_path, agent_session_id, tracer)
    assert agent_session(db, agent_session_id).session_id == CODEX_ID
    assert span_attributes(spans, 'piloop.job', agent_session_id)['session_id'] == CODEX_ID
    use_harness(db, campaign, HARNESS_PROMPT_TO_FILE, entire_agent='codex', session_id_pattern=codex.session_id_pattern)
    silent = request(db, campaign)
    launch(db, runtime, tmp_path, silent, tracer)
    assert agent_session(db, silent).session_id is None
    assert events(spans, 'agent_session.session_unknown') == [{'agent_session': silent, 'runtime': 'subprocess'}], (
        'nothing to attach, now or later'
    )


def test_a_session_reported_late_is_asked_for_again_before_the_agent_is_released(
    campaign: Campaign,
    db: Database,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
    mocker: MockerFixture,
) -> None:
    """A harness slow to start reports no session before the wait. It is asked again before its pane closes.

    A failure of that last ask is recorded, and the agent is still released.
    """
    codex = DEFAULT_HARNESSES['codex']
    use_harness(db, campaign, CODEX_LIKE, entire_agent='codex', session_id_pattern=codex.session_id_pattern)
    mocker.patch.object(runtime, 'session_id', side_effect=[None, CODEX_ID])
    late = request(db, campaign)
    launch(db, runtime, tmp_path, late, tracer)
    assert agent_session(db, late).session_id == CODEX_ID
    assert events(spans, 'agent_session.session_unknown') == [], 'found on the second ask'

    mocker.patch.object(runtime, 'session_id', side_effect=[None, RuntimeError('server_unavailable')])
    lost = request(db, campaign)
    launch(db, runtime, tmp_path, lost, tracer)
    assert agent_session(db, lost).session_id is None and len(runtime.killed) == 2
    assert events(spans, 'agent_session.session_unknown') == [
        {'agent_session': lost, 'runtime': 'subprocess', 'error': 'server_unavailable'}
    ]


def test_the_session_is_attached_to_entire_from_the_cwd_when_the_agent_ends(
    campaign: Campaign,
    db: Database,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
    entire_log,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    use_harness(db, campaign, HARNESS_PROMPT_TO_FILE)
    plain = request(db, campaign)
    launch(db, runtime, tmp_path, plain, tracer)
    assert entire_log() == [] and agent_session(db, plain).checkpoint_id is None, (
        'no entire_agent: entire is never called'
    )
    use_harness(db, campaign, HARNESS_PROMPT_TO_FILE, **MINTED_HARNESS)
    agent_session_id = request(db, campaign)
    launch(db, runtime, tmp_path, agent_session_id, tracer)
    w = agent_session(db, agent_session_id)
    assert w.checkpoint_id == CHECKPOINT
    assert entire_log() == [
        {'argv': ['session', 'attach', w.session_id, '--agent', 'pi'], 'cwd': str(tmp_path / 'cwd')},
        {'argv': ['session', 'info', w.session_id, '--json'], 'cwd': str(tmp_path / 'cwd')},
    ], "from the agent's own cwd: pi and claude key their session stores by it; the id comes from info's JSON"
    assert events(spans, 'agent_session.session_attached') == [
        {
            'agent_session': agent_session_id,
            'session_id': w.session_id,
            'checkpoint': CHECKPOINT,
            'agent': 'Pi',
            'tokens': TOKENS,
        }
    ]
    assert span_attributes(spans, 'piloop.job', agent_session_id)['checkpoint'] == CHECKPOINT
    with db.session() as s:
        [(_, finished)] = in_order(s, AgentSessionFinished, Event.target_id == agent_session_id)
        assert (finished.role, finished.status) == (WATCHDOG, ExitStatus.EXITED)
    for mode in ('added', 'existing'):
        monkeypatch.setenv('FAKE_ENTIRE_MODE', mode)
        other = request(db, campaign)
        launch(db, runtime, tmp_path, other, tracer)
        assert agent_session(db, other).checkpoint_id == CHECKPOINT, (
            'HEAD carrying a trailer adds the session to that checkpoint; a session entire already knows is not an error'
        )
    monkeypatch.setenv('FAKE_ENTIRE_MODE', 'fail')
    failed = request(db, campaign)
    launch(db, runtime, tmp_path, failed, tracer)
    w = agent_session(db, failed)
    assert (w.state, w.error, w.checkpoint_id) == (AgentSessionState.EXITED, None, None), 'the row is fine'
    assert events(spans, 'agent_session.session_unattached') == [
        {
            'agent_session': failed,
            'session_id': w.session_id,
            'error': f'entire session attach exit 1: Error: invalid session ID: {w.session_id}',
        }
    ]
    assert entire_log()[-1]['argv'][:2] == ['session', 'attach'], 'no info call after a failed attach'


def test_the_daemons_entire_version_gates_attach_and_is_on_the_event(
    campaign: Campaign,
    db: Database,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
    entire_log,
) -> None:
    use_harness(db, campaign, HARNESS_PROMPT_TO_FILE, **MINTED_HARNESS)
    run = controller(db, runtime, tmp_path, tracer)
    gated = run.launcher
    workers_of(run).worker.entire_version = '0.10.5'
    (tmp_path / 'cwd').mkdir()
    old = request(db, campaign)
    gated.launch(
        old, brief='b', cwd=tmp_path / 'cwd', log_dir=tmp_path / 'logs', artifact=Checkout(path=tmp_path / 'cwd')
    )
    assert entire_log() == [] and agent_session(db, old).checkpoint_id is None, 'an untested entire is not asked'
    w = agent_session(db, old)
    assert events(spans, 'agent_session.session_unattached') == [
        {
            'agent_session': old,
            'session_id': w.session_id,
            'error': f'entire 0.10.5 is older than {MIN_ENTIRE}; upgrade it: {ENTIRE_INSTALL}',
        }
    ], 'nothing is attached until entire is upgraded'
    workers_of(run).worker.entire_version = '0.11.0'
    new = request(db, campaign)
    gated.launch(
        new, brief='b', cwd=tmp_path / 'cwd', log_dir=tmp_path / 'logs', artifact=Checkout(path=tmp_path / 'cwd')
    )
    assert agent_session(db, new).checkpoint_id == CHECKPOINT
    assert events(spans, 'agent_session.session_attached')[0]['entire'] == '0.11.0'


def test_attach_failures_are_events_and_never_touch_the_row(
    campaign: Campaign,
    db: Database,
    runtime: ScriptRuntime,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
    entire_log,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    use_harness(db, campaign, HARNESS_PROMPT_TO_FILE, **MINTED_HARNESS)
    monkeypatch.setattr(capture, 'settings', Settings(home=tmp_path, entire_attach_timeout_seconds=0.3))
    monkeypatch.setenv('FAKE_ENTIRE_MODE', 'hang')
    slow = request(db, campaign)
    launch(db, runtime, tmp_path, slow, tracer)
    monkeypatch.setenv('FAKE_ENTIRE_MODE', 'silent')
    silent = request(db, campaign)
    launch(db, runtime, tmp_path, silent, tracer)
    monkeypatch.setenv('PATH', str(tmp_path))
    missing = request(db, campaign)
    launch(db, runtime, tmp_path, missing, tracer)
    assert [(e['agent_session'], e['error']) for e in events(spans, 'agent_session.session_unattached')] == [
        (slow, 'entire session attach timed out after 0.3s'),
        (silent, f'entire session info reports no checkpoint for session {agent_session(db, silent).session_id}'),
        (missing, 'entire is not on PATH; install it: curl -fsSL https://entire.io/install.sh | bash'),
    ]
    for agent_session_id in (slow, silent, missing):
        w = agent_session(db, agent_session_id)
        assert (w.state, w.checkpoint_id) == (AgentSessionState.EXITED, None) and w.session_id is not None, (
            'the row is fine'
        )


def test_the_agents_token_usage_is_summed_from_its_session_file_when_it_ends(
    campaign: Campaign,
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
    use_harness(db, campaign, writes_session, **minted)
    agent_session_id = request(db, campaign)
    launch(db, runtime, tmp_path, agent_session_id, tracer)
    w = agent_session(db, agent_session_id)
    assert w.session_id is not None, 'a usage spec alone mints the id: no Entire needed to know what an agent cost'
    assert w.usage == Usage(
        input=4,
        output=993,
        cache_read=6763,
        cache_write=8718,
        total=16478,
        turns=2,
        source=str(folder / f'2026-09-09T05-48-14-300Z_{w.session_id}.jsonl'),
    ), "pi's store is keyed by the agent's cwd, the worktree; read before the caller removes it"
    assert span_attributes(spans, 'piloop.job', agent_session_id)['tokens'] == 16478
    assert events(spans, 'agent_session.usage_unknown') == []
    assert anomalies(db) == [], 'it spent tokens: it started, however fast it ended'

    use_harness(db, campaign, HARNESS_PROMPT_TO_FILE, **minted)
    silent = request(db, campaign)
    launch(db, runtime, tmp_path, silent, tracer)
    w = agent_session(db, silent)
    assert (w.state, w.error, w.usage) == (AgentSessionState.EXITED, None, None), 'the row is fine'
    assert events(spans, 'agent_session.usage_unknown') == [
        {
            'agent_session': silent,
            'error': f'no session file for {w.session_id}: nothing matches'
            f' {home}/.pi/agent/sessions/*/*_{w.session_id}.jsonl',
        }
    ]
    use_harness(db, campaign, HARNESS_PROMPT_TO_FILE)
    plain = request(db, campaign)
    launch(db, runtime, tmp_path, plain, tracer)
    assert agent_session(db, plain).usage is None and len(events(spans, 'agent_session.usage_unknown')) == 1, (
        'no usage spec: nothing to read and nothing to say, like no entire_agent'
    )
    mocker.patch.object(Launcher, '_prepare', side_effect=RuntimeError('no config'))
    broken = request(db, campaign)
    launch(db, runtime, tmp_path, broken, tracer)
    assert (agent_session(db, broken).state, agent_session(db, broken).usage) == (AgentSessionState.KILLED, None)
    assert len(events(spans, 'agent_session.usage_unknown')) == 1, 'an agent that never ran has no session to read'


def test_a_killed_agents_usage_is_read_too(
    campaign: Campaign, db: Database, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The usage of an agent killed at its timeout is read too: it spent its whole budget."""
    home = tmp_path / 'home'
    monkeypatch.setenv('HOME', str(home))
    folder = home / '.pi' / 'agent' / 'sessions' / CWD_ENCODERS['dash_separators_wrapped'](tmp_path / 'cwd')
    writes_then_hangs = (
        f'#!/bin/sh\nmkdir -p {folder}\ncp {SESSIONS / "pi.jsonl"} "{folder}/2026-09-09T05-48-14-300Z_$2.jsonl"\n'
        'sleep 30 &\necho $! > ../child.pid\nwait\n'
    )
    use_harness(db, campaign, writes_then_hangs, headless=['--session-id', '{session_id}', '{prompt}'], usage=PI_SPEC)
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        config = CampaignConfig.model_validate(p.config).model_dump(mode='json')
        config['roles']['watchdog']['timeout'] = 0.5
        p.config = config
        s.commit()
    agent_session_id = request(db, campaign)
    outcome = launch(db, runtime, tmp_path, agent_session_id)
    w = agent_session(db, agent_session_id)
    assert (exit_of(outcome).status, w.state, w.error) == (
        ExitStatus.TIMED_OUT,
        AgentSessionState.KILLED,
        'agent timeout after 0.5s',
    )
    assert w.usage is not None and (w.usage.total, w.usage.turns) == (16478, 2)


def span_attributes(spans: InMemorySpanExporter, name: str, agent_session_id: AgentSessionId) -> dict[str, object]:
    [span] = [
        s
        for s in spans.get_finished_spans()
        if s.name == name and (s.attributes or {}).get('agent_session') == agent_session_id
    ]
    return dict(span.attributes or {})


def test_fill_keeps_literal_braces() -> None:
    assert fill('{"k": "{model}"}', model='m') == '{"k": "m"}'


def test_the_argv_gets_the_agents_cwd(campaign: Campaign, db: Database, runtime: ScriptRuntime, tmp_path: Path) -> None:
    use_harness(db, campaign, RECORDS_ARGV_AND_ENV, headless=['--cd', '{cwd}', '{prompt}'])
    launch(db, runtime, tmp_path, request(db, campaign))
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
        }, 'PILOOP_PROCESS_TAG and PILOOP_ACTOR reach every command; the MCP renderer excludes the keys'


def test_a_brief_past_the_limit_of_one_argument_reaches_the_agent_as_a_file(
    campaign: Campaign, db: Database, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A brief longer than Linux's limit on one argument (``MAX_ARG_STRLEN``, 128 KiB) reaches the agent as a file.

    The agent's first prompt names the file instead of holding the brief.
    """
    use_harness(db, campaign, RECORDS_ARGV_AND_ENV)
    brief = 'x' * 200 * 1024 + '\n'
    cwd = tmp_path / 'cwd'
    cwd.mkdir()
    outcome = launcher(db, runtime, tmp_path).launch(
        request(db, campaign), brief=brief, cwd=cwd, log_dir=tmp_path / 'w', artifact=Checkout(path=cwd)
    )
    assert (exit_of(outcome).status, exit_of(outcome).error) == (ExitStatus.EXITED, None)
    assert (cwd / 'argv.txt').read_text().splitlines() == [PROMPT.format(path=tmp_path / 'w' / 'brief.md')]
    assert (tmp_path / 'w' / 'brief.md').read_text() == brief


def test_a_herdr_agent_is_prompted_with_where_its_brief_is(
    campaign: Campaign, db: Database, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Herdr submits ``AgentSpec.prompt`` as one argument of ``agent prompt <name> <text>``, under the same limit."""
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        p.config = {**p.config, 'runtime': 'herdr'}
        s.commit()
    specs: list[AgentSpec] = []
    start = runtime.start
    monkeypatch.setattr(runtime, 'start', lambda spec: specs.append(spec) or start(spec))
    launcher(db, runtime, tmp_path).launch(
        request(db, campaign),
        brief='y' * 200 * 1024,
        cwd=tmp_path,
        log_dir=tmp_path / 'w',
        artifact=Checkout(path=tmp_path),
    )
    [spec] = specs
    assert (spec.prompt, spec.args) == (PROMPT.format(path=tmp_path / 'w' / 'brief.md'), [])


def give_watchdog_servers(db: Database, campaign: Campaign, servers: dict[str, object]) -> None:
    """``[mcp.*]`` tables in the stored config, all listed by ``[roles.watchdog] mcp``."""
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        cfg = CampaignConfig.model_validate(p.config)
        watchdog = cfg.roles.watchdog.model_copy(update={'mcp': list(servers)})
        roles = cfg.roles.model_copy(update={'watchdog': watchdog})
        dumped = cfg.model_copy(update={'roles': roles}).model_dump(mode='json') | {'mcp': servers}
        p.config = CampaignConfig.model_validate(dumped).model_dump(mode='json')
        s.commit()


RECORDS_ARGV_AND_ENV = '#!/bin/sh\nprintf "%s\\n" "$@" > argv.txt\nenv > env.txt\n'


def test_the_mcp_element_expands_to_the_roles_servers_and_their_env_reaches_the_agent(
    campaign: Campaign, db: Database, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    use_harness(db, campaign, RECORDS_ARGV_AND_ENV, mcp='pi', headless=['--no-extensions', MCP, '{prompt}'])
    give_watchdog_servers(db, campaign, {'probe': {'command': ['cat'], 'env': ['PROBE_KEY']}})
    monkeypatch.setenv('PROBE_KEY', 'probe-value')
    specs: list[AgentSpec] = []
    start = runtime.start
    monkeypatch.setattr(runtime, 'start', lambda spec: specs.append(spec) or start(spec))
    agent_session_id = request(db, campaign)
    launch(db, runtime, tmp_path, agent_session_id)
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
    assert agent_session(db, agent_session_id).error is None


def test_a_variable_a_roles_server_names_but_the_daemon_lacks_fails_the_launch_naming_it(
    campaign: Campaign, db: Database, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    use_harness(db, campaign, RECORDS_ARGV_AND_ENV, mcp='pi', headless=['--no-extensions', MCP, '{prompt}'])
    give_watchdog_servers(db, campaign, {'probe': {'url': 'https://x', 'bearer_token_env': 'PROBE_TOKEN'}})
    monkeypatch.delenv('PROBE_TOKEN', raising=False)
    agent_session_id = request(db, campaign)
    launch(db, runtime, tmp_path, agent_session_id)
    w = agent_session(db, agent_session_id)
    assert w.state is AgentSessionState.KILLED and w.error is not None
    assert w.error.startswith(
        "McpError: MCP servers ['probe'] need PROBE_TOKEN in the environment piloop serve runs in"
    )
    assert not (tmp_path / 'cwd' / 'argv.txt').exists(), 'never started'


def test_servers_that_cannot_be_rendered_fail_the_launch_on_the_row(
    campaign: Campaign, db: Database, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    use_harness(db, campaign, RECORDS_ARGV_AND_ENV, mcp='codex', headless=[MCP, '{prompt}'])
    give_watchdog_servers(db, campaign, {'probe': {'command': ['cat']}})
    agent_session_id = request(db, campaign)
    launch(db, runtime, tmp_path, agent_session_id)
    w = agent_session(db, agent_session_id)
    assert w.state is AgentSessionState.KILLED and w.error is not None and w.error.startswith('McpError: ')
    assert 'printed no server list' in w.error, 'the stand-in is no codex: `mcp list --json` printed nothing'


def test_a_subprocess_agent_gets_the_arguments_that_resume_its_session(
    campaign: Campaign, db: Database, runtime: ScriptRuntime, tmp_path: Path, tracer: Tracer, mocker: MockerFixture
) -> None:
    """``{prompt}`` stays for the queued messages; ``{session_id}`` stays when the harness reports its own id."""
    use_harness(
        db, campaign, HARNESS_PROMPT_TO_FILE, resume=['resume', '{session_id}', '--model', '{model}', '{prompt}']
    )
    start = mocker.spy(runtime, 'start')
    launch(db, runtime, tmp_path, request(db, campaign), tracer)
    [spec] = [c.args[0] for c in start.call_args_list]
    assert spec.resume_args == ['resume', '{session_id}', '--model', 'watchdog-model', '{prompt}']


def test_an_agent_whose_start_cannot_be_recorded_is_killed_and_waited_for(
    campaign: Campaign, db: Database, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """When the record of the start failed, the agent ran on with nothing that watched it, and nothing waited for its
    process. Python then warned ``subprocess N is still running`` when it collected the ``Popen``. The worker ends
    it, and reports a failed start."""
    use_harness(db, campaign, '#!/bin/sh\nsleep 30\n')
    agent_session_id = request(db, campaign)
    mocker.patch('piloop.agents.results.CampaignResults.started', side_effect=RuntimeError('database closed'))
    outcome = launch(db, runtime, tmp_path, agent_session_id, Tracer(None))
    [proc] = runtime._procs.values()
    assert proc.returncode is not None, 'killed and waited for, not left running unwatched'
    assert outcome == NotStarted('RuntimeError: database closed')


EFFORT_FLAG = {'claude': ('--effort', '{}'), 'codex': ('-c', 'model_reasoning_effort="{}"')}
"""How each built-in harness takes the role's ``effort``."""


def research_argv(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    mocker: MockerFixture,
    harness: str,
    **research: object,
) -> list[str]:
    """The argv that a research agent starts on the built-in ``harness``, with ``research`` over its role's table."""
    write_toml(Path(campaign.path), HARNESS_PROMPT_TO_FILE)
    cfg = load_campaign_config(
        Path(campaign.path),
        harnesses=[harness],
        model={'script': 'm', harness: 'the-campaigns-model'},
        harness={
            harness: {
                'executable': str(Path(campaign.path).parent / 'harness.sh'),
                'headless': [a for a in DEFAULT_HARNESSES[harness].headless if a != MCP],
            }
        },
        roles={'research': research},
    )
    assert cfg.role_models(IMPLEMENTER)[harness] == 'the-campaigns-model'
    with db.session() as s:
        s.get_one(Campaign, campaign.id).config = cfg.model_dump(mode='json')
        s.commit()
    start = mocker.spy(runtime, 'start')
    launch(db, runtime, tmp_path, request(db, campaign, kind=RESEARCH))
    [spec] = [c.args[0] for c in start.call_args_list]
    return spec.args


@pytest.mark.parametrize(('harness', 'model'), [('claude', 'claude-opus-5-5'), ('codex', 'gpt-6.1-sol')])
def test_research_defaults_to_its_models_at_high_effort(
    campaign: Campaign,
    db: Database,
    runtime: ScriptRuntime,
    tmp_path: Path,
    mocker: MockerFixture,
    harness: str,
    model: str,
) -> None:
    args = research_argv(db, campaign, runtime, tmp_path, mocker, harness)
    flag, value = EFFORT_FLAG[harness]
    assert {('--model', model), (flag, value.format('high'))} <= set(zip(args, args[1:], strict=False))


@pytest.mark.parametrize('harness', ['claude', 'codex'])
def test_the_research_table_overrides_its_model_and_effort(
    campaign: Campaign, db: Database, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture, harness: str
) -> None:
    args = research_argv(db, campaign, runtime, tmp_path, mocker, harness, model={harness: 'mine'}, effort='low')
    flag, value = EFFORT_FLAG[harness]
    assert {('--model', 'mine'), (flag, value.format('low'))} <= set(zip(args, args[1:], strict=False))
