import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path

import pytest
from dirty_equals import IsPositive, IsStr
from inline_snapshot import snapshot

from piloop import settings as settings_module
from piloop.config import RuntimeKind
from piloop.harnesses import DEFAULT_HARNESSES
from piloop.runtime import Handle, StartFailed, WorkerSpec, WorkerStatus, herdr
from piloop.runtime.factory import make_runtime
from piloop.runtime.herdr import HerdrError, HerdrRuntime
from piloop.runtime.subprocess import SubprocessRuntime
from piloop.settings import Settings
from tests.fixtures.fake_herdr import AGENT, AGENT_SESSION, SESSION_ID

Response = dict[str, object]
ERR: Response = {'error': {'code': 'agent_blocked', 'message': 'agent w1 is blocked'}}


SESSION = ['--session', 'piloop']


@pytest.fixture(autouse=True)
def no_quiet_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    """A still screen is still at once: the fake's screens do not move unless a test scripts them to."""
    monkeypatch.setattr('piloop.runtime.herdr.QUIET_SECONDS', 0)


def calls(log: Path) -> list[list[str]]:
    """Every argv the fake herdr saw, without the ``--session piloop`` prefix each one must carry."""
    argvs = [json.loads(line) for line in log.read_text().splitlines()]
    assert all(argv[:2] == SESSION for argv in argvs), argvs
    return [argv[2:] for argv in argvs]


def scripted(monkeypatch: pytest.MonkeyPatch, **responses: list[Response]) -> None:
    monkeypatch.setenv('FAKE_HERDR_SCRIPT', json.dumps({k.replace('_', ' '): v for k, v in responses.items()}))


def spec(tmp_path: Path) -> WorkerSpec:
    return WorkerSpec(
        name='w1',
        label='w1 idea#1 the thing',
        executable='pi',
        args=['--model', 'm'],
        prompt='do the thing\nwith two lines',
        cwd=tmp_path / 'wt',
        log_dir=tmp_path / 'logs',
        env={'PILOOP_RUN': '7'},
    )


def home_calls(tab_id: str, pane_id: str, cwd: Path) -> list[list[str]]:
    """What laying out a home tab sends: the rename (a workspace's first tab), the split, ``piloop watch`` in it."""
    watch = f'{shlex.quote(sys.executable)} -m piloop watch {shlex.quote(str(cwd))}'
    split = ['pane', 'split', pane_id, '--direction', 'right', '--ratio', '0.5', '--cwd', str(cwd)]
    env = ['--env', f'PILOOP_HOME={settings_module.settings.home}', '--no-focus']
    return [['tab', 'rename', tab_id, 'home'], [*split, *env], ['pane', 'run', 'w7:p3', watch]]


def test_start_creates_workspace_tab_agent_and_prompts(tmp_path: Path, herdr_log: Path) -> None:
    rt = HerdrRuntime('demo', tmp_path)
    handle = rt.start(spec(tmp_path))
    token = IsStr(regex='[0-9a-f]{32}')
    assert handle == Handle(
        'herdr',
        {'workspace_id': 'w7', 'tab_id': 'w7:t2', 'pane_id': 'w7:p2', 'agent': 'w1', 'token': token, 'kind': 'pi'},
    )
    assert calls(herdr_log) == (
        [
            ['workspace', 'list'],
            ['workspace', 'create', '--cwd', str(tmp_path), '--label', 'demo', '--no-focus'],
            ['workspace', 'report-metadata', 'w7', '--source', 'piloop', '--token', f'root={tmp_path}'],
            *home_calls('w7:t1', 'w7:p1', tmp_path),
            [
                'tab',
                'create',
                '--workspace',
                'w7',
                '--cwd',
                str(tmp_path / 'wt'),
                '--label',
                'w1 idea#1 the thing',
                '--no-focus',
                '--env',
                f'PILOOP_WORKER_TOKEN={handle.data["token"]}',
            ],
            ['pane', 'run', 'w7:p2', f'eval "$(cat {tmp_path}/logs/env-{handle.data["token"]}.fifo)"'],
            ['agent', 'start', 'w1', '--kind', 'pi', '--pane', 'w7:p2', '--timeout', '60000', '--', '--model', 'm'],
            ['agent', 'get', 'w1'],
            ['agent', 'prompt', 'w1', 'do the thing\nwith two lines'],
            ['agent', 'get', 'w1'],
        ]
    )


def pane_env(log: Path) -> dict[str, str]:
    """The environment the fake pane shell had after sourcing what piloop sent it (see ``fake_herdr``)."""
    dump = Path(f'{log}.pane-env')
    deadline = time.monotonic() + 5
    while not dump.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    return dict(line.split('=', 1) for line in dump.read_text().splitlines() if '=' in line)


def test_the_env_reaches_the_pane_shell_and_no_value_reaches_an_argv(tmp_path: Path, herdr_log: Path) -> None:
    """Forwarded MCP keys go through a FIFO that the pane's shell sources.

    No value appears on a herdr command line (``ps``), and nothing is left on disk.
    """
    secret = "s3nt1nel-v4lue 'quoted' $HOME\\n"
    HerdrRuntime('demo', tmp_path).start(replace(spec(tmp_path), env={'PROBE_KEY': secret, 'PILOOP_RUN': '7'}))
    assert not any(secret in arg or 's3nt1nel' in arg for argv in calls(herdr_log) for arg in argv)
    env = pane_env(herdr_log)
    assert (env['PROBE_KEY'], env['PILOOP_RUN']) == (secret, '7')
    assert list((tmp_path / 'logs').iterdir()) == [], 'the FIFO is gone'


def test_a_shell_that_never_reads_its_env_fails_the_start(
    tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr('piloop.runtime.herdr.SHELL_READY_POLL_SECONDS', 0.01)
    monkeypatch.setattr(settings_module, 'settings', Settings(herdr_shell_ready_seconds=0.05))
    scripted(monkeypatch, pane_run=[{'text': ''}])
    with pytest.raises(StartFailed, match='HerdrError: shell_not_ready: the pane shell did not read its environment'):
        HerdrRuntime('demo', tmp_path).start(spec(tmp_path))
    assert calls(herdr_log)[-1] == ['tab', 'close', 'w7:t2']
    assert list((tmp_path / 'logs').iterdir()) == [], 'the FIFO is gone'


def seq(n: int, status: str = 'done') -> Response:
    return {'result': {'type': 'agent_info', 'agent': {'agent_status': status, 'state_change_seq': n}}}


def test_a_brief_is_submitted_once_and_picked_up_by_a_state_change(
    tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pickup is the state sequence advancing after the prompt, not a state.

    A Codex whose first-run dialogs were just answered already reports ``done``, which does not show that it read the
    brief. No pickup and no screen text that shows the brief was taken is a lost brief. The brief is never pasted
    again, because a second paste lands beside the first in the composer.
    """
    monkeypatch.setattr('piloop.runtime.herdr.PICKUP_POLL_SECONDS', 0.01)
    monkeypatch.setattr('piloop.runtime.herdr.settings_module.settings', Settings(herdr_prompt_pickup_seconds=5))
    scripted(monkeypatch, agent_get=[seq(5), seq(5), seq(7, 'working')])
    HerdrRuntime('demo', tmp_path).start(spec(tmp_path))
    assert [c[:2] for c in calls(herdr_log)].count(['agent', 'prompt']) == 1, 'stale done, then picked up'

    herdr_log.write_text('')
    monkeypatch.setattr('piloop.runtime.herdr.settings_module.settings', Settings(herdr_prompt_pickup_seconds=0.05))
    scripted(monkeypatch, agent_get=[seq(5)], pane_read=[{'text': 'Welcome to pi\n> do the thing\n'}])
    with pytest.raises(
        StartFailed, match='^HerdrError: prompt_lost: w1 did not start working after its brief was submitted$'
    ):
        HerdrRuntime('demo', tmp_path).start(spec(tmp_path))
    assert [c[:2] for c in calls(herdr_log)].count(['agent', 'prompt']) == 1
    assert calls(herdr_log)[-1] == ['tab', 'close', 'w7:t2']

    scripted(monkeypatch, agent_get=[{'error': {'code': 'server_unreachable', 'message': 'no socket'}}])
    with pytest.raises(StartFailed, match='server_unreachable'):
        HerdrRuntime('demo', tmp_path).start(spec(tmp_path))


def test_existing_workspace_is_reused_and_cached(
    tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    listed: Response = {'result': {'type': 'workspace_list', 'workspaces': [workspace('w3', 'demo', tmp_path)]}}
    scripted(monkeypatch, workspace_list=[listed])
    rt = HerdrRuntime('demo', tmp_path)
    rt.start(spec(tmp_path))
    rt.start(spec(tmp_path))
    verbs = [c[:2] for c in calls(herdr_log)]
    assert verbs.count(['workspace', 'list']) == 1 and ['workspace', 'create'] not in verbs
    assert calls(herdr_log)[1][2:4] == ['--workspace', 'w3']


def workspace(workspace_id: str, label: str, root: Path) -> dict[str, object]:
    """A listed workspace as herdr reports one piloop created: its ``root`` token is the project path."""
    return {'workspace_id': workspace_id, 'label': label, 'tokens': {'root': str(root)}}


def created_workspace(workspace: Mapping[str, object]) -> Response:
    """``workspace create``'s answer: the workspace with the tab and pane Herdr opens it with."""
    first = {'workspace_id': workspace['workspace_id']}
    return {
        'result': {
            'workspace': workspace,
            'tab': {**first, 'tab_id': f'{workspace["workspace_id"]}:t1', 'label': '1'},
            'root_pane': {**first, 'pane_id': f'{workspace["workspace_id"]}:p1'},
        }
    }


def test_same_named_directories_get_their_own_workspaces(
    tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first, second = tmp_path / 'a' / 'repo', tmp_path / 'b' / 'repo'
    listed: Response = {'result': {'workspaces': [workspace('w1', 'repo', first)]}}
    created = created_workspace({'workspace_id': 'w2', 'label': 'repo (b)'})
    scripted(monkeypatch, workspace_list=[listed], workspace_create=[created])
    assert HerdrRuntime('repo', second)._workspace() == 'w2'
    assert calls(herdr_log)[:3] == snapshot(
        [
            ['workspace', 'list'],
            ['workspace', 'create', '--cwd', str(second), '--label', 'repo (b)', '--no-focus'],
            ['workspace', 'report-metadata', 'w2', '--source', 'piloop', '--token', f'root={second}'],
        ]
    )

    both: Response = {'result': {'workspaces': [workspace('w1', 'repo', first), workspace('w2', 'repo (b)', second)]}}
    scripted(monkeypatch, workspace_list=[both])
    assert HerdrRuntime('repo', second)._workspace() == 'w2'
    assert HerdrRuntime('repo', first)._workspace() == 'w1'
    assert [c[:2] for c in calls(herdr_log)].count(['workspace', 'create']) == 1, 'both found by root, none created'


def test_start_retries_while_the_shell_is_starting(
    tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    busy: Response = {'error': {'code': 'agent_pane_busy', 'message': 'not an available shell'}}
    scripted(
        monkeypatch, agent_start=[busy, busy, {'result': {'type': 'agent_started', 'agent': {'agent_status': 'idle'}}}]
    )
    monkeypatch.setattr('piloop.runtime.herdr.SHELL_READY_POLL_SECONDS', 0.01)
    HerdrRuntime('demo', tmp_path).start(spec(tmp_path))
    assert [c[:2] for c in calls(herdr_log)].count(['agent', 'start']) == 3


NOT_READY: Response = {'error': {'code': 'agent_not_ready', 'message': 'blocked during startup'}}
CLAUDE_DIALOGS = [('trust this folder', ['down', 'enter'])]
CODEX_DIALOGS = [('Do you trust the contents of this directory', ['enter']), ('Press t to trust all', ['t'])]
TRUST_SCREEN: Response = {
    'text': '  Do you trust the contents of this directory? Trusting allows\n› 1. Yes, continue\n'
}
HOOKS_SCREEN: Response = {
    'text': '  ⚠ 1 hook needs review before it can run.\n  Press t to trust all; enter to review hooks; esc to close\n'
}
WELCOME_SCREEN: Response = {'text': '  >_ OpenAI Codex (v0.153.4)\n'}


def dialog_calls(log: Path) -> list[list[str]]:
    return [c for c in calls(log) if c[:2] in (['pane', 'read'], ['agent', 'send-keys'], ['agent', 'wait'])]


def test_start_accepts_a_known_first_launch_dialog_herdr_reports_as_not_ready(
    tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    trust: Response = {'text': 'Quick safety check: do you trust this folder?\n'}
    scripted(monkeypatch, agent_start=[NOT_READY], pane_read=[trust, {'text': 'Welcome to Claude Code\n'}])
    HerdrRuntime('demo', tmp_path).start(replace(spec(tmp_path), executable='claude', startup_dialogs=CLAUDE_DIALOGS))
    assert dialog_calls(herdr_log) == snapshot(
        [
            ['pane', 'read', 'w7:p2', '--source', 'visible'],
            ['agent', 'send-keys', 'w1', 'down'],
            ['agent', 'send-keys', 'w1', 'enter'],
            ['pane', 'read', 'w7:p2', '--source', 'visible'],
            ['agent', 'wait', 'w1', '--until', 'idle', '--until', 'done', '--until', 'blocked', '--timeout', '60000'],
        ]
    )


def test_start_answers_two_dialogs_in_order_when_herdr_reports_them_as_idle(
    tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Herdr reports Codex's trust and hooks screens as ``idle``, so ``agent start`` succeeds with them on screen."""
    scripted(monkeypatch, pane_read=[TRUST_SCREEN, HOOKS_SCREEN, WELCOME_SCREEN])
    HerdrRuntime('demo', tmp_path).start(replace(spec(tmp_path), executable='codex', startup_dialogs=CODEX_DIALOGS))
    assert dialog_calls(herdr_log) == snapshot(
        [
            ['pane', 'read', 'w7:p2', '--source', 'visible'],
            ['agent', 'send-keys', 'w1', 'enter'],
            ['pane', 'read', 'w7:p2', '--source', 'visible'],
            ['agent', 'wait', 'w1', '--until', 'idle', '--until', 'done', '--until', 'blocked', '--timeout', '60000'],
            ['agent', 'send-keys', 'w1', 't'],
            ['pane', 'read', 'w7:p2', '--source', 'visible'],
            ['agent', 'wait', 'w1', '--until', 'idle', '--until', 'done', '--until', 'blocked', '--timeout', '60000'],
        ]
    )


def sent_keys(log: Path) -> list[str]:
    return [c[-1] for c in calls(log) if c[:2] == ['agent', 'send-keys']]


def test_a_stale_redraw_of_an_answered_dialog_does_not_skip_the_next_one(
    tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Herdr reports ``idle`` while the dialog is up, so ``agent wait`` returns before the TUI redraws."""
    scripted(monkeypatch, pane_read=[TRUST_SCREEN, TRUST_SCREEN, HOOKS_SCREEN, WELCOME_SCREEN])
    monkeypatch.setattr('piloop.runtime.herdr.DIALOG_REDRAW_POLL_SECONDS', 0.01)
    HerdrRuntime('demo', tmp_path).start(replace(spec(tmp_path), executable='codex', startup_dialogs=CODEX_DIALOGS))
    assert sent_keys(herdr_log) == ['enter', 't']
    assert [c[:2] for c in calls(herdr_log)].count(['pane', 'read']) == 4


def test_a_dialog_that_never_leaves_the_screen_fails_the_start(
    tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    scripted(monkeypatch, pane_read=[TRUST_SCREEN])
    monkeypatch.setattr('piloop.runtime.herdr.DIALOG_REDRAW_POLL_SECONDS', 0.01)
    monkeypatch.setattr(settings_module, 'settings', Settings(herdr_start_timeout_seconds=0.1))
    with pytest.raises(StartFailed, match='still shows a startup dialog after it was answered$'):
        HerdrRuntime('demo', tmp_path).start(replace(spec(tmp_path), executable='codex', startup_dialogs=CODEX_DIALOGS))
    assert sent_keys(herdr_log) == ['enter'] and calls(herdr_log)[-1] == ['tab', 'close', 'w7:t2']


def test_start_answers_only_the_dialogs_on_screen(
    tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    scripted(monkeypatch, agent_start=[NOT_READY], pane_read=[HOOKS_SCREEN, WELCOME_SCREEN])
    HerdrRuntime('demo', tmp_path).start(replace(spec(tmp_path), executable='codex', startup_dialogs=CODEX_DIALOGS))
    assert sent_keys(herdr_log) == ['t'], 'the trusted directory'

    before = len(calls(herdr_log))
    scripted(monkeypatch, pane_read=[WELCOME_SCREEN])
    HerdrRuntime('demo', tmp_path).start(replace(spec(tmp_path), executable='codex', startup_dialogs=CODEX_DIALOGS))
    assert [c[:2] for c in calls(herdr_log)[before:]].count(['agent', 'send-keys']) == 0, 'nothing to answer'


def test_codex_defaults_answer_the_hooks_list(tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The default Codex dialogs answer the hooks review when it shows as a numbered list without a trust dialog."""
    hooks_list: Response = {
        'text': '  Hooks need review\n  1 hook is new or changed.\n› 1. Review hooks\n  2. Trust all and continue\n'
        "  3. Continue without trusting (hooks won't run)\n  Press enter to confirm or esc to go back\n"
    }
    scripted(monkeypatch, agent_start=[NOT_READY], pane_read=[hooks_list, WELCOME_SCREEN])
    dialogs = DEFAULT_HARNESSES['codex'].startup_dialogs
    HerdrRuntime('demo', tmp_path).start(replace(spec(tmp_path), executable='codex', startup_dialogs=dialogs))
    assert sent_keys(herdr_log) == ['down', 'enter']


def test_start_fails_on_an_unexpected_dialog(tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    scripted(monkeypatch, agent_start=[NOT_READY], pane_read=[{'text': 'Something else entirely\n'}])
    with pytest.raises(StartFailed, match='unexpected startup dialog$'):
        HerdrRuntime('demo', tmp_path).start(replace(spec(tmp_path), executable='codex', startup_dialogs=CODEX_DIALOGS))
    assert calls(herdr_log)[-1] == ['tab', 'close', 'w7:t2']


def test_start_fails_on_an_unexpected_dialog_after_a_known_one(
    tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    blocked: Response = {'result': {'type': 'agent_info', 'agent': {'agent_status': 'blocked'}}}
    scripted(monkeypatch, agent_wait=[blocked], pane_read=[TRUST_SCREEN, {'text': 'Something else entirely\n'}])
    with pytest.raises(StartFailed, match='unexpected startup dialog'):
        HerdrRuntime('demo', tmp_path).start(replace(spec(tmp_path), executable='codex', startup_dialogs=CODEX_DIALOGS))
    assert sent_keys(herdr_log) == ['enter'], 'the first was answered'


def test_start_failure_closes_the_tab(tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    scripted(monkeypatch, agent_start=[{'error': {'code': 'agent_not_ready', 'message': 'blocked during startup'}}])
    with pytest.raises(StartFailed, match='agent_not_ready: blocked during startup'):
        HerdrRuntime('demo', tmp_path).start(spec(tmp_path))
    assert calls(herdr_log)[-1] == ['tab', 'close', 'w7:t2']


def test_wait_maps_states(tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    rt = HerdrRuntime('demo', tmp_path)
    handle = Handle('herdr', {'workspace_id': 'w7', 'tab_id': 'w7:t2', 'pane_id': 'w7:p2', 'agent': 'w1'})
    outcome = rt.wait(handle, timeout=2.5)
    assert (outcome.status, outcome.error) == (WorkerStatus.IDLE, None)
    assert outcome.seconds == IsPositive()
    assert calls(herdr_log)[-3:] == snapshot(
        [
            ['agent', 'wait', 'w1', '--until', 'idle', '--until', 'done', '--until', 'blocked', '--timeout', IsStr()],
            ['pane', 'read', 'w7:p2', '--source', 'visible'],
            ['pane', 'read', 'w7:p2', '--source', 'visible'],
        ]
    ), 'idle, and the screen held still'

    scripted(monkeypatch, agent_wait=[{'error': {'code': 'agent_not_running', 'message': 'gone'}}])
    assert rt.wait(handle, timeout=1).status is WorkerStatus.EXITED

    scripted(monkeypatch, agent_wait=[{'error': {'code': 'timeout', 'message': 'timed out'}}])
    outcome = rt.wait(handle, timeout=1)
    assert (outcome.status, outcome.error) == (WorkerStatus.TIMED_OUT, 'worker timeout after 1s')
    assert calls(herdr_log)[-1] == ['tab', 'close', 'w7:t2'], 'timeout kills the worker'

    scripted(monkeypatch, agent_wait=[{'error': {'code': 'server_unreachable', 'message': 'no socket'}}])
    with pytest.raises(HerdrError, match='server_unreachable'):
        rt.wait(handle, timeout=1)


def test_a_blocked_agent_is_waited_for_until_the_grace_runs_out_then_its_dialog_is_kept(
    tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A blocked agent is not finished: Claude Code can ask about a command and deny it by itself a few minutes later.

    Claude Code asks about some commands (``rm -rf $S/$g``) even under ``--dangerously-skip-permissions``.
    """
    rt = HerdrRuntime('demo', tmp_path)
    handle = Handle('herdr', {'workspace_id': 'w7', 'tab_id': 'w7:t2', 'pane_id': 'w7:p2', 'agent': 'w1'})
    scripted(monkeypatch, agent_wait=[seq(6, 'blocked'), seq(7, 'working'), seq(8, 'idle')])
    outcome = rt.wait(handle, timeout=5)
    assert (outcome.status, outcome.error) == (WorkerStatus.IDLE, None)
    untils = [c[3:-2] for c in calls(herdr_log) if c[:2] == ['agent', 'wait']][:3]
    assert untils == [
        ['--until', 'idle', '--until', 'done', '--until', 'blocked'],
        ['--until', 'working', '--until', 'idle', '--until', 'done'],
        ['--until', 'idle', '--until', 'done', '--until', 'blocked'],
    ], 'blocked: wait for it to leave that state, then for the agent to finish'

    monkeypatch.setattr('piloop.runtime.herdr.BLOCKED_GRACE_SECONDS', 0.2)
    herdr_log.write_text('')  # the fake indexes its script by the calls in the log
    timed_out: Response = {'error': {'code': 'timeout', 'message': 'timed out'}}
    scripted(monkeypatch, agent_wait=[seq(6, 'blocked'), timed_out], pane_read=[{'text': 'Do you want to proceed?\n'}])
    outcome = rt.wait(handle, timeout=5)
    assert (outcome.status, outcome.error) == (
        WorkerStatus.BLOCKED,
        'agent is blocked waiting for input:\nDo you want to proceed?',
    ), 'still blocked after the grace: finished, with the question it was stuck on (the pane goes with the worker)'
    assert calls(herdr_log)[-1] == ['pane', 'read', 'w7:p2', '--source', 'visible']


@pytest.mark.usefixtures('herdr_log')
@pytest.mark.parametrize('works_on', [True, False], ids=['async-question', 'dialog'])
def test_a_blocked_agent_whose_screen_moves_is_not_stuck_however_long_herdr_says_blocked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, works_on: bool
) -> None:
    """Blocked counts as stuck only when the screen holds still for the whole grace.

    Codex can queue a question (``request_user_input_async``) and keep working. Herdr reads that screen as blocked. A
    dialog that nobody answers still ends at the grace.
    """
    clock = [0.0]
    monkeypatch.setattr('piloop.runtime.herdr.time', type('Clock', (), {'monotonic': staticmethod(lambda: clock[0])}))
    grace = herdr.BLOCKED_GRACE_SECONDS
    states = ['blocked'] + [None] * int(2 * grace / settings_module.settings.herdr_wait_chunk_seconds) + ['idle']

    def agent_wait(agent: str, until: list[str], timeout: float) -> dict[str, object]:
        state = states.pop(0)
        if state is None:  # nothing changed state within the chunk
            clock[0] += timeout
            raise HerdrError('timeout', 'timed out')
        return {'agent': {'agent_status': state, 'state_change_seq': 1}}

    def screen(pane_id: str) -> str:
        return f'• ran tool call at {clock[0]:g}s\n› Question queued' if works_on else 'Do you want to proceed?\n'

    rt = HerdrRuntime('demo', tmp_path, sleep=lambda _: None)
    monkeypatch.setattr(rt, '_agent_wait', agent_wait)
    monkeypatch.setattr(rt, '_screen', screen)
    outcome = rt.wait(Handle('herdr', {'pane_id': 'w7:p2', 'agent': 'w1'}), timeout=10 * grace)
    if works_on:
        assert (outcome.status, outcome.error) == (WorkerStatus.IDLE, None), 'it worked on: it ends when it is done'
        assert outcome.seconds == 2 * grace, 'it outlived two graces under a question Herdr reads as blocked'
    else:
        assert (outcome.status, outcome.error) == (
            WorkerStatus.BLOCKED,
            'agent is blocked waiting for input:\nDo you want to proceed?',
        )
        assert outcome.seconds == grace, 'a screen that held still for the whole grace is stuck'


def jobs_handle() -> Handle:
    return Handle('herdr', {'workspace_id': 'w7', 'tab_id': 'w7:t2', 'pane_id': 'w7:p2', 'agent': 'w1', 'token': 't'})


def scripted_jobs(monkeypatch: pytest.MonkeyPatch, jobs: list[list[str]], resumed: list[bool]) -> None:
    """What each look at the agent's jobs finds, and whether each wait for a state change after them sees one."""
    looks, changes = iter(jobs), iter(resumed)
    monkeypatch.setattr(HerdrRuntime, 'jobs', lambda self, handle: next(looks))
    monkeypatch.setattr(HerdrRuntime, '_state_changed', lambda self, agent, before, timeout: next(changes))
    monkeypatch.setattr('piloop.runtime.herdr.JOBS_POLL_SECONDS', 0.01)


def test_idle_with_jobs_in_flight_is_not_finished(
    tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Idle is not finished while a job the agent started still runs, such as an evaluate in a background shell.

    Claude Code resumes by itself when its background shell ends, so nothing is prompted.
    """
    scripted_jobs(monkeypatch, [['123 sleep 45'], ['123 sleep 45'], [], []], resumed=[True])
    scripted(monkeypatch, agent_wait=[seq(9), seq(9), seq(9), seq(11)])
    outcome = HerdrRuntime('demo', tmp_path).wait(jobs_handle(), timeout=5)
    assert (outcome.status, outcome.error) == (WorkerStatus.IDLE, None)
    verbs = [c[:2] for c in calls(herdr_log)]
    assert verbs.count(['agent', 'wait']) == 4 and ['agent', 'prompt'] not in verbs, (
        'two looks with jobs, one once they ended (the agent resumed by itself), one after it finished'
    )


def test_an_agent_left_idle_when_its_jobs_end_is_prompted_to_read_their_results(
    tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A job started with ``nohup … &`` tells nobody that it ended, so the idle agent is prompted to read its output."""
    scripted_jobs(monkeypatch, [['77 sleep 50'], [], []], resumed=[False, True])
    scripted(monkeypatch, agent_wait=[seq(9), seq(9), seq(12)])
    outcome = HerdrRuntime('demo', tmp_path).wait(jobs_handle(), timeout=5)
    assert (outcome.status, outcome.error) == (WorkerStatus.IDLE, None)
    prompts = [c for c in calls(herdr_log) if c[:2] == ['agent', 'prompt']]
    assert prompts == [['agent', 'prompt', 'w1', herdr.JOBS_DONE_PROMPT]]

    scripted_jobs(monkeypatch, [['77 sleep 50'], [], []], resumed=[False])
    scripted(monkeypatch, agent_wait=[seq(9)], agent_prompt=[ERR])
    outcome = HerdrRuntime('demo', tmp_path).wait(jobs_handle(), timeout=5)
    assert outcome.status is WorkerStatus.IDLE, 'a refused prompt: the next look decides, and nothing is in flight'

    scripted_jobs(monkeypatch, [['77 sleep 50'], []], resumed=[False, False])
    scripted(monkeypatch, agent_wait=[seq(9)])
    outcome = HerdrRuntime('demo', tmp_path).wait(jobs_handle(), timeout=5)
    assert outcome.status is WorkerStatus.IDLE, 'prompted but never picked up: finished, not a loop'


def process_info(monkeypatch: pytest.MonkeyPatch, response: Response) -> None:
    monkeypatch.setenv('FAKE_HERDR_SCRIPT', json.dumps({'pane process-info': [response]}))


def tagged_sleep(token: str, *, own_session: bool) -> subprocess.Popen[bytes]:
    env = {**os.environ, herdr.TOKEN_ENV: token}
    return subprocess.Popen(['sleep', '30'], env=env, start_new_session=own_session)


def test_a_session_without_a_server_starts_one_and_polls_until_it_answers(
    herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    not_running: Response = {'error': {'code': 'server_not_running', 'message': 'no herdr server is running'}}
    scripted(monkeypatch, workspace_list=[not_running, not_running, {'result': {'workspaces': []}}])
    monkeypatch.setattr(herdr, 'SERVER_POLL_SECONDS', 0.01)
    assert herdr._call('workspace', 'list') == {'workspaces': []}
    verbs = calls(herdr_log)
    assert [c for c in verbs if c != ['server']] == [['workspace', 'list']] * 4 and verbs.count(['server']) == 1, (
        'refused, then the server (detached) is polled until it answers and the call retried'
    )

    scripted(monkeypatch, workspace_list=[not_running])
    monkeypatch.setattr(herdr.settings_module, 'settings', Settings(herdr_server_start_seconds=0.05))
    with pytest.raises(HerdrError, match='server_not_running'):
        herdr._call('workspace', 'list')


UNAVAILABLE: Response = {
    'error': {
        'code': 'server_unavailable',
        'message': 'request handling failed: timed out waiting for app response after 5000 ms',
    }
}


def test_a_server_that_does_not_answer_for_a_moment_does_not_end_the_agent(
    tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One ``server_unavailable`` under load killed four healthy workers (comments #11239, #11244): a read is retried."""
    monkeypatch.setattr(herdr, 'UNAVAILABLE_BACKOFF_SECONDS', 0.01)
    scripted(monkeypatch, agent_wait=[UNAVAILABLE, UNAVAILABLE, {'result': {'agent': AGENT}}])
    handle = Handle('herdr', {'workspace_id': 'w7', 'tab_id': 'w7:t2', 'pane_id': 'w7:p2', 'agent': 'w1'})
    outcome = HerdrRuntime('demo', tmp_path).wait(handle, timeout=60)
    assert (outcome.status, outcome.error) == (WorkerStatus.IDLE, None)
    verbs = [c[:2] for c in calls(herdr_log)]
    assert verbs.count(['agent', 'wait']) == 3 and ['tab', 'close'] not in verbs, 'retried, and nothing was killed'


def test_a_server_that_stays_unavailable_fails_the_read_after_the_window(
    herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(herdr, 'UNAVAILABLE_BACKOFF_SECONDS', 0.01)
    monkeypatch.setattr(herdr, 'UNAVAILABLE_MAX_BACKOFF_SECONDS', 0.02)
    monkeypatch.setattr(settings_module, 'settings', Settings(herdr_unavailable_seconds=0.2))
    scripted(monkeypatch, agent_get=[UNAVAILABLE])
    with pytest.raises(HerdrError, match='server_unavailable'):
        herdr._call('agent', 'get', 'w1')
    assert len(calls(herdr_log)) > 2, 'retried until the window ran out'


def test_a_call_that_changes_something_is_never_sent_twice(herdr_log: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The server may have applied a prompt that it did not answer, so a retry could type it twice."""
    monkeypatch.setattr(herdr, 'UNAVAILABLE_BACKOFF_SECONDS', 0.01)
    scripted(monkeypatch, agent_prompt=[UNAVAILABLE, {'result': {'agent': AGENT}}])
    with pytest.raises(HerdrError, match='server_unavailable'):
        herdr._call('agent', 'prompt', 'w1', 'hello')
    assert calls(herdr_log) == [['agent', 'prompt', 'w1', 'hello']]


def test_a_read_that_times_out_is_retried(herdr_log: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A screen read that times out under load lost the transcripts of the workers it then killed (comment #11244)."""
    monkeypatch.setattr(herdr, 'UNAVAILABLE_BACKOFF_SECONDS', 0.01)
    real = subprocess.run
    timeouts = [subprocess.TimeoutExpired(['herdr'], 30)]

    def stalls_once(
        argv: list[str], *, capture_output: bool, text: bool, timeout: float
    ) -> subprocess.CompletedProcess[str]:
        if timeouts:
            raise timeouts.pop()
        return real(argv, capture_output=capture_output, text=True, timeout=timeout)

    monkeypatch.setattr(herdr.subprocess, 'run', stalls_once)
    scripted(monkeypatch, pane_read=[{'text': 'the screen'}])
    assert herdr._run('pane', 'read', 'w7:p2') == 'the screen'

    monkeypatch.setattr(settings_module, 'settings', Settings(herdr_unavailable_seconds=0))
    timeouts.append(subprocess.TimeoutExpired(['herdr'], 30))
    with pytest.raises(subprocess.TimeoutExpired):
        herdr._run('pane', 'read', 'w7:p2')


def test_jobs_are_tagged_processes_outside_the_agents_session(
    tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A tagged process in a session of its own is a job.

    The fake reports this test process as the pane shell. A tagged child in its session belongs to the agent (an MCP
    server, a helper). An untagged process belongs to someone else.
    """
    rt = HerdrRuntime('demo', tmp_path)
    token = os.urandom(8).hex()
    handle = Handle('herdr', {**jobs_handle().data, 'token': token})
    helper, job, stranger = tagged_sleep(token, own_session=False), tagged_sleep(token, own_session=True), None
    try:
        stranger = tagged_sleep('someone-else', own_session=True)
        assert rt.jobs(handle) == [f'{job.pid} sleep 30']
        assert calls(herdr_log)[-1] == ['pane', 'process-info', '--pane', 'w7:p2']
        assert rt.jobs(Handle('herdr', {**handle.data, 'token': None})) == [], 'a handle from before the token'
        assert HerdrRuntime('demo', tmp_path).jobs(Handle('herdr', {'pane_id': 'w7:p2', 'agent': 'w1'})) == []

        rt.kill(handle)
        assert job.wait(5) == -9 and helper.wait(5) == -9, 'kill ends what the worker left running, in any session'
        assert stranger.poll() is None
    finally:
        for proc in (helper, job, stranger):
            if proc is not None:
                proc.kill()
                proc.wait()

    process_info(monkeypatch, {'error': {'code': 'pane_not_found', 'message': 'gone'}})
    assert rt.jobs(handle) == [], 'the pane is gone: the next wait finds the agent gone'
    process_info(monkeypatch, {'result': {'process_info': {'shell_pid': 2**22 + 1}}})
    assert rt.jobs(handle) == [], 'the shell is gone'
    process_info(monkeypatch, {'error': {'code': 'io', 'message': 'boom'}})
    with pytest.raises(HerdrError, match='io: boom'):
        rt.jobs(handle)


def test_kill_tolerates_a_tagged_process_that_ends_first(
    tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(herdr, 'tagged', lambda marker: [(2**22 + 1, 1, 'gone already')])
    HerdrRuntime('demo', tmp_path).kill(jobs_handle())
    assert calls(herdr_log) == [['tab', 'close', 'w7:t2']]


def test_wait_is_chunked_so_a_tab_closed_from_outside_is_noticed(
    tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    timed_out: Response = {'error': {'code': 'timeout', 'message': 'timed out'}}
    gone: Response = {'error': {'code': 'agent_not_found', 'message': 'closed from outside'}}
    scripted(monkeypatch, agent_wait=[timed_out, gone])
    monkeypatch.setattr(settings_module, 'settings', Settings(herdr_wait_chunk_seconds=0.5))
    handle = Handle('herdr', {'workspace_id': 'w7', 'tab_id': 'w7:t2', 'pane_id': 'w7:p2', 'agent': 'w1'})
    assert HerdrRuntime('demo', tmp_path).wait(handle, timeout=60).status is WorkerStatus.EXITED
    assert [c[-1] for c in calls(herdr_log)] == ['500', '500']


def test_session_id_is_what_herdr_detected_once_the_harness_started(
    tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr('piloop.runtime.herdr.PICKUP_POLL_SECONDS', 0.01)
    monkeypatch.setattr('piloop.runtime.herdr.settings_module.settings', Settings(herdr_prompt_pickup_seconds=0.5))
    rt = HerdrRuntime('demo', tmp_path)
    handle = Handle('herdr', {'workspace_id': 'w7', 'tab_id': 'w7:t2', 'pane_id': 'w7:p2', 'agent': 'w1'})
    assert rt.session_id(handle) == SESSION_ID
    assert calls(herdr_log) == [['agent', 'get', 'w1']]
    detected: Response = {'result': {'agent': {**AGENT, 'agent_session': AGENT_SESSION}}}
    scripted(monkeypatch, agent_get=[seq(5), seq(5), seq(5), detected])
    assert rt.session_id(handle) == SESSION_ID, 'absent until the harness is up: polled'
    assert len(calls(herdr_log)) == 4, (
        'the fake indexes its script by every agent get so far: two without, then one with'
    )
    scripted(monkeypatch, agent_get=[seq(5)])
    assert rt.session_id(handle) is None, 'never detected within the wait'
    scripted(monkeypatch, agent_get=[{'error': {'code': 'agent_not_found', 'message': 'gone'}}])
    assert rt.session_id(handle) is None, 'already exited'
    scripted(monkeypatch, agent_get=[{'error': {'code': 'io', 'message': 'boom'}}])
    with pytest.raises(HerdrError, match='io: boom'):
        rt.session_id(handle)


def test_status_prompt_read_kill(tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    rt = HerdrRuntime('demo', tmp_path)
    handle = Handle('herdr', {'workspace_id': 'w7', 'tab_id': 'w7:t2', 'pane_id': 'w7:p2', 'agent': 'w1'})
    assert rt.status(handle) is WorkerStatus.WORKING
    rt.prompt(handle, 'try harder')
    assert rt.read(handle, lines=40) == 'hello from the agent\n'
    rt.kill(handle)
    assert calls(herdr_log) == snapshot(
        [
            ['agent', 'get', 'w1'],
            ['agent', 'prompt', 'w1', 'try harder'],
            ['agent', 'read', 'w1', '--source', 'recent-unwrapped', '--lines', '40'],
            ['tab', 'close', 'w7:t2'],
        ]
    )

    scripted(monkeypatch, agent_get=[{'error': {'code': 'agent_not_found', 'message': 'gone'}}])
    assert rt.status(handle) is WorkerStatus.EXITED
    scripted(monkeypatch, agent_get=[{'error': {'code': 'io', 'message': 'boom'}}])
    with pytest.raises(HerdrError, match='io: boom'):
        rt.status(handle)

    scripted(monkeypatch, agent_prompt=[ERR])
    with pytest.raises(RuntimeError, match='w1 is blocked on an approval or question'):
        rt.prompt(handle, 'x')
    scripted(monkeypatch, agent_prompt=[{'error': {'code': 'io', 'message': 'boom'}}])
    with pytest.raises(HerdrError, match='io: boom'):
        rt.prompt(handle, 'x')

    scripted(monkeypatch, tab_close=[{'error': {'code': 'tab_not_found', 'message': 'gone'}}])
    rt.kill(handle)
    scripted(monkeypatch, tab_close=[{'error': {'code': 'io', 'message': 'boom'}}])
    with pytest.raises(HerdrError):
        rt.kill(handle)


def test_missing_binary_and_malformed_output(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('PATH', str(tmp_path))
    with pytest.raises(HerdrError, match='not_installed'):
        HerdrRuntime('demo', tmp_path)

    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    noisy = bin_dir / 'herdr'
    noisy.write_text('#!/bin/sh\necho "connection refused" >&2\nexit 1\n')
    noisy.chmod(0o755)
    monkeypatch.setenv('PATH', str(bin_dir))
    with pytest.raises(HerdrError, match='cli_error: connection refused'):
        HerdrRuntime('demo', tmp_path).status(Handle('herdr', {'agent': 'w1'}))

    noisy.write_text('#!/bin/sh\nexit 1\n')
    with pytest.raises(HerdrError, match='exited non-zero without output'):
        HerdrRuntime('demo', tmp_path).status(Handle('herdr', {'agent': 'w1'}))

    noisy.write_text('#!/bin/sh\necho \'{"id": "x", "result": [1, 2]}\'\n')
    with pytest.raises(HerdrError, match='bad_response: expected a JSON object, got list'):
        HerdrRuntime('demo', tmp_path).status(Handle('herdr', {'agent': 'w1'}))


@pytest.mark.parametrize('kind', list(RuntimeKind))
def test_every_runtime_kind_is_registered(kind: RuntimeKind, tmp_path: Path, herdr_log: Path) -> None:
    runtime = make_runtime(kind, project='p', project_path=tmp_path)
    assert runtime.kind == kind
    assert isinstance(runtime, {RuntimeKind.HERDR: HerdrRuntime, RuntimeKind.SUBPROCESS: SubprocessRuntime}[kind])


@pytest.mark.herdr
@pytest.mark.skipif(shutil.which('herdr') is None, reason='herdr is not installed')
def test_real_herdr_session_is_started_headless_and_layout_roundtrips(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The first call starts the server of a throwaway session, and a workspace, tab and pane round trip against it.

    No agent is started, because that costs a model call. The human's own sessions are not touched.
    """
    session = f'piloop-test-{os.getpid()}'
    monkeypatch.setattr(settings_module, 'settings', Settings(herdr_session=session))
    rt = HerdrRuntime('demo', tmp_path)
    try:
        workspace_id = rt._workspace()
        assert HerdrRuntime('demo', tmp_path)._workspace() == workspace_id, 'found again by its root token'
        twin = tmp_path / 'twin' / 'demo'
        twin.mkdir(parents=True)
        twin_id = HerdrRuntime('demo', twin)._workspace()
        assert twin_id != workspace_id, 'a same-named directory gets its own'
        labels = {w['workspace_id']: w['label'] for w in herdr._call('workspace', 'list')['workspaces']}
        assert [labels[workspace_id], labels[twin_id]] == ['demo', 'demo (twin)'], (
            'only the workspaces piloop created: a new session also has a default one'
        )
        tab = herdr._call(
            'tab', 'create', '--workspace', workspace_id, '--cwd', str(tmp_path), '--label', 'w1', '--no-focus'
        )
        pane_id = tab['root_pane']['pane_id']
        assert herdr._call('pane', 'run', pane_id, 'echo', 'piloop-smoke') == {}
        matched = herdr._call('pane', 'wait-output', pane_id, '--regex', 'piloop-smoke', '--timeout', '10000')
        assert 'piloop-smoke' in matched['matched_line']
        rt.kill(Handle('herdr', {'tab_id': tab['tab']['tab_id'], 'agent': 'w1'}))
        socket = herdr.session_socket()
        assert socket is not None and socket.endswith(f'/sessions/{session}/herdr.sock')
    finally:
        subprocess.run(['herdr', 'session', 'stop', session], check=True, capture_output=True)
        subprocess.run(['herdr', 'session', 'delete', session], check=True, capture_output=True)


def test_agent_start_may_take_the_whole_window_piloop_asks_herdr_for(
    tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The CLI timeout of ``agent start`` is the start window plus the transport grace, not the plain CLI timeout."""
    timeouts: dict[str, float] = {}
    real = subprocess.run

    def spy(argv: list[str], *, capture_output: bool, text: bool, timeout: float) -> subprocess.CompletedProcess[str]:
        timeouts[' '.join(argv[3:5])] = timeout
        return real(argv, capture_output=capture_output, text=True, timeout=timeout)

    monkeypatch.setattr(herdr.subprocess, 'run', spy)
    monkeypatch.setattr(
        settings_module, 'settings', Settings(herdr_start_timeout_seconds=60, herdr_cli_timeout_seconds=30)
    )
    HerdrRuntime('demo', tmp_path).start(spec(tmp_path))
    assert (timeouts['agent start'], timeouts['tab create']) == (90, 30), 'the requested window plus transport grace'


def test_a_failed_start_keeps_what_its_screen_showed_before_the_tab_closes(
    tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed start reads the pane's screen before it closes the tab, so the screen is not lost with the pane."""
    monkeypatch.setattr('piloop.runtime.herdr.PICKUP_POLL_SECONDS', 0.01)
    monkeypatch.setattr('piloop.runtime.herdr.settings_module.settings', Settings(herdr_prompt_pickup_seconds=0.02))
    scripted(monkeypatch, agent_get=[seq(5)], pane_read=[{'text': 'Welcome to Claude Code\n> do the thing\n'}])
    with pytest.raises(StartFailed) as raised:
        HerdrRuntime('demo', tmp_path).start(spec(tmp_path))
    assert raised.value.screen == 'Welcome to Claude Code\n> do the thing\n'
    assert calls(herdr_log)[-2:] == [['pane', 'read', 'w7:p2', '--lines', '500'], ['tab', 'close', 'w7:t2']]

    herdr_log.write_text('')
    scripted(monkeypatch, agent_get=[seq(5)], pane_read=[{'error': {'code': 'pane_not_found', 'message': 'gone'}}])
    with pytest.raises(StartFailed, match='prompt_lost') as raised:
        HerdrRuntime('demo', tmp_path).start(spec(tmp_path))
    assert raised.value.screen == '(unreadable: HerdrError: pane_not_found: gone)', 'the start error stands'


def claude_handle(kind: str = 'claude') -> Handle:
    return Handle('herdr', {'workspace_id': 'w7', 'tab_id': 'w7:t2', 'pane_id': 'w7:p2', 'agent': 'w1', 'kind': kind})


QUEUED_SCREEN: Response = {
    'text': '● Here is the essay\n  The data begins\n\n────\n❯ Press up to edit queued messages\n────\n'
    '  ⏵⏵ bypass permissions on\n\n\n'
}
RETRY_SCREEN: Response = {
    'text': '✻ Waiting for API response · will retry in 2m 40s · check your network\n────\n❯ \n────\n'
}
QUOTA_BANNER = (
    '✻ 503 included_quota_unknown: Cannot verify included subscription quota; paid fallback is disabled (last upstream'
    ' e… · Retrying in 0s · attempt 5/10'
)
QUOTA_RETRY_SCREEN: Response = {
    'text': f'● Running for i in 1 2 3; do ./eval.sh "$i"…\n\n{QUOTA_BANNER}\n\n────\n❯ \n────\n'
    '  Opus 5.5 · ~/.piloop/runs/184/worktree ⎇ ac2d0e8 · 11% · $0.41 · 03:16\n'
    '  ⏵⏵ bypass permissions on (shift+tab to cycle)\n'
}
THINKING_SCREEN: Response = {
    'text': '● Finding the worktree\n\n✶ Forming… (thinking with high effort)\n\n────\n❯ \n────\n  Opus 5.5\n'
}
DONE_SCREEN: Response = {'text': '● Done: committed and left my finding.\n────\n❯ \n────\n'}
CAPACITY_TEXT = (
    '• Launching four preparation shards now\n■ Selected model is at capacity. Please try a different model.\n'
    '  1 background terminal running · /ps to view · /stop to close\n› Ask Codex to do anything\n'
)
CAPACITY_SCREEN: Response = {'text': CAPACITY_TEXT}


CODEX_STARTUP_SCREEN: Response = {
    'text': '  >_ OpenAI Codex (v0.160.1)\n     ~/.piloop/runs/312/worktree\n  permissions: YOLO mode\n\n'
    '› [Pasted Content 74481 chars]\n\n  Waiting for startup  · esc cancel\n'
}
"""Codex took the brief before its app server was up. It submits the brief when the server is up, and Herdr's state
does not change until then."""
QUOTA_STOPPED_SCREEN: Response = {
    'text': '⎿ 503 included_quota_unknown: Cannot verify included subscription quota\n────\n❯ \n────\n'
}


@pytest.mark.parametrize(
    ('kind', 'screen'),
    [
        ('codex', CODEX_STARTUP_SCREEN),
        ('claude', QUOTA_RETRY_SCREEN),
        ('claude', RETRY_SCREEN),
        ('claude', QUOTA_STOPPED_SCREEN),
    ],
    ids=['codex-startup', 'api-retry-attempt', 'api-retry', 'api-error'],
)
def test_a_brief_taken_by_an_agent_still_starting_or_retrying_is_delivered(
    kind: str, screen: Response, tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A screen that shows the agent starting up or retrying the API means it took the brief.

    Herdr's state does not change within the pickup window in these cases. The start succeeds, and ``wait`` judges
    what follows.
    """
    monkeypatch.setattr('piloop.runtime.herdr.PICKUP_POLL_SECONDS', 0.01)
    monkeypatch.setattr('piloop.runtime.herdr.settings_module.settings', Settings(herdr_prompt_pickup_seconds=0.05))
    scripted(monkeypatch, agent_get=[seq(5)], pane_read=[screen])
    HerdrRuntime('demo', tmp_path).start(replace(spec(tmp_path), executable=kind))
    verbs = [c[:2] for c in calls(herdr_log)]
    assert verbs.count(['agent', 'prompt']) == 1 and ['tab', 'close'] not in verbs

    scripted(monkeypatch, agent_get=[seq(5)], pane_read=[screen])
    with pytest.raises(StartFailed, match='prompt_lost'):
        HerdrRuntime('demo', tmp_path).start(spec(tmp_path))  # pi: another kind's text is just text


STREAMING: list[Response] = [
    {'text': '● Here is the essay\n  The data'},
    {'text': '● Here is the essay\n  The data begins'},
]


@pytest.mark.parametrize(
    ('kind', 'busy'),
    [
        ('pi', STREAMING),
        ('claude', [QUEUED_SCREEN, QUEUED_SCREEN]),
        ('claude', [RETRY_SCREEN, RETRY_SCREEN]),
        ('claude', [QUOTA_RETRY_SCREEN, QUOTA_RETRY_SCREEN]),
        ('claude', [THINKING_SCREEN, THINKING_SCREEN]),
        ('codex', [CODEX_STARTUP_SCREEN, CODEX_STARTUP_SCREEN]),
    ],
    ids=['streaming', 'queued-prompt', 'api-retry', 'api-retry-attempt', 'thinking', 'codex-startup'],
)
def test_idle_on_a_screen_that_still_works_is_not_finished(
    kind: str, busy: list[Response], tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Idle on a screen that still changes, or that shows a busy text, is an agent at work.

    Herdr reports Claude Code as ``done`` when a prompt is typed in while it streams, while it waits to retry the API,
    and while it thinks at length with a spinner that can hold still.
    """
    sleeps: list[float] = []
    scripted(monkeypatch, pane_read=[*busy, DONE_SCREEN])
    outcome = HerdrRuntime('demo', tmp_path, sleep=sleeps.append).wait(claude_handle(kind), timeout=5)
    assert (outcome.status, outcome.error) == (WorkerStatus.IDLE, None)
    verbs = [c[:2] for c in calls(herdr_log)]
    assert (verbs.count(['pane', 'read']), verbs.count(['agent', 'wait'])) == (4, 2), 'two looks, two reads each'
    assert sleeps == [herdr.QUIET_SECONDS] * 2 and ['agent', 'prompt'] not in verbs

    herdr_log.write_text('')
    scripted(monkeypatch, pane_read=[QUEUED_SCREEN])
    assert (
        HerdrRuntime('demo', tmp_path, sleep=sleeps.append).wait(claude_handle('pi'), timeout=5).status
        is WorkerStatus.IDLE
    )
    assert [c[:2] for c in calls(herdr_log)].count(['agent', 'wait']) == 1, "another kind's text is just text"


def test_a_turn_that_ended_on_an_api_error_is_resumed_then_fails(
    tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A turn that ended on an API error is resumed after each backoff, then the run fails with the error.

    An error such as "Selected model is at capacity" is transient, so an agent idle on it is not finished.
    """
    sleeps: list[float] = []
    monkeypatch.setattr(HerdrRuntime, '_state_changed', lambda self, agent, before, timeout: False)
    resumed: Response = {'text': f'{CAPACITY_TEXT}› {herdr.RESUME_PROMPT}\n• Shards done; committed.\n›\n'}
    scripted(monkeypatch, pane_read=[CAPACITY_SCREEN, CAPACITY_SCREEN, resumed])
    outcome = HerdrRuntime('demo', tmp_path, sleep=sleeps.append).wait(claude_handle('codex'), timeout=1000)
    assert (outcome.status, outcome.error) == (WorkerStatus.IDLE, None), 'the banner above the resume is an old one'
    prompts = [c for c in calls(herdr_log) if c[:2] == ['agent', 'prompt']]
    assert prompts == [['agent', 'prompt', 'w1', herdr.RESUME_PROMPT]]
    assert sleeps == [herdr.QUIET_SECONDS, 60, herdr.QUIET_SECONDS]

    herdr_log.write_text('')
    sleeps.clear()
    scripted(monkeypatch, pane_read=[CAPACITY_SCREEN])
    outcome = HerdrRuntime('demo', tmp_path, sleep=sleeps.append).wait(claude_handle('codex'), timeout=1000)
    assert outcome.status is WorkerStatus.IDLE
    assert outcome.error == f'{herdr.TURN_FAILED_ERROR}:\n{CAPACITY_TEXT.strip()}'
    assert [s for s in sleeps if s != herdr.QUIET_SECONDS] == [60, 120, 240], 'resumed after each backoff, then failed'
    assert [c[:2] for c in calls(herdr_log)].count(['agent', 'prompt']) == 3

    herdr_log.write_text('')
    scripted(monkeypatch, pane_read=[{'error': {'code': 'pane_not_found', 'message': 'gone'}}])
    assert (
        HerdrRuntime('demo', tmp_path, sleep=sleeps.append).wait(claude_handle('codex'), timeout=1000).error is None
    ), 'the pane went'
    scripted(monkeypatch, pane_read=[{'error': {'code': 'io', 'message': 'boom'}}])
    with pytest.raises(HerdrError, match='io: boom'):
        HerdrRuntime('demo', tmp_path, sleep=sleeps.append).wait(claude_handle('codex'), timeout=1000)


def test_a_claude_turn_that_stopped_on_a_quota_503_is_resumed(
    tmp_path: Path, herdr_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A ``503 included_quota_unknown`` is transient.

    Once Claude Code stops retrying it, the turn is resumed like any API error. The worker is not taken as finished.
    """
    sleeps: list[float] = []
    monkeypatch.setattr(HerdrRuntime, '_state_changed', lambda self, agent, before, timeout: False)
    stopped = '⎿ 503 included_quota_unknown: Cannot verify included subscription quota\n────\n❯ \n────\n'
    resumed: Response = {'text': f'{stopped}❯ {herdr.RESUME_PROMPT}\n● Committed.\n────\n❯ \n────\n'}
    scripted(monkeypatch, pane_read=[{'text': stopped}, {'text': stopped}, resumed])
    outcome = HerdrRuntime('demo', tmp_path, sleep=sleeps.append).wait(claude_handle(), timeout=1000)
    assert (outcome.status, outcome.error) == (WorkerStatus.IDLE, None)
    assert [c for c in calls(herdr_log) if c[:2] == ['agent', 'prompt']] == [
        ['agent', 'prompt', 'w1', herdr.RESUME_PROMPT]
    ]
    assert sleeps == [herdr.QUIET_SECONDS, 60, herdr.QUIET_SECONDS]
