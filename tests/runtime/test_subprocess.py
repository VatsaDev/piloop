import os
import signal
import stat
import subprocess
import sys
import threading
import time
from dataclasses import fields
from pathlib import Path

import psutil
import pytest
from dirty_equals import IsPositive, IsStr

from piloop.plugins.runtime import subprocess as subprocess_runtime
from piloop.plugins.runtime.contract import (
    AgentSpec,
    AgentStart,
    ExitStatus,
    Handle,
    NotSteerable,
    locked,
    take,
)
from piloop.plugins.runtime.subprocess import Subprocess
from piloop.proc import alive, started


def script(tmp_path: Path, body: str) -> Path:
    path = tmp_path / 'agent_session.sh'
    path.write_text(f'#!/bin/sh\n{body}\n')
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return path


def spec(
    tmp_path: Path,
    executable: Path,
    *args: str,
    env: dict[str, str] | None = None,
    pattern: str | None = None,
    resume: list[str] | None = None,
) -> AgentSpec:
    return AgentSpec(
        executable=str(executable),
        args=list(args),
        cwd=tmp_path,
        log_dir=tmp_path / 'logs',
        env=env or {},
        session_id_pattern=pattern,
        resume_args=resume or [],
    )


def test_runs_to_completion_and_captures_output(tmp_path: Path) -> None:
    rt = Subprocess().open()
    exe = script(tmp_path, 'echo "prompt=$1 var=$PILOOP_TEST token=$PILOOP_PROCESS_TAG"; echo oops >&2; exit 3')
    handle = rt.start(spec(tmp_path, exe, 'the brief', env={'PILOOP_TEST': 'yes'}))
    assert handle.runtime == 'subprocess'
    assert handle.data == {
        'pid': IsPositive(),
        'pgid': handle.data['pid'],
        'start_time': IsPositive(),
        'started_at': IsPositive(),
        'log_dir': str(tmp_path / 'logs'),
        'session_id_pattern': None,
        'token': IsStr(regex='[0-9a-f]{32}'),
    }
    outcome = rt.wait(handle, timeout=10)
    assert (outcome.status, outcome.exit_code, outcome.error) == (ExitStatus.EXITED, 3, 'agent exit 3')
    assert outcome.seconds == IsPositive()
    assert not rt._alive(handle)
    assert rt.read(handle) == f'prompt=the brief var=yes token={handle.data["token"]}'
    assert (tmp_path / 'logs' / 'agent_session.stderr').read_text() == 'oops\n'


def test_timeout_kills_the_whole_process_group(tmp_path: Path) -> None:
    rt = Subprocess().open()
    exe = script(tmp_path, 'sleep 30 &\necho $! > child.pid\nwait')
    handle = rt.start(spec(tmp_path, exe))
    assert rt._alive(handle)
    outcome = rt.wait(handle, timeout=0.5)
    assert (outcome.status, outcome.exit_code, outcome.error) == (
        ExitStatus.TIMED_OUT,
        None,
        'agent timeout after 0.5s',
    )
    child = int((tmp_path / 'child.pid').read_text())
    time.sleep(0.2)
    assert started(child) is None, 'the sleeping grandchild must die with the group'


ESCAPE = f"""
{sys.executable} -c 'import os, time; os.setsid(); open("escaped.pid", "w").write(str(os.getpid())); time.sleep(60)' &
while [ ! -s escaped.pid ]; do sleep 0.05; done
"""


def escaped(tmp_path: Path) -> tuple[int, float]:
    """The pid and start time of the child that left the agent's session, as Claude Code's Bash calls do."""
    path, deadline = tmp_path / 'escaped.pid', time.monotonic() + 10
    while not (path.is_file() and path.read_text()) and time.monotonic() < deadline:
        time.sleep(0.05)
    pid = int(path.read_text())
    start = started(pid)
    assert start is not None
    return pid, start


def gone(pid: int, start: float) -> bool:
    deadline = time.monotonic() + 5
    while alive(pid, start) and time.monotonic() < deadline:
        time.sleep(0.05)
    return not alive(pid, start)


def test_a_timeout_kills_what_the_agent_started_in_a_session_of_its_own(tmp_path: Path) -> None:
    """A Claude Code Bash call runs in its own session, outside the agent's group: the builds it ran outlived the
    agent by an hour. Every process of the agent carries its token, and the kill ends the tagged ones."""
    rt = Subprocess().open()
    handle = rt.start(spec(tmp_path, script(tmp_path, f'{ESCAPE}sleep 30')))
    pid, start = escaped(tmp_path)
    try:
        assert os.getsid(pid) != handle.data['pid'], 'the child left the group that killpg reaches'
        assert rt.wait(handle, timeout=0.5).status is ExitStatus.TIMED_OUT
        assert gone(pid, start), 'the escaped child dies with the agent'
    finally:
        if alive(pid, start):
            os.kill(pid, signal.SIGKILL)


def test_kill_ends_an_escaped_child_after_the_agent_itself_exited(tmp_path: Path) -> None:
    """The launcher kills every agent once its wait returns. An agent that exited first leaves its jobs behind."""
    rt = Subprocess().open()
    handle = rt.start(spec(tmp_path, script(tmp_path, ESCAPE)))
    pid, start = escaped(tmp_path)
    try:
        assert rt.wait(handle, timeout=10).status is ExitStatus.EXITED
        assert alive(pid, start), 'the agent exited, its job did not'
        Subprocess().open().kill(Handle(handle.runtime, dict(handle.data)))
        assert gone(pid, start), 'any instance, from the handle in the database'
    finally:
        if alive(pid, start):
            os.kill(pid, signal.SIGKILL)


def test_session_id_is_read_from_stdout_while_the_process_still_runs(tmp_path: Path) -> None:
    rt = Subprocess().open()
    exe = script(tmp_path, 'sleep 0.3; echo \'{"type":"thread.started","thread_id":"abc-123"}\'; sleep 5')
    handle = rt.start(spec(tmp_path, exe, pattern=r'"thread_id":"([a-z0-9-]+)"'))
    started = time.monotonic()
    assert rt.session_id(handle) == 'abc-123'
    assert 0.2 < time.monotonic() - started < 3, 'returned as soon as the line appeared'
    rt.kill(handle)
    silent = rt.start(spec(tmp_path, script(tmp_path, 'echo nothing of note'), pattern=r'"thread_id":"([a-z0-9-]+)"'))
    started = time.monotonic()
    assert rt.session_id(silent) is None
    assert time.monotonic() - started < 3, 'a process that ended without the line is not waited for'
    rt.wait(silent, timeout=5)
    unpatterned = rt.start(spec(tmp_path, script(tmp_path, 'echo thread_id')))
    assert rt.session_id(unpatterned) is None, 'no pattern, no id'
    rt.wait(unpatterned, timeout=5)


def test_a_harness_without_resume_arguments_takes_no_message(tmp_path: Path) -> None:
    rt = Subprocess().open()
    handle = rt.start(spec(tmp_path, script(tmp_path, 'sleep 5')))
    with pytest.raises(NotSteerable, match='its harness has no resume arguments, or the daemon'):
        rt.prompt(handle, 'hi')
    rt.kill(handle)
    rt.kill(handle)
    assert not rt._alive(handle)


def test_rehydrated_handle_is_observable_without_an_exit_code(tmp_path: Path) -> None:
    starter = Subprocess().open()
    handle = starter.start(spec(tmp_path, script(tmp_path, 'sleep 5')))
    other = Subprocess().open()  # another process would rebuild the handle from the database
    rehydrated = Handle(handle.runtime, dict(handle.data))
    assert other._alive(rehydrated)
    other.kill(rehydrated)
    starter.wait(handle, timeout=5)
    assert not other._alive(rehydrated)
    assert other.wait(rehydrated, timeout=1).exit_code is None
    assert other.read(Handle('subprocess', {**handle.data, 'log_dir': str(tmp_path / 'nope')})) == ''


def test_reused_pid_is_not_mistaken_for_the_implementer(tmp_path: Path) -> None:
    rt = Subprocess().open()
    handle = rt.start(spec(tmp_path, script(tmp_path, 'true')))
    rt.wait(handle, timeout=5)
    impostor = Handle('subprocess', {**handle.data, 'pid': os.getpid(), 'start_time': -1.0})
    assert not Subprocess().open()._alive(impostor)


@pytest.mark.parametrize('error', [ProcessLookupError, PermissionError])
def test_kill_tolerates_a_vanished_group(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error: type[OSError]) -> None:
    """The group exits as it is killed: Linux answers ESRCH, macOS EPERM for a group of zombies."""
    rt = Subprocess().open()
    handle = rt.start(spec(tmp_path, script(tmp_path, 'sleep 5')))
    killpg = os.killpg

    def gone(pgid: int, signum: int) -> None:
        killpg(pgid, signum)
        while psutil.Process(pgid).status() != psutil.STATUS_ZOMBIE:  # exited, and not reaped yet
            time.sleep(0.01)
        raise error

    monkeypatch.setattr(os, 'killpg', gone)
    rt.kill(handle)
    monkeypatch.undo()
    rt.kill(handle)


def test_started_is_the_start_time_of_a_live_pid_and_none_for_a_gone_one() -> None:
    assert started(os.getpid()) == IsPositive()
    assert started(2**22 + 12345) is None


def test_an_agent_that_exits_at_once_still_starts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """macOS answers ``getpgid`` with ESRCH for a child that already exited; the start must not ask it."""

    def gone(pid: int) -> int:
        raise ProcessLookupError(3, 'No such process')

    monkeypatch.setattr(os, 'getpgid', gone)
    rt = Subprocess().open()
    handle = rt.start(spec(tmp_path, script(tmp_path, 'exit 0')))
    assert handle.data['pgid'] == handle.data['pid']
    assert rt.wait(handle, timeout=10).status is ExitStatus.EXITED


TURNS = """
if [ "$1" = resume ]; then echo "turn 2 of $2: $3" >> turns.log; exit 0; fi
echo '{"thread_id":"t-42"}'
echo "turn 1: $1" >> turns.log
sleep 1
"""


def test_a_message_waits_for_the_turn_to_end_then_resumes_the_session_with_it(tmp_path: Path) -> None:
    """The subprocess runtime queues a message: nothing reaches the turn in progress. When the process exits,
    the harness runs again with ``resume_args``: the same session, the queued messages as its prompt."""
    rt = Subprocess().open()
    exe = script(tmp_path, TURNS)
    handle = rt.start(
        spec(
            tmp_path, exe, 'the brief', pattern=r'"thread_id":"([^"]+)"', resume=['resume', '{session_id}', '{prompt}']
        )
    )
    assert (tmp_path / 'logs' / 'agent_session.resumer').is_file()
    elsewhere = Subprocess().open()  # the scheduler's push and piloop prompt use instances of their own
    elsewhere.prompt(handle, '[piloop] note on run#1 by research#2: stop after the next commit')
    elsewhere.prompt(handle, 'and say so')
    while not (tmp_path / 'turns.log').exists():
        time.sleep(0.05)
    assert 'turn 2' not in (tmp_path / 'turns.log').read_text(), 'the turn in progress gets nothing'
    outcome = rt.wait(handle, timeout=20)
    assert (outcome.status, outcome.error) == (ExitStatus.EXITED, None)
    assert (tmp_path / 'turns.log').read_text() == (
        'turn 1: the brief\nturn 2 of t-42: [piloop] note on run#1 by research#2: stop after the next commit\n'
        'and say so\n'
    ), 'one resumed turn for both messages, one per line'
    assert not (tmp_path / 'logs' / 'queued-messages').exists()
    with pytest.raises(NotSteerable, match='has ended'):
        elsewhere.prompt(handle, 'too late')


def test_a_hook_of_the_harness_takes_the_messages_during_the_turn_and_no_turn_follows(tmp_path: Path) -> None:
    """``PILOOP_MESSAGES`` names the queue: a harness that runs a hook between two tool calls takes them there."""
    rt = Subprocess().open()
    go = tmp_path / 'go'
    os.mkfifo(go)
    exe = script(tmp_path, f'read line < {go}; echo "queue=$PILOOP_MESSAGES" > env.txt')
    handle = rt.start(spec(tmp_path, exe, 'brief', resume=['resume', 'id', '{prompt}']))
    rt.prompt(handle, 'first')
    rt.prompt(handle, 'second')
    queue = tmp_path / 'logs' / 'queued-messages'
    assert take(queue) == ['first', 'second']
    assert take(queue) == [], 'taken once'
    go.write_text('\n')
    assert rt.wait(handle, timeout=10).error is None
    assert (tmp_path / 'env.txt').read_text() == f'queue={queue}\n'
    assert not rt._resume(handle), 'nothing left for a turn of its own'


def test_a_resumed_turn_is_the_one_kill_and_status_reach_from_any_instance(tmp_path: Path) -> None:
    rt = Subprocess().open()
    go = tmp_path / 'go'
    os.mkfifo(go)
    exe = script(tmp_path, f'if [ "$1" = resume ]; then sleep 30; else read line < {go}; fi')
    handle = rt.start(spec(tmp_path, exe, 'brief', resume=['resume', 'a-minted-id', '{prompt}']))
    rt.prompt(handle, 'more')
    assert not rt._resume(handle), 'the first turn still runs'
    go.write_text('\n')  # the first turn ends only now, so the message cannot find the agent ended
    while rt._alive(handle):
        time.sleep(0.05)
    assert rt._resume(handle), 'the launcher filled in the minted session id'
    elsewhere = Subprocess().open()
    assert elsewhere._alive(handle), 'the resumed process, from agent_session.current'
    elsewhere.kill(handle)
    assert not elsewhere._alive(handle) and not rt._alive(handle)


def test_queued_messages_stay_when_the_turn_failed_or_the_session_is_unknown_or_another_instance_waits(
    tmp_path: Path,
) -> None:
    rt = Subprocess().open()
    resume = ['resume', '{session_id}', '{prompt}']
    failed = rt.start(spec(tmp_path, script(tmp_path, 'sleep 0.3; exit 2'), resume=resume))
    rt.prompt(failed, 'x')
    assert rt.wait(failed, timeout=10).exit_code == 2
    assert (tmp_path / 'logs' / 'queued-messages').read_text() == '"x"\n', 'a failed turn is not resumed'

    (tmp_path / 'logs' / 'queued-messages').unlink()
    unknown = rt.start(spec(tmp_path, script(tmp_path, 'sleep 0.3'), pattern='id=(\\w+)', resume=resume))
    rt.prompt(unknown, 'y')
    assert rt.wait(unknown, timeout=10).error is None
    assert (tmp_path / 'logs' / 'queued-messages').exists(), 'no session id to resume'

    (tmp_path / 'logs' / 'queued-messages').write_text('\n')
    blank = rt.start(spec(tmp_path, script(tmp_path, 'true'), resume=['again', '{prompt}']))
    assert rt.wait(blank, timeout=10).error is None, 'an empty queue starts no turn'

    (tmp_path / 'logs' / 'queued-messages').unlink()
    reattached = Subprocess().open()
    other = rt.start(spec(tmp_path, script(tmp_path, 'sleep 0.3'), resume=resume))
    rt.prompt(other, 'z')
    assert reattached.wait(other, timeout=10).error is None, 'only the starting instance knows the environment'
    assert (tmp_path / 'logs' / 'queued-messages').read_text() == '"z"\n'
    rt.wait(other, timeout=10)  # reap it


def test_a_re_attached_agent_refuses_messages_that_no_turn_would_take(tmp_path: Path) -> None:
    """After an in-place restart, the new daemon's runtime cannot resume the agent: it holds no spec, no environment."""
    rt = Subprocess().open()
    handle = rt.start(spec(tmp_path, script(tmp_path, 'sleep 0.5'), resume=['resume', 'id', '{prompt}']))
    rt.prompt(handle, 'before the restart')
    reattached = Subprocess().open()
    waiting = threading.Thread(target=reattached.wait, args=(handle, 10))
    waiting.start()
    while (tmp_path / 'logs' / 'agent_session.resumer').exists():
        time.sleep(0.01)
    with pytest.raises(NotSteerable, match='the daemon that started it was restarted'):
        reattached.prompt(handle, 'after it')
    waiting.join()
    rt.wait(handle, timeout=10)  # reap it


def test_queueing_and_draining_hold_one_lock_so_no_message_falls_between_them(tmp_path: Path) -> None:
    """A message appended while a turn starts is either in that turn's prompt or left for the next one."""
    rt = Subprocess().open()
    handle = rt.start(spec(tmp_path, script(tmp_path, 'sleep 0.5'), resume=['resume', 'id', '{prompt}']))
    queue = tmp_path / 'logs' / 'queued-messages'
    with locked(tmp_path / 'logs'):
        appending = threading.Thread(target=rt.prompt, args=(handle, 'waits for the lock'))
        appending.start()
        appending.join(0.3)
        assert appending.is_alive() and not queue.exists(), 'the append waits while the queue is drained'
    appending.join()
    assert queue.read_text() == '"waits for the lock"\n'
    while rt._alive(handle):
        time.sleep(0.01)
    with locked(tmp_path / 'logs'):
        draining = threading.Thread(target=rt._resume, args=(handle,))
        draining.start()
        draining.join(0.3)
        assert draining.is_alive() and queue.exists(), 'the drain waits while a message is appended'
    draining.join()
    assert not queue.exists()
    rt.wait(handle, timeout=10)


def test_a_start_that_fails_after_the_spawn_kills_and_waits_for_the_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """macOS answered a call after the spawn with ESRCH for a quick agent, and the start failed with its ``Popen``
    never waited for. Python then warned ``subprocess N is still running`` in whatever test collected it."""

    def gone(pid: int) -> float:
        raise ProcessLookupError(3, 'No such process')

    monkeypatch.setattr(subprocess_runtime, 'started', gone)
    spawned: list[subprocess.Popen[bytes]] = []
    real = subprocess_runtime._spawn

    def spawn(spec: AgentSpec, args: list[str], mode: str, token: str) -> subprocess.Popen[bytes]:
        spawned.append(proc := real(spec, args, mode, token))
        return proc

    monkeypatch.setattr(subprocess_runtime, '_spawn', spawn)
    rt = Subprocess().open()
    with pytest.raises(ProcessLookupError):
        rt.start(spec(tmp_path, script(tmp_path, 'sleep 30')))
    assert rt._procs == {}, 'not kept: nothing would ever wait for it'
    assert spawned[0].returncode is not None, 'killed and waited for'


def test_a_start_is_a_spec_once_the_worker_fills_in_where_it_runs(tmp_path: Path) -> None:
    """``AgentStart`` names no place of the machine that made it: ``at`` fills ``{cwd}`` and ``{log_dir}``."""
    start = AgentStart(
        executable='pi',
        args=['--log', '{log_dir}/pi.log', 'Your brief is in {log_dir}/brief.md'],
        env={'PILOOP_ACTOR': 'implementer#1'},
        session_id_pattern='id=(\\S+)',
        resume_args=['--cd', '{cwd}', '{prompt}'],
    )
    spec = start.at(tmp_path / 'wt', tmp_path / 'logs', {'PILOOP_HOME': str(tmp_path)})
    assert spec.args == ['--log', f'{tmp_path}/logs/pi.log', f'Your brief is in {tmp_path}/logs/brief.md']
    assert spec.resume_args == ['--cd', f'{tmp_path}/wt', '{prompt}']
    assert spec.env == {'PILOOP_ACTOR': 'implementer#1', 'PILOOP_HOME': str(tmp_path)}
    filled = {'args', 'resume_args', 'env'}
    assert {f.name: getattr(spec, f.name) for f in fields(AgentStart) if f.name not in filled} == {
        f.name: getattr(start, f.name) for f in fields(AgentStart) if f.name not in filled
    }, 'every other field of the start, as it is'
