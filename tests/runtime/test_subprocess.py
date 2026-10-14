import os
import stat
import threading
import time
from pathlib import Path

import pytest
from dirty_equals import IsPositive
from inline_snapshot import snapshot

from piloop.proc import started
from piloop.runtime import Handle, NotSteerable, WorkerSpec, WorkerStatus, subprocess as subprocess_runtime
from piloop.runtime.subprocess import SubprocessRuntime


def script(tmp_path: Path, body: str) -> Path:
    path = tmp_path / 'worker.sh'
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
) -> WorkerSpec:
    return WorkerSpec(
        name='w1',
        label='w1',
        executable=str(executable),
        args=list(args),
        prompt='the brief',
        cwd=tmp_path,
        log_dir=tmp_path / 'logs',
        env=env or {},
        session_id_pattern=pattern,
        resume_args=resume or [],
    )


def test_runs_to_completion_and_captures_output(tmp_path: Path) -> None:
    rt = SubprocessRuntime()
    exe = script(tmp_path, 'echo "prompt=$1 var=$PILOOP_TEST"; echo oops >&2; exit 3')
    handle = rt.start(spec(tmp_path, exe, 'the brief', env={'PILOOP_TEST': 'yes'}))
    assert handle.runtime == 'subprocess'
    assert handle.data == {
        'pid': IsPositive(),
        'pgid': handle.data['pid'],
        'start_time': IsPositive(),
        'started_at': IsPositive(),
        'log_dir': str(tmp_path / 'logs'),
        'session_id_pattern': None,
    }
    outcome = rt.wait(handle, timeout=10)
    assert (outcome.status, outcome.exit_code, outcome.error) == (WorkerStatus.EXITED, 3, 'worker exit 3')
    assert outcome.seconds == IsPositive()
    assert rt.status(handle) is WorkerStatus.EXITED
    assert rt.read(handle) == snapshot('prompt=the brief var=yes')
    assert (tmp_path / 'logs' / 'worker.stderr').read_text() == 'oops\n'


def test_timeout_kills_the_whole_process_group(tmp_path: Path) -> None:
    rt = SubprocessRuntime()
    exe = script(tmp_path, 'sleep 30 &\necho $! > child.pid\nwait')
    handle = rt.start(spec(tmp_path, exe))
    assert rt.status(handle) is WorkerStatus.WORKING
    outcome = rt.wait(handle, timeout=0.5)
    assert (outcome.status, outcome.exit_code, outcome.error) == (
        WorkerStatus.TIMED_OUT,
        None,
        'worker timeout after 0.5s',
    )
    child = int((tmp_path / 'child.pid').read_text())
    time.sleep(0.2)
    assert started(child) is None, 'the sleeping grandchild must die with the group'


def test_session_id_is_read_from_stdout_while_the_process_still_runs(tmp_path: Path) -> None:
    rt = SubprocessRuntime()
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
    rt = SubprocessRuntime()
    handle = rt.start(spec(tmp_path, script(tmp_path, 'sleep 5')))
    with pytest.raises(NotSteerable, match='its harness has no resume arguments, or the daemon'):
        rt.prompt(handle, 'hi')
    rt.kill(handle)
    rt.kill(handle)
    assert rt.status(handle) is WorkerStatus.EXITED


def test_no_jobs_are_reported(tmp_path: Path) -> None:
    """A one-shot process is not watched for what it leaves running, so no worker of it is ever ``silent``."""
    rt = SubprocessRuntime()
    handle = rt.start(spec(tmp_path, script(tmp_path, 'sleep 5 &\nwait')))
    assert rt.jobs(handle) == []
    rt.kill(handle)


def test_rehydrated_handle_is_observable_without_an_exit_code(tmp_path: Path) -> None:
    starter = SubprocessRuntime()
    handle = starter.start(spec(tmp_path, script(tmp_path, 'sleep 5')))
    other = SubprocessRuntime()  # another process would rebuild the handle from the database
    rehydrated = Handle(handle.runtime, dict(handle.data))
    assert other.status(rehydrated) is WorkerStatus.WORKING
    other.kill(rehydrated)
    starter.wait(handle, timeout=5)
    assert other.status(rehydrated) is WorkerStatus.EXITED
    assert other.wait(rehydrated, timeout=1).exit_code is None
    assert other.read(Handle('subprocess', {**handle.data, 'log_dir': str(tmp_path / 'nope')})) == ''


def test_reused_pid_is_not_mistaken_for_the_worker(tmp_path: Path) -> None:
    rt = SubprocessRuntime()
    handle = rt.start(spec(tmp_path, script(tmp_path, 'true')))
    rt.wait(handle, timeout=5)
    impostor = Handle('subprocess', {**handle.data, 'pid': os.getpid(), 'start_time': -1.0})
    assert SubprocessRuntime().status(impostor) is WorkerStatus.EXITED


def test_kill_tolerates_a_vanished_group(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    rt = SubprocessRuntime()
    handle = rt.start(spec(tmp_path, script(tmp_path, 'sleep 5')))

    def gone(*_: object) -> None:
        raise ProcessLookupError

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
    rt = SubprocessRuntime()
    handle = rt.start(spec(tmp_path, script(tmp_path, 'exit 0')))
    assert handle.data['pgid'] == handle.data['pid']
    assert rt.wait(handle, timeout=10).status is WorkerStatus.EXITED


TURNS = """
if [ "$1" = resume ]; then echo "turn 2 of $2: $3" >> turns.log; exit 0; fi
echo '{"thread_id":"t-42"}'
echo "turn 1: $1" >> turns.log
sleep 1
"""


def test_a_message_waits_for_the_turn_to_end_then_resumes_the_session_with_it(tmp_path: Path) -> None:
    """The subprocess runtime queues as Herdr does: nothing reaches the turn in progress. When the process exits,
    the harness runs again with ``resume_args``: the same session, the queued messages as its prompt."""
    rt = SubprocessRuntime()
    exe = script(tmp_path, TURNS)
    handle = rt.start(
        spec(
            tmp_path, exe, 'the brief', pattern=r'"thread_id":"([^"]+)"', resume=['resume', '{session_id}', '{prompt}']
        )
    )
    assert (tmp_path / 'logs' / 'worker.resumer').is_file()
    elsewhere = SubprocessRuntime()  # the scheduler's push and piloop prompt use instances of their own
    elsewhere.prompt(handle, '[piloop] note on run#1 by research#2: stop after the next commit')
    elsewhere.prompt(handle, 'and say so')
    while not (tmp_path / 'turns.log').exists():
        time.sleep(0.05)
    assert 'turn 2' not in (tmp_path / 'turns.log').read_text(), 'the turn in progress gets nothing'
    outcome = rt.wait(handle, timeout=20)
    assert (outcome.status, outcome.error) == (WorkerStatus.EXITED, None)
    assert (tmp_path / 'turns.log').read_text() == (
        'turn 1: the brief\nturn 2 of t-42: [piloop] note on run#1 by research#2: stop after the next commit\n'
        'and say so\n'
    ), 'one resumed turn for both messages, one per line'
    assert not (tmp_path / 'logs' / 'queued-messages').exists()
    with pytest.raises(NotSteerable, match='has ended'):
        elsewhere.prompt(handle, 'too late')


def test_a_resumed_turn_is_the_one_kill_and_status_reach_from_any_instance(tmp_path: Path) -> None:
    rt = SubprocessRuntime()
    exe = script(tmp_path, 'if [ "$1" = resume ]; then sleep 30; fi')
    handle = rt.start(spec(tmp_path, exe, 'brief', resume=['resume', 'a-minted-id', '{prompt}']))
    rt.prompt(handle, 'more')
    assert not rt._resume(handle), 'the first turn still runs'
    while rt._alive(handle):
        time.sleep(0.05)
    assert rt._resume(handle), 'the launcher filled in the minted session id'
    elsewhere = SubprocessRuntime()
    assert elsewhere.status(handle) is WorkerStatus.WORKING, 'the resumed process, from worker.current'
    elsewhere.kill(handle)
    assert elsewhere.status(handle) is WorkerStatus.EXITED and rt.status(handle) is WorkerStatus.EXITED


def test_queued_messages_stay_when_the_turn_failed_or_the_session_is_unknown_or_another_instance_waits(
    tmp_path: Path,
) -> None:
    rt = SubprocessRuntime()
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
    reattached = SubprocessRuntime()
    other = rt.start(spec(tmp_path, script(tmp_path, 'sleep 0.3'), resume=resume))
    rt.prompt(other, 'z')
    assert reattached.wait(other, timeout=10).error is None, 'only the starting instance knows the environment'
    assert (tmp_path / 'logs' / 'queued-messages').read_text() == '"z"\n'
    rt.wait(other, timeout=10)  # reap it


def test_a_re_attached_agent_refuses_messages_that_no_turn_would_take(tmp_path: Path) -> None:
    """After an in-place restart, the new daemon's runtime cannot resume the agent: it holds no spec, no environment."""
    rt = SubprocessRuntime()
    handle = rt.start(spec(tmp_path, script(tmp_path, 'sleep 0.5'), resume=['resume', 'id', '{prompt}']))
    rt.prompt(handle, 'before the restart')
    reattached = SubprocessRuntime()
    waiting = threading.Thread(target=reattached.wait, args=(handle, 10))
    waiting.start()
    while (tmp_path / 'logs' / 'worker.resumer').exists():
        time.sleep(0.01)
    with pytest.raises(NotSteerable, match='the daemon that started it was restarted'):
        reattached.prompt(handle, 'after it')
    waiting.join()
    rt.wait(handle, timeout=10)  # reap it


def test_queueing_and_draining_hold_one_lock_so_no_message_falls_between_them(tmp_path: Path) -> None:
    """A message appended while a turn starts is either in that turn's prompt or left for the next one."""
    rt = SubprocessRuntime()
    handle = rt.start(spec(tmp_path, script(tmp_path, 'sleep 0.5'), resume=['resume', 'id', '{prompt}']))
    queue = tmp_path / 'logs' / 'queued-messages'
    with subprocess_runtime._locked(tmp_path / 'logs'):
        appending = threading.Thread(target=rt.prompt, args=(handle, 'waits for the lock'))
        appending.start()
        appending.join(0.3)
        assert appending.is_alive() and not queue.exists(), 'the append waits while the queue is drained'
    appending.join()
    assert queue.read_text() == '"waits for the lock"\n'
    while rt._alive(handle):
        time.sleep(0.01)
    with subprocess_runtime._locked(tmp_path / 'logs'):
        draining = threading.Thread(target=rt._resume, args=(handle,))
        draining.start()
        draining.join(0.3)
        assert draining.is_alive() and queue.exists(), 'the drain waits while a message is appended'
    draining.join()
    assert not queue.exists()
    rt.wait(handle, timeout=10)
