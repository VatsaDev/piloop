"""The daemon as a real process: SIGTERM, `serve --detach`, `stop`, the pid file, service unit text."""

import os
import plistlib
import signal
import stat
import subprocess
import sys
import time
from pathlib import Path

import pytest
from sqlalchemy import select

from piloop import proc
from piloop.agents.launcher import STOPPED_ERROR
from piloop.daemon.serve import (
    SERVE_ARGV,
    SecretsError,
    _mtime_ns,
    acquire,
    restart,
    running_pid,
    secrets,
    service_unit,
)
from piloop.db.models import Idea, Run, RunStatus, Worker, WorkerState
from piloop.db.session import Database, migrate
from piloop.proc import child_env
from piloop.settings import Settings
from tests.conftest import HARNESS_SLEEPS, hold_lock, register, wait_for_worker_handle, write_toml

PILOOP = [sys.executable, '-m', 'piloop']


def env_for(home: Path) -> dict[str, str]:
    return child_env() | {'PILOOP_HOME': str(home), 'PILOOP_TICK_SECONDS': '0.05', 'COLUMNS': '200'}


def piloop(home: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run([*PILOOP, *args], capture_output=True, text=True, env=env_for(home), timeout=60)


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def wait_until(predicate, *, timeout: float = 15) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, 'condition not met in time'
        time.sleep(0.05)


def test_sigterm_kills_workers_and_exits(project_path: Path, tmp_path: Path) -> None:
    cfg = Settings(home=tmp_path / 'home')
    write_toml(project_path, HARNESS_SLEEPS, worker_timeout=60)
    db = Database(cfg.database_url, actor=cfg.actor)
    migrate(db.engine)
    project = register(db, project_path)
    with db.session() as s:
        s.add(Idea(track='score', project_id=project.id, title='long', hypothesis='long', created_by='t'))
        s.commit()

    with (tmp_path / 'daemon.log').open('w') as log:
        daemon = subprocess.Popen(SERVE_ARGV, stdin=subprocess.DEVNULL, stdout=log, stderr=log, env=env_for(cfg.home))
    handle = wait_for_worker_handle(db)
    child_pid_file = cfg.home / 'runs' / '1' / 'child.pid'
    wait_until(child_pid_file.is_file)
    child_pid = int(child_pid_file.read_text())
    assert int(cfg.pid_file.read_text()) == daemon.pid

    started = time.monotonic()
    daemon.send_signal(signal.SIGTERM)
    assert daemon.wait(timeout=15) == 0
    assert time.monotonic() - started < 5, 'SIGTERM does not wait for the worker timeout'
    assert proc.started(int(str(handle['pid']))) is None and proc.started(child_pid) is None
    with db.session() as s:
        run = s.get_one(Run, 1)
        assert (run.status, run.error) == (RunStatus.FAILED, STOPPED_ERROR)
        assert [(w.state, w.error) for w in s.scalars(select(Worker))] == [(WorkerState.KILLED, STOPPED_ERROR)]
    db.close()
    assert not cfg.pid_file.exists()
    assert 'piloop.shutdown' in (tmp_path / 'daemon.log').read_text()


def test_detach_survives_the_cli_and_stop_ends_it(tmp_path: Path) -> None:
    cfg = Settings(home=tmp_path / 'home')
    started = piloop(cfg.home, 'serve', '--detach')
    assert started.returncode == 0 and started.stdout.rstrip().endswith(f'log {cfg.log_file}')
    wait_until(cfg.pid_file.is_file)
    pid = int(cfg.pid_file.read_text())
    assert started.stdout.startswith(f'daemon started: pid {pid},') and alive(pid)
    assert os.getsid(pid) == pid, 'its own session: closing the terminal cannot reach it'

    again = piloop(cfg.home, 'serve', '--detach')
    foreground = piloop(cfg.home, 'serve')
    assert again.returncode == foreground.returncode == 1
    assert f'already running (pid {pid}' in again.stderr and f'already running (pid {pid}' in foreground.stderr
    assert piloop(cfg.home, 'status').stdout.startswith(f'daemon: pid {pid}, up 0:00:0')

    stopped = piloop(cfg.home, 'stop')
    assert (stopped.returncode, stopped.stdout.strip()) == (0, f'daemon pid {pid} stopped')
    assert not alive(pid) and not cfg.pid_file.exists()
    log = cfg.log_file.read_text()
    assert 'piloop.start' in log and 'piloop.shutdown' in log
    nothing = piloop(cfg.home, 'stop')
    assert (nothing.returncode, nothing.stderr.strip()) == (1, 'no daemon is running')
    assert piloop(cfg.home, 'status').stdout.startswith('daemon: not running')


def test_a_detached_start_whose_secrets_command_fails_fails_too(tmp_path: Path) -> None:
    cfg = Settings(home=tmp_path / 'home')
    env = env_for(cfg.home) | {'PILOOP_SECRETS_COMMAND': 'echo denied >&2; exit 3'}
    started = subprocess.run([*PILOOP, 'serve', '--detach'], capture_output=True, text=True, env=env, timeout=60)
    assert started.returncode == 1 and 'before taking' in started.stderr, 'the child failed before the lock'
    log = cfg.log_file.read_text()
    assert 'denied' in log and 'PILOOP_SECRETS_COMMAND (echo denied >&2; exit 3) failed: exit 3' in log


def test_restart_execs_the_same_process_keeping_its_environment(tmp_path: Path) -> None:
    cfg = Settings(home=tmp_path / 'home')
    env = env_for(cfg.home) | {'PILOOP_TEST_MARKER': 'from-the-unit'}
    started = subprocess.run([*PILOOP, 'serve', '--detach'], capture_output=True, text=True, env=env, timeout=60)
    assert started.returncode == 0, started.stderr
    pid = int(cfg.pid_file.read_text())

    restarted = piloop(cfg.home, 'restart')
    assert (restarted.returncode, restarted.stdout.strip()) == (0, f'daemon pid {pid} restarted')
    assert running_pid(cfg) == pid and alive(pid), 'the same process, so systemd or launchd still holds it'
    assert (proc.environment(pid) or {}).get('PILOOP_TEST_MARKER') == 'from-the-unit'
    log = cfg.log_file.read_text()
    assert log.count('piloop.start') == 2 and log.count('piloop.shutdown') == 1
    assert piloop(cfg.home, 'stop').returncode == 0
    wait_until(lambda: not alive(pid))  # stop returns once the lock is free; the process exits just after
    nothing = piloop(cfg.home, 'restart')
    assert (nothing.returncode, nothing.stderr.strip()) == (1, 'no daemon is running')


def test_restart_gives_up_on_a_holder_that_never_comes_back(tmp_path: Path) -> None:
    cfg = Settings(home=tmp_path / 'home')
    for stubborn in hold_lock(cfg.pid_file, 'stubborn'):
        assert restart(cfg, stubborn.pid, 0.3) is None, 'the signal killed it and no service manager restarted it'
    cfg.pid_file.unlink()
    assert _mtime_ns(cfg) is None


def test_simultaneous_starts_let_exactly_one_daemon_claim(tmp_path: Path) -> None:
    cfg = Settings(home=tmp_path / 'home')
    starts = [
        subprocess.Popen(
            [*PILOOP, 'serve', '--detach'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env_for(cfg.home),
        )
        for _ in range(2)
    ]
    outputs = [p.communicate(timeout=60) for p in starts]
    winner, loser = sorted(zip([p.returncode for p in starts], outputs, strict=True))
    pid = int(cfg.pid_file.read_text())
    assert winner[0] == 0 and winner[1][0].startswith(f'daemon started: pid {pid},')
    assert loser[0] == 1 and f'already running (pid {pid}' in loser[1][1]
    assert piloop(cfg.home, 'stop').returncode == 0 and not alive(pid)


def test_a_free_lock_means_not_running_whatever_the_file_says(tmp_path: Path) -> None:
    cfg = Settings(home=tmp_path)
    assert running_pid(cfg) is None
    sleeper = subprocess.Popen(['sleep', '60'])
    try:
        cfg.pid_file.write_text(str(sleeper.pid))
        assert running_pid(cfg) is None, 'a live pid without the lock is not a daemon'
        cfg.pid_file.write_text('not a pid')
        assert running_pid(cfg) is None
        assert cfg.pid_file.is_file() and sleeper.poll() is None, 'nothing is removed and nothing is signalled'
    finally:
        sleeper.kill()
        sleeper.wait()


def test_the_lock_holder_is_the_daemon(tmp_path: Path) -> None:
    cfg = Settings(home=tmp_path)
    for holder in hold_lock(cfg.pid_file):
        assert running_pid(cfg) == holder.pid
        with pytest.raises(SystemExit, match=f'already running \\(pid {holder.pid}'):
            acquire(cfg)
    assert running_pid(cfg) is None, 'the kernel released the lock with the process'


def test_symlinked_or_foreign_pid_file_is_refused(tmp_path: Path, mocker) -> None:
    cfg = Settings(home=tmp_path)
    target = tmp_path / 'victim.pid'
    target.write_text('1')
    cfg.pid_file.symlink_to(target)
    with pytest.raises(SystemExit, match='serve.pid is a symlink'):
        running_pid(cfg)
    with pytest.raises(SystemExit, match='serve.pid is a symlink'):
        acquire(cfg)
    assert cfg.pid_file.is_symlink() and target.read_text() == '1'
    cfg.pid_file.unlink()
    cfg.pid_file.write_text('1')
    other_uid = os.getuid() + 1
    mocker.patch('os.getuid', return_value=other_uid)
    with pytest.raises(SystemExit, match=f'serve.pid is not owned by uid {other_uid}'):
        running_pid(cfg)
    assert cfg.pid_file.read_text() == '1'


def test_acquire_creates_a_private_home_and_overwrites_a_stale_file(tmp_path: Path) -> None:
    cfg = Settings(home=tmp_path / 'home')
    fd = acquire(cfg)
    assert stat.S_IMODE(cfg.home.stat().st_mode) == 0o700 and stat.S_IMODE(cfg.pid_file.stat().st_mode) == 0o600
    assert cfg.pid_file.read_text() == str(os.getpid()) and running_pid(cfg) == os.getpid()
    os.close(fd)
    assert running_pid(cfg) is None, 'closing the descriptor releases the lock'
    cfg.pid_file.write_text('424242')
    fd = acquire(cfg)
    assert cfg.pid_file.read_text() == str(os.getpid()), 'a stale file is simply overwritten'
    os.close(fd)


def test_service_unit_texts(tmp_path: Path) -> None:
    cfg = Settings(home=tmp_path)
    unit = service_unit(cfg, 'linux')
    assert f'ExecStart={sys.executable} -m piloop serve' in unit
    assert 'Restart=on-failure' in unit and f'Environment="PILOOP_HOME={tmp_path}"' in unit
    assert f'Environment="PATH={os.environ["PATH"]}"' in unit
    awkward = Settings(home=tmp_path / 'my "home" 50% back\\slash')
    assert f'Environment="PILOOP_HOME={tmp_path}/my \\"home\\" 50%% back\\\\slash"' in service_unit(awkward, 'linux')
    plist = plistlib.loads(service_unit(cfg, 'darwin').encode())
    assert plist == {
        'Label': 'dev.piloop.serve',
        'ProgramArguments': SERVE_ARGV,
        'EnvironmentVariables': {'PILOOP_HOME': str(tmp_path), 'PATH': os.environ['PATH']},
        'RunAtLoad': True,
        'KeepAlive': {'SuccessfulExit': False},
        'StandardOutPath': str(tmp_path / 'serve.log'),
        'StandardErrorPath': str(tmp_path / 'serve.log'),
    }
    command = Settings(home=tmp_path, secrets_command="op inject -i '%h/keys'")
    assert 'Environment="PILOOP_SECRETS_COMMAND=op inject -i \'%%h/keys\'"' in service_unit(command, 'linux')
    env = plistlib.loads(service_unit(command, 'darwin').encode())['EnvironmentVariables']
    assert env['PILOOP_SECRETS_COMMAND'] == "op inject -i '%h/keys'", (
        'the unit starts the daemon with its secrets command'
    )


def test_the_secrets_command_prints_name_value_lines() -> None:
    assert secrets("printf 'EXA_API_KEY=e-1\\n\\nX_2=a=b\\n'") == {'EXA_API_KEY': 'e-1', 'X_2': 'a=b'}
    assert secrets('printf A=1') == {'A': '1'}, 'no trailing newline needed'


@pytest.mark.parametrize(
    ('command', 'error'),
    [
        ('exit 3', 'exit 3'),
        ('true', 'printed no NAME=value line'),
        ("printf 'A=hunter2\\nhunter2\\n'", 'line 2 is not NAME=value'),
        ("printf '1A=hunter2\\n'", 'line 1 is not NAME=value'),
        ("printf 'A=\\n'", 'A is empty'),
        ("printf 'A=hunter2 x\\n'", 'A contains whitespace'),
        ("printf 'A=hunter2\\r\\n'", 'A contains whitespace'),
        ("printf 'A=hunter2\\nA=hunter2\\n'", 'A is printed twice'),
    ],
)
def test_a_secrets_command_that_fails_or_prints_anything_else_is_refused_without_its_values(
    command: str, error: str
) -> None:
    with pytest.raises(SecretsError) as e:
        secrets(command)
    assert str(e.value) == error


def test_a_secrets_command_that_cannot_start_or_hangs_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr('piloop.daemon.serve.SECRETS_TIMEOUT_SECONDS', 0.2)
    started = time.monotonic()
    with pytest.raises(SecretsError, match=r'^timed out after 0\.2s$'):
        secrets('sleep 30 & sleep 30')
    assert time.monotonic() - started < 10, 'the whole group is killed, not just sh'
    monkeypatch.setenv('PATH', str(tmp_path))
    with pytest.raises(SecretsError, match='^could not start: '):
        secrets('true')
