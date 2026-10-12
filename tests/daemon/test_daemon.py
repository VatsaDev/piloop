import os
import re
import signal
import subprocess
from pathlib import Path

import pytest
from inline_snapshot import snapshot
from pytest_mock import MockerFixture
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError

from piloop.agents.launcher import STOPPED_ERROR
from piloop.config import load_project_config
from piloop.daemon.main import ACTOR, Daemon
from piloop.daemon.serve import RESTART_SIGNAL, SERVE_ARGV
from piloop.db.models import (
    Candidate,
    Idea,
    IdeaStatus,
    Project,
    Run,
    RunStatus,
    Verdict,
    Worker,
    WorkerKind,
    WorkerState,
)
from piloop.proc import alive, started
from piloop.settings import Settings
from tests.conftest import HARNESS_NOOP, ScriptRuntime, factory, register, write_toml


def test_daemon_end_to_end(project_path: Path, tmp_path: Path, mocker: MockerFixture) -> None:
    home = tmp_path / 'home'
    runtime = ScriptRuntime()
    daemon = Daemon(Settings(home=home, tick_seconds=0.05))
    mocker.patch.object(daemon.executor.launcher, 'runtime_factory', factory(runtime))
    project = register(daemon.db, project_path)
    with daemon.db.session() as s:
        s.add(Idea(track='score', project_id=project.id, title='ok', hypothesis='ok', created_by='t'))
        s.commit()

    daemon.scheduler.reap()
    assert daemon.scheduler.tick() == [1]
    daemon.scheduler.wait(30)
    with daemon.db.session() as s:
        run = s.get_one(Run, 1)
        assert (run.status, run.updated_by) == (RunStatus.SUCCEEDED, ACTOR)
    assert daemon.scheduler.tick() == [], 'a successful run does not wake the watchdog'

    write_toml(project_path, HARNESS_NOOP)
    with daemon.db.session() as s:
        s.get_one(Project, project.id).config = load_project_config(project_path).model_dump(mode='json')
        s.add(Idea(track='score', project_id=project.id, title='bad', hypothesis='bad', created_by='t', max_runs=1))
        s.commit()
    assert daemon.scheduler.tick() == [2]
    daemon.scheduler.wait(30)
    assert daemon.scheduler.tick() == [3], 'the failure requests a watchdog sweep, launched on the same tick'
    daemon.scheduler.wait(30)
    with daemon.db.session() as s:
        watchdog = s.get_one(Worker, 3)
        assert (watchdog.kind, watchdog.reason, watchdog.state, watchdog.harness) == (
            WorkerKind.WATCHDOG,
            'anomaly: run#2 failed',
            WorkerState.EXITED,
            'script',
        )
        assert watchdog.log_dir == str(home / 'workers' / '3')
    brief = (home / 'workers' / '3' / 'brief.md').read_text()
    assert 'Trigger: anomaly: run#2 failed' in brief and '## This sweep: an incident' in brief
    assert daemon.scheduler.tick() == [], 'one sweep per failure'
    daemon.shutdown(timeout=5)


def test_serve_runs_until_interrupted(tmp_path: Path, mocker: MockerFixture) -> None:
    daemon = Daemon(Settings(home=tmp_path / 'home', tick_seconds=0.01))
    ticks = {'n': 0}

    def tick() -> list[int]:
        ticks['n'] += 1
        if ticks['n'] == 3:
            raise KeyboardInterrupt
        return []

    mocker.patch.object(daemon.scheduler, 'tick', side_effect=tick)
    daemon.serve()
    assert ticks['n'] == 3
    assert not (tmp_path / 'home' / 'serve.pid').exists()


def test_serve_checks_entire_once_and_the_launcher_learns_the_version(
    tmp_path: Path,
    mocker: MockerFixture,
    entire_log,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def serve(cfg: Settings) -> Daemon:
        daemon = Daemon(cfg)
        mocker.patch.object(daemon.scheduler, 'tick', side_effect=KeyboardInterrupt)
        daemon.serve()
        return daemon

    cfg = Settings(home=tmp_path / 'home', tick_seconds=0.01)
    daemon = serve(cfg)
    assert daemon.executor.launcher.entire_version == '0.10.6'
    assert [c['argv'] for c in entire_log()] == [['version']], 'once per start, not per worker'
    assert re.search(
        rf'piloop\.start \d+\.\ds db={re.escape(cfg.database_url)} entire=0\.10\.6', capsys.readouterr().err
    )
    monkeypatch.setenv('FAKE_ENTIRE_VERSION', '0.10.5')
    assert serve(cfg).executor.launcher.entire_version == '0.10.5', 'the launcher skips attach below the floor'
    assert 'entire.too_old version=0.10.5 min=0.10.6' in capsys.readouterr().err
    monkeypatch.setenv('PATH', str(tmp_path))
    assert serve(cfg).executor.launcher.entire_version is None
    assert (
        'entire.missing error=entire is not on PATH; install it: curl -fsSL https://entire.io/install.sh | bash'
        in capsys.readouterr().err
    )


def test_serve_merges_what_the_secrets_command_prints_into_its_environment_before_anything_runs(
    tmp_path: Path, mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv('FAKE_EXA_KEY', 'before-rotation')  # restored, so the merged values leave with the test
    monkeypatch.setenv('FAKE_XAI_KEY', 'unset-by-the-test')
    command = "printf 'FAKE_EXA_KEY=exa-secret\\nFAKE_XAI_KEY=xai-secret\\n'"
    daemon = Daemon(Settings(home=tmp_path / 'home', tick_seconds=0.01, secrets_command=command))
    seen = {}
    mocker.patch.object(daemon.scheduler, 'reap', side_effect=lambda: seen.update(exa=os.environ['FAKE_EXA_KEY']))
    mocker.patch.object(daemon.scheduler, 'tick', side_effect=KeyboardInterrupt)
    daemon.serve()
    assert seen == {'exa': 'exa-secret'}, 'merged before the reap, so before any child starts'
    assert os.environ['FAKE_XAI_KEY'] == 'xai-secret'
    err = capsys.readouterr().err
    assert 'secrets=FAKE_EXA_KEY,FAKE_XAI_KEY' in err, 'the names, on the start span'
    assert 'exa-secret' not in err and 'xai-secret' not in err


def test_a_failing_secrets_command_fails_the_start_naming_the_command(tmp_path: Path, mocker: MockerFixture) -> None:
    cfg = Settings(home=tmp_path / 'home', secrets_command="printf 'EXA_API_KEY=a b'")
    daemon = Daemon(cfg)
    tick = mocker.patch.object(daemon.scheduler, 'tick')
    failed = "PILOOP_SECRETS_COMMAND (printf 'EXA_API_KEY=a b') failed: EXA_API_KEY contains whitespace"
    with pytest.raises(SystemExit, match=f'^{re.escape(failed)}$'):
        daemon.serve()
    assert not tick.called and not cfg.pid_file.exists(), 'nothing ran, the lock is released'


def test_serve_stops_on_sigterm_and_restores_the_handler(tmp_path: Path, mocker: MockerFixture) -> None:
    cfg = Settings(home=tmp_path / 'home', tick_seconds=0.01)
    daemon = Daemon(cfg)
    previous = signal.getsignal(signal.SIGTERM)
    ticks = {'n': 0}

    def tick() -> list[int]:
        ticks['n'] += 1
        assert cfg.pid_file.read_text() == str(os.getpid())
        assert signal.getsignal(signal.SIGTERM) is not previous
        if ticks['n'] == 3:
            os.kill(os.getpid(), signal.SIGTERM)
        return []

    def stop(timeout: float) -> None:
        os.kill(os.getpid(), signal.SIGTERM)  # a second SIGTERM during shutdown must not fire the default action
        real_stop(timeout)

    mocker.patch.object(daemon.scheduler, 'tick', side_effect=tick)
    real_stop = daemon.scheduler.stop
    mocker.patch.object(daemon.scheduler, 'stop', side_effect=stop)
    daemon.serve()
    assert ticks['n'] == 3
    assert signal.getsignal(signal.SIGTERM) is previous
    assert not cfg.pid_file.exists()


def test_the_restart_signal_hands_the_agents_over_then_execs_serve_in_place(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    cfg = Settings(home=tmp_path / 'home', tick_seconds=0.01)
    daemon = Daemon(cfg)
    previous = signal.getsignal(RESTART_SIGNAL)
    execv = mocker.patch('piloop.daemon.main.os.execv')
    shutdown = mocker.spy(daemon, 'shutdown')
    hand_over, stop = mocker.spy(daemon.scheduler, 'hand_over'), mocker.spy(daemon.scheduler, 'stop')

    def tick() -> list[int]:
        assert not execv.called
        os.kill(os.getpid(), RESTART_SIGNAL)
        return []

    mocker.patch.object(daemon.scheduler, 'tick', side_effect=tick)
    daemon.serve()
    assert shutdown.called and not cfg.pid_file.exists(), 'shut down as on SIGTERM, lock released'
    assert hand_over.called and not stop.called, 'its agents are left for the new code'
    assert execv.call_args.args == (SERVE_ARGV[0], SERVE_ARGV)
    assert signal.getsignal(RESTART_SIGNAL) is previous


def test_a_restart_whose_shutdown_fails_still_execs(tmp_path: Path, mocker: MockerFixture) -> None:
    """A restart execs the new code even when its shutdown raises, for example on a locked database.

    Without the exec, the threads of the runs in flight keep the old process and its lock alive. The new daemon's
    ``reap`` fails the runs that the old one could not. A stop on SIGTERM that raises keeps the lock instead.
    """
    cfg = Settings(home=tmp_path / 'home', tick_seconds=0.01)
    daemon = Daemon(cfg)
    execv = mocker.patch('piloop.daemon.main.os.execv')
    mocker.patch.object(daemon.scheduler, 'tick', side_effect=lambda: os.kill(os.getpid(), RESTART_SIGNAL))
    mocker.patch.object(daemon.scheduler, 'hand_over', side_effect=OperationalError('SELECT', {}, Exception('locked')))
    with pytest.raises(OperationalError):
        daemon.serve()
    daemon.db.close()
    assert execv.call_args.args == (SERVE_ARGV[0], SERVE_ARGV)

    execv.reset_mock()
    cfg = Settings(home=tmp_path / 'stopped', tick_seconds=0.01)
    daemon = Daemon(cfg)
    previous = signal.getsignal(signal.SIGTERM)
    mocker.patch.object(daemon.scheduler, 'tick', side_effect=lambda: os.kill(os.getpid(), signal.SIGTERM))
    mocker.patch.object(daemon.scheduler, 'stop', side_effect=OperationalError('INSERT', {}, Exception('locked')))
    with pytest.raises(OperationalError):
        daemon.serve()
    daemon.db.close()
    assert not execv.called and cfg.pid_file.exists(), 'a stop keeps the lock while its run threads may live on'
    assert signal.getsignal(signal.SIGTERM) is previous, 'a second SIGTERM ends what is left'


HARNESS_HELD = (
    '#!/bin/sh\necho a >> solve.txt\ngit commit -qam first\necho $$ > ../agent.pid\n'
    'while [ -f {hold} ]; do sleep 0.05; done\necho b >> solve.txt\ngit commit -qam second\n'
)
"""A worker that commits, says it is under way, waits while the test holds it, then commits again."""


def test_an_in_place_restart_leaves_the_agents_running_and_the_new_daemon_finishes_their_runs(
    project_path: Path, tmp_path: Path, mocker: MockerFixture
) -> None:
    """An in-place restart does not kill the agents, and the new daemon finishes their runs.

    An agent is a process of its own (a Herdr pane or its own session), so it outlives the exec. The new daemon picks
    it up where it is, and the idea does not run again.
    """
    hold = tmp_path / 'hold'
    hold.touch()
    write_toml(project_path, HARNESS_HELD.format(hold=hold), worker_timeout=60)
    cfg = Settings(home=tmp_path / 'home', tick_seconds=0.01)
    first = Daemon(cfg)
    project = register(first.db, project_path)
    with first.db.session() as s:
        s.add(Idea(track='score', project_id=project.id, title='two steps', hypothesis='x', created_by='t', max_runs=1))
        s.commit()
    execv = mocker.patch('piloop.daemon.main.os.execv')
    under_way = cfg.home / 'runs' / '1' / 'agent.pid'
    tick = first.scheduler.tick
    mocker.patch.object(
        first.scheduler,
        'tick',
        side_effect=lambda: os.kill(os.getpid(), RESTART_SIGNAL) if under_way.exists() else tick(),
    )
    first.serve()
    assert execv.called
    agent = int(under_way.read_text())
    start = started(agent)
    assert start is not None and alive(agent, start), 'the agent is still running'
    with first.db.session() as s:
        assert (s.get_one(Run, 1).status, s.get_one(Worker, 1).state) == (RunStatus.RUNNING, WorkerState.RUNNING)
    assert (cfg.home / 'runs' / '1' / 'worktree').is_dir()

    second = Daemon(cfg)
    second.scheduler.reap()
    hold.unlink()
    second.scheduler.wait(30)
    with second.db.session() as s:
        run = s.get_one(Run, 1)
        assert (run.status, run.error, s.get_one(Worker, 1).state) == (RunStatus.SUCCEEDED, None, WorkerState.EXITED)
        made = s.scalars(select(Candidate).where(Candidate.run_id == 1).order_by(Candidate.id)).all()
        assert [(c.summary, c.verdict) for c in made] == [('first', Verdict.KEEP), ('second', Verdict.KEEP)]
        assert s.get_one(Idea, 1).status is IdeaStatus.DONE
        assert s.scalar(select(func.count()).select_from(Run)) == 1, 'the idea never ran again'
    assert not (cfg.home / 'runs' / '1' / 'worktree').exists()
    second.shutdown(timeout=5)
    # In a real restart the exec ends the old thread. Here it lives on, sees its agent end, and leaves the run alone.
    first.scheduler.wait(10)
    first.db.close()


def test_serve_refuses_when_a_daemon_holds_the_pid_file(
    tmp_path: Path, mocker: MockerFixture, lock_holder: subprocess.Popen[str]
) -> None:
    cfg = Settings(home=tmp_path)
    daemon = Daemon(cfg)
    shutdown = mocker.patch.object(daemon, 'shutdown')
    with pytest.raises(SystemExit, match=f'already running \\(pid {lock_holder.pid}'):
        daemon.serve()
    assert not shutdown.called, "refusing must not touch the other daemon's runs"
    assert cfg.pid_file.read_text() == str(lock_holder.pid)


def test_shutdown_fails_open_runs(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    daemon = Daemon(Settings(home=tmp_path / 'home'))
    with daemon.db.session() as s:
        s.add(Project(name='p', path='/p', config={}))
        s.add(Idea(track='score', project_id=1, title='x', hypothesis='x', created_by='t'))
        s.add(Run(idea_id=1, attempt=1, base_candidate_id=1, harness='h', model='m', status=RunStatus.RUNNING))
        s.add(Worker(kind=WorkerKind.RESEARCH, project_id=1, reason='by hand'))
        s.commit()
    daemon.shutdown(timeout=1)
    with daemon.db.session() as s:
        assert (s.get_one(Run, 1).status, s.get_one(Run, 1).error) == (RunStatus.FAILED, STOPPED_ERROR)
        [request] = s.scalars(select(Worker)).all()
        assert request.state is WorkerState.REQUESTED, 'a request survives a restart'
    daemon.db.close()
    assert [line.split(' ', 1)[1] for line in capsys.readouterr().err.splitlines()] == snapshot(
        ['run.aborted run=1 error=daemon stopped', 'piloop.shutdown 0.0s']
    )
