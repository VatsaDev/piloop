"""The commands about the install: ``upgrade``, ``doctor`` and ``install-skills``."""

import shlex
import subprocess
from typing import Annotated

import cyclopts

from piloop.cli import common, doctor as doctor_module, skills
from piloop.cli.daemon import restarted
from piloop.cli.upgrade import InstallError, requirement, source
from piloop.daemon.serve import running_pid
from piloop.proc import PILOOP_ARGV, child_env
from piloop.version import installed


def upgrade(
    *, timeout: Annotated[float, cyclopts.Parameter(help='Seconds to wait for a running daemon to restart.')] = 30
) -> None:
    """Upgrade the CLI, the skills and the database to the latest version, and restart a running daemon.

    It reads the source that uv installed the CLI from (the uv receipt), and runs
    `uv tool upgrade --reinstall-package piloop piloop`. Then the new code runs `piloop install-skills`. A running
    daemon restarts in place, as with `piloop restart`: its agents keep running, and the new code migrates the
    database and re-attaches to them. When no daemon runs, the new code migrates the database and the daemon stays
    stopped. A checkout installed with `uv sync` is not a uv tool: upgrade it with `git pull`.
    """
    try:
        req = requirement(common.settings.uv_tool_dir)
    except InstallError as e:
        raise SystemExit(str(e)) from e
    before = installed().removeprefix('piloop ')
    common.console.print(f'CLI: {before}, installed by uv from {source(req)}')
    # --reinstall-package: without it, uv serves a directory install (PILOOP_SRC=<checkout>) from its cached build
    _run(['uv', 'tool', 'upgrade', '--reinstall-package', 'piloop', 'piloop'])
    after = _new_binary('--version').stdout.strip()
    common.console.print(f'CLI: {before} → {after.removeprefix("piloop ")}')
    common.console.print(_new_binary('install-skills').stdout.rstrip(), markup=False, highlight=False, soft_wrap=True)
    pid = running_pid(common.settings)
    if pid is not None:
        # The restart execs the new code in the daemon's process: a new code that cannot start would leave its agents
        # without a daemon, so check it first and keep the old daemon running.
        can_start = _new_binary('serve', '--check', check=False)
        if can_start.returncode:
            raise SystemExit(
                f'daemon: pid {pid} not restarted, and it keeps running the old code: {can_start.stderr.strip()}.'
                ' Fix it, then `piloop restart`'
            )
        common.console.print(
            f'daemon: restarting pid {pid}; its running agents keep going and the new code re-attaches'
        )
        common.console.print(f'daemon: pid {restarted(pid, timeout)} runs the new code')
    # this process runs the old code and knows only the old migrations, so the new code (or daemon) migrates
    common.console.print(_new_binary('migrate').stdout.rstrip(), markup=False, highlight=False, soft_wrap=True)
    if pid is None:
        common.console.print('daemon: not running, left stopped')
    checked = _new_binary('doctor', check=False)
    # its stderr has the deprecation warnings of the piloop.toml files it loaded; they matter most after an upgrade
    common.console.print((checked.stdout + checked.stderr).rstrip(), markup=False, highlight=False, soft_wrap=True)
    if checked.returncode:
        raise SystemExit('piloop doctor found problems after the upgrade: each FAIL above says how to fix it')


def _new_binary(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    """Run the newly installed piloop. uv rebuilds the venv in place, so ``sys.executable`` now imports the new code.

    With ``check``, a failure of the child exits this command too. For example, the database can still be ahead of
    the code that the upgrade fetched. The child sees the same homes as this process.
    """
    homes = {
        'PILOOP_HOME': common.settings.home,
        'UV_TOOL_DIR': common.settings.uv_tool_dir,
        'PILOOP_AGENT_SKILLS_DIR': common.settings.agent_skills_dir,
        **({'PILOOP_CLAUDE_SKILLS_DIR': claude} if (claude := common.settings.claude_skills_dir) else {}),
    }
    try:
        return subprocess.run(
            [*PILOOP_ARGV, *args],
            capture_output=True,
            text=True,
            check=check,
            env=child_env() | {k: str(v) for k, v in homes.items()},
        )
    except subprocess.CalledProcessError as e:
        raise SystemExit(
            f'piloop {shlex.join(args)} on the new code failed with exit {e.returncode}: {e.stderr.strip()}'
        ) from e


def doctor_cmd() -> None:
    """Check the install without writing anything, and say how to fix each failure.

    It notes a newer piloop at the source of the CLI, and checks that the daemon runs the installed code, the database
    is at head, each piloop.toml loads, the MCP servers can start, the harnesses are on PATH and have the skills of
    this CLI, and Herdr answers. The commands it names use PILOOP_CLI.
    """
    results = doctor_module.checks(common.settings)
    common.console.print('\n'.join(doctor_module.report(results)), markup=False, highlight=False, soft_wrap=True)
    if any(c.fix is not None for c in results):
        raise SystemExit(1)


def install_skills() -> None:
    """Install the orchestrator skills for every harness on PATH.

    Claude Code reads them from ~/.claude/skills (or $CLAUDE_CONFIG_DIR/skills). Codex, pi, Gemini CLI and Cursor read
    them from ~/.agents/skills. PILOOP_CLAUDE_SKILLS_DIR and PILOOP_AGENT_SKILLS_DIR install them elsewhere, for a
    second install such as a build of main next to a release. It replaces piloop's own skills there and nothing else. A running harness loads them
    at its next session. The installer and `piloop upgrade` run this command.
    """
    found = skills.targets(common.settings)
    for target, harnesses in found.items():
        skills.install(target)
        line = f'skills: {", ".join(skills.bundled())} → {target} ({", ".join(harnesses)})'
        common.console.print(line, markup=False, highlight=False, soft_wrap=True)
    if not found:
        common.console.print(
            f'skills: none of {", ".join(skills.SKILL_DIRS)} is on PATH, nothing installed', soft_wrap=True
        )


def _run(argv: list[str]) -> None:
    """Run a tool with its output on this terminal. Exit with an error when it fails."""
    common.console.print(f'$ {shlex.join(argv)}', style='dim')
    try:
        rc = subprocess.run(argv, env=child_env()).returncode
    except FileNotFoundError as e:
        raise SystemExit(f'{argv[0]} is not on PATH') from e
    if rc:
        raise SystemExit(f'{shlex.join(argv)} failed with exit {rc}')
