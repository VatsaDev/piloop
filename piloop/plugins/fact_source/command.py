"""``command``: the built-in source that runs a command which prints ``METRIC <name>=<value>`` lines."""

import subprocess
from pathlib import Path
from subprocess import PIPE

from pydantic import Field

from piloop.plugins.fact_source.contract import FactReadError
from piloop.plugins.fact_source.kit.readings import Readings
from piloop.proc import STDERR_IN_ERROR, child_env, kill_group
from piloop.scoring.evaluate import parse_metrics

TIMEOUT = 60
"""The seconds that the command can take. The daemon's reading waits for it, so a hung command fails."""


class Command(Readings):
    """``command``: run ``command`` at the repo root with the daemon's environment, which is where its secrets come from.

    It prints one ``METRIC <name>=<value>`` line for each name of ``metrics``, as the evaluate command does, for example
    ``METRIC spend=412.3``. It must exit with code 0 within one minute.
    """

    command: list[str] = Field(min_length=1)
    """The command (argv). Example: ``["uv", "run", "scripts/spend.py"]``."""
    metrics: list[str] = Field(min_length=1)
    """The names that it prints, each a fact of the campaign. Example: ``["spend"]``."""

    def names(self) -> tuple[str, ...]:
        return tuple(self.metrics)

    def read(self, repo: Path) -> dict[str, float]:
        """Run the command and parse its ``METRIC`` lines.

        Raises:
            FactReadError: The command did not start, timed out, exited non-zero, or printed no line for a name.
        """
        try:
            proc = subprocess.Popen(
                self.command, cwd=repo, env=child_env(), stdout=PIPE, stderr=PIPE, start_new_session=True
            )
        except OSError as e:
            raise FactReadError(f'could not start: {e}') from e
        with proc:
            try:
                out, err = proc.communicate(timeout=TIMEOUT)
            except subprocess.TimeoutExpired as e:
                # Kill the children too. A child that holds the pipe open blocks the read below.
                kill_group(proc)
                proc.communicate()
                raise FactReadError(f'timed out after {TIMEOUT}s') from e
        if proc.returncode != 0:
            raise FactReadError(f'exit {proc.returncode}: {err.decode(errors="replace").strip()[-STDERR_IN_ERROR:]}')
        found = parse_metrics(out.decode(errors='replace'))
        if missing := [name for name in self.metrics if name not in found]:
            raise FactReadError(f'printed no METRIC line for: {", ".join(missing)}')
        return {name: found[name] for name in self.metrics}
