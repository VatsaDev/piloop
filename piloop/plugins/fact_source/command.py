"""``command``: the built-in source that runs a command which writes its values to ``PILOOP_RESULT_FILE``."""

import subprocess
import tempfile
from pathlib import Path
from subprocess import PIPE
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError

from piloop import deprecated
from piloop.plugins.fact_source.contract import FactReadError
from piloop.plugins.fact_source.kit.readings import Readings
from piloop.proc import RESULT_ENV, STDERR_IN_ERROR, ResultPipe, child_env, kill_group
from piloop.scoring.evaluate import parse_metrics

TIMEOUT = 60
"""The seconds that the command can take. The daemon's reading waits for it, so a hung command fails."""


class Reading(BaseModel):
    """What the command writes to ``PILOOP_RESULT_FILE``, as one line of JSON: ``{"metrics": {"spend": 412.3}}``."""

    model_config = ConfigDict(extra='forbid', frozen=True)

    metrics: dict[str, Annotated[float, Field(strict=True, allow_inf_nan=False)]]
    """The value of each name of ``metrics``."""


READING = TypeAdapter(Reading)


class Command(Readings):
    """``command``: run ``command`` at the repo root with the daemon's environment, which is where its secrets come from.

    It writes one line of JSON to the pipe that ``PILOOP_RESULT_FILE`` names, with a value for each name of
    ``metrics``, for example ``{"metrics": {"spend": 412.3}}``. It must exit with code 0 within one minute.
    """

    command: list[str] = Field(min_length=1)
    """The command (argv). Example: ``["uv", "run", "scripts/spend.py"]``."""
    metrics: list[str] = Field(min_length=1)
    """The names that it reports, each a fact of the campaign. Example: ``["spend"]``."""

    def names(self) -> tuple[str, ...]:
        return tuple(self.metrics)

    def read(self, repo: Path) -> dict[str, float]:
        """Run the command and read its result.

        Raises:
            FactReadError: The command did not start, timed out, exited non-zero, or wrote a result that is not one
                ``Reading`` with exactly the names of ``metrics``.
        """
        with tempfile.TemporaryDirectory() as tmp, ResultPipe(Path(tmp) / 'result') as results:
            try:
                proc = subprocess.Popen(
                    self.command,
                    cwd=repo,
                    env=child_env() | {RESULT_ENV: str(results.path)},
                    stdout=PIPE,
                    stderr=PIPE,
                    start_new_session=True,
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
        found = self._found(results.close(), out.decode(errors='replace'), self.metrics)
        if missing := [name for name in self.metrics if name not in found]:
            raise FactReadError(f'reported no value for: {", ".join(missing)}')
        if unknown := sorted(found.keys() - set(self.metrics)):
            raise FactReadError(f'reported {", ".join(unknown)}, which metrics does not name')
        return found

    @staticmethod
    def _found(lines: list[str], stdout: str, names: list[str]) -> dict[str, float]:
        """The values of its one result line, or, until 0.4.0, those of ``names`` among its ``METRIC`` lines.

        Raises:
            FactReadError: It wrote no result, more than one, one that is not a ``Reading``, or both kinds.
        """
        printed = parse_metrics(stdout)
        match lines:
            case [] if printed:
                deprecated.until('0.4.0', 'METRIC lines of the command fact source: delete this branch')
                return {name: printed[name] for name in names if name in printed}
            case []:
                raise FactReadError(f'wrote no result to {RESULT_ENV}')
            case [_] if printed:
                raise FactReadError(f'wrote a result to {RESULT_ENV} and printed METRIC lines: drop the METRIC lines')
            case [line]:
                try:
                    return dict(READING.validate_json(line).metrics)
                except ValidationError as e:
                    raise FactReadError(f'wrote a result that is not {{"metrics": {{"<name>": <number>}}}}: {e}') from e
            case _:
                raise FactReadError(f'wrote {len(lines)} results to {RESULT_ENV}; one reading writes one line')
