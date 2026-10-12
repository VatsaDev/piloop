"""Token usage for a worker row. The launcher reads it when an agent ends, and the scheduler when it closes a row.

When the scheduler closes rows after a daemon stop or crash, the worktree is gone. So the usage spec comes from the
project's current config, and the session file is found by a scan of the store, without a cwd.
"""

from pathlib import Path

from sqlalchemy.orm import Session

from piloop.config import harness_of
from piloop.db.models import Worker
from piloop.usage import Usage, UsageError, read_usage


def usage_of(s: Session, worker: Worker, cwd: Path | None = None) -> Usage | UsageError | None:
    """Return the usage of one ended worker from the files its harness left. Never raises.

    None for a harness without a usage spec. A ``UsageError`` says why nothing could be read, for example a harness
    that was removed from ``piloop.toml``: an old row keeps only the harness name.
    """
    if worker.project_id is None or worker.harness is None:
        return None
    definition = harness_of(s, worker)
    if definition is None:
        return UsageError(f'harness {worker.harness!r} is no longer configured for the project')
    log_dir = Path(worker.log_dir) if worker.log_dir else None
    try:
        return read_usage(definition.usage, worker.session_id, cwd, log_dir)
    except UsageError as e:
        return e
    except Exception as e:  # an unreadable file or a bug in a reader must not stop the row from closing
        return UsageError(f'{type(e).__name__}: {e}')
