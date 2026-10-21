"""What every command module shares: the console, the settings, the session and finding a campaign.

Command modules read ``common.console`` and ``common.settings`` through this module, so that a test replaces each in
one place.
"""

import json
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Annotated

import cyclopts
from rich.console import Console
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.campaign.campaigns import checked_out, find
from piloop.campaign.ops import Refused
from piloop.daemon.serve import running_pid
from piloop.db.models import Campaign, CampaignStatus
from piloop.db.session import (
    BackupFailed,
    Database,
    OlderDaemon,
    SchemaAhead,
    migrate as migrate_schema,
    schema_mismatch,
)
from piloop.reports.feed import PROTOCOL
from piloop.settings import settings

console = Console()

CampaignRef = Annotated[str, cyclopts.Parameter(help='The campaign name, or a path inside its repo.')]
HARNESS_HELP = 'Use this harness instead of the rotation.'
JsonFlag = Annotated[
    bool,
    cyclopts.Parameter(
        name='--json', negative=(), help='Print one JSON document on stdout, for a program. Messages go to stderr.'
    ),
]


def print_json(document: Mapping[str, object]) -> None:
    """Print ``document`` on one line of stdout, with the protocol version first. Rich neither wraps nor colors it.

    Raises:
        ValueError: A float in it is not finite, which JSON cannot say. The reader keeps them out of its documents.
    """
    console.out(json.dumps({'protocol': PROTOCOL, **document}, allow_nan=False), highlight=False)


def json_error(code: str, message: str) -> SystemExit:
    """Print an error document on stdout, and return the exit that also says ``message`` on stderr."""
    print_json({'error': {'code': code, 'message': message}})
    return SystemExit(message)


@contextmanager
def session(*, create: bool = False) -> Generator[Session]:
    """Open the database, which must be at the schema of this piloop (``refuse_other_schema``), and yield one session.

    With ``create`` (``piloop init``), a database that does not exist yet is created at head first.
    """
    db = Database(settings.database_url, actor=settings.actor)
    try:
        if create and not (settings.home / 'piloop.db').is_file():
            migrate(db)
        refuse_other_schema()
        with db.session() as s:
            yield s
    finally:
        db.close()


def campaign(s: Session, ref: str) -> Campaign:
    """Find the campaign that ``ref`` names, or exit with how to register it."""
    found = find(s, ref)
    if found is None:
        raise SystemExit(f'no campaign {ref!r}: register it with `piloop init <path>`')
    return found


def campaigns(s: Session, ref: str | None) -> list[Campaign]:
    """The campaigns for a command without a ref: the one cwd is in, else the only active one, else every campaign."""
    if ref:
        return [campaign(s, ref)]
    if here := checked_out(s, Path.cwd()):
        return [here]
    active = s.scalars(select(Campaign).where(Campaign.status == CampaignStatus.ACTIVE)).all()
    return list(active) if len(active) == 1 else list(s.scalars(select(Campaign)).all())


def refuse_other_schema() -> None:
    """Exit, without a change, unless the database exists and is at the schema of this piloop.

    Only ``piloop serve``, ``piloop migrate`` and ``piloop upgrade`` migrate (``migrate``), and only ``piloop init``
    creates a missing database. Every other command calls this before it opens the database. The message names the
    revision found, the one this piloop knows, and what to run.
    """
    if (e := schema_mismatch(settings.home / 'piloop.db', running_pid(settings))) is not None:
        raise SystemExit(str(e)) from e


def migrate(db: Database) -> Path | None:
    """Migrate the database to head, with a backup first, and return the backup. Create it when it does not exist.

    It exits, without a change, under the running daemon of an older piloop, when the database is ahead of this piloop,
    and when the backup fails.
    """
    try:
        return migrate_schema(db.engine, backups=settings.migration_backups, daemon=running_pid(settings))
    except (OlderDaemon, SchemaAhead, BackupFailed) as e:
        db.close()
        raise SystemExit(str(e)) from e


@contextmanager
def refused() -> Generator[None]:
    """Exit with the message of an operation that refused its input (``piloop.campaign.ops.Refused``)."""
    try:
        yield
    except Refused as e:
        raise SystemExit(str(e)) from e
