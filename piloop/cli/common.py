"""What every command module shares: the console, the settings, the session and finding a project.

Command modules read ``common.console`` and ``common.settings`` through this module, so that a test replaces each in
one place.
"""

from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Annotated

import cyclopts
from rich.console import Console
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.campaign.ops import Refused
from piloop.campaign.project import checked_out, find
from piloop.db.models import Project, ProjectStatus
from piloop.db.session import Database, SchemaAhead, migrate as migrate_schema
from piloop.settings import settings

console = Console()

ProjectRef = Annotated[str, cyclopts.Parameter(help='The project name, or a path inside its repo.')]
HARNESS_HELP = 'Use this harness instead of the rotation.'


@contextmanager
def session() -> Generator[Session]:
    """Open and migrate the database, and yield one session for the command."""
    db = Database(settings.database_url, actor=settings.actor)
    migrate(db)
    try:
        with db.session() as s:
            yield s
    finally:
        db.close()


def project(s: Session, ref: str) -> Project:
    """Find the project that ``ref`` names, or exit with how to register it."""
    found = find(s, ref)
    if found is None:
        raise SystemExit(f'no project {ref!r}: register it with `piloop init <path>`')
    return found


def projects(s: Session, ref: str | None) -> list[Project]:
    """The projects for a command without a ref: the one cwd is in, else the only active one, else every project."""
    if ref:
        return [project(s, ref)]
    if here := checked_out(s, Path.cwd()):
        return [here]
    active = s.scalars(select(Project).where(Project.status == ProjectStatus.ACTIVE)).all()
    return list(active) if len(active) == 1 else list(s.scalars(select(Project)).all())


def migrate(db: Database) -> None:
    """Migrate the database to head, or close it and exit when its schema is ahead of this piloop."""
    try:
        migrate_schema(db.engine)
    except SchemaAhead as e:
        db.close()
        raise SystemExit(str(e)) from e


@contextmanager
def refused() -> Generator[None]:
    """Exit with the message of an operation that refused its input (``piloop.campaign.ops.Refused``)."""
    try:
        yield
    except Refused as e:
        raise SystemExit(str(e)) from e
