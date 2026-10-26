"""What the commands that open the home's database share: the session, finding a campaign, the schema check.

Each command that calls the API instead (``piloop.cli.views``) imports none of it.
"""

from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from piloop.api.contract import defaulted
from piloop.campaign.campaigns import UnknownCampaign, find, named, selected
from piloop.campaign.ops import Refused
from piloop.cli import common
from piloop.daemon.serve import running_pid
from piloop.db.models import Campaign
from piloop.db.session import (
    BackupFailed,
    Database,
    OlderDaemon,
    SchemaMismatch,
    migrate as migrate_schema,
    schema_mismatch,
)
from piloop.plugins.cas.contract import CAS, Cas


@contextmanager
def database() -> Generator[Database]:
    """Open the database, which must be at the schema of this piloop (``refuse_other_schema``), until the block ends."""
    db = Database(common.settings.database_url, actor=common.settings.actor)
    try:
        refuse_other_schema()
        yield db
    finally:
        db.close()


@contextmanager
def session() -> Generator[Session]:
    """One session on ``database``."""
    with database() as db, db.session() as s:
        yield s


def campaign(s: Session, ref: str) -> Campaign:
    """Find the campaign that ``ref`` names, or exit with how to register it."""
    try:
        return named(s, ref)
    except UnknownCampaign as e:
        raise SystemExit(str(e)) from e


def known(ref: str) -> None:
    """Exit unless the home has a campaign that ``ref`` names. Reads the database read-only; check its schema first."""
    engine = create_engine(f'sqlite:///file:{common.settings.home / "piloop.db"}?mode=ro&uri=true')
    try:
        with Session(engine) as s:
            found = find(s, ref)
    finally:
        engine.dispose()
    if found is None:
        raise SystemExit(str(UnknownCampaign(ref)))


def campaigns(s: Session, ref: str | None) -> list[Campaign]:
    """The campaigns for a command without a ref: the one cwd is in, else the only active one, else every campaign."""
    try:
        return defaulted(selected(s, ref, Path.cwd()))
    except UnknownCampaign as e:
        raise SystemExit(str(e)) from e


def cas() -> Cas:
    """The CAS of this home, as ``[plugins.cas]`` selects it."""
    return CAS.selected(common.settings.providers).open(common.settings.home)


def refuse_other_schema() -> None:
    """Exit, without a change, unless the database exists and is at the schema of this piloop.

    Only ``piloop serve``, which ``piloop init`` starts, ``piloop migrate`` and ``piloop upgrade`` migrate
    (``migrate``). Every other command calls this before it opens the database. The message names the revision found,
    the one this piloop knows, and what to run.
    """
    if (
        e := schema_mismatch(common.settings.home / 'piloop.db', running_pid(common.settings), common.settings.actor)
    ) is not None:
        raise SystemExit(str(e)) from e


def migrate(db: Database) -> Path | None:
    """Migrate the database to head, with a backup first, and return the backup. Create it when it does not exist.

    It exits, without a change, under the running daemon of an older piloop, when the database is ahead of this piloop,
    and when the backup fails.
    """
    try:
        return migrate_schema(
            db.engine, cas=cas(), backups=common.settings.migration_backups, daemon=running_pid(common.settings)
        )
    except (OlderDaemon, SchemaMismatch, BackupFailed) as e:
        db.close()
        raise SystemExit(str(e)) from e


@contextmanager
def refused() -> Generator[None]:
    """Exit with the message of an operation that refused its input (``piloop.campaign.ops.Refused``)."""
    try:
        yield
    except Refused as e:
        raise SystemExit(str(e)) from e
