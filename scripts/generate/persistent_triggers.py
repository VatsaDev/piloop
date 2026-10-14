"""Generate ``piloop/db/persistent_triggers.sql``: a copy of the triggers that the migrations install."""

from piloop.db.triggers import persistent_sql
from scripts.generate import ROOT

PATH = ROOT / 'piloop' / 'db' / 'persistent_triggers.sql'


def generate() -> str:
    """Return the DDL of ``piloop.db.triggers.PERSISTENT``, so that a trigger change shows up in a review as SQL."""
    return persistent_sql()
