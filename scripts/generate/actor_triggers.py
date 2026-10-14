"""Generate ``piloop/db/actor_triggers.sql``: the triggers that every connection installs for itself."""

from piloop.db.triggers import temp_sql
from scripts.generate import ROOT

PATH = ROOT / 'piloop' / 'db' / 'actor_triggers.sql'


def generate() -> str:
    """Return the DDL of ``piloop.db.triggers.TEMP``. Every connection runs it."""
    return temp_sql()
