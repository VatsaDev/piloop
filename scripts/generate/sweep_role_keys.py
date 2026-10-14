"""Generate ``docs/snippets/generated/sweep-role-keys.mdx``: the keys that a sweeping role adds."""

from piloop.config import RoleConfig, SweepRole
from scripts.generate import SNIPPETS, header
from scripts.generate.markdown import meanings

PATH = SNIPPETS / 'sweep-role-keys.mdx'


def generate() -> str:
    """Return the table of the keys that ``SweepRole`` adds to ``RoleConfig``."""
    keys = [k for k in SweepRole.model_fields if k not in RoleConfig.model_fields]
    return header('the fields of `SweepRole` in `piloop/config.py`') + '\n' + meanings(SweepRole, keys)
