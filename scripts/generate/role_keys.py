"""Generate ``docs/snippets/generated/role-keys.mdx``: the keys of every role."""

from piloop.config import RoleConfig
from scripts.generate import SNIPPETS, header
from scripts.generate.markdown import meanings

PATH = SNIPPETS / 'role-keys.mdx'


def generate() -> str:
    """Return the table of the keys of every role, from the fields of ``RoleConfig``."""
    return header('the fields of `RoleConfig` in `piloop/config.py`') + '\n' + meanings(RoleConfig)
