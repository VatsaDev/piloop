"""Generate ``docs/snippets/generated/settings.mdx``: the environment variables of the global settings."""

from piloop.settings import Settings
from scripts.generate import SNIPPETS, header
from scripts.generate.markdown import environment

PATH = SNIPPETS / 'settings.mdx'


def generate() -> str:
    """Return the table of the environment variables of ``Settings``."""
    return header('the fields of `Settings` in `piloop/settings.py`') + '\n' + environment(Settings)
