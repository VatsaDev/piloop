"""Generate ``docs/snippets/generated/stop.mdx``: the keys of the ``[stop]`` table."""

from piloop.config import Stop
from scripts.generate import SNIPPETS, header
from scripts.generate.markdown import meanings

PATH = SNIPPETS / 'stop.mdx'


def generate() -> str:
    """Return the table of the keys of ``[stop]``, from the fields of ``Stop``."""
    return header('the fields of `Stop` in `piloop/config.py`') + '\n' + meanings(Stop)
