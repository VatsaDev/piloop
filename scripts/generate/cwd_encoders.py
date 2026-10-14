"""Generate ``docs/snippets/generated/cwd-encoders.mdx``: the built-in encoders of a working directory."""

from piloop.usage.jsonl import CWD_ENCODERS
from scripts.generate import SNIPPETS, header
from scripts.generate.markdown import registry_table

PATH = SNIPPETS / 'cwd-encoders.mdx'


def generate() -> str:
    """Return the table of the built-in ``cwd`` encoders, from their docstrings."""
    return (
        header('the docstrings of the encoders in `CWD_ENCODERS`, `piloop/usage/jsonl.py`')
        + '\n'
        + registry_table('`cwd`', dict(CWD_ENCODERS))
    )
