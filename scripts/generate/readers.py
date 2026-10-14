"""Generate ``docs/snippets/generated/readers.mdx``: the built-in usage readers."""

from piloop.usage import READERS
from scripts.generate import SNIPPETS, header
from scripts.generate.markdown import registry_table

PATH = SNIPPETS / 'readers.mdx'


def generate() -> str:
    """Return the table of the built-in usage readers, from their docstrings."""
    return (
        header('the docstrings of the readers in `READERS`, `piloop/usage/`')
        + '\n'
        + registry_table('Reader', dict(READERS))
    )
