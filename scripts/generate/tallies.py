"""Generate ``docs/snippets/generated/tallies.mdx``: the built-in tallies of a usage reader."""

import sys

from piloop.usage.jsonl import TALLIES
from scripts.generate import SNIPPETS, header
from scripts.generate.markdown import registry_table

PATH = SNIPPETS / 'tallies.mdx'


def generate() -> str:
    """Return the table of the built-in tallies, from the docstrings of their modules."""
    modules: dict[str, object] = {n: sys.modules[t.__module__] for n, t in TALLIES.items()}
    return (
        header('the docstrings of the modules of `TALLIES`, `piloop/usage/`') + '\n' + registry_table('Tally', modules)
    )
