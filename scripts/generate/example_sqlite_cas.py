"""Generate ``docs/snippets/generated/example-sqlite-cas.mdx``: the example plugin ``examples/plugins/sqlite-cas``."""

from scripts.generate import SNIPPETS
from scripts.generate.plugin_examples import snippet

PATH = SNIPPETS / 'example-sqlite-cas.mdx'


def generate() -> str:
    """Return the entry points and the code of the example."""
    return snippet('sqlite-cas', 'piloop_sqlite_cas/__init__.py')
