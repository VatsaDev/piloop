"""Generate ``docs/snippets/generated/example-dir-store.mdx``: the example plugin ``examples/plugins/dir-store``."""

from scripts.generate import SNIPPETS
from scripts.generate.plugin_examples import snippet

PATH = SNIPPETS / 'example-dir-store.mdx'


def generate() -> str:
    """Return the entry points and the code of the example."""
    return snippet('dir-store', 'piloop_dir_store/__init__.py')
