"""Generate ``docs/snippets/generated/example-no-unit-regression.mdx``: the example plugin ``examples/plugins/no-unit-regression``."""

from scripts.generate import SNIPPETS
from scripts.generate.plugin_examples import snippet

PATH = SNIPPETS / 'example-no-unit-regression.mdx'


def generate() -> str:
    """Return the entry points and the code of the example."""
    return snippet('no-unit-regression', 'piloop_no_unit_regression/__init__.py')
