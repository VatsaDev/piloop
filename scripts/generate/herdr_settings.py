"""Generate ``docs/snippets/generated/herdr-settings.mdx``: the environment variables of the Herdr runtime."""

from piloop.plugins.runtime.herdr import HerdrSettings
from scripts.generate import SNIPPETS, header
from scripts.generate.markdown import environment

PATH = SNIPPETS / 'herdr-settings.mdx'


def generate() -> str:
    """Return the table of the environment variables of ``HerdrSettings``."""
    return (
        header('the fields of `HerdrSettings` in `piloop/plugins/runtime/herdr.py`') + '\n' + environment(HerdrSettings)
    )
