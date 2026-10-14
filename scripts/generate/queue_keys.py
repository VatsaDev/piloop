"""Generate ``docs/snippets/generated/queue-keys.mdx``: how each built-in harness takes a message under Herdr."""

from piloop.harnesses import DEFAULT_HARNESSES
from scripts.generate import SNIPPETS, header

PATH = SNIPPETS / 'queue-keys.mdx'


def generate() -> str:
    """Return how each built-in harness takes a message under an interactive runtime, from its ``queue_key``."""
    queued = [f'`{name}` (`{h.queue_key.key}`)' for name, h in DEFAULT_HARNESSES.items() if h.queue_key is not None]
    typed = [f'`{name}`' for name, h in DEFAULT_HARNESSES.items() if h.queue_key is None]
    return header('the `queue_key` of `DEFAULT_HARNESSES` in `piloop/harnesses.py`') + (
        f"\nUnder the `herdr` runtime, piloop types the message into the agent's pane. {_and(typed)} keep what arrives "
        f'while they work for the end of the turn. {_and(queued)} would end the turn to take it, so piloop presses the '
        'queue key of the harness (`[harness.<name>] queue_key`) to queue it.\n'
    )


def _and(items: list[str]) -> str:
    """Return ``items`` as a list in a sentence: ``a``, ``a and b``, ``a, b and c``."""
    return ' and '.join(filter(None, [', '.join(items[:-1]), items[-1]]))
