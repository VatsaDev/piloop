"""Generate ``docs/snippets/generated/harnesses.mdx``: the built-in harnesses."""

from piloop.harnesses import DEFAULT_HARNESSES
from scripts.generate import SNIPPETS, header

PATH = SNIPPETS / 'harnesses.mdx'


def generate() -> str:
    """Return a table of the built-in harnesses: program, usage reader and MCP renderer."""
    head = ['Harness', 'Executable', 'Usage reader', 'MCP renderer']
    rows = ['| ' + ' | '.join(head) + ' |', '|' + '---|' * len(head)]
    for name, h in DEFAULT_HARNESSES.items():
        usage = f'`{h.usage.reader}` (tally `{h.usage.tally}`)' if h.usage else 'none'
        rows.append(f'| `{name}` | `{h.executable}` | {usage} | `{h.mcp or "none"}` |')
    return header('`DEFAULT_HARNESSES` in `piloop/harnesses.py`') + '\n' + '\n'.join(rows) + '\n'
