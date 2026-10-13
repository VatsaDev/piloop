"""pi's session file: ``message.usage`` on every assistant message, one for each model response.

``{"type": "message", "message": {"role": "assistant", "usage": {input, output, cacheRead, cacheWrite, totalTokens,
cost, …}}}``. The convention is Anthropic's: ``input`` does not include cached tokens, which ``cacheRead`` and
``cacheWrite`` report separately. ``totalTokens`` is pi's own sum of the four. Checked against pi 0.5x files under
``~/.pi/agent/sessions``.
"""

from collections.abc import Iterable

from piloop.usage.jsonl import Counts, Record, add, as_int, child, said, tally, texts, zero


@tally('pi')
def sum_pi(records: Iterable[Record]) -> tuple[Counts, int]:
    """Add up the ``usage`` of every assistant message. ``total`` comes from ``totalTokens``."""
    counts = zero()
    turns = 0
    for rec in records:
        message = child(rec, 'message')
        if rec.get('type') != 'message' or message is None or message.get('role') != 'assistant':
            continue
        usage = child(message, 'usage')
        if usage is None:
            continue
        turns += 1
        add(counts, usage, input='input', output='output', cache_read='cacheRead', cache_write='cacheWrite')
        counts.total += as_int(usage.get('totalTokens'))
    return counts, turns


@said('pi')
def said_pi(rec: Record) -> str | None:
    """The ``text`` blocks of an assistant message. ``toolCall`` and thinking blocks are not text for the human."""
    message = child(rec, 'message')
    if rec.get('type') != 'message' or message is None or message.get('role') != 'assistant':
        return None
    return texts(message.get('content'), 'text')
