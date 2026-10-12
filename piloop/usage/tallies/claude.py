"""Claude Code's session file: ``message.usage`` on ``assistant`` lines, one API response for each ``message.id``.

``{"type": "assistant", "message": {"id", "usage": {input_tokens, output_tokens, cache_read_input_tokens,
cache_creation_input_tokens}}}``. Streaming writes one line for each content block, and each line has the usage of
the whole response, so each ``message.id`` counts once. Without duplicates, the sums match the file's own
``cost-state`` record. The convention is Anthropic's, as for pi: ``input`` does not include cached tokens. The total
is the sum of the four fields. The file has only this session's responses: Claude's side calls (the title, the
summary) are not in it. Checked against Claude Code 2.1 files under ``~/.claude/projects``.
"""

from collections.abc import Iterable

from piloop.usage.jsonl import Counts, Record, add, child, said, tally, texts, zero


@tally('claude')
def sum_claude(records: Iterable[Record]) -> tuple[Counts, int]:
    """Add up the usage of each distinct ``message.id``. ``total`` is the sum of the four fields."""
    counts = zero()
    seen: set[object] = set()
    for rec in records:
        message = child(rec, 'message')
        if rec.get('type') != 'assistant' or message is None:
            continue
        usage = child(message, 'usage')
        if usage is None or message.get('id') in seen:
            continue
        seen.add(message.get('id'))
        add(
            counts,
            usage,
            input='input_tokens',
            output='output_tokens',
            cache_read='cache_read_input_tokens',
            cache_write='cache_creation_input_tokens',
        )
    counts.total = counts.input + counts.output + counts.cache_read + counts.cache_write
    return counts, len(seen)


@said('claude')
def said_claude(rec: Record) -> str | None:
    """The ``text`` blocks of an ``assistant`` line. Thinking and ``tool_use`` blocks are not text for the human."""
    message = child(rec, 'message')
    return texts(message.get('content'), 'text') if rec.get('type') == 'assistant' and message is not None else None
