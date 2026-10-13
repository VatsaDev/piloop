"""Codex's rollout file: a ``token_count`` event with running totals after every response. The last one is the total.

``{"type": "event_msg", "payload": {"type": "token_count", "info": {"total_token_usage": {input_tokens,
cached_input_tokens, cache_write_input_tokens, output_tokens, total_tokens}}}}``. ``info`` is null when the event
only updates rate limits. The convention is OpenAI's: ``input_tokens`` includes the cached part, so ``total_tokens``
is input plus output. The same numbers are at the end of the stdout of ``codex exec --json`` (``turn.completed``),
but the rollout file exists under both runtimes. Checked against Codex 0.153 files under ``~/.codex/sessions``.
"""

from collections.abc import Iterable

from piloop.usage.jsonl import Counts, Record, add, as_int, child, said, tally, texts, zero


@tally('codex')
def sum_codex(records: Iterable[Record]) -> tuple[Counts, int]:
    """The last ``token_count`` event with an ``info``. The number of turns is the number of these events."""
    counts = zero()
    turns = 0
    for rec in records:
        payload = child(rec, 'payload')
        if rec.get('type') != 'event_msg' or payload is None or payload.get('type') != 'token_count':
            continue
        info = child(payload, 'info')
        usage = child(info, 'total_token_usage') if info is not None else None
        if usage is None:
            continue
        turns += 1
        counts = zero()
        add(
            counts,
            usage,
            input='input_tokens',
            output='output_tokens',
            cache_read='cached_input_tokens',
            cache_write='cache_write_input_tokens',
        )
        counts.total = as_int(usage.get('total_tokens'))
    return counts, turns


@said('codex')
def said_codex(rec: Record) -> str | None:
    """The ``output_text`` blocks of an assistant ``response_item`` message, including text between tool calls.

    ``{"type": "response_item", "payload": {"type": "message", "role": "assistant", "content": [{"type":
    "output_text", "text"}]}}`` in Codex 0.153 rollouts.
    """
    payload = child(rec, 'payload')
    if rec.get('type') != 'response_item' or payload is None or payload.get('role') != 'assistant':
        return None
    return texts(payload.get('content'), 'output_text')
