"""Token usage of each agent, kept in ``agent_sessions.usage``.

piloop reads the usage from the files that the agent's harness leaves when the agent ends (``Harness.usage``). The
result is a ``Usage``, with the same meaning for every harness, so one ``json_extract`` reads it in SQL and the
harnesses compare. ``input`` is the uncached input, ``cache_read`` and ``cache_write`` are the input read from and
written to the prompt cache. ``total`` is what the harness itself counts as the session's tokens.
"""

from pydantic import BaseModel, ConfigDict


class Usage(BaseModel):
    """The token usage of one agent session, as ``agent_sessions.usage`` stores it. A harness's ``usage`` returns it."""

    model_config = ConfigDict(frozen=True, use_attribute_docstrings=True)

    input: int
    """The input tokens that the prompt cache did not serve."""
    output: int
    cache_read: int
    """The input tokens that the prompt cache served."""
    cache_write: int
    """The input tokens written to the prompt cache."""
    total: int
    """What the harness counts as the session's tokens."""
    turns: int
    """The number of model responses (API calls) in the session."""


class UsageError(RuntimeError):
    """The usage could not be read.

    The message says why: no session id, no file, no usage in the file, or a harness that is no longer configured.
    """


def human(tokens: int) -> str:
    """A short form of a token count: ``1.2M``, ``340k``, ``987``."""
    if tokens >= 1_000_000:
        return f'{tokens / 1_000_000:.1f}M'
    if tokens >= 1_000:
        return f'{tokens / 1_000:.0f}k'
    return str(tokens)
