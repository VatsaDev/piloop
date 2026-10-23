"""The runtime registry, and the runtime of a launched agent session row."""

from typing import Protocol

from piloop.db.models import AgentSession
from piloop.plugins.runtime.contract import RUNTIMES, Handle, Runtime


def make_runtime(kind: str) -> Runtime:
    """Create the runtime called ``kind``."""
    return RUNTIMES.get(kind).model()


class RuntimeFactory(Protocol):
    """The signature of ``make_runtime``. Tests substitute their own."""

    def __call__(self, kind: str, /) -> Runtime:
        """Create the runtime called ``kind``."""
        ...


def handle_for(agent_session: AgentSession) -> Handle:
    """Return the runtime address of a launched agent session row. A requested row has none yet."""
    if agent_session.runtime is None:
        raise ValueError(f'agent_session#{agent_session.id} is {agent_session.state}: not launched yet')
    return Handle(agent_session.runtime, agent_session.handle)


def runtime_for(agent_session: AgentSession, factory: RuntimeFactory = make_runtime) -> Runtime:
    """Return the runtime that owns a launched agent session row."""
    return factory(handle_for(agent_session).runtime)
