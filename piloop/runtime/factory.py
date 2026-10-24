"""The runtimes of this machine, and the runtime of a launched agent session row."""

from typing import Annotated, Protocol

from pydantic import ConfigDict, TypeAdapter

from piloop.db.models import AgentSession
from piloop.plugins.runtime.contract import RUNTIMES, Handle, Runtime, RuntimeConfig
from piloop.settings import Settings, settings

RUNTIME_TABLES: TypeAdapter[dict[str, RuntimeConfig]] = TypeAdapter(
    Annotated[dict[str, RuntimeConfig], RUNTIMES.tables()], config=ConfigDict(title='[runtimes]')
)


def runtimes(cfg: Settings = settings) -> dict[str, RuntimeConfig]:
    """Every runtime, with its settings from ``[runtimes.<name>]`` of ``config.toml``.

    Raises:
        ValidationError: A table names no runtime, or settings that its runtime refuses.
    """
    return RUNTIME_TABLES.validate_python(cfg.runtimes)


def make_runtime(name: str, cfg: Settings = settings) -> Runtime:
    """Open the runtime called ``name``, with its settings in ``cfg``.

    Raises:
        MissingProvider: No runtime has that name.
    """
    RUNTIMES.get(name)
    return runtimes(cfg)[name].open()


class RuntimeFactory(Protocol):
    """The signature of ``make_runtime``. Tests substitute their own."""

    def __call__(self, name: str, /) -> Runtime:
        """Open the runtime called ``name``."""
        ...


def handle_for(agent_session: AgentSession) -> Handle:
    """Return the runtime address of a launched agent session row. A requested row has none yet."""
    if agent_session.runtime is None:
        raise ValueError(f'agent_session#{agent_session.id} is {agent_session.state}: not launched yet')
    return Handle(agent_session.runtime, agent_session.handle)


def runtime_for(agent_session: AgentSession, factory: RuntimeFactory = make_runtime) -> Runtime:
    """Return the runtime that owns a launched agent session row."""
    return factory(handle_for(agent_session).runtime)
