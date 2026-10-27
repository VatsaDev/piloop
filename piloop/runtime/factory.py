"""The runtimes of this machine, and the runtime of a launched agent session row."""

from typing import Protocol

from piloop.db.models import AgentSession
from piloop.plugins.host import MissingProvider
from piloop.plugins.runtime.contract import RUNTIMES, Handle, Runtime, RuntimeConfig
from piloop.settings import Settings, settings


def runtimes(cfg: Settings = settings) -> dict[str, RuntimeConfig]:
    """Every runtime of this machine, by name.

    They are the tables of ``[plugins.runtime.<name>]`` in ``config.toml``, and each runtime that needs no settings.

    Raises:
        ValidationError: A table names no runtime, or settings that its runtime refuses.
    """
    return RUNTIMES.instances(cfg.providers)


def make_runtime(name: str, cfg: Settings = settings) -> Runtime:
    """Open the runtime called ``name``, with its settings in ``cfg``.

    Raises:
        MissingProvider: This machine has no runtime of that name.
        ValidationError: ``config.toml`` does not validate.
    """
    found = runtimes(cfg)
    if name not in found:
        raise MissingProvider(f'no runtime {name!r} on this machine; there are: {", ".join(found)}')
    return found[name].open(name)


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
