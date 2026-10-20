"""The runtime capability: where an agent runs. Host tier: a runtime starts and controls processes.

The contract is the ``Runtime`` protocol (``piloop.runtime``). ``runtime`` in ``piloop.toml``, or a role's
``[roles.<role>] runtime``, names the provider, and ``[runtimes.<name>]`` in ``config.toml`` holds its settings.
"""

from typing import TypeIs

from piloop.harnesses import Invocation
from piloop.plugins.host import Capability, Selection
from piloop.runtime import Runtime


def _is_runtime(loaded: object, name: str) -> TypeIs[type[Runtime]]:
    """Check the class attributes of a runtime. The methods are checked by the type checker of the package."""
    return (
        isinstance(loaded, type)
        and getattr(loaded, 'kind', None) == name
        and isinstance(getattr(loaded, 'invocation', None), Invocation)
    )


def _check(name: str, loaded: object) -> type[Runtime]:
    if _is_runtime(loaded, name):
        return loaded
    raise TypeError(
        f'is not a piloop runtime class: it needs kind = {name!r} and an invocation ({", ".join(Invocation)})'
    )


RUNTIMES = Capability(
    'runtime',
    group='piloop.runtimes',
    check=_check,
    selection=Selection('campaign', 'runtime', per_role=True, settings='runtimes'),
)
"""Every runtime by name. Each entry point of the group names a ``Runtime`` class whose ``kind`` is its name."""
