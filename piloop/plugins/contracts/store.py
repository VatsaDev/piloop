"""The remote backend capability: where the artifact store mirrors its blobs. Host tier: a backend does I/O.

The contract is ``piloop.store.RemoteConfig``: a backend's settings model, whose ``open()`` returns the ``Remote``.
``[storage.remote] kind`` in ``config.toml`` names the provider, and its other keys are the backend's settings.
"""

from piloop.plugins.host import Capability, Selection
from piloop.store import RemoteConfig


def _check(name: str, loaded: object) -> type[RemoteConfig]:
    if isinstance(loaded, type) and issubclass(loaded, RemoteConfig):
        return loaded
    raise TypeError('is not a piloop.store.RemoteConfig')


REMOTES = Capability(
    'remote backend', group='piloop.stores', check=_check, selection=Selection('home', 'storage.remote.kind')
)
"""The settings model of every remote backend by kind."""
