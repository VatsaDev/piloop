"""The storage backend capability: where the artifact store mirrors its blobs. Host tier: a backend does I/O.

The contract is ``piloop.store.StorageBackendConfig``: a backend's settings model, whose ``open()`` returns the
``StorageBackend``.
``[storage.backend] kind`` in ``config.toml`` names the provider, and its other keys are the backend's settings.
"""

from piloop.plugins.host import Capability, Selection
from piloop.store import StorageBackendConfig


def _check(name: str, loaded: object) -> type[StorageBackendConfig]:
    if isinstance(loaded, type) and issubclass(loaded, StorageBackendConfig):
        return loaded
    raise TypeError('is not a piloop.store.StorageBackendConfig')


STORAGE_BACKENDS = Capability(
    'storage backend', group='piloop.stores', check=_check, selection=Selection('home', 'storage.backend.kind')
)
"""The settings model of every storage backend by kind."""
