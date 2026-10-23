"""The storage backend capability: where the artifact store mirrors its blobs. Host tier: a backend does I/O.

The contract is ``StorageBackendConfig``: a backend's settings model, whose ``open()`` returns the
``StorageBackend``.
``[storage.backend] kind`` in ``config.toml`` names the provider, and its other keys are the backend's settings.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Protocol

from pydantic import BaseModel, ConfigDict

from piloop.plugins.host import Capability, Scope, Selection


class StorageBackend(Protocol):
    """Where blobs are mirrored: whole compressed objects by key. ``StorageBackendConfig.open`` makes one."""

    url: str

    def upload(self, key: str, path: Path) -> None:
        """Store the file at ``path`` under ``key``, streaming it."""
        ...

    def download(self, key: str, path: Path) -> bool:
        """Write the object under ``key`` to ``path``, streaming it; False when there is none."""
        ...

    def has(self, key: str) -> bool:
        """Whether an object is stored under ``key``."""
        ...


class StorageBackendConfig(BaseModel, ABC):
    """The settings of a storage backend, ``[storage.backend]``: each backend subclasses it with its own keys.

    ``kind`` names the backend (``piloop.plugins.storage_backend.contract.STORAGE_BACKENDS``). ``open`` makes the
    ``StorageBackend`` that the settings describe. ``piloop.plugins.storage_backend.s3.S3Config`` and ``piloop.plugins.storage_backend.gcs.GcsConfig`` are
    the built-in ones.
    """

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    kind: str
    """The backend: the name it is registered under."""

    @abstractmethod
    def open(self) -> StorageBackend:
        """The remote these settings describe."""


STORAGE_BACKENDS = Capability(
    'storage_backend',
    StorageBackendConfig,
    group='piloop.stores',
    selection=Selection(Scope.HOME, 'storage.backend.kind'),
)
"""The settings model of every storage backend by kind."""
