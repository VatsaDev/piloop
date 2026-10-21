"""The store of a PILOOP_HOME as ``[storage]`` configures it, and the registry of storage backends.

``[storage.backend]`` picks its backend by ``kind``. The backend's settings model (a ``StorageBackendConfig``) owns the
other keys of the table and checks them when the store opens. ``s3`` and ``gcs`` are built in (``piloop.store.s3``,
``piloop.store.gcs``). A package adds a backend with an entry point in the ``piloop.stores`` group: its name is the
``kind``, and it names the ``StorageBackendConfig`` subclass.
"""

from datetime import timedelta

from piloop.plugins.contracts.store import STORAGE_BACKENDS
from piloop.plugins.host import MissingProvider
from piloop.settings import GB, BackendTable, Settings
from piloop.store import StorageBackendConfig, Store


def backend_config(table: BackendTable) -> StorageBackendConfig:
    """Check ``[storage.backend]`` against the settings model of its backend.

    Raises:
        ValueError: No backend has that ``kind``.
        pydantic.ValidationError: The backend refuses a key.
    """
    try:
        backend = STORAGE_BACKENDS.get(table.kind)
    except MissingProvider as e:
        raise ValueError(f'[storage.backend] kind = {table.kind!r}: {e}') from e
    return backend.model_validate(table.model_dump())


def open_store(settings: Settings) -> Store:
    """The store of this PILOOP_HOME, with its remote when ``[storage.backend]`` names one."""
    storage = settings.storage
    return Store(
        settings.store_dir,
        backend_config(storage.backend).open() if storage.backend is not None else None,
        evict_after=timedelta(days=storage.evict_after_days) if storage.evict_after_days is not None else None,
        max_local_bytes=int(storage.local_max_gb * GB) if storage.local_max_gb is not None else None,
    )
