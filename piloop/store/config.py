"""The store of a PILOOP_HOME as ``[storage]`` configures it, and the registry of remote backends.

``[storage.remote]`` picks its backend by ``kind``. The backend's settings model (a ``RemoteConfig``) owns the other
keys of the table and checks them when the store opens. ``s3`` and ``gcs`` are built in (``piloop.store.s3``,
``piloop.store.gcs``). A package adds a backend with an entry point in the ``piloop.stores`` group: its name is the
``kind``, and it names the ``RemoteConfig`` subclass.
"""

from datetime import timedelta

from piloop.plugins.contracts.store import REMOTES
from piloop.plugins.host import MissingProvider
from piloop.settings import GB, RemoteTable, Settings
from piloop.store import RemoteConfig, Store


def remote_config(table: RemoteTable) -> RemoteConfig:
    """Check ``[storage.remote]`` against the settings model of its backend.

    Raises:
        ValueError: No backend has that ``kind``.
        pydantic.ValidationError: The backend refuses a key.
    """
    try:
        backend = REMOTES.get(table.kind)
    except MissingProvider as e:
        raise ValueError(f'[storage.remote] kind = {table.kind!r}: {e}') from e
    return backend.model_validate(table.model_dump())


def open_store(settings: Settings) -> Store:
    """The store of this PILOOP_HOME, with its remote when ``[storage.remote]`` names one."""
    storage = settings.storage
    return Store(
        settings.store_dir,
        remote_config(storage.remote).open() if storage.remote is not None else None,
        evict_after=timedelta(days=storage.evict_after_days) if storage.evict_after_days is not None else None,
        max_local_bytes=int(storage.local_max_gb * GB) if storage.local_max_gb is not None else None,
    )
