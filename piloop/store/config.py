"""The store of a PILOOP_HOME as ``[storage]`` configures it, and the registry of remote backends.

``[storage.remote]`` picks its backend by ``kind``. The backend's settings model (a ``RemoteConfig``) owns the other
keys of the table and checks them when the store opens. ``s3`` and ``gcs`` are built in (``piloop.store.s3``,
``piloop.store.gcs``). A package adds a backend with an entry point in the ``piloop.stores`` group: its name is the
``kind``, and it names the ``RemoteConfig`` subclass.
"""

from datetime import timedelta
from functools import cache
from importlib.metadata import entry_points

from piloop.settings import GB, RemoteTable, Settings
from piloop.store import RemoteConfig, Store
from piloop.store.gcs import GcsConfig
from piloop.store.s3 import S3Config

STORE_GROUP = 'piloop.stores'
"""The entry point group of the remote backends that packages add."""
BUILTIN_REMOTES: dict[str, type[RemoteConfig]] = {'s3': S3Config, 'gcs': GcsConfig}


@cache
def remotes() -> dict[str, type[RemoteConfig]]:
    """Return the settings model of every remote backend by kind: the built-in ones, then the entry points.

    Raises:
        TypeError: An entry point does not name a ``RemoteConfig`` subclass.
        ValueError: An entry point reuses the name of a built-in backend.
    """
    found = dict(BUILTIN_REMOTES)
    for ep in entry_points(group=STORE_GROUP):
        if ep.name in found:
            raise ValueError(f'entry point {ep.name} = {ep.value} in {STORE_GROUP}: {ep.name!r} is a backend already')
        loaded = ep.load()
        if not (isinstance(loaded, type) and issubclass(loaded, RemoteConfig)):
            raise TypeError(f'entry point {ep.name} = {ep.value} in {STORE_GROUP} is not a piloop.store.RemoteConfig')
        found[ep.name] = loaded
    return found


def remote_config(table: RemoteTable) -> RemoteConfig:
    """Check ``[storage.remote]`` against the settings model of its backend.

    Raises:
        ValueError: No backend has that ``kind``.
        pydantic.ValidationError: The backend refuses a key.
    """
    backend = remotes().get(table.kind)
    if backend is None:
        raise ValueError(f'[storage.remote] kind = {table.kind!r}: no such backend; there is {", ".join(remotes())}')
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
