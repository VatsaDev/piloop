"""The Google Cloud Storage remote of the artifact store: one object per blob under ``gs://<bucket>/<prefix>/...``.

``[storage.backend]`` with ``kind = "gcs"`` selects it, and ``GcsConfig`` holds its keys. It shows how the table
changes with the backend: S3 names one URL and a size threshold, GCS a bucket, a prefix and one storage class. Every
GCS class reads in milliseconds, so there is no threshold: ``STANDARD`` costs nothing to read back, the colder
classes cost less to keep and bill a minimum storage time and each read. Credentials are Google's Application Default
Credentials (``GOOGLE_APPLICATION_CREDENTIALS``, ``gcloud auth application-default login``, or the instance's service
account), or a service account key file that ``credentials_file`` names. The config holds a path, never a secret. Uploads and downloads stream, resumable past the client's threshold.

It needs the ``gcs`` extra (``uv tool install 'piloop[gcs]'``), imported when a ``Gcs`` is made. Retired with
``piloop.store`` (its module docstring has the condition).
"""

from pathlib import Path
from typing import TYPE_CHECKING, Literal

from pydantic import field_validator

from piloop.plugins.storage_backend.contract import StorageBackendConfig

if TYPE_CHECKING:
    from google.cloud.storage import Bucket

type StorageClass = Literal['STANDARD', 'NEARLINE', 'COLDLINE', 'ARCHIVE']


class GcsConfig(StorageBackendConfig):
    """``[storage.backend]`` with ``kind = "gcs"``: a bucket, a prefix in it, and the storage class of each blob."""

    kind: Literal['gcs'] = 'gcs'
    """The Google Cloud Storage backend."""
    bucket: str
    """The bucket name, without ``gs://``."""
    prefix: str = ''
    """The folder in the bucket that the store's objects go under. Empty: the bucket's root."""
    storage_class: StorageClass = 'STANDARD'
    """The storage class of every blob."""
    project: str | None = None
    """The Google Cloud project to bill. Unset: the project of the credentials."""
    credentials_file: Path | None = None
    """A service account key file (JSON) to use instead of Application Default Credentials. A path, never the key."""

    @field_validator('bucket')
    @classmethod
    def _bare_name(cls, bucket: str) -> str:
        if not bucket or '/' in bucket or bucket.startswith('gs:'):
            raise ValueError(f'{bucket!r} is no bucket name: name the bucket alone and the folder in prefix')
        return bucket

    def open(self) -> Gcs:
        return Gcs(self)


class Gcs:
    """Whole objects by key under one bucket and prefix."""

    def __init__(self, config: GcsConfig) -> None:
        try:  # here, not at the top: the extra is optional, and only a store with this remote needs it
            from google.api_core.exceptions import NotFound
            from google.cloud import storage
        except ImportError as e:
            raise ImportError(
                "[storage.backend] kind = 'gcs' needs the gcs extra: uv tool install --reinstall 'piloop[gcs]'"
            ) from e
        prefix = config.prefix.strip('/')
        self.prefix = f'{prefix}/' if prefix else ''
        self.url = f'gs://{config.bucket}/{prefix}'.rstrip('/')
        """One spelling of the bucket and prefix, so that the store knows the destination it uploaded to again."""
        self.storage_class = config.storage_class
        client = (
            storage.Client(project=config.project)
            if config.credentials_file is None
            else storage.Client.from_service_account_json(str(config.credentials_file), project=config.project)
        )
        self.bucket: Bucket = client.bucket(config.bucket)
        self._not_found: type[NotFound] = NotFound

    def upload(self, key: str, path: Path) -> None:
        blob = self.bucket.blob(self.prefix + key)
        blob.storage_class = self.storage_class
        blob.upload_from_filename(str(path))

    def download(self, key: str, path: Path) -> bool:
        try:
            self.bucket.blob(self.prefix + key).download_to_filename(str(path))
        except self._not_found:
            return False
        return True

    def has(self, key: str) -> bool:
        return self.bucket.blob(self.prefix + key).exists()
