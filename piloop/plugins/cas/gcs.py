"""``gcs``: a CAS in a Google Cloud Storage bucket, one object per blob under ``gs://<bucket>/<prefix>/…``.

Every GCS class reads in milliseconds, so there is no size threshold as for ``s3``: ``STANDARD`` costs nothing to read
back, the colder classes cost less to keep and bill a minimum storage time and each read. Credentials are Google's
Application Default Credentials (``GOOGLE_APPLICATION_CREDENTIALS``, ``gcloud auth application-default login``, or the
instance's service account), or a service account key file that ``credentials_file`` names. The config holds a path,
never a secret. Uploads and downloads stream, resumable past the client's threshold.

It needs the ``gcs`` extra (``uv tool install 'piloop[gcs]'``), imported when the store opens. Retirement: deleted when
covalence's ``Kv.gcs`` is on PyPI with compression from Python, and ``gcs`` opens ``Cas.kv`` over it.
"""

import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, Literal

from pydantic import field_validator

from piloop.plugins.cas.contract import Cas, CasProvider
from piloop.plugins.cas.kit.objects import ObjectsCas
from piloop.plugins.cas.s3 import TMP

if TYPE_CHECKING:
    from google.cloud.storage import Bucket

type StorageClass = Literal['STANDARD', 'NEARLINE', 'COLDLINE', 'ARCHIVE']


class Gcs(CasProvider):
    """``[plugins.cas] provider = "gcs"``: a bucket, a prefix in it, and the storage class of each blob."""

    bucket: str
    """The bucket name, without ``gs://``."""
    prefix: str = ''
    """The folder in the bucket that the objects go under. Empty: the bucket's root."""
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

    def open(self, home: Path) -> Cas:
        return ObjectsCas(GcsObjects(self, home / TMP), home / TMP)

    def where(self, home: Path) -> str:
        return f'gs://{self.bucket}/{self.prefix.strip("/")}'.rstrip('/')


class GcsObjects:
    """Whole objects by key under one bucket and prefix."""

    def __init__(self, config: Gcs, scratch: Path) -> None:
        try:  # here, not at the top: the extra is optional, and only a store in GCS needs it
            from google.api_core.exceptions import NotFound
            from google.cloud import storage
        except ImportError as e:
            raise ImportError(
                "[plugins.cas] provider = 'gcs' needs the gcs extra: uv tool install --reinstall 'piloop[gcs]'"
            ) from e
        prefix = config.prefix.strip('/')
        self.prefix = f'{prefix}/' if prefix else ''
        self.storage_class = config.storage_class
        self.scratch = scratch
        client = (
            storage.Client(project=config.project)
            if config.credentials_file is None
            else storage.Client.from_service_account_json(str(config.credentials_file), project=config.project)
        )
        self.bucket: Bucket = client.bucket(config.bucket)
        self._not_found: type[NotFound] = NotFound

    def put(self, key: str, path: Path) -> None:
        blob = self.bucket.blob(self.prefix + key)
        blob.storage_class = self.storage_class
        blob.upload_from_filename(str(path))

    @contextmanager
    def open(self, key: str) -> Generator[BinaryIO | None]:
        self.scratch.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=self.scratch) as tmp:
            try:
                self.bucket.blob(self.prefix + key).download_to_filename(tmp.name)
            except self._not_found:
                yield None
                return
            with open(tmp.name, 'rb') as file:
                yield file

    def has(self, key: str) -> bool:
        return self.bucket.blob(self.prefix + key).exists()
