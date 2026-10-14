"""The S3 remote of the artifact store: one object per blob under ``s3://<bucket>/<prefix>/blake3/<hash>/bytes.zst``.

``[storage.remote]`` with ``kind = "s3"`` selects it, and ``S3Config`` holds its keys. An object's storage class is
chosen by its compressed size: ``storage_class`` (Glacier Instant Retrieval by default: millisecond reads at a fraction
of STANDARD's price), except below ``standard_below`` bytes, where Glacier IR would bill a 128 KB minimum for each and
STANDARD is cheaper. Uploads and downloads stream through boto3's transfer manager (multipart past its threshold).
Credentials are boto3's own chain: the environment, a profile, an instance role. GCS fits as the same class pointed at
its S3-compatible endpoint.

boto3 is imported when an ``S3`` is made, not with this module: it takes ~0.5 s, and the settings that name this
backend are read by every command. Retired with ``piloop.store`` (its module docstring has the condition).
"""

from pathlib import Path
from typing import TYPE_CHECKING, Literal
from urllib.parse import urlparse

from pydantic import Field, field_validator

from piloop.store import RemoteConfig

if TYPE_CHECKING:
    from botocore.exceptions import ClientError
    from mypy_boto3_s3 import S3Client

STANDARD = 'STANDARD'
NOT_FOUND = ('404', 'NoSuchKey', 'NotFound')


class S3Config(RemoteConfig):
    """``[storage.remote]`` with ``kind = "s3"``: a bucket and prefix, and the storage class of each blob."""

    kind: Literal['s3'] = 's3'
    """The S3 backend."""
    url: str
    """``s3://<bucket>[/<prefix>]``: every blob is uploaded there as it is stored."""
    storage_class: str = 'GLACIER_IR'
    """The S3 storage class of a blob of at least ``standard_below`` compressed bytes."""
    standard_below: int = Field(default=128 * 1024, ge=0)
    """Compressed size under which a blob is stored as ``STANDARD``: Glacier IR bills every object as at least 128 KB."""

    @field_validator('url')
    @classmethod
    def _s3_url(cls, url: str) -> str:
        parsed = urlparse(url)
        if parsed.scheme != 's3' or not parsed.netloc:
            raise ValueError(f'{url!r} is no s3://<bucket>[/<prefix>] URL')
        return url

    def open(self) -> S3:
        return S3(self)


class S3:
    """Whole objects by key under one bucket and prefix."""

    def __init__(self, config: S3Config, client: S3Client | None = None) -> None:
        import boto3  # here, not at the top: see the module docstring
        from botocore.exceptions import ClientError

        parsed = urlparse(config.url)
        self.bucket = parsed.netloc
        prefix = parsed.path.strip('/')
        self.prefix = f'{prefix}/' if prefix else ''
        self.url = f's3://{self.bucket}/{prefix}'.rstrip('/')
        """One spelling of the bucket and prefix, so that the store knows the destination it uploaded to again."""
        self.storage_class = config.storage_class
        self.standard_below = config.standard_below
        self.client = client or boto3.client('s3')
        self._client_error: type[ClientError] = ClientError

    def storage_class_of(self, size: int) -> str:
        """The class an object of ``size`` compressed bytes is stored in."""
        return STANDARD if size < self.standard_below else self.storage_class

    def upload(self, key: str, path: Path) -> None:
        self.client.upload_file(
            str(path),
            self.bucket,
            self.prefix + key,
            ExtraArgs={'StorageClass': self.storage_class_of(path.stat().st_size)},
        )

    def download(self, key: str, path: Path) -> bool:
        try:
            self.client.download_file(self.bucket, self.prefix + key, str(path))
        except self._client_error as e:
            if _not_found(e):
                return False
            raise
        return True

    def has(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=self.prefix + key)
        except self._client_error as e:
            if _not_found(e):
                return False
            raise
        return True


def _not_found(e: ClientError) -> bool:
    return e.response.get('Error', {}).get('Code') in NOT_FOUND
