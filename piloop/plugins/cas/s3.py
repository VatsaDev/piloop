"""``s3``: a CAS in an S3 bucket, one object per blob under ``s3://<bucket>/<prefix>/blake3/<digest>/bytes.zst``.

An object's storage class is chosen by its compressed size: ``storage_class`` (Glacier Instant Retrieval by default:
millisecond reads at a fraction of STANDARD's price), except below ``standard_below`` bytes, where Glacier IR would bill
a 128 KB minimum for each and STANDARD is cheaper. Uploads and downloads stream through boto3's transfer manager
(multipart past its threshold). Credentials are boto3's own chain: the environment, a profile, an instance role.

boto3 is imported when the store opens, not with this module: it takes ~0.5 s, and the settings that name this
provider are read by every command. Retirement: deleted when covalence's ``Kv.s3`` takes a storage class and
compression from Python, and ``s3`` opens ``Cas.kv`` over it.
"""

import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO
from urllib.parse import urlparse

from pydantic import Field, field_validator

from piloop.plugins.cas.contract import Cas, CasProvider
from piloop.plugins.cas.kit.objects import ObjectsCas

if TYPE_CHECKING:
    from botocore.exceptions import ClientError
    from mypy_boto3_s3 import S3Client

STANDARD = 'STANDARD'
NOT_FOUND = ('404', 'NoSuchKey', 'NotFound')
TMP = 'tmp'
"""``<PILOOP_HOME>/tmp``: where an object is staged on its way up or down."""


class S3(CasProvider):
    """``[plugins.cas] provider = "s3"``: a bucket and prefix, and the storage class of each blob."""

    url: str
    """``s3://<bucket>[/<prefix>]``: the blobs go there."""
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

    def open(self, home: Path) -> Cas:
        return ObjectsCas(S3Objects(self, home / TMP), home / TMP)

    def where(self, home: Path) -> str:
        parsed = urlparse(self.url)
        return f's3://{parsed.netloc}/{parsed.path.strip("/")}'.rstrip('/')


class S3Objects:
    """Whole objects by key under one bucket and prefix."""

    def __init__(self, config: S3, scratch: Path, client: S3Client | None = None) -> None:
        import boto3  # here, not at the top: see the module docstring
        from botocore.exceptions import ClientError

        parsed = urlparse(config.url)
        self.bucket = parsed.netloc
        prefix = parsed.path.strip('/')
        self.prefix = f'{prefix}/' if prefix else ''
        self.storage_class = config.storage_class
        self.standard_below = config.standard_below
        self.scratch = scratch
        self.client = client or boto3.client('s3')
        self._client_error: type[ClientError] = ClientError

    def storage_class_of(self, size: int) -> str:
        """The class an object of ``size`` compressed bytes is stored in."""
        return STANDARD if size < self.standard_below else self.storage_class

    def put(self, key: str, path: Path) -> None:
        self.client.upload_file(
            str(path),
            self.bucket,
            self.prefix + key,
            ExtraArgs={'StorageClass': self.storage_class_of(path.stat().st_size)},
        )

    @contextmanager
    def open(self, key: str) -> Generator[BinaryIO | None]:
        self.scratch.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=self.scratch) as tmp:
            try:
                self.client.download_file(self.bucket, self.prefix + key, tmp.name)
            except self._client_error as e:
                if not _not_found(e):
                    raise
                yield None
                return
            with open(tmp.name, 'rb') as file:
                yield file

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
