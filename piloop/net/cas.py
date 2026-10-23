"""The wire format of a CAS over HTTP: ``piloop cas serve`` serves it, and the ``http`` CAS provider calls it.

It is covalence's HTTP CAS: one blob per raw BLAKE3 CID, its raw bytes in both directions, so that a client can switch
between the two servers.

- ``HEAD /v1/cas/<cid>``: 200 when the server holds the blob, else 404.
- ``GET /v1/cas/<cid>``: its bytes, ``application/octet-stream``, with ``Content-Length``; 404 when it is missing.
- ``PUT /v1/cas/<cid>``: the bytes, with ``Content-Length`` (else 411). 201 once they are stored, also when they were
  there already; 400 ``cid_mismatch`` when they do not hash to ``<cid>``, and nothing is stored.

An error's body is ``ErrorBody``, and a 503 has ``Retry-After``. Each request sends ``Authorization: Bearer <token>``.
"""

from enum import StrEnum

from covalence import Cid
from pydantic import BaseModel

from piloop.plugins.cas.contract import BLAKE3

ROUTE = '/v1/cas/{cid}'


class Code(StrEnum):
    """The ``code`` of an error's body, which a client branches on."""

    UNAUTHENTICATED = 'unauthenticated'
    PERMISSION_DENIED = 'permission_denied'
    INVALID_CID = 'invalid_cid'
    LENGTH_REQUIRED = 'length_required'
    CID_MISMATCH = 'cid_mismatch'
    NOT_FOUND = 'not_found'
    INVALID_REQUEST = 'invalid_request'
    UNAVAILABLE = 'unavailable'
    INTERNAL = 'internal'


class ErrorBody(BaseModel):
    """The body of an error: ``code`` to branch on, ``message`` for a human."""

    code: str
    """A ``Code``, as text: a code that another server adds still parses."""
    message: str


def route(cid: Cid) -> str:
    """The path of a blob: its raw CID, whatever the codec of ``cid``."""
    return ROUTE.format(cid=Cid.from_parts('raw', BLAKE3, cid.digest))
