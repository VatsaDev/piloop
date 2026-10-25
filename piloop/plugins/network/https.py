"""``https``, the built-in network: bearer tokens (``piloop token``), over TLS that the server or a proxy ends.

```toml
[plugins.network]
provider = "https"
listen = "0.0.0.0:8443"
tls = { cert = "/etc/piloop/cert.pem", key = "/etc/piloop/key.pem" }
```

A server that other machines can reach and that has no TLS would send its tokens in the clear: an address that is not
loopback needs ``tls``. ``tls = "proxy"`` says that TLS ends in front of the server, at a reverse proxy or an SSH
tunnel. A machine that dials the server sends the token in ``PILOOP_HTTPS_TOKEN``. A worker that the server starts gets
a token of its own, and dials ``url``.
"""

import os
import socket
import ssl
from collections.abc import Mapping
from typing import Literal, Self, assert_never

import httpx2 as httpx
from pydantic import BaseModel, ConfigDict, FilePath, SecretStr, model_validator

from piloop.api.contract import Access
from piloop.net.tokens import Caller
from piloop.plugins.network.contract import (
    Enrollment,
    Listener,
    NetworkConfig,
    NetworkHost,
    NoCredential,
    NotEnrolled,
    Peer,
    Refused,
)

TIMEOUT = httpx.Timeout(30, connect=10)
TOKEN_ENV = 'PILOOP_HTTPS_TOKEN'
"""The variable that holds the bearer token of a machine that dials the server (``piloop worker``)."""


class Certificate(BaseModel):
    """The server's certificate chain and its private key, as PEM files."""

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    cert: FilePath
    """The certificate, then the chain of its issuers."""
    key: FilePath
    """The certificate's private key, unencrypted."""


class Https(NetworkConfig):
    """``https``: the keys of ``[plugins.network]``."""

    tls: Certificate | Literal['proxy'] | None = None
    """The server ends TLS with a certificate, or ``"proxy"``: a reverse proxy or an SSH tunnel ends it. None: plain
    HTTP, which only a loopback ``listen`` may use."""
    url: str | None = None
    """Where other machines reach this server, such as ``https://piloop.example.com:8443``: what a worker that the
    server starts dials. None: the server starts none."""

    @model_validator(mode='after')
    def _secured(self) -> Self:
        if self.listen is not None and not self.listen.loopback and self.tls is None:
            raise ValueError(
                f'listen = {str(self.listen)!r} is not loopback, and without tls its tokens would cross the network in'
                ' the clear: set tls = { cert = …, key = … }, or tls = "proxy" when a proxy ends TLS in front of it'
            )
        return self

    def open(self, host: NetworkHost) -> HttpsNetwork:
        return HttpsNetwork(host, self)

    def client(self, server: str) -> httpx.Client:
        """A client that sends the bearer token of ``TOKEN_ENV``.

        It trusts the system's certificates, or those of ``SSL_CERT_FILE`` when it is set.
        """
        if not (token := os.environ.get(TOKEN_ENV)):
            raise NoCredential(f'set {TOKEN_ENV} to the secret that `piloop token create <name>` printed on the server')
        return httpx.Client(base_url=server, headers={'Authorization': f'Bearer {token}'}, timeout=TIMEOUT)


class HttpsNetwork:
    """``Network``: a TCP socket, with TLS when ``tls`` names a certificate, and a bearer token on every request."""

    takes_tokens = True

    def __init__(self, host: NetworkHost, config: Https) -> None:
        self.host = host
        self.config = config

    def listener(self) -> Listener | None:
        listen = self.config.listen
        if listen is None:
            return None
        family = socket.AF_INET6 if ':' in listen.host else socket.AF_INET
        sock = socket.create_server((listen.host, listen.port), family=family)
        match self.config.tls:
            case Certificate(cert=cert, key=key):
                tls = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
                tls.load_cert_chain(cert, key)
                return Listener(sock, tls)
            case 'proxy' | None:  # None only on loopback: plain HTTP
                return Listener(sock, None)
            case _:
                assert_never(self.config.tls)

    def identify(self, headers: Mapping[str, str], peer: Peer) -> Caller | Refused:
        """No caller: ``https`` knows only piloop's tokens, which core verifies before it asks the network."""
        if headers.get('authorization'):
            return Refused('the credential is not a piloop token (plt_…): send Authorization: Bearer <token>')
        return Refused('no bearer token: send Authorization: Bearer <token> (piloop token create)')

    def enroll(self, principal: str, access: Access) -> Enrollment:
        """A token called ``principal``, in place of the one that has that name, in ``TOKEN_ENV``."""
        if self.config.url is None:
            raise NotEnrolled('set url in [plugins.network] to where other machines reach this server')
        secret = self.host.tokens.mint(principal, access)
        return Enrollment(server=self.config.url, env={TOKEN_ENV: SecretStr(secret)})

    def revoke(self, principal: str) -> None:
        self.host.tokens.revoke(principal)
