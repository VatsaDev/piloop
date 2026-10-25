"""The network capability: how other machines reach the server of ``piloop serve --listen``, and who a caller is.

Host tier: a provider binds a socket and makes HTTP clients. ``[plugins.network] provider`` in ``config.toml`` selects
one (``https`` by default). The entry point names a ``NetworkConfig`` class, whose fields are the other keys of the
table and always have ``listen``, which ``piloop serve --listen`` overrides. Its ``open`` makes the server's
``Network``, and its ``client`` dials a server.

What a caller may do once it is known is core's (``piloop.net``), not the provider's.
"""

import ipaddress
import socket
import ssl
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Annotated, Protocol

import httpx2 as httpx
from pydantic import BaseModel, ConfigDict, Field, SecretStr, WithJsonSchema, model_validator

from piloop.api.contract import Access
from piloop.net.tokens import Caller, Tokens
from piloop.plugins.host import Capability, Choice, Parameters, Scope
from piloop.plugins.secrets.contract import HomeSecrets


class Address(BaseModel):
    """A host and port, ``HOST:PORT``, such as ``127.0.0.1:8443`` or ``[::1]:8443``. Port 0 listens on any free port."""

    model_config = ConfigDict(frozen=True)

    host: str
    port: int = Field(ge=0, le=65535)

    @model_validator(mode='before')
    @classmethod
    def _parse(cls, value: object) -> object:
        if not isinstance(value, str):
            return value
        host, colon, port = value.rpartition(':')
        if not colon or not host or not port.isdigit():
            raise ValueError(f'{value!r} is not HOST:PORT')
        return {'host': host.removeprefix('[').removesuffix(']'), 'port': int(port)}

    @property
    def loopback(self) -> bool:
        """Whether only this machine can connect: ``localhost`` or a loopback IP."""
        if self.host == 'localhost':
            return True
        try:
            return ipaddress.ip_address(self.host).is_loopback
        except ValueError:
            return False

    def __str__(self) -> str:
        return f'[{self.host}]:{self.port}' if ':' in self.host else f'{self.host}:{self.port}'


class Peer(BaseModel):
    """The other end of a request's connection, as the server sees it.

    A provider that knows a caller by its connection rather than by a token reads it: a tailnet one asks the tailnet
    who has ``address``, an mTLS one maps ``certificate``, and a trusted CIDR list admits ``address``.
    """

    model_config = ConfigDict(frozen=True)

    address: Address | None
    """Where the caller connects from. None when the socket has no address, such as a Unix socket."""
    certificate: bytes | None
    """The certificate that the caller presented in the TLS handshake, in DER. None without TLS, or when the listener's
    TLS asks for none."""


@dataclass(frozen=True)
class Refused:
    """Why the server does not know a caller: it answers 401 with this."""

    reason: str


class NoCredential(Exception):
    """This machine has no credential to dial the server with. The message says how to give it one."""


class Enrollment(BaseModel):
    """How a machine that the server starts dials it: the server's URL, and the environment that holds its credential.

    The provider's ``client`` on that machine reads ``env``. It never goes into an event, a span or an argv.
    """

    model_config = ConfigDict(frozen=True)

    server: str
    env: dict[str, SecretStr]


class NotEnrolled(Exception):
    """The network cannot give a machine a credential to dial the server with. The message says why."""


@dataclass(frozen=True)
class Listener:
    """A socket that is bound and listening, and the TLS that the server ends on it, or None for plain HTTP."""

    socket: socket.socket
    tls: ssl.SSLContext | None


@dataclass(frozen=True)
class NetworkHost:
    """What piloop gives a network provider."""

    tokens: Tokens
    """The bearer tokens of ``piloop token``, which ``enroll`` mints and ``revoke`` deletes."""
    secrets: HomeSecrets
    """The home's secrets (``piloop secret set``), for a credential that ``enroll`` hands out."""


class Network(Protocol):
    """How machines reach the server, and who a caller is. The daemon makes one when it starts."""

    takes_tokens: bool
    """Whether core admits piloop's own tokens (``plt_``) on this network. False: the network authenticates every caller
    itself (``identify``), and core refuses a ``plt_`` bearer, so no token minted in the home opens a way around it."""

    def listener(self) -> Listener | None:
        """Bind ``listen``, or return None when it is None. The daemon calls it once."""
        ...

    def identify(self, headers: Mapping[str, str], peer: Peer) -> Caller | Refused:
        """Who sends a request with these headers (their names in lower case), from ``peer``.

        Core verifies a piloop token (``Bearer plt_…``) itself, on every network, and a call on the home's Unix socket is
        its owner. Every other request comes here, with its headers as sent: another bearer, such as an edge's key, too.
        """
        ...

    def enroll(self, principal: str, access: Access) -> Enrollment:
        """Give the machine called ``principal`` a credential of ``access``, in place of any that it had.

        ``identify`` then knows its calls as ``principal``'s. A provisioner's instance gets one when it starts (ARC's
        just-in-time runner config).

        Raises:
            NotEnrolled: The network cannot give one.
        """
        ...

    def revoke(self, principal: str) -> None:
        """End the credential of ``principal``: its next call is refused. One that is gone already is a success."""
        ...


class NetworkConfig(Parameters, ABC):
    """A network's ``[plugins.network]``: core reads ``listen``, and the provider the other fields.

    A provider refuses, when it validates, a ``listen`` that it cannot secure.
    """

    listen: Annotated[Address, WithJsonSchema({'type': 'string'})] | None = None
    """Where the server listens, as ``HOST:PORT``. None: no server. ``piloop serve --listen`` overrides it."""

    @abstractmethod
    def open(self, host: NetworkHost) -> Network:
        """Make the server's side of the network. The daemon calls it once, when it starts."""

    @abstractmethod
    def client(self, server: str) -> httpx.Client:
        """The dialing side, which needs no host: a client of the server at ``server``, with this machine's credential.

        The provider finds the credential itself, so its caller (``piloop worker``) knows no kind of credential.

        Raises:
            NoCredential: This machine has none.
        """

    def listening(self, listen: Address | None) -> NetworkConfig:
        """This network, with ``listen`` (``piloop serve --listen``) in place of its own when it is not None.

        Raises:
            ValidationError: The network refuses the address.
        """
        if listen is None:
            return self
        return type(self).model_validate(self.model_dump(exclude_unset=True) | {'listen': listen})


NETWORKS = Capability('network', NetworkConfig, choice=Choice(Scope.HOME, default='https'))
"""Every network by name. ``https``, built in and the default, takes bearer tokens over TLS."""
