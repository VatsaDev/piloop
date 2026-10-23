"""The network capability: how other machines reach the server of ``piloop serve --listen``, and who a caller is.

Host tier: a provider binds a socket and makes HTTP clients. ``[plugins.network] provider`` in ``config.toml`` selects
one (``https`` by default). The entry point names a ``NetworkConfig`` class, whose fields are the other keys of the
table and always have ``listen``, which ``piloop serve --listen`` overrides. Its ``open`` makes the ``Network``.

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
from pydantic import BaseModel, ConfigDict, Field, WithJsonSchema, model_validator

from piloop.net.tokens import Caller, Tokens
from piloop.plugins.host import Capability, Choice, Parameters, Scope


class Address(BaseModel):
    """Where to listen: ``HOST:PORT``, such as ``127.0.0.1:8443`` or ``[::1]:8443``. Port 0 takes any free port."""

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


@dataclass(frozen=True)
class Refused:
    """Why the server does not know a caller: it answers 401 with this."""

    reason: str


@dataclass(frozen=True)
class Listener:
    """A socket that is bound and listening, and the TLS that the server ends on it, or None for plain HTTP."""

    socket: socket.socket
    tls: ssl.SSLContext | None


@dataclass(frozen=True)
class NetworkHost:
    """What piloop gives a network provider."""

    tokens: Tokens
    """The bearer tokens of ``piloop token``, to verify a caller's secret against."""


class Network(Protocol):
    """How machines reach the server, and who a caller is. The daemon makes one when it starts."""

    def listener(self) -> Listener | None:
        """Bind ``listen``, or return None when it is None. The daemon calls it once."""
        ...

    def identify(self, headers: Mapping[str, str]) -> Caller | Refused:
        """Who sends a request with these headers (their names in lower case)."""
        ...

    def client(self, server: str, token: str) -> httpx.Client:
        """A client of the server at the URL ``server``, which calls it with ``token``."""
        ...


class NetworkConfig(Parameters, ABC):
    """A network's ``[plugins.network]``: core reads ``listen``, and the provider the other fields.

    A provider refuses, when it validates, a ``listen`` that it cannot secure.
    """

    listen: Annotated[Address, WithJsonSchema({'type': 'string'})] | None = None
    """Where the server listens, as ``HOST:PORT``. None: no server. ``piloop serve --listen`` overrides it."""

    @abstractmethod
    def open(self, host: NetworkHost) -> Network:
        """Make the network. The daemon calls it once, when it starts."""

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
