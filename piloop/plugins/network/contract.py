"""The network capability: how other machines reach the server of ``piloop serve --listen``, and who a caller is.

Host tier: a provider binds a socket and makes HTTP clients. ``[plugins.network] provider`` in ``config.toml`` selects
one (``https`` by default), and the other keys of the table are its parameters (``Parameters``), which always have
``listen``. ``piloop serve --listen`` overrides it. The entry point names a class: ``Network(host, parameters)``.

What a caller may do once it is known is core's (``piloop.net``), not the provider's.
"""

import ipaddress
import socket
import ssl
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Annotated, ClassVar, Protocol, TypeIs

import httpx2 as httpx
from pydantic import BaseModel, ConfigDict, Field, WithJsonSchema, model_validator

from piloop.net.tokens import Caller, Tokens
from piloop.plugins.host import Capability, PluginTable, Selection


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


class NetworkParameters(BaseModel):
    """The base of every network provider's ``Parameters``: core reads ``listen``, and the provider the rest."""

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    listen: Annotated[Address, WithJsonSchema({'type': 'string'})] | None = None
    """Where the server listens, as ``HOST:PORT``. None: no server. ``piloop serve --listen`` overrides it."""


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

    Parameters: ClassVar[type[NetworkParameters]]

    def __init__(self, host: NetworkHost, parameters: NetworkParameters) -> None:
        """Made with ``parameters`` checked against ``Parameters``: a provider refuses a server that it cannot secure."""
        ...

    def listener(self) -> Listener | None:
        """Bind ``listen``, or return None when it is None. The daemon calls it once."""
        ...

    def identify(self, headers: Mapping[str, str]) -> Caller | Refused:
        """Who sends a request with these headers (their names in lower case)."""
        ...

    def client(self, server: str, token: str) -> httpx.Client:
        """A client of the server at the URL ``server``, which calls it with ``token``."""
        ...


def _is_network(loaded: object) -> TypeIs[type[Network]]:
    parameters = getattr(loaded, 'Parameters', None)
    return (
        isinstance(loaded, type)
        and isinstance(parameters, type)
        and issubclass(parameters, NetworkParameters)
        and all(callable(getattr(loaded, m, None)) for m in ('listener', 'identify', 'client'))
    )


def _check(name: str, loaded: object) -> type[Network]:
    if _is_network(loaded):
        return loaded
    raise TypeError(
        'is not a piloop network class: it needs Parameters (a NetworkParameters) and the methods of Network'
    )


NETWORKS = Capability(
    'network', group='piloop.networks', check=_check, selection=Selection('home', 'plugins.network.provider')
)
"""Every network by name. ``https``, built in and the default, takes bearer tokens over TLS."""
DEFAULT_NETWORK = 'https'
"""The network of a home whose ``config.toml`` has no ``[plugins.network]``."""


def selected(table: PluginTable | None, listen: Address | None) -> tuple[type[Network], NetworkParameters]:
    """The network that ``[plugins.network]`` selects, ``DEFAULT_NETWORK`` without one, and its checked parameters.

    ``listen``, from ``piloop serve --listen``, replaces the table's.

    Raises:
        MissingProvider: No network has that name.
        ValidationError: Its ``Parameters`` refuse the table, or the address.
    """
    provider = NETWORKS.get(table.provider if table is not None else DEFAULT_NETWORK)
    keys = table.keys if table is not None else {}
    return provider, provider.Parameters.model_validate(keys | ({'listen': listen} if listen is not None else {}))
