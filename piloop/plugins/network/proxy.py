"""``proxy``: a server behind a proxy that authenticates every request and stamps who sent it in headers.

```toml
[plugins.network]
provider = "proxy"
listen = "0.0.0.0:8443"
url = "https://piloop.example.com"
claims = { org = "x-org-id", principal = "x-principal-id" }
name_claim = "principal"
rules = [
  { when = { org = "acme", principal = "key-0123" }, access = "worker" },
  { when = { org = "acme" }, access = "act" },
]
```

The proxy (Envoy's ext_authz, an oauth2-proxy, a cloud load balancer's authentication) checks each caller's own
credential, removes any copy of its headers that the caller sent, and stamps its own. ``claims`` names those headers;
``rules`` give a caller its access by the values of its claims, the first rule that matches winning, as Envoy's RBAC and
Envoy Gateway's authorization rules do; a caller that no rule matches is refused. A worker is named by the
``worker_header`` that it sends, anyone else by its ``name_claim``.

This network checks no token of its own, and core refuses piloop's own tokens (``plt_``) on it, so ``listen`` must be
reachable from the proxy alone, by a network policy or a firewall: whoever reaches it can claim anything. TLS ends at
the proxy.

Workers share one credential that the proxy accepts, which its ``worker`` rule matches. ``enroll`` hands a provisioned
worker that credential, from the home's secret ``worker_credential_secret`` (``piloop secret set``), and its name.
``revoke`` has nothing to end: the provisioner stops the worker's machine, and revoking the credential at the proxy
ends every worker.
"""

import os
import socket
from collections.abc import Mapping
from typing import Final, Self

import httpx2 as httpx
from pydantic import BaseModel, ConfigDict, SecretStr, model_validator

from piloop.actor import Actor
from piloop.api.contract import Access
from piloop.mcp import EnvName
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

CREDENTIAL_ENV: Final = 'PILOOP_PROXY_CREDENTIAL'
"""The worker credential that a machine that dials the server sends as its bearer, for the proxy to check."""
NAME_ENV: Final = 'PILOOP_WORKER_NAME'
"""The name of a worker that dials the server, which it sends in ``worker_header``."""
TIMEOUT = httpx.Timeout(30, connect=10)


class Rule(BaseModel):
    """A caller whose claims have these values gets ``access``."""

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    when: dict[str, str | list[str]]
    """Each claim, by name, and the value that it must have, or one of several. Every claim must match."""
    access: Access
    """What the caller may do: ``read``, ``act``, ``admin``, or ``worker`` for the workers' credential."""

    def matches(self, claims: Mapping[str, str | None]) -> bool:
        return all(
            (value := claims[claim]) is not None and value in ([want] if isinstance(want, str) else want)
            for claim, want in self.when.items()
        )


class Proxy(NetworkConfig):
    """``proxy``: the keys of ``[plugins.network]``. A server (``listen``) needs ``rules``."""

    url: str | None = None
    """Where workers reach this server, through the proxy: what a provisioned worker dials. None: it starts none."""
    claims: dict[str, str] = {}
    """Each claim, by name, and the header in which the proxy stamps it: ``{ org = "x-org-id" }``."""
    rules: list[Rule] = []
    """Who gets which access, by their claims: the first rule that matches wins, and no match is refused."""
    name_claim: str | None = None
    """The claim that names a caller who is not a worker, such as its principal. Needed by any rule but ``worker``."""
    worker_header: str = 'x-piloop-worker'
    """The header in which a worker names itself. The proxy passes it through."""
    actor_header: str = 'x-piloop-actor'
    """The header in which a caller asks to act as another actor than itself, such as ``orchestrator``. The proxy
    passes it through."""
    worker_credential_secret: EnvName = 'PILOOP_WORKER_CREDENTIAL'
    """The home's secret that holds the workers' credential, which ``enroll`` hands to a provisioned worker."""

    @model_validator(mode='after')
    def _claims_known(self) -> Self:
        named = {claim for rule in self.rules for claim in rule.when}
        if self.name_claim is not None:
            named.add(self.name_claim)
        if unknown := sorted(named - self.claims.keys()):
            raise ValueError(f'claims names no header for {", ".join(unknown)}')
        if self.listen is not None and not self.rules:
            raise ValueError('a server behind the proxy needs rules: who gets which access')
        if self.name_claim is None and any(rule.access is not Access.WORKER for rule in self.rules):
            raise ValueError('a rule gives access to callers who are not workers: name_claim names them')
        return self

    def open(self, host: NetworkHost) -> ProxyNetwork:
        return ProxyNetwork(host, self)

    def client(self, server: str, actor: Actor | None = None) -> httpx.Client:
        """A client that sends this worker's credential as its bearer, its name, and ``actor`` in ``actor_header``."""
        credential, name = os.environ.get(CREDENTIAL_ENV), os.environ.get(NAME_ENV)
        if not credential or not name:
            raise NoCredential(f'set {CREDENTIAL_ENV} to the worker credential and {NAME_ENV} to this worker name')
        headers = {'Authorization': f'Bearer {credential}', self.worker_header: name}
        if actor is not None:
            headers[self.actor_header] = actor.signature
        return httpx.Client(base_url=server, headers=headers, timeout=TIMEOUT)


class ProxyNetwork:
    """``Network``: plain HTTP behind the proxy, and the caller that the proxy stamped."""

    takes_tokens = False
    """The proxy is the one authentication: a ``plt_`` token minted in the home would be a way around it."""

    def __init__(self, host: NetworkHost, config: Proxy) -> None:
        self.host = host
        self.config = config

    def listener(self) -> Listener | None:
        if (listen := self.config.listen) is None:
            return None
        family = socket.AF_INET6 if ':' in listen.host else socket.AF_INET
        return Listener(socket.create_server((listen.host, listen.port), family=family), None)

    def identify(self, headers: Mapping[str, str], peer: Peer) -> Caller | Refused:
        config = self.config
        claims = {claim: headers.get(header.lower()) for claim, header in config.claims.items()}
        rule = next((rule for rule in config.rules if rule.matches(claims)), None)
        if rule is None:
            return Refused('no rule admits the claims that the proxy stamped')
        header = config.worker_header if rule.access is Access.WORKER else config.claims[config.name_claim or '']
        if not (name := headers.get(header.lower())):
            return Refused(f'the caller is named in {header}, and it is empty')
        return Caller(name, rule.access)

    def actor(self, headers: Mapping[str, str]) -> str | None:
        """The actor that ``actor_header`` names."""
        return headers.get(self.config.actor_header.lower())

    def enroll(self, principal: str, access: Access) -> Enrollment:
        """The workers' credential of the home's secret, and ``principal`` as the worker's name."""
        config = self.config
        if config.url is None:
            raise NotEnrolled('set url in [plugins.network] to where workers reach this server through the proxy')
        if (credential := self.host.secrets.get(config.worker_credential_secret)) is None:
            raise NotEnrolled(
                f'the home has no secret {config.worker_credential_secret}: piloop secret set '
                f'{config.worker_credential_secret}, with the worker credential'
            )
        return Enrollment(server=config.url, env={CREDENTIAL_ENV: credential, NAME_ENV: SecretStr(principal)})

    def revoke(self, principal: str) -> None:
        """Nothing to end: the workers share the credential, and the provisioner stops the worker's machine."""
