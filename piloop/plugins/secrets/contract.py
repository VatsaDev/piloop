"""The secrets capability: values that agents get in their environment, and that nothing shows again.

A **secret** is a name and a value. Its name is an environment variable's (``EnvName``). It belongs to the home, for
every campaign, or to one campaign, whose secret wins over the home's of the same name: a GitHub repository's secret
wins over its organization's. Config names a secret, never its value: ``{ secret = "NAME" }`` in ``[harness.<name>]
env`` (``piloop.harnesses.SecretRef``), and each variable of an ``[mcp.<name>]`` server, which is the name of a
secret. An agent's job
names the secrets that it needs (``AgentJob.secrets``), and its worker gets their values when it starts the job. No
listing, event, span or job holds a value.

Host tier: a provider stores values. ``[plugins.secrets] provider`` in ``config.toml`` selects one (``local`` by
default). Its methods are the contract, and each takes the session of the operation that calls it. The operations here
check who writes, and record who set a secret and when in its events (``SecretSet``), in that transaction. A provider
that seals values wraps ``local``: its ``put`` seals, its ``get`` opens.
"""

from abc import ABC, abstractmethod
from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pydantic import SecretStr, TypeAdapter
from sqlalchemy.orm import Session

from piloop.actor import Actor
from piloop.db.events import emit, has, latest
from piloop.db.models import AgentSession, Event, EventModel
from piloop.db.session import Database
from piloop.ids import HOME, AgentSessionId, CampaignId, TargetKind, TargetRef
from piloop.mcp import EnvName
from piloop.plugins.host import Capability, Choice, Parameters, Scope

type Owner = CampaignId | None
"""Whose a secret is: a campaign's, or None for the home's."""


@dataclass(frozen=True)
class SecretKey:
    """What a secret is known by: its owner and its name, never its value."""

    owner: Owner
    name: EnvName


class SecretsProvider(Parameters, ABC):
    """A secrets provider: its parameters, the keys of ``[plugins.secrets]`` besides ``provider``, and its store."""

    @abstractmethod
    def put(self, s: Session, owner: Owner, name: EnvName, value: SecretStr) -> None:
        """Store ``value`` as ``owner``'s secret ``name``, in place of any. The caller commits."""

    @abstractmethod
    def delete(self, s: Session, owner: Owner, name: EnvName) -> bool:
        """Delete ``owner``'s secret ``name``, and return whether there was one. The caller commits."""

    @abstractmethod
    def names(self, s: Session) -> list[SecretKey]:
        """Every secret, by owner (the home's first) and name."""

    @abstractmethod
    def get(self, s: Session, owner: Owner, names: Collection[EnvName]) -> dict[EnvName, SecretStr]:
        """The values of those of ``names`` that ``owner`` has. Only ``values`` calls it."""


SECRETS = Capability(
    'secrets',
    SecretsProvider,
    choice=Choice(Scope.HOME, default='local'),
    group='piloop.secrets',
    noun='secrets provider',
)
"""Every secrets provider by name. ``local``, built in and the default, keeps the values in the home's database."""


@dataclass(frozen=True)
class HomeSecrets:
    """The home's secrets of a database, which a network provider reads the credential that it hands out from."""

    db: Database
    provider: SecretsProvider

    def get(self, name: EnvName) -> SecretStr | None:
        """The value of the home's secret ``name``, or None when the home has none."""
        with self.db.session() as s:
            return self.provider.get(s, None, {name}).get(name)


class SecretSet(EventModel):
    """On the home or on a campaign, the secret's owner: a value of the secret ``name``, never the value itself."""

    kind: Literal['secret.set'] = 'secret.set'
    name: EnvName


class SecretDeleted(EventModel):
    """On the home or on a campaign, the secret's owner: the secret ``name`` is gone."""

    kind: Literal['secret.deleted'] = 'secret.deleted'
    name: EnvName


def target(owner: Owner) -> TargetRef:
    """What the events of ``owner``'s secrets are on."""
    return HOME if owner is None else TargetRef(TargetKind.CAMPAIGN, owner)


def set_secret(
    s: Session, provider: SecretsProvider, actor: Actor, owner: Owner, name: EnvName, value: SecretStr
) -> None:
    """Set ``owner``'s secret ``name`` to ``value``, in place of any. The caller commits."""
    provider.put(s, owner, name, value)
    emit(s, SecretSet(name=name), target(owner), actor)


def remove_secret(s: Session, provider: SecretsProvider, actor: Actor, owner: Owner, name: EnvName) -> bool:
    """Delete ``owner``'s secret ``name``, and return whether there was one. The caller commits."""
    if not provider.delete(s, owner, name):
        return False
    emit(s, SecretDeleted(name=name), target(owner), actor)
    return True


@dataclass(frozen=True)
class Listed:
    """A secret as ``piloop secret list`` shows it: who set it last and when, from its events."""

    secret: SecretKey
    set_at: datetime | None
    """None for a value that piloop did not set: a provider elsewhere."""
    set_by: Actor | None


ACTOR: TypeAdapter[Actor] = TypeAdapter(Actor)


def listed(s: Session, provider: SecretsProvider) -> list[Listed]:
    """Every secret, by owner (the home's first) and name, with who set it last and when."""
    found: list[Listed] = []
    for secret in provider.names(s):
        match latest(s, SecretSet, has(SecretSet, name=secret.name), Event.target == target(secret.owner)):
            case (event, _):
                found.append(Listed(secret, event.at, ACTOR.validate_python(event.actor)))
            case _:
                found.append(Listed(secret, None, None))
    return found


def values(
    s: Session, provider: SecretsProvider, agent_session: AgentSessionId, names: Collection[EnvName]
) -> dict[EnvName, SecretStr]:
    """The values of those of ``names`` that the agent session's campaign or the home has: the campaign's win.

    An executor calls it when it starts an agent's job, for the job's ``secrets``.
    """
    wanted = set(names)
    home = provider.get(s, None, wanted)
    row = s.get(AgentSession, agent_session)
    campaign = row.campaign_id if row is not None else None
    return home if campaign is None else home | provider.get(s, campaign, wanted)
