"""The provisioner capability: how a pool's machines are started and stopped, a Modal sandbox or a Kubernetes Job.

An **instance** is one machine that runs ``piloop worker --server … --pool <pool>`` and takes leases like any other
worker. The ``workers`` executor decides how many each pool needs (``piloop.plugins.executor.kit.scaling``): the
provisioner only lists, starts and stops them. Core picks each instance's name, which is the worker's everywhere: its
credential, its ``lease_workers`` row, and the tag on the compute. So ``start`` takes a name, not a count, and a
repeated call is a no-op: Karpenter's ``Create(NodeClaim)``, not the cluster autoscaler's ``IncreaseSize(delta)``.

Host tier: a provisioner calls a platform. It is a table nested in a pool, ``[plugins.executor.pools.<name>.provisioner
.compute]``, which ``provider`` selects (``PROVISIONERS.nested``). Its ``open`` makes the live ``Provisioning``.

After Ray's ``NodeProvider`` (``create_node``, ``terminate_node``, ``non_terminated_nodes(tag_filters)``), Karpenter's
``CloudProvider`` and the Nomad autoscaler's target plugins, whose scaling policy is the same for every target.
"""

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from pydantic import BaseModel, ConfigDict, JsonValue, SecretStr

from piloop.otel import Tracer
from piloop.plugins.host import Capability, Parameters


class Launch(BaseModel):
    """What core asks a provisioner to start: one instance, by name."""

    model_config = ConfigDict(frozen=True)

    name: str
    """The worker's name. The provider tags the instance with it, and with the owner (``ProvisionerHost.owner``)."""
    pool: str
    argv: list[str]
    """What the instance runs: ``piloop worker --server <url> --pool <pool>``, with ``--once`` for a pool of one lease
    per instance."""
    env: dict[str, SecretStr]
    """The worker's credential (``Network.enroll``). The provider puts it in the platform's secret store: never in an
    argv, a log or a span."""


class InstanceState(StrEnum):
    """An instance, as its provider sees it."""

    PENDING = 'pending'
    """The provider accepted it, and it does not run yet."""
    RUNNING = 'running'
    ENDED = 'ended'
    """It exited or was deleted, and the provider still lists it."""


class Instance(BaseModel):
    """One instance that a provider lists."""

    model_config = ConfigDict(frozen=True)

    name: str
    pool: str
    state: InstanceState
    handle: dict[str, JsonValue]
    """The provider's own id of it: a sandbox id, a pod uid, an EC2 instance id."""


class StartRefused(Exception):
    """``start`` failed for a reason that a retry does not cure: a quota, no capacity, a bad image. The message says it.

    Any other exception of ``start`` is tried again.
    """


class Provisioning(Protocol):
    """A provisioner's live side: the three verbs on the platform. The daemon calls it from one thread at a time."""

    def list(self) -> Sequence[Instance]:
        """Every instance tagged with this server's owner, in any pool, that the platform still has."""
        ...

    def start(self, launch: Launch) -> None:
        """Start the instance ``launch.name``. Started already: nothing. Raises ``StartRefused``."""
        ...

    def stop(self, name: str) -> None:
        """Stop the instance ``name``. An instance that is gone already is a success."""
        ...


@dataclass(frozen=True)
class ProvisionerHost:
    """What piloop gives a provisioner."""

    owner: str
    """The id of this server (a UUID that its home keeps): tag every instance with it, and list only those. Two servers
    in one account never stop each other's instances."""
    tracer: Tracer


class ProvisionerConfig(Parameters, ABC):
    """A provisioner's parameters: the keys of its ``compute`` table besides ``provider``."""

    @abstractmethod
    def open(self, host: ProvisionerHost) -> Provisioning:
        """Make the live side. The executor calls it once, when the daemon starts."""


PROVISIONERS = Capability('provisioner', ProvisionerConfig)
"""Every provisioner by name. A pool's ``compute`` table selects one: there is no default, and no
``[plugins.provisioner]``."""
