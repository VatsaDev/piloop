"""A provisioner for tests, ``memory``: its instances are rows of a ``Platform`` in this process, which a test reads and
breaks. Each ``platform`` name is one cloud account, which several servers (owners) can share."""

from collections.abc import Sequence
from dataclasses import dataclass, field

from piloop.plugins.provisioner.contract import (
    Instance,
    InstanceState,
    Launch,
    ProvisionerConfig,
    ProvisionerHost,
    StartRefused,
)


@dataclass
class Machine:
    owner: str
    launch: Launch
    state: InstanceState = InstanceState.PENDING


@dataclass
class Platform:
    """One account: its machines by name, and what a test makes it do."""

    machines: dict[str, Machine] = field(default_factory=dict)
    starts: list[str] = field(default_factory=list)
    """Each start, by name, in order: a repeated one too."""
    refuse: str | None = None
    """Refuse every start with this reason."""
    down: bool = False
    """Answer no call."""
    flaky: int = 0
    """Fail this many of the next starts, as a connection that drops."""

    def owned(self, owner: str) -> list[Machine]:
        return [m for m in self.machines.values() if m.owner == owner]


PLATFORMS: dict[str, Platform] = {}


class Memory(ProvisionerConfig):
    """``memory``: the machines of ``PLATFORMS[platform]``."""

    platform: str

    def open(self, host: ProvisionerHost) -> MemoryProvisioning:
        return MemoryProvisioning(PLATFORMS.setdefault(self.platform, Platform()), host.owner)


@dataclass
class MemoryProvisioning:
    platform: Platform
    owner: str

    def _answer(self) -> None:
        if self.platform.down:
            raise ConnectionError('the platform does not answer')

    def list(self) -> Sequence[Instance]:
        self._answer()
        return [
            Instance(name=m.launch.name, pool=m.launch.pool, state=m.state, handle={'id': m.launch.name})
            for m in self.platform.owned(self.owner)
        ]

    def start(self, launch: Launch) -> None:
        self._answer()
        self.platform.starts.append(launch.name)
        if self.platform.flaky:
            self.platform.flaky -= 1
            raise ConnectionError('the connection dropped')
        if self.platform.refuse is not None:
            raise StartRefused(self.platform.refuse)
        self.platform.machines.setdefault(launch.name, Machine(self.owner, launch))

    def stop(self, name: str) -> None:
        self._answer()
        if (machine := self.platform.machines.get(name)) is not None and machine.owner == self.owner:
            del self.platform.machines[name]
