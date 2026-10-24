"""The contract of a provisioner: one check that every provider passes, piloop's or a package's.

``check_provisioning`` drives a provider through what the ``workers`` executor asks of it, on the real platform: it
starts one instance of ``launch``, so give it a cheap one. A broken rule raises ``ContractBroken``, which says which.
"""

import time
from collections.abc import Callable

from piloop.plugins.host import ContractBroken
from piloop.plugins.provisioner.contract import Instance, InstanceState, Launch, Provisioning

type Open = Callable[[str], Provisioning]
"""The provider opened for an owner id."""


def _require(ok: bool, rule: str) -> None:
    if not ok:
        raise ContractBroken(rule)


def _until(found: Callable[[], bool], timeout: float, rule: str) -> None:
    deadline = time.monotonic() + timeout
    while not found():
        if time.monotonic() > deadline:
            raise ContractBroken(rule)
        time.sleep(min(1.0, timeout / 20))


def _named(p: Provisioning, name: str) -> list[Instance]:
    return [i for i in p.list() if i.name == name]


def check_provisioning(open_for: Open, launch: Launch, *, timeout: float = 120) -> None:
    """Check the provider that ``open_for`` opens, with one instance of ``launch``, waiting up to ``timeout`` seconds."""
    owner, other = open_for(f'conformance-{launch.name}'), open_for(f'conformance-other-{launch.name}')
    _require(not _named(owner, launch.name), 'list() shows no instance before its start')
    owner.stop(launch.name)  # a name that never started is gone already
    owner.start(launch)
    owner.start(launch)
    _until(lambda: len(_named(owner, launch.name)) == 1, timeout, 'list() shows a started instance once')
    [found] = _named(owner, launch.name)
    _require(found.pool == launch.pool, 'list() shows the pool of the instance')
    _require(not _named(other, launch.name), "list() shows only its owner's instances")
    owner.stop(launch.name)
    owner.stop(launch.name)
    _until(
        lambda: all(i.state is InstanceState.ENDED for i in _named(owner, launch.name)),
        timeout,
        'list() shows a stopped instance ended, or not at all',
    )
