"""The ``modal`` provisioner, against a stand-in for Modal's SDK: no network, no credentials, and the conformance
check of every provisioner."""

import re
import sys
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from itertools import count

import modal
import pytest
from pydantic import SecretStr

from piloop.otel import Tracer
from piloop.plugins.host import ContractBroken
from piloop.plugins.provisioner.contract import (
    PROVISIONERS,
    Instance,
    InstanceState,
    Launch,
    ProvisionerHost,
    StartRefused,
)
from piloop.plugins.provisioner.kit.conformance import check_provisioning
from piloop.plugins.provisioner.modal import Modal, ModalProvisioning

IDS = count(1)
LAUNCH = Launch(
    name='gpu-0123abcd',
    pool='gpu',
    argv=['piloop', 'worker', '--server', 'https://piloop.test', '--pool', 'gpu'],
    env={'PILOOP_HTTPS_TOKEN': SecretStr('plt_secret')},
)


@dataclass
class FakeSandbox:
    argv: tuple[str, ...]
    options: dict[str, object]
    object_id: str = field(default_factory=lambda: f'sb-{next(IDS)}')
    code: int | None = None

    def get_tags(self) -> dict[str, str]:
        tags = self.options['tags']
        assert isinstance(tags, dict)
        return tags

    def poll(self) -> int | None:
        return self.code

    def terminate(self) -> None:
        self.code = 137


@dataclass
class FakeModal:
    """What the stand-in holds: the sandboxes of every app, and what the next create raises."""

    sandboxes: list[FakeSandbox] = field(default_factory=list)
    apps: list[tuple[str, str | None]] = field(default_factory=list)
    raises: Exception | None = None
    ended_listed: bool = False
    """List a sandbox that ended too, as Modal does for a while."""

    def lookup(self, name: str, *, create_if_missing: bool = False, environment_name: str | None = None) -> object:
        assert create_if_missing
        self.apps.append((name, environment_name))
        return type('App', (), {'app_id': f'ap-{name}'})()

    def create(self, *argv: str, **options: object) -> FakeSandbox:
        if self.raises is not None:
            raise self.raises
        running = [s for s in self.sandboxes if s.options['name'] == options['name'] and s.code is None]
        if running:
            raise modal.exception.AlreadyExistsError(f'a sandbox is named {options["name"]}')
        sandbox = FakeSandbox(argv, options)
        self.sandboxes.append(sandbox)
        return sandbox

    def listed(self, *, app_id: str, tags: dict[str, str]) -> Iterator[FakeSandbox]:
        for sandbox in self.sandboxes:
            if (sandbox.code is None or self.ended_listed) and tags.items() <= sandbox.get_tags().items():
                yield sandbox


@pytest.fixture
def sdk(monkeypatch: pytest.MonkeyPatch) -> FakeModal:
    fake = FakeModal()
    monkeypatch.setattr(modal.App, 'lookup', fake.lookup)
    monkeypatch.setattr(modal.Sandbox, 'create', fake.create)
    monkeypatch.setattr(modal.Sandbox, 'list', fake.listed)
    monkeypatch.setattr(modal.Secret, 'from_dict', lambda env: ('secret', env))
    monkeypatch.setattr(modal.Secret, 'from_name', lambda name, environment_name=None: ('named', name))
    monkeypatch.setattr(modal.Image, 'from_registry', lambda tag: ('image', tag))
    return fake


def opened(owner: str, **keys: object) -> ModalProvisioning:
    config = PROVISIONERS.validated({'provider': 'modal', 'image': 'ghcr.io/acme/piloop-worker:0.3', **keys})
    assert isinstance(config, Modal)
    return config.open(ProvisionerHost(owner, Tracer(None)))


def test_modal_passes_the_provisioner_conformance_check(sdk: FakeModal) -> None:
    check_provisioning(opened, LAUNCH, timeout=1)


def test_a_sandbox_runs_the_argv_with_the_credential_in_a_secret_and_this_servers_tags(sdk: FakeModal) -> None:
    keys = {'gpu': 'A100', 'secrets': ['piloop-cas'], 'env': {'PILOOP_TICK_SECONDS': '2'}, 'timeout_hours': 2}
    provisioning = opened('owner-a', environment='dev', **keys)
    assert sdk.apps == [], 'opening touches no network'
    provisioning.start(LAUNCH)
    [sandbox] = sdk.sandboxes
    assert sandbox.argv == tuple(LAUNCH.argv) and 'plt_secret' not in ' '.join(sandbox.argv)
    assert sandbox.options['secrets'] == [('secret', {'PILOOP_HTTPS_TOKEN': 'plt_secret'}), ('named', 'piloop-cas')]
    assert sandbox.get_tags() == {'piloop-owner': 'owner-a', 'piloop-name': LAUNCH.name, 'piloop-pool': 'gpu'}
    assert (sandbox.options['gpu'], sandbox.options['timeout'], sandbox.options['env']) == (
        'A100',
        7200,
        {'PILOOP_TICK_SECONDS': '2'},
    )
    assert sandbox.options['image'] == ('image', 'ghcr.io/acme/piloop-worker:0.3')
    assert sdk.apps == [('piloop', 'dev')], 'one lookup of the app'
    [instance] = provisioning.list()
    assert (instance.name, instance.state, instance.handle) == (
        LAUNCH.name,
        InstanceState.RUNNING,
        {'id': sandbox.object_id},
    )


@pytest.mark.parametrize(
    'error',
    [
        modal.exception.InvalidError('no such GPU'),
        modal.exception.ResourceExhaustedError('no capacity'),
        modal.exception.NotFoundError('no secret piloop-cas'),
    ],
)
def test_what_modal_refuses_is_a_refused_start_and_anything_else_is_tried_again(
    sdk: FakeModal, error: Exception
) -> None:
    sdk.raises = error
    with pytest.raises(StartRefused, match=type(error).__name__):
        opened('owner-a').start(LAUNCH)
    sdk.raises = modal.exception.ConnectionError('reset')
    with pytest.raises(modal.exception.ConnectionError):
        opened('owner-a').start(LAUNCH)


def test_a_sandbox_that_ended_is_listed_ended_until_modal_drops_it(sdk: FakeModal) -> None:
    provisioning = opened('owner-a')
    provisioning.start(LAUNCH)
    sdk.sandboxes[0].code, sdk.ended_listed = 0, True
    assert [i.state for i in provisioning.list()] == [InstanceState.ENDED]


def test_the_modal_table_has_its_own_keys() -> None:
    with pytest.raises(ValueError, match='timeout_hours'):
        PROVISIONERS.validated({'provider': 'modal', 'image': 'i', 'timeout_hours': 25})
    with pytest.raises(ValueError, match='image'):
        PROVISIONERS.validated({'provider': 'modal'})


def test_modal_without_its_extra_says_how_to_install_it(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, 'modal', None)
    with pytest.raises(ImportError, match=r'piloop\[modal\]'):
        Modal(image='i').open(ProvisionerHost('owner-a', Tracer(None)))


@dataclass
class Broken:
    """A provisioner over the stand-in that breaks one rule: it forgets its starts, or lists every owner's."""

    real: ModalProvisioning
    sdk: FakeModal
    rule: str

    def list(self) -> Sequence[Instance]:
        match self.rule:
            case 'forgetful':
                return []
            case _:
                return [
                    Instance(name=s.get_tags()['piloop-name'], pool='gpu', state=InstanceState.RUNNING, handle={})
                    for s in self.sdk.sandboxes
                    if s.code is None
                ]

    def start(self, launch: Launch) -> None:
        self.real.start(launch)

    def stop(self, name: str) -> None:
        self.real.stop(name)


@pytest.mark.parametrize(
    ('broken', 'rule'),
    [('forgetful', 'list() shows a started instance once'), ('leaky', "list() shows only its owner's instances")],
)
def test_the_conformance_check_names_the_rule_that_a_provider_breaks(sdk: FakeModal, broken: str, rule: str) -> None:
    with pytest.raises(ContractBroken, match=re.escape(rule)):
        check_provisioning(lambda owner: Broken(opened(owner), sdk, broken), LAUNCH, timeout=0.05)
