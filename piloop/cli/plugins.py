"""``piloop plugins``: every capability, its providers, and which provider the configuration selects."""

from pathlib import Path

from piloop.cli import common
from piloop.cli.doctor import stored_campaigns
from piloop.config import CampaignConfig
from piloop.daemon.main import preflight, unbound_failures
from piloop.plugins.host import (
    Capability,
    Many,
    MissingProvider,
    One,
    Parameters,
    PluginError,
    Plugins,
    Scope,
    declared,
    refs,
)
from piloop.settings import CONFIG_FILE, Settings


def plugins_cmd() -> None:
    """Show every capability, each of its providers and the package it comes from, then what the configuration selects.

    It loads the entry points as the daemon does when it starts, and fails on one of a capability that the daemon
    uses that does not load, is not of its kind, or reuses a name. A broken package of a capability that nothing
    selects, which the daemon does not load, is shown with its error.
    """
    try:
        preflight(common.settings)
    except PluginError as e:
        raise SystemExit(str(e)) from e
    unbound = unbound_failures(common.settings)
    lines: list[str] = []
    for capability in declared():
        lines.append(f'{capability.noun}s ({capability.tier} tier, {capability.group}):')
        if (error := unbound.get(capability)) is not None:
            lines.append(f'  not loaded, and nothing selects a {capability.noun}: {error}')
            continue
        sources = capability.sources()
        width = max(map(len, sources), default=0)
        lines += [f'  {name:<{width}}  {source}' for name, source in sources.items()]
    lines += ['selected:', *(f'  {line}' for line in selections(common.settings))]
    common.console.print('\n'.join(lines), markup=False, highlight=False, soft_wrap=True)


def selections(cfg: Settings) -> list[str]:
    """Which provider the configuration selects for each capability, its package and version, and where from.

    ``piloop.toml`` is read as the daemon reads it: the copy, with the defaults of the package roles, that the last
    ``piloop init`` stored.
    """
    config = cfg.home / CONFIG_FILE
    campaigns = [
        (f'in the config of {c.name} that the last `piloop init` stored from {Path(c.path)}/piloop.toml', c.config)
        for c in stored_campaigns(cfg.home / 'piloop.db')
        if isinstance(c.config, CampaignConfig)  # else piloop doctor says why the stored config does not load
    ]
    found: list[str] = []
    for capability in declared():
        match capability.table:
            case One(scope=Scope.HOME):
                found += _chosen(capability, cfg.providers, f'in {config}')
            case One():  # a [plugins] table of piloop.toml
                for stored, campaign in campaigns:
                    found += _chosen(capability, campaign.plugins, stored, f'{campaign.name}: ')
            case Many(scope=scope):
                for stored, campaign in campaigns:
                    home = cfg.providers if scope is Scope.HOME else campaign.plugins
                    found += _named_in(capability, campaign, capability.instances(home), stored)
            case _:  # no table: nothing chooses it
                pass
    return found


def _chosen(capability: Capability[Parameters], plugins: Plugins, where: str, who: str = '') -> list[str]:
    """What ``[plugins.<name>]`` of one file chooses: nothing without the table."""
    if not isinstance(provider := plugins.get(capability.name), Parameters):
        return []
    name = capability.name_of(provider)
    return [f'{who}{_provider(capability, name)} ([plugins.{capability.name}] provider, {where})']


def _named_in(
    capability: Capability[Parameters], campaign: CampaignConfig, instances: dict[str, Parameters], stored: str
) -> list[str]:
    """The provider of each instance of ``capability`` that a key of the campaign names (``Ref``), and the key.

    An instance of no provider, such as a harness that only runs its argv, is left out.
    """
    found: list[str] = []
    for ref in refs(campaign):
        if ref.capability is not capability or (instance := instances.get(ref.name)) is None:
            continue
        try:
            provider = capability.name_of(instance)
        except MissingProvider:
            continue
        found.append(f'{campaign.name}: {_provider(capability, provider)} ({_key(ref.at)}, {stored})')
    return found


def _key(at: tuple[str | int, ...]) -> str:
    """Where a reference is, as a user writes it: ``runtime``, or ``[plugins.role.fast] runtime``."""
    *table, key = [str(k) for k in at if isinstance(k, str)]
    return f'[{".".join(table)}] {key}' if table else key


def _provider(capability: Capability[Parameters], name: str) -> str:
    return f'{capability.noun} {name}, from {capability.sources()[name].release}'
