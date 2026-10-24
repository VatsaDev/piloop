"""``piloop plugins``: every capability, its providers, and which provider the configuration selects."""

from pathlib import Path
from typing import Annotated, get_args, get_origin

from pydantic import BaseModel

from piloop.cli import common
from piloop.cli.doctor import stored_campaigns
from piloop.config import CampaignConfig
from piloop.daemon.main import preflight, unbound_failures
from piloop.plugins.host import Capability, Choice, Named, Parameters, PluginError, Plugins, Scope, declared
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
        (f'in the config of {name} that the last `piloop init` stored from {Path(path)}/piloop.toml', campaign)
        for name, path, campaign in stored_campaigns(cfg.home / 'piloop.db')
        if isinstance(campaign, CampaignConfig)  # else piloop doctor says why the stored config does not load
    ]
    found: list[str] = []
    for capability in declared():
        match capability.choice:
            case None:
                for stored, campaign in campaigns:
                    found += _named_in(capability, campaign, stored)
            case Choice(scope=Scope.HOME):
                found += _chosen(capability, cfg.providers, f'in {config}')
            case _:  # a [plugins] table of piloop.toml
                for stored, campaign in campaigns:
                    found += _chosen(capability, campaign.plugins, stored, f'{campaign.name}: ')
    return found


def _chosen(capability: Capability[Parameters], plugins: Plugins, where: str, who: str = '') -> list[str]:
    """What ``[plugins.<name>]`` of one file chooses: nothing without the table."""
    if (provider := plugins.get(capability.name)) is None:
        return []
    name = capability.name_of(provider)
    return [f'{who}{_provider(capability, name)} ([plugins.{capability.name}] provider, {where})']


def _named_in(capability: Capability[Parameters], campaign: CampaignConfig, stored: str) -> list[str]:
    """What the keys of one campaign that name a provider of ``capability`` name: at the top, then in each role."""
    roles = [(f'{campaign.name} {name}', f'[roles.{name}] ', role) for name, role in campaign.roles.all().items()]
    tables: list[tuple[str, str, BaseModel]] = [(campaign.name, '', campaign), *roles]
    return [
        f'{who}: {_provider(capability, name)} ({table}{key}, {stored})'
        for who, table, model in tables
        for key, name in _names(model, capability).items()
    ]


def _names(model: BaseModel, capability: Capability[Parameters]) -> dict[str, str]:
    """The fields of ``model`` that name a provider of ``capability`` (``Named``) and are set, with the name."""
    found: dict[str, str] = {}
    for field, info in type(model).model_fields.items():
        optional = [m for h in get_args(info.annotation) if get_origin(h) is Annotated for m in h.__metadata__]
        metadata = [*info.metadata, *optional]
        named = any(isinstance(m, Named) and m.capability is capability for m in metadata)
        if named and isinstance(value := getattr(model, field), str):
            found[field] = value
    return found


def _provider(capability: Capability[Parameters], name: str) -> str:
    return f'{capability.noun} {name}, from {capability.sources()[name].release}'
