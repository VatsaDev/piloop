"""``piloop plugins``: every capability, its providers, and which provider the configuration selects."""

from pathlib import Path

from piloop.cli import common
from piloop.cli.doctor import stored_projects
from piloop.config import ROLES, ProjectConfig
from piloop.daemon.main import preflight, unbound_failures
from piloop.plugins.host import Activatable, PluginError, Selection, declared, lookup
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
    home = cfg.model_dump(mode='json')
    found: list[str] = []
    for capability in declared():
        if (selection := capability.selection) is None:
            continue
        if selection.scope == 'home':
            if (name := lookup(home, selection.key)) is not None:
                found.append(f'{_provider(capability, str(name))} ({_key(selection)} in {config})')
            continue
        for project_name, path, project in stored_projects(cfg.home / 'piloop.db'):
            if isinstance(project, ProjectConfig):  # else piloop doctor says why the stored config does not load
                stored = f'in the config of {project_name} that the last `piloop init` stored from {Path(path)}'
                found += _in_project(capability, selection, project, f'{stored}/piloop.toml', cfg)
    return found


def _in_project(
    capability: Activatable, selection: Selection, project: ProjectConfig, stored: str, cfg: Settings
) -> list[str]:
    """What one project selects: for each role, when a role's table can select too."""
    data = project.model_dump(mode='json')
    field = ProjectConfig.model_fields.get(selection.key)
    default = field.default if field is not None else None
    roles = data['roles'] if selection.per_role else {}
    found: list[str] = []
    for role in roles or [None]:
        package = ROLES.providers().get(role) if role is not None else None
        if role is not None and (name := lookup(roles[role], selection.key)) is not None:
            from_package = package is not None and not package.source.built_in
            if from_package and lookup(package.value.model_dump(mode='json'), selection.key) == name:
                key = f'the default of the {role} role from {package.source.release}'
            else:
                key = f'[roles.{role}] {selection.key}'
        elif (name := lookup(data, selection.key)) is not None:
            key = f'{_key(selection)}, its default' if name == default else _key(selection)
        else:
            continue
        settings = selection.settings
        given = settings and lookup(cfg.model_dump(mode='json'), f'{settings}.{name}') is not None
        extra = f'; its settings: [{settings}.{name}] in {cfg.home / CONFIG_FILE}' if given else ''
        who = f'{project.name} {role}' if role is not None else project.name
        found.append(f'{who}: {_provider(capability, str(name))} ({key}, {stored}{extra})')
    return found


def _key(selection: Selection) -> str:
    return f'[{selection.table}] {selection.key.rpartition(".")[2]}' if selection.table else selection.key


def _provider(capability: Activatable, name: str) -> str:
    return f'{capability.noun} {name}, from {capability.sources()[name].release}'
