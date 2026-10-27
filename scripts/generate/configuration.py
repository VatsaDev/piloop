"""Generate ``docs/reference/configuration.mdx``: every key of ``piloop.toml`` and of ``config.toml``."""

import inspect
from typing import get_args

from pydantic import BaseModel, RootModel

from piloop.config import CampaignConfig
from piloop.deprecated import BRIDGES, CONFIG_BRIDGES
from piloop.plugins.harness.contract import HARNESSES
from piloop.plugins.host import Capability, Many, One, Parameters, Pick, Scope, declared
from piloop.plugins.role.contract import ROLES
from piloop.plugins.trigger.contract import TRIGGERS
from piloop.settings import Settings
from scripts.generate import ROOT, header
from scripts.generate.markdown import JsonSchema, json_schema, mdx, table

PATH = ROOT / 'docs' / 'reference' / 'configuration.mdx'
SOURCE = (
    'the docstrings of the models that `CampaignConfig` reaches, `piloop/deprecated.py`, or'
    ' `scripts/generate/configuration.py`'
)
HEAD = f"""---
title: Configuration
description: Every key of piloop.toml, with its type and its default.
---

{header(SOURCE)}
`piloop.toml` is at the root of the campaign repo. `piloop init` reads it again each time you run it, and stores a
copy. The daemon uses that copy, so an edit takes effect only when you run `piloop init`.

To override a key when `piloop init` reads the file, set `PILOOP_CAMPAIGN_<KEY>`. Join nested keys with `__`, for
example `PILOOP_CAMPAIGN_PLUGINS__RUNTIME__USE=subprocess` or `PILOOP_CAMPAIGN_PLUGINS__ROLE__IMPLEMENTER__MAX_CONCURRENT=2`. The stored copy keeps the
override until the next `piloop init`. The variable has no effect on `piloop serve`.

A type in the tables links to the section that describes it. A table that you leave out has the defaults of its
section.

## Top-level keys

"""


def _section(model: type[BaseModel], defs: JsonSchema, only: set[str] | None = None) -> str:
    schema = json_schema(model)
    own = defs | schema.get('$defs', {})
    return f'\n## {model.__name__}\n\n{mdx(schema.get("description", ""))}\n\n{table(schema, own, only)}'


def _nested(model: type[BaseModel], found: list[type[BaseModel]]) -> list[type[BaseModel]]:
    """Return the models reachable from the fields of ``model``, in declaration order, each once."""
    for field in model.model_fields.values():
        for sub in _models_in(field.annotation):
            if sub not in found:
                found.append(sub)
                _nested(sub, found)
    return found


def _models_in(annotation: object) -> list[type[BaseModel]]:
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return [annotation]
    return [m for arg in get_args(annotation) for m in _models_in(arg)]


def _own_keys(model: type[BaseModel], base: type[BaseModel]) -> set[str]:
    """Return the keys that ``model`` adds to ``base`` or gives another default."""
    mine, theirs = json_schema(model)['properties'], json_schema(base)['properties']
    return {name for name, prop in mine.items() if theirs.get(name) != prop}


def generate() -> str:
    """Return the configuration reference: the top-level keys, then one table per nested model, then the triggers.

    A model that subclasses another config model gets the table of its base once, and then a table of the keys that
    it adds or gives another default.
    """
    top = json_schema(CampaignConfig)
    defs = top.get('$defs', {})
    out = HEAD + table(top, defs)
    shown: list[type[BaseModel]] = []

    def show(model: type[BaseModel]) -> str:
        if model in shown or issubclass(model, RootModel):  # a table of tables: the key above describes it
            return ''
        shown.append(model)
        base = model.__mro__[1]
        if base in (BaseModel, Parameters) or not issubclass(base, BaseModel):
            return _section(model, defs)
        return show(base) + _section(model, defs, _own_keys(model, base))

    for model in _nested(CampaignConfig, []):
        out += show(model)
    out += plugins_section(Scope.CAMPAIGN, '##')
    for model in [HARNESSES.contract, *ROLES.builtins().values(), *TRIGGERS.builtins().values()]:
        out += show(model)
    return out + global_section() + deprecated_section()


def global_section() -> str:
    """Return the tables of ``$PILOOP_HOME/config.toml``: each model table of ``Settings``, then the plugins and runtimes."""
    out = (
        '\n## Global settings: `config.toml`\n\n`$PILOOP_HOME/config.toml` holds the settings that every campaign of'
        ' one `PILOOP_HOME` shares. To override a key, set `PILOOP_<TABLE>__<KEY>`, for example'
        ' `PILOOP_STORAGE__MIN_FREE_GB`.\n'
    )
    for name, field in Settings.model_fields.items():
        for model in _models_in(field.annotation):
            schema = json_schema(model)
            out += f'\n### `[{name}]`\n\n{mdx(schema.get("description", ""))}\n\n{table(schema, {})}'
    return out + plugins_section(Scope.HOME, '###')


def deprecated_section() -> str:
    """Return the keys that ``piloop.deprecated`` still accepts, one bullet per bridge; nothing when there are none."""
    bullets = '\n'.join(
        f'- {mdx(" ".join(str(inspect.getdoc(b.rewrite)).split()))}' for b in (*BRIDGES, *CONFIG_BRIDGES)
    )
    return (
        '\n## Deprecated keys\n\nThese keys still work. Each one shows a `PiloopDeprecationWarning` that names the file,'
        ' the key, its replacement and the release that removes it. The changelog lists them too.\n\n'
        f'{bullets}\n'
    )


PLUGINS = {
    Scope.CAMPAIGN: '`[plugins]` of `piloop.toml` holds the plugins of the campaign.',
    Scope.HOME: '`[plugins]` of `config.toml` holds the plugins of the home, which the daemon checks before it starts.',
}


def plugins_section(scope: Scope, level: str) -> str:
    """Return ``[plugins]`` of the file of ``scope``: a section for each of its capabilities, with its providers' keys.

    A capability of one instance has ``[plugins.<name>]``, and one of several has ``[plugins.<name>.<instance>]``, and
    ``use`` in ``[plugins.<name>]`` when it picks the instances in use.
    """
    out = (
        f'\n{level} `[plugins]`\n\n{PLUGINS[scope]} A capability that has one instance takes `[plugins.<capability>]`:'
        ' `provider`, the name of its entry point (the default without it), and the keys of that provider. One that'
        ' has several takes `[plugins.<capability>.<name>]` for each: its `provider` is the one called `<name>` when'
        ' the table has none, and each provider that needs no keys is an instance of its own name with its defaults.'
        ' `[plugins.<capability>]` itself holds the settings of such a capability: `use` names the instances that the'
        ' campaign uses, and is unset for the providers that are on by default (`DEFAULT`). No instance is called'
        ' `use`.\n'
    )
    for capability in declared():
        match capability.table:
            case One(scope=found, default=default) if found is scope:
                chosen = f' Without a table: `{default}`.' if default is not None else ''
                out += f'\n{level}# `[plugins.{capability.name}]`\n\nThe {capability.noun}.{chosen}\n'
                out += _providers(capability, level + '#')
            case Many(scope=found, use=use):
                if use is not None and use.scope is scope:
                    out += _use(capability, use.pick, level)
                if found is scope:
                    out += _instances(capability, scope, level)
            case _:
                pass
    return out


def _use(capability: Capability[Parameters], pick: Pick, level: str) -> str:
    """Return ``use`` of ``[plugins.<name>]``: what it names, and its default."""
    each = capability.name.replace('_', ' ')
    used = ', '.join(f'`{name}`' for name in capability.used({}))
    what = {
        Pick.ONE: f'the {each} instance that the campaign uses, by name',
        Pick.ORDERED: f'the {each} instances that the campaign uses, by name, in order',
    }[pick]
    return f'\n{level}# `[plugins.{capability.name}] use`\n\n{what.capitalize()}. Unset: {used}.\n'


def _instances(capability: Capability[Parameters], scope: Scope, level: str) -> str:
    """Return ``[plugins.<name>.<instance>]``: the keys of an instance."""
    each = capability.name.replace('_', ' ')
    out = f'\n{level}# `[plugins.{capability.name}.<name>]`\n\nEach {each} by name.\n'
    contract = capability.contract.__name__
    if scope is Scope.CAMPAIGN:
        return out + f'\nEach table takes the keys of [{contract}](#{contract.lower()}), and those of its provider.\n'
    return out + _providers(capability, level + '#')


def _providers(capability: Capability[Parameters], level: str) -> str:
    """Return the keys of each built-in provider of ``capability``, and the models that its keys reach."""
    out = ''
    for provider, model in capability.builtins().items():
        schema = json_schema(model)
        if not schema.get('properties'):
            out += f'\nWith `provider = "{provider}"`, the table has no other key.\n'
            continue
        out += f'\nWith `provider = "{provider}"`:\n\n{table(schema, schema.get("$defs", {}))}'
        for inner in [m for m in _nested(model, []) if m.__name__ in schema.get('$defs', {})]:
            nested = json_schema(inner)
            description = mdx(nested.get('description', ''))
            out += f'\n{level} {inner.__name__}\n\n{description}\n\n{table(nested, nested.get("$defs", {}))}'
    return out
