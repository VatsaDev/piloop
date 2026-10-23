"""Generate ``docs/reference/configuration.mdx``: every key of ``piloop.toml`` and of ``config.toml``."""

import inspect
from typing import get_args

from pydantic import BaseModel

from piloop.config import CampaignConfig
from piloop.deprecated import BRIDGES, CONFIG_BRIDGES
from piloop.plugins.host import Scope, declared
from piloop.plugins.runtime.herdr import HerdrSettings
from piloop.plugins.trigger.contract import TRIGGERS
from piloop.settings import StorageSettings
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
example `PILOOP_CAMPAIGN_RUNTIME=herdr` or `PILOOP_CAMPAIGN_ROLES__IMPLEMENTER__MAX_CONCURRENT=2`. The stored copy keeps the
override until the next `piloop init`. The variable has no effect on `piloop serve`.

A type in the tables links to the section that describes it. A table that you leave out has the defaults of its
section.

## Top-level keys

"""


def _section(model: type[BaseModel], defs: JsonSchema, only: set[str] | None = None) -> str:
    schema = json_schema(model)
    return f'\n## {model.__name__}\n\n{mdx(schema.get("description", ""))}\n\n{table(schema, defs, only)}'


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
        if model in shown:
            return ''
        shown.append(model)
        base = model.__mro__[1]
        if base is BaseModel or not issubclass(base, BaseModel):
            return _section(model, defs)
        return show(base) + _section(model, defs, _own_keys(model, base))

    for model in [*_nested(CampaignConfig, []), *TRIGGERS.builtins().values()]:
        out += show(model)
    return out + global_section() + deprecated_section()


def global_section() -> str:
    """Return the tables of the global settings file, ``$PILOOP_HOME/config.toml``: ``[storage]``, ``[plugins]``, runtimes."""
    storage = json_schema(StorageSettings)
    out = (
        '\n## Global settings: `config.toml`\n\n`$PILOOP_HOME/config.toml` holds the settings that every campaign of'
        ' one `PILOOP_HOME` shares. To override a key, set `PILOOP_<TABLE>__<KEY>`, for example'
        ' `PILOOP_STORAGE__MIN_FREE_GB`.\n\n### `[storage]`\n\n'
        f'{mdx(storage.get("description", ""))}\n\n{table(storage, {})}'
    )
    out += home_plugins()
    herdr = json_schema(HerdrSettings)
    out += (
        '\n### `[runtimes.<name>]`\n\nThe settings of a runtime, which the runtime reads and checks. An environment'
        ' variable wins over the file. The `herdr` runtime reads `[runtimes.herdr]`, and `PILOOP_HERDR_<KEY>`'
        ' overrides a key:\n\n'
        f'{table(herdr, {}, {k for k in herdr["properties"] if k.islower()})}'
    )
    return out


def deprecated_section() -> str:
    """Return the keys that ``piloop.deprecated`` still accepts, one bullet per bridge; nothing when there are none."""
    bullets = '\n'.join(f'- {mdx(" ".join(str(inspect.getdoc(b)).split()))}' for b in (*BRIDGES, *CONFIG_BRIDGES))
    return (
        '\n## Deprecated keys\n\nThese keys still work. Each one shows a `PiloopDeprecationWarning` that names the file,'
        ' the key, its replacement and the release that removes it. The changelog lists them too.\n\n'
        f'{bullets}\n'
    )


def home_plugins() -> str:
    """Return ``[plugins]`` of ``config.toml``: a table for each capability of the home, with its providers' keys."""
    out = (
        '\n### `[plugins]`\n\n`[plugins.<capability>]` selects the provider of a capability of the home: `provider`, the'
        " name of its entry point (the default without it), and the provider's own keys, which the daemon checks before"
        ' it starts.\n'
    )
    for capability in declared():
        if capability.choice is None or capability.choice.scope is not Scope.HOME:
            continue
        out += f'\n#### `[plugins.{capability.name}]`\n\nSelects the {capability.noun}.\n'
        for provider, model in capability.builtins().items():
            if isinstance(model, type) and issubclass(model, BaseModel):
                schema = json_schema(model)
                out += f'\nWith `provider = "{provider}"`:\n\n{table(schema, schema.get("$defs", {}))}'
                for inner in [m for m in _nested(model, []) if m.__name__ in schema.get('$defs', {})]:
                    nested = json_schema(inner)
                    out += f'\n#### {inner.__name__}\n\n{mdx(nested.get("description", ""))}\n\n{table(nested, {})}'
    return out
