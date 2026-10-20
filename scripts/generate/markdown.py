"""Markdown from the code of piloop: the text of docstrings, and tables of the keys of models and of registries.

A docstring that a page needs and that is missing stops the generator. Every table comes from the built-in
registries, never the entry points of the installed packages, so the output is the same on every machine.
"""

import inspect
import json
import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from pydantic import BaseModel
from pydantic.json_schema import GenerateJsonSchema
from pydantic_settings import BaseSettings

from piloop.harnesses import DEFAULT_HARNESSES
from piloop.mcp import RENDERERS
from piloop.plugins.contracts.runtime import RUNTIMES
from piloop.triggers import TRIGGERS
from piloop.usage import READERS
from piloop.usage.jsonl import TALLIES

BUILT_IN: dict[tuple[str, str], Iterable[str]] = {
    ('ProjectConfig', 'runtime'): RUNTIMES.builtins(),
    ('RoleConfig', 'runtime'): RUNTIMES.builtins(),
    ('ProjectConfig', 'harnesses'): DEFAULT_HARNESSES,
    ('SweepRole', 'triggers'): TRIGGERS.builtins(),
    ('Trigger', 'kind'): TRIGGERS.builtins(),
    ('Harness', 'mcp'): RENDERERS,
    ('UsageSpec', 'reader'): READERS,
    ('UsageSpec', 'tally'): TALLIES,
}
"""The keys whose value names an entry of a registry, by model and key. Their description ends with the built-in names,
so no docstring has to list them."""
SECTION = re.compile(r'^[A-Z][a-z]+:$', re.MULTILINE)
"""The header of a Google-style docstring section (``Note:``, ``Raises:``). The text above the first one is for users."""
JsonSchema = dict[str, Any]  # JSON Schema is untyped by nature
CODE = re.compile(r'(``.+?``|`[^`]+`)')


def mdx(text: str) -> str:
    """Make docstring text safe for MDX: code spans with one backtick, and ``{}<>`` escaped outside code."""
    out = []
    for i, part in enumerate(CODE.split(text)):
        if i % 2:
            out.append(f'`{part.strip("`")}`')
        else:
            out.append(re.sub(r'([{}<>])', r'\\\1', part))
    return ''.join(out)


def cell(text: str) -> str:
    """Make text safe for one cell of a Markdown table."""
    return mdx(' '.join(text.split())).replace('|', '\\|')


def _anchor(name: str) -> str:
    return f'[{name}](#{name.lower()})'


def _type(prop: JsonSchema, defs: JsonSchema) -> str:
    if '$ref' in prop:
        name = str(prop['$ref']).rsplit('/', 1)[-1]
        target = defs[name]
        return _anchor(name) if 'properties' in target else _type(target, defs)
    if 'anyOf' in prop:
        return ' \\| '.join(_type(p, defs) for p in prop['anyOf'])
    if 'enum' in prop:
        return ' \\| '.join(f'`{v}`' for v in prop['enum'])
    t = str(prop.get('type', 'object'))
    if t == 'array' and 'prefixItems' in prop:  # a fixed-shape tuple
        return f'tuple\\[{", ".join(_type(p, defs) for p in prop["prefixItems"])}\\]'
    if t == 'array':
        return f'list\\[{_type(prop["items"], defs)}\\]'
    if t == 'object' and 'additionalProperties' in prop:
        return f'dict\\[string, {_type(prop["additionalProperties"], defs)}\\]'
    return t


def _default(name: str, prop: JsonSchema, schema: JsonSchema) -> str:
    if name in schema.get('required', []):
        return 'required'
    if '$ref' in prop and isinstance(prop.get('default'), dict):
        return '`{}`'  # a nested table left out: every key at the default that its own table shows
    return f'`{json.dumps(prop.get("default"))}`'


def _described(schema: JsonSchema, name: str) -> str:
    """Return the description of a key, with the built-in names of the registry that its value names (``BUILT_IN``)."""
    desc = str(schema['properties'][name].get('description', ''))
    if (names := BUILT_IN.get((schema['title'], name))) is not None:
        desc += ' Built in: ' + ', '.join(f'``{n}``' for n in sorted(names)) + '.'
    return desc


def table(schema: JsonSchema, defs: JsonSchema, only: set[str] | None = None) -> str:
    """Return a table of the keys of ``schema`` (or of ``only``): each type, default and description."""
    rows = ['| Key | Type | Default | Description |', '|---|---|---|---|']
    for name, prop in schema['properties'].items():
        if only is not None and name not in only:
            continue
        desc = cell(_described(schema, name))
        rows.append(f'| `{name}` | {_type(prop, defs)} | {_default(name, prop, schema)} | {desc} |')
    return '\n'.join(rows) + '\n'


def meanings(model: type[BaseModel], only: Iterable[str] | None = None) -> str:
    """Return a table of the keys of ``model`` (or of ``only``, in that order) with their descriptions."""
    schema = json_schema(model)
    rows = ['| Key | Meaning |', '|---|---|']
    rows += [f'| `{name}` | {cell(_described(schema, name))} |' for name in only or schema['properties']]
    return '\n'.join(rows) + '\n'


def json_schema(model: type[BaseModel]) -> JsonSchema:
    """Return the JSON schema of ``model``."""
    return model.model_json_schema(schema_generator=GenerateJsonSchema)


def doc(obj: object, what: str) -> str:
    """Return the docstring of ``obj``, cleaned. Stop when it has none: a page shows it."""
    text = getattr(obj, '__doc__', None)
    if not text or not text.strip():
        raise SystemExit(f'{what} has no docstring, and a page of the docs shows it: write one')
    return inspect.cleandoc(text)


def lead(obj: object, what: str) -> str:
    """Return the first paragraph of the docstring of ``obj``, on one line."""
    return ' '.join(doc(obj, what).split('\n\n')[0].split())


def told(obj: object, what: str) -> str:
    """Return the docstring of ``obj`` above its first section (``SECTION``), on one line: the part for users."""
    return ' '.join(SECTION.split(doc(obj, what))[0].split())


def own(cls: type, name: str) -> object:
    """Return the attribute ``name`` that ``cls`` itself defines. Stop when it only inherits one."""
    if name not in vars(cls):
        raise SystemExit(f'{cls.__name__} does not define {name}, and a page of the docs shows it')
    return vars(cls)[name]


def registry_table(head: str, entries: dict[str, object]) -> str:
    """Return a table of a registry of the code: each name, and the first paragraph of its docstring."""
    rows = [f'| {head} | What it does |', '|---|---|']
    rows += [f'| `{name}` | {cell(lead(entry, name))} |' for name, entry in entries.items()]
    return '\n'.join(rows) + '\n'


def environment(settings: type[BaseSettings]) -> str:
    """Return a table of the environment variables of a settings class: each name, its default and its meaning."""
    prefix = str(settings.model_config.get('env_prefix', ''))
    rows = ['| Variable | Default | Meaning |', '|---|---|---|']
    for name, field in settings.model_fields.items():
        alias = field.validation_alias
        variable = alias if isinstance(alias, str) else f'{prefix}{name}'.upper()
        value = field.default
        if isinstance(value, dict):  # [runtimes.<name>]: each runtime's own settings class has its variables
            continue
        if isinstance(value, BaseModel):  # a table of config.toml: one variable for each of its keys
            rows.append(
                f'| `{variable}__<KEY>` | [its table](/reference/configuration#{name}) | {cell(field.description or "")} |'
            )
            continue
        default = 'none' if value is None else f'`{str(value).replace(str(Path.home()), "~")}`'
        default = '`false`' if value is False else default
        rows.append(f'| `{variable}` | {default} | {cell(field.description or "")} |')
    return '\n'.join(rows) + '\n'
