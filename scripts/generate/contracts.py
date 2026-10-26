"""Generate ``docs/snippets/generated/contract-*.mdx``: the members of each contract on the extension points page.

Each snippet is a table of the members of the classes that a capability's contract names, from the code: each public
method with its signature, and each documented attribute (a field, a ``ClassVar``) with its type. The text of each row
is the first paragraph of its docstring. The page keeps only the narrative by hand, so a contract never drifts from it.
"""

import ast
import inspect
import textwrap
from annotationlib import Format
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from piloop.plugins.cas.contract import Cas, CasProvider, Evictable, Upkeep
from piloop.plugins.executor.contract import Executor, ExecutorConfig
from piloop.plugins.fact_source import facts
from piloop.plugins.fact_source.contract import FactSource
from piloop.plugins.fact_source.kit.readings import Readings
from piloop.plugins.network.contract import Network, NetworkConfig
from piloop.plugins.section.contract import Rendering, Section
from piloop.plugins.stop_rule.contract import Escalate, Go, Hold, Stop, StopCase, StopRule
from piloop.plugins.trigger.contract import Fire, Pass, Trigger
from piloop.plugins.verdict_rule.contract import (
    Branch,
    Case,
    Criterion,
    Judgement,
    Moved,
    Node,
    Rebase,
    Side,
    VerdictRule,
)
from scripts.generate import SNIPPETS, header
from scripts.generate.markdown import cell, lead


@dataclass(frozen=True)
class Contract:
    """One snippet: the classes whose members it lists, in order, and where they are defined."""

    classes: tuple[type, ...]
    source: str


CONTRACTS: dict[str, Contract] = {
    'cas': Contract((CasProvider, Cas, Upkeep, Evictable), '`piloop/plugins/cas/contract.py`'),
    'executor': Contract((ExecutorConfig, Executor), '`piloop/plugins/executor/contract.py`'),
    'network': Contract((NetworkConfig, Network), '`piloop/plugins/network/contract.py`'),
    'verdict-rule': Contract(
        (VerdictRule, Case, Criterion, Side, Judgement, Node, Branch, Moved, Rebase),
        '`piloop/plugins/verdict_rule/contract.py`',
    ),
    'trigger': Contract((Trigger, Pass, Fire), '`piloop/plugins/trigger/contract.py`'),
    'section': Contract((Section, Rendering), '`piloop/plugins/section/contract.py`'),
    'stop-rule': Contract((StopRule, StopCase), '`piloop/plugins/stop_rule/contract.py`'),
    'fact-source': Contract((FactSource, Readings), '`piloop/plugins/fact_source/`'),
}
"""Each contract snippet by the end of its name: ``contract-<name>.mdx``."""


def generate_each() -> dict[Path, str]:
    """Return each generated file of this module by its path: the contracts, the answers of a stop rule, the facts."""
    found = {
        SNIPPETS / f'contract-{name}.mdx': header(f'the docstrings in {c.source}') + '\n' + _members(c.classes)
        for name, c in CONTRACTS.items()
    }
    found[SNIPPETS / 'stop-answers.mdx'] = (
        header('the docstrings in `piloop/plugins/stop_rule/contract.py`') + '\n' + _answers()
    )
    found[SNIPPETS / 'facts.mdx'] = header('`piloop/plugins/fact_source/facts.py`') + '\n' + _facts()
    return found


def _members(classes: tuple[type, ...]) -> str:
    rows = ['| Member | What it is |', '|---|---|']
    for cls in classes:
        attributes = _attribute_docs(cls)
        for name, (annotation, text) in attributes.items():
            rows.append(f'| {_code(f"{cls.__name__}.{name}: {annotation}")} | {cell(text)} |')
        for name, member in vars(cls).items():
            fn = member.__func__ if isinstance(member, classmethod | staticmethod) else member
            if name.startswith('_') or not inspect.isfunction(fn):
                continue
            rows.append(
                f'| {_code(f"{cls.__name__}.{name}{_signature(fn)}")} | {cell(lead(fn, f"{cls.__name__}.{name}"))} |'
            )
    return '\n'.join(rows) + '\n'


def _signature(fn: Callable[..., object]) -> str:
    """The signature of a method without ``self`` or ``cls``, with its annotations as the code writes them."""
    found = inspect.signature(fn, annotation_format=Format.STRING)
    params = [p for i, p in enumerate(found.parameters.values()) if not (i == 0 and p.name in ('self', 'cls'))]
    return str(found.replace(parameters=params)).replace("'", '')


def _code(text: str) -> str:
    """``text`` as code in a cell of a table: its pipes escaped, as GFM needs inside a cell."""
    return f'`{text}`'.replace('|', '\\|')


def _annotation(node: ast.expr) -> str:
    """The annotation as a reader needs it: ``Annotated[T, ...]`` is ``T``, wherever it is."""
    holder = ast.Expression(body=node)
    for parent in ast.walk(holder):
        for field, value in ast.iter_fields(parent):
            while _annotated(value):
                value = value.slice.elts[0]
                setattr(parent, field, value)
    return ast.unparse(holder.body)


def _annotated(node: object) -> bool:
    return (
        isinstance(node, ast.Subscript)
        and isinstance(node.value, ast.Name)
        and node.value.id == 'Annotated'
        and isinstance(node.slice, ast.Tuple)
    )


def _attribute_docs(cls: type) -> dict[str, tuple[str, str]]:
    """The attributes that ``cls`` itself annotates, each with the docstring right under it, in order.

    One without a docstring stops the generator: the page shows every attribute of a contract.
    """
    tree = ast.parse(textwrap.dedent(inspect.getsource(cls)))
    body = tree.body[0].body if isinstance(tree.body[0], ast.ClassDef) else []
    found: dict[str, tuple[str, str]] = {}
    for node, after in zip(body, [*body[1:], None], strict=True):
        if not (isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)):
            continue
        if not (
            isinstance(after, ast.Expr) and isinstance(after.value, ast.Constant) and isinstance(after.value.value, str)
        ):
            raise SystemExit(
                f'{cls.__name__}.{node.target.id} has no docstring, and a page of the docs shows it: write one'
            )
        text = inspect.cleandoc(after.value.value).split('\n\n')[0]
        found[node.target.id] = (_annotation(node.annotation), text)
    return found


def _answers() -> str:
    """The answers of a stop rule, and what piloop does with each: the first paragraph of each class's docstring."""
    rows = ['| Answer | What piloop does |', '|---|---|']
    for answer in (Go, Hold, Stop, Escalate):
        fields = ', '.join(_attribute_docs(answer))
        rows.append(f'| `{answer.__name__}({fields})` | {cell(lead(answer, answer.__name__))} |')
    return '\n'.join(rows) + '\n'


def _facts() -> str:
    """The facts of the built-in sources ``campaign`` and ``optimization``, and the ratios over facts that a rule names."""
    rows = ['| Fact | Source | Type | What it is |', '|---|---|---|---|']
    for source, supplied in (('campaign', facts.CAMPAIGN), ('optimization', facts.OPTIMIZATION)):
        for name in [n for n, v in vars(facts).items() if isinstance(v, facts.Fact) and v in supplied]:
            fact = getattr(facts, name)
            rows.append(
                f'| `{name}` (`{fact.name}`) | `{source}` | {_code(_type_of(name).strip("`"))} | {cell(fact.doc)} |'
            )
    ratios = ['| Ratio | What it is |', '|---|---|']
    for fn in (
        facts.reading,
        facts.since_keep,
        facts.per_keep,
        facts.per_evaluation,
        facts.worker_spend,
        facts.per_run,
    ):
        ratios.append(f'| {_code(fn.__name__ + _plain(fn))} | {cell(lead(fn, fn.__name__))} |')
    return '\n'.join(rows) + '\n\n' + '\n'.join(ratios) + '\n'


def _plain(fn: Callable[..., object]) -> str:
    return str(inspect.signature(fn, annotation_format=Format.STRING)).replace("'", '')


def _type_of(name: str) -> str:
    """The type parameter of a fact, as ``facts.py`` writes it: ``Fact[int](...)``."""
    tree = ast.parse(inspect.getsource(facts))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in node.targets):
            call = node.value
            if isinstance(call, ast.Call) and isinstance(call.func, ast.Subscript):
                return f'`{ast.unparse(call.func.slice)}`'
    raise SystemExit(f'{name} in piloop/plugins/fact_source/facts.py is not a Fact[<type>](...)')
