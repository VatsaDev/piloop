"""Generate ``docs/snippets/generated/imports.mdx``: the graph of the imports between the subpackages of piloop."""

import ast
from pathlib import Path

from scripts.generate import ROOT, SNIPPETS, header

PATH = SNIPPETS / 'imports.mdx'
PACKAGE = ROOT / 'piloop'


def _node(path: Path) -> str:
    """Return the top-level subpackage or module of ``piloop`` that the file ``path`` belongs to."""
    return path.relative_to(PACKAGE).parts[0].removesuffix('.py')


def _imported(tree: ast.Module) -> set[str]:
    """Return the top-level subpackages and modules of ``piloop`` that the import statements of ``tree`` name."""
    found: set[str] = set()
    for stmt in ast.walk(tree):
        if isinstance(stmt, ast.Import):
            names = [alias.name for alias in stmt.names]
        elif isinstance(stmt, ast.ImportFrom) and stmt.module == 'piloop':
            names = [f'piloop.{alias.name}' for alias in stmt.names]
        elif isinstance(stmt, ast.ImportFrom) and stmt.module is not None:
            names = [stmt.module]
        else:
            continue
        found |= {name.split('.')[1] for name in names if name.startswith('piloop.')}
    return found


def imports() -> dict[str, set[str]]:
    """Return, for each top-level subpackage and module of ``piloop``, the other ones that it imports."""
    graph: dict[str, set[str]] = {}
    for path in sorted(PACKAGE.rglob('*.py')):
        node = _node(path)
        if not node.startswith('__'):
            graph.setdefault(node, set()).update(_imported(ast.parse(path.read_text())) - {node})
    return graph


def _leaves(graph: dict[str, set[str]]) -> set[str]:
    """Return the top-level modules that import nothing from ``piloop`` but other such modules."""
    modules = {p.stem for p in PACKAGE.glob('*.py')} & graph.keys()
    leaves: set[str] = set()
    while grown := {m for m in modules - leaves if graph[m] <= leaves}:
        leaves |= grown
    return leaves


def _reachable(graph: dict[str, set[str]], start: str) -> set[str]:
    seen: set[str] = set()
    todo = [start]
    while todo:
        for nxt in graph[todo.pop()] - seen:
            seen.add(nxt)
            todo.append(nxt)
    return seen


def _label(node: str) -> str:
    return f'{node}/' if (PACKAGE / node).is_dir() else f'{node}.py'


def generate() -> str:
    """Return the import graph of the subpackages as a Mermaid flowchart, and a table of every direct import.

    The flowchart leaves out the shared leaf modules and every arrow that a longer path already implies, so that the
    layers show.
    """
    graph = imports()
    leaves = _leaves(graph)
    inner = {node: deps - leaves for node, deps in graph.items() if node not in leaves}
    lines = ['```mermaid', 'flowchart TB', *(f'    {node}["{_label(node)}"]' for node in sorted(inner))]
    for node, deps in sorted(inner.items()):
        for dep in sorted(deps):
            if not any(dep in _reachable(inner, other) for other in deps - {dep}):
                lines.append(f'    {node} --> {dep}')
    shared = ', '.join(f'`{_label(leaf)}`' for leaf in sorted(leaves))
    rows = ['| Subpackage or module | Imports directly |', '|---|---|']
    for node, deps in sorted(graph.items()):
        rows.append(f'| `{_label(node)}` | {", ".join(f"`{_label(d)}`" for d in sorted(deps)) or "nothing"} |')
    note = f'The graph leaves out the shared modules {shared}. Every layer may import them.'
    layout = '\n'.join([*lines, '```', '', note, '', *rows, ''])
    return header('the imports of the modules in `piloop/`') + '\n' + layout
