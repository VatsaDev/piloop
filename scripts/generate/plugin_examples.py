"""The snippet of an example plugin in ``examples/plugins/``: its entry points, then its code, as they are."""

import tomllib

from scripts.generate import ROOT, header

EXAMPLES = ROOT / 'examples' / 'plugins'


def snippet(name: str, *files: str) -> str:
    """Return the entry point tables of ``examples/plugins/<name>/pyproject.toml``, then each of its ``files``."""
    root = EXAMPLES / name
    groups = tomllib.loads((root / 'pyproject.toml').read_text())['project']['entry-points']
    tables = '\n\n'.join(
        f'[project.entry-points."{group}"]\n' + '\n'.join(f'{n} = "{v}"' for n, v in eps.items())
        for group, eps in groups.items()
    )
    blocks = [f'```text pyproject.toml\n{tables}\n```']
    blocks += [f'```python {f}\n{(root / f).read_text().rstrip()}\n```' for f in files]
    return header(f'the example plugin in `examples/plugins/{name}/`') + '\n' + '\n\n'.join(blocks) + '\n'
