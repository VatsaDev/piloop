"""Generate ``docs/snippets/generated/sections.mdx``: the built-in sections."""

from piloop.plugins.section.contract import SECTIONS, Surface
from scripts.generate import SNIPPETS, header
from scripts.generate.markdown import cell, lead

PATH = SNIPPETS / 'sections.mdx'


def generate() -> str:
    """Return a table of the built-in sections, in their order: what each one shows, where by default, its parameters."""
    rows = ['| Section | Shows | Shown by default in | Order | Parameters |', '|---|---|---|---|---|']
    for name, section in sorted(SECTIONS.builtins().items(), key=lambda item: (item[1].ORDER, item[0])):
        params = ', '.join(f'`{field}`' for field in section.model_fields) or 'none'
        shown = ', '.join(s.value for s in Surface if s in section.SHOWN_IN) or 'none'
        rows.append(f'| `{name}` | {cell(lead(section, section.__name__))} | {shown} | {section.ORDER} | {params} |')
    source = 'the docstrings of the sections of the `piloop.sections` entry points in `pyproject.toml`, `piloop/plugins/section/`'
    return header(source) + '\n' + '\n'.join(rows) + '\n'
