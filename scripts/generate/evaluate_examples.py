"""Generate ``docs/snippets/generated/evaluate-examples.mdx``: the bundled example projects."""

from piloop.campaign.project import EXAMPLE_DATA, TEMPLATES
from scripts.generate import SNIPPETS, header
from scripts.generate.markdown import cell

PATH = SNIPPETS / 'evaluate-examples.mdx'


def generate() -> str:
    """Return a table of the bundled example projects that ``piloop init --example`` writes: name, goal, data."""
    rows = ['| Example | Goal | Data it downloads |', '|---|---|---|']
    for name, data in EXAMPLE_DATA.items():
        goal = (TEMPLATES / name / 'brief.md').read_text().splitlines()[0].removeprefix('# ')
        rows.append(f'| `{name}` | {cell(goal)} | {", ".join(f"`{d}`" for d in data)} |')
    source = 'the briefs of the examples in `piloop/campaign/templates/`, or `EXAMPLE_DATA`'
    return header(source) + '\n' + '\n'.join(rows) + '\n'
