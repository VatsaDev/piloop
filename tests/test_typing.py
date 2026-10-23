"""ty checks a provider in its own package: its overrides, its parameters, and the capability it is given to."""

import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).parent.parent
WRONG = Path(__file__).parent / 'typing' / 'wrong.py'


def test_ty_reports_one_error_on_each_wrong_line_of_a_provider_and_nothing_else() -> None:
    checked = subprocess.run(
        [Path(sys.executable).parent / 'ty', 'check', '--output-format', 'concise', WRONG.relative_to(ROOT)],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    found = Counter(int(m[1]) for m in re.finditer(r'^tests/typing/wrong\.py:(\d+):\d+: error', checked.stdout, re.M))
    marked = [n for n, line in enumerate(WRONG.read_text().splitlines(), 1) if line.endswith('# wrong')]
    assert len(marked) == 3
    assert found == Counter(marked), checked.stdout
