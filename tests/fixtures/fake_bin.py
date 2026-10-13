#!/usr/bin/env python3
"""Stand-in for any CLI piloop shells out to (``uv``, ``pi``): symlink it under the tool's name on PATH.

Appends one JSON line ``[<name>, *argv]`` to ``$FAKE_BIN_LOG`` and prints ``<name>: ok``; exits 1 (after printing
``<name>: boom`` on stderr) when ``$FAKE_BIN_FAIL`` is its name.
"""

import json
import os
import sys
from pathlib import Path


def main() -> int:
    name = Path(sys.argv[0]).name
    with open(os.environ['FAKE_BIN_LOG'], 'a') as log:
        log.write(json.dumps([name, *sys.argv[1:]]) + '\n')
    if os.environ.get('FAKE_BIN_FAIL') == name:
        print(f'{name}: boom', file=sys.stderr)
        return 1
    print(f'{name}: ok')
    return 0


if __name__ == '__main__':
    sys.exit(main())
