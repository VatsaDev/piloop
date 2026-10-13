#!/usr/bin/env python3
"""Stand-in for the Entire CLI (0.10.6) as piloop drives it: symlink it as ``entire`` on PATH.

Appends one JSON line ``{"argv": [...], "cwd": "..."}`` to ``$FAKE_ENTIRE_LOG`` and answers like the real binary does
non-interactively:
- ``version``: ``Entire CLI <$FAKE_ENTIRE_VERSION>`` (default 0.10.6) and two more lines.
- ``session attach``, per ``$FAKE_ENTIRE_MODE``: ``created`` (default), ``added`` (HEAD's message carried an
  Entire-Checkpoint trailer), ``existing`` (a retry of an attached session), ``fail`` (exit 1 with the error on stderr),
  ``silent`` (exit 0 but the session gets no checkpoint) or ``hang``.
- ``session info <sid> --json``: the session's state; ``last_checkpoint_id`` is ``CHECKPOINT`` unless the mode is
  ``silent``, ``agent`` is Entire's display name for the ``--agent`` the attach in the log named.
Anything else prints ``entire: ok``.
"""

import json
import os
import sys
import time

CHECKPOINT = '01M29FSXJHMJ9X718SBKJ1K0E2'
TOKENS = 234444
AGENT_NAMES = {'pi': 'Pi', 'claude-code': 'Claude Code', 'codex': 'Codex'}
CREATED = """Attached session {sid}
  Captured: 1 turn · gemini-flash-latest · 24.4k tokens
  Created checkpoint {cp}

Last commit: b13acf5 base

Copy to your commit message to attach:

  Entire-Checkpoint: {cp}
"""
ADDED = """Attached session {sid}
  Captured: 1 turn · gemini-flash-latest · 1.3k tokens
  Added to existing checkpoint {cp}
"""
EXISTING = """Session {sid} already has checkpoint {cp}

Last commit: b13acf5 base

Copy to your commit message to attach:

  Entire-Checkpoint: {cp}
"""
OUTPUTS = {'created': CREATED, 'added': ADDED, 'existing': EXISTING}


def info(sid: str, mode: str, log_path: str) -> dict[str, object]:
    with open(log_path) as log:
        attaches = [json.loads(line)['argv'] for line in log]
    agents = [argv[4] for argv in attaches if argv[:4] == ['session', 'attach', sid, '--agent'] and len(argv) > 4]
    return {
        'session_id': sid,
        'agent': AGENT_NAMES.get(agents[-1], agents[-1]) if agents else 'Unknown',
        'status': 'ended',
        'started_at': '2026-09-12T01:13:58.859194948Z',
        'last_active': '2026-09-12T01:13:58.859194948Z',
        'turns': 1,
        'checkpoints': 0,
        'last_checkpoint_id': None if mode == 'silent' else CHECKPOINT,
        'tokens': {'total': TOKENS, 'input': 22440, 'cache_read': 209024, 'cache_write': 0, 'output': 2980},
        'last_prompt': '# Worker\n',
    }


def main() -> int:
    log_path = os.environ['FAKE_ENTIRE_LOG']
    with open(log_path, 'a') as log:
        log.write(json.dumps({'argv': sys.argv[1:], 'cwd': os.getcwd()}) + '\n')
    mode = os.environ.get('FAKE_ENTIRE_MODE', 'created')
    if sys.argv[1:2] == ['version']:
        print(
            f'Entire CLI {os.environ.get("FAKE_ENTIRE_VERSION", "0.10.6")}\nGo version: go1.26.6\nOS/Arch: linux/amd64'
        )
        return 0
    if sys.argv[1:3] == ['session', 'info']:
        print(json.dumps(info(sys.argv[3], mode, log_path), indent=2))
        return 0
    if sys.argv[1:3] != ['session', 'attach']:
        print('entire: ok')
        return 0
    if mode == 'fail':
        print(f'Error: invalid session ID: {sys.argv[3]}', file=sys.stderr)
        return 1
    if mode == 'hang':
        time.sleep(30)
    if mode in OUTPUTS:
        print(OUTPUTS[mode].format(sid=sys.argv[3], cp=CHECKPOINT), end='')
    return 0


if __name__ == '__main__':
    sys.exit(main())
