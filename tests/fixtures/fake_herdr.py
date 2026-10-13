#!/usr/bin/env python3
"""Stand-in for the ``herdr`` CLI: records argv, answers with canned JSON shaped like herdr 0.8.2.

Behaviour is driven by two environment variables so tests can script it:
- ``FAKE_HERDR_LOG``: file to append one JSON line of argv per invocation (the ``--session <name>`` prefix included).
- ``FAKE_HERDR_SCRIPT``: JSON object mapping ``"<sub> <verb>"`` to a list of responses used in call order (the
  last one repeats). A response is ``{"result": {...}}`` (stdout, exit 0) ``{"error": {"code", "message"}}`` (stderr, exit 1)
  or ``{"text": "..."}`` (raw stdout, as ``read`` and ``session list --json`` do). Anything not scripted gets a sensible
  default; the unscripted ``agent get`` carries the ``agent_session`` Herdr detects once the harness has started
  (``SESSION_ID``), so a scripted ``agent get`` without it is an agent whose session Herdr never saw.

An unscripted ``pane run`` of a ``. <file>`` line runs it the way the pane's shell would, in the background, then writes
that shell's environment to ``$FAKE_HERDR_LOG.pane-env``; other command lines are only logged.
"""

import json
import os
import shlex
import subprocess
import sys

AGENT = {
    'agent': 'pi',
    'agent_status': 'idle',
    'state_change_seq': 5,
    'name': 'w1',
    'pane_id': 'w7:p2',
    'tab_id': 'w7:t2',
    'workspace_id': 'w7',
}
SESSION_ID = '01a092fd-3c1e-7b2a-9d4f-5e6a7b8c9d0e'
"""The harness session id the unscripted ``agent get`` reports, as Herdr's ``agent_session.value``."""
AGENT_SESSION = {'agent': 'pi', 'kind': 'id', 'source': 'herdr:pi', 'value': SESSION_ID}
SOCKET = '/tmp/fake-herdr/sessions/piloop/herdr.sock'
"""Socket of the fake's ``piloop`` session; a test that wants to be "inside" it sets HERDR_SOCKET_PATH to this."""
DEFAULTS = {
    'server': {'text': ''},
    'session list': {'text': json.dumps({'sessions': [{'name': 'piloop', 'running': True, 'socket_path': SOCKET}]})},
    'workspace list': {'result': {'type': 'workspace_list', 'workspaces': []}},
    'workspace create': {
        'result': {
            'type': 'workspace_created',
            'workspace': {'workspace_id': 'w7', 'label': 'demo', 'number': 1},
            'tab': {'tab_id': 'w7:t1', 'workspace_id': 'w7', 'label': 'demo'},
            'root_pane': {'pane_id': 'w7:p1', 'tab_id': 'w7:t1', 'workspace_id': 'w7'},
        }
    },
    'workspace report-metadata': {'text': ''},
    'tab create': {
        'result': {
            'type': 'tab_created',
            'tab': {'tab_id': 'w7:t2', 'workspace_id': 'w7', 'label': 'w1'},
            'root_pane': {'pane_id': 'w7:p2', 'tab_id': 'w7:t2', 'workspace_id': 'w7'},
        }
    },
    'tab list': {'result': {'type': 'tab_list', 'tabs': []}},
    'tab rename': {'result': {'type': 'tab_info', 'tab': {'tab_id': 'w7:t1', 'workspace_id': 'w7', 'label': 'home'}}},
    'pane split': {
        'result': {'type': 'pane_info', 'pane': {'pane_id': 'w7:p3', 'tab_id': 'w7:t1', 'workspace_id': 'w7'}}
    },
    'pane run': {'text': ''},
    'pane list': {
        'result': {'type': 'pane_list', 'panes': [{'pane_id': 'w7:p2', 'tab_id': 'w7:t2', 'workspace_id': 'w7'}]}
    },
    'tab focus': {'result': {'type': 'tab_info', 'tab': {'tab_id': 'w7:t2', 'workspace_id': 'w7', 'focused': True}}},
    'tab close': {'result': {'type': 'tab_closed', 'tab_id': 'w7:t2'}},
    'agent start': {'result': {'type': 'agent_started', 'agent': AGENT, 'argv': ['pi']}},
    'agent prompt': {'result': {'type': 'agent_prompted', 'agent': AGENT}},
    'agent wait': {'result': {'type': 'agent_info', 'agent': AGENT}},
    'agent read': {'text': 'hello from the agent\n'},
    'agent send-keys': {'result': {'type': 'agent_keys_sent', 'agent': AGENT}},
    'pane read': {'text': ''},
    'integration status': {
        'text': 'pi: current (v8) (/h/.pi/agent/extensions/herdr-agent-state.ts)\n'
        'claude: current (v8) (/h/.claude/hooks/herdr-agent-state.sh)\n'
        'codex: current (v8) (/h/.codex/herdr-agent-state.sh)\n'
    },
}


def command(argv: list[str]) -> str:
    """``"<sub> <verb>"`` of an argv, the global ``--session <name>`` skipped."""
    if argv[:1] == ['--session']:
        argv = argv[2:]
    return ' '.join(argv[:2])


def main() -> int:
    key = command(sys.argv[1:])
    log_path = os.environ['FAKE_HERDR_LOG']
    with open(log_path, 'a') as log:
        log.write(json.dumps(sys.argv[1:]) + '\n')
    scripted = json.loads(os.environ.get('FAKE_HERDR_SCRIPT', '{}')).get(key)
    with open(log_path) as log:
        seen = sum(1 for line in log if command(json.loads(line)) == key) - 1
    if scripted:
        # the n-th call to this subcommand gets the n-th scripted response; the last one repeats
        response = scripted[min(seen, len(scripted) - 1)]
    elif key == 'agent get':
        # the unscripted agent changes state every time it is looked at: a prompt is always picked up
        response = {
            'result': {
                'type': 'agent_info',
                'agent': {
                    **AGENT,
                    'agent_status': 'working',
                    'state_change_seq': 5 + seen,
                    'agent_session': AGENT_SESSION,
                },
            }
        }
    elif key == 'pane run' and sys.argv[-1].startswith('eval '):
        dump = shlex.quote(f'{log_path}.pane-env')
        shell = f'{sys.argv[-1]} && env > {dump}.tmp && mv {dump}.tmp {dump}'
        devnull = subprocess.DEVNULL  # piloop reads this fake's stdout to EOF; the shell must not hold it open
        subprocess.Popen(['sh', '-c', shell], stdin=devnull, stdout=devnull, stderr=devnull, start_new_session=True)
        response = DEFAULTS[key]
    elif key == 'pane process-info':
        # the pane's shell is whoever ran this fake: a live pid with a session of its own
        response = {'result': {'type': 'pane_process_info', 'process_info': {'shell_pid': os.getppid()}}}
    else:
        response = DEFAULTS.get(key, {'error': {'code': 'unknown_command', 'message': key}})
    ident = f'cli:{key.replace(" ", ":")}'
    if 'text' in response:  # `read` prints the terminal text itself, not JSON
        sys.stdout.write(str(response['text']))
        return 0
    if 'error' in response:
        print(json.dumps({'error': response['error'], 'id': ident}), file=sys.stderr)
        return 1
    print(json.dumps({'id': ident, 'result': response['result']}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
