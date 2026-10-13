"""MCP servers: the definition, the variables it names, and each harness's renderer."""

import json
import os
import re
import sys
import tomllib
from pathlib import Path

import pytest
from pydantic import ValidationError

from piloop.config import ProjectConfig
from piloop.db.models import WorkerKind
from piloop.harnesses import DEFAULT_HARNESSES, Harness
from piloop.mcp import (
    MCP,
    RENDERERS,
    Launch,
    McpError,
    McpServer,
    Rendered,
    codex,
    env_names,
    expand,
    forwarded_env,
    missing_env,
)
from tests.conftest import write_script

EXA = McpServer(url='https://mcp.exa.ai/mcp', headers_env={'x-api-key': 'EXA_API_KEY'})
FIRECRAWL = McpServer(url='https://mcp.firecrawl.dev/v2/mcp', bearer_token_env='FIRECRAWL_API_KEY')
OPEN = McpServer(url='https://open.example/mcp')
XAI = McpServer(command=['python3', 'xai.py', 'grok'], env=['XAI_API_KEY', 'EXA_API_KEY'])
PLAIN = McpServer(command=['cat'])


def launch(tmp_path: Path, servers: dict[str, McpServer], executable: str = 'codex') -> Launch:
    return Launch(servers, executable, tmp_path, tmp_path, dict(os.environ), [MCP])


@pytest.mark.parametrize(
    ('table', 'error'),
    [
        ({}, 'either url (streamable HTTP) or command (stdio)'),
        ({'url': 'https://x', 'command': ['x']}, 'either url'),
        ({'command': ['x'], 'headers_env': {'a': 'B'}}, 'headers_env and bearer_token_env apply to a url server only'),
        ({'command': ['x'], 'bearer_token_env': 'B'}, 'apply to a url server only'),
        ({'url': 'https://x', 'env': ['A']}, 'env applies to a command server only'),
        ({'command': []}, 'at least 1 item'),
        ({'url': 'https://x', 'bearer_token_env': 'B', 'headers_env': {'AUTHORIZATION': 'C'}}, 'drop it from'),
        ({'url': 'https://x', 'headers_env': {'a': 'not a name'}}, 'String should match pattern'),
        ({'command': ['x'], 'env': ['1X']}, 'String should match pattern'),
        ({'url': 'https://x', 'headers': {'a': 'b'}}, 'Extra inputs are not permitted'),
    ],
)
def test_a_server_is_http_or_stdio_and_names_variables_where_they_belong(table: dict[str, object], error: str) -> None:
    with pytest.raises(ValidationError, match=re.escape(error)):
        McpServer.model_validate(table)


def test_the_variables_servers_need_are_forwarded_by_name_and_a_missing_one_fails_naming_it() -> None:
    assert XAI.env_names == ['XAI_API_KEY', 'EXA_API_KEY']
    assert env_names([EXA, FIRECRAWL, XAI, OPEN]) == ['EXA_API_KEY', 'FIRECRAWL_API_KEY', 'XAI_API_KEY'], 'each once'
    assert missing_env([EXA, XAI], ['EXA_API_KEY']) == ['XAI_API_KEY']
    environ = {'EXA_API_KEY': 'e', 'XAI_API_KEY': 'x', 'OTHER': 'o'}
    assert forwarded_env({'exa': EXA, 'xai': XAI}, environ) == {'EXA_API_KEY': 'e', 'XAI_API_KEY': 'x'}
    assert forwarded_env({}, environ) == {}
    with pytest.raises(McpError, match=r"servers \['exa', 'fc'\] need FIRECRAWL_API_KEY in the environment") as e:
        forwarded_env({'exa': EXA, 'fc': FIRECRAWL}, {'EXA_API_KEY': 'secret-value'})
    assert 'secret-value' not in str(e.value)


def test_expand_replaces_only_the_whole_mcp_element() -> None:
    argv = ['-p', MCP, '--settings', 'x{mcp}y']
    assert expand(argv, Rendered(['a', 'b'])) == ['-p', 'a', 'b', '--settings', 'x{mcp}y']
    assert expand(argv, Rendered([])) == ['-p', '--settings', 'x{mcp}y']


def test_claude_is_strict_and_inlines_its_servers_with_variable_references(tmp_path: Path) -> None:
    render = RENDERERS['claude'].render
    assert render(launch(tmp_path, {})) == Rendered(['--strict-mcp-config']), "no servers: none of the user's either"
    rendered = render(launch(tmp_path, {'exa': EXA, 'fc': FIRECRAWL, 'open': OPEN, 'xai': XAI, 'cat': PLAIN}))
    assert rendered.args[:2] == ['--strict-mcp-config', '--mcp-config'] and rendered.env == {}
    assert json.loads(rendered.args[2])['mcpServers'] == {
        'exa': {'type': 'http', 'url': 'https://mcp.exa.ai/mcp', 'headers': {'x-api-key': '${EXA_API_KEY}'}},
        'fc': {
            'type': 'http',
            'url': 'https://mcp.firecrawl.dev/v2/mcp',
            'headers': {'Authorization': 'Bearer ${FIRECRAWL_API_KEY}'},
        },
        'open': {'type': 'http', 'url': 'https://open.example/mcp'},
        'xai': {
            'type': 'stdio',
            'command': 'python3',
            'args': ['xai.py', 'grok'],
            'env': {'XAI_API_KEY': '${XAI_API_KEY}', 'EXA_API_KEY': '${EXA_API_KEY}'},
        },
        'cat': {'type': 'stdio', 'command': 'cat', 'args': []},
    }


FAKE_CODEX = r"""
import json, os, sys

args = sys.argv[1:]
with open(sys.argv[0] + '.calls', 'a') as f:
    f.write(json.dumps({'cwd': os.getcwd(), 'args': args}) + '\n')
flags = [value for flag, value in zip(args, args[1:]) if flag == '-c']
servers = json.loads(os.environ.get('FAKE_CODEX_LISTED', '{}'))
plugins = [] if 'features.plugins=false' in flags else os.environ.get('FAKE_CODEX_PLUGINS', '').split()
servers |= dict.fromkeys(plugins, True)
for flag in flags:
    key, _, value = flag.partition('=')
    if key.startswith(os.environ.get('FAKE_CODEX_REJECTS') or '\0') or (
        key.startswith('mcp_servers.') and key.endswith('.enabled') and key.split('.')[1] in plugins
    ):
        sys.exit(f'Error: failed to load bootstrap configuration\n\nCaused by:\n    invalid transport\n    in `{key}`')
    match key.split('.'):
        case ['mcp_servers', name, 'enabled'] if name not in os.environ.get('FAKE_CODEX_STICKY', '').split():
            servers[name] = value == 'true'
        case ['mcp_servers', name]:
            servers[name] = True
if 'FAKE_CODEX_STDOUT' in os.environ:
    print(os.environ['FAKE_CODEX_STDOUT'], end='')
    sys.exit('oops' if os.environ.get('FAKE_CODEX_RC') else 0)
print(json.dumps([{'name': name, 'enabled': on} for name, on in servers.items()]))
"""


@pytest.fixture
def fake_codex(tmp_path: Path) -> Path:
    """A ``codex`` whose ``mcp list --json`` applies the ``-c`` flags to the servers of its config layers
    (``$FAKE_CODEX_LISTED``) and plugins (``$FAKE_CODEX_PLUGINS``) the way Codex 0.160.1 does, and records each call.

    ``$FAKE_CODEX_REJECTS``: a key prefix it refuses; ``$FAKE_CODEX_STICKY``: servers it keeps on regardless;
    ``$FAKE_CODEX_STDOUT``: printed instead of the list (with ``oops`` on stderr and exit 1 if ``$FAKE_CODEX_RC``)."""
    return write_script(tmp_path / 'codex', f'#!{sys.executable}\n{FAKE_CODEX}')


def calls(fake: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in Path(f'{fake}.calls').read_text().splitlines()]


HARNESS_ARGV = ['--model', 'm', '-c', 'projects={"/w"={trust_level="trusted"}}', '--sandbox', 'x', MCP, 'go -c']


def codex_launch(tmp_path: Path, fake: Path, servers: dict[str, McpServer], **fake_env: str) -> Launch:
    env = dict(os.environ) | {f'FAKE_CODEX_{k.upper()}': v for k, v in fake_env.items()}
    cwd = tmp_path / 'worktree'
    cwd.mkdir(exist_ok=True)
    return Launch(servers, str(fake), cwd, tmp_path, env, HARNESS_ARGV)


def overrides(args: list[str]) -> dict[str, object]:
    """``-c key=value`` pairs parsed the way Codex parses them: the value as TOML."""
    assert args[::2] == ['-c'] * (len(args) // 2)
    parsed: dict[str, object] = {}
    for item in args[1::2]:
        key, _, value = item.partition('=')
        parsed[key] = tomllib.loads(f'v = {value}')['v']
    return parsed


def test_codex_turns_off_every_server_codex_would_load_then_adds_the_roles(tmp_path: Path, fake_codex: Path) -> None:
    render = RENDERERS['codex'].render
    listed = json.dumps({'node_repl': True, 'computer-use': False})
    servers = {'exa': EXA, 'fc': FIRECRAWL, 'xai': XAI, 'open': OPEN}
    rendered = render(codex_launch(tmp_path, fake_codex, servers, listed=listed))
    parsed = overrides(rendered.args)
    assert parsed == {
        'features.apps': False,
        'features.plugins': False,
        'mcp_servers.node_repl.enabled': False,
        'mcp_servers.computer-use.enabled': False,
        'mcp_servers.exa': {'url': 'https://mcp.exa.ai/mcp', 'env_http_headers': {'x-api-key': 'EXA_API_KEY'}},
        'mcp_servers.fc': {'url': 'https://mcp.firecrawl.dev/v2/mcp', 'bearer_token_env_var': 'FIRECRAWL_API_KEY'},
        'mcp_servers.xai': {
            'command': 'python3',
            'args': ['xai.py', 'grok'],
            'env_vars': ['XAI_API_KEY', 'EXA_API_KEY'],
        },
        'mcp_servers.open': {'url': 'https://open.example/mcp'},
        'shell_environment_policy.exclude': ['EXA_API_KEY', 'FIRECRAWL_API_KEY', 'XAI_API_KEY'],
    }
    harness = ['-c', 'projects={"/w"={trust_level="trusted"}}']
    assert calls(fake_codex) == [
        {
            'cwd': str(tmp_path / 'worktree'),
            'args': [*harness, '-c', 'features.apps=false', '-c', 'features.plugins=false', 'mcp', 'list', '--json'],
        },
        {'cwd': str(tmp_path / 'worktree'), 'args': [*harness, *rendered.args, 'mcp', 'list', '--json']},
    ], "listed in the agent's cwd under the harness's own config, then checked under the rendered flags"
    assert render(codex_launch(tmp_path, fake_codex, {})) == Rendered(
        [*codex.OWN, '-c', 'shell_environment_policy.exclude=[]']
    )


def test_codex_keeps_a_plugins_servers_out_without_addressing_them(tmp_path: Path, fake_codex: Path) -> None:
    """``mcp_servers.<name>.enabled=false`` on a plugin's server stops Codex at startup: plugins are turned off whole."""
    listed = json.dumps({'usersrv': True})
    rendered = RENDERERS['codex'].render(
        codex_launch(tmp_path, fake_codex, {'exa': EXA}, listed=listed, plugins='linear')
    )
    assert not [a for a in rendered.args if 'linear' in a]
    assert overrides(rendered.args)['features.plugins'] is False
    assert overrides(rendered.args)['mcp_servers.usersrv.enabled'] is False


def test_codex_refuses_a_name_both_configs_use_and_a_list_it_cannot_read(tmp_path: Path, fake_codex: Path) -> None:
    render = RENDERERS['codex'].render
    with pytest.raises(McpError, match=r"Codex's own config defines MCP servers \['exa'\]"):
        render(codex_launch(tmp_path, fake_codex, {'exa': EXA}, listed='{"exa": true}'))
    with pytest.raises(McpError, match='Codex rejects its config: .* exit 1: oops'):
        render(codex_launch(tmp_path, fake_codex, {}, stdout='', rc='1'))
    with pytest.raises(McpError, match='printed no server list'):
        render(codex_launch(tmp_path, fake_codex, {}, stdout='WARNING: not json'))
    with pytest.raises(McpError, match='printed no server list'):
        render(codex_launch(tmp_path, fake_codex, {}, stdout='[{"nom": "x"}]'))
    with pytest.raises(McpError, match='FileNotFoundError'):
        render(codex_launch(tmp_path, tmp_path / 'no-codex', {}))


def test_codex_fails_the_launch_loudly_when_it_would_not_start_or_would_load_more(
    tmp_path: Path, fake_codex: Path
) -> None:
    """What Codex would die on at startup, or load beside the role's servers, fails the launch with Codex's words."""
    render = RENDERERS['codex'].render
    with pytest.raises(McpError, match=r'(?s)Codex rejects its config: .*invalid transport\n    in `mcp_servers.exa`'):
        render(codex_launch(tmp_path, fake_codex, {'exa': EXA}, rejects='mcp_servers.exa'))
    with pytest.raises(McpError, match=re.escape("Codex would load MCP servers ['exa', 'node_repl'] under piloop's")):
        render(codex_launch(tmp_path, fake_codex, {'exa': EXA}, listed='{"node_repl": true}', sticky='node_repl'))


def test_codex_list_has_a_timeout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    slow = write_script(tmp_path / 'slow-codex', '#!/bin/sh\nsleep 5\n')
    monkeypatch.setattr(codex, 'LIST_SECONDS', 0.2)
    with pytest.raises(McpError, match='TimeoutExpired'):
        RENDERERS['codex'].render(launch(tmp_path, {}, str(slow)))


def test_codex_toml_round_trips_what_a_server_holds() -> None:
    value = {'plain': 'a', 'needs quotes': 'quote " back \\ newline \n del \x7f é', 'list': ['x', 'y'], 'nested': {}}
    assert tomllib.loads(f'v = {codex.toml(value)}')['v'] == value
    with pytest.raises(TypeError, match='no TOML rendering for int'):
        codex.toml(1)


def test_pi_writes_the_servers_to_the_log_dir_and_loads_the_adapter_on_them_alone(tmp_path: Path) -> None:
    pi = RENDERERS['pi']
    assert pi.render(launch(tmp_path, {})) == Rendered([]), 'headless pi has no MCP at all without servers'
    rendered = pi.render(launch(tmp_path, {'exa': EXA, 'fc': FIRECRAWL, 'open': OPEN, 'xai': XAI, 'cat': PLAIN}))
    config = tmp_path / 'mcp.json'
    assert rendered == Rendered(
        ['-e', 'npm:pi-mcp-adapter@4.0.0', '--mcp-config', str(config)], {'PI_MCP_CONFIG_MODE': 'exclusive'}
    )
    assert json.loads(config.read_text())['mcpServers'] == {
        'exa': {'url': 'https://mcp.exa.ai/mcp', 'headers': {'x-api-key': '${EXA_API_KEY}'}},
        'fc': {'url': 'https://mcp.firecrawl.dev/v2/mcp', 'headers': {'Authorization': 'Bearer ${FIRECRAWL_API_KEY}'}},
        'open': {'url': 'https://open.example/mcp'},
        'xai': {
            'command': 'python3',
            'args': ['xai.py', 'grok'],
            'env': {'XAI_API_KEY': '${XAI_API_KEY}', 'EXA_API_KEY': '${EXA_API_KEY}'},
        },
        'cat': {'command': 'cat', 'args': []},
    }


def test_pi_refuses_an_argv_that_keeps_its_extensions_on() -> None:
    refuses = RENDERERS['pi'].refuses
    assert refuses(['-p', '--no-extensions', MCP]) is None
    assert refuses(['-p', '-ne', MCP]) is None
    why = refuses(['--model', 'x', MCP])
    assert why is not None and 'only with its own extensions off' in why
    assert RENDERERS['claude'].refuses(['anything']) is None, 'the default accepts every argv'


def config(**extra: object) -> dict[str, object]:
    """A minimal project config table with ``extra`` keys on top."""
    base: dict[str, object] = {
        'name': 'demo',
        'evaluate': ['true'],
        'runtime': 'subprocess',
        'harnesses': ['claude', 'codex', 'pi'],
        'model': {'claude': 'c', 'codex': 'x', 'pi': 'p'},
        'metrics': [{'name': 's', 'direction': 'higher', 'role': 'primary'}],
        'mcp': {'exa': EXA.model_dump(), 'xai': XAI.model_dump()},
    }
    return base | extra


def test_a_role_lists_servers_by_table_and_gets_exactly_those() -> None:
    cfg = ProjectConfig.model_validate(config(roles={'research': {'mcp': ['xai', 'exa']}}))
    assert cfg.role_servers(WorkerKind.RESEARCH) == {'xai': XAI, 'exa': EXA}
    assert cfg.role_servers(WorkerKind.WORKER) == {}, 'a role lists none by default'
    with pytest.raises(ValidationError, match=re.escape("roles.worker.mcp: no [mcp.<name>] table for ['nope']")):
        ProjectConfig.model_validate(config(roles={'worker': {'mcp': ['exa', 'nope']}}))
    with pytest.raises(ValidationError, match='String should match pattern'):
        ProjectConfig.model_validate(config(mcp={'a.b': OPEN.model_dump()}))


def test_every_harness_a_role_with_servers_may_run_must_carry_them() -> None:
    plain = {'executable': 'x', 'headless': ['{prompt}']}
    custom = config(harnesses=['claude', 'plain'], model={'claude': 'c', 'plain': 'p'}, harness={'plain': plain})
    assert ProjectConfig.model_validate(custom).harnesses == ['claude', 'plain'], 'no role lists a server: any will do'
    with pytest.raises(ValidationError, match="harness 'plain' takes no MCP servers in its subprocess argv"):
        ProjectConfig.model_validate(custom | {'roles': {'watchdog': {'mcp': ['exa']}}})
    without = {'executable': 'x', 'headless': ['{prompt}'], 'mcp': 'claude'}
    with pytest.raises(ValidationError, match="harness 'plain' takes no MCP servers"):
        ProjectConfig.model_validate(custom | {'harness': {'plain': without}, 'roles': {'watchdog': {'mcp': ['exa']}}})
    herdr = config(runtime='herdr', roles={'research': {'mcp': ['exa']}})
    with pytest.raises(ValidationError, match="harness 'pi' under the herdr runtime: pi takes MCP servers only with"):
        ProjectConfig.model_validate(herdr)
    ProjectConfig.model_validate(herdr | {'roles': {'research': {'mcp': ['exa'], 'harnesses': ['claude', 'codex']}}})


def test_a_harness_names_a_known_renderer_and_mcp_only_with_one() -> None:
    with pytest.raises(
        ValidationError, match=re.escape("unknown mcp renderer 'opencode'; known: ['claude', 'codex', 'pi']")
    ):
        Harness(executable='x', headless=['{prompt}'], mcp='opencode')
    with pytest.raises(ValidationError, match=re.escape('{mcp} in the argv needs mcp set to a renderer')):
        Harness(executable='x', headless=[MCP, '{prompt}'])
    assert {name: (h.mcp, MCP in h.headless, MCP in h.interactive) for name, h in DEFAULT_HARNESSES.items()} == {
        'pi': ('pi', True, True),
        'claude': ('claude', True, True),
        'codex': ('codex', True, True),
    }
    claude = DEFAULT_HARNESSES['claude'].headless
    assert claude[claude.index(MCP) + 1].startswith('--'), (
        "a flag follows claude's variadic --mcp-config, not the prompt"
    )
