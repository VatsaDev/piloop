"""MCP servers: the definition, the variables it names, and how each built-in harness renders them."""

import json
import os
import re
import sys
import tomllib
from collections.abc import Mapping
from pathlib import Path

import pytest
from pydantic import ValidationError

from piloop.config import CampaignConfig
from piloop.ids import IMPLEMENTER
from piloop.mcp import McpServer, env_names
from piloop.plugins.harness import codex
from piloop.plugins.harness.claude import ClaudeMcp
from piloop.plugins.harness.codex import CodexMcp
from piloop.plugins.harness.contract import MCP, Custom, McpArgs, McpError, McpLaunch, each_provider, expand
from piloop.plugins.harness.pi import PiMcp
from tests.conftest import RESEARCH, write_script

EXA = McpServer(url='https://mcp.exa.ai/mcp', headers_env={'x-api-key': 'EXA_API_KEY'})
FIRECRAWL = McpServer(url='https://mcp.firecrawl.dev/v2/mcp', bearer_token_env='FIRECRAWL_API_KEY')
OPEN = McpServer(url='https://open.example/mcp')
XAI = McpServer(command=['python3', 'xai.py', 'grok'], env=['XAI_API_KEY', 'EXA_API_KEY'])
PLAIN = McpServer(command=['cat'])


def launch(tmp_path: Path, servers: dict[str, McpServer], executable: str = 'codex') -> McpLaunch:
    return McpLaunch(servers, executable, tmp_path, tmp_path, dict(os.environ), [MCP])


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


def test_the_variables_that_servers_need_are_named_each_once() -> None:
    assert XAI.env_names == ['XAI_API_KEY', 'EXA_API_KEY']
    assert env_names([EXA, FIRECRAWL, XAI, OPEN]) == ['EXA_API_KEY', 'FIRECRAWL_API_KEY', 'XAI_API_KEY']


def test_expand_replaces_only_the_whole_mcp_element() -> None:
    argv = ['-p', MCP, '--settings', 'x{mcp}y']
    assert expand(argv, McpArgs(['a', 'b'])) == ['-p', 'a', 'b', '--settings', 'x{mcp}y']
    assert expand(argv, McpArgs([])) == ['-p', '--settings', 'x{mcp}y']


def test_claude_is_strict_and_inlines_its_servers_with_variable_references(tmp_path: Path) -> None:
    render = ClaudeMcp().render
    assert render(launch(tmp_path, {})) == McpArgs(['--strict-mcp-config']), "no servers: none of the user's either"
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


HARNESS_ARGV = ['--model', 'm', '-c', 'campaigns={"/w"={trust_level="trusted"}}', '--sandbox', 'x', MCP, 'go -c']


def codex_launch(tmp_path: Path, fake: Path, servers: dict[str, McpServer], **fake_env: str) -> McpLaunch:
    env = dict(os.environ) | {f'FAKE_CODEX_{k.upper()}': v for k, v in fake_env.items()}
    cwd = tmp_path / 'worktree'
    cwd.mkdir(exist_ok=True)
    return McpLaunch(servers, str(fake), cwd, tmp_path, env, HARNESS_ARGV)


def overrides(args: list[str]) -> dict[str, object]:
    """``-c key=value`` pairs parsed the way Codex parses them: the value as TOML."""
    assert args[::2] == ['-c'] * (len(args) // 2)
    parsed: dict[str, object] = {}
    for item in args[1::2]:
        key, _, value = item.partition('=')
        parsed[key] = tomllib.loads(f'v = {value}')['v']
    return parsed


def test_codex_turns_off_every_server_codex_would_load_then_adds_the_roles(tmp_path: Path, fake_codex: Path) -> None:
    render = CodexMcp().render
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
    harness = ['-c', 'campaigns={"/w"={trust_level="trusted"}}']
    assert calls(fake_codex) == [
        {
            'cwd': str(tmp_path / 'worktree'),
            'args': [*harness, '-c', 'features.apps=false', '-c', 'features.plugins=false', 'mcp', 'list', '--json'],
        },
        {'cwd': str(tmp_path / 'worktree'), 'args': [*harness, *rendered.args, 'mcp', 'list', '--json']},
    ], "listed in the agent's cwd under the harness's own config, then checked under the rendered flags"
    assert render(codex_launch(tmp_path, fake_codex, {})) == McpArgs(
        [*codex.OWN, '-c', 'shell_environment_policy.exclude=[]']
    )


def test_codex_keeps_a_plugins_servers_out_without_addressing_them(tmp_path: Path, fake_codex: Path) -> None:
    """``mcp_servers.<name>.enabled=false`` on a plugin's server stops Codex at startup: plugins are turned off whole."""
    listed = json.dumps({'usersrv': True})
    rendered = CodexMcp().render(codex_launch(tmp_path, fake_codex, {'exa': EXA}, listed=listed, plugins='linear'))
    assert not [a for a in rendered.args if 'linear' in a]
    assert overrides(rendered.args)['features.plugins'] is False
    assert overrides(rendered.args)['mcp_servers.usersrv.enabled'] is False


def test_codex_refuses_a_name_both_configs_use_and_a_list_it_cannot_read(tmp_path: Path, fake_codex: Path) -> None:
    render = CodexMcp().render
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
    render = CodexMcp().render
    with pytest.raises(McpError, match=r'(?s)Codex rejects its config: .*invalid transport\n    in `mcp_servers.exa`'):
        render(codex_launch(tmp_path, fake_codex, {'exa': EXA}, rejects='mcp_servers.exa'))
    with pytest.raises(McpError, match=re.escape("Codex would load MCP servers ['exa', 'node_repl'] under piloop's")):
        render(codex_launch(tmp_path, fake_codex, {'exa': EXA}, listed='{"node_repl": true}', sticky='node_repl'))


def test_codex_list_has_a_timeout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    slow = write_script(tmp_path / 'slow-codex', '#!/bin/sh\nsleep 5\n')
    monkeypatch.setattr(codex, 'LIST_SECONDS', 0.2)
    with pytest.raises(McpError, match='TimeoutExpired'):
        CodexMcp().render(launch(tmp_path, {}, str(slow)))


def test_codex_toml_round_trips_what_a_server_holds() -> None:
    value = {'plain': 'a', 'needs quotes': 'quote " back \\ newline \n del \x7f é', 'list': ['x', 'y'], 'nested': {}}
    assert tomllib.loads(f'v = {codex.toml(value)}')['v'] == value
    with pytest.raises(TypeError, match='no TOML rendering for int'):
        codex.toml(1)


def test_pi_writes_the_servers_to_the_log_dir_and_loads_the_adapter_on_them_alone(tmp_path: Path) -> None:
    pi = PiMcp()
    assert pi.render(launch(tmp_path, {})) == McpArgs([]), 'headless pi has no MCP at all without servers'
    rendered = pi.render(launch(tmp_path, {'exa': EXA, 'fc': FIRECRAWL, 'open': OPEN, 'xai': XAI, 'cat': PLAIN}))
    config = tmp_path / 'mcp.json'
    assert rendered == McpArgs(
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
    refuses = PiMcp().refuses
    assert refuses(['-p', '--no-extensions', MCP]) is None
    assert refuses(['-p', '-ne', MCP]) is None
    why = refuses(['--model', 'x', MCP])
    assert why is not None and 'only with its own extensions off' in why
    assert ClaudeMcp().refuses(['anything']) is None, 'the default accepts every argv'


MODELS = {'claude': {'model': 'c'}, 'codex': {'model': 'x'}, 'pi': {'model': 'p'}}


def config(**extra: object) -> dict[str, object]:
    """A minimal campaign config table with ``extra`` keys on top."""
    base: dict[str, object] = {
        'name': 'demo',
        'evaluate': ['true'],
        'metrics': [{'name': 's', 'direction': 'higher', 'role': 'primary'}],
        'mcp': {'exa': EXA.model_dump(), 'xai': XAI.model_dump()},
        'plugins': {'harness': {'use': ['claude', 'codex', 'pi'], **MODELS}, 'runtime': {'use': 'subprocess'}},
    }
    return base | extra


def test_a_role_lists_servers_by_table_and_gets_exactly_those() -> None:
    cfg = CampaignConfig.model_validate(
        config(plugins={'harness': MODELS, 'role': {'research': {'mcp': ['xai', 'exa']}}})
    )
    assert cfg.role_servers(RESEARCH) == {'xai': XAI, 'exa': EXA}
    assert cfg.role_servers(IMPLEMENTER) == {}, 'a role lists none by default'
    with pytest.raises(
        ValidationError, match=re.escape("plugins.role.implementer.mcp: no [mcp.<name>] table for ['nope']")
    ):
        CampaignConfig.model_validate(
            config(plugins={'harness': MODELS, 'role': {'implementer': {'mcp': ['exa', 'nope']}}})
        )
    with pytest.raises(ValidationError, match='String should match pattern'):
        CampaignConfig.model_validate(config(mcp={'a.b': OPEN.model_dump()}))


def test_every_harness_a_role_with_servers_may_run_must_carry_them() -> None:
    def with_plain(plain: Mapping[str, object], **role: object) -> dict[str, object]:
        harness = {'use': ['claude', 'plain'], 'claude': {'model': 'c'}, 'plain': {'model': 'p', **plain}}
        return config(plugins={'harness': harness, 'role': {'watchdog': role}})

    plain = {'executable': 'x', 'headless': ['{prompt}']}
    custom = with_plain(plain)
    assert CampaignConfig.model_validate(custom).harnesses == ['claude', 'plain'], 'no role lists a server: any will do'
    with pytest.raises(
        ValidationError,
        match=re.escape("harness 'plain': it takes no MCP servers"),
    ):
        CampaignConfig.model_validate(with_plain(plain, mcp=['exa']))
    without = {'provider': 'claude', 'executable': 'x', 'headless': ['--session-id', '{session_id}', '{prompt}']}
    with pytest.raises(ValidationError, match="harness 'plain': its headless argv has no {mcp} element"):
        CampaignConfig.model_validate(with_plain(without, mcp=['exa']))
    extended = {'provider': 'pi', 'headless': ['--session-id', '{session_id}', MCP, '{prompt}']}
    with pytest.raises(ValidationError, match=re.escape("harness 'plain': pi takes MCP servers only with")):
        CampaignConfig.model_validate(with_plain(extended, mcp=['exa']))


def test_only_a_harness_that_takes_servers_has_an_mcp_element() -> None:
    with pytest.raises(ValidationError, match=re.escape('{mcp} in the argv of a harness that takes no MCP servers')):
        Custom(executable='x', headless=[MCP, '{prompt}'])
    assert {name: (MCP in h.headless, MCP in h.resume) for name, h in each_provider().items()} == {
        'pi': (True, True),
        'claude': (True, True),
        'codex': (True, True),
    }
    claude = each_provider()['claude'].headless
    assert claude[claude.index(MCP) + 1].startswith('--'), (
        "a flag follows claude's variadic --mcp-config, not the prompt"
    )
