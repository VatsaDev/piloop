import json
import re
import subprocess
from collections.abc import Mapping
from pathlib import Path
from unittest.mock import ANY

import pytest
from inline_snapshot import snapshot

from piloop.campaign.campaigns import check_takes
from piloop.config import CampaignConfig, ConfigError, Metric, MetricRole, load_campaign_config
from piloop.ids import IMPLEMENTER, RoleName, TrackName
from piloop.plugins.harness.contract import Harness, each_provider
from piloop.plugins.metric import Direction
from piloop.plugins.role.contract import Files, ImplementerRole
from piloop.plugins.role.orchestrator import OrchestratorRole
from piloop.plugins.role.research import ResearchRole
from piloop.plugins.role.scout import ScoutRole
from piloop.plugins.role.watchdog import WatchdogRole
from piloop.plugins.stop_rule.contract import STOP_RULES, StopRule
from piloop.plugins.stop_rule.limits import Limits
from piloop.plugins.trigger.contract import TRIGGERS
from piloop.plugins.trigger.low_water import LowWaterTrigger
from piloop.plugins.trigger.seed import SeedTrigger
from piloop.plugins.trigger.stalled import StalledTrigger
from piloop.scoring.ledger import criterion
from tests.conftest import AGENT_TIMEOUT, RESEARCH, SCOUT, WATCHDOG, role_of


def test_load_precedence(campaign_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = load_campaign_config(campaign_path)
    assert (cfg.name, cfg.runtime, cfg.harnesses, cfg.harness_for('script').model) == (
        'demo',
        'subprocess',
        ['script'],
        'test-model',
    )
    assert [t.primary.name for t in cfg.all_tracks] == ['score'] and [m.name for m in cfg.metrics] == ['score', 'size']
    assert cfg.harness_for('script').headless == ['{prompt}']
    assert cfg.harness_for('pi') == each_provider()['pi']
    override = {'executable': 'x'}
    loaded = load_campaign_config(campaign_path, plugins={'harness': {'script': override}})
    assert (loaded.harness_for('script').executable, loaded.harness_for('script').model) == ('x', 'test-model'), (
        'an override merges into the table of the file'
    )

    monkeypatch.setenv('PILOOP_CAMPAIGN_PLUGINS__ROLE__IMPLEMENTER__MAX_CONCURRENT', '3')
    monkeypatch.setenv('PILOOP_CAMPAIGN_EVALUATE_TIMEOUT', '9')
    cfg = load_campaign_config(campaign_path)
    assert (role_of(cfg, ImplementerRole).max_concurrent, cfg.evaluate_timeout) == (3, 9)
    assert load_campaign_config(campaign_path, evaluate_timeout=None).evaluate_timeout == 9


def test_a_piloop_toml_on_the_removed_herdr_runtime_says_where_to_read_why(campaign_path: Path) -> None:
    gone = re.escape(
        'the herdr runtime is removed in piloop 0.3.0: write runtime = "subprocess", or delete the key'
        ' (https://piloop.autoresearch.inc/guides/upgrading#the-herdr-runtime)'
    )
    with pytest.raises(ConfigError, match=f"  plugins.runtime.use = 'herdr': {gone}"):
        load_campaign_config(campaign_path, plugins={'runtime': {'use': 'herdr'}})
    with pytest.raises(ConfigError, match=f"  plugins.role.research.runtime = 'herdr': {gone}"):
        load_campaign_config(campaign_path, plugins={'role': {'research': {'runtime': 'herdr'}}})


def test_a_piloop_toml_with_the_removed_entire_agent_says_where_to_read_why(campaign_path: Path) -> None:
    gone = re.escape(
        'entire_agent: removed in piloop 0.3.0; delete the key (https://piloop.autoresearch.inc/guides/upgrading#entire)'
    )
    harness = {'executable': 'x', 'headless': ['{prompt}'], 'entire_agent': 'pi'}
    with pytest.raises(ConfigError, match=f'  plugins.harness.x: .*{gone}'):
        load_campaign_config(campaign_path, plugins={'harness': {'x': harness}})


def test_a_table_of_a_built_in_harness_changes_only_its_keys_and_env_takes_values_and_secrets(
    campaign_path: Path,
) -> None:
    env = {'ANTHROPIC_BASE_URL': 'https://gw.example.com', 'ANTHROPIC_API_KEY': {'secret': 'TEAM_KEY'}}
    claude = load_campaign_config(campaign_path, plugins={'harness': {'claude': {'env': env}}}).harness_for('claude')
    built_in = each_provider()['claude']
    assert claude.model_copy(update={'env': {}}) == built_in, 'the built-in keys that the table does not set'
    assert (claude.literals, claude.secrets) == (
        {'ANTHROPIC_BASE_URL': 'https://gw.example.com'},
        {'ANTHROPIC_API_KEY': 'TEAM_KEY'},
    )
    with pytest.raises(ConfigError, match='plugins.harness'):
        load_campaign_config(campaign_path, plugins={'harness': ['claude']})
    with pytest.raises(ConfigError, match='plugins.harness.claude.env.ANTHROPIC_API_KEY'):
        load_campaign_config(
            campaign_path, plugins={'harness': {'claude': {'env': {'ANTHROPIC_API_KEY': {'value': 'sk-1'}}}}}
        )


def test_a_harness_key_that_only_herdr_read_says_where_to_read_why(campaign_path: Path) -> None:
    with pytest.raises(
        ConfigError,
        match=re.escape(
            '  plugins.harness.script: interactive, queue_key: removed in piloop 0.3.0; delete the key'
            ' (https://piloop.autoresearch.inc/guides/upgrading#the-herdr-runtime); entire_agent: removed in piloop'
            ' 0.3.0; delete the key (https://piloop.autoresearch.inc/guides/upgrading#entire)'
        ),
    ):
        load_campaign_config(
            campaign_path,
            plugins={
                'harness': {
                    'script': {
                        'executable': 'x',
                        'headless': ['{prompt}'],
                        'entire_agent': 'pi',
                        'interactive': ['x'],
                        'queue_key': 'Tab',
                    }
                }
            },
        )


def test_a_piloop_project_variable_fails_with_its_new_name(
    campaign_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv('PILOOP_PROJECT_EVALUATE_TIMEOUT', '9')
    with pytest.raises(
        ConfigError,
        match=re.escape(
            'is not loaded: PILOOP_PROJECT_EVALUATE_TIMEOUT is PILOOP_CAMPAIGN_EVALUATE_TIMEOUT: PILOOP_PROJECT_<KEY> is'
            ' removed in piloop 0.3.0; rename or unset it (https://piloop.autoresearch.inc/guides/upgrading#removed-keys)'
        ),
    ):
        load_campaign_config(campaign_path)


def test_roles_are_typed_per_kind(campaign_path: Path) -> None:
    cfg = load_campaign_config(campaign_path)
    assert cfg.roles.all() == {
        IMPLEMENTER: ImplementerRole(timeout=AGENT_TIMEOUT, max_concurrent=1),
        RoleName('orchestrator'): OrchestratorRole(),
        RESEARCH: ResearchRole(timeout=AGENT_TIMEOUT, max_ideas=0, max_concurrent=1),
        SCOUT: ScoutRole(),
        WATCHDOG: WatchdogRole(harnesses=['script'], model={'script': 'watchdog-model'}, timeout=AGENT_TIMEOUT),
    }, 'every role, in the order of its entry point, with its table over its defaults'
    assert cfg.role(RESEARCH) is role_of(cfg, ResearchRole)
    assert cfg.role_harnesses(RESEARCH) == ['script'], 'unset: the campaign rotation'
    assert cfg.role_models(WATCHDOG) == {'script': 'watchdog-model'}, "role map over each harness's model"
    assert cfg.role_models(RESEARCH) == {'script': 'test-model', 'claude': 'claude-opus-5-5', 'codex': 'gpt-6.1-sol'}
    assert CampaignConfig.model_validate(cfg.model_dump(mode='json')) == cfg, 'round-trips through the campaign row'
    dumped = cfg.model_dump(mode='json')['plugins']
    assert dumped['role']['scout'] == {}, 'a role without a table is stored by its name alone'
    assert dumped['harness']['script'] == {'model': 'test-model', 'executable': ANY, 'headless': ['{prompt}']}
    defaults = CampaignConfig(
        name='bare',
        metrics=[Metric(name='a', direction=Direction.HIGHER, role=MetricRole.PRIMARY)],
        evaluate=['true'],
        plugins={'harness': {'pi': {'model': 'm'}}},
    )
    assert defaults.context == [], 'no analysis artifacts unless the campaign names them'
    implementer, watchdog = role_of(defaults, ImplementerRole), role_of(defaults, WatchdogRole)
    research = role_of(defaults, ResearchRole)
    assert (implementer.timeout, implementer.max_concurrent) == (1800, 4)
    assert (watchdog.harnesses, watchdog.model) == (None, None), 'the campaign rotation and models'
    assert (watchdog.timeout, watchdog.interval) == (600, 1800)
    assert (research.timeout, research.max_ideas) == (900, 1)
    assert research.triggers == [LowWaterTrigger(buffer=2), SeedTrigger(), StalledTrigger()]
    assert research.max_concurrent == 1, 'concurrent sweeps duplicate each other and race on seeds'


def test_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_campaign_config(tmp_path)


def test_validators(campaign_path: Path) -> None:
    with pytest.raises(
        ValueError, match=re.escape("plugins.harness.use[0] = 'nope': no [plugins.harness.nope]; there")
    ):
        load_campaign_config(campaign_path, plugins={'harness': {'use': ['nope']}})
    with pytest.raises(ValueError, match=re.escape("plugins.role.implementer: no model for the harnesses ['pi']")):
        load_campaign_config(campaign_path, plugins={'harness': {'use': ['script', 'pi']}})
    with pytest.raises(ValueError, match=re.escape('plugins.harness.use: List should have at least 1 item')):
        load_campaign_config(campaign_path, plugins={'harness': {'use': []}})
    with pytest.raises(ValueError, match=re.escape('plugins.runtime: the instances are in the other file')):
        load_campaign_config(campaign_path, plugins={'runtime': {'fast': {'provider': 'subprocess'}}})
    with pytest.raises(ValueError, match='relative path'):
        load_campaign_config(campaign_path, link=['/etc'])
    with pytest.raises(ValueError, match='relative path'):
        load_campaign_config(campaign_path, plugins={'role': {'research': {'addendum': '../elsewhere.md'}}})
    with pytest.raises(ValueError, match='at least one metric'):
        load_campaign_config(campaign_path, metrics=[{'name': 'a', 'direction': 'higher'}])
    with pytest.raises(ValueError, match='plugins.role.janitor'):
        load_campaign_config(campaign_path, plugins={'role': {'janitor': {}}})
    with pytest.raises(ValueError, match='plugins.role.implementer.max_ideas'):
        load_campaign_config(campaign_path, plugins={'role': {'implementer': {'max_ideas': 3}}})
    with pytest.raises(ValueError, match='plugins.role.research.min_ready'):
        load_campaign_config(campaign_path, plugins={'role': {'research': {'min_ready': 3}}})
    with pytest.raises(ValueError, match=re.escape("plugins.role.research.harnesses[0] = 'nope': no [plugins.harness")):
        load_campaign_config(campaign_path, plugins={'role': {'research': {'harnesses': ['nope']}}})
    with pytest.raises(ValueError, match=re.escape("plugins.role.research.model = 'nope': no [plugins.harness.nope]")):
        load_campaign_config(campaign_path, plugins={'role': {'research': {'model': {'nope': 'm'}}}})
    with pytest.raises(ValueError, match=re.escape("plugins.role.research: no model for the harnesses ['pi']")):
        load_campaign_config(campaign_path, plugins={'role': {'research': {'harnesses': ['pi']}}})
    with pytest.raises(ValueError, match='plugins.harness.claude.max_concurrent'):
        load_campaign_config(campaign_path, plugins={'harness': {'claude': {'max_concurrent': 0}}})


def test_an_unknown_harness_key_in_the_file_fails_loudly(campaign_path: Path) -> None:
    """A misspelt key such as ``startup_dialog`` fails: dropping it silently would leave the dialogs unanswered."""
    toml = campaign_path / 'piloop.toml'
    toml.write_text(
        toml.read_text()
        + '\n[plugins.harness.codex]\nexecutable = "codex"\nheadless = ["{prompt}"]\nstartup_dialog = ["trust", ["enter"]]\n'
    )
    with pytest.raises(ValueError, match='plugins.harness.codex.startup_dialog'):
        load_campaign_config(campaign_path)


def test_the_limits_of_the_stop_rule_are_optional_and_validated(campaign_path: Path) -> None:
    def rule(cfg: CampaignConfig) -> StopRule:
        return STOP_RULES.selected(cfg.plugins)

    assert rule(load_campaign_config(campaign_path)) == Limits(), 'no limit: open-ended'
    limits = {'target': 0.85, 'max_runs': 200, 'max_hours': 12, 'plateau': 25}
    cfg = load_campaign_config(campaign_path, plugins={'stop_rule': limits})
    assert rule(cfg) == Limits(target=0.85, max_runs=200, max_hours=12.0, plateau=25)
    assert rule(CampaignConfig.model_validate(cfg.model_dump(mode='json'))) == rule(cfg)
    with pytest.raises(ValueError, match='plugins.stop_rule.target'):
        load_campaign_config(campaign_path, plugins={'stop_rule': {'target': 'high'}})
    with pytest.raises(ValueError, match='plugins.stop_rule.max_runs'):
        load_campaign_config(campaign_path, plugins={'stop_rule': {'max_runs': 0}})
    with pytest.raises(ValueError, match='plugins.stop_rule.plateau'):
        load_campaign_config(campaign_path, plugins={'stop_rule': {'plateau': 0}})
    with pytest.raises(ValueError, match='plugins.stop_rule.max_hours'):
        load_campaign_config(campaign_path, plugins={'stop_rule': {'max_hours': 0}})
    with pytest.raises(ValueError, match='plugins.stop_rule.budget'):
        load_campaign_config(campaign_path, plugins={'stop_rule': {'budget': 3}})


def test_an_invalid_piloop_toml_names_the_file_each_key_its_value_and_the_fix(campaign_path: Path) -> None:
    toml = campaign_path / 'piloop.toml'
    text = toml.read_text()
    toml.write_text(
        text.replace('name = "demo"', 'colour = "blue"').replace('direction = "higher"', 'direction = "up"')
    )
    with pytest.raises(ConfigError) as caught:
        load_campaign_config(campaign_path)
    assert str(caught.value) == (
        f'{toml} is not a valid piloop config (3 problem(s)):\n'
        '  name: Field required. Fix: add the key; https://piloop.autoresearch.inc/reference/configuration says what it takes\n'
        "  metrics[0].direction = 'up': Input should be 'higher' or 'lower'. Fix: https://piloop.autoresearch.inc/reference/configuration"
        ' describes the key\n'
        "  colour = 'blue': Extra inputs are not permitted. Fix: delete the key or correct its spelling;"
        ' https://piloop.autoresearch.inc/reference/configuration lists every key'
    )
    toml.write_text(text.replace('role = "primary"', ''))
    with pytest.raises(ConfigError, match=r'\(the file\): at least one metric must be primary\. Fix:'):
        load_campaign_config(campaign_path)
    toml.write_text('name = ')
    with pytest.raises(ConfigError, match=f'{toml} is not valid TOML: '):
        load_campaign_config(campaign_path)


def test_metric_direction() -> None:
    lower = Metric(name='m', direction=Direction.LOWER, role=MetricRole.PRIMARY)
    assert lower.better(1.0, 2.0) and not lower.better(2.0, 1.0)


def test_default_harnesses_have_prompt_placeholder() -> None:
    assert {name: '{prompt}' in h.headless for name, h in each_provider().items()} == snapshot(
        {'pi': True, 'claude': True, 'codex': True}
    )


def test_default_harnesses_need_their_session_id_and_say_where_it_comes_from() -> None:
    found = {n: (h.needs_session_id, h.session_id_pattern) for n, h in each_provider().items()}
    assert found == snapshot(
        {
            'pi': (True, None),
            'claude': (True, None),
            'codex': (True, '"thread_id":"([0-9a-f-]{36})"'),
        }
    ), 'pi and claude are told their id; codex prints its own, which the subprocess runtime reads back'
    for name in ('pi', 'claude'):
        h = each_provider()[name]
        assert '--session-id {session_id}' in ' '.join(h.headless), name
    assert '--session-dir' not in each_provider()['pi'].headless, "usage reads pi's own session store"
    Harness(executable='x', headless=['{prompt}'])
    for resume in (['{prompt}'], ['--resume', '{session_id}']):
        with pytest.raises(ValueError, match=r'resume must include \{session_id\} and \{prompt\}'):
            Harness(executable='x', headless=['{prompt}'], resume=resume)
    assert [' '.join(each_provider()[name].resume[:3]) for name in ('pi', 'claude', 'codex')] == [
        '-p --mode json',
        '-p --output-format json',
        'exec resume --json',
    ], 'each continues the session of its headless run (verified live with pi 0.99.2, Claude Code, codex-cli 0.162.1)'


def test_default_harnesses_take_the_roles_effort_in_every_argv() -> None:
    thinking = {
        'pi': '--thinking {effort}',
        'claude': '--effort {effort}',
        'codex': '-c model_reasoning_effort="{effort}"',
    }
    for name, flags in thinking.items():
        h = each_provider()[name]
        assert all(flags in ' '.join(argv) for argv in (h.headless, h.resume)), name


@pytest.mark.parametrize('mode', ['headless', 'resume'])
def test_pi_keeps_research_capabilities_enabled_with_skills_off(mode: str) -> None:
    argv = getattr(each_provider()['pi'], mode)
    assert '--no-skills' in argv
    assert not {'--no-tools', '-nt', '--no-extensions', '-ne', '--no-context-files', '-nc'} & set(argv)
    assert argv[argv.index('--thinking') + 1] == '{effort}'


def test_metric_roles_are_validated(campaign_path: Path) -> None:
    primary = {'name': 'score', 'direction': 'higher', 'role': 'primary'}
    guard = {'name': 'size', 'direction': 'lower', 'role': 'guardrail', 'margin': 3, 'floor': 10}
    cfg = load_campaign_config(campaign_path, metrics=[primary, guard])
    [track] = cfg.all_tracks
    assert (track.name, track.primary.name, track.share, track.addendum) == ('score', 'score', 1, None), (
        'one primary is one track named after it, with nothing to configure'
    )
    assert ([m.name for m in cfg.guardrails], [m.name for m in track.guardrails]) == (['size'], ['size'])
    assert not cfg.multitrack and cfg.track(TrackName('score')) == track
    assert cfg.guardrails[0].gain(9, 10) == 1 and track.primary.gain(9, 10) == -1
    with pytest.raises(KeyError, match="no track 'speed' in piloop.toml; tracks: score"):
        cfg.track(TrackName('speed'))
    with pytest.raises(ValueError, match="guardrail 'size' needs a margin or a fraction"):
        load_campaign_config(campaign_path, metrics=[primary, {**guard, 'margin': None}])
    with pytest.raises(ValueError, match='set margin or fraction, not both'):
        load_campaign_config(campaign_path, metrics=[primary, {**guard, 'fraction': 0.1}])
    relative = load_campaign_config(campaign_path, metrics=[primary, {**guard, 'margin': None, 'fraction': 0.1}])
    assert criterion(relative.guardrails[0]).allowed(40) == 4.0, "a fraction of the bar, in the metric's units"
    with pytest.raises(ValueError, match="'size': floor, margin and fraction apply to guardrails and primaries only"):
        load_campaign_config(campaign_path, metrics=[primary, {**guard, 'role': 'watch'}])
    with pytest.raises(
        ValueError, match="'score': floor, margin and fraction on a primary apply only with several tracks"
    ):
        load_campaign_config(campaign_path, metrics=[{**primary, 'floor': 1}])
    with pytest.raises(ValueError, match="'size': track applies to primaries only"):
        load_campaign_config(campaign_path, metrics=[primary, {**guard, 'track': 'x'}])
    with pytest.raises(ValueError, match=r'metrics\[1\]\.margin = -1'):
        load_campaign_config(campaign_path, metrics=[primary, {**guard, 'margin': -1}])
    with pytest.raises(
        ValueError, match=r'metrics\[0\]\.primary = True: Extra inputs are not permitted\. Fix: delete the key'
    ):
        load_campaign_config(campaign_path, metrics=[{'name': 'score', 'direction': 'higher', 'primary': True}])
    with pytest.raises(ValueError, match='plugins.verdict_rule.confidence'):
        load_campaign_config(campaign_path, plugins={'verdict_rule': {'confidence': 1}})


ACCURACY = {'name': 'score', 'direction': 'higher', 'role': 'primary', 'track': 'accuracy', 'margin': 1, 'floor': 2}
COST = {'name': 'size', 'direction': 'lower', 'role': 'primary', 'track': 'serving', 'margin': 5}
SHARES = {'accuracy': {'share': 0.75, 'addendum': 'briefs/accuracy.md'}, 'serving': {'share': 0.25}}


def test_tracks_are_primaries_each_with_a_margin_a_table_and_a_share(campaign_path: Path) -> None:
    cfg = load_campaign_config(campaign_path, metrics=[ACCURACY, COST], tracks=SHARES)
    assert cfg.multitrack
    assert [(t.name, t.primary.name, t.share, t.addendum, [m.name for m in t.guardrails]) for t in cfg.all_tracks] == [
        ('accuracy', 'score', 0.75, 'briefs/accuracy.md', ['size']),
        ('serving', 'size', 0.25, None, ['score']),
    ]
    assert CampaignConfig.model_validate(cfg.model_dump(mode='json')) == cfg, 'round-trips through the campaign row'
    tracks = {'metrics': [ACCURACY, COST], 'tracks': SHARES}
    refusals = {
        r"tracks \['accuracy'\] have more than one primary": {'metrics': [ACCURACY, {**COST, 'track': 'accuracy'}]},
        r"primaries \['size'\] need a margin or a fraction": {
            **tracks,
            'metrics': [ACCURACY, {**COST, 'margin': None}],
        },
        r"tracks \['serving'\] need a \[tracks.<name>\] table": {**tracks, 'tracks': {'accuracy': {'share': 1}}},
        r"\[tracks\] \['speed'\] name no track a primary declares": {
            **tracks,
            'tracks': {**SHARES, 'speed': {'share': 0}},
        },
        'track shares sum to 0.9, not 1': {**tracks, 'tracks': {**SHARES, 'serving': {'share': 0.15}}},
        r'track shares sum to 0.5, not 1': {'tracks': {'score': {'share': 0.5}}},
        "the stop rule 'limits' limits the head, which needs a campaign of one track": {
            **tracks,
            'plugins': {'stop_rule': {'target': 3}},
        },
        "'briefs/../x' must be a relative path inside the repo": {
            **tracks,
            'tracks': {**SHARES, 'serving': {'share': 0.25, 'addendum': 'briefs/../x'}},
        },
    }
    for message, overrides in refusals.items():
        with pytest.raises(ValueError, match=message):
            load_campaign_config(campaign_path, **overrides)
    one = load_campaign_config(campaign_path, tracks={'score': {'addendum': 'briefs/score.md'}})
    assert one.all_tracks[0].addendum == 'briefs/score.md', 'one track may still have its addendum'
    guarded = [
        {'name': 'score', 'direction': 'higher', 'role': 'primary'},
        {**COST, 'role': 'guardrail', 'track': None},
    ]
    assert load_campaign_config(campaign_path, metrics=[ACCURACY, COST], tracks=SHARES).evaluator_key == (
        load_campaign_config(campaign_path, metrics=guarded).evaluator_key
    ), 'tracks, margins and floors are verdict policy: they re-measure nothing'


def test_the_evaluator_is_what_scores_the_judging_metrics_not_how_the_verdict_reads_them(campaign_path: Path) -> None:
    primary = {'name': 'score', 'direction': 'higher', 'role': 'primary'}
    size = {'name': 'size', 'direction': 'lower', 'role': 'guardrail', 'margin': 3}
    cfg = load_campaign_config(campaign_path, metrics=[primary, size])
    assert cfg.evaluator_definition == {
        'evaluate': cfg.evaluate,
        'link': ['data'],
        'judging': [{'name': 'score', 'direction': 'higher'}, {'name': 'size', 'direction': 'lower'}],
    }

    def key(*metrics: Mapping[str, object], **overrides: object) -> str:
        return load_campaign_config(campaign_path, metrics=list(metrics), **overrides).evaluator_key

    unchanged = {
        'a new margin': key(primary, {**size, 'margin': 9}),
        'a primary that is a guardrail now': key(
            {**primary, 'track': 'a', 'margin': 1},
            {**size, 'role': 'primary', 'track': 'b'},
            tracks={'a': {'share': 0.5}, 'b': {'share': 0.5}},
        ),
        'another order of the metrics': key(size, primary),
        'a verdict rule': key(primary, size, plugins={'verdict_rule': {'confidence': 0.9}}),
        'a new watch metric': key(primary, size, {'name': 'eval_cost_usd', 'direction': 'lower'}),
        'no ladder': key(primary, size, ladder=[]),
    }
    assert unchanged == dict.fromkeys(unchanged, cfg.evaluator_key), 'none changes a verdict already given'
    changed = {
        'a watch metric that judges now': key(primary, size, {**size, 'name': 'cost', 'margin': 1}),
        'a guardrail that only watches now': key(primary, {'name': 'size', 'direction': 'lower'}),
        'a new direction': key({**primary, 'direction': 'lower'}, size),
        'a new evaluate command': key(primary, size, evaluate=['true']),
        'a new linked input': key(primary, size, link=['data', 'more']),
        'a ladder': key(primary, size, ladder=[SCREEN]),
        "a rung's command": key(primary, size, ladder=[{**SCREEN, 'evaluate': ['smoke']}]),
    }
    assert key(primary, size, ladder=[SCREEN]) == key(
        primary, size, ladder=[{**SCREEN, 'stop': {'size': {'floor': 9}}}]
    ), "a rung's stop is the policy of the climb"
    assert len({cfg.evaluator_key, *changed.values()}) == len(changed) + 1, 'each can change a verdict'
    assert (
        load_campaign_config(campaign_path).evaluator_key
        == load_campaign_config(campaign_path, metrics=[primary]).evaluator_key
    ), 'a watch metric is no part of the evaluator'


def test_repeats_marks_a_noisy_metric_and_a_track_pairs_with_the_largest_repeats_of_its_deciding_metrics(
    campaign_path: Path,
) -> None:
    timing = {'name': 'seconds', 'direction': 'lower', 'role': 'guardrail', 'margin': 1, 'repeats': 3}
    cpu = {'name': 'cpu', 'direction': 'lower', 'role': 'guardrail', 'margin': 1, 'repeats': 5}
    primary = {'name': 'score', 'direction': 'higher', 'role': 'primary'}
    assert load_campaign_config(campaign_path).all_tracks[0].repeats is None, 'no noisy metric: one sample'
    noisy = load_campaign_config(campaign_path, metrics=[primary, timing, cpu])
    assert noisy.all_tracks[0].repeats == 5
    assert (
        noisy.evaluator_key
        == load_campaign_config(
            campaign_path, metrics=[primary, {**timing, 'repeats': None}, {**cpu, 'repeats': None}]
        ).evaluator_key
    ), 'repeats is verdict policy: it re-measures nothing'
    serving = load_campaign_config(campaign_path, metrics=[ACCURACY, {**COST, 'repeats': 3}], tracks=SHARES)
    assert [t.repeats for t in serving.all_tracks] == [3, 3], 'the primary of another track is a guardrail'
    with pytest.raises(ValueError, match="'size': repeats applies to primaries and guardrails only"):
        load_campaign_config(campaign_path, metrics=[primary, {'name': 'size', 'direction': 'lower', 'repeats': 3}])
    with pytest.raises(ValueError, match=r'metrics\[1\]\.repeats = 0'):
        load_campaign_config(campaign_path, metrics=[primary, {**timing, 'repeats': 0}])


def test_default_harnesses_keep_agents_out_of_the_harness_memory() -> None:
    """A memory the harness keys by repo would carry one agent's notes into every later agent of the campaign."""
    off = {'claude': ['"autoMemoryEnabled":false'], 'codex': ['-c', 'features.memories=false']}
    for name, flags in off.items():
        h = each_provider()[name]
        assert ' '.join(flags) in ' '.join(h.headless) and ' '.join(flags) in ' '.join(h.resume), name


def test_default_claude_moves_a_long_bash_command_to_the_background() -> None:
    """Claude agents get a 5-minute default and a 10-minute maximum for a foreground Bash command, whatever the user's."""
    claude = each_provider()['claude']
    for argv in (claude.headless, claude.resume):
        settings = json.loads(argv[argv.index('--settings') + 1])
        assert settings['env'] == {'BASH_DEFAULT_TIMEOUT_MS': '300000', 'BASH_MAX_TIMEOUT_MS': '600000'}


def test_default_claude_takes_the_queued_messages_after_each_tool_call(tmp_path: Path) -> None:
    """A headless turn can last an hour, and a message waits for its end. A hook after each tool call reads the queue.

    Without a message, the hook costs a file check: piloop starts only when the queue has one.
    """
    claude = each_provider()['claude']
    commands = set()
    for argv in (claude.headless, claude.resume):
        settings = json.loads(argv[argv.index('--settings') + 1])
        [entry] = settings['hooks']['PostToolUse']
        [hook] = entry['hooks']
        commands.add(hook['command'])
    [command] = commands
    fake = tmp_path / 'piloop'
    fake.write_text('#!/bin/sh\necho "took: $*"\n')
    fake.chmod(0o755)
    queue = tmp_path / 'queued-messages'

    def hook(**env: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ['sh', '-c', command],
            env={'PATH': '/usr/bin:/bin', 'PILOOP_BIN': str(fake)} | env,
            capture_output=True,
            text=True,
            check=False,
        )

    assert (hook().returncode, hook().stdout) == (0, ''), 'no queue named'
    assert (hook(PILOOP_MESSAGES=str(queue)).returncode, hook(PILOOP_MESSAGES=str(queue)).stdout) == (0, '')
    queue.write_text('"x"\n')
    assert hook(PILOOP_MESSAGES=str(queue)).stdout == 'took: agent messages --harness claude\n'


def test_a_table_with_a_new_name_defines_a_role_that_needs_a_prompt_file_and_triggers(campaign_path: Path) -> None:
    (campaign_path / 'reviewer.md').write_text('# Review\n')
    when = 'SELECT 1 FROM ideas WHERE campaign_id = :campaign AND created_at > :since'
    good = {'prompt': 'reviewer.md', 'triggers': [{'provider': 'sql', 'when': when}], 'runtime': 'subprocess'}
    cfg = load_campaign_config(campaign_path, plugins={'role': {'reviewer': good}})
    assert list(cfg.roles.all()) == ['implementer', 'orchestrator', 'research', 'scout', 'watchdog', 'reviewer']
    assert list(cfg.roles.sweeping()) == ['orchestrator', 'research', 'scout', 'watchdog', 'reviewer'], (
        'the implementer does runs, not sweeps'
    )
    assert cfg.role_runtime(RoleName('reviewer')) == 'subprocess'
    assert CampaignConfig.model_validate(cfg.model_dump(mode='json')) == cfg, 'round-trips through the campaign row'

    def fails(roles: dict[str, object]) -> str:
        with pytest.raises(ConfigError) as e:
            load_campaign_config(campaign_path, plugins={'role': roles})
        return str(e.value).split('\n', 1)[1]

    def reviewer(**keys: object) -> dict[str, object]:
        return {'reviewer': {**good, **keys}}

    assert fails({'reviewer': {'triggers': []}}) == snapshot(
        '  plugins.role.reviewer: a new role needs a prompt: the file that starts its brief. Fix:'
        ' https://piloop.autoresearch.inc/reference/configuration describes the key'
    )
    assert fails({'reviewer': {'prompt': 'reviewer.md'}}) == snapshot(
        '  plugins.role.reviewer.triggers: Field required. Fix: add the key; https://piloop.autoresearch.inc/reference/configuration says what it takes'
    )
    assert "plugins.role.reviewer.prompt = 'missing.md': no such file" in fails(reviewer(prompt='missing.md'))
    assert (
        "plugins.role.reviewer.triggers[0]: provider = 'nope': unknown trigger 'nope'; registered triggers:"
        f' {", ".join(TRIGGERS.providers())}' in fails(reviewer(triggers=[{'provider': 'nope'}]))
    )
    assert 'plugins.role.reviewer.triggers[0].every = 3: Extra inputs are not permitted' in fails(
        reviewer(triggers=[{'provider': 'cron', 'every': 3}])
    )
    filler = load_campaign_config(
        campaign_path, plugins={'role': reviewer(triggers=[{'provider': 'low_water', 'buffer': 1}], files='ideas')}
    )
    assert cfg.roles.sweeping()[RoleName('reviewer')].max_ideas == 1, 'any sweeping role can fill the backlog'
    assert filler.roles.sweeping()[RoleName('reviewer')].triggers == [LowWaterTrigger(buffer=1)]
    assert fails({'worker': {'max_concurrent': 2}}) == snapshot(
        '  plugins.role.worker: [roles.worker] is removed in piloop 0.3.0: rename the table [roles.implementer] (https://piloop.autoresearch.inc/guides/upgrading#removed-keys). Fix: https://piloop.autoresearch.inc/reference/configuration describes the key'
    ), 'not a new sweeping role named worker'
    for workdir in ('repo', 'head'):
        assert fails(reviewer(workdir=workdir)) == snapshot(
            '  plugins.role.reviewer: workdir is removed in piloop 0.3.0: every agent works in a worktree of its own, never in the campaign checkout. Delete the key (https://piloop.autoresearch.inc/guides/upgrading#workdir). Fix: https://piloop.autoresearch.inc/reference/configuration describes the key'
        )
    for query, error in {
        'DELETE FROM ideas': 'not authorized',
        'SELECT nope FROM ideas': 'no such column: nope',
        'SELECT 1 FROM ideas WHERE id = :idea': 'You did not supply a value for binding parameter :idea',
        'SELECT 1; SELECT 2': 'You can only execute one statement at a time',
    }.items():
        assert f'the query does not run on the piloop database: {error}' in fails(
            reviewer(triggers=[{'provider': 'sql', 'when': query}])
        ), query
    for name in ('run', 'Review Bot'):
        assert f'{name!r} is not a role name: use lowercase letters, digits, _ and -' in fails({name: good})


def test_a_table_with_runs_defines_a_role_that_does_runs_with_the_implementer_defaults(campaign_path: Path) -> None:
    quick = {'runs': True, 'takes': "priority >= 3 OR track = 'speed'", 'timeout': 60, 'max_concurrent': 1}
    cfg = load_campaign_config(campaign_path, plugins={'role': {'quick': quick}})
    assert list(cfg.roles.all()) == ['implementer', 'orchestrator', 'research', 'scout', 'watchdog', 'quick']
    assert list(cfg.roles.doing_runs()) == ['quick', 'implementer'], 'the roles of the file fill their slots first'
    assert list(cfg.roles.sweeping()) == ['orchestrator', 'research', 'scout', 'watchdog']
    role = cfg.roles.doing_runs()[RoleName('quick')]
    assert (role.prompt, role.PROMPT, role.files, role.explore_every) == (
        None,
        ImplementerRole.PROMPT,
        Files.NOTHING,
        4,
    )
    assert cfg.run_slots == 1 + cfg.roles.doing_runs()[IMPLEMENTER].max_concurrent
    assert CampaignConfig.model_validate(cfg.model_dump(mode='json')) == cfg, 'round-trips through the campaign row'

    def fails(roles: dict[str, object]) -> str:
        with pytest.raises(ConfigError) as e:
            load_campaign_config(campaign_path, plugins={'role': roles})
        return str(e.value).split('\n', 1)[1]

    def refused(takes: str) -> str:
        cfg = load_campaign_config(campaign_path, plugins={'role': {'quick': {'runs': True, 'takes': takes}}})
        with pytest.raises(ConfigError) as e:
            check_takes(campaign_path, cfg)
        return str(e.value).split('\n', 1)[1]

    assert "plugins.role.quick.takes = 'priority >>= 3': the condition does not run on the ideas table" in refused(
        'priority >>= 3'
    )
    assert 'no such column: nope' in refused('nope = 1')
    assert 'You did not supply a value for binding parameter :campaign' in refused('campaign_id = :campaign')
    assert 'plugins.role.quick.triggers: Extra inputs are not permitted' in fails(
        {'quick': {'runs': True, 'triggers': []}}
    )
    assert "plugins.role.quick.takes = 'priority > 1': Extra inputs are not permitted" in fails(
        {'quick': {'takes': 'priority > 1', 'triggers': []}}
    )
    assert 'plugins.role.watchdog.runs = True: Input should be False' in fails({'watchdog': {'runs': True}})
    assert 'plugins.role.implementer.runs = False: Input should be True' in fails({'implementer': {'runs': False}})


SCREEN = {'name': 'screen', 'stop': {'score': {'margin': 0.1}}}


@pytest.mark.parametrize(
    ('ladder', 'error'),
    [
        ([SCREEN, SCREEN], "ladder: rungs ['screen'] appear more than once"),
        ([{**SCREEN, 'stop': {'accuracy': {'floor': 1}}}], "ladder.screen.stop: ['accuracy'] are not in [[metrics]]"),
        (
            [SCREEN, {'name': 'smoke', 'stop': {'score': {'floor': 1}}, 'evaluate': ['smoke']}],
            'ladder: the rungs with their own evaluate come first',
        ),
        ([{**SCREEN, 'stop': {'score': {'margin': 0.1, 'fraction': 0.1}}}], 'set margin or fraction, not both'),
        ([{**SCREEN, 'stop': {'score': {}}}], "stop of ['score']: set floor, margin or fraction"),
        ([{**SCREEN, 'stop': {}}], 'stop'),
        ([{**SCREEN, 'name': 'Bad Name'}], 'name'),
    ],
)
def test_a_ladder_names_its_rungs_once_stops_on_known_metrics_and_runs_its_own_commands_first(
    campaign_path: Path, ladder: list[dict[str, object]], error: str
) -> None:
    with pytest.raises(ConfigError, match=re.escape(error)):
        load_campaign_config(campaign_path, ladder=ladder)


def test_a_rung_may_stop_on_a_floor_and_a_regression_together(campaign_path: Path) -> None:
    rung = {'name': 'smoke', 'evaluate': ['smoke'], 'stop': {'score': {'floor': 1, 'fraction': 0.2}}}
    cfg = load_campaign_config(campaign_path, ladder=[rung, SCREEN])
    assert [r.name for r in cfg.ladder] == ['smoke', 'screen']


def test_a_capability_of_one_instance_written_as_named_tables_says_so(campaign_path: Path) -> None:
    """``[plugins.verdict_rule.strict]`` reads as a key ``strict`` of the one verdict rule: the error says what to write."""
    with pytest.raises(ConfigError) as caught:
        load_campaign_config(campaign_path, plugins={'verdict_rule': {'strict': {'confidence': 0.99}}})
    assert str(caught.value).split('\n', 1)[1] == snapshot(
        '  plugins.verdict_rule: a verdict rule has one instance, not one for each name (strict): write its keys in [plugins.verdict_rule], with provider = "<verdict rule>" to choose one. Fix: https://piloop.autoresearch.inc/reference/configuration describes the key'
    )
    with pytest.raises(ConfigError, match=r"plugins\.verdict_rule\.confidence = 'high'"):
        load_campaign_config(campaign_path, plugins={'verdict_rule': {'confidence': 'high'}})
    held = load_campaign_config(campaign_path, plugins={'stop_rule': {'hold_at': {'hours': 5}}})
    assert STOP_RULES.selected(held.plugins) == Limits(hold_at={'hours': 5}), 'a parameter that is a table is one'


def test_a_capability_s_own_settings_are_in_its_table_and_use_names_the_instances_in_use(
    campaign_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``[plugins.harness] use`` is the rotation, ``[plugins.runtime] use`` the runtime: unset, the defaults."""
    claude = {'use': ['script', 'claude'], 'claude': {'model': 'c'}}
    cfg = load_campaign_config(campaign_path, plugins={'harness': claude})
    assert cfg.harnesses == ['script', 'claude'] and cfg.model_dump(mode='json')['plugins']['harness']['use'] == [
        'script',
        'claude',
    ]
    assert CampaignConfig.model_validate(cfg.model_dump(mode='json')) == cfg, 'use round-trips through the row'
    primary = {'name': 'a', 'direction': 'higher', 'role': 'primary'}
    pi = {'harness': {'pi': {'model': 'm'}}}
    bare = CampaignConfig.model_validate({'name': 'b', 'evaluate': ['true'], 'metrics': [primary], 'plugins': pi})
    assert (bare.harnesses, bare.runtime) == (['pi'], 'subprocess'), 'the providers that are on by default'
    monkeypatch.setenv('PILOOP_CAMPAIGN_PLUGINS__HARNESS__USE', '["script"]')
    assert load_campaign_config(campaign_path).harnesses == ['script'], 'a variable holds a JSON list'
