import json
import re
import subprocess
from pathlib import Path

import pytest
from inline_snapshot import snapshot

from piloop.config import CampaignConfig, ConfigError, Metric, MetricRole, Roles, Stop, load_campaign_config
from piloop.harnesses import DEFAULT_HARNESSES, Harness
from piloop.ids import BUILT_IN_ROLES, RESEARCH, WATCHDOG, RoleName, TrackName
from piloop.plugins.metric import Direction
from piloop.plugins.role.contract import Files, ImplementerRole
from piloop.plugins.role.research import ResearchRole
from piloop.plugins.role.watchdog import WatchdogRole
from piloop.plugins.trigger.contract import TRIGGERS
from piloop.plugins.trigger.low_water import LowWaterTrigger
from piloop.plugins.trigger.seed import SeedTrigger
from piloop.plugins.trigger.stalled import StalledTrigger


def test_load_precedence(campaign_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = load_campaign_config(campaign_path)
    assert (cfg.name, cfg.runtime, cfg.harnesses, cfg.model) == (
        'demo',
        'subprocess',
        ['script'],
        {'script': 'test-model'},
    )
    assert [t.primary.name for t in cfg.all_tracks] == ['score'] and [m.name for m in cfg.metrics] == ['score', 'size']
    assert cfg.harness_for('script').headless == ['{prompt}']
    assert cfg.harness_for('pi') is DEFAULT_HARNESSES['pi']
    override = {'executable': 'x', 'headless': ['{prompt}']}
    assert load_campaign_config(campaign_path, harness={'script': override}).harness_for('script').executable == 'x'

    monkeypatch.setenv('PILOOP_CAMPAIGN_ROLES__IMPLEMENTER__MAX_CONCURRENT', '3')
    monkeypatch.setenv('PILOOP_CAMPAIGN_EVALUATE_TIMEOUT', '9')
    cfg = load_campaign_config(campaign_path)
    assert (cfg.roles.implementer.max_concurrent, cfg.evaluate_timeout) == (3, 9)
    assert load_campaign_config(campaign_path, evaluate_timeout=None).evaluate_timeout == 9


def test_a_piloop_toml_on_the_removed_herdr_runtime_says_where_to_read_why(campaign_path: Path) -> None:
    gone = re.escape(
        'the herdr runtime is removed in piloop 0.3.0: write runtime = "subprocess", or delete the key'
        ' (https://piloop.autoresearch.inc/guides/upgrading#the-herdr-runtime)'
    )
    with pytest.raises(ConfigError, match=f"  runtime = 'herdr': {gone}"):
        load_campaign_config(campaign_path, runtime='herdr')
    with pytest.raises(ConfigError, match=f"  roles.research.runtime = 'herdr': {gone}"):
        load_campaign_config(campaign_path, roles={'research': {'runtime': 'herdr'}})


def test_a_piloop_toml_with_the_removed_entire_agent_says_where_to_read_why(campaign_path: Path) -> None:
    gone = re.escape(
        'entire_agent is removed in piloop 0.3.0, with Entire: delete the key'
        ' (https://piloop.autoresearch.inc/guides/upgrading#entire)'
    )
    harness = {'executable': 'x', 'headless': ['{prompt}'], 'entire_agent': 'pi'}
    with pytest.raises(ConfigError, match=f'  harness.x: .*{gone}'):
        load_campaign_config(campaign_path, harness={'x': harness})


def test_roles_are_typed_per_kind(campaign_path: Path) -> None:
    cfg = load_campaign_config(campaign_path)
    assert cfg.roles == snapshot(
        Roles(
            implementer=ImplementerRole(timeout=20.0, max_concurrent=1),
            watchdog=WatchdogRole(harnesses=['script'], model={'script': 'watchdog-model'}, timeout=20.0),
            research=ResearchRole(timeout=20.0, max_ideas=0, max_concurrent=1),
        )
    )
    assert cfg.role(RESEARCH) is cfg.roles.research
    assert set(Roles.model_fields) == set(BUILT_IN_ROLES), 'every built-in role has its table'
    assert cfg.role_harnesses(RESEARCH) == ['script'], 'unset: the campaign rotation'
    assert cfg.role_models(WATCHDOG) == {'script': 'watchdog-model'}, 'role map over the campaign map'
    assert CampaignConfig.model_validate(cfg.model_dump(mode='json')) == cfg, 'round-trips through the campaign row'
    defaults = CampaignConfig(
        name='bare',
        metrics=[Metric(name='a', direction=Direction.HIGHER, role=MetricRole.PRIMARY)],
        evaluate=['true'],
        model={'pi': 'm'},
    )
    assert defaults.context == [], 'no analysis artifacts unless the campaign names them'
    defaults = defaults.roles
    assert (defaults.implementer.timeout, defaults.implementer.max_concurrent) == (1800, 4)
    assert (defaults.watchdog.harnesses, defaults.watchdog.model) == (None, None), 'the campaign rotation and models'
    assert (defaults.watchdog.timeout, defaults.watchdog.interval) == (600, 1800)
    assert (defaults.research.timeout, defaults.research.max_ideas) == (900, 1)
    assert defaults.research.triggers == [LowWaterTrigger(buffer=2), SeedTrigger(), StalledTrigger()]
    assert defaults.research.max_concurrent == 1, 'concurrent sweeps duplicate each other and race on seeds'


def test_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_campaign_config(tmp_path)


def test_validators(campaign_path: Path) -> None:
    with pytest.raises(ValueError, match='unknown harnesses'):
        load_campaign_config(campaign_path, harnesses=['nope'])
    with pytest.raises(ValueError, match='no model configured'):
        load_campaign_config(campaign_path, harnesses=['script', 'pi'])
    with pytest.raises(ValueError, match='relative path'):
        load_campaign_config(campaign_path, link=['/etc'])
    with pytest.raises(ValueError, match='relative path'):
        load_campaign_config(campaign_path, roles={'research': {'addendum': '../elsewhere.md'}})
    with pytest.raises(ValueError, match='at least one metric'):
        load_campaign_config(campaign_path, metrics=[{'name': 'a', 'direction': 'higher'}])
    with pytest.raises(ValueError, match='roles.janitor'):
        load_campaign_config(campaign_path, roles={'janitor': {}})
    with pytest.raises(ValueError, match='roles.implementer.max_ideas'):
        load_campaign_config(campaign_path, roles={'implementer': {'max_ideas': 3}})
    with pytest.raises(ValueError, match='roles.research.min_ready'):
        load_campaign_config(campaign_path, roles={'research': {'min_ready': 3}})
    with pytest.raises(ValueError, match='roles.research: unknown harnesses'):
        load_campaign_config(campaign_path, roles={'research': {'harnesses': ['nope']}})
    with pytest.raises(ValueError, match='roles.research: no model configured'):
        load_campaign_config(campaign_path, roles={'research': {'harnesses': ['pi']}})
    with pytest.raises(ValueError, match='max_concurrent: unknown harnesses'):
        load_campaign_config(campaign_path, max_concurrent={'nope': 1})
    with pytest.raises(ValueError, match='greater than or equal to 1'):
        load_campaign_config(campaign_path, max_concurrent={'claude': 0})


def test_an_unknown_harness_key_in_the_file_fails_loudly(campaign_path: Path) -> None:
    """A misspelt key such as ``startup_dialog`` fails: dropping it silently would leave the dialogs unanswered."""
    toml = campaign_path / 'piloop.toml'
    toml.write_text(
        toml.read_text()
        + '\n[harness.codex]\nexecutable = "codex"\nheadless = ["{prompt}"]\nstartup_dialog = ["trust", ["enter"]]\n'
    )
    with pytest.raises(ValueError, match='harness.codex.startup_dialog'):
        load_campaign_config(campaign_path)


def test_stop_conditions_are_optional_and_validated(campaign_path: Path) -> None:
    assert load_campaign_config(campaign_path).stop == Stop(), 'no [stop] table: open-ended'
    cfg = load_campaign_config(campaign_path, stop={'target': 0.85, 'max_runs': 200, 'max_hours': 12, 'plateau': 25})
    assert cfg.stop == snapshot(Stop(target=0.85, max_runs=200, max_hours=12.0, plateau=25))
    assert CampaignConfig.model_validate(cfg.model_dump(mode='json')).stop == cfg.stop
    with pytest.raises(ValueError, match='stop.target'):
        load_campaign_config(campaign_path, stop={'target': 'high'})
    with pytest.raises(ValueError, match='stop.max_runs'):
        load_campaign_config(campaign_path, stop={'max_runs': 0})
    with pytest.raises(ValueError, match='stop.plateau'):
        load_campaign_config(campaign_path, stop={'plateau': 0})
    with pytest.raises(ValueError, match='stop.max_hours'):
        load_campaign_config(campaign_path, stop={'max_hours': 0})
    with pytest.raises(ValueError, match='stop.budget'):
        load_campaign_config(campaign_path, stop={'budget': 3})


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
    assert {name: '{prompt}' in h.headless for name, h in DEFAULT_HARNESSES.items()} == snapshot(
        {'pi': True, 'claude': True, 'codex': True}
    )


def test_default_harnesses_need_their_session_id_and_say_where_it_comes_from() -> None:
    found = {n: (h.needs_session_id, h.session_id_pattern) for n, h in DEFAULT_HARNESSES.items()}
    assert found == snapshot(
        {
            'pi': (True, None),
            'claude': (True, None),
            'codex': (True, '"thread_id":"([0-9a-f-]{36})"'),
        }
    ), 'pi and claude are told their id; codex prints its own, which the subprocess runtime reads back'
    for name in ('pi', 'claude'):
        h = DEFAULT_HARNESSES[name]
        assert '--session-id {session_id}' in ' '.join(h.headless), name
    assert '--session-dir' not in DEFAULT_HARNESSES['pi'].headless, "usage reads pi's own session store"
    Harness(executable='x', headless=['{prompt}'])
    for resume in (['{prompt}'], ['--resume', '{session_id}']):
        with pytest.raises(ValueError, match=r'resume must include \{session_id\} and \{prompt\}'):
            Harness(executable='x', headless=['{prompt}'], resume=resume)
    assert [' '.join(DEFAULT_HARNESSES[name].resume[:3]) for name in ('pi', 'claude', 'codex')] == [
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
        h = DEFAULT_HARNESSES[name]
        assert all(flags in ' '.join(argv) for argv in (h.headless, h.resume)), name


def test_metric_roles_are_validated(campaign_path: Path) -> None:
    primary = {'name': 'score', 'direction': 'higher', 'role': 'primary'}
    guard = {'name': 'size', 'direction': 'lower', 'role': 'guardrail', 'margin': 3, 'floor': 10}
    cfg = load_campaign_config(campaign_path, metrics=[primary, guard])
    [track] = cfg.all_tracks
    assert (track.name, track.primary.name, track.share, track.addendum) == ('score', 'score', 1, None), (
        'one primary is one track named after it, with nothing to configure'
    )
    assert ([m.name for m in cfg.guardrails], [m.name for m in track.guardrails], cfg.confidence) == (
        ['size'],
        ['size'],
        0.95,
    )
    assert not cfg.multitrack and cfg.track(TrackName('score')) == track
    assert cfg.guardrails[0].gain(9, 10) == 1 and track.primary.gain(9, 10) == -1
    with pytest.raises(KeyError, match="no track 'speed' in piloop.toml; tracks: score"):
        cfg.track(TrackName('speed'))
    with pytest.raises(ValueError, match="guardrail 'size' needs a margin"):
        load_campaign_config(campaign_path, metrics=[primary, {**guard, 'margin': None}])
    with pytest.raises(ValueError, match="'size': margin and floor apply to guardrails and primaries only"):
        load_campaign_config(campaign_path, metrics=[primary, {**guard, 'role': 'watch'}])
    with pytest.raises(ValueError, match="'score': margin and floor on a primary apply only with several tracks"):
        load_campaign_config(campaign_path, metrics=[{**primary, 'floor': 1}])
    with pytest.raises(ValueError, match="'size': track applies to primaries only"):
        load_campaign_config(campaign_path, metrics=[primary, {**guard, 'track': 'x'}])
    with pytest.raises(ValueError, match=r'metrics\[1\]\.margin = -1'):
        load_campaign_config(campaign_path, metrics=[primary, {**guard, 'margin': -1}])
    with pytest.raises(
        ValueError, match=r'metrics\[0\]\.primary = True: Extra inputs are not permitted\. Fix: delete the key'
    ):
        load_campaign_config(campaign_path, metrics=[{'name': 'score', 'direction': 'higher', 'primary': True}])
    with pytest.raises(ValueError, match='confidence'):
        load_campaign_config(campaign_path, confidence=1)


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
        r"primaries \['size'\] need a margin": {**tracks, 'metrics': [ACCURACY, {**COST, 'margin': None}]},
        r"tracks \['serving'\] need a \[tracks.<name>\] table": {**tracks, 'tracks': {'accuracy': {'share': 1}}},
        r"\[tracks\] \['speed'\] name no track a primary declares": {
            **tracks,
            'tracks': {**SHARES, 'speed': {'share': 0}},
        },
        'track shares sum to 0.9, not 1': {**tracks, 'tracks': {**SHARES, 'serving': {'share': 0.15}}},
        r'track shares sum to 0.5, not 1': {'tracks': {'score': {'share': 0.5}}},
        r'\[stop\] target needs a campaign of one track': {**tracks, 'stop': {'target': 3}},
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
    assert load_campaign_config(campaign_path, metrics=[ACCURACY, COST], tracks=SHARES).evaluator_key == (
        load_campaign_config(campaign_path).evaluator_key
    ), 'tracks, margins and floors are verdict policy: they re-measure nothing'


def test_the_evaluator_is_what_scores_not_how_the_verdict_reads_it(campaign_path: Path) -> None:
    cfg = load_campaign_config(campaign_path)
    assert cfg.evaluator_definition['metrics'] == [
        {'name': 'score', 'direction': 'higher'},
        {'name': 'size', 'direction': 'lower'},
    ]
    guarded = load_campaign_config(
        campaign_path,
        confidence=0.9,
        metrics=[
            {'name': 'score', 'direction': 'higher', 'role': 'primary'},
            {'name': 'size', 'direction': 'lower', 'role': 'guardrail', 'margin': 3},
        ],
    )
    assert guarded.evaluator_key == cfg.evaluator_key, 'a new margin re-measures nothing'
    flipped = load_campaign_config(campaign_path, metrics=[{'name': 'score', 'direction': 'lower', 'role': 'primary'}])
    assert flipped.evaluator_key != cfg.evaluator_key


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
        h = DEFAULT_HARNESSES[name]
        assert ' '.join(flags) in ' '.join(h.headless) and ' '.join(flags) in ' '.join(h.resume), name


def test_default_claude_moves_a_long_bash_command_to_the_background() -> None:
    """Claude agents get a 5-minute default and a 10-minute maximum for a foreground Bash command, whatever the user's."""
    claude = DEFAULT_HARNESSES['claude']
    for argv in (claude.headless, claude.resume):
        settings = json.loads(argv[argv.index('--settings') + 1])
        assert settings['env'] == {'BASH_DEFAULT_TIMEOUT_MS': '300000', 'BASH_MAX_TIMEOUT_MS': '600000'}


def test_default_claude_takes_the_queued_messages_after_each_tool_call(tmp_path: Path) -> None:
    """A headless turn can last an hour, and a message waits for its end. A hook after each tool call reads the queue.

    Without a message, the hook costs a file check: piloop starts only when the queue has one.
    """
    claude = DEFAULT_HARNESSES['claude']
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
    assert hook(PILOOP_MESSAGES=str(queue)).stdout == 'took: agent messages --format claude-hook\n'


def test_a_table_with_a_new_name_defines_a_role_that_needs_a_prompt_file_and_triggers(campaign_path: Path) -> None:
    (campaign_path / 'reviewer.md').write_text('# Review\n')
    when = 'SELECT 1 FROM ideas WHERE campaign_id = :campaign AND created_at > :since'
    good = {'prompt': 'reviewer.md', 'triggers': [{'provider': 'sql', 'when': when}], 'runtime': 'subprocess'}
    cfg = load_campaign_config(campaign_path, roles={'reviewer': good})
    assert list(cfg.roles.all()) == ['implementer', 'watchdog', 'research', 'scout', 'reviewer']
    assert list(cfg.roles.sweeping()) == ['watchdog', 'research', 'scout', 'reviewer'], (
        'the implementer does runs, not sweeps'
    )
    assert cfg.role_runtime(RoleName('reviewer')) == 'subprocess'
    assert CampaignConfig.model_validate(cfg.model_dump(mode='json')) == cfg, 'round-trips through the campaign row'

    def fails(roles: dict[str, object]) -> str:
        with pytest.raises(ConfigError) as e:
            load_campaign_config(campaign_path, roles=roles)
        return str(e.value).split('\n', 1)[1]

    def reviewer(**keys: object) -> dict[str, object]:
        return {'reviewer': {**good, **keys}}

    assert fails({'reviewer': {'triggers': []}}) == snapshot(
        '  roles.reviewer: a new role needs a prompt: the file that starts its brief. Fix:'
        ' https://piloop.autoresearch.inc/reference/configuration describes the key'
    )
    assert fails({'reviewer': {'prompt': 'reviewer.md'}}) == snapshot(
        '  roles.reviewer.triggers: Field required. Fix: add the key; https://piloop.autoresearch.inc/reference/configuration says what it takes'
    )
    assert "roles.reviewer.prompt = 'missing.md': no such file" in fails(reviewer(prompt='missing.md'))
    assert (
        "roles.reviewer.triggers[0]: provider = 'nope': unknown trigger 'nope'; registered triggers:"
        f' {", ".join(TRIGGERS.providers())}' in fails(reviewer(triggers=[{'provider': 'nope'}]))
    )
    assert 'roles.reviewer.triggers[0].every = 3: Extra inputs are not permitted' in fails(
        reviewer(triggers=[{'provider': 'cron', 'every': 3}])
    )
    filler = load_campaign_config(
        campaign_path, roles=reviewer(triggers=[{'provider': 'low_water', 'buffer': 1}], files='ideas')
    )
    assert cfg.roles.sweeping()[RoleName('reviewer')].max_ideas == 1, 'any sweeping role can fill the backlog'
    assert filler.roles.sweeping()[RoleName('reviewer')].triggers == [LowWaterTrigger(buffer=1)]
    for workdir in ('repo', 'head'):
        assert fails(reviewer(workdir=workdir)) == snapshot(
            '  roles.reviewer: workdir is removed in piloop 0.3.0: every agent works in a worktree of its own, never in the campaign checkout. Delete the key (https://piloop.autoresearch.inc/guides/upgrading#workdir). Fix: https://piloop.autoresearch.inc/reference/configuration describes the key'
        )
    for query, error in {
        'DELETE FROM ideas': 'not authorized',
        'SELECT nope FROM ideas': 'no such column: nope',
        'SELECT 1 FROM projects': 'no such table: projects: `projects` is `campaigns` since piloop 0.2.0; see https://',
        'SELECT i.PROJECT_ID FROM ideas i': 'no such column: i.PROJECT_ID: `project_id` is `campaign_id`',
        'SELECT 1 FROM Workers': 'no such table: Workers: `workers` is `agent_sessions` since piloop 0.2.0',
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
    cfg = load_campaign_config(campaign_path, roles={'quick': quick})
    assert list(cfg.roles.all()) == ['implementer', 'watchdog', 'research', 'scout', 'quick']
    assert list(cfg.roles.doing_runs()) == ['quick', 'implementer'], 'the roles of the file fill their slots first'
    assert list(cfg.roles.sweeping()) == ['watchdog', 'research', 'scout']
    role = cfg.roles.doing_runs()[RoleName('quick')]
    assert (role.prompt, role.PROMPT, role.files, role.explore_every) == (None, 'implementer.md', Files.NOTHING, 4)
    assert cfg.run_slots == 1 + cfg.roles.implementer.max_concurrent
    assert CampaignConfig.model_validate(cfg.model_dump(mode='json')) == cfg, 'round-trips through the campaign row'

    def fails(roles: dict[str, object]) -> str:
        with pytest.raises(ConfigError) as e:
            load_campaign_config(campaign_path, roles=roles)
        return str(e.value).split('\n', 1)[1]

    assert "roles.quick.takes = 'priority >>= 3': the condition does not run on the ideas table" in fails(
        {'quick': {'runs': True, 'takes': 'priority >>= 3'}}
    )
    assert 'no such column: nope' in fails({'quick': {'runs': True, 'takes': 'nope = 1'}})
    assert (
        '`project_id` is `campaign_id` since piloop 0.2.0; see https://piloop.autoresearch.inc/guides/upgrading'
        in fails({'quick': {'runs': True, 'takes': 'project_id = 1'}})
    )
    assert 'You did not supply a value for binding parameter :campaign' in fails(
        {'quick': {'runs': True, 'takes': 'campaign_id = :campaign'}}
    )
    assert 'roles.quick.triggers: Extra inputs are not permitted' in fails({'quick': {'runs': True, 'triggers': []}})
    assert "roles.quick.takes = 'priority > 1': Extra inputs are not permitted" in fails(
        {'quick': {'takes': 'priority > 1', 'triggers': []}}
    )
    assert 'roles.watchdog.runs = True: Input should be False' in fails({'watchdog': {'runs': True}})
    assert 'roles.implementer.runs = False: Input should be True' in fails({'implementer': {'runs': False}})
