import json
from pathlib import Path

import pytest
from inline_snapshot import snapshot

from piloop.config import (
    ConfigError,
    Direction,
    Metric,
    MetricRole,
    ProjectConfig,
    ResearchRole,
    Roles,
    RuntimeKind,
    Stop,
    WatchdogRole,
    WorkerRole,
    load_project_config,
)
from piloop.db.models import WorkerKind
from piloop.harnesses import DEFAULT_HARNESSES, Harness


def test_load_precedence(project_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = load_project_config(project_path)
    assert (cfg.name, cfg.runtime, cfg.harnesses, cfg.model) == (
        'demo',
        RuntimeKind.SUBPROCESS,
        ['script'],
        {'script': 'test-model'},
    )
    assert [t.primary.name for t in cfg.all_tracks] == ['score'] and [m.name for m in cfg.metrics] == ['score', 'size']
    assert (cfg.harness_for('script').headless, cfg.harness_for('script').startup_dialogs) == (['{prompt}'], [])
    assert cfg.harness_for('pi') is DEFAULT_HARNESSES['pi']
    override = {'executable': 'x', 'headless': ['{prompt}'], 'startup_dialogs': [['trust', ['down', 'enter']]]}
    assert load_project_config(project_path, harness={'script': override}).harness_for('script').startup_dialogs == [
        ('trust', ['down', 'enter'])
    ]

    monkeypatch.setenv('PILOOP_PROJECT_ROLES__WORKER__MAX_CONCURRENT', '3')
    monkeypatch.setenv('PILOOP_PROJECT_RUNTIME', 'herdr')
    cfg = load_project_config(project_path)
    assert (cfg.roles.worker.max_concurrent, cfg.runtime) == (3, RuntimeKind.HERDR)
    assert load_project_config(project_path, runtime=None).runtime is RuntimeKind.HERDR


def test_roles_are_typed_per_kind(project_path: Path) -> None:
    cfg = load_project_config(project_path)
    assert cfg.roles == snapshot(
        Roles(
            worker=WorkerRole(timeout=20.0, max_concurrent=1),
            watchdog=WatchdogRole(harnesses=['script'], model={'script': 'watchdog-model'}, timeout=20.0),
            research=ResearchRole(timeout=20.0, max_ideas=0, max_concurrent=1),
        )
    )
    assert cfg.role(WorkerKind.RESEARCH) is cfg.roles.research
    assert set(Roles.model_fields) == {k.value for k in WorkerKind}, 'every kind has its table'
    assert cfg.role_harnesses(WorkerKind.RESEARCH) == ['script'], 'unset: the project rotation'
    assert cfg.role_models(WorkerKind.WATCHDOG) == {'script': 'watchdog-model'}, 'role map over the project map'
    assert ProjectConfig.model_validate(cfg.model_dump(mode='json')) == cfg, 'round-trips through the project row'
    defaults = ProjectConfig(
        name='bare',
        metrics=[Metric(name='a', direction=Direction.HIGHER, role=MetricRole.PRIMARY)],
        evaluate=['true'],
        model={'pi': 'm'},
    ).roles
    assert (defaults.worker.timeout, defaults.worker.max_concurrent) == (1800, 4)
    assert (defaults.watchdog.harnesses, defaults.watchdog.model) == (None, None), 'the project rotation and models'
    assert (defaults.watchdog.timeout, defaults.watchdog.interval) == (600, 1800)
    assert (defaults.research.timeout, defaults.research.max_ideas, defaults.research.buffer) == (900, 1, 2)
    assert defaults.research.context == [], 'no analysis artifacts unless the project names them'
    assert defaults.research.max_concurrent == 1, 'concurrent sweeps duplicate each other and race on seeds'


def test_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_project_config(tmp_path)


def test_validators(project_path: Path) -> None:
    with pytest.raises(ValueError, match='unknown harnesses'):
        load_project_config(project_path, harnesses=['nope'])
    with pytest.raises(ValueError, match='no model configured'):
        load_project_config(project_path, harnesses=['script', 'pi'])
    with pytest.raises(ValueError, match='relative path'):
        load_project_config(project_path, link=['/etc'])
    with pytest.raises(ValueError, match='relative path'):
        load_project_config(project_path, roles={'research': {'addendum': '../elsewhere.md'}})
    with pytest.raises(ValueError, match='at least one metric'):
        load_project_config(project_path, metrics=[{'name': 'a', 'direction': 'higher'}])
    with pytest.raises(ValueError, match='roles.janitor'):
        load_project_config(project_path, roles={'janitor': {}})
    with pytest.raises(ValueError, match='roles.worker.max_ideas'):
        load_project_config(project_path, roles={'worker': {'max_ideas': 3}})
    with pytest.raises(ValueError, match='roles.research.min_ready'):
        load_project_config(project_path, roles={'research': {'min_ready': 3}})
    with pytest.raises(ValueError, match='roles.research: unknown harnesses'):
        load_project_config(project_path, roles={'research': {'harnesses': ['nope']}})
    with pytest.raises(ValueError, match='roles.research: no model configured'):
        load_project_config(project_path, roles={'research': {'harnesses': ['pi']}})
    with pytest.raises(ValueError, match='max_concurrent: unknown harnesses'):
        load_project_config(project_path, max_concurrent={'nope': 1})
    with pytest.raises(ValueError, match='greater than or equal to 1'):
        load_project_config(project_path, max_concurrent={'claude': 0})


def test_an_unknown_harness_key_in_the_file_fails_loudly(project_path: Path) -> None:
    """A misspelt key such as ``startup_dialog`` fails: dropping it silently would leave the dialogs unanswered."""
    toml = project_path / 'piloop.toml'
    toml.write_text(
        toml.read_text()
        + '\n[harness.codex]\nexecutable = "codex"\nheadless = ["{prompt}"]\nstartup_dialog = ["trust", ["enter"]]\n'
    )
    with pytest.raises(ValueError, match='harness.codex.startup_dialog'):
        load_project_config(project_path)


def test_stop_conditions_are_optional_and_validated(project_path: Path) -> None:
    assert load_project_config(project_path).stop == Stop(), 'no [stop] table: open-ended'
    cfg = load_project_config(project_path, stop={'target': 0.85, 'max_runs': 200, 'max_hours': 12, 'plateau': 25})
    assert cfg.stop == snapshot(Stop(target=0.85, max_runs=200, max_hours=12.0, plateau=25))
    assert ProjectConfig.model_validate(cfg.model_dump(mode='json')).stop == cfg.stop
    with pytest.raises(ValueError, match='stop.target'):
        load_project_config(project_path, stop={'target': 'high'})
    with pytest.raises(ValueError, match='stop.max_runs'):
        load_project_config(project_path, stop={'max_runs': 0})
    with pytest.raises(ValueError, match='stop.plateau'):
        load_project_config(project_path, stop={'plateau': 0})
    with pytest.raises(ValueError, match='stop.max_hours'):
        load_project_config(project_path, stop={'max_hours': 0})
    with pytest.raises(ValueError, match='stop.budget'):
        load_project_config(project_path, stop={'budget': 3})


def test_an_invalid_piloop_toml_names_the_file_each_key_its_value_and_the_fix(project_path: Path) -> None:
    toml = project_path / 'piloop.toml'
    text = toml.read_text()
    toml.write_text(
        text.replace('name = "demo"', 'colour = "blue"').replace('direction = "higher"', 'direction = "up"')
    )
    with pytest.raises(ConfigError) as caught:
        load_project_config(project_path)
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
        load_project_config(project_path)
    toml.write_text('name = ')
    with pytest.raises(ConfigError, match=f'{toml} is not valid TOML: '):
        load_project_config(project_path)


def test_metric_direction() -> None:
    lower = Metric(name='m', direction=Direction.LOWER, role=MetricRole.PRIMARY)
    assert lower.better(1.0, 2.0) and not lower.better(2.0, 1.0)


def test_default_harnesses_have_prompt_placeholder() -> None:
    assert {name: '{prompt}' in h.headless for name, h in DEFAULT_HARNESSES.items()} == snapshot(
        {'pi': True, 'claude': True, 'codex': True}
    )


def test_default_harnesses_are_entire_agents_and_say_where_their_session_id_comes_from() -> None:
    capture = {n: (h.entire_agent, h.session_id_pattern) for n, h in DEFAULT_HARNESSES.items()}
    assert capture == snapshot(
        {
            'pi': ('pi', None),
            'claude': ('claude-code', None),
            'codex': ('codex', '"thread_id":"([0-9a-f-]{36})"'),
        }
    ), 'pi and claude are told their id; codex prints its own, which the subprocess runtime reads back'
    for name in ('pi', 'claude'):
        h = DEFAULT_HARNESSES[name]
        assert '--session-id {session_id}' in ' '.join(h.interactive) and '--session-id {session_id}' in ' '.join(
            h.headless
        ), name
    assert '--session-dir' not in DEFAULT_HARNESSES['pi'].headless, "entire looks in pi's own session store"
    with pytest.raises(ValueError, match=r'headless must pass \{session_id\} to the agent or session_id_pattern'):
        Harness(executable='x', interactive=['{session_id}'], headless=['{prompt}'], entire_agent='pi')
    interactive_only_by_herdr = Harness(executable='x', headless=['{prompt}', '{session_id}'], entire_agent='pi')
    assert interactive_only_by_herdr.interactive == [], 'the Herdr runtime reports the id: no placeholder needed there'
    Harness(executable='x', headless=['{prompt}'], entire_agent='codex', session_id_pattern='id=(\\w+)')
    Harness(executable='x', headless=['{prompt}'])


def test_interactive_pi_keeps_the_users_extensions() -> None:
    """Herdr detects pi's working/idle state through an extension the user installed."""
    pi = DEFAULT_HARNESSES['pi']
    assert '--no-extensions' not in pi.interactive and '--no-extensions' in pi.headless


def test_default_harnesses_raise_the_thinking_level() -> None:
    thinking = {
        'pi': ['--thinking', 'high'],
        'claude': ['--effort', 'high'],
        'codex': ['-c', 'model_reasoning_effort="high"'],
    }
    for name, flags in thinking.items():
        h = DEFAULT_HARNESSES[name]
        assert ' '.join(flags) in ' '.join(h.interactive) and ' '.join(flags) in ' '.join(h.headless), name
    assert '-c check_for_update_on_startup=false' in ' '.join(DEFAULT_HARNESSES['codex'].interactive), (
        'an update dialog before the prompt is a dialog nobody answers'
    )


def test_metric_roles_are_validated(project_path: Path) -> None:
    primary = {'name': 'score', 'direction': 'higher', 'role': 'primary'}
    guard = {'name': 'size', 'direction': 'lower', 'role': 'guardrail', 'margin': 3, 'floor': 10}
    cfg = load_project_config(project_path, metrics=[primary, guard])
    [track] = cfg.all_tracks
    assert (track.name, track.primary.name, track.share, track.addendum) == ('score', 'score', 1, None), (
        'one primary is one track named after it, with nothing to configure'
    )
    assert ([m.name for m in cfg.guardrails], [m.name for m in track.guardrails], cfg.confidence) == (
        ['size'],
        ['size'],
        0.95,
    )
    assert not cfg.multitrack and cfg.track('score') == track
    assert cfg.guardrails[0].gain(9, 10) == 1 and track.primary.gain(9, 10) == -1
    with pytest.raises(KeyError, match="no track 'speed' in piloop.toml; tracks: score"):
        cfg.track('speed')
    with pytest.raises(ValueError, match="guardrail 'size' needs a margin"):
        load_project_config(project_path, metrics=[primary, {**guard, 'margin': None}])
    with pytest.raises(ValueError, match="'size': margin and floor apply to guardrails and primaries only"):
        load_project_config(project_path, metrics=[primary, {**guard, 'role': 'watch'}])
    with pytest.raises(ValueError, match="'score': margin and floor on a primary apply only with several tracks"):
        load_project_config(project_path, metrics=[{**primary, 'floor': 1}])
    with pytest.raises(ValueError, match="'size': track applies to primaries only"):
        load_project_config(project_path, metrics=[primary, {**guard, 'track': 'x'}])
    with pytest.raises(ValueError, match=r'metrics\[1\]\.margin = -1'):
        load_project_config(project_path, metrics=[primary, {**guard, 'margin': -1}])
    with pytest.raises(
        ValueError, match=r'metrics\[0\]\.primary = True: Extra inputs are not permitted\. Fix: delete the key'
    ):
        load_project_config(project_path, metrics=[{'name': 'score', 'direction': 'higher', 'primary': True}])
    with pytest.raises(ValueError, match='confidence'):
        load_project_config(project_path, confidence=1)


ACCURACY = {'name': 'score', 'direction': 'higher', 'role': 'primary', 'track': 'accuracy', 'margin': 1, 'floor': 2}
COST = {'name': 'size', 'direction': 'lower', 'role': 'primary', 'track': 'serving', 'margin': 5}
SHARES = {'accuracy': {'share': 0.75, 'addendum': 'briefs/accuracy.md'}, 'serving': {'share': 0.25}}


def test_tracks_are_primaries_each_with_a_margin_a_table_and_a_share(project_path: Path) -> None:
    cfg = load_project_config(project_path, metrics=[ACCURACY, COST], tracks=SHARES)
    assert cfg.multitrack
    assert [(t.name, t.primary.name, t.share, t.addendum, [m.name for m in t.guardrails]) for t in cfg.all_tracks] == [
        ('accuracy', 'score', 0.75, 'briefs/accuracy.md', ['size']),
        ('serving', 'size', 0.25, None, ['score']),
    ]
    assert ProjectConfig.model_validate(cfg.model_dump(mode='json')) == cfg, 'round-trips through the project row'
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
        r'\[stop\] target needs a project of one track': {**tracks, 'stop': {'target': 3}},
        "'briefs/../x' must be a relative path inside the repo": {
            **tracks,
            'tracks': {**SHARES, 'serving': {'share': 0.25, 'addendum': 'briefs/../x'}},
        },
    }
    for message, overrides in refusals.items():
        with pytest.raises(ValueError, match=message):
            load_project_config(project_path, **overrides)
    one = load_project_config(project_path, tracks={'score': {'addendum': 'briefs/score.md'}})
    assert one.all_tracks[0].addendum == 'briefs/score.md', 'one track may still have its addendum'
    assert load_project_config(project_path, metrics=[ACCURACY, COST], tracks=SHARES).evaluator_key == (
        load_project_config(project_path).evaluator_key
    ), 'tracks, margins and floors are verdict policy: they re-measure nothing'


def test_the_evaluator_is_what_scores_not_how_the_verdict_reads_it(project_path: Path) -> None:
    cfg = load_project_config(project_path)
    assert cfg.evaluator_definition['metrics'] == [
        {'name': 'score', 'direction': 'higher'},
        {'name': 'size', 'direction': 'lower'},
    ]
    guarded = load_project_config(
        project_path,
        confidence=0.9,
        metrics=[
            {'name': 'score', 'direction': 'higher', 'role': 'primary'},
            {'name': 'size', 'direction': 'lower', 'role': 'guardrail', 'margin': 3},
        ],
    )
    assert guarded.evaluator_key == cfg.evaluator_key, 'a new margin re-measures nothing'
    flipped = load_project_config(project_path, metrics=[{'name': 'score', 'direction': 'lower', 'role': 'primary'}])
    assert flipped.evaluator_key != cfg.evaluator_key


def test_default_harnesses_keep_agents_out_of_the_harness_memory() -> None:
    """A memory the harness keys by repo would carry one agent's notes into every later agent of the project."""
    off = {'claude': ['"autoMemoryEnabled":false'], 'codex': ['-c', 'features.memories=false']}
    for name, flags in off.items():
        h = DEFAULT_HARNESSES[name]
        assert ' '.join(flags) in ' '.join(h.interactive) and ' '.join(flags) in ' '.join(h.headless), name


def test_default_claude_moves_a_long_bash_command_to_the_background() -> None:
    """Claude agents get a 5-minute default and a 10-minute maximum for a foreground Bash command, whatever the user's."""
    claude = DEFAULT_HARNESSES['claude']
    for argv in (claude.interactive, claude.headless):
        settings = json.loads(argv[argv.index('--settings') + 1])
        assert settings['env'] == {'BASH_DEFAULT_TIMEOUT_MS': '300000', 'BASH_MAX_TIMEOUT_MS': '600000'}
