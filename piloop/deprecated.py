"""Compat code that piloop keeps until a set release: deprecated ``piloop.toml`` keys, old names and old behavior.

Mark each piece of compat code with ``until``: the release that deletes it and what to delete. A test finds every
marker and fails once ``pyproject.toml`` reaches its release, so the code goes before that release ships.

A deprecated key still loads, with a warning, until that release. To deprecate a key, write a bridge: a function that
takes the file path, the parsed TOML and the release that removes the key, changes the old key to the new form in place
and calls ``warn`` with that release. Declare it with ``bridge``, which marks it as ``until`` does and registers it.
Its docstring is its entry under "Deprecated keys" in the generated configuration reference
(``docs/reference/configuration.mdx``). The release notes list it under ``Deprecated``.

When its marker fails, delete the bridge's function and its tests, and run ``make docs``. The old key then fails as an
unknown key, and the release notes list it under ``Removed``.

Where an unknown-key error would mislead, a ``removed_<key>`` guard fails with a link to the upgrading guide instead,
marked with ``until`` for the release after: a key that would load as something else (``[roles.worker]`` as a new
role), one that would silently change nothing (a ``PILOOP_PROJECT_<KEY>`` variable), or a key of a removed feature.
"""

import os
import re
import warnings
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from piloop.version import Version


class Marked(Protocol):
    """What ``until`` returns: a decorator that returns what it decorates."""

    def __call__[T](self, obj: T, /) -> T: ...


def _same[T](obj: T, /) -> T:
    return obj


def until(version: str, reason: str) -> Marked:
    """Mark compat code that piloop keeps until release ``version``. ``reason`` says what to delete and what replaces it.

    Call it as a statement where the code is (a branch, an alias, a table entry), or put it on a function or a class as
    a decorator. It only parses ``version`` (``Version.parse``), so a malformed one fails when the code runs.
    ``tests/test_deprecated.py`` reads every call in ``piloop/`` and fails once ``pyproject.toml`` is at ``version``.
    Write both arguments as literal strings, ``version`` as ``X.Y.Z``, and call it as ``deprecated.until`` (here,
    ``until``), so that the test can read it.

    Raises:
        ValueError: ``version`` is not ``X.Y.Z``.
    """
    Version.parse(version)
    return _same


type Rewrite = Callable[[Path, dict[str, object], Version], None]
"""The body of a bridge: it rewrites the parsed file in place, and warns that the old key goes in the given release."""


@dataclass(frozen=True)
class Bridge:
    """A bridge that ``apply`` runs on a parsed file: its rewrite, and the release that removes it."""

    rewrite: Rewrite
    removed_in: Version

    def __call__(self, toml: Path, raw: dict[str, object]) -> None:
        self.rewrite(toml, raw, self.removed_in)


BRIDGES: list[Bridge] = []
"""The bridges of ``piloop.toml``, in the order that they run: the order of their definitions."""
CONFIG_BRIDGES: list[Bridge] = []
"""The bridges of ``config.toml``, the global settings file (``piloop.settings.config_file``)."""


def bridge(version: str, reason: str, into: list[Bridge] = BRIDGES) -> Callable[[Rewrite], Bridge]:
    """Declare a bridge that piloop keeps until release ``version``, as ``until`` marks code, and register it in ``into``.

    The bridge gets ``version`` to pass to ``warn``. ``tests/test_deprecated.py`` reads every call, as it does
    ``until``'s.
    """
    removed_in = Version.parse(version)

    def register(rewrite: Rewrite) -> Bridge:
        found = Bridge(rewrite, removed_in)
        into.append(found)
        return found

    return register


# Where the ``default`` action records the warnings it showed, so a file loaded twice in one process warns once.
_shown: dict[str | tuple[str, type[Warning], int], int] = {}


class PiloopDeprecationWarning(DeprecationWarning):
    """A deprecated piloop key that still works until the release that removes it. The CLI shows each one once."""


def _table(raw: dict[str, object], *path: str) -> dict[str, object] | None:
    """Return the table at ``path`` in the parsed file, or None. A value that is not a table is left to validation."""
    table: object = raw
    for key in path:
        table = table.get(key) if isinstance(table, dict) else None
    return table if isinstance(table, dict) else None


def _nth(toml: Path, table: str, key: str) -> int:
    """How many lines of ``toml`` set ``key`` before the header of ``table``: ``warn``'s ``nth`` for ``table.key``."""
    lines = toml.read_text().splitlines()
    header = next((n for n, line in enumerate(lines) if line.strip() == f'[{table}]'), len(lines))
    return sum(line.partition('=')[0].strip() == key for line in lines[:header])


@bridge('0.4.0', 'kind in a trigger entry is provider: delete the trigger_kind bridge')
def trigger_kind(toml: Path, raw: dict[str, object], removed_in: Version) -> None:
    """``kind`` in an entry of ``[roles.<name>] triggers`` is ``provider``: ``{ provider = "cron" }``."""
    for name in _table(raw, 'roles') or {}:
        role = _table(raw, 'roles', name)
        triggers = role.get('triggers') if role is not None else None
        if (
            role is None
            or not isinstance(triggers, list)
            or not any(isinstance(t, dict) and 'kind' in t for t in triggers)
        ):
            continue
        role['triggers'] = [
            {('provider' if k == 'kind' else k): v for k, v in t.items()} if isinstance(t, dict) else t
            for t in triggers
        ]
        warn(
            toml,
            f'roles.{name}.triggers',
            'kind in a trigger is deprecated; write { provider = "<trigger>", … }',
            removed_in=removed_in,
            nth=_nth(toml, f'roles.{name}', 'triggers'),
        )


@bridge('0.4.0', '[stop] is the limits of [plugins.stop_rule]: delete the stop_table bridge')
def stop_table(toml: Path, raw: dict[str, object], removed_in: Version) -> None:
    """``[stop] <key>`` is ``[plugins.stop_rule] <key>``, a limit of the stop rule. A rule without that key refuses it.

    A key that ``[plugins.stop_rule]`` sets too keeps its value there.
    """
    if not isinstance(old := raw.get('stop'), dict):
        return
    del raw['stop']
    _into(raw, 'stop_rule', old)
    warn(toml, 'stop', '[stop] is deprecated; write its keys in [plugins.stop_rule]', removed_in=removed_in)


@bridge('0.4.0', '[spend] limit is [plugins.stop_rule] max_spend: delete the spend_limit bridge')
def spend_limit(toml: Path, raw: dict[str, object], removed_in: Version) -> None:
    """``[spend] limit`` is ``[plugins.stop_rule] max_spend``. A value that ``[plugins.stop_rule]`` sets too wins."""
    spend = _table(raw, 'spend')
    if spend is None or 'limit' not in spend:
        return
    _into(raw, 'stop_rule', {'max_spend': spend.pop('limit')})
    warn(
        toml,
        'spend.limit',
        '[spend] limit is deprecated; write max_spend in [plugins.stop_rule]',
        removed_in=removed_in,
        nth=_nth(toml, 'spend', 'limit'),
    )


@bridge('0.4.0', 'confidence is [plugins.verdict_rule] confidence: delete the confidence bridge')
def confidence(toml: Path, raw: dict[str, object], removed_in: Version) -> None:
    """The top-level ``confidence`` is ``[plugins.verdict_rule] confidence``. A value that the table sets too wins."""
    if 'confidence' not in raw:
        return
    _into(raw, 'verdict_rule', {'confidence': raw.pop('confidence')})
    warn(toml, 'confidence', 'confidence is deprecated; write it in [plugins.verdict_rule]', removed_in=removed_in)


OLD_RULE_ENV = re.compile(r'PILOOP_CAMPAIGN_(?:STOP__(?P<stop>\w+)|(?P<confidence>CONFIDENCE))', re.IGNORECASE)
"""The variable of a key of ``[stop]``, or of ``confidence``: those keys are in a rule's table now. ``[spend] limit`` has
none: ``[spend]`` is still a table, so its variable fails as an unknown key."""


@bridge('0.4.0', "the variables of [stop] and confidence are the rules': delete rule_env and OLD_RULE_ENV")
def rule_env(toml: Path, raw: dict[str, object], removed_in: Version) -> None:
    """``PILOOP_CAMPAIGN_STOP__<KEY>`` and ``PILOOP_CAMPAIGN_CONFIDENCE`` set keys of the rules' tables.

    They are ``PILOOP_CAMPAIGN_PLUGINS__STOP_RULE__<KEY>`` and ``PILOOP_CAMPAIGN_PLUGINS__VERDICT_RULE__CONFIDENCE``. The
    old variable still wins over the file.
    """
    for name, value in sorted(os.environ.items()):
        if (old := OLD_RULE_ENV.fullmatch(name)) is None:
            continue
        rule, key = ('stop_rule', old['stop'].lower()) if old['stop'] else ('verdict_rule', 'confidence')
        if (table := _rule(raw, rule)) is not None:
            table[key] = value
        new = f'PILOOP_CAMPAIGN_PLUGINS__{rule.upper()}__{key.upper()}'
        warn(toml, name, f'{name} is deprecated; set {new}', removed_in=removed_in)


def _into(raw: dict[str, object], rule: str, keys: Mapping[str, object]) -> None:
    """Add ``keys`` to ``[plugins.<rule>]`` of the parsed file, under the keys that the table sets."""
    if (table := _rule(raw, rule)) is not None:
        for key, value in keys.items():
            table.setdefault(key, value)


def _rule(raw: dict[str, object], rule: str) -> dict[str, object] | None:
    """``[plugins.<rule>]`` of the parsed file, added when it is missing.

    None when a value on its way is not a table: the validation refuses it.
    """
    plugins = raw.setdefault('plugins', {})
    table = plugins.setdefault(rule, {}) if isinstance(plugins, dict) else None
    return table if isinstance(table, dict) else None


@bridge(
    '0.4.0',
    '[storage.backend] and its eviction keys are [plugins.cas] cached: delete the storage_backend bridge',
    CONFIG_BRIDGES,
)
def storage_backend(toml: Path, raw: dict[str, object], removed_in: Version) -> None:
    """In ``config.toml``, ``[storage.backend]`` is the ``remote`` of ``[plugins.cas] provider = "cached"``.

    Its ``kind`` is the remote's ``provider``. ``evict_after_days`` and ``local_max_gb`` of ``[storage]`` move with
    it. A ``[plugins.cas]`` table of the file wins: the old keys then go, with the warning, and change nothing.
    """
    storage = _table(raw, 'storage')
    if storage is None or (backend := storage.pop('backend', None)) is None:
        return
    remote = dict(backend) if isinstance(backend, dict) else {}
    cached: dict[str, object] = {'provider': 'cached', 'remote': {'provider': remote.pop('kind', None), **remote}}
    for key in ('evict_after_days', 'local_max_gb'):
        if key in storage:
            cached[key] = storage.pop(key)
    plugins = raw.setdefault('plugins', {})
    if isinstance(plugins, dict):
        plugins.setdefault('cas', cached)
    warn(
        toml,
        'storage.backend',
        '[storage.backend] is deprecated; write [plugins.cas] provider = "cached" and remote = { provider = <its kind>,'
        ' <its keys> }, with evict_after_days and local_max_gb there too',
        removed_in=removed_in,
    )


@bridge(
    '0.4.0',
    'max_evaluations in config.toml is [plugins.executor.pools.serve] evaluations: delete the max_evaluations bridge',
    CONFIG_BRIDGES,
)
def max_evaluations(toml: Path, raw: dict[str, object], removed_in: Version) -> None:
    """In ``config.toml``, ``max_evaluations`` is ``evaluations`` in ``[plugins.executor.pools.serve]``.

    A value that ``[plugins.executor.pools.serve]`` sets too wins.
    """
    if (old := raw.pop('max_evaluations', None)) is None:
        return
    _slots(raw, old)
    warn(
        toml,
        'max_evaluations',
        'max_evaluations is deprecated; write evaluations in [plugins.executor.pools.serve]',
        removed_in=removed_in,
    )


OLD_MAX_EVALUATIONS_ENV = 'PILOOP_MAX_EVALUATIONS'


@until(
    '0.4.0',
    'PILOOP_MAX_EVALUATIONS is PILOOP_PLUGINS__EXECUTOR__POOLS__SERVE__EVALUATIONS: delete max_evaluations_env',
)
def max_evaluations_env(environ: Mapping[str, str], raw: dict[str, object]) -> dict[str, object]:
    """Return ``raw``, the parsed ``config.toml``, with ``PILOOP_MAX_EVALUATIONS`` in it, with a warning.

    It becomes ``[plugins.executor.pools.serve] evaluations``, unless the file sets that.
    """
    if (old := environ.get(OLD_MAX_EVALUATIONS_ENV)) is None:
        return raw
    _slots(raw, old)
    warnings.warn(
        f'{OLD_MAX_EVALUATIONS_ENV} is deprecated; set PILOOP_PLUGINS__EXECUTOR__POOLS__SERVE__EVALUATIONS;'
        ' removed in piloop 0.4.0',
        PiloopDeprecationWarning,
        stacklevel=2,
    )
    return raw


WORKERS = 'workers'
"""The executor whose slots ``max_evaluations`` was in 0.2.0, the only one there was."""


def _slots(raw: dict[str, object], old: object) -> None:
    """Put ``old`` in ``raw`` as ``[plugins.executor.pools.serve] evaluations``, if it selects ``workers`` or nothing.

    Another executor has no such key: the old one goes, with its warning, and changes nothing.
    """
    plugins = raw.setdefault('plugins', {})
    executor = plugins.setdefault('executor', {'provider': WORKERS}) if isinstance(plugins, dict) else None
    if isinstance(executor, dict) and executor.setdefault('provider', WORKERS) == WORKERS:
        pools = executor.setdefault('pools', {})
        serve = pools.setdefault('serve', {}) if isinstance(pools, dict) else None
        if isinstance(serve, dict):
            serve.setdefault('evaluations', old)


HERDR = 'herdr'
"""The name of the runtime that 0.3.0 removed."""
HERDR_UPGRADE = 'https://piloop.autoresearch.inc/guides/upgrading#the-herdr-runtime'
"""What to do about the removed herdr runtime."""


@until(
    '0.4.0',
    'runtime = "herdr" of a 0.2 piloop.toml: delete removed_runtime, HERDR, HERDR_UPGRADE and the runtime\'s _not_removed',
)
def removed_runtime(name: str) -> str | None:
    """Why ``runtime = <name>`` does not load when piloop removed that runtime, else None."""
    if name != HERDR:
        return None
    return f'the herdr runtime is removed in piloop 0.3.0: write runtime = "subprocess", or delete the key ({HERDR_UPGRADE})'


WORKDIR_UPGRADE = 'https://piloop.autoresearch.inc/guides/upgrading#workdir'
"""What to do about the removed ``[roles.<name>] workdir``."""


@until('0.4.0', 'workdir of a 0.2 piloop.toml: delete removed_workdir and WORKDIR_UPGRADE')
def removed_workdir() -> str:
    """Why a role table with ``workdir`` does not load: every agent works in a worktree of its own now."""
    return (
        'workdir is removed in piloop 0.3.0: every agent works in a worktree of its own, never in the campaign'
        f' checkout. Delete the key ({WORKDIR_UPGRADE})'
    )


ENTIRE_UPGRADE = 'https://piloop.autoresearch.inc/guides/upgrading#entire'
"""What to do about ``entire_agent``, which 0.3.0 removed with Entire."""
REMOVED_HARNESS_KEYS = {
    **dict.fromkeys(
        ('interactive', 'startup_dialogs', 'busy_screens', 'failed_turn_screens', 'queue_key', 'composer'),
        HERDR_UPGRADE,
    ),
    'entire_agent': ENTIRE_UPGRADE,
}
"""The keys of a ``[harness.<name>]`` table that 0.3.0 removed, with what to do about each: the keys that only the
herdr runtime read, and ``entire_agent``."""


@until('0.4.0', 'the keys of a 0.2 harness table: delete removed_harness_keys, REMOVED_HARNESS_KEYS, ENTIRE_UPGRADE')
def removed_harness_keys(harness: Collection[str]) -> str | None:
    """Why a ``[harness.<name>]`` table with a key that 0.3.0 removed does not load, else None."""
    by_link: dict[str, list[str]] = {}
    for key in (k for k in REMOVED_HARNESS_KEYS if k in harness):
        by_link.setdefault(REMOVED_HARNESS_KEYS[key], []).append(key)
    if not by_link:
        return None
    return '; '.join(
        f'{", ".join(keys)}: removed in piloop 0.3.0; delete the key ({link})' for link, keys in by_link.items()
    )


REMOVED_KEYS = 'https://piloop.autoresearch.inc/guides/upgrading#removed-keys'
"""The keys that 0.2.0 deprecated and 0.3.0 removed."""


@until('0.4.0', '[roles.worker] of a 0.2 piloop.toml: delete removed_worker and its call in Roles')
def removed_worker() -> str:
    """Why ``[roles.worker]`` does not load: it would define a new role, not set the implementer's keys."""
    return f'[roles.worker] is removed in piloop 0.3.0: rename the table [roles.implementer] ({REMOVED_KEYS})'


USAGE_PLACEHOLDERS = {
    '{claude_config_dir}': '{env:CLAUDE_CONFIG_DIR:~/.claude}',
    '{codex_home}': '{env:CODEX_HOME:~/.codex}',
}
"""The placeholders of a ``[harness.<name>.usage] file`` that 0.3.0 removed, with what to write instead."""


@until('0.4.0', '{claude_config_dir} and {codex_home} of a 0.2 usage file: delete removed_placeholders and its call')
def removed_placeholders(glob: str) -> str | None:
    """Why a usage ``file`` with a placeholder that 0.3.0 removed does not load, else None.

    Without this, the glob would match no session file, and no agent's tokens would be recorded.
    """
    if not (found := [old for old in USAGE_PLACEHOLDERS if old in glob]):
        return None
    for old in found:
        glob = glob.replace(old, USAGE_PLACEHOLDERS[old])
    return f'{" and ".join(found)}: removed in piloop 0.3.0; write {glob!r} ({REMOVED_KEYS})'


OLD_ENV_PREFIX = 'PILOOP_PROJECT_'
"""The prefix of the variables that override ``piloop.toml`` before 0.2.0. ``PILOOP_CAMPAIGN_`` since."""


@until('0.4.0', 'PILOOP_PROJECT_<KEY> of a 0.2 environment: delete removed_env, OLD_ENV_PREFIX and its call')
def removed_env(environ: Mapping[str, str]) -> str | None:
    """Why ``piloop.toml`` does not load while a ``PILOOP_PROJECT_<KEY>`` variable is set, else None.

    Without this, the variable would change nothing, silently.
    """
    if not (found := sorted(n for n in environ if n.upper().startswith(OLD_ENV_PREFIX))):
        return None
    renamed = ', '.join(f'{n} is {"PILOOP_CAMPAIGN_" + n[len(OLD_ENV_PREFIX) :]}' for n in found)
    return f'{renamed}: PILOOP_PROJECT_<KEY> is removed in piloop 0.3.0; rename or unset it ({REMOVED_KEYS})'


def show_in_cli() -> None:
    """Show each deprecation once. By default, Python hides a ``DeprecationWarning`` outside ``__main__``."""
    warnings.simplefilter('default', PiloopDeprecationWarning)


def warn(toml: Path, key: str, message: str, *, removed_in: Version, nth: int = 0) -> None:
    """Warn at the line of ``toml`` that sets ``key`` for the ``nth`` time, or at line 0 if no line sets it.

    A dotted ``key`` (``roles.research.scout``) is set by its table header, or by a line that sets its last part. The
    output looks like a compiler error: ``piloop.toml:12: PiloopDeprecationWarning: …``, then the line.
    """
    last = key.rpartition('.')[2]
    lines = [
        n
        for n, line in enumerate(toml.read_text().splitlines(), 1)
        if line.strip() == f'[{key}]' or line.partition('=')[0].strip() == last
    ]
    lineno = lines[nth] if nth < len(lines) else 0
    text = f'{message}; removed in piloop {removed_in}'
    warnings.warn_explicit(text, PiloopDeprecationWarning, str(toml), lineno, registry=_shown)


def apply(toml: Path, raw: dict[str, object], bridges: list[Bridge] = BRIDGES) -> None:
    """Run every bridge on the parsed file. ``config.load_campaign_config`` calls this before it validates the file."""
    for b in bridges:
        b(toml, raw)
