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
"""

import os
import re
import warnings
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from piloop.actor import Agent
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


SCOUT_SHARED = ('runtime', 'harnesses', 'model', 'timeout', 'mcp', 'addendum')
"""The keys of ``[roles.research]`` that a scout sweep used before the scout was a role of its own."""


def _table(raw: dict[str, object], *path: str) -> dict[str, object] | None:
    """Return the table at ``path`` in the parsed file, or None. A value that is not a table is left to validation."""
    table: object = raw
    for key in path:
        table = table.get(key) if isinstance(table, dict) else None
    return table if isinstance(table, dict) else None


def _is(trigger: object, name: str) -> bool:
    """Whether ``trigger`` is an entry of the trigger ``name``: ``trigger_kind`` has made ``kind`` its ``provider``."""
    return isinstance(trigger, dict) and trigger.get('provider') == name


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


RESEARCH_TRIGGERS = ({'provider': 'low_water'}, {'provider': 'seed'}, {'provider': 'stalled'})
"""The default triggers of research."""


@bridge('0.3.0', '[roles.worker] is [roles.implementer]: delete the worker_role bridge')
def worker_role(toml: Path, raw: dict[str, object], removed_in: Version) -> None:
    """``[roles.worker]`` is ``[roles.implementer]``: the role that runs one idea in its own worktree.

    A key that ``[roles.implementer]`` sets too keeps its value there.
    """
    roles = _table(raw, 'roles')
    if roles is None or (old := roles.pop('worker', None)) is None:
        return
    roles['implementer'] = (old if isinstance(old, dict) else {}) | (_table(roles, 'implementer') or {})
    warn(toml, 'roles.worker', '[roles.worker] is deprecated; write [roles.implementer]', removed_in=removed_in)


@bridge('0.3.0', '[roles.research.scout] is [roles.scout]: delete the research_scout bridge')
def research_scout(toml: Path, raw: dict[str, object], removed_in: Version) -> None:
    """``[roles.research.scout]`` is ``[roles.scout]``, a role of its own.

    The keys of ``[roles.research]`` that the scout used (``runtime``, ``harnesses``, ``model``, ``timeout``, ``mcp``
    and ``addendum``) are copied into it, unless ``[roles.scout]`` sets them.
    """
    research = _table(raw, 'roles', 'research')
    if research is None or (scout := research.pop('scout', None)) is None:
        return
    roles = _table(raw, 'roles')
    assert roles is not None  # research is in it
    shared = {k: research[k] for k in SCOUT_SHARED if k in research}
    roles['scout'] = shared | (scout if isinstance(scout, dict) else {}) | (_table(roles, 'scout') or {})
    warn(
        toml,
        'roles.research.scout',
        '[roles.research.scout] is deprecated; write [roles.scout], a role of its own, with the keys of'
        ' [roles.research] that it needs (mcp, harnesses, …)',
        removed_in=removed_in,
    )


@bridge('0.3.0', 'the scout trigger of [roles.research] is gone: delete the scout_trigger bridge')
def scout_trigger(toml: Path, raw: dict[str, object], removed_in: Version) -> None:
    """The ``scout`` trigger of ``[roles.research] triggers`` is gone: the scout is a role with its own ``cron``."""
    research = _table(raw, 'roles', 'research')
    triggers = research.get('triggers') if research is not None else None
    if research is None or not isinstance(triggers, list) or not any(_is(t, 'scout') for t in triggers):
        return
    research['triggers'] = [t for t in triggers if not _is(t, 'scout')]
    warn(
        toml,
        'roles.research.triggers',
        'the scout trigger is deprecated; delete it: the scout is [roles.scout], with its own cron trigger',
        removed_in=removed_in,
        nth=_nth(toml, 'roles.research', 'triggers'),
    )


@bridge('0.3.0', '[roles.research] buffer is a low_water trigger buffer: delete the research_buffer bridge')
def research_buffer(toml: Path, raw: dict[str, object], removed_in: Version) -> None:
    """``[roles.research] buffer`` is the ``buffer`` of research's ``low_water`` trigger.

    Without ``triggers`` in the table, research gets its default triggers with that ``buffer``.
    """
    research = _table(raw, 'roles', 'research')
    if research is None or 'buffer' not in research:
        return
    buffer = research.pop('buffer')
    triggers = research.get('triggers', RESEARCH_TRIGGERS)
    if isinstance(triggers, list | tuple):
        research['triggers'] = [t | {'buffer': buffer} if _is(t, 'low_water') else t for t in triggers]
    warn(
        toml,
        'roles.research.buffer',
        '[roles.research] buffer is deprecated; write triggers ='
        ' [{ provider = "low_water", buffer = … }, { provider = "seed" }, { provider = "stalled" }]',
        removed_in=removed_in,
    )


@bridge('0.3.0', '[roles.research] context is the top-level context: delete the research_context bridge')
def research_context(toml: Path, raw: dict[str, object], removed_in: Version) -> None:
    """``[roles.research] context`` is the top-level ``context``: every sweep's research pack lists the files."""
    research = _table(raw, 'roles', 'research')
    if research is None or 'context' not in research:
        return
    old = research.pop('context')
    top = raw.get('context', [])
    raw['context'] = [*top, *old] if isinstance(top, list) and isinstance(old, list) else old
    warn(
        toml,
        'roles.research.context',
        '[roles.research] context is deprecated; write context = [...] at the top of the file',
        removed_in=removed_in,
    )


USAGE_PLACEHOLDERS = {
    '{claude_config_dir}': '{env:CLAUDE_CONFIG_DIR:~/.claude}',
    '{codex_home}': '{env:CODEX_HOME:~/.codex}',
}


@bridge('0.3.0', '{claude_config_dir} and {codex_home} are {env:…} placeholders: delete the usage_dirs bridge')
def usage_dirs(toml: Path, raw: dict[str, object], removed_in: Version) -> None:
    """``{claude_config_dir}`` and ``{codex_home}`` in ``[harness.<name>.usage] file`` are ``{env:…}`` placeholders.

    ``{claude_config_dir}`` is ``{env:CLAUDE_CONFIG_DIR:~/.claude}``, and ``{codex_home}`` is
    ``{env:CODEX_HOME:~/.codex}``.
    """
    harnesses = _table(raw, 'harness') or {}
    for name in harnesses:
        usage = _table(raw, 'harness', name, 'usage')
        glob = usage.get('file') if usage is not None else None
        if usage is None or not isinstance(glob, str) or not any(old in glob for old in USAGE_PLACEHOLDERS):
            continue
        for old, new in USAGE_PLACEHOLDERS.items():
            glob = glob.replace(old, new)
        usage['file'] = glob
        warn(
            toml,
            f'harness.{name}.usage.file',
            f'{{claude_config_dir}} and {{codex_home}} are deprecated; write {glob!r}',
            removed_in=removed_in,
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
    '0.3.0', '[storage.remote] in config.toml is [storage.backend]: delete the storage_remote bridge', CONFIG_BRIDGES
)
def storage_remote(toml: Path, raw: dict[str, object], removed_in: Version) -> None:
    """In ``config.toml``, ``[storage.remote]`` is ``[storage.backend]``: the storage backend that mirrors the store.

    A key that ``[storage.backend]`` sets too keeps its value there.
    """
    storage = _table(raw, 'storage')
    if storage is None or (old := storage.pop('remote', None)) is None:
        return
    storage['backend'] = (old if isinstance(old, dict) else {}) | (_table(storage, 'backend') or {})
    warn(toml, 'storage.remote', '[storage.remote] is deprecated; write [storage.backend]', removed_in=removed_in)


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


@until('0.4.0', 'entire_agent of a 0.2 piloop.toml: delete removed_entire_agent, ENTIRE_UPGRADE and its call')
def removed_entire_agent(harness: Mapping[str, object]) -> str | None:
    """Why a ``[harness.<name>]`` table does not load when it sets ``entire_agent``, else None."""
    if 'entire_agent' not in harness:
        return None
    return f'entire_agent is removed in piloop 0.3.0, with Entire: delete the key ({ENTIRE_UPGRADE})'


OLD_ENV_PREFIX = 'PILOOP_PROJECT_'
"""The prefix of the environment variables that override ``piloop.toml`` before 0.2.0. ``PILOOP_CAMPAIGN_`` now."""


@until('0.3.0', 'PILOOP_PROJECT_<KEY> is PILOOP_CAMPAIGN_<KEY>: delete old_env and OLD_ENV_PREFIX')
def old_env(toml: Path, environ: Mapping[str, str]) -> None:
    """Warn for each ``PILOOP_PROJECT_<KEY>`` variable: it still overrides its key, after ``PILOOP_CAMPAIGN_<KEY>``."""
    for name in sorted(n for n in environ if n.upper().startswith(OLD_ENV_PREFIX)):
        new = 'PILOOP_CAMPAIGN_' + name[len(OLD_ENV_PREFIX) :]
        warn(toml, name, f'{name} is deprecated; set {new}', removed_in=Version(0, 3, 0))


OLD_SIGNATURE = 'worker#'
"""The signature of an implementer before 0.2.0, ``worker#<id>``. An agent that an older daemon started outlives an
upgrade with it: in its ``PILOOP_ACTOR``, and in the SQL that its brief told it to write (``'<you>'``)."""


@until('0.3.0', 'a 0.1.0 implementer signs worker#<id>: delete signatures and OLD_SIGNATURE, match <kind>#<id>')
def signatures(agent: Agent) -> tuple[str, ...]:
    """Return the signatures of an agent session's rows: ``<kind>#<id>``, and an implementer's ``worker#<id>`` too.

    The checks that find an agent's own rows (its work at startup, its finding, its open escalation, its own comments,
    its silence) match authors against these, so a row that a 0.1.0 agent signs after the upgrade still counts.
    """
    return (agent.signature, f'{OLD_SIGNATURE}{agent.session}') if agent.role == 'implementer' else (agent.signature,)


@until('0.3.0', 'PILOOP_ACTOR=worker#<id> of a 0.1.0 agent: delete old_actor and its call in Settings.actor')
def old_actor(actor: str) -> str:
    """Return ``actor`` with the implementer's signature for ``worker#<id>``, with a warning; else ``actor`` as it is."""
    if not actor.startswith(OLD_SIGNATURE):
        return actor
    new = 'implementer#' + actor.removeprefix(OLD_SIGNATURE)
    warnings.warn(
        f'PILOOP_ACTOR={actor} is deprecated; the daemon signs an implementer {new}; removed in piloop 0.3.0',
        PiloopDeprecationWarning,
        stacklevel=2,
    )
    return new


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
