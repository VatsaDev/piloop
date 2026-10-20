"""Deprecated ``piloop.toml`` keys. A deprecated key still loads, with a warning, until the release that removes it.

To deprecate a key, add a bridge to ``BRIDGES``. A bridge is a function that takes the file path and the parsed TOML,
changes the old key to the new form in place and calls ``warn``. Its docstring is its entry under "Deprecated keys" in
the generated configuration reference (``docs/reference/configuration.mdx``). The release notes list it under
``Deprecated``.

To remove a bridge, delete its function, its ``BRIDGES`` entry and its tests, and run ``make docs``. The release notes
list the key under ``Removed``. The old key then fails as an unknown key.
"""

import warnings
from collections.abc import Callable
from pathlib import Path

Bridge = Callable[[Path, dict[str, object]], None]

# Where the ``default`` action records the warnings it showed, so a file loaded twice in one process warns once.
_shown: dict[str | tuple[str, type[Warning], int], int] = {}


class PiloopDeprecationWarning(DeprecationWarning):
    """A deprecated piloop key that still works until the release that removes it. The CLI shows each one once."""


REMOVED_IN = '0.3.0'
"""The release that removes the keys that this release deprecates."""
SCOUT_SHARED = ('runtime', 'harnesses', 'model', 'timeout', 'mcp', 'addendum', 'workdir')
"""The keys of ``[roles.research]`` that a scout sweep used before the scout was a role of its own."""


def _table(raw: dict[str, object], *path: str) -> dict[str, object] | None:
    """Return the table at ``path`` in the parsed file, or None. A value that is not a table is left to validation."""
    table: object = raw
    for key in path:
        table = table.get(key) if isinstance(table, dict) else None
    return table if isinstance(table, dict) else None


def research_scout(toml: Path, raw: dict[str, object]) -> None:
    """``[roles.research.scout]`` is ``[roles.scout]``, a role of its own.

    The keys of ``[roles.research]`` that the scout used (``runtime``, ``harnesses``, ``model``, ``timeout``, ``mcp``,
    ``addendum`` and ``workdir``) are copied into it, unless ``[roles.scout]`` sets them.
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
        removed_in=REMOVED_IN,
    )


def scout_trigger(toml: Path, raw: dict[str, object]) -> None:
    """The ``scout`` trigger of ``[roles.research] triggers`` is gone: the scout is a role with its own ``cron``."""
    research = _table(raw, 'roles', 'research')
    triggers = research.get('triggers') if research is not None else None
    if research is None or not isinstance(triggers, list) or not any(_is_scout(t) for t in triggers):
        return
    research['triggers'] = [t for t in triggers if not _is_scout(t)]
    lines = toml.read_text().splitlines()
    header = next((n for n, line in enumerate(lines) if line.strip() == '[roles.research]'), len(lines))
    nth = sum(line.partition('=')[0].strip() == 'triggers' for line in lines[:header])
    warn(
        toml,
        'roles.research.triggers',
        'the scout trigger is deprecated; delete it: the scout is [roles.scout], with its own cron trigger',
        removed_in=REMOVED_IN,
        nth=nth,
    )


def _is_scout(trigger: object) -> bool:
    return isinstance(trigger, dict) and trigger.get('kind') == 'scout'


RESEARCH_TRIGGERS = ({'kind': 'low_water'}, {'kind': 'seed'})
"""The default triggers of research when ``[roles.research] buffer`` was deprecated."""


def research_buffer(toml: Path, raw: dict[str, object]) -> None:
    """``[roles.research] buffer`` is the ``buffer`` of research's ``low_water`` trigger.

    Without ``triggers`` in the table, research gets its default triggers with that ``buffer``.
    """
    research = _table(raw, 'roles', 'research')
    if research is None or 'buffer' not in research:
        return
    buffer = research.pop('buffer')
    triggers = research.get('triggers', RESEARCH_TRIGGERS)
    if isinstance(triggers, list | tuple):
        research['triggers'] = [
            t | {'buffer': buffer} if isinstance(t, dict) and t.get('kind') == 'low_water' else t for t in triggers
        ]
    warn(
        toml,
        'roles.research.buffer',
        '[roles.research] buffer is deprecated; write triggers = [{ kind = "low_water", buffer = … }, { kind = "seed" }]',
        removed_in=REMOVED_IN,
    )


def research_context(toml: Path, raw: dict[str, object]) -> None:
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
        removed_in=REMOVED_IN,
    )


USAGE_PLACEHOLDERS = {
    '{claude_config_dir}': '{env:CLAUDE_CONFIG_DIR:~/.claude}',
    '{codex_home}': '{env:CODEX_HOME:~/.codex}',
}


def usage_dirs(toml: Path, raw: dict[str, object]) -> None:
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
            removed_in=REMOVED_IN,
        )


BRIDGES: tuple[Bridge, ...] = (research_scout, scout_trigger, research_buffer, research_context, usage_dirs)


def herdr_keys(toml: Path, raw: dict[str, object]) -> None:
    """In ``config.toml``, ``herdr_<key> = …`` is ``<key> = …`` in its ``[runtimes.herdr]`` table.

    The Herdr settings are the Herdr runtime's own. Their environment variables (``PILOOP_HERDR_<KEY>``) do not change.
    """
    for key in [k for k in raw if k.startswith('herdr_')]:
        runtimes = raw.setdefault('runtimes', {})
        herdr = runtimes.setdefault('herdr', {}) if isinstance(runtimes, dict) else None
        if not isinstance(herdr, dict):
            return  # a [runtimes] value that is not a table of tables: the validation refuses it
        herdr[key.removeprefix('herdr_')] = raw.pop(key)
        warn(
            toml,
            key,
            f'{key} in config.toml is deprecated; write {key.removeprefix("herdr_")} in its [runtimes.herdr] table',
            removed_in=REMOVED_IN,
        )


CONFIG_BRIDGES: tuple[Bridge, ...] = (herdr_keys,)
"""The bridges of ``config.toml``, the global settings file (``piloop.settings.config_file``)."""


def show_in_cli() -> None:
    """Show each deprecation once. By default, Python hides a ``DeprecationWarning`` outside ``__main__``."""
    warnings.simplefilter('default', PiloopDeprecationWarning)


def warn(toml: Path, key: str, message: str, *, removed_in: str, nth: int = 0) -> None:
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


def bridge(toml: Path, raw: dict[str, object], bridges: tuple[Bridge, ...] = BRIDGES) -> None:
    """Run every bridge on the parsed file. ``config.load_project_config`` calls this before it validates the file."""
    for b in bridges:
        b(toml, raw)
