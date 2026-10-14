"""Run all code and documentation generation steps: ``uv run -m scripts.generate_all [--mode write|check|dry-run]``.

Each module of ``scripts.generate`` returns the full content of one generated file. The files are the reference pages
that are generated whole, the parts of other pages under ``docs/snippets/generated/`` (a page imports each one as a
Mintlify snippet), and the trigger SQL. ``--mode check`` fails on the first stale file and shows its diff.
"""

import argparse

from scripts.generate import (
    Mode,
    actor_triggers,
    configuration,
    cwd_encoders,
    data_model,
    emit,
    evaluate_examples,
    harnesses,
    herdr_settings,
    imports,
    mcp_keys,
    persistent_triggers,
    queue_keys,
    readers,
    remotes,
    renderers,
    role_defaults,
    role_keys,
    roles,
    run_role_keys,
    runtime_protocol,
    runtimes,
    schema,
    settings,
    states,
    stop,
    sweep_role_keys,
    tallies,
    triggers,
)

GENERATORS = (
    configuration,
    schema,
    actor_triggers,
    persistent_triggers,
    data_model,
    states,
    imports,
    roles,
    triggers,
    runtimes,
    runtime_protocol,
    remotes,
    harnesses,
    mcp_keys,
    renderers,
    readers,
    tallies,
    cwd_encoders,
    evaluate_examples,
    stop,
    role_keys,
    run_role_keys,
    sweep_role_keys,
    role_defaults,
    queue_keys,
    settings,
    herdr_settings,
)


def main(mode: Mode) -> None:
    """Generate every file in ``mode``."""
    for generator in GENERATORS:
        emit(generator.PATH, generator.generate(), mode)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--mode', type=Mode, choices=list(Mode), default=Mode.WRITE)
    main(parser.parse_args().mode)
