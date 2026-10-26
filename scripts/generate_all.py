"""Run all code and documentation generation steps: ``uv run -m scripts.generate_all [--mode write|check|dry-run]``.

Each module of ``scripts.generate`` returns the full content of one generated file. The files are the reference pages
that are generated whole, the parts of other pages under ``docs/snippets/generated/`` (a page imports each one as a
Mintlify snippet), and the trigger SQL. ``--mode check`` fails on the first stale file and shows its diff.
"""

import argparse

from scripts.generate import (
    Mode,
    capabilities,
    cas_providers,
    configuration,
    contracts,
    data_model,
    emit,
    evaluate_examples,
    example_inline_executor,
    example_keep_cost,
    example_min_gain,
    example_nice_runtime,
    example_sqlite_cas,
    example_stop_after_keeps,
    harnesses,
    imports,
    mcp_keys,
    openapi,
    roles,
    runtime_protocol,
    runtimes,
    schema,
    sections,
    settings,
    states,
    stop,
    triggers,
)

GENERATORS = (
    configuration,
    schema,
    data_model,
    states,
    imports,
    roles,
    triggers,
    sections,
    runtimes,
    runtime_protocol,
    cas_providers,
    harnesses,
    mcp_keys,
    evaluate_examples,
    stop,
    settings,
    example_nice_runtime,
    example_sqlite_cas,
    example_min_gain,
    example_stop_after_keeps,
    example_keep_cost,
    example_inline_executor,
    capabilities,
    openapi,
)


def main(mode: Mode) -> None:
    """Generate every file in ``mode``."""
    for generator in GENERATORS:
        emit(generator.PATH, generator.generate(), mode)
    for path, content in contracts.generate_each().items():  # one module, a file for each contract
        emit(path, content, mode)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--mode', type=Mode, choices=list(Mode), default=Mode.WRITE)
    main(parser.parse_args().mode)
