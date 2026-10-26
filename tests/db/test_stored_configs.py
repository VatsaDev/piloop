"""The stored configs of real homes migrate through the current chain and validate.

``tests/fixtures/configs/<revision>-<n>.json`` is a ``campaigns.config`` as a real home stored it at that revision,
structure only: its keys, tables and value types, with every path, name, command and text replaced. Together, the
files of a revision hold every key and value type that those homes stored at it. A campaign on the herdr runtime was
moved off it first, as the upgrade guide asks before 0.3. ``0018-4`` has a ``[harness.codex]`` table of piloop 0.2,
with ``mcp`` null.
"""

import sqlite3
from contextlib import closing
from itertools import groupby
from pathlib import Path

import pytest
from alembic import command
from pydantic import TypeAdapter, ValidationError
from sqlalchemy import create_engine

from piloop.config import CampaignConfig
from piloop.db.session import migrate
from tests.conftest import disk
from tests.db.test_migrations import alembic

CONFIGS = Path(__file__).parents[1] / 'fixtures' / 'configs'
STORED = TypeAdapter(CampaignConfig)
BY_REVISION = {
    revision: [path.stem for path in paths]
    for revision, paths in groupby(sorted(CONFIGS.glob('*.json')), key=lambda path: path.stem.partition('-')[0])
}


@pytest.mark.parametrize('revision', sorted(BY_REVISION))
def test_each_stored_config_of_a_real_home_migrates_and_validates(tmp_path: Path, revision: str) -> None:
    path = tmp_path / 'piloop.db'
    command.upgrade(alembic(path), revision)
    names = BY_REVISION[revision]
    with closing(sqlite3.connect(path)) as con:
        for name in names:
            config = (CONFIGS / f'{name}.json').read_text()
            con.execute('INSERT INTO campaigns (name, path, config) VALUES (?, ?, ?)', (name, f'/{name}', config))
        con.commit()
    engine = create_engine(f'sqlite:///{path}')
    migrate(engine, cas=disk(tmp_path), backups=0, daemon=None)
    engine.dispose()
    failed = {}
    with closing(sqlite3.connect(path)) as con:
        for name, config in con.execute('SELECT name, config FROM campaigns ORDER BY id'):
            try:
                STORED.validate_json(config)
            except ValidationError as e:
                failed[name] = str(e)
    assert failed == {}, 'an upgrade breaks a config that a real home stored'
