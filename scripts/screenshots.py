"""Render ``piloop watch`` screens of a real campaign as SVG files, for the docs.

    uv run python scripts/screenshots.py <PILOOP_HOME> <campaign> <out_dir>

It drives the real app headless (Textual's test pilot) on the campaign's database and saves what each screen shows.
It only reads: it presses navigation keys, never a key that writes.
"""

import asyncio
import sys
from pathlib import Path

from piloop.actor import System
from piloop.db.session import Database
from piloop.watch.app import Watch

SIZE = (160, 46)
SHOTS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ('watch-campaign', ()),
    ('watch-candidates', ('4',)),
    ('watch-ideas', ('3',)),
)


async def shoot(db: Database, db_file: Path, campaign: str, out: Path) -> None:
    """Open the campaign once per shot, press its keys, and save the screen."""
    for name, keys in SHOTS:
        app = Watch(db, db_file, campaign, System.HUMAN)
        async with app.run_test(size=SIZE) as pilot:
            await pilot.pause(0.5)
            for key in keys:
                await pilot.press(key)
                await pilot.pause(0.3)
            app.save_screenshot(filename=f'{name}.svg', path=str(out))
        print(out / f'{name}.svg')


def main() -> None:
    """Parse the arguments and render every shot."""
    home, campaign, out = Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3])
    out.mkdir(parents=True, exist_ok=True)
    db_file = home / 'piloop.db'
    db = Database(f'sqlite:///{db_file}', actor=System.HUMAN)
    try:
        asyncio.run(shoot(db, db_file, campaign, out))
    finally:
        db.close()


if __name__ == '__main__':
    main()
