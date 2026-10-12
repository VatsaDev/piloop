"""Score ``solve.py``. Prints ``METRIC accuracy=`` and ``METRIC cost_seconds=``.

Kaggle's test set has no labels, so the holdout is a fixed 80/20 split of ``data/train.csv`` by PassengerId.
``cost_seconds`` is the CPU time (user + system) that ``solve.py`` takes to train and predict on one thread. It is the
lowest of ``REPEATS`` runs, so a busy machine changes it less. Any failure exits non-zero with the reason on stderr,
and piloop records a crash. When piloop sets ``PILOOP_UNITS_FILE``, this script also writes each holdout passenger's
share of the accuracy there, so the verdict can tell a broad gain from a lucky few rows.
"""

import csv
import json
import os
import resource
import subprocess
import sys
import tempfile
from pathlib import Path

DATA = Path(__file__).parent / 'data' / 'train.csv'
HOLDOUT_EVERY = 5
REPEATS = 3
TIMEOUT = 120
ONE_THREAD = dict.fromkeys(('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS'), '1')


def main() -> int:
    """Split, run ``solve.py`` ``REPEATS`` times, score the first run's predictions."""
    with DATA.open(newline='') as f:
        rows = list(csv.DictReader(f))
    train = [r for r in rows if int(r['PassengerId']) % HOLDOUT_EVERY != 0]
    holdout = [r for r in rows if int(r['PassengerId']) % HOLDOUT_EVERY == 0]
    labels = {r['PassengerId']: r['Survived'] for r in holdout}
    fields = list(rows[0])

    with tempfile.TemporaryDirectory() as tmp:
        t = Path(tmp)
        _write(t / 'train.csv', fields, train)
        _write(t / 'test.csv', [c for c in fields if c != 'Survived'], holdout)
        cpu: list[float] = []
        for i in range(REPEATS):
            out = t / f'preds-{i}.csv'
            cmd = [sys.executable, 'solve.py', '--train', t / 'train.csv', '--test', t / 'test.csv', '--out', out]
            before = _children_cpu()
            try:
                proc = subprocess.run(
                    [str(c) for c in cmd], capture_output=True, text=True, timeout=TIMEOUT, env=os.environ | ONE_THREAD
                )
            except subprocess.TimeoutExpired:
                return _fail(f'solve.py exceeded {TIMEOUT}s')
            cpu.append(_children_cpu() - before)
            if proc.returncode != 0:
                return _fail(f'solve.py exit {proc.returncode}\n{proc.stdout[-2000:]}\n{proc.stderr[-4000:]}')
        try:
            with (t / 'preds-0.csv').open(newline='') as f:
                preds = {p['PassengerId']: str(int(float(p['Survived']))) for p in csv.DictReader(f)}
        except (OSError, KeyError, ValueError) as e:
            return _fail(f'could not read predictions: {e}')

    missing = labels.keys() - preds.keys()
    if missing:
        return _fail(f'missing predictions for {len(missing)} rows')
    correct = sum(preds[k] == v for k, v in labels.items())
    print(f'METRIC accuracy={correct / len(labels):.4f}')
    print(f'METRIC cost_seconds={min(cpu):.2f}')
    if units_file := os.environ.get('PILOOP_UNITS_FILE'):
        shares = {k: (preds[k] == v) / len(labels) for k, v in labels.items()}
        Path(units_file).write_text(json.dumps({'accuracy': shares}))
    return 0


def _children_cpu() -> float:
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return usage.ru_utime + usage.ru_stime


def _fail(reason: str) -> int:
    print(reason, file=sys.stderr)
    return 1


def _write(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    with path.open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)


if __name__ == '__main__':
    raise SystemExit(main())
