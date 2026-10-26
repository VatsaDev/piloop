"""Score ``compressor.py`` on the first 10 MB of enwik8, and report ``bytes`` and ``seconds`` as one JSON line.

``bytes`` is the compressed size plus the size of ``compressor.py``: as in the Hutter Prize, the decompressor counts.
``seconds`` is the CPU time (user + system) of compression plus decompression. Each step runs in its own Python
process, so this process measures the time and the compressor cannot change the result. CPU time, unlike wall-clock
time, changes little when other jobs load the machine. If the round trip does not give back the exact input, or a
step fails, this script exits non-zero with the reason on stderr, and piloop records a crash.
"""

import json
import os
import resource
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).parent
ARCHIVE = ROOT / 'data' / 'enwik8.zip'
COMPRESSOR = ROOT / 'compressor.py'
SIZE = 10**7
TIMEOUT = 300
STEP = """
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import compressor
src, dst = Path(sys.argv[3]), Path(sys.argv[4])
dst.write_bytes(getattr(compressor, sys.argv[2])(src.read_bytes()))
"""


def main() -> int:
    """Run the round trip and print the metrics."""
    with zipfile.ZipFile(ARCHIVE) as z, z.open('enwik8') as f:
        original = f.read(SIZE)
    with tempfile.TemporaryDirectory() as tmp:
        t = Path(tmp)
        (t / 'input').write_bytes(original)
        for step, src, dst in (('compress', 'input', 'blob'), ('decompress', 'blob', 'output')):
            try:
                proc = subprocess.run(
                    [sys.executable, '-c', STEP, str(ROOT), step, str(t / src), str(t / dst)],
                    capture_output=True,
                    text=True,
                    timeout=TIMEOUT,
                    cwd=t,
                )
            except subprocess.TimeoutExpired:
                return fail(f'{step} took longer than {TIMEOUT} s')
            if proc.returncode != 0:
                return fail(f'{step} failed with exit code {proc.returncode}\n{proc.stderr[-4000:]}')
        if (t / 'output').read_bytes() != original:
            return fail('decompress(compress(data)) is not equal to data')
        compressed = (t / 'blob').stat().st_size
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    report(
        {
            'metrics': {
                'bytes': compressed + COMPRESSOR.stat().st_size,
                'seconds': round(usage.ru_utime + usage.ru_stime, 2),
            }
        }
    )
    return 0


def report(result: dict[str, object]) -> None:
    """Append the result to ``PILOOP_RESULT_FILE`` when piloop runs this command, else print it."""
    line = json.dumps(result)
    if path := os.environ.get('PILOOP_RESULT_FILE'):
        with open(path, 'a') as f:
            f.write(line + '\n')
    else:
        print(line)


def fail(reason: str) -> int:
    """Print the reason on stderr and return the exit code for a crash."""
    print(reason, file=sys.stderr)
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
