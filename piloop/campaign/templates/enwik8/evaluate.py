"""Score ``compressor.py`` on the first 10 MB of enwik8. Prints ``METRIC bytes=`` and ``METRIC seconds=``.

``bytes`` is the compressed size plus the size of ``compressor.py``: as in the Hutter Prize, the decompressor counts.
``seconds`` is the wall-clock time of compression plus decompression. Each step runs in its own Python process, so
this process measures the time and the compressor cannot change the result. If the round trip does not give back
the exact input, or a step fails, this script exits non-zero with the reason on stderr, and piloop records a crash.
"""

import subprocess
import sys
import tempfile
import time
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
        seconds = 0.0
        for step, src, dst in (('compress', 'input', 'blob'), ('decompress', 'blob', 'output')):
            started = time.monotonic()
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
            seconds += time.monotonic() - started
            if proc.returncode != 0:
                return fail(f'{step} failed with exit code {proc.returncode}\n{proc.stderr[-4000:]}')
        if (t / 'output').read_bytes() != original:
            return fail('decompress(compress(data)) is not equal to data')
        compressed = (t / 'blob').stat().st_size
    print(f'METRIC bytes={compressed + COMPRESSOR.stat().st_size}')
    print(f'METRIC seconds={seconds:.2f}')
    return 0


def fail(reason: str) -> int:
    """Print the reason on stderr and return the exit code for a crash."""
    print(reason, file=sys.stderr)
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
