"""Hold the daemon's lock on a pid file, the way ``piloop.daemon.serve.acquire`` does: ``hold_lock.py <pid file> [stubborn]``.

Prints ``held`` once the lock is taken; ``stubborn`` ignores SIGTERM, for the ``stop`` timeout test.
"""

import fcntl
import os
import signal
import sys
import time


def main() -> None:
    """Lock, write our pid, announce, sleep."""
    fd = os.open(sys.argv[1], os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    os.ftruncate(fd, 0)
    os.write(fd, str(os.getpid()).encode())
    os.fsync(fd)
    if sys.argv[2:] == ['stubborn']:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    print('held', flush=True)
    time.sleep(60)


if __name__ == '__main__':
    main()
