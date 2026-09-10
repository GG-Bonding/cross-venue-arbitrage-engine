import os
from pathlib import Path


class SessionLock:
    """OS-owned lock: a crashed process releases ownership without deleting audit files."""

    def __init__(self, database: Path):
        self.path = Path(str(database.resolve()) + ".run.lock")
        self.file = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.file = self.path.open("a+b")
        if self.file.tell() == 0:
            self.file.write(b"0")
            self.file.flush()
        self.file.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self.file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self.file.close()
            raise RuntimeError(
                f"Paper session already running for database lock={self.path}"
            ) from exc
        return self

    def __exit__(self, *_):
        # Closing the handle releases the OS lock on both Windows and POSIX.
        self.file.close()
