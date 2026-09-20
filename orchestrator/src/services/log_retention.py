"""Retention for the shared logs directory.

Every orchestrator process writes its own log and alert files into one shared
volume, named after the container hostname (``worker-<hostname>.log``,
``alerts-worker-<hostname>.jsonl``, …). A container gets a new hostname each
time it is recreated, and the old files are never rotated or deleted again,
because rotation only applies to files a running process writes. Without
cleanup the directory would grow with every restart.

Two mechanisms keep it bounded:

* **Pruning.** At startup each process deletes per-process files, and rotated
  copies, that nobody has written to for longer than the retention period
  (``LOG_RETENTION_DAYS``, default 7, the same as the rotation backups).
* **Keep-alive.** A live process may be idle and write nothing for days, so each
  process refreshes the modification time of its own files every hour. Only
  files of processes that are gone can therefore look stale.
"""

import os
import threading
import time
from pathlib import Path

DEFAULT_RETENTION_DAYS = 7.0
KEEPALIVE_SECONDS = 3600.0

# Files that belong to one process, plus rotated copies (``<name>.<date>``).
# The API's current ``orchestrator.log`` is not listed: its name never changes
# and its own handler manages it; only its rotated copies are.
PRUNABLE_PATTERNS = (
    "worker-*.log*",
    "alerts-*.jsonl*",
    "orchestrator.log.*",
    "worker_daemon.log*",  # names used before per-process files
    "alerts.jsonl*",
)

_keepalive_lock = threading.Lock()
_keepalive_paths: list[Path] = []
_keepalive_thread: threading.Thread | None = None


def retention_days() -> float:
    """Retention period from ``LOG_RETENTION_DAYS``, default 7 days."""
    raw = os.environ.get("LOG_RETENTION_DAYS")
    if raw is None or not raw.strip():
        return DEFAULT_RETENTION_DAYS
    try:
        days = float(raw)
    except ValueError as e:
        raise ValueError(f"LOG_RETENTION_DAYS must be a number, got {raw!r}") from e
    if days <= 0:
        raise ValueError("LOG_RETENTION_DAYS must be positive")
    return days


def is_stale(path: Path, max_age_days: float, now: float | None = None) -> bool:
    """True if the file has not been modified for ``max_age_days``."""
    now = time.time() if now is None else now
    try:
        return now - path.stat().st_mtime > max_age_days * 86400
    except OSError:
        return False


def prune_stale_files(
    log_dir: str | Path,
    max_age_days: float | None = None,
    keep: set[str] | None = None,
    now: float | None = None,
) -> list[Path]:
    """Delete per-process files not written to for ``max_age_days``.

    Args:
        log_dir: The shared logs directory.
        max_age_days: Retention period; defaults to :func:`retention_days`.
        keep: File names never deleted (the calling process's own files).
        now: Current time, for tests.

    Returns:
        The deleted paths.
    """
    directory = Path(log_dir)
    if not directory.is_dir():
        return []
    max_age = retention_days() if max_age_days is None else max_age_days
    keep = keep or set()

    deleted: list[Path] = []
    candidates = {p for pattern in PRUNABLE_PATTERNS for p in directory.glob(pattern)}
    for path in sorted(candidates):
        if path.name in keep or not path.is_file():
            continue
        if is_stale(path, max_age, now):
            try:
                path.unlink()
                deleted.append(path)
            except OSError:
                continue  # removed by another process at the same time
    return deleted


def touch(paths: list[Path]) -> None:
    """Refresh the modification time of existing files."""
    for path in paths:
        try:
            os.utime(path)
        except OSError:
            continue


def _keepalive_loop(interval: float) -> None:
    while True:
        time.sleep(interval)
        with _keepalive_lock:
            paths = list(_keepalive_paths)
        touch(paths)


def start_keepalive(paths: list[Path], interval: float = KEEPALIVE_SECONDS) -> None:
    """Keep this process's files fresh so other processes never prune them.

    Runs one daemon thread per process; calling it again replaces the paths.
    """
    global _keepalive_thread
    if interval <= 0:
        raise ValueError("interval must be positive")
    with _keepalive_lock:
        _keepalive_paths[:] = [Path(p) for p in paths]
        if _keepalive_thread is None or not _keepalive_thread.is_alive():
            _keepalive_thread = threading.Thread(
                target=_keepalive_loop, args=(interval,), name="log-keepalive", daemon=True
            )
            _keepalive_thread.start()


def keepalive_paths() -> list[Path]:
    """Files currently kept alive by this process (for tests and diagnostics)."""
    with _keepalive_lock:
        return list(_keepalive_paths)
