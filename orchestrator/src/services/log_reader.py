"""Read the orchestrator's own logs and alerts for the dashboard.

The API and every worker replica write into one shared logs directory (a Docker
volume), each process to its own files:

* ``orchestrator.log``, ``worker-<hostname>.log`` - normal logs
* ``alerts-api-<hostname>.jsonl``, ``alerts-worker-<hostname>.jsonl`` - alerts

This module merges them. Only the current files are read (rotated copies such as
``orchestrator.log.2026-09-02`` are skipped), and only the tail of each file, so
a dashboard polling every few seconds stays cheap.

Log lines can contain raw service error text, and this is exposed through the
API, so every line goes through the same redaction as stored errors.
"""

import json
import os
import re
from pathlib import Path

from services.log_retention import is_stale, retention_days
from services.privacy import sanitize_error

MAX_LINES = 1000
TAIL_BYTES = 512 * 1024
LINE_CHARS = 2000

_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?")
_DEFAULT_DIRS = ("logs", "/app/src/logs")


def clamp(value: int, low: int = 1, high: int = MAX_LINES) -> int:
    """Keep a requested count within bounds."""
    return max(low, min(high, int(value)))


def _sort_key(timestamp: str) -> str:
    """Normalise '2026-09-18T23:23:03.38' and '2026-09-18 23:23:03,38' alike."""
    return timestamp.replace("T", " ").replace(",", ".")


class LogReader:
    """Merged, redacted view of the shared logs directory.

    Args:
        log_dir: Directory to read. Defaults to ``LOG_DIR``, then ``logs``, then
            ``/app/src/logs``, whichever exists first.
        tail_bytes: Maximum bytes read from the end of each file.
        max_age_days: Files not written to for longer are skipped (they belong
            to processes that are gone). Defaults to ``LOG_RETENTION_DAYS``.
    """

    def __init__(
        self,
        log_dir: str | Path | None = None,
        tail_bytes: int = TAIL_BYTES,
        max_age_days: float | None = None,
    ):
        if tail_bytes <= 0:
            raise ValueError("tail_bytes must be positive")
        if max_age_days is not None and max_age_days <= 0:
            raise ValueError("max_age_days must be positive")
        self._explicit_dir = Path(log_dir) if log_dir else None
        self._tail_bytes = tail_bytes
        self._max_age_days = max_age_days

    # files
    def directory(self) -> Path | None:
        """The logs directory in use, or None if none exists."""
        candidates = (
            [self._explicit_dir]
            if self._explicit_dir
            else [os.environ.get("LOG_DIR"), *_DEFAULT_DIRS]
        )
        for candidate in candidates:
            if candidate and Path(candidate).is_dir():
                return Path(candidate)
        return None

    def _files(self, directory: Path, pattern: str) -> list[Path]:
        """Current files matching ``pattern``, skipping stale ones."""
        max_age = self._max_age_days or retention_days()
        return [p for p in sorted(directory.glob(pattern)) if not is_stale(p, max_age)]

    def _tail(self, path: Path) -> list[str]:
        """Last lines of a file, reading at most ``tail_bytes``."""
        try:
            with open(path, "rb") as f:
                f.seek(0, os.SEEK_END)
                size = f.tell()
                start = max(0, size - self._tail_bytes)
                f.seek(start)
                data = f.read()
        except OSError:
            return []
        lines = data.decode("utf-8", errors="replace").splitlines()
        if start > 0 and lines:
            lines = lines[1:]  
        return lines

    # logs
    def _records(self) -> list[tuple[str, str]]:
        """All log records from the current *.log files as (sort key, text).
        """
        directory = self.directory()
        if directory is None:
            return []

        records: list[tuple[str, str]] = []
        for path in self._files(directory, "*.log"):
            current: list[str] | None = None
            key = ""
            for line in self._tail(path):
                match = _TIMESTAMP.search(line[:64])
                if match:
                    if current is not None:
                        records.append((key, "\n".join(current)))
                    key = _sort_key(match.group(0))
                    current = [line]
                elif current is not None:
                    current.append(line)
            if current is not None:
                records.append((key, "\n".join(current)))

        records.sort(key=lambda r: r[0])  
        return records

    def workflow_log(self, workflow_id: str, lines: int = 200) -> tuple[list[str], bool]:
        """Recent log records mentioning a workflow.

        Returns:
            (records, matched). When nothing mentions the workflow, the most
            recent records of all processes are returned with matched=False.
        """
        if not workflow_id or not workflow_id.strip():
            raise ValueError("workflow_id is required")
        lines = clamp(lines)

        records = [text for _, text in self._records()]
        matching = [text for text in records if workflow_id in text]
        chosen = (matching or records)[-lines:]
        return [sanitize_error(text, LINE_CHARS) for text in chosen], bool(matching)

    # alerts
    def alerts(self, limit: int = 50) -> list[dict]:
        """Newest alerts from every current ``alerts*.jsonl`` file."""
        limit = clamp(limit)
        directory = self.directory()
        if directory is None:
            return []

        alerts: list[dict] = []
        for path in self._files(directory, "alerts*.jsonl"):
            for line in self._tail(path):
                try:
                    alert = json.loads(line)
                except ValueError:
                    continue
                if not isinstance(alert, dict):
                    continue
                for field in ("message", "last_error"):
                    if isinstance(alert.get(field), str):
                        alert[field] = sanitize_error(alert[field], LINE_CHARS)
                alerts.append(alert)

        alerts.sort(key=lambda a: _sort_key(str(a.get("timestamp", ""))), reverse=True)
        return alerts[:limit]
