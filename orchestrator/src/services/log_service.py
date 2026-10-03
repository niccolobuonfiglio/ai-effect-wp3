"""Logging configuration for the orchestrator."""

import json
import logging
import os
import time
from datetime import datetime, timezone
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from services.log_retention import prune_stale_files, start_keepalive

ALERTS_LOGGER = "orchestrator.alerts"

_RESERVED_ATTRS = frozenset(
    vars(logging.LogRecord("", 0, "", 0, "", (), None)).keys()
) | {"message", "asctime", "taskName"}


def get_alerts_logger() -> logging.Logger:
    """Logger for operational alerts meant to be forwarded to TEF monitoring."""
    return logging.getLogger(ALERTS_LOGGER)


class JsonFormatter(logging.Formatter):
    """Formats each record as a single-line JSON object.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in vars(record).items():
            if key in _RESERVED_ATTRS or key.startswith("_"):
                continue
            payload[key] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class SizeAndTimeRotatingHandler(TimedRotatingFileHandler):
    """Log handler that rotates logs by both size and time."""

    def __init__(self, filename, max_bytes, backup_count=0, **kwargs):
        """Initialize handler with size and time-based rotation."""
        self.max_bytes = max_bytes
        super().__init__(filename, backupCount=backup_count, **kwargs)

    def shouldRollover(self, record):
        """Determine if rollover should occur (by time or file size)."""
        # Time-based rollover
        t = int(time.time())
        if t >= self.rolloverAt:
            return 1

        # Size-based rollover
        if self.stream and self.max_bytes > 0:
            self.stream.seek(0, os.SEEK_END)
            if self.stream.tell() >= self.max_bytes:
                return 1

        return 0

    def doRollover(self):
        """Perform the log file rollover."""
        super().doRollover()
        self.rolloverAt = self.computeRollover(int(time.time()))


def configure_logging(
    log_dir: str = "logs",
    log_file: str = "orchestrator.log",
    level: int = logging.INFO,
    max_bytes: int = 5 * 1024 * 1024,  # 5 MB
    backup_count: int = 7,
    console: bool = True,
    log_format: str | None = None,
    alerts_file: str | None = "alerts.jsonl",
    manage_retention: bool = True,
) -> logging.Logger:
    """Configure root logger with console and rotating file handlers.
    """
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(level)

    # Clear existing handlers
    logger.handlers.clear()

    log_format = (log_format or os.environ.get("LOG_FORMAT", "text")).lower()
    if log_format not in ("text", "json"):
        raise ValueError(f"log_format must be 'text' or 'json', got {log_format!r}")

    if log_format == "json":
        formatter: logging.Formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
        )

    # File handler with size and time rotation
    file_handler = SizeAndTimeRotatingHandler(
        filename=os.path.join(log_dir, log_file),
        when="midnight",
        interval=1,
        max_bytes=max_bytes,
        backup_count=backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler
    if console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    alerts_logger = get_alerts_logger()
    for handler in alerts_logger.handlers:
        handler.close()
    alerts_logger.handlers.clear()
    if alerts_file:
        alerts_handler = SizeAndTimeRotatingHandler(
            filename=os.path.join(log_dir, alerts_file),
            when="midnight",
            interval=1,
            max_bytes=max_bytes,
            backup_count=backup_count,
            encoding="utf-8",
        )
        alerts_handler.setFormatter(JsonFormatter())
        alerts_logger.addHandler(alerts_handler)

    if manage_retention:
        own = [name for name in (log_file, alerts_file) if name]
        prune_stale_files(log_dir, keep=set(own))
        start_keepalive([Path(log_dir) / name for name in own])

    return logger
