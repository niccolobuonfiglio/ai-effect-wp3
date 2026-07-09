"""Logging configuration for the orchestrator."""

import logging
import os
import time
from logging.handlers import TimedRotatingFileHandler


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
) -> logging.Logger:
    """Configure root logger with console and rotating file handlers.

    Args:
        log_dir: Directory for log files.
        log_file: Log file name.
        level: Logging level.
        max_bytes: Max file size before rotation.
        backup_count: Number of backup files to keep.
        console: Whether to also log to console.

    Returns:
        Configured root logger.
    """
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(level)

    # Clear existing handlers
    logger.handlers.clear()

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

    return logger
