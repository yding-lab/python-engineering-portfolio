"""Centralized logging configuration for the pipeline."""

from __future__ import annotations

import json
import logging
import logging.config
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SECRET_PATTERNS = [
    re.compile(r"(://[^:/@\s]+:)[^@\s]+(@)"),  # user:password@ in connection URLs
    re.compile(r"(?i)((?:password|passwd|pwd|secret|token|api[_-]?key)\s*[=:]\s*)[^\s,;&'\"]+"),
]


def redact(text: str) -> str:
    """Mask credentials in a log message."""
    for pattern in SECRET_PATTERNS:
        text = pattern.sub(lambda m: m.group(1) + "***" + (m.group(2) if m.lastindex == 2 else ""), text)
    secret = os.getenv("DB_PASSWORD")
    if secret and len(secret) >= 4:
        text = text.replace(secret, "***")
    return text


class RedactingFilter(logging.Filter):
    """Applied to every handler: credentials never reach the console or the log file."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact(record.getMessage())
        record.args = None
        if record.exc_info and record.exc_info[1] is not None:
            record.exc_text = redact(logging.Formatter().formatException(record.exc_info))
            record.exc_info = None
        return True


class JsonFormatter(logging.Formatter):
    """Format structured logs as JSON for local or cloud ingestion."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        elif record.exc_text:
            payload["exception"] = record.exc_text

        for field in (
            "pipeline_run_id",
            "stage",
            "record_count",
            "duration_seconds",
            "destination",
        ):
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value

        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_logging(
    log_level: str = "INFO",
    log_directory: str | Path = "logs",
) -> None:
    """Configure console and rotating file-style logging.

    The project writes human-readable output to the console and structured
    JSON lines to a file so logs can later be shipped to systems such as
    CloudWatch, Datadog, or ELK.
    """
    log_dir = Path(log_directory)
    log_dir.mkdir(parents=True, exist_ok=True)

    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "filters": {"redact": {"()": RedactingFilter}},
        "formatters": {
            "console": {
                "format": (
                    "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
                )
            },
            "json": {
                "()": JsonFormatter,
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": log_level.upper(),
                "formatter": "console",
                "filters": ["redact"],
                "stream": "ext://sys.stdout",
            },
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": log_level.upper(),
                "formatter": "json",
                "filters": ["redact"],
                "filename": str(log_dir / "pipeline.jsonl"),
                "maxBytes": 5_000_000,
                "backupCount": 5,
                "encoding": "utf-8",
            },
        },
        "root": {
            "level": log_level.upper(),
            "handlers": ["console", "file"],
        },
    }

    logging.config.dictConfig(config)


def get_logger(name: str) -> logging.Logger:
    """Return a named logger for a pipeline module."""
    return logging.getLogger(name)
