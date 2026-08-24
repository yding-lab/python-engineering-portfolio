"""Centralized logging configuration for the pipeline."""

from __future__ import annotations

import json
import logging
import logging.config
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


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

        for field in (
            "pipeline_run_id",
            "stage",
            "record_count",
            "duration_seconds",
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
                "stream": "ext://sys.stdout",
            },
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": log_level.upper(),
                "formatter": "json",
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
