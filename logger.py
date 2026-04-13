# ============================================================
# utils/logger.py — Centralized Structured JSON Logging
# ============================================================

import json
import logging
import sys
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    """Emit log records as single-line JSON — compatible with Cloud Logging."""

    LEVEL_MAP = {
        logging.DEBUG:    "DEBUG",
        logging.INFO:     "INFO",
        logging.WARNING:  "WARNING",
        logging.ERROR:    "ERROR",
        logging.CRITICAL: "CRITICAL",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "severity":  self.LEVEL_MAP.get(record.levelno, "INFO"),
            "logger":    record.name,
            "message":   record.getMessage(),
            "module":    record.module,
            "funcName":  record.funcName,
            "lineNo":    record.lineno,
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def get_logger(name: str = "healthcare_pipeline", level: int = logging.INFO) -> logging.Logger:
    """Return a structured JSON logger.

    Usage
    -----
    >>> from utils.logger import get_logger
    >>> log = get_logger(__name__)
    >>> log.info("Pipeline started", extra={"run_date": "2024-01-01"})
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.propagate = False
    return logger
