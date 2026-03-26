"""
Centralized logging configuration for the Aggregator service.

Logging Levels (low → high severity):
  DEBUG    – detailed diagnostic info (frame counts, internal state)
  INFO     – high-level operational messages (job started, S3 upload done)
  WARNING  – something unexpected happened but processing can continue
  ERROR    – a failure that needs attention (failed S3 op, service call error)
  CRITICAL – a severe failure that stops the service from working

Developer log file → /app/logs/aggregator.log  (JSON format, DEBUG+)
Console output     → stdout/docker logs          (human-readable, INFO+)
"""

import logging
import os
import json
from datetime import datetime, timedelta
from log_context import request_id_var, user_id_var

LOG_DIR = "/app/logs"

def _current_log_file(service_name: str) -> str:
    """Return the current month log file path for a service."""
    now = datetime.utcnow()
    filename = f"{service_name}-{now.month:02d}-{now.year}.jsonl"
    return os.path.join(LOG_DIR, filename)


def _cleanup_old_logs(service_name: str, months_to_keep: int = 3) -> None:
    """
    Delete log files older than `months_to_keep` months for the given service.
    Expects files named like: service_name-MM-YYYY.jsonl
    """
    try:
        if not os.path.isdir(LOG_DIR):
            return

        now = datetime.utcnow()
        for fname in os.listdir(LOG_DIR):
            if not fname.startswith(f"{service_name}-") or not fname.endswith(".jsonl"):
                continue

            parts = fname[len(service_name) + 1 : -5].split("-")  # MM-YYYY
            if len(parts) != 2:
                continue
            month_str, year_str = parts
            try:
                month = int(month_str)
                year = int(year_str)
                file_date = datetime(year=year, month=month, day=1)
            except ValueError:
                continue

            months_diff = (now.year - file_date.year) * 12 + (now.month - file_date.month)
            if months_diff >= months_to_keep:
                try:
                    os.remove(os.path.join(LOG_DIR, fname))
                except OSError:
                    pass
    except Exception:
        pass


class CustomJsonFormatter(logging.Formatter):
    """
    Format logs as JSON with standard fields matching the Java backend format.
    Injects request_id and user_id from contextvars.
    """
    def __init__(self, service_name: str):
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        # Standardize severity names to match Java expectations
        severity = record.levelname

        request_id = request_id_var.get("unknown") or "unknown"
        user_id = user_id_var.get("unknown") or "unknown"

        log_obj = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "severity": severity,
            "service": self.service_name,
            "pid": record.process,
            "thread": record.threadName,
            "requestId": request_id,
            "userId": user_id,
            "message": record.getMessage()
        }
        
        # Add exception info if present
        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)
            
        return json.dumps(log_obj)


def setup_logger(name: str) -> logging.Logger:
    """
    Create and return a logger with:
      - A StreamHandler (console) using a human-readable format at INFO level.
      - A FileHandler (JSON) writing to /app/logs/aggregator.jsonl at DEBUG level.
    """
    os.makedirs(LOG_DIR, exist_ok=True)
    _cleanup_old_logs(service_name="face_service")

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # allow all levels; handlers filter independently

    log_mode = os.getenv("LOG_MODE", "users").lower()
    file_log_level = logging.DEBUG if log_mode == "developers" else logging.INFO

    if logger.handlers:
        for h in logger.handlers:
            if isinstance(h, logging.FileHandler):
                h.setLevel(file_log_level)
        return logger

    # --- Console Handler (human-readable, INFO+) ---
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
    )
    logger.addHandler(console_handler)

    # --- File Handler (JSON, INFO+, monthly file) ---
    log_file = _current_log_file(service_name="face_service")
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(file_log_level)
    file_handler.setFormatter(CustomJsonFormatter(service_name="face_service"))
    logger.addHandler(file_handler)

    return logger
