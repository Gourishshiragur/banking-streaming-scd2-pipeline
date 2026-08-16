"""
Structured JSON logging so pipeline runs are machine-parseable
(easy to ship to Log Analytics / CloudWatch / ELK in a real deployment).
"""
import json
import logging
import sys
import time


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        # allow callers to attach structured fields via `extra={"fields": {...}}`
        if hasattr(record, "fields"):
            payload.update(record.fields)
        return json.dumps(payload)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


class timed_block:
    """Context manager that logs task duration + status with structured fields."""

    def __init__(self, logger: logging.Logger, task_id: str, batch_id: str):
        self.logger = logger
        self.task_id = task_id
        self.batch_id = batch_id

    def __enter__(self):
        self._start = time.time()
        self.logger.info(
            f"task started: {self.task_id}",
            extra={"fields": {"task_id": self.task_id, "batch_id": self.batch_id, "status": "STARTED"}},
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = round(time.time() - self._start, 3)
        status = "FAILED" if exc_type else "SUCCEEDED"
        level = self.logger.error if exc_type else self.logger.info
        level(
            f"task {status.lower()}: {self.task_id}",
            extra={
                "fields": {
                    "task_id": self.task_id,
                    "batch_id": self.batch_id,
                    "status": status,
                    "duration_seconds": duration,
                }
            },
        )
        return False  # do not suppress exceptions
