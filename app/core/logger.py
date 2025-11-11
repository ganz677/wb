from __future__ import annotations
import json
import logging
from logging import LogRecord
import os

class JSONFormatter(logging.Formatter):
    def format(self, record: LogRecord) -> str:
        base = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "lvl": record.levelname,
            "name": record.name,
            "msg": record.getMessage(),
        }
        for key in ("event", "ingest", "generated", "sent", "wb", "retry_after", "sleep_sec"):
            if hasattr(record, key):
                base[key] = getattr(record, key)
        if record.exc_info:
            base["exc"] = self.formatException(record.exc_info)
        return json.dumps(base, ensure_ascii=False)

def setup_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    root = logging.getLogger()
    root.setLevel(level)

    for h in list(root.handlers):
        root.removeHandler(h)

    h = logging.StreamHandler()
    h.setLevel(level)
    h.setFormatter(JSONFormatter())
    root.addHandler(h)

    logging.captureWarnings(True)
