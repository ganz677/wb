# app/logger.py
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
        # складываем популярные extra-поля
        for key in ("event", "ingest", "generated", "sent", "wb", "retry_after", "sleep_sec"):
            if hasattr(record, key):
                base[key] = getattr(record, key)
        if record.exc_info:
            base["exc"] = self.formatException(record.exc_info)
        return json.dumps(base, ensure_ascii=False)

def setup_logging() -> None:
    # Уровень из ENV, по умолчанию INFO
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    root = logging.getLogger()
    root.setLevel(level)

    # убрать старые хендлеры (важно при перезапусках)
    for h in list(root.handlers):
        root.removeHandler(h)

    h = logging.StreamHandler()
    h.setLevel(level)
    h.setFormatter(JSONFormatter())
    root.addHandler(h)

    # чтобы дочерние логгеры писали в root
    logging.captureWarnings(True)
