import enum


class Status(str, enum.Enum):
    loaded = "loaded"
    generated = "generated"
    sent = "sent"
    failed = "failed"
