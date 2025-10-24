from __future__ import annotations

from typing import Optional

from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import Integer, Text, DateTime, String, Enum, Index, func
from datetime import datetime
from app.db.enums import Status


class CommonMixin:
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    wb_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    nm_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True,
    )
    text: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
    )
    created_at_wb: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    status: Mapped[Status] = mapped_column(
        Enum(Status), index=True, default=Status.loaded
    )
    answer_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
