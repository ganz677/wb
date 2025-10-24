from typing import Optional

from sqlalchemy import String, Integer
from sqlalchemy.orm import Mapped, mapped_column

from .mixins import TimeStampMixin, CommonMixin
from .base import Base


class Feedback(CommonMixin, TimeStampMixin, Base):
    username: Mapped[Optional[str]] = mapped_column(
        String(128),
        nullable=True,
    )
    product_name: Mapped[Optional[str]] = mapped_column(
        String(128),
        nullable=True,
    )
    product_valuation: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True,
    )
