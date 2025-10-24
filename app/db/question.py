from .mixins import TimeStampMixin, CommonMixin
from .base import Base


class Question(CommonMixin, TimeStampMixin, Base):
    pass
