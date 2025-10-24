from pydantic import BaseModel
from datetime import datetime


class FeedbackIn(BaseModel):
    id: str
    text: str
    createdDate: datetime
    nmId: int
    username: str | None = None
    productValuation: int | None = None


class QuestionIn(BaseModel):
    id: str
    text: str
    createdDate: datetime
    nmId: int
