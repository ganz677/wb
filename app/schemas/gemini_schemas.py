from pydantic import BaseModel


class AnswerInput(BaseModel):
    kind: str
    text: str
    product_name: str | None = None
    rating: int | None = None
