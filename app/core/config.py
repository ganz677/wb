import os
from dotenv import load_dotenv
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

load_dotenv(os.path.join(os.path.dirname(__file__), "../../.env"))


class DataBaseSettings(BaseModel):
    user: str
    password: str
    host: str
    port: int
    db_name: str
    pool_pre_ping: bool = True

    @property
    def url(self) -> str:
        return f"postgresql+psycopg://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"


class APISettings(BaseModel):
    WB_TOKEN: str | None = None
    WB_BASE_URL: str = "https://feedbacks-api.wildberries.ru/api/v1"

    GEMINI_TOKENS: str | None = None
    GEMINI_MODEL: str | None = "gemini-2.5-flash"

    TAKE: int = 500
    POLL_INTERVAL_SEC: int = 30


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="APP__",
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        populate_by_name=True,
    )

    db: DataBaseSettings
    api_keys: APISettings



settings: Settings = Settings()
