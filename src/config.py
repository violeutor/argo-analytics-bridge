"""
Bridge Config — lädt aus Env-Vars / .env
"""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Railway Postgres (Shadow-DB)
    database_url: str

    # Anthropic (Claude NER)
    anthropic_api_key: str

    # Optionaler API-Key für internen Zugriff (Argo-Backend → Bridge)
    bridge_api_key: str = ""

    # Rate-Limit Bundesanzeiger (Sekunden zwischen Requests)
    ba_rate_limit_sec: float = 3.0

    # Cron-Schedule (APScheduler cron-Syntax)
    cron_hour: int = 3      # 03:00 UTC täglich
    cron_minute: int = 0

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
