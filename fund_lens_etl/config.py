"""Configuration management using Pydantic settings."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # API Configuration
    fec_api_key: str

    # Database Configuration
    database_url: str

    # ETL Configuration
    election_cycle: int = 2026
    target_state: str = "MD"
    max_pages_per_committee: int | None = None
    batch_size: int = 1000
    api_rate_limit_delay: float = 0.1

    # Logging
    log_level: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


# noinspection PyArgumentList
@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()  # type: ignore
