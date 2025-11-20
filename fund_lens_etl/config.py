"""Configuration management using Pydantic settings."""

from datetime import datetime
from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

# FEC data availability constants
FEC_DATA_START_YEAR = 1980  # FEC electronic data begins in 1980


def get_current_cycle() -> int:
    """
    Get the current election cycle (current or next even year).

    Returns:
        Current election cycle year
    """
    current_year = datetime.now().year
    if current_year % 2 == 0:
        return current_year
    return current_year + 1


def get_max_cycle() -> int:
    """
    Get maximum allowed election cycle (4 years beyond current cycle).

    Returns:
        Maximum election cycle year
    """
    return get_current_cycle() + 4


class ElectionCycle(int):
    """
    Validated election cycle (two-year period ending in even year).

    FEC uses two-year cycles ending in even years (e.g., 2024 covers 2023-2024).
    """

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v: int) -> int:
        """Validate election cycle is an even year within valid range."""
        if not isinstance(v, int):
            raise ValueError("Election cycle must be an integer")

        if v < FEC_DATA_START_YEAR:
            raise ValueError(
                f"Election cycle must be {FEC_DATA_START_YEAR} or later "
                f"(FEC electronic data starts in {FEC_DATA_START_YEAR})"
            )

        max_cycle = get_max_cycle()
        if v > max_cycle:
            raise ValueError(
                f"Election cycle must be {max_cycle} or earlier " f"(current cycle + 4 years)"
            )

        if v % 2 != 0:
            raise ValueError(
                f"Election cycle must be an even year (e.g., 2024, 2026). "
                f"Got {v}. Did you mean {v + 1}?"
            )

        return v


# Find project root (where .env should be)
PROJECT_ROOT = Path(__file__).parent.parent


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # API Configuration
    fec_api_key: str

    # Database Configuration
    database_url: str

    # ETL Configuration
    batch_size: int = 1000

    # Incremental Load Configuration
    # 180-day lookback accounts for:
    # - Quarterly filing deadlines (~45 days after quarter end)
    # - Late amendments to previous filings
    # - FEC processing delays
    lookback_days: int = 180
    sort_order: str = "asc"

    # Rate Limiting
    max_requests_per_minute: int = 55
    max_requests_per_hour: int = 950
    api_rate_limit_delay: float = 1.1

    # Logging
    log_level: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),  # Explicit path to .env
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


# noinspection PyArgumentList
@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()  # type: ignore


def validate_election_cycle(cycle: int) -> int:
    """
    Validate and return election cycle.

    Args:
        cycle: Election cycle year

    Returns:
        Validated cycle year

    Raises:
        ValueError: If cycle is invalid
    """
    return ElectionCycle.validate(cycle)
