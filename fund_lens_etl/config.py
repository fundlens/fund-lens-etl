"""Configuration management using Pydantic settings."""

from datetime import datetime
from enum import Enum
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class USState(str, Enum):
    """US State codes."""

    AL = "AL"
    AK = "AK"
    AZ = "AZ"
    AR = "AR"
    CA = "CA"
    CO = "CO"
    CT = "CT"
    DE = "DE"
    FL = "FL"
    GA = "GA"
    HI = "HI"
    ID = "ID"
    IL = "IL"
    IN = "IN"
    IA = "IA"
    KS = "KS"
    KY = "KY"
    LA = "LA"
    ME = "ME"
    MD = "MD"
    MA = "MA"
    MI = "MI"
    MN = "MN"
    MS = "MS"
    MO = "MO"
    MT = "MT"
    NE = "NE"
    NV = "NV"
    NH = "NH"
    NJ = "NJ"
    NM = "NM"
    NY = "NY"
    NC = "NC"
    ND = "ND"
    OH = "OH"
    OK = "OK"
    OR = "OR"
    PA = "PA"
    RI = "RI"
    SC = "SC"
    SD = "SD"
    TN = "TN"
    TX = "TX"
    UT = "UT"
    VT = "VT"
    VA = "VA"
    WA = "WA"
    WV = "WV"
    WI = "WI"
    WY = "WY"
    DC = "DC"


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


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # API Configuration
    fec_api_key: str

    # Database Configuration
    database_url: str

    # ETL Configuration
    batch_size: int = 1000

    # Incremental Load Configuration
    lookback_days: int = 90  # Days to look back from last processed contribution
    sort_order: str = "asc"  # Default to ascending (oldest first)

    # Rate Limiting (FEC API limits: 60 requests/minute, 1000 requests/hour)
    max_requests_per_minute: int = 55  # Buffer below limit
    max_requests_per_hour: int = 950  # Buffer below limit
    api_rate_limit_delay: float = 1.1  # Minimum seconds between requests

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
