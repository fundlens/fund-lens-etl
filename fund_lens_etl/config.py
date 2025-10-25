"""Config module"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database configuration
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "root")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "fund_lens")

# FEC API configuration
FEC_API_KEY = os.getenv("FEC_API_KEY")
if not FEC_API_KEY:
    raise ValueError("FEC_API_KEY environment variable is required")

FEC_API_BASE_URL = os.getenv("FEC_API_BASE_URL", "https://api.open.fec.gov/v1")
FEC_API_TIMEOUT = int(os.getenv("FEC_API_TIMEOUT", "30"))  # seconds
FEC_MAX_RETRIES = int(os.getenv("FEC_MAX_RETRIES", "3"))
FEC_RATE_LIMIT_CALLS = int(
    os.getenv("FEC_RATE_LIMIT_CALLS", "1000")
)  # calls per hour (standard key)
FEC_RATE_LIMIT_PERIOD = int(
    os.getenv("FEC_RATE_LIMIT_PERIOD", "3600")
)  # seconds (1 hour)
FEC_RESULTS_PER_PAGE = int(
    os.getenv("FEC_RESULTS_PER_PAGE", "100")
)  # max allowed by FEC

# Retry configuration
FEC_RETRY_BACKOFF_FACTOR = float(
    os.getenv("FEC_RETRY_BACKOFF_FACTOR", "2.0")
)  # exponential backoff: 1s, 2s, 4s
FEC_RETRY_STATUSES = [429, 500, 502, 503, 504]  # HTTP status codes to retry
