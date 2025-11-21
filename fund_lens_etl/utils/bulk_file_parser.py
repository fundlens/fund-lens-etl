"""Utilities for parsing FEC bulk data files."""

import logging
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

import pandas as pd
from prefect import get_run_logger
from prefect.exceptions import MissingContextError


def get_logger():
    """Get logger - Prefect if available, otherwise standard logging."""
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(__name__)


def parse_fec_date(date_str: str | None) -> datetime | None:
    """
    Parse FEC date format (MMDDYYYY or MDDYYYY) to datetime.

    FEC dates may have leading zeros omitted (e.g., "3312025" for "03312025").

    Args:
        date_str: Date string in MMDDYYYY or MDDYYYY format (e.g., "12312024", "3312025")

    Returns:
        Parsed datetime or None if invalid/empty

    Examples:
        >>> parse_fec_date("12312024")
        datetime.datetime(2024, 12, 31, 0, 0)
        >>> parse_fec_date("3312025")
        datetime.datetime(2025, 3, 31, 0, 0)
        >>> parse_fec_date(None)
        None
        >>> parse_fec_date("")
        None
    """
    if not date_str or pd.isna(date_str):
        return None

    try:
        # Remove any whitespace
        date_str = str(date_str).strip()

        # FEC dates can be 7 or 8 characters (leading zero may be omitted)
        if len(date_str) == 7:
            # MDDYYYY format - pad with leading zero
            date_str = "0" + date_str
        elif len(date_str) != 8:
            # Invalid length
            return None

        month = int(date_str[:2])
        day = int(date_str[2:4])
        year = int(date_str[4:8])

        return datetime(year, month, day)
    except (ValueError, AttributeError):
        return None


def read_bulk_file_chunked(
    file_path: Path | str,
    header_file_path: Path | str,
    chunksize: int = 100_000,
    dtype: dict | None = None,
) -> Iterator[pd.DataFrame]:
    """
    Read FEC bulk data file in chunks.

    FEC bulk files are pipe-delimited (|) with no header row.
    Header information comes from separate header files.

    Args:
        file_path: Path to the bulk data file (.txt)
        header_file_path: Path to the header file (.csv)
        chunksize: Number of rows per chunk
        dtype: Optional dtype specifications for columns

    Yields:
        DataFrame chunks with proper column names

    Example:
        >>> for chunk in read_bulk_file_chunked("data/cm.txt", "data/cm_header.csv"):
        ...     process_chunk(chunk)
    """
    logger = get_logger()

    # Read header file to get column names
    header_df = pd.read_csv(header_file_path)
    column_names = header_df.columns.tolist()

    logger.info(
        f"Reading bulk file {file_path} with {len(column_names)} columns "
        f"in chunks of {chunksize:,} rows"
    )

    # Read data file in chunks
    # FEC uses pipe (|) as delimiter, no header in data file
    chunks = pd.read_csv(
        file_path,
        sep="|",
        names=column_names,
        header=None,
        chunksize=chunksize,
        dtype=dtype,
        na_values=["", " ", "NULL", "null"],
        keep_default_na=True,
        low_memory=False,  # Read entire file to infer types
        encoding="utf-8",
    )

    for i, chunk in enumerate(chunks, start=1):
        logger.debug(f"Processing chunk {i} ({len(chunk):,} rows)")
        yield chunk


def read_bulk_file(
    file_path: Path | str,
    header_file_path: Path | str,
    dtype: dict | None = None,
) -> pd.DataFrame:
    """
    Read entire FEC bulk data file into memory.

    Use read_bulk_file_chunked() for large files to avoid memory issues.

    Args:
        file_path: Path to the bulk data file (.txt)
        header_file_path: Path to the header file (.csv)
        dtype: Optional dtype specifications for columns

    Returns:
        Complete DataFrame

    Example:
        >>> df = read_bulk_file("data/cm.txt", "data/cm_header.csv")
    """
    logger = get_logger()

    # Read header file to get column names
    header_df = pd.read_csv(header_file_path)
    column_names = header_df.columns.tolist()

    logger.info(f"Reading entire bulk file {file_path}")

    # Read entire data file
    df = pd.read_csv(
        file_path,
        sep="|",
        names=column_names,
        header=None,
        dtype=dtype,
        na_values=["", " ", "NULL", "null"],
        keep_default_na=True,
        low_memory=False,
        encoding="utf-8",
    )

    logger.info(f"Loaded {len(df):,} rows, {len(df.columns)} columns")

    return df


def standardize_zip_code(zip_code: str | None) -> str | None:
    """
    Standardize ZIP code to 5 digits (remove ZIP+4 extension).

    Args:
        zip_code: ZIP code string (may be 5 or 9 digits)

    Returns:
        5-digit ZIP code or None if invalid

    Examples:
        >>> standardize_zip_code("21201")
        '21201'
        >>> standardize_zip_code("212014532")
        '21201'
        >>> standardize_zip_code("21201-4532")
        '21201'
        >>> standardize_zip_code(None)
        None
    """
    if not zip_code or pd.isna(zip_code):
        return None

    # Convert to string and remove any whitespace/dashes
    zip_str = str(zip_code).strip().replace("-", "")

    # Take first 5 digits
    if len(zip_str) >= 5:
        return zip_str[:5]

    return None


def clean_text_field(text: str | None) -> str | None:
    """
    Clean text field by trimming whitespace and converting empty to None.

    Args:
        text: Text string to clean

    Returns:
        Cleaned text or None if empty

    Examples:
        >>> clean_text_field("  Hello  ")
        'Hello'
        >>> clean_text_field("")
        None
        >>> clean_text_field(None)
        None
    """
    if not text or pd.isna(text):
        return None

    cleaned = str(text).strip()

    return cleaned if cleaned else None
