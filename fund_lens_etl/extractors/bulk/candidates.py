"""Bulk file extractor for FEC candidates."""

import logging
from collections.abc import Iterator
from pathlib import Path

import pandas as pd
from prefect import get_run_logger
from prefect.exceptions import MissingContextError

from fund_lens_etl.extractors.base import BaseExtractor
from fund_lens_etl.utils.bulk_file_parser import (
    clean_text_field,
    read_bulk_file,
    read_bulk_file_chunked,
    standardize_zip_code,
)


def get_logger():
    """Get logger - Prefect if available, otherwise standard logging."""
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(__name__)


class BulkFECCandidateExtractor(BaseExtractor):
    """Extract candidate data from FEC bulk files."""

    # Mapping from bulk file columns to API/Bronze table columns
    # Note: Matching API schema - address fields have "address_" prefix
    COLUMN_MAPPING = {
        "CAND_ID": "candidate_id",
        "CAND_NAME": "name",
        "CAND_PTY_AFFILIATION": "party",
        # Skip CAND_ELECTION_YR - API has election_years (array), not election_year (single)
        "CAND_OFFICE_ST": "state",
        "CAND_OFFICE": "office",
        "CAND_OFFICE_DISTRICT": "district",
        "CAND_ICI": "incumbent_challenge",  # API uses "incumbent_challenge" not "incumbent_challenger_status"
        "CAND_STATUS": "candidate_status",
        # Skip CAND_PCC (principal_committee_id) - not in Bronze schema from API
        "CAND_ST1": "address_street_1",  # API prefixes with "address_"
        "CAND_ST2": "address_street_2",
        "CAND_CITY": "address_city",
        "CAND_ST": "address_state",
        "CAND_ZIP": "address_zip",
    }

    def get_source_name(self) -> str:
        """Get source system name."""
        return "FEC_BULK"

    def extract(self, **kwargs) -> pd.DataFrame:
        """
        Extract candidate data from bulk file.

        Args:
            file_path: Path to cn.txt bulk file
            header_file_path: Path to cn_header_file.csv
            election_cycle: Election cycle year (e.g., 2026)
            **kwargs: Additional arguments

        Returns:
            DataFrame with candidate data in Bronze table format
        """
        file_path = kwargs.get("file_path")
        header_file_path = kwargs.get("header_file_path")
        election_cycle = kwargs.get("election_cycle")

        if not file_path or not header_file_path:
            raise ValueError("file_path and header_file_path are required")
        if not election_cycle:
            raise ValueError("election_cycle is required")

        logger = get_logger()
        logger.info(f"Extracting candidates from bulk file: {file_path}")

        # Read entire file (candidates are small, ~6K records)
        df = read_bulk_file(file_path, header_file_path)

        # Keep only columns we're mapping
        columns_to_keep = list(self.COLUMN_MAPPING.keys())
        df = df[columns_to_keep]

        # Rename columns to match API format
        df = df.rename(columns=self.COLUMN_MAPPING)

        # Clean and standardize data
        df = self._clean_data(df)

        # Add election cycle (bulk files don't include this field)
        df["cycles"] = [[election_cycle]] * len(df)  # JSON array with single cycle

        logger.info(f"Extracted {len(df):,} candidates from bulk file")

        return df

    def extract_chunked(
        self,
        file_path: Path | str,
        header_file_path: Path | str,
        election_cycle: int,
        chunksize: int = 10_000,
        **kwargs,
    ) -> Iterator[pd.DataFrame]:
        """
        Extract candidate data in chunks.

        Args:
            file_path: Path to cn.txt bulk file
            header_file_path: Path to cn_header_file.csv
            election_cycle: Election cycle year (e.g., 2026)
            chunksize: Rows per chunk
            **kwargs: Additional arguments

        Yields:
            DataFrame chunks with candidate data
        """
        logger = get_logger()
        logger.info(f"Extracting candidates from bulk file in chunks: {file_path}")

        for chunk in read_bulk_file_chunked(file_path, header_file_path, chunksize):
            # Keep only columns we're mapping
            columns_to_keep = list(self.COLUMN_MAPPING.keys())
            chunk = chunk[columns_to_keep]

            # Rename columns to match API format
            chunk = chunk.rename(columns=self.COLUMN_MAPPING)

            # Clean and standardize data
            chunk = self._clean_data(chunk)

            # Add election cycle (bulk files don't include this field)
            chunk["cycles"] = [[election_cycle]] * len(chunk)  # JSON array with single cycle

            yield chunk

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize candidate data.

        Args:
            df: Raw DataFrame from bulk file

        Returns:
            Cleaned DataFrame
        """
        df = df.copy()

        # Standardize ZIP codes (5 digits only)
        if "address_zip" in df.columns:
            df["address_zip"] = df["address_zip"].apply(standardize_zip_code)

        # Clean text fields (trim whitespace, convert empty to None)
        text_fields = [
            "name",
            "party",
            "state",
            "office",
            "district",
            "incumbent_challenge",
            "candidate_status",
            "address_street_1",
            "address_street_2",
            "address_city",
            "address_state",
        ]

        for field in text_fields:
            if field in df.columns:
                df[field] = df[field].apply(clean_text_field)

        # Uppercase state codes
        if "state" in df.columns:
            df["state"] = df["state"].str.upper()
        if "address_state" in df.columns:
            df["address_state"] = df["address_state"].str.upper()

        # Format district as 2-character string (e.g., "01", "02")
        # pandas may read as float (1.0) so convert properly
        if "district" in df.columns:
            df["district"] = df["district"].apply(
                lambda x: f"{int(float(x)):02d}" if pd.notna(x) and x != "" else None
            )

        # Convert pandas NaN to None for database compatibility
        df = df.where(pd.notna(df), None)

        return df
