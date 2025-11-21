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
    COLUMN_MAPPING = {
        "CAND_ID": "candidate_id",
        "CAND_NAME": "name",
        "CAND_PTY_AFFILIATION": "party",
        "CAND_ELECTION_YR": "election_year",
        "CAND_OFFICE_ST": "state",
        "CAND_OFFICE": "office",
        "CAND_OFFICE_DISTRICT": "district",
        "CAND_ICI": "incumbent_challenger_status",
        "CAND_STATUS": "candidate_status",
        "CAND_PCC": "principal_committee_id",
        "CAND_ST1": "street_1",
        "CAND_ST2": "street_2",
        "CAND_CITY": "city",
        "CAND_ST": "candidate_state",
        "CAND_ZIP": "zip",
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
            **kwargs: Additional arguments

        Returns:
            DataFrame with candidate data in Bronze table format
        """
        file_path = kwargs.get("file_path")
        header_file_path = kwargs.get("header_file_path")

        if not file_path or not header_file_path:
            raise ValueError("file_path and header_file_path are required")

        logger = get_logger()
        logger.info(f"Extracting candidates from bulk file: {file_path}")

        # Read entire file (candidates are small, ~6K records)
        df = read_bulk_file(file_path, header_file_path)

        # Rename columns to match API format
        df = df.rename(columns=self.COLUMN_MAPPING)

        # Clean and standardize data
        df = self._clean_data(df)

        logger.info(f"Extracted {len(df):,} candidates from bulk file")

        return df

    def extract_chunked(
        self,
        file_path: Path | str,
        header_file_path: Path | str,
        chunksize: int = 10_000,
        **kwargs,
    ) -> Iterator[pd.DataFrame]:
        """
        Extract candidate data in chunks.

        Args:
            file_path: Path to cn.txt bulk file
            header_file_path: Path to cn_header_file.csv
            chunksize: Rows per chunk
            **kwargs: Additional arguments

        Yields:
            DataFrame chunks with candidate data
        """
        logger = get_logger()
        logger.info(f"Extracting candidates from bulk file in chunks: {file_path}")

        for chunk in read_bulk_file_chunked(file_path, header_file_path, chunksize):
            # Rename columns to match API format
            chunk = chunk.rename(columns=self.COLUMN_MAPPING)

            # Clean and standardize data
            chunk = self._clean_data(chunk)

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
        if "zip" in df.columns:
            df["zip"] = df["zip"].apply(standardize_zip_code)

        # Clean text fields (trim whitespace, convert empty to None)
        text_fields = [
            "name",
            "party",
            "state",
            "office",
            "district",
            "incumbent_challenger_status",
            "candidate_status",
            "street_1",
            "street_2",
            "city",
            "candidate_state",
        ]

        for field in text_fields:
            if field in df.columns:
                df[field] = df[field].apply(clean_text_field)

        # Uppercase state codes
        if "state" in df.columns:
            df["state"] = df["state"].str.upper()
        if "candidate_state" in df.columns:
            df["candidate_state"] = df["candidate_state"].str.upper()

        # Convert election year to integer
        if "election_year" in df.columns:
            df["election_year"] = pd.to_numeric(df["election_year"], errors="coerce")

        # Convert pandas NaN to None for database compatibility
        df = df.where(pd.notna(df), None)

        return df
