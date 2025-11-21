"""Bulk file extractor for FEC committees."""

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


class BulkFECCommitteeExtractor(BaseExtractor):
    """Extract committee data from FEC bulk files."""

    # Mapping from bulk file columns to API/Bronze table columns
    # Note: Bulk file has CONNECTED_ORG_NM and CAND_ID which are not in the
    # Bronze schema (API doesn't return them). We skip these columns.
    COLUMN_MAPPING = {
        "CMTE_ID": "committee_id",
        "CMTE_NM": "name",
        "TRES_NM": "treasurer_name",
        "CMTE_ST1": "street_1",
        "CMTE_ST2": "street_2",
        "CMTE_CITY": "city",
        "CMTE_ST": "state",
        "CMTE_ZIP": "zip",
        "CMTE_DSGN": "designation",
        "CMTE_TP": "committee_type",
        "CMTE_PTY_AFFILIATION": "party",
        "CMTE_FILING_FREQ": "filing_frequency",
        "ORG_TP": "organization_type",
        # Skipping CONNECTED_ORG_NM - not in Bronze schema
        # Skipping CAND_ID - Bronze has candidate_ids (array) from API, not CAND_ID (single)
    }

    def get_source_name(self) -> str:
        """Get source system name."""
        return "FEC_BULK"

    def extract(self, **kwargs) -> pd.DataFrame:
        """
        Extract committee data from bulk file.

        Args:
            file_path: Path to cm.txt bulk file
            header_file_path: Path to cm_header_file.csv
            **kwargs: Additional arguments

        Returns:
            DataFrame with committee data in Bronze table format
        """
        file_path = kwargs.get("file_path")
        header_file_path = kwargs.get("header_file_path")

        if not file_path or not header_file_path:
            raise ValueError("file_path and header_file_path are required")

        logger = get_logger()
        logger.info(f"Extracting committees from bulk file: {file_path}")

        # Read entire file (committees are small, ~17K records)
        df = read_bulk_file(file_path, header_file_path)

        # Rename columns to match API format
        df = df.rename(columns=self.COLUMN_MAPPING)

        # Clean and standardize data
        df = self._clean_data(df)

        logger.info(f"Extracted {len(df):,} committees from bulk file")

        return df

    def extract_chunked(
        self,
        file_path: Path | str,
        header_file_path: Path | str,
        chunksize: int = 10_000,
        **kwargs,
    ) -> Iterator[pd.DataFrame]:
        """
        Extract committee data in chunks.

        Args:
            file_path: Path to cm.txt bulk file
            header_file_path: Path to cm_header_file.csv
            chunksize: Rows per chunk
            **kwargs: Additional arguments

        Yields:
            DataFrame chunks with committee data
        """
        logger = get_logger()
        logger.info(f"Extracting committees from bulk file in chunks: {file_path}")

        for chunk in read_bulk_file_chunked(file_path, header_file_path, chunksize):
            # Rename columns to match API format
            chunk = chunk.rename(columns=self.COLUMN_MAPPING)

            # Clean and standardize data
            chunk = self._clean_data(chunk)

            yield chunk

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize committee data.

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
            "treasurer_name",
            "street_1",
            "street_2",
            "city",
            "state",
            "designation",
            "committee_type",
            "party",
            "filing_frequency",
            "organization_type",
            "connected_organization_name",
        ]

        for field in text_fields:
            if field in df.columns:
                df[field] = df[field].apply(clean_text_field)

        # Uppercase state codes
        if "state" in df.columns:
            df["state"] = df["state"].str.upper()

        # Convert pandas NaN to None for database compatibility
        df = df.where(pd.notna(df), None)

        return df
