"""Bulk file extractor for FEC individual contributions (Schedule A)."""

import logging
from collections.abc import Iterator
from pathlib import Path

import pandas as pd
from prefect import get_run_logger
from prefect.exceptions import MissingContextError

from fund_lens_etl.extractors.base import BaseExtractor
from fund_lens_etl.utils.bulk_file_parser import (
    clean_text_field,
    parse_fec_date,
    read_bulk_file_chunked,
    standardize_zip_code,
)


def get_logger():
    """Get logger - Prefect if available, otherwise standard logging."""
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(__name__)


class BulkFECContributionExtractor(BaseExtractor):
    """Extract individual contribution data (Schedule A) from FEC bulk files."""

    # Mapping from bulk file columns to API/Bronze table columns
    COLUMN_MAPPING = {
        "CMTE_ID": "committee_id",
        "AMNDT_IND": "amendment_indicator",
        "RPT_TP": "report_type",
        "TRANSACTION_PGI": "election_type",
        "IMAGE_NUM": "image_number",
        "TRANSACTION_TP": "transaction_type",
        "ENTITY_TP": "entity_type",
        "NAME": "contributor_name",
        "CITY": "contributor_city",
        "STATE": "contributor_state",
        "ZIP_CODE": "contributor_zip",
        "EMPLOYER": "contributor_employer",
        "OCCUPATION": "contributor_occupation",
        "TRANSACTION_DT": "contribution_receipt_date",
        "TRANSACTION_AMT": "contribution_receipt_amount",
        "OTHER_ID": "other_id",
        "TRAN_ID": "transaction_id",
        "FILE_NUM": "file_number",
        "MEMO_CD": "memo_code",
        "MEMO_TEXT": "memo_text",
        "SUB_ID": "sub_id",
    }

    def get_source_name(self) -> str:
        """Get source system name."""
        return "FEC_BULK"

    def extract(self, **kwargs) -> pd.DataFrame:
        """
        Not recommended - use extract_chunked() for large contribution files.

        The individual contributions file (itcont.txt) has ~9M records.
        Loading all at once will use excessive memory.
        """
        raise NotImplementedError(
            "Use extract_chunked() for contribution files to avoid memory issues. "
            "The itcont.txt file has millions of records."
        )

    def extract_chunked(
        self,
        file_path: Path | str,
        header_file_path: Path | str,
        election_cycle: int,
        chunksize: int = 100_000,
        **kwargs,
    ) -> Iterator[pd.DataFrame]:
        """
        Extract contribution data in chunks.

        The contribution file is very large (~9.2M records, 1.6GB).
        This method processes it in manageable chunks.

        Args:
            file_path: Path to itcont.txt bulk file
            header_file_path: Path to indiv_header_file.csv
            election_cycle: Election cycle year (e.g., 2026) to populate two_year_transaction_period
            chunksize: Rows per chunk (default 100K for memory efficiency)
            **kwargs: Additional arguments

        Yields:
            DataFrame chunks with contribution data in Bronze table format

        Example:
            >>> extractor = BulkFECContributionExtractor()
            >>> for chunk in extractor.extract_chunked("data/itcont.txt", "data/indiv_header.csv", 2026):
            ...     loader.load(session, chunk)
        """
        logger = get_logger()
        logger.info(f"Extracting individual contributions from bulk file in chunks: {file_path}")

        chunk_num = 0
        total_rows = 0

        # Force date column to be read as string to prevent pandas from inferring as numeric
        # This prevents loss of leading zeros and ensures parse_fec_date receives valid strings
        dtype_spec = {
            "TRANSACTION_DT": str,
        }

        for chunk in read_bulk_file_chunked(
            file_path, header_file_path, chunksize, dtype=dtype_spec
        ):
            chunk_num += 1
            total_rows += len(chunk)

            # Rename columns to match API format
            chunk = chunk.rename(columns=self.COLUMN_MAPPING)

            # Clean and standardize data
            chunk = self._clean_data(chunk)

            # Add election cycle (bulk files don't include this field)
            chunk["two_year_transaction_period"] = election_cycle

            # Log progress every 10 chunks
            if chunk_num % 10 == 0:
                logger.info(f"Processed {chunk_num} chunks ({total_rows:,} rows) from bulk file")

            yield chunk

        logger.info(f"Completed extraction: {chunk_num} chunks, {total_rows:,} total rows")

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize contribution data.

        Args:
            df: Raw DataFrame from bulk file

        Returns:
            Cleaned DataFrame
        """
        df = df.copy()

        # Parse contribution date (MMDDYYYY format)
        if "contribution_receipt_date" in df.columns:
            df["contribution_receipt_date"] = df["contribution_receipt_date"].apply(parse_fec_date)
            # Convert NaT to None for PostgreSQL compatibility
            df["contribution_receipt_date"] = df["contribution_receipt_date"].where(
                df["contribution_receipt_date"].notna(), None
            )

        # Convert contribution amount to float
        if "contribution_receipt_amount" in df.columns:
            df["contribution_receipt_amount"] = pd.to_numeric(
                df["contribution_receipt_amount"], errors="coerce"
            )

        # Standardize ZIP codes (5 digits only)
        if "contributor_zip" in df.columns:
            df["contributor_zip"] = df["contributor_zip"].apply(standardize_zip_code)

        # Clean text fields (trim whitespace, convert empty to None)
        text_fields = [
            "contributor_name",
            "contributor_city",
            "contributor_state",
            "contributor_employer",
            "contributor_occupation",
            "amendment_indicator",
            "report_type",
            "election_type",
            "transaction_type",
            "entity_type",
            "memo_code",
            "memo_text",
        ]

        for field in text_fields:
            if field in df.columns:
                df[field] = df[field].apply(clean_text_field)

        # Uppercase state codes
        if "contributor_state" in df.columns:
            df["contributor_state"] = df["contributor_state"].str.upper()

        # Ensure sub_id is a string (it's the primary key)
        if "sub_id" in df.columns:
            df["sub_id"] = df["sub_id"].astype(str)

        # Convert pandas NaN to None for database compatibility
        df = df.where(pd.notna(df), None)

        return df
