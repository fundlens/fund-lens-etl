"""Maryland contribution extractor."""

import logging
import tempfile
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
from prefect import get_run_logger
from prefect.exceptions import MissingContextError

from fund_lens_etl.clients.maryland import MarylandCRISClient, generate_contribution_hash
from fund_lens_etl.extractors.base import BaseExtractor


def get_logger():
    """Get logger - Prefect if available, otherwise standard logging."""
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(__name__)


class MarylandContributionExtractor(BaseExtractor):
    """
    Extract contribution data from Maryland MDCRIS.

    Downloads CSV from MDCRIS and converts to DataFrame with standardized column names.
    Generates content hash for each record to enable deduplication.
    """

    # Column mapping from CSV headers to model fields
    COLUMN_MAPPING = {
        "Receiving Committee": "receiving_committee",
        "Filing Period": "filing_period",
        "Contribution Date": "contribution_date",
        "Contributor Name": "contributor_name",
        "Contributor Address": "contributor_address",
        "Contributor Type": "contributor_type",
        "Contribution Type": "contribution_type",
        "Contribution Amount": "contribution_amount",
        "Employer Name": "employer_name",
        "Employer Occupation": "employer_occupation",
        "Office": "office",
        "Fundtype": "fund_type",
    }

    def __init__(self, client: MarylandCRISClient | None = None):
        """
        Initialize extractor.

        Args:
            client: Optional pre-configured MDCRIS client
        """
        self.client = client or MarylandCRISClient()

    def get_source_name(self) -> str:
        """Get source system identifier."""
        return "MARYLAND"

    def extract(
        self,
        filing_year: int | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        output_path: Path | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Extract contribution data from MDCRIS.

        Args:
            filing_year: Optional filing year filter
            start_date: Optional contribution start date filter
            end_date: Optional contribution end date filter
            output_path: Optional path to save raw CSV (uses temp file if not provided)

        Returns:
            DataFrame with contribution records
        """
        logger = get_logger()
        logger.info(
            f"Extracting Maryland contributions: "
            f"filing_year={filing_year}, start_date={start_date}, end_date={end_date}"
        )

        # Determine output path
        if output_path is None:
            temp_dir = Path(tempfile.mkdtemp())
            output_path = temp_dir / f"md_contributions_{filing_year or 'all'}.csv"

        # Download CSV
        csv_path = self.client.download_contributions(
            output_path=output_path,
            filing_year=filing_year,
            start_date=start_date,
            end_date=end_date,
        )

        # Read CSV
        df = pd.read_csv(csv_path, dtype=str)
        logger.info(f"Read {len(df)} records from CSV")

        # Handle trailing comma in CSV (creates empty last column)
        if df.columns[-1] == "" or df.columns[-1].startswith("Unnamed"):
            df = df.iloc[:, :-1]

        # Rename columns to match model
        df = df.rename(columns=self.COLUMN_MAPPING)

        # Strip whitespace and normalize null values
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].str.strip()
                # Replace various null representations with actual None
                df[col] = df[col].apply(
                    lambda x: None
                    if x in ("", "nan", "NaN", "None", "NULL", "null") or pd.isna(x)
                    else x
                )

        # Generate content hash for deduplication
        df["content_hash"] = df.apply(
            lambda row: generate_contribution_hash(
                receiving_committee=row.get("receiving_committee", ""),
                contribution_date=row.get("contribution_date", ""),
                contributor_name=row.get("contributor_name", ""),
                contributor_address=row.get("contributor_address", ""),
                contribution_amount=row.get("contribution_amount", ""),
                contribution_type=row.get("contribution_type", ""),
            ),
            axis=1,
        )

        # Drop duplicates based on content hash
        initial_count = len(df)
        df = df.drop_duplicates(subset=["content_hash"], keep="first")
        if len(df) < initial_count:
            logger.info(f"Removed {initial_count - len(df)} duplicate records")

        logger.info(f"Extracted {len(df)} unique contribution records")
        return df
