"""Maryland committee extractor."""

import logging
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd
from prefect import get_run_logger
from prefect.exceptions import MissingContextError

from fund_lens_etl.clients.maryland import MarylandCRISClient
from fund_lens_etl.extractors.base import BaseExtractor


def get_logger():
    """Get logger - Prefect if available, otherwise standard logging."""
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(__name__)


class MarylandCommitteeExtractor(BaseExtractor):
    """
    Extract committee data from Maryland MDCRIS.

    Downloads CSV from MDCRIS and converts to DataFrame with standardized column names.
    Committees have a unique CCF ID which serves as the natural key.
    """

    # Column mapping from CSV headers to model fields
    COLUMN_MAPPING = {
        "Committee Type": "committee_type",
        "CCF ID": "ccf_id",
        "Committee Name": "committee_name",
        "Committee Status": "committee_status",
        "Citation Violations": "citation_violations",
        "Election Type": "election_type",
        "Registered Date": "registered_date",
        "Amended Date": "amended_date",
        "Chairperson Name": "chairperson_name",
        "Chairperson Address": "chairperson_address",
        "Treasurer Name": "treasurer_name",
        "Treasurer Address": "treasurer_address",
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
        status: str | None = None,
        committee_type: str | None = None,
        output_path: Path | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Extract committee data from MDCRIS.

        Args:
            status: Committee status filter ('A' for Active, None for all)
            committee_type: Committee type filter
            output_path: Optional path to save raw CSV (uses temp file if not provided)

        Returns:
            DataFrame with committee records
        """
        logger = get_logger()
        logger.info(f"Extracting Maryland committees: status={status}, type={committee_type}")

        # Determine output path
        if output_path is None:
            temp_dir = Path(tempfile.mkdtemp())
            output_path = temp_dir / f"md_committees_{status or 'all'}.csv"

        # Download CSV
        csv_path = self.client.download_committees(
            output_path=output_path,
            status=status,
            committee_type=committee_type,
        )

        # Read CSV
        df = pd.read_csv(csv_path, dtype=str)
        logger.info(f"Read {len(df)} records from CSV")

        # Handle trailing comma in CSV (creates empty last column)
        if df.columns[-1] == "" or df.columns[-1].startswith("Unnamed"):
            df = df.iloc[:, :-1]

        # Rename columns to match model
        df = df.rename(columns=self.COLUMN_MAPPING)

        # Strip whitespace from string columns
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].str.strip()

        # Replace empty strings with None
        df = df.replace({"": None, "nan": None, "NaN": None})

        # Ensure ccf_id is present and not null
        if "ccf_id" not in df.columns:
            raise ValueError("CSV missing required 'CCF ID' column")

        # Drop any records with null ccf_id
        null_ccf_count = df["ccf_id"].isna().sum()
        if null_ccf_count > 0:
            logger.warning(f"Dropping {null_ccf_count} records with null CCF ID")
            df = df.dropna(subset=["ccf_id"])

        # Drop duplicates based on ccf_id (should not happen, but just in case)
        initial_count = len(df)
        df = df.drop_duplicates(subset=["ccf_id"], keep="first")
        if len(df) < initial_count:
            logger.warning(f"Removed {initial_count - len(df)} duplicate CCF IDs")

        logger.info(f"Extracted {len(df)} unique committee records")
        return df
