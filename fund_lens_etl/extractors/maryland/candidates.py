"""Maryland candidate extractor."""

import hashlib
import logging
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd
from prefect import get_run_logger
from prefect.exceptions import MissingContextError

from fund_lens_etl.clients.maryland import MarylandSBEClient
from fund_lens_etl.extractors.base import BaseExtractor


def get_logger():
    """Get logger - Prefect if available, otherwise standard logging."""
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(__name__)


def _to_str(value: Any) -> str:
    """Convert value to string, handling None and NaN."""
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value)


def generate_candidate_hash(
    office_name: Any,
    district: Any,
    last_name: Any,
    first_name: Any,
    party: Any,
    election_year: Any,
    election_type: Any,
) -> str:
    """
    Generate SHA-256 hash for candidate deduplication.

    Args:
        office_name: Office being sought
        district: District if applicable
        last_name: Candidate last name
        first_name: Candidate first name
        party: Political party
        election_year: Election year
        election_type: Primary, General, or Special

    Returns:
        64-character hex hash string
    """
    content = "|".join(
        [
            _to_str(office_name),
            _to_str(district),
            _to_str(last_name),
            _to_str(first_name),
            _to_str(party),
            _to_str(election_year),
            _to_str(election_type),
        ]
    )
    return hashlib.sha256(content.encode()).hexdigest()


class MarylandCandidateExtractor(BaseExtractor):
    """
    Extract candidate data from Maryland State Board of Elections.

    Downloads CSV files from elections.maryland.gov and converts to DataFrame.
    Generates content hash for each record to enable deduplication.
    """

    # Column mapping from CSV headers to model fields
    COLUMN_MAPPING = {
        "Office Name": "office_name",
        "Contest Run By District Name and Number": "district",
        "Candidate Ballot Last Name and Suffix": "candidate_last_name",
        "Candidate First Name and Middle Name": "candidate_first_name",
        "Additional Information": "additional_info",
        "Office Political Party": "party",
        "Candidate Residential Jurisdiction": "jurisdiction",
        "Candidate Gender": "gender",
        "Candidate Status": "status",
        "Filing Type and Date": "filing_type_and_date",
        "Campaign Mailing Address": "campaign_address",
        "Campaign Mailing City State and Zip": "campaign_city_state_zip",
        "Public Phone": "phone",
        "Email": "email",
        "Website": "website",
        "Facebook": "facebook",
        "X": "twitter",
        "Other": "other_social",
        "Committee Name": "committee_name",
    }

    def __init__(self, client: MarylandSBEClient | None = None):
        """
        Initialize extractor.

        Args:
            client: Optional pre-configured SBE client
        """
        self.client = client or MarylandSBEClient()

    def get_source_name(self) -> str:
        """Get source system identifier."""
        return "MARYLAND_SBE"

    def extract(
        self,
        year: int | None = None,
        output_dir: Path | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Extract candidate data for a given election year.

        Downloads all available candidate files (Primary, General, Special)
        and combines them into a single DataFrame.

        Args:
            year: Election year (required, defaults to current year if not provided)
            output_dir: Optional directory to save raw CSVs (uses temp dir if not provided)
            **kwargs: Additional keyword arguments (unused)

        Returns:
            DataFrame with candidate records
        """
        from datetime import datetime

        if year is None:
            year = datetime.now().year

        logger = get_logger()
        logger.info(f"Extracting Maryland candidates for {year}")

        # Determine output directory
        if output_dir is None:
            output_dir = Path(tempfile.mkdtemp())

        # Download all available candidate files
        csv_paths = self.client.download_candidates(
            year=year,
            output_dir=output_dir,
        )

        if not csv_paths:
            logger.warning(f"No candidate files found for {year}")
            return pd.DataFrame()

        # Read and combine all CSVs
        all_dfs = []
        for csv_path in csv_paths:
            # Determine election type from filename
            filename = csv_path.name
            if "Special" in filename:
                election_type = "Special"
            elif "General" in filename:
                election_type = "General"
            else:
                election_type = "Primary"

            try:
                df = pd.read_csv(csv_path, dtype=str)
                logger.info(f"Read {len(df)} records from {filename}")

                # Add election metadata
                df["election_year"] = year
                df["election_type"] = election_type

                all_dfs.append(df)
            except Exception as e:
                logger.warning(f"Failed to read {csv_path}: {e}")
                continue

        if not all_dfs:
            logger.warning(f"No valid candidate data found for {year}")
            return pd.DataFrame()

        # Combine all DataFrames
        df = pd.concat(all_dfs, ignore_index=True)
        logger.info(f"Combined {len(df)} total candidate records")

        # Handle trailing comma in CSV (creates empty last column)
        empty_cols = [col for col in df.columns if col == "" or col.startswith("Unnamed")]
        if empty_cols:
            df = df.drop(columns=empty_cols)

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

        # Ensure required columns exist
        required_cols = ["office_name", "candidate_last_name", "candidate_first_name", "status"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"CSV missing required columns: {missing_cols}")

        # Generate content hash for deduplication
        df["content_hash"] = df.apply(
            lambda row: generate_candidate_hash(
                office_name=row.get("office_name", ""),
                district=row.get("district", ""),
                last_name=row.get("candidate_last_name", ""),
                first_name=row.get("candidate_first_name", ""),
                party=row.get("party", ""),
                election_year=row.get("election_year", 0),
                election_type=row.get("election_type", ""),
            ),
            axis=1,
        )

        # Drop duplicates based on content hash
        initial_count = len(df)
        df = df.drop_duplicates(subset=["content_hash"], keep="first")
        if len(df) < initial_count:
            logger.info(f"Removed {initial_count - len(df)} duplicate records")

        logger.info(f"Extracted {len(df)} unique candidate records for {year}")
        return df

    def extract_multiple_years(
        self,
        years: list[int],
        output_dir: Path | None = None,
    ) -> pd.DataFrame:
        """
        Extract candidate data for multiple election years.

        Args:
            years: List of election years
            output_dir: Optional directory to save raw CSVs

        Returns:
            DataFrame with candidate records from all years
        """
        logger = get_logger()
        logger.info(f"Extracting Maryland candidates for years: {years}")

        all_dfs = []
        for year in years:
            try:
                df = self.extract(year=year, output_dir=output_dir)
                if not df.empty:
                    all_dfs.append(df)
            except Exception as e:
                logger.warning(f"Failed to extract candidates for {year}: {e}")
                continue

        if not all_dfs:
            return pd.DataFrame()

        combined_df = pd.concat(all_dfs, ignore_index=True)

        # Deduplicate across years (same candidate may appear in multiple files)
        initial_count = len(combined_df)
        combined_df = combined_df.drop_duplicates(subset=["content_hash"], keep="first")
        if len(combined_df) < initial_count:
            logger.info(f"Removed {initial_count - len(combined_df)} cross-year duplicates")

        logger.info(
            f"Extracted {len(combined_df)} total unique candidates across {len(years)} years"
        )
        return combined_df
