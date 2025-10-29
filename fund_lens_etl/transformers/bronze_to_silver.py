"""Transformers for bronze to silver layer."""

from datetime import UTC, datetime

import pandas as pd
from prefect import get_run_logger

from fund_lens_etl.transformers.base import BaseTransformer


class BronzeToSilverFECTransformer(BaseTransformer):
    """Transform FEC bronze data to silver layer."""

    def get_source_layer(self) -> str:
        """Get source layer name."""
        return "bronze"

    def get_target_layer(self) -> str:
        """Get target layer name."""
        return "silver"

    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform bronze FEC Schedule A data to silver layer.

        Cleaning operations:
        - Standardize data types
        - Clean ZIP codes (5 digits only)
        - Handle missing values with defaults
        - Extract nested committee information
        - Add transformation metadata

        Args:
            df: Bronze layer DataFrame (raw API response)
            **kwargs: Additional parameters (unused)

        Returns:
            Cleaned DataFrame ready for silver layer
        """
        logger = get_run_logger()
        logger.info(f"Transforming {len(df)} bronze records to silver")

        if df.empty:
            logger.warning("Empty DataFrame received, returning empty silver DataFrame")
            return pd.DataFrame()

        # Create silver DataFrame with cleaned fields
        silver_df = pd.DataFrame()

        # Source reference
        silver_df["bronze_sub_id"] = df["sub_id"]

        # Transaction identifiers
        silver_df["transaction_id"] = df["transaction_id"]
        silver_df["file_number"] = pd.to_numeric(df["file_number"], errors="coerce")

        # Dates (convert to datetime)
        silver_df["contribution_date"] = pd.to_datetime(
            df["contribution_receipt_date"], errors="coerce"
        )

        # Amounts (ensure numeric)
        silver_df["contribution_amount"] = pd.to_numeric(
            df["contribution_receipt_amount"], errors="coerce"
        )
        silver_df["contributor_aggregate_ytd"] = pd.to_numeric(
            df["contributor_aggregate_ytd"], errors="coerce"
        )

        # Contributor information (standardized)
        silver_df["contributor_name"] = df["contributor_name"].fillna("UNKNOWN")
        silver_df["contributor_first_name"] = df["contributor_first_name"]
        silver_df["contributor_last_name"] = df["contributor_last_name"]
        silver_df["contributor_city"] = df["contributor_city"]
        silver_df["contributor_state"] = df["contributor_state"]

        # Clean ZIP codes - 5 digits only
        silver_df["contributor_zip"] = (
            df["contributor_zip"].astype(str).str[:5].replace("nan", None)
        )

        # Employer and occupation with defaults
        silver_df["contributor_employer"] = df["contributor_employer"].fillna("NOT PROVIDED")
        silver_df["contributor_occupation"] = df["contributor_occupation"].fillna("NOT PROVIDED")
        silver_df["entity_type"] = df["entity_type"].fillna("UNKNOWN")

        # Committee information
        silver_df["committee_id"] = df["committee_id"]

        # Extract committee name from nested 'committee' object
        silver_df["committee_name"] = df.apply(
            lambda row: (
                row["committee"].get("name") if isinstance(row.get("committee"), dict) else None
            ),
            axis=1,
        )

        # Extract committee type from nested object
        silver_df["committee_type"] = df.apply(
            lambda row: (
                row["committee"].get("committee_type")
                if isinstance(row.get("committee"), dict)
                else None
            ),
            axis=1,
        )

        silver_df["committee_designation"] = df["recipient_committee_designation"]

        # Transaction details
        silver_df["receipt_type"] = df["receipt_type"]
        silver_df["election_type"] = df["election_type"]
        silver_df["memo_text"] = df["memo_text"]

        # Metadata
        silver_df["election_cycle"] = pd.to_numeric(
            df["two_year_transaction_period"], errors="coerce"
        )
        silver_df["report_year"] = pd.to_numeric(df["report_year"], errors="coerce")

        # Add timestamps
        silver_df["created_at"] = datetime.now(UTC)
        silver_df["updated_at"] = datetime.now(UTC)

        # Data quality checks
        initial_count = len(silver_df)

        # Drop rows with missing critical fields
        silver_df = silver_df.dropna(
            subset=["bronze_sub_id", "contribution_date", "contribution_amount"]
        )

        dropped_count = initial_count - len(silver_df)
        if dropped_count > 0:
            logger.warning(
                f"Dropped {dropped_count} records due to missing critical fields "
                "(sub_id, date, or amount)"
            )

        # Log data quality metrics
        logger.info(f"Transformation complete: {len(silver_df)} records")
        logger.info(
            f"Date range: {silver_df['contribution_date'].min()} to {silver_df['contribution_date'].max()}"
        )
        logger.info(f"Total amount: ${silver_df['contribution_amount'].sum():,.2f}")
        logger.info(f"Unique contributors: {silver_df['contributor_name'].nunique()}")
        logger.info(f"Unique committees: {silver_df['committee_id'].nunique()}")

        return silver_df
