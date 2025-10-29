"""Transformer for bronze to silver layer."""

import logging

import pandas as pd
from prefect import get_run_logger
from prefect.exceptions import MissingContextError

from fund_lens_etl.transformers.base import BaseTransformer


def get_logger():
    """Get logger - Prefect if available, otherwise standard logging."""
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(__name__)


# noinspection PyArgumentEqualDefault
class BronzeToSilverFECTransformer(BaseTransformer):
    """Transform FEC Schedule A data from bronze to silver layer."""

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
        - Standardize column names
        - Handle missing values
        - Normalize text fields (trim, uppercase states)
        - Standardize ZIP codes (5 digits)
        - Convert dates to proper datetime
        - Validate and clean numeric fields
        - Remove duplicate records

        Args:
            df: Bronze layer DataFrame
            **kwargs: Additional transformation parameters

        Returns:
            Cleaned DataFrame ready for silver layer
        """
        logger = get_logger()
        logger.info(f"Transforming {len(df)} bronze records to silver")

        if df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return pd.DataFrame()

        # Create a copy to avoid modifying original
        df = df.copy()

        # 1. Standardize column names (bronze -> silver mapping)
        column_mapping = {
            "sub_id": "sub_id",
            "transaction_id": "transaction_id",
            "file_number": "file_number",
            "contribution_receipt_date": "contribution_date",
            "contribution_receipt_amount": "contribution_amount",
            "contributor_aggregate_ytd": "contributor_aggregate_ytd",
            "contributor_name": "contributor_name",
            "contributor_first_name": "contributor_first_name",
            "contributor_last_name": "contributor_last_name",
            "contributor_city": "contributor_city",
            "contributor_state": "contributor_state",
            "contributor_zip": "contributor_zip",
            "contributor_employer": "contributor_employer",
            "contributor_occupation": "contributor_occupation",
            "entity_type": "entity_type",
            "committee_id": "committee_id",
            "recipient_committee_designation": "committee_designation",
            "receipt_type": "receipt_type",
            "election_type": "election_type",
            "memo_text": "memo_text",
            "two_year_transaction_period": "election_cycle",
            "report_year": "report_year",
        }

        df = df.rename(columns=column_mapping)

        # 2. Extract committee info from raw_json
        if "raw_json" in df.columns:

            def extract_committee_info(row):
                """Extract committee details from raw_json."""
                if pd.isna(row.get("raw_json")):
                    return pd.Series({"committee_name": None, "committee_type": None})
                # noinspection PyBroadException
                try:
                    raw_data = row["raw_json"]
                    if isinstance(raw_data, str):
                        import json

                        raw_data = json.loads(raw_data)

                    # Extract from committee object
                    if isinstance(raw_data, dict) and "committee" in raw_data:
                        committee = raw_data["committee"]
                        if isinstance(committee, dict):
                            return pd.Series(
                                {
                                    "committee_name": committee.get("name"),
                                    "committee_type": committee.get("committee_type"),
                                }
                            )
                    return pd.Series({"committee_name": None, "committee_type": None})
                except Exception:
                    return pd.Series({"committee_name": None, "committee_type": None})

            committee_info = df.apply(extract_committee_info, axis=1)
            df["committee_name"] = committee_info["committee_name"]
            df["committee_type"] = committee_info["committee_type"]
        else:
            df["committee_name"] = None
            df["committee_type"] = None

        # 3. Clean text fields - trim whitespace
        text_columns = [
            "contributor_name",
            "contributor_first_name",
            "contributor_last_name",
            "contributor_city",
            "contributor_employer",
            "contributor_occupation",
        ]

        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                # Replace 'nan' string with actual None
                df[col] = df[col].replace(["nan", "None", ""], None)

        # 4. Standardize state codes to uppercase
        if "contributor_state" in df.columns:
            df["contributor_state"] = df["contributor_state"].str.upper()

        # 5. Normalize ZIP codes to 5 digits
        if "contributor_zip" in df.columns:
            df["contributor_zip"] = (
                df["contributor_zip"]
                .astype(str)
                .str.replace(r"[^\d]", "", regex=True)  # Remove non-digits
                .str[:5]  # Take first 5 digits
                .replace("", None)  # Empty strings to None
            )

        # 6. Ensure numeric fields are proper types
        numeric_columns = ["contribution_amount", "contributor_aggregate_ytd"]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # 7. Ensure date fields are datetime
        if "contribution_date" in df.columns:
            df["contribution_date"] = pd.to_datetime(df["contribution_date"], errors="coerce")

        # 8. Remove exact duplicates based on sub_id
        initial_count = len(df)
        df = df.drop_duplicates(subset=["sub_id"], keep="first")
        duplicates_removed = initial_count - len(df)

        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed} duplicate records")

        # 9. Add metadata
        df["loaded_at"] = pd.Timestamp.now()

        logger.info(f"Transformation complete: {len(df)} clean records")

        return df
