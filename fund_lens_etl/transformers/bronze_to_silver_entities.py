"""Transformers for bronze to silver entity data (committees, candidates)."""

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
class BronzeToSilverCommitteeTransformer(BaseTransformer):
    """Transform FEC committee data from bronze to silver layer."""

    def get_source_layer(self) -> str:
        """Get source layer name."""
        return "bronze"

    def get_target_layer(self) -> str:
        """Get target layer name."""
        return "silver"

    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform bronze FEC committee data to silver layer.

        Cleaning operations:
        - Standardize column names
        - Handle missing values
        - Normalize text fields
        - Standardize ZIP codes (5 digits)
        - Remove duplicates

        Args:
            df: Bronze layer DataFrame
            **kwargs: Additional transformation parameters

        Returns:
            Cleaned DataFrame ready for silver layer
        """
        logger = get_logger()
        logger.info(f"Transforming {len(df)} bronze committee records to silver")

        if df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return pd.DataFrame()

        # Create a copy
        df = df.copy()

        # 1. Map columns bronze -> silver
        column_mapping = {
            "committee_id": "source_committee_id",
            "name": "name",
            "committee_type": "committee_type",
            "designation": "designation",
            "party": "party",
            "state": "state",
            "city": "city",
            "zip": "zip",
            "treasurer_name": "treasurer_name",
            "is_active": "is_active",
        }

        # Select and rename columns (including candidate_ids for processing)
        available_columns = [col for col in column_mapping if col in df.columns]
        if "candidate_ids" in df.columns:
            available_columns.append("candidate_ids")

        df = df[available_columns].rename(columns=column_mapping)

        # 2. Clean text fields
        text_columns = ["name", "city", "treasurer_name"]
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace(["nan", "None", ""], None)

        # 3. Extract primary candidate_id from candidate_ids array
        if "candidate_ids" in df.columns:
            df["candidate_id"] = df["candidate_ids"].apply(
                lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None
            )
            # Drop the candidate_ids column as we only need candidate_id
            df = df.drop(columns=["candidate_ids"])
        else:
            df["candidate_id"] = None

        # 4. Standardize state codes to uppercase
        if "state" in df.columns:
            df["state"] = df["state"].str.upper()

        # 5. Standardize party codes to uppercase
        if "party" in df.columns:
            df["party"] = df["party"].str.upper()

        # 6. Normalize ZIP codes to 5 digits
        if "zip" in df.columns:
            df["zip"] = (
                df["zip"]
                .astype(str)
                .str.replace(r"[^\d]", "", regex=True)
                .str[:5]
                .replace("", None)
            )

        # 7. Add election_cycle from kwargs or infer
        election_cycle = kwargs.get("election_cycle")
        if election_cycle:
            df["election_cycle"] = election_cycle
        elif "cycles" in df.columns:
            # Use the most recent cycle from the cycles array
            df["election_cycle"] = df["cycles"].apply(
                lambda x: max(x) if isinstance(x, list) and len(x) > 0 else None
            )
            # Drop cycles column after extracting election_cycle
            df = df.drop(columns=["cycles"])
        else:
            logger.warning("No election_cycle provided and can't infer from data")
            df["election_cycle"] = None

        # 8. Remove duplicates based on source_committee_id
        initial_count = len(df)
        df = df.drop_duplicates(subset=["source_committee_id"], keep="first")
        duplicates_removed = initial_count - len(df)

        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed} duplicate records")

        # 9. Add metadata
        df["loaded_at"] = pd.Timestamp.now()

        logger.info(f"Transformation complete: {len(df)} clean records")

        return df


# noinspection PyArgumentEqualDefault
class BronzeToSilverCandidateTransformer(BaseTransformer):
    """Transform FEC candidate data from bronze to silver layer."""

    def get_source_layer(self) -> str:
        """Get source layer name."""
        return "bronze"

    def get_target_layer(self) -> str:
        """Get target layer name."""
        return "silver"

    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform bronze FEC candidate data to silver layer.

        Cleaning operations:
        - Standardize column names
        - Handle missing values
        - Normalize text fields
        - Standardize state/district codes
        - Remove duplicates

        Args:
            df: Bronze layer DataFrame
            **kwargs: Additional transformation parameters

        Returns:
            Cleaned DataFrame ready for silver layer
        """
        logger = get_logger()
        logger.info(f"Transforming {len(df)} bronze candidate records to silver")

        if df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return pd.DataFrame()

        # Create a copy
        df = df.copy()

        # 1. Map columns bronze -> silver
        column_mapping = {
            "candidate_id": "source_candidate_id",
            "name": "name",
            "office": "office",
            "state": "state",
            "district": "district",
            "party": "party",
            "is_active": "is_active",
        }

        # Select and rename columns
        available_columns = [col for col in column_mapping if col in df.columns]
        df = df[available_columns].rename(columns=column_mapping)

        # 2. Clean text fields
        text_columns = ["name"]
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace(["nan", "None", ""], None)

        # 3. Standardize state codes to uppercase
        if "state" in df.columns:
            df["state"] = df["state"].str.upper()

        # 4. Standardize office codes to uppercase
        if "office" in df.columns:
            df["office"] = df["office"].str.upper()

        # 5. Standardize party codes to uppercase
        if "party" in df.columns:
            df["party"] = df["party"].str.upper()

        # 6. Add election_cycle from kwargs or infer
        election_cycle = kwargs.get("election_cycle")
        if election_cycle:
            df["election_cycle"] = election_cycle
        elif "cycles" in df.columns:
            # Use the most recent cycle from the cycles array
            df["election_cycle"] = df["cycles"].apply(
                lambda x: max(x) if isinstance(x, list) and len(x) > 0 else None
            )
        else:
            logger.warning("No election_cycle provided and can't infer from data")
            df["election_cycle"] = None

        # 7. Remove duplicates based on source_candidate_id
        initial_count = len(df)
        df = df.drop_duplicates(subset=["source_candidate_id"], keep="first")
        duplicates_removed = initial_count - len(df)

        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed} duplicate records")

        # 8. Add metadata
        df["loaded_at"] = pd.Timestamp.now()

        logger.info(f"Transformation complete: {len(df)} clean records")

        return df
