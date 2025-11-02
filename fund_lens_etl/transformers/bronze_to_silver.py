"""Transformer for bronze to silver layer."""

import logging

import pandas as pd
from fund_lens_models.bronze.fec import (
    BronzeFECCandidate,
    BronzeFECCommittee,
)
from prefect import get_run_logger
from prefect.exceptions import MissingContextError
from sqlalchemy import select
from sqlalchemy.orm import Session

from fund_lens_etl.transformers.base import BaseTransformer


def get_logger():
    """Get logger - Prefect if available, otherwise standard logging."""
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(__name__)


class BronzeToSilverFECTransformer(BaseTransformer):
    """Transform FEC Schedule A data from bronze to silver layer."""

    def __init__(self, session: Session | None = None):
        """
        Initialize transformer.

        Args:
            session: SQLAlchemy session for database queries (required for JOINs)
        """
        self.session = session

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
        - Enrich with committee data via JOIN
        - Enrich with candidate data via JOIN
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

        # 2. Enrich with committee data via JOIN
        if self.session is not None and "committee_id" in df.columns:
            df = self._enrich_with_committee_data(df)
        else:
            logger.warning(
                "No session provided or committee_id missing - skipping committee enrichment"
            )
            df["committee_name"] = None
            df["committee_type"] = None
            df["committee_party"] = None

        # 3. Enrich with candidate data via JOIN
        if self.session is not None and "committee_id" in df.columns:
            df = self._enrich_with_candidate_data(df)
        else:
            logger.warning(
                "No session provided or committee_id missing - skipping candidate enrichment"
            )
            df["candidate_id"] = None
            df["candidate_name"] = None
            df["candidate_office"] = None
            df["candidate_party"] = None

        # 4. Clean text fields - trim whitespace
        text_columns = [
            "contributor_name",
            "contributor_first_name",
            "contributor_last_name",
            "contributor_city",
            "committee_name",
        ]

        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                # Replace 'nan' string with actual None
                df[col] = df[col].replace(["nan", "None", ""], None)

        # Handle employer/occupation separately - use "NOT PROVIDED" instead of None
        # to match model defaults (required for PACs which don't have this data)
        for col in ["contributor_employer", "contributor_occupation"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace(["nan", "None", ""], "NOT PROVIDED")

        # 5. Standardize state codes to uppercase
        if "contributor_state" in df.columns:
            df["contributor_state"] = df["contributor_state"].str.upper()

        # 6. Normalize ZIP codes to 5 digits
        if "contributor_zip" in df.columns:
            df["contributor_zip"] = (
                df["contributor_zip"]
                .astype(str)
                .str.replace(r"[^\d]", "", regex=True)  # Remove non-digits
                .str[:5]  # Take first 5 digits
                .replace("", None)  # Empty strings to None
            )

        # 7. Ensure numeric fields are proper types
        numeric_columns = ["contribution_amount", "contributor_aggregate_ytd"]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # 8. Ensure date fields are datetime
        if "contribution_date" in df.columns:
            df["contribution_date"] = pd.to_datetime(df["contribution_date"], errors="coerce")

        # 9. Remove exact duplicates based on sub_id
        initial_count = len(df)
        # noinspection PyArgumentEqualDefault
        df = df.drop_duplicates(subset=["sub_id"], keep="first")
        duplicates_removed = initial_count - len(df)

        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed} duplicate records")

        # 10. Add metadata
        df["loaded_at"] = pd.Timestamp.now()

        logger.info(f"Transformation complete: {len(df)} clean records")

        return df

    def _enrich_with_committee_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich DataFrame with committee data via JOIN.

        Args:
            df: DataFrame with committee_id column

        Returns:
            DataFrame with committee_name, committee_type, committee_party added
        """
        logger = get_logger()

        if self.session is None:
            raise ValueError("Session is required for enrichment")

        logger.info("Enriching with committee data from bronze_fec_committee")

        # Get unique committee IDs from the DataFrame
        unique_committee_ids = df["committee_id"].dropna().unique().tolist()

        if not unique_committee_ids:
            logger.warning("No committee IDs found in DataFrame")
            df["committee_name"] = None
            df["committee_type"] = None
            df["committee_party"] = None
            return df

        # Query committee data
        stmt = select(
            BronzeFECCommittee.committee_id,
            BronzeFECCommittee.name,
            BronzeFECCommittee.committee_type,
            BronzeFECCommittee.party,
        ).where(BronzeFECCommittee.committee_id.in_(unique_committee_ids))

        result = self.session.execute(stmt)
        committee_data = pd.DataFrame(
            result.fetchall(),
            columns=["committee_id", "committee_name", "committee_type", "committee_party"],
        )

        logger.info(f"Found {len(committee_data)} committees in bronze layer")

        # LEFT JOIN with contribution data
        df = df.merge(
            committee_data,
            on="committee_id",
            how="left",
        )

        # Log any missing committees
        missing_committees = df["committee_name"].isna().sum()
        if missing_committees > 0:
            logger.warning(
                f"{missing_committees} contributions have committee_ids not found in bronze_fec_committee"
            )

        return df

    def _enrich_with_candidate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich DataFrame with candidate data via JOIN through committee.

        Args:
            df: DataFrame with committee_id column

        Returns:
            DataFrame with candidate_id, candidate_name, candidate_office, candidate_party added
        """
        logger = get_logger()

        if self.session is None:
            raise ValueError("Session is required for enrichment")

        logger.info("Enriching with candidate data from bronze_fec_candidate")

        # Get unique committee IDs
        unique_committee_ids = df["committee_id"].dropna().unique().tolist()

        if not unique_committee_ids:
            logger.warning("No committee IDs found in DataFrame")
            df["candidate_id"] = None
            df["candidate_name"] = None
            df["candidate_office"] = None
            df["candidate_party"] = None
            return df

        # First get committee → candidate mapping
        stmt = select(
            BronzeFECCommittee.committee_id,
            BronzeFECCommittee.candidate_ids,
        ).where(BronzeFECCommittee.committee_id.in_(unique_committee_ids))

        result = self.session.execute(stmt)
        committee_candidate_map = {}

        for row in result:
            committee_id = row[0]
            candidate_ids = row[1]  # This is a JSON array
            # Take the first candidate_id if multiple exist
            if candidate_ids and len(candidate_ids) > 0:
                committee_candidate_map[committee_id] = candidate_ids[0]

        # Map committee_id → candidate_id
        df["candidate_id"] = df["committee_id"].map(committee_candidate_map)

        # Now get candidate details
        unique_candidate_ids = df["candidate_id"].dropna().unique().tolist()

        if unique_candidate_ids:
            candidate_stmt = select(  # Changed variable name
                BronzeFECCandidate.candidate_id,
                BronzeFECCandidate.name,
                BronzeFECCandidate.office,
                BronzeFECCandidate.party,
            ).where(BronzeFECCandidate.candidate_id.in_(unique_candidate_ids))

            result = self.session.execute(candidate_stmt)
            candidate_data = pd.DataFrame(
                result.fetchall(),
                columns=["candidate_id", "candidate_name", "candidate_office", "candidate_party"],
            )

            logger.info(f"Found {len(candidate_data)} candidates in bronze layer")

            # LEFT JOIN with contribution data
            df = df.merge(
                candidate_data,
                on="candidate_id",
                how="left",
            )
        else:
            logger.info("No candidate IDs found in committees")
            df["candidate_name"] = None
            df["candidate_office"] = None
            df["candidate_party"] = None

        return df
