"""Transformer for Maryland bronze to silver layer."""

import logging
import re
from datetime import datetime

import pandas as pd
from fund_lens_models.bronze.maryland import BronzeMarylandCommittee
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


def parse_maryland_date(date_str: str | None) -> datetime | None:
    """
    Parse Maryland date string to datetime.

    Maryland dates come in format MM/DD/YYYY.

    Args:
        date_str: Date string from source

    Returns:
        Parsed datetime or None if invalid
    """
    if not date_str or pd.isna(date_str):
        return None

    try:
        return datetime.strptime(date_str.strip(), "%m/%d/%Y")
    except (ValueError, AttributeError):
        return None


def parse_maryland_address(address: str | None) -> dict[str, str | None]:
    """
    Parse Maryland address string into components.

    Maryland addresses come as a single string, typically:
    "123 Main St  Baltimore MD 21201" or similar formats.

    Args:
        address: Raw address string

    Returns:
        Dict with city, state, zip (all may be None)
    """
    result: dict[str, str | None] = {"city": None, "state": None, "zip": None}

    if not address or pd.isna(address):
        return result

    address = str(address).strip()

    # Try to extract ZIP code (5 digits, optionally with -4 extension)
    zip_match = re.search(r"\b(\d{5})(?:-\d{4})?\s*$", address)
    if zip_match:
        result["zip"] = zip_match.group(1)

    # Try to extract state (2-letter code before ZIP)
    state_match = re.search(r"\b([A-Z]{2})\s+\d{5}", address)
    if state_match:
        result["state"] = state_match.group(1)

    # Try to extract city (word(s) before state)
    # This is a best-effort extraction
    city_match = re.search(r"\b([A-Za-z\s]+?)\s+[A-Z]{2}\s+\d{5}", address)
    if city_match:
        city = city_match.group(1).strip()
        # Clean up - remove common street suffixes that might be captured
        city = re.sub(
            r"\b(St|Ave|Rd|Dr|Ln|Ct|Blvd|Way|Pl|Cir|Ter|Pkwy|Hwy)\b\.?\s*$",
            "",
            city,
            flags=re.IGNORECASE,
        ).strip()
        if city:
            result["city"] = city.title()

    return result


class BronzeToSilverMarylandContributionTransformer(BaseTransformer):
    """Transform Maryland contribution data from bronze to silver layer."""

    def __init__(self, session: Session | None = None):
        """
        Initialize transformer.

        Args:
            session: SQLAlchemy session for database queries (required for committee JOINs)
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
        Transform bronze Maryland contribution data to silver layer.

        Cleaning operations:
        - Parse dates from string format
        - Convert amounts to numeric
        - Parse addresses to extract city/state/zip
        - Enrich with committee data (CCF ID, type)
        - Clean text fields
        - Remove invalid records

        Args:
            df: Bronze layer DataFrame
            **kwargs: Additional transformation parameters

        Returns:
            Cleaned DataFrame ready for silver layer
        """
        logger = get_logger()
        logger.info(f"Transforming {len(df)} bronze MD contribution records to silver")

        if df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return pd.DataFrame()

        # Create a copy to avoid modifying original
        df = df.copy()

        # 1. Map column names from bronze to silver
        column_mapping = {
            "content_hash": "source_content_hash",
            "receiving_committee": "committee_name",
            "filing_period": "filing_period",
            "contribution_date": "contribution_date_str",
            "contributor_name": "contributor_name",
            "contributor_address": "contributor_address",
            "contributor_type": "contributor_type",
            "contribution_type": "contribution_type",
            "contribution_amount": "contribution_amount_str",
            "employer_name": "employer_name",
            "employer_occupation": "employer_occupation",
            "office": "office",
            "fund_type": "fund_type",
        }

        df = df.rename(columns=column_mapping)

        # 2. Parse dates
        df["contribution_date"] = df["contribution_date_str"].apply(parse_maryland_date)

        # 3. Parse amounts to numeric
        df["contribution_amount"] = pd.to_numeric(df["contribution_amount_str"], errors="coerce")

        # 4. Parse addresses
        address_parts = df["contributor_address"].apply(parse_maryland_address)
        df["contributor_city"] = address_parts.apply(lambda x: x["city"])
        df["contributor_state"] = address_parts.apply(lambda x: x["state"])
        df["contributor_zip"] = address_parts.apply(lambda x: x["zip"])

        # 5. Enrich with committee data
        if self.session is not None:
            df = self._enrich_with_committee_data(df)
        else:
            logger.warning("No session provided - skipping committee enrichment")
            df["committee_ccf_id"] = None
            df["committee_type"] = None

        # 6. Clean text fields
        text_columns = [
            "contributor_name",
            "committee_name",
            "employer_name",
            "employer_occupation",
        ]
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace(["nan", "None", ""], None)

        # 7. Filter out invalid records
        required_fields = {
            "contribution_date": "contribution date",
            "contribution_amount": "contribution amount",
            "contributor_name": "contributor name",
            "committee_name": "committee name",
        }

        invalid_mask = pd.Series([False] * len(df), index=df.index)
        invalid_reasons = {}

        for field, field_name in required_fields.items():
            if field in df.columns:
                field_invalid = df[field].isna()
                invalid_mask = invalid_mask | field_invalid
                invalid_count = field_invalid.sum()
                if invalid_count > 0:
                    invalid_reasons[field_name] = invalid_count

        valid_df = df[~invalid_mask].copy()
        invalid_count = len(df) - len(valid_df)

        if invalid_count > 0:
            reason_str = ", ".join(
                [f"{count} missing {field}" for field, count in invalid_reasons.items()]
            )
            logger.warning(f"Filtered out {invalid_count} invalid records ({reason_str})")

        # 8. Select final columns for silver layer
        silver_columns = [
            "source_content_hash",
            "contribution_date",
            "contribution_amount",
            "contribution_type",
            "fund_type",
            "contributor_name",
            "contributor_type",
            "contributor_address",
            "contributor_city",
            "contributor_state",
            "contributor_zip",
            "employer_name",
            "employer_occupation",
            "committee_name",
            "committee_ccf_id",
            "committee_type",
            "filing_period",
            "office",
        ]

        # Only include columns that exist
        final_columns = [col for col in silver_columns if col in valid_df.columns]
        valid_df = valid_df[final_columns]

        logger.info(f"Transformation complete: {len(valid_df)} clean records")
        return valid_df

    def _enrich_with_committee_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich DataFrame with committee data via name matching.

        Args:
            df: DataFrame with committee_name column

        Returns:
            DataFrame with committee_ccf_id and committee_type added
        """
        logger = get_logger()

        if self.session is None:
            raise ValueError("Session is required for enrichment")

        logger.info("Enriching with committee data from bronze_md_committee")

        # Get unique committee names
        unique_names = df["committee_name"].dropna().unique().tolist()

        if not unique_names:
            logger.warning("No committee names found in DataFrame")
            df["committee_ccf_id"] = None
            df["committee_type"] = None
            return df

        # Query committee data by name
        stmt = select(
            BronzeMarylandCommittee.committee_name,
            BronzeMarylandCommittee.ccf_id,
            BronzeMarylandCommittee.committee_type,
        ).where(BronzeMarylandCommittee.committee_name.in_(unique_names))

        result = self.session.execute(stmt)
        committee_data = pd.DataFrame(
            result.fetchall(),
            columns=["committee_name", "committee_ccf_id", "committee_type"],
        )

        logger.info(f"Found {len(committee_data)} committees in bronze layer")

        # LEFT JOIN with contribution data
        df = df.merge(
            committee_data,
            on="committee_name",
            how="left",
        )

        # Log any missing committees
        missing_count = df["committee_ccf_id"].isna().sum()
        if missing_count > 0:
            logger.warning(
                f"{missing_count} contributions have committees not found in bronze_md_committee"
            )

        return df


class BronzeToSilverMarylandCommitteeTransformer(BaseTransformer):
    """Transform Maryland committee data from bronze to silver layer."""

    def get_source_layer(self) -> str:
        """Get source layer name."""
        return "bronze"

    def get_target_layer(self) -> str:
        """Get target layer name."""
        return "silver"

    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform bronze Maryland committee data to silver layer.

        Args:
            df: Bronze layer DataFrame
            **kwargs: Additional transformation parameters

        Returns:
            Cleaned DataFrame ready for silver layer
        """
        logger = get_logger()
        logger.info(f"Transforming {len(df)} bronze MD committee records to silver")

        if df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return pd.DataFrame()

        df = df.copy()

        # 1. Map column names
        column_mapping = {
            "ccf_id": "source_ccf_id",
            "committee_name": "name",
            "committee_type": "committee_type",
            "committee_status": "status",
            "election_type": "election_type",
            "registered_date": "registered_date_str",
            "amended_date": "amended_date_str",
            "chairperson_name": "chairperson_name",
            "treasurer_name": "treasurer_name",
            "citation_violations": "citation_violations",
        }

        df = df.rename(columns=column_mapping)

        # 2. Parse dates (convert NaT to None for PostgreSQL compatibility)
        df["registered_date"] = df["registered_date_str"].apply(parse_maryland_date)
        df["amended_date"] = df["amended_date_str"].apply(parse_maryland_date)
        # Convert to object dtype and replace NaT/NaN with None for PostgreSQL
        df["registered_date"] = (
            df["registered_date"].astype(object).where(df["registered_date"].notna(), None)
        )
        df["amended_date"] = (
            df["amended_date"].astype(object).where(df["amended_date"].notna(), None)
        )

        # 3. Determine active status
        df["is_active"] = df["status"].str.lower() == "active"

        # 4. Determine if has violations
        df["has_violations"] = df["citation_violations"].notna() & (df["citation_violations"] != "")

        # 5. Clean text fields
        text_columns = ["name", "chairperson_name", "treasurer_name"]
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace(["nan", "None", ""], None)

        # 6. Select final columns
        silver_columns = [
            "source_ccf_id",
            "name",
            "committee_type",
            "status",
            "is_active",
            "election_type",
            "registered_date",
            "amended_date",
            "chairperson_name",
            "treasurer_name",
            "has_violations",
            "citation_violations",
        ]

        final_columns = [col for col in silver_columns if col in df.columns]
        df = df[final_columns]

        logger.info(f"Transformation complete: {len(df)} clean records")
        return df


class BronzeToSilverMarylandCandidateTransformer(BaseTransformer):
    """Transform Maryland candidate data from bronze to silver layer."""

    def __init__(self, session: Session | None = None):
        """
        Initialize transformer.

        Args:
            session: SQLAlchemy session for committee lookups
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
        Transform bronze Maryland candidate data to silver layer.

        Args:
            df: Bronze layer DataFrame
            **kwargs: Additional transformation parameters

        Returns:
            Cleaned DataFrame ready for silver layer
        """
        logger = get_logger()
        logger.info(f"Transforming {len(df)} bronze MD candidate records to silver")

        if df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return pd.DataFrame()

        df = df.copy()

        # 1. Map column names
        column_mapping = {
            "content_hash": "source_content_hash",
            "candidate_first_name": "first_name",
            "candidate_last_name": "last_name",
            "office_name": "office",
            "district": "district",
            "party": "party",
            "jurisdiction": "jurisdiction",
            "gender": "gender",
            "status": "status",
            "election_year": "election_year",
            "election_type": "election_type",
            "committee_name": "committee_name",
            "email": "email",
            "phone": "phone",
            "website": "website",
        }

        df = df.rename(columns=column_mapping)

        # 2. Create full name
        df["name"] = df.apply(
            lambda row: f"{row['first_name']} {row['last_name']}".strip(),
            axis=1,
        )

        # 3. Determine active status
        active_statuses = ["active", "seeking the nomination"]
        df["is_active"] = df["status"].str.lower().isin(active_statuses)

        # 4. Enrich with committee CCF ID
        if self.session is not None and "committee_name" in df.columns:
            df = self._enrich_with_committee_id(df)
        else:
            df["committee_ccf_id"] = None

        # 5. Clean text fields
        text_columns = ["name", "first_name", "last_name", "party", "office"]
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace(["nan", "None", ""], None)

        # 6. Clean contact fields
        if "phone" in df.columns:
            # Remove non-digit characters from phone
            df["phone"] = df["phone"].astype(str).str.replace(r"[^\d]", "", regex=True)
            df["phone"] = df["phone"].replace(["", "nan"], None)

        # 7. Select final columns
        silver_columns = [
            "source_content_hash",
            "name",
            "first_name",
            "last_name",
            "party",
            "gender",
            "office",
            "district",
            "jurisdiction",
            "status",
            "is_active",
            "election_year",
            "election_type",
            "committee_name",
            "committee_ccf_id",
            "email",
            "phone",
            "website",
        ]

        final_columns = [col for col in silver_columns if col in df.columns]
        df = df[final_columns]

        logger.info(f"Transformation complete: {len(df)} clean records")
        return df

    def _enrich_with_committee_id(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich DataFrame with committee CCF ID via name matching.

        Args:
            df: DataFrame with committee_name column

        Returns:
            DataFrame with committee_ccf_id added
        """
        logger = get_logger()

        if self.session is None:
            raise ValueError("Session is required for enrichment")

        # Get unique committee names
        unique_names = df["committee_name"].dropna().unique().tolist()

        if not unique_names:
            df["committee_ccf_id"] = None
            return df

        # Query committee data
        stmt = select(
            BronzeMarylandCommittee.committee_name,
            BronzeMarylandCommittee.ccf_id,
        ).where(BronzeMarylandCommittee.committee_name.in_(unique_names))

        result = self.session.execute(stmt)
        committee_map = {row[0]: row[1] for row in result}

        logger.info(f"Found {len(committee_map)} committee matches for candidates")

        # Map committee names to CCF IDs
        df["committee_ccf_id"] = df["committee_name"].map(committee_map)

        return df
