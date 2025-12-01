"""Silver layer loaders for Maryland campaign finance data."""

import logging
from datetime import UTC, datetime

import pandas as pd
from fund_lens_models.silver import (
    SilverMarylandCandidate,
    SilverMarylandCommittee,
    SilverMarylandContribution,
)
from prefect import get_run_logger
from prefect.exceptions import MissingContextError
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from fund_lens_etl.loaders.base import BaseLoader


def get_logger():
    """Get logger - Prefect if available, otherwise standard logging."""
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(__name__)


class SilverMarylandContributionLoader(BaseLoader):
    """Load Maryland contribution data into silver layer."""

    def get_target_table(self) -> str:
        """Get target table name."""
        return "silver_md_contribution"

    def load(
        self,
        session: Session,
        df: pd.DataFrame,
        **kwargs,
    ) -> int:
        """
        Load contribution data into silver table.

        Uses UPSERT (INSERT ... ON CONFLICT) to handle duplicates.
        Conflicts are detected via source_content_hash.

        Args:
            session: Database session
            df: DataFrame from BronzeToSilverMarylandContributionTransformer
            **kwargs: Additional parameters

        Returns:
            Number of records inserted/updated
        """
        logger = get_logger()

        if df.empty:
            logger.warning("Empty DataFrame provided to loader")
            return 0

        logger.info(f"Loading {len(df)} Maryland contribution records to silver")

        # Add metadata columns
        now = datetime.now(UTC)
        df = df.copy()
        df["created_at"] = now
        df["updated_at"] = now

        # Filter to only columns that exist in the model
        model_columns = list(SilverMarylandContribution.__table__.columns.keys())
        # Exclude 'id' as it's auto-generated
        model_columns = [col for col in model_columns if col != "id"]
        available_columns = [col for col in model_columns if col in df.columns]

        # Keep only the columns that exist in both DataFrame and model
        df_filtered = df[available_columns]

        # Remove duplicates based on source_content_hash (keep first occurrence)
        initial_count = len(df_filtered)
        df_filtered = df_filtered.drop_duplicates(subset=["source_content_hash"], keep="first")
        dedupe_count = initial_count - len(df_filtered)

        if dedupe_count > 0:
            logger.warning(f"Removed {dedupe_count} duplicate source_content_hashes from batch")

        logger.info(f"Filtered from {len(df.columns)} to {len(available_columns)} columns")

        # Convert to dict records for insert
        records = df_filtered.to_dict(orient="records")

        # Use PostgreSQL UPSERT (INSERT ... ON CONFLICT DO UPDATE)
        stmt = insert(SilverMarylandContribution).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=["source_content_hash"],
            set_={
                "updated_at": now,
                "contribution_date": stmt.excluded.contribution_date,
                "contribution_amount": stmt.excluded.contribution_amount,
                "contribution_type": stmt.excluded.contribution_type,
                "contributor_name": stmt.excluded.contributor_name,
                "contributor_type": stmt.excluded.contributor_type,
                "contributor_city": stmt.excluded.contributor_city,
                "contributor_state": stmt.excluded.contributor_state,
                "contributor_zip": stmt.excluded.contributor_zip,
                "employer_name": stmt.excluded.employer_name,
                "employer_occupation": stmt.excluded.employer_occupation,
                "committee_name": stmt.excluded.committee_name,
                "committee_ccf_id": stmt.excluded.committee_ccf_id,
                "committee_type": stmt.excluded.committee_type,
            },
        )

        result = session.execute(stmt)
        session.commit()

        inserted = getattr(result, "rowcount", 0)
        logger.info(f"Successfully loaded {inserted} Maryland contribution records to silver")

        return inserted


class SilverMarylandCommitteeLoader(BaseLoader):
    """Load Maryland committee data into silver layer."""

    def get_target_table(self) -> str:
        """Get target table name."""
        return "silver_md_committee"

    def load(
        self,
        session: Session,
        df: pd.DataFrame,
        **kwargs,
    ) -> int:
        """
        Load committee data into silver table.

        Uses UPSERT (INSERT ... ON CONFLICT) to handle duplicates.
        Conflicts are detected via source_ccf_id.

        Args:
            session: Database session
            df: DataFrame from BronzeToSilverMarylandCommitteeTransformer
            **kwargs: Additional parameters

        Returns:
            Number of records inserted/updated
        """
        logger = get_logger()

        if df.empty:
            logger.warning("Empty DataFrame provided to loader")
            return 0

        logger.info(f"Loading {len(df)} Maryland committee records to silver")

        # Add metadata columns
        now = datetime.now(UTC)
        df = df.copy()
        df["created_at"] = now
        df["updated_at"] = now

        # Filter to only columns that exist in the model
        model_columns = list(SilverMarylandCommittee.__table__.columns.keys())
        # Exclude 'id' as it's auto-generated
        model_columns = [col for col in model_columns if col != "id"]
        available_columns = [col for col in model_columns if col in df.columns]

        # Keep only the columns that exist in both DataFrame and model
        df_filtered = df[available_columns]

        # Remove duplicates based on source_ccf_id (keep first occurrence)
        initial_count = len(df_filtered)
        df_filtered = df_filtered.drop_duplicates(subset=["source_ccf_id"], keep="first")
        dedupe_count = initial_count - len(df_filtered)

        if dedupe_count > 0:
            logger.warning(f"Removed {dedupe_count} duplicate source_ccf_ids from batch")

        logger.info(f"Filtered from {len(df.columns)} to {len(available_columns)} columns")

        # Convert to dict records for insert
        records = df_filtered.to_dict(orient="records")

        # Use PostgreSQL UPSERT (INSERT ... ON CONFLICT DO UPDATE)
        stmt = insert(SilverMarylandCommittee).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=["source_ccf_id"],
            set_={
                "updated_at": now,
                "name": stmt.excluded.name,
                "committee_type": stmt.excluded.committee_type,
                "status": stmt.excluded.status,
                "is_active": stmt.excluded.is_active,
                "election_type": stmt.excluded.election_type,
                "amended_date": stmt.excluded.amended_date,
                "chairperson_name": stmt.excluded.chairperson_name,
                "treasurer_name": stmt.excluded.treasurer_name,
                "has_violations": stmt.excluded.has_violations,
                "citation_violations": stmt.excluded.citation_violations,
            },
        )

        result = session.execute(stmt)
        session.commit()

        inserted = getattr(result, "rowcount", 0)
        logger.info(f"Successfully loaded {inserted} Maryland committee records to silver")

        return inserted


class SilverMarylandCandidateLoader(BaseLoader):
    """Load Maryland candidate data into silver layer."""

    def get_target_table(self) -> str:
        """Get target table name."""
        return "silver_md_candidate"

    def load(
        self,
        session: Session,
        df: pd.DataFrame,
        **kwargs,
    ) -> int:
        """
        Load candidate data into silver table.

        Uses UPSERT (INSERT ... ON CONFLICT) to handle duplicates.
        Conflicts are detected via source_content_hash.

        Args:
            session: Database session
            df: DataFrame from BronzeToSilverMarylandCandidateTransformer
            **kwargs: Additional parameters

        Returns:
            Number of records inserted/updated
        """
        logger = get_logger()

        if df.empty:
            logger.warning("Empty DataFrame provided to loader")
            return 0

        logger.info(f"Loading {len(df)} Maryland candidate records to silver")

        # Add metadata columns
        now = datetime.now(UTC)
        df = df.copy()
        df["created_at"] = now
        df["updated_at"] = now

        # Filter to only columns that exist in the model
        model_columns = list(SilverMarylandCandidate.__table__.columns.keys())
        # Exclude 'id' as it's auto-generated
        model_columns = [col for col in model_columns if col != "id"]
        available_columns = [col for col in model_columns if col in df.columns]

        # Keep only the columns that exist in both DataFrame and model
        df_filtered = df[available_columns]

        # Remove duplicates based on source_content_hash (keep first occurrence)
        initial_count = len(df_filtered)
        df_filtered = df_filtered.drop_duplicates(subset=["source_content_hash"], keep="first")
        dedupe_count = initial_count - len(df_filtered)

        if dedupe_count > 0:
            logger.warning(f"Removed {dedupe_count} duplicate source_content_hashes from batch")

        logger.info(f"Filtered from {len(df.columns)} to {len(available_columns)} columns")

        # Convert to dict records for insert
        records = df_filtered.to_dict(orient="records")

        # Use PostgreSQL UPSERT (INSERT ... ON CONFLICT DO UPDATE)
        stmt = insert(SilverMarylandCandidate).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=["source_content_hash"],
            set_={
                "updated_at": now,
                "name": stmt.excluded.name,
                "first_name": stmt.excluded.first_name,
                "last_name": stmt.excluded.last_name,
                "party": stmt.excluded.party,
                "gender": stmt.excluded.gender,
                "office": stmt.excluded.office,
                "district": stmt.excluded.district,
                "jurisdiction": stmt.excluded.jurisdiction,
                "status": stmt.excluded.status,
                "is_active": stmt.excluded.is_active,
                "committee_name": stmt.excluded.committee_name,
                "committee_ccf_id": stmt.excluded.committee_ccf_id,
                "email": stmt.excluded.email,
                "phone": stmt.excluded.phone,
                "website": stmt.excluded.website,
            },
        )

        result = session.execute(stmt)
        session.commit()

        inserted = getattr(result, "rowcount", 0)
        logger.info(f"Successfully loaded {inserted} Maryland candidate records to silver")

        return inserted
