"""Bronze layer loaders for Maryland campaign finance data."""

import logging
from datetime import UTC, datetime

import pandas as pd
from fund_lens_models.bronze import (
    BronzeMarylandCandidate,
    BronzeMarylandCommittee,
    BronzeMarylandContribution,
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


class BronzeMarylandContributionLoader(BaseLoader):
    """Load Maryland contribution data into bronze layer."""

    def get_target_table(self) -> str:
        """Get target table name."""
        return "bronze_md_contribution"

    def load(
        self,
        session: Session,
        df: pd.DataFrame,
        source_system: str = "MARYLAND",
        **kwargs,
    ) -> int:
        """
        Load contribution data into bronze table.

        Uses UPSERT (INSERT ... ON CONFLICT) to handle duplicates.
        Conflicts are detected via content_hash.

        Args:
            session: Database session
            df: DataFrame from MarylandContributionExtractor
            source_system: Source system identifier
            **kwargs: Additional parameters

        Returns:
            Number of records inserted/updated
        """
        logger = get_logger()

        if df.empty:
            logger.warning("Empty DataFrame provided to loader")
            return 0

        logger.info(f"Loading {len(df)} Maryland contribution records to bronze")

        # Add metadata columns
        now = datetime.now(UTC)
        df = df.copy()
        df["created_at"] = now
        df["updated_at"] = now
        df["source_system"] = source_system
        df["ingestion_timestamp"] = now

        # Filter to only columns that exist in the model
        model_columns = list(BronzeMarylandContribution.__table__.columns.keys())
        # Exclude 'id' as it's auto-generated
        model_columns = [col for col in model_columns if col != "id"]
        available_columns = [col for col in model_columns if col in df.columns]

        # Keep only the columns that exist in both DataFrame and model
        df_filtered = df[available_columns]

        # Remove duplicates based on content_hash (keep first occurrence)
        initial_count = len(df_filtered)
        df_filtered = df_filtered.drop_duplicates(subset=["content_hash"], keep="first")
        dedupe_count = initial_count - len(df_filtered)

        if dedupe_count > 0:
            logger.warning(f"Removed {dedupe_count} duplicate content_hashes from batch")

        logger.info(f"Filtered from {len(df.columns)} to {len(available_columns)} columns")

        # Convert to dict records for insert
        records = df_filtered.to_dict(orient="records")

        # Use PostgreSQL UPSERT (INSERT ... ON CONFLICT DO UPDATE)
        stmt = insert(BronzeMarylandContribution).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=["content_hash"],
            set_={
                "updated_at": now,
                "receiving_committee": stmt.excluded.receiving_committee,
                "contribution_amount": stmt.excluded.contribution_amount,
                "contributor_name": stmt.excluded.contributor_name,
            },
        )

        result = session.execute(stmt)
        session.commit()

        inserted = getattr(result, "rowcount", 0)
        logger.info(f"Successfully loaded {inserted} Maryland contribution records")

        return inserted


class BronzeMarylandCommitteeLoader(BaseLoader):
    """Load Maryland committee data into bronze layer."""

    def get_target_table(self) -> str:
        """Get target table name."""
        return "bronze_md_committee"

    def load(
        self,
        session: Session,
        df: pd.DataFrame,
        source_system: str = "MARYLAND",
        **kwargs,
    ) -> int:
        """
        Load committee data into bronze table.

        Uses UPSERT (INSERT ... ON CONFLICT) to handle duplicates.
        Conflicts are detected via ccf_id.

        Args:
            session: Database session
            df: DataFrame from MarylandCommitteeExtractor
            source_system: Source system identifier
            **kwargs: Additional parameters

        Returns:
            Number of records inserted/updated
        """
        logger = get_logger()

        if df.empty:
            logger.warning("Empty DataFrame provided to loader")
            return 0

        logger.info(f"Loading {len(df)} Maryland committee records to bronze")

        # Add metadata columns
        now = datetime.now(UTC)
        df = df.copy()
        df["created_at"] = now
        df["updated_at"] = now
        df["source_system"] = source_system
        df["ingestion_timestamp"] = now

        # Filter to only columns that exist in the model
        model_columns = list(BronzeMarylandCommittee.__table__.columns.keys())
        # Exclude 'id' as it's auto-generated
        model_columns = [col for col in model_columns if col != "id"]
        available_columns = [col for col in model_columns if col in df.columns]

        # Keep only the columns that exist in both DataFrame and model
        df_filtered = df[available_columns]

        # Remove duplicates based on ccf_id (keep first occurrence)
        initial_count = len(df_filtered)
        df_filtered = df_filtered.drop_duplicates(subset=["ccf_id"], keep="first")
        dedupe_count = initial_count - len(df_filtered)

        if dedupe_count > 0:
            logger.warning(f"Removed {dedupe_count} duplicate ccf_ids from batch")

        logger.info(f"Filtered from {len(df.columns)} to {len(available_columns)} columns")

        # Convert to dict records for insert
        records = df_filtered.to_dict(orient="records")

        # Use PostgreSQL UPSERT (INSERT ... ON CONFLICT DO UPDATE)
        stmt = insert(BronzeMarylandCommittee).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=["ccf_id"],
            set_={
                "updated_at": now,
                "committee_name": stmt.excluded.committee_name,
                "committee_status": stmt.excluded.committee_status,
                "committee_type": stmt.excluded.committee_type,
                "election_type": stmt.excluded.election_type,
                "amended_date": stmt.excluded.amended_date,
                "chairperson_name": stmt.excluded.chairperson_name,
                "chairperson_address": stmt.excluded.chairperson_address,
                "treasurer_name": stmt.excluded.treasurer_name,
                "treasurer_address": stmt.excluded.treasurer_address,
                "citation_violations": stmt.excluded.citation_violations,
            },
        )

        result = session.execute(stmt)
        session.commit()

        inserted = getattr(result, "rowcount", 0)
        logger.info(f"Successfully loaded {inserted} Maryland committee records")

        return inserted


class BronzeMarylandCandidateLoader(BaseLoader):
    """Load Maryland candidate data into bronze layer."""

    def get_target_table(self) -> str:
        """Get target table name."""
        return "bronze_md_candidate"

    def load(
        self,
        session: Session,
        df: pd.DataFrame,
        source_system: str = "MARYLAND_SBE",
        **kwargs,
    ) -> int:
        """
        Load candidate data into bronze table.

        Uses UPSERT (INSERT ... ON CONFLICT) to handle duplicates.
        Conflicts are detected via content_hash.

        Args:
            session: Database session
            df: DataFrame from MarylandCandidateExtractor
            source_system: Source system identifier
            **kwargs: Additional parameters

        Returns:
            Number of records inserted/updated
        """
        logger = get_logger()

        if df.empty:
            logger.warning("Empty DataFrame provided to loader")
            return 0

        logger.info(f"Loading {len(df)} Maryland candidate records to bronze")

        # Add metadata columns
        now = datetime.now(UTC)
        df = df.copy()
        df["created_at"] = now
        df["updated_at"] = now
        df["source_system"] = source_system
        df["ingestion_timestamp"] = now

        # Filter to only columns that exist in the model
        model_columns = list(BronzeMarylandCandidate.__table__.columns.keys())
        # Exclude 'id' as it's auto-generated
        model_columns = [col for col in model_columns if col != "id"]
        available_columns = [col for col in model_columns if col in df.columns]

        # Keep only the columns that exist in both DataFrame and model
        df_filtered = df[available_columns]

        # Remove duplicates based on content_hash (keep first occurrence)
        initial_count = len(df_filtered)
        df_filtered = df_filtered.drop_duplicates(subset=["content_hash"], keep="first")
        dedupe_count = initial_count - len(df_filtered)

        if dedupe_count > 0:
            logger.warning(f"Removed {dedupe_count} duplicate content_hashes from batch")

        logger.info(f"Filtered from {len(df.columns)} to {len(available_columns)} columns")

        # Convert to dict records for insert
        records = df_filtered.to_dict(orient="records")

        # Use PostgreSQL UPSERT (INSERT ... ON CONFLICT DO UPDATE)
        stmt = insert(BronzeMarylandCandidate).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=["content_hash"],
            set_={
                "updated_at": now,
                "status": stmt.excluded.status,
                "filing_type_and_date": stmt.excluded.filing_type_and_date,
                "committee_name": stmt.excluded.committee_name,
                "phone": stmt.excluded.phone,
                "email": stmt.excluded.email,
                "website": stmt.excluded.website,
                "facebook": stmt.excluded.facebook,
                "twitter": stmt.excluded.twitter,
            },
        )

        result = session.execute(stmt)
        session.commit()

        inserted = getattr(result, "rowcount", 0)
        logger.info(f"Successfully loaded {inserted} Maryland candidate records")

        return inserted
