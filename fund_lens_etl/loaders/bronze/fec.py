"""Bronze layer loaders for FEC data."""

import logging
from datetime import UTC, datetime

import pandas as pd
from fund_lens_models.bronze import (
    BronzeFECCandidate,
    BronzeFECCommittee,
    BronzeFECScheduleA,
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


class BronzeFECScheduleALoader(BaseLoader):
    """Load Schedule A contributions into bronze layer."""

    def get_target_table(self) -> str:
        """Get target table name."""
        return "bronze_fec_schedule_a"

    def load(
        self,
        session: Session,
        df: pd.DataFrame,
        source_system: str = "FEC",
        **kwargs,
    ) -> int:
        """
        Load Schedule A data into bronze table.

        Args:
            session: Database session
            df: DataFrame from FECScheduleAExtractor
            source_system: Source system identifier
            **kwargs: Additional parameters

        Returns:
            Number of records inserted/updated
        """
        logger = get_logger()

        if df.empty:
            logger.warning("Empty DataFrame provided to loader")
            return 0

        logger.info(f"Loading {len(df)} Schedule A records to bronze")

        # Add metadata columns
        now = datetime.now(UTC)
        df = df.copy()
        df["created_at"] = now
        df["updated_at"] = now
        df["source_system"] = source_system
        df["ingestion_timestamp"] = now

        # Filter to only columns that exist in the model
        model_columns = list(BronzeFECScheduleA.__table__.columns.keys())
        available_columns = [col for col in model_columns if col in df.columns]

        # Keep only the columns that exist in both DataFrame and model
        df_filtered = df[available_columns]

        # Remove duplicates based on sub_id (keep first occurrence)
        initial_count = len(df_filtered)
        # noinspection PyArgumentEqualDefault
        df_filtered = df_filtered.drop_duplicates(subset=["sub_id"], keep="first")
        dedupe_count = initial_count - len(df_filtered)

        if dedupe_count > 0:
            logger.warning(f"Removed {dedupe_count} duplicate sub_ids from batch")

        logger.info(f"Filtered from {len(df.columns)} to {len(available_columns)} columns")

        # Convert to dict records for insert
        records = df_filtered.to_dict(orient="records")

        # Use PostgreSQL UPSERT (INSERT ... ON CONFLICT DO UPDATE)
        stmt = insert(BronzeFECScheduleA).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=["sub_id"],
            set_={
                "updated_at": now,
                "contribution_receipt_amount": stmt.excluded.contribution_receipt_amount,
                "contributor_name": stmt.excluded.contributor_name,
                "raw_json": stmt.excluded.raw_json,
            },
        )

        result = session.execute(stmt)
        session.commit()

        inserted = getattr(result, "rowcount", 0)
        logger.info(f"Successfully loaded {inserted} Schedule A records")

        return inserted


class BronzeFECCommitteeLoader(BaseLoader):
    """Load committee data into bronze layer."""

    def get_target_table(self) -> str:
        """Get target table name."""
        return "bronze_fec_committee"

    def load(
        self,
        session: Session,
        df: pd.DataFrame,
        source_system: str = "FEC",
        **kwargs,
    ) -> int:
        """
        Load committee data into bronze table.

        Args:
            session: Database session
            df: DataFrame from FECCommitteeExtractor
            source_system: Source system identifier
            **kwargs: Additional parameters

        Returns:
            Number of records inserted/updated
        """
        logger = get_logger()

        if df.empty:
            logger.warning("Empty DataFrame provided to loader")
            return 0

        logger.info(f"Loading {len(df)} committee records to bronze")

        # Add metadata columns
        now = datetime.now(UTC)
        df = df.copy()
        df["created_at"] = now
        df["updated_at"] = now
        df["source_system"] = source_system
        df["ingestion_timestamp"] = now

        # Remove extracted_at (added by extractor, not in model)
        if "extracted_at" in df.columns:
            df = df.drop(columns=["extracted_at"])

        # Set is_active based on data (if not provided)
        if "is_active" not in df.columns:
            df["is_active"] = True  # Default to active

        # Convert to dict records
        records = df.to_dict(orient="records")

        # UPSERT
        stmt = insert(BronzeFECCommittee).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=["committee_id"],
            set_={
                "updated_at": now,
                "name": stmt.excluded.name,
                "committee_type": stmt.excluded.committee_type,
                "committee_type_full": stmt.excluded.committee_type_full,
                "party": stmt.excluded.party,
                "party_full": stmt.excluded.party_full,
                "organization_type": stmt.excluded.organization_type,
                "organization_type_full": stmt.excluded.organization_type_full,
                "filing_frequency": stmt.excluded.filing_frequency,
                "first_file_date": stmt.excluded.first_file_date,
                "last_file_date": stmt.excluded.last_file_date,
                "candidate_ids": stmt.excluded.candidate_ids,
                "cycles": stmt.excluded.cycles,
                "raw_json": stmt.excluded.raw_json,
            },
        )

        result = session.execute(stmt)
        session.commit()

        inserted = getattr(result, "rowcount", 0)
        logger.info(f"Successfully loaded {inserted} committee records")

        return inserted


class BronzeFECCandidateLoader(BaseLoader):
    """Load candidate data into bronze layer."""

    def get_target_table(self) -> str:
        """Get target table name."""
        return "bronze_fec_candidate"

    def load(
        self,
        session: Session,
        df: pd.DataFrame,
        source_system: str = "FEC",
        **kwargs,
    ) -> int:
        """
        Load candidate data into bronze table.

        Args:
            session: Database session
            df: DataFrame from FECCandidateExtractor
            source_system: Source system identifier
            **kwargs: Additional parameters

        Returns:
            Number of records inserted/updated
        """
        logger = get_logger()

        if df.empty:
            logger.warning("Empty DataFrame provided to loader")
            return 0

        logger.info(f"Loading {len(df)} candidate records to bronze")

        # Add metadata columns
        now = datetime.now(UTC)
        df = df.copy()

        # Convert district_number from float to int (handle NaN)
        if "district_number" in df.columns:
            df["district_number"] = (
                df["district_number"].fillna(0).astype("Int64")
            )  # nullable integer

        df["created_at"] = now
        df["updated_at"] = now
        df["source_system"] = source_system
        df["ingestion_timestamp"] = now

        # Remove extracted_at (added by extractor, not in model)
        if "extracted_at" in df.columns:
            df = df.drop(columns=["extracted_at"])

        # Set is_active based on candidate_inactive if not provided
        if "is_active" not in df.columns:
            if "candidate_inactive" in df.columns:
                df["is_active"] = ~df["candidate_inactive"].fillna(False)
            else:
                df["is_active"] = True  # Default to active

        # Convert to dict records
        records = df.to_dict(orient="records")

        # UPSERT
        stmt = insert(BronzeFECCandidate).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=["candidate_id"],
            set_={
                "updated_at": now,
                "name": stmt.excluded.name,
                "candidate_first_name": stmt.excluded.candidate_first_name,
                "candidate_last_name": stmt.excluded.candidate_last_name,
                "candidate_middle_name": stmt.excluded.candidate_middle_name,
                "office": stmt.excluded.office,
                "office_full": stmt.excluded.office_full,
                "district": stmt.excluded.district,
                "district_number": stmt.excluded.district_number,
                "party": stmt.excluded.party,
                "party_full": stmt.excluded.party_full,
                "incumbent_challenge": stmt.excluded.incumbent_challenge,
                "incumbent_challenge_full": stmt.excluded.incumbent_challenge_full,
                "candidate_status": stmt.excluded.candidate_status,
                "candidate_inactive": stmt.excluded.candidate_inactive,
                "first_file_date": stmt.excluded.first_file_date,
                "last_file_date": stmt.excluded.last_file_date,
                "last_f2_date": stmt.excluded.last_f2_date,
                "has_raised_funds": stmt.excluded.has_raised_funds,
                "federal_funds_flag": stmt.excluded.federal_funds_flag,
                "address_city": stmt.excluded.address_city,
                "address_state": stmt.excluded.address_state,
                "address_street_1": stmt.excluded.address_street_1,
                "address_street_2": stmt.excluded.address_street_2,
                "address_zip": stmt.excluded.address_zip,
                "cycles": stmt.excluded.cycles,
                "election_years": stmt.excluded.election_years,
                "election_districts": stmt.excluded.election_districts,
                "raw_json": stmt.excluded.raw_json,
            },
        )

        result = session.execute(stmt)
        session.commit()

        inserted = getattr(result, "rowcount", 0)
        logger.info(f"Successfully loaded {inserted} candidate records")

        return inserted
