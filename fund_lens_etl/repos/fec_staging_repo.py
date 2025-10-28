"""Repository for fec_contributions_staging table operations."""

import logging

from sqlalchemy import select
from sqlalchemy.orm import Session

from fund_lens_etl.models.fec_contribution_staging import FECContributionStaging

logger = logging.getLogger(__name__)


# noinspection PyMethodMayBeStatic
class FECContributionStagingRepo:
    """
    Repository for fec_contributions_staging table operations.

    Handles all database CRUD operations for FEC contribution staging records.
    Does not manage sessions - caller must provide session for transaction control.
    """

    def __init__(self):
        """Initialize the repository."""
        pass

    def insert(
        self, session: Session, contribution: FECContributionStaging
    ) -> FECContributionStaging:
        """
        Insert a staging contribution record into the database.

        Args:
            session: SQLAlchemy session (managed by caller)
            contribution: FECContributionStaging model instance to insert

        Returns:
            The inserted FECContributionStaging with id populated

        Raises:
            SQLAlchemyError: If database operation fails
        """
        logger.debug(
            f"Inserting staging contribution: cmte_id={contribution.cmte_id}, "
            f"name={contribution.name}, sub_id={contribution.sub_id}"
        )
        session.add(contribution)
        session.flush()  # Flush to get the ID without committing
        logger.info(f"Inserted staging contribution with id={contribution.id}")
        return contribution

    def get_by_raw_filing_id(
        self, session: Session, raw_filing_id: int
    ) -> list[FECContributionStaging]:
        """
        Get all staging contributions from a specific raw filing.

        Args:
            session: SQLAlchemy session
            raw_filing_id: ID of the raw filing record

        Returns:
            List of FECContributionStaging records
        """
        stmt = (
            select(FECContributionStaging)
            .where(FECContributionStaging.raw_filing_id == raw_filing_id)
            .order_by(FECContributionStaging.ingested_at.desc())
        )

        return list(session.execute(stmt).scalars().all())

    def get_unstandardized(
        self, session: Session, limit: int | None = None
    ) -> list[FECContributionStaging]:
        """
        Get contributions that haven't been standardized yet.

        Args:
            session: SQLAlchemy session
            limit: Maximum number of records to return (None = all)

        Returns:
            List of FECContributionStaging records where standardized=False
        """
        stmt = (
            select(FECContributionStaging)
            .where(FECContributionStaging.standardized.is_(False))
            .order_by(FECContributionStaging.ingested_at.asc())
        )

        if limit:
            stmt = stmt.limit(limit)

        return list(session.execute(stmt).scalars().all())

    def get_by_date_range(
        self,
        session: Session,
        start_date: str | None = None,
        end_date: str | None = None,
        standardized: bool | None = None,
    ) -> list[FECContributionStaging]:
        """
        Get contributions by transaction date range.

        Args:
            session: SQLAlchemy session
            start_date: Start date in YYYYMMDD format (FEC format)
            end_date: End date in YYYYMMDD format (FEC format)
            standardized: Filter by standardized flag (None = all records)

        Returns:
            List of FECContributionStaging records
        """
        stmt = select(FECContributionStaging)

        if start_date:
            stmt = stmt.where(FECContributionStaging.transaction_dt >= start_date)
        if end_date:
            stmt = stmt.where(FECContributionStaging.transaction_dt <= end_date)
        if standardized is not None:
            stmt = stmt.where(FECContributionStaging.standardized == standardized)

        stmt = stmt.order_by(FECContributionStaging.transaction_dt.desc())

        return list(session.execute(stmt).scalars().all())

    def get_existing_sub_ids(self, session: Session, sub_ids: list[str]) -> set[str]:
        """
        Check which sub_ids already exist in the staging table.

        Args:
            session: Database session
            sub_ids: List of FEC sub_ids to check

        Returns:
            Set of sub_ids that already exist in the database
        """
        if not sub_ids:
            return set()

        stmt = select(FECContributionStaging.sub_id).where(
            FECContributionStaging.sub_id.in_(sub_ids)
        )

        result = session.execute(stmt).scalars().all()
        return set(result)
