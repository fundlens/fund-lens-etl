"""Repository for raw_filings table operations."""

import logging
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from fund_lens_etl.models.raw_filing import RawFiling

logger = logging.getLogger(__name__)


# noinspection PyMethodMayBeStatic
class RawFilingRepo:
    """
    Repository for raw_filings table operations.

    Handles all database CRUD operations for raw filing records.
    Does not manage sessions - caller must provide session for transaction control.
    """

    def __init__(self):
        """Initialize the repository."""
        pass

    def insert(self, session: Session, raw_filing: RawFiling) -> RawFiling:
        """
        Insert a raw filing record into the database.

        Args:
            session: SQLAlchemy session (managed by caller)
            raw_filing: RawFiling model instance to insert

        Returns:
            The inserted RawFiling with id populated

        Raises:
            SQLAlchemyError: If database operation fails
        """
        logger.debug(
            f"Inserting raw filing: source={raw_filing.source}, file_hash={raw_filing.file_hash}"
        )
        session.add(raw_filing)
        session.flush()  # Flush to get the ID without committing
        logger.info(f"Inserted raw filing with id={raw_filing.id}")
        return raw_filing

    def get_by_file_hash(self, session: Session, file_hash: str) -> RawFiling | None:
        """
        Get a raw filing by file hash.

        Args:
            session: SQLAlchemy session
            file_hash: SHA-256 hash of the file

        Returns:
            RawFiling if found, None otherwise
        """
        stmt = select(RawFiling).where(RawFiling.file_hash == file_hash)
        return session.execute(stmt).scalar_one_or_none()

    def get_by_source_and_date_range(
        self,
        session: Session,
        source: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[RawFiling]:
        """
        Get raw filings by source and optional date range.

        Args:
            session: SQLAlchemy session
            source: Source identifier (e.g., 'fec_api', 'fec_bulk')
            start_date: Filter for filings ingested on or after this date
            end_date: Filter for filings ingested on or before this date

        Returns:
            List of RawFiling records
        """
        stmt = select(RawFiling).where(RawFiling.source == source)

        if start_date:
            stmt = stmt.where(RawFiling.ingested_at >= start_date)
        if end_date:
            stmt = stmt.where(RawFiling.ingested_at <= end_date)

        stmt = stmt.order_by(RawFiling.ingested_at.desc())
        return list(session.execute(stmt).scalars().all())
