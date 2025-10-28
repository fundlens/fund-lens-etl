"""Repository for extraction metadata operations."""

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from fund_lens_etl.models.extraction_metadata import ExtractionMetadata


# noinspection PyMethodMayBeStatic
class ExtractionMetadataRepo:
    """Repository for managing extraction metadata."""

    def get_metadata(
        self, session: Session, source: str, entity_type: str, state: str, cycle: int
    ) -> ExtractionMetadata | None:
        """
        Get metadata for a specific extraction configuration.

        Args:
            session: Database session
            source: Source system (e.g., 'fec_api')
            entity_type: Entity type (e.g., 'contributions')
            state: State code (e.g., 'MD')
            cycle: Two-year transaction period (e.g., 2026)

        Returns:
            ExtractionMetadata if found, None otherwise
        """
        stmt = select(ExtractionMetadata).where(
            ExtractionMetadata.source == source,
            ExtractionMetadata.entity_type == entity_type,
            ExtractionMetadata.state == state,
            ExtractionMetadata.cycle == cycle,
        )

        result = session.execute(stmt).scalar_one_or_none()
        return result

    def upsert_metadata(
        self,
        session: Session,
        source: str,
        entity_type: str,
        state: str,
        cycle: int,
        last_processed_date: datetime | None,
        records_extracted: int,
        status: str = "active",
    ) -> ExtractionMetadata:
        """
        Create or update extraction metadata.

        Args:
            session: Database session
            source: Source system
            entity_type: Entity type
            state: State code
            cycle: Two-year transaction period
            last_processed_date: Latest contribution date processed
            records_extracted: Number of records extracted
            status: Extraction status (active, paused, completed)

        Returns:
            Updated or created ExtractionMetadata
        """
        # Try to get existing metadata
        metadata = self.get_metadata(session, source, entity_type, state, cycle)

        if metadata:
            # Update existing
            metadata.last_processed_date = last_processed_date
            metadata.last_extraction_at = datetime.now(UTC)
            metadata.records_extracted = records_extracted
            metadata.status = status
            metadata.updated_at = datetime.now(UTC)
        else:
            # Create new
            metadata = ExtractionMetadata(
                source=source,
                entity_type=entity_type,
                state=state,
                cycle=cycle,
                last_processed_date=last_processed_date,
                last_extraction_at=datetime.now(),
                records_extracted=records_extracted,
                status=status,
            )
            session.add(metadata)

        session.flush()
        return metadata

    def get_last_processed_date(
        self, session: Session, source: str, entity_type: str, state: str, cycle: int
    ) -> datetime | None:
        """
        Get the last processed date for an extraction configuration.

        Args:
            session: Database session
            source: Source system
            entity_type: Entity type
            state: State code
            cycle: Two-year transaction period

        Returns:
            Last processed date if exists, None for first run
        """
        metadata = self.get_metadata(session, source, entity_type, state, cycle)
        return metadata.last_processed_date if metadata else None
