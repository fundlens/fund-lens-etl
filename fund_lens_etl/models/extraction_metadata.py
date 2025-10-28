"""Extraction metadata model for tracking incremental loads."""

from datetime import datetime

from sqlalchemy import DateTime, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from fund_lens_etl.database import Base


class ExtractionMetadata(Base):
    """
    Tracks the state of incremental data extractions.

    This table maintains metadata about what data has been extracted from each source,
    allowing for incremental loads that only fetch new data since the last run.
    """

    __tablename__ = "extraction_metadata"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source: Mapped[str] = mapped_column(
        String(50), nullable=False, comment="Source system (e.g., fec_api)"
    )
    entity_type: Mapped[str] = mapped_column(
        String(50), nullable=False, comment="Type of entity (e.g., contributions)"
    )
    state: Mapped[str] = mapped_column(String(2), nullable=False, comment="State code (e.g., MD)")
    cycle: Mapped[int] = mapped_column(
        Integer, nullable=False, comment="Two-year transaction period (e.g., 2026)"
    )
    last_processed_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Latest contribution date processed",
    )
    last_extraction_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment="When this extraction run completed",
    )
    records_extracted: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Number of records extracted in last run",
    )
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="active",
        comment="active, paused, completed",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        UniqueConstraint(
            "source", "entity_type", "state", "cycle", name="uq_extraction_metadata_key"
        ),
        Index(
            "ix_extraction_metadata_lookup",
            "source",
            "entity_type",
            "state",
            "cycle",
            "status",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<ExtractionMetadata(source={self.source}, entity_type={self.entity_type}, "
            f"state={self.state}, cycle={self.cycle}, "
            f"last_processed_date={self.last_processed_date})>"
        )
