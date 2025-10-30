"""Bronze layer models - raw data from source systems."""

from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import JSON, Date, DateTime, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from fund_lens_etl.models.base import Base, SourceMetadataMixin, TimestampMixin


class BronzeFECScheduleA(Base, TimestampMixin, SourceMetadataMixin):
    """Raw FEC Schedule A contribution data."""

    __tablename__ = "bronze_fec_schedule_a"

    # Primary key
    sub_id: Mapped[str] = mapped_column(String(255), primary_key=True)

    # Transaction identifiers
    transaction_id: Mapped[str | None] = mapped_column(String(255))
    file_number: Mapped[int | None] = mapped_column(Integer)
    amendment_indicator: Mapped[str | None] = mapped_column(String(10))

    # Contribution details
    contribution_receipt_date: Mapped[date | None] = mapped_column(Date)
    contribution_receipt_amount: Mapped[float | None] = mapped_column(Numeric(12, 2))
    contributor_aggregate_ytd: Mapped[float | None] = mapped_column(Numeric(12, 2))

    # Contributor information
    contributor_name: Mapped[str | None] = mapped_column(String(500))
    contributor_first_name: Mapped[str | None] = mapped_column(String(255))
    contributor_last_name: Mapped[str | None] = mapped_column(String(255))
    contributor_middle_name: Mapped[str | None] = mapped_column(String(255))
    contributor_city: Mapped[str | None] = mapped_column(String(255))
    contributor_state: Mapped[str | None] = mapped_column(String(2))
    contributor_zip: Mapped[str | None] = mapped_column(String(10))
    contributor_employer: Mapped[str | None] = mapped_column(String(500))
    contributor_occupation: Mapped[str | None] = mapped_column(String(255))
    entity_type: Mapped[str | None] = mapped_column(String(10))

    # Committee information (recipient)
    committee_id: Mapped[str | None] = mapped_column(String(20), index=True)
    recipient_committee_designation: Mapped[str | None] = mapped_column(String(10))
    recipient_committee_type: Mapped[str | None] = mapped_column(String(10))
    recipient_committee_org_type: Mapped[str | None] = mapped_column(String(10))

    # Transaction details
    receipt_type: Mapped[str | None] = mapped_column(String(10))
    election_type: Mapped[str | None] = mapped_column(String(10))
    memo_text: Mapped[str | None] = mapped_column(Text)
    memo_code: Mapped[str | None] = mapped_column(String(10))

    # Metadata
    two_year_transaction_period: Mapped[int | None] = mapped_column(Integer)
    report_year: Mapped[int | None] = mapped_column(Integer)
    report_type: Mapped[str | None] = mapped_column(String(10))

    # Raw JSON for full record preservation
    raw_json: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    def __repr__(self) -> str:
        return (
            f"<BronzeFECScheduleA(sub_id={self.sub_id}, amount={self.contribution_receipt_amount})>"
        )


class BronzeFECCandidate(Base, TimestampMixin, SourceMetadataMixin):
    """Raw FEC candidate data."""

    __tablename__ = "bronze_fec_candidate"

    # Primary key
    candidate_id: Mapped[str] = mapped_column(String(20), primary_key=True)

    # Candidate information
    name: Mapped[str | None] = mapped_column(String(500))
    candidate_first_name: Mapped[str | None] = mapped_column(String(255))
    candidate_last_name: Mapped[str | None] = mapped_column(String(255))
    candidate_middle_name: Mapped[str | None] = mapped_column(String(255))

    # Office information
    office: Mapped[str | None] = mapped_column(String(1))  # H, S, P
    office_full: Mapped[str | None] = mapped_column(String(50))
    state: Mapped[str | None] = mapped_column(String(2), index=True)
    district: Mapped[str | None] = mapped_column(String(2))
    district_number: Mapped[int | None] = mapped_column(Integer)

    # Party affiliation
    party: Mapped[str | None] = mapped_column(String(10))
    party_full: Mapped[str | None] = mapped_column(String(100))

    # Incumbency status
    incumbent_challenge: Mapped[str | None] = mapped_column(String(1))
    incumbent_challenge_full: Mapped[str | None] = mapped_column(String(100))

    # Election cycles and years
    cycles: Mapped[list[int] | None] = mapped_column(JSON)
    election_years: Mapped[list[int] | None] = mapped_column(JSON)
    election_districts: Mapped[list[str] | None] = mapped_column(JSON)

    # Status
    is_active: Mapped[bool | None] = mapped_column()
    candidate_status: Mapped[str | None] = mapped_column(String(1))
    candidate_inactive: Mapped[bool | None] = mapped_column()

    # Filing dates
    first_file_date: Mapped[date | None] = mapped_column(Date)
    last_file_date: Mapped[date | None] = mapped_column(Date)
    last_f2_date: Mapped[date | None] = mapped_column(Date)

    # Financial flags
    has_raised_funds: Mapped[bool | None] = mapped_column()
    federal_funds_flag: Mapped[bool | None] = mapped_column()

    # Address information
    address_city: Mapped[str | None] = mapped_column(String(255))
    address_state: Mapped[str | None] = mapped_column(String(2))
    address_street_1: Mapped[str | None] = mapped_column(String(500))
    address_street_2: Mapped[str | None] = mapped_column(String(500))
    address_zip: Mapped[str | None] = mapped_column(String(10))

    # Raw JSON for full record preservation
    raw_json: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    def __repr__(self) -> str:
        return f"<BronzeFECCandidate(candidate_id={self.candidate_id}, name={self.name})>"


class BronzeFECCommittee(Base, TimestampMixin, SourceMetadataMixin):
    """Raw FEC committee data."""

    __tablename__ = "bronze_fec_committee"

    # Primary key
    committee_id: Mapped[str] = mapped_column(String(20), primary_key=True)

    # Committee information
    name: Mapped[str | None] = mapped_column(String(500))
    committee_type: Mapped[str | None] = mapped_column(String(1))
    committee_type_full: Mapped[str | None] = mapped_column(String(100))
    designation: Mapped[str | None] = mapped_column(String(1))
    designation_full: Mapped[str | None] = mapped_column(String(100))

    # Party affiliation (NEW)
    party: Mapped[str | None] = mapped_column(String(10))
    party_full: Mapped[str | None] = mapped_column(String(100))

    # Location
    state: Mapped[str | None] = mapped_column(String(2), index=True)
    city: Mapped[str | None] = mapped_column(String(255))
    street_1: Mapped[str | None] = mapped_column(String(500))
    street_2: Mapped[str | None] = mapped_column(String(500))
    zip: Mapped[str | None] = mapped_column(String(10))

    # Treasurer
    treasurer_name: Mapped[str | None] = mapped_column(String(500))

    # Organization details (NEW)
    organization_type: Mapped[str | None] = mapped_column(String(1))
    organization_type_full: Mapped[str | None] = mapped_column(String(100))

    # Filing information (NEW)
    filing_frequency: Mapped[str | None] = mapped_column(String(1))
    first_file_date: Mapped[date | None] = mapped_column(Date)
    last_file_date: Mapped[date | None] = mapped_column(Date)

    # Affiliated candidate
    candidate_ids: Mapped[list[str] | None] = mapped_column(JSON)

    # Status
    is_active: Mapped[bool | None] = mapped_column()
    cycles: Mapped[list[int] | None] = mapped_column(JSON)

    # Raw JSON for full record preservation
    raw_json: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    def __repr__(self) -> str:
        return f"<BronzeFECCommittee(committee_id={self.committee_id}, name={self.name})>"


class BronzeFECExtractionState(Base, TimestampMixin):
    """Track extraction state for incremental loads."""

    __tablename__ = "bronze_fec_extraction_state"

    # Composite primary key
    committee_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    election_cycle: Mapped[int] = mapped_column(Integer, primary_key=True)

    # Last successfully processed contribution
    last_contribution_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    last_sub_id: Mapped[str] = mapped_column(String(255), nullable=False)

    # Extraction statistics
    total_contributions_extracted: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_extraction_timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        nullable=False,
    )

    # Extraction window for this run
    extraction_start_date: Mapped[date | None] = mapped_column(Date)
    extraction_end_date: Mapped[date | None] = mapped_column(Date)

    # Status tracking
    is_complete: Mapped[bool] = mapped_column(default=False, nullable=False)
    last_page_processed: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    def __repr__(self) -> str:
        return (
            f"<BronzeFECExtractionState("
            f"committee_id={self.committee_id}, "
            f"cycle={self.election_cycle}, "
            f"last_date={self.last_contribution_date}"
            f")>"
        )
